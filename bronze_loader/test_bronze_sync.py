#!/usr/bin/env python3
"""End-to-end test of bronze_sync.py against a real Postgres, with an in-memory fake instead of Oracle.
Run:  PG_DSN="host=/tmp port=55432 user=postgres dbname=bronze_test" python3 test_bronze_sync.py
(the database must exist and be empty)"""
import datetime as dt
import decimal
import os
import tempfile

import psycopg

import bronze_sync as bs

D = dt.datetime
LOG_DIR = tempfile.mkdtemp().replace("\\", "/")
CFG = tempfile.mktemp(suffix=".toml")
open(CFG, "w").write(f'''
[settings]
log_file = "{LOG_DIR}/b.log"
arraysize = 50
retries = 1
retry_sleep = 0
[[zone]]
name = "SZ"
schemas = ["PORTAL"]
[[table]]
zone = "SZ"
name = "EVT"
mode = "append"
chunk_col = "TS"
watermark_col = "TS"
partitioned = true
verify_counts = true
lookback_hours = 48
[[table]]
zone = "SZ"
name = "MST"
watermark_col = "UPD_DT"
[[table]]
zone = "SZ"
name = "BIG"
mode = "append"
chunk_col = "TS"
[[zone]]
name = "NZ"
schemas = ["CRM"]
[[table]]
zone = "NZ"
name = "IBIB"
mode = "merge"
pk = ["IBASE"]
watermark_col = "UPTIM"
[[table]]
zone = "NZ"
name = "BUT000"
mode = "merge"
pk = ["PARTNER"]
watermark_col = "CHDAT"
[[table]]
zone = "NZ"
name = "CRMD_ORDERADM_H"
mode = "merge"
pk = ["GUID"]
watermark_col = "CHANGED_AT"
''')


def col(name, typ, p=None, s=None, nullable=False):
    return dict(name=name, type=typ, precision=p, scale=s, nullable=nullable)


class Fake:
    """stands in for OracleSource"""
    NOW = D(2026, 11, 4, 12, 0)

    def __init__(self):
        self.cols = {
            "EVT": [col("ID", "NUMBER", 18, 0), col("TS", "DATE"), col("NAME", "VARCHAR2"), col("AMT", "NUMBER", 10, 2)],
            "MST": [col("ID", "NUMBER", 9, 0), col("UPD_DT", "DATE"), col("NAME", "VARCHAR2")],
            "BIG": [col("ID", "NUMBER", 18, 0), col("TS", "DATE")],
            "SMALL": [col("K", "VARCHAR2"), col("V", "NUMBER")],
        }
        self.pks = {"MST": ["ID"]}
        self.rows = {"EVT": [], "MST": [], "BIG": [], "SMALL": [(f"k{i}", i) for i in range(5)]}
        for i in range(300):           # 3 days x 100 rows
            self.rows["EVT"].append((i, D(2026, 10, 1 + i // 100, i % 24, 0), "n\x00ame%d" % i, decimal.Decimal("1.50")))
        for i in range(50):
            self.rows["MST"].append((i, D(2026, 10, 2), f"cust{i}"))
        for i in range(600):           # 5 days x 120
            self.rows["BIG"].append((i, D(2026, 10, 1 + i // 120, 1, 0)))

        # North Zone SAP CRM tables
        self.nz_cols = {
            "IBIB": [col("IBASE", "NUMBER", 18, 0), col("CRTIM", "VARCHAR2"), col("UPTIM", "DATE"), col("EXTID", "VARCHAR2"), col("AUTHG", "VARCHAR2"), col("ZZSERVICETYP", "VARCHAR2")],
            "BUT000": [col("PARTNER", "VARCHAR2"), col("NAME_FIRST", "VARCHAR2"), col("NAME_LAST", "VARCHAR2"), col("NAME_LST2", "VARCHAR2"), col("ZZFIELD10", "VARCHAR2"), col("CHDAT", "DATE")],
            "CRMD_ORDERADM_H": [col("GUID", "VARCHAR2"), col("OBJECT_ID", "VARCHAR2"), col("PROCESS_TYPE", "VARCHAR2"), col("CHANGED_AT", "DATE")],
        }
        self.nz_pks = {
            "IBIB": ["IBASE"],
            "BUT000": ["PARTNER"],
            "CRMD_ORDERADM_H": ["GUID"]
        }
        self.nz_rows = {
            "IBIB": [(146785800 + i, "20250319062844", D(2026, 10, 2), f"01632-29737{i}", "FZP", "BHARAT FIBER BB") for i in range(25)],
            "BUT000": [(f"102076625{i}", f"FIRST_{i}", f"LAST_{i}", f"AACFI4502{i%10}", f"07AAAAA000{i:02d}", D(2026, 10, 2)) for i in range(30)],
            "CRMD_ORDERADM_H": [(f"52E27F920ABA48BDE10000000ACAD0{i:02d}", f"80117830{i:02d}", "ZTA", D(2026, 10, 2)) for i in range(20)]
        }
        self.cols.update(self.nz_cols)
        self.pks.update(self.nz_pks)
        self.rows.update(self.nz_rows)

        self.fail = lambda table, pred, nth: False
        self.calls = {}

    def tables(self, schema=None):
        if schema == "CRM":
            return [(t, len(r)) for t, r in self.nz_rows.items()]
        if schema == "PORTAL":
            return [(t, len(r)) for t, r in self.rows.items() if t not in self.nz_rows]
        # None or "*"
        return [(t, len(r)) for t, r in self.rows.items()]

    def schemas(self):
        return ["CRM", "PORTAL"]

    def current_schema(self):
        return "CRM"

    def all_accessible_tables(self):
        res = []
        for t, r in self.nz_rows.items():
            res.append(dict(owner="CRM", table_name=t, num_rows=len(r)))
        for t, r in self.rows.items():
            if t not in self.nz_rows:
                res.append(dict(owner="PORTAL", table_name=t, num_rows=len(r)))
        return res

    def columns(self, schema, table):
        return self.cols[table]

    def pk_cols(self, schema, table):
        return self.pks.get(table, [])

    def now(self):
        return self.NOW

    def bounds(self, schema, table, c):
        i = [x["name"] for x in self.cols[table]].index(c)
        v = [r[i] for r in self.rows[table]]
        return (min(v), max(v)) if v else (None, None)

    def _sel(self, table, pred):
        names = [x["name"] for x in self.cols[table]]
        out = []
        for r in self.rows[table]:
            if pred["kind"] == "all":
                out.append(r)
                continue
            v = r[names.index(pred["col"])]
            if pred["kind"] == "range" and pred["a"] <= v < pred["b"]:
                out.append(r)
            if pred["kind"] == "window" and pred["a"] < v <= pred["b"]:
                out.append(r)
            if pred["kind"] == "null" and v is None:
                out.append(r)
        return out

    def count(self, schema, table, pred):
        return len(self._sel(table, pred))

    def stream(self, schema, table, cols, pred, hint=None):
        n = self.calls[(table, pred["a"])] = self.calls.get((table, pred["a"]), 0) + 1
        names = [x["name"] for x in self.cols[table]]
        idx = [names.index(c) for c in cols]
        rows = [tuple(r[i] for i in idx) for r in self._sel(table, pred)]
        for k in range(0, len(rows), 50):
            if k > 0 and self.fail(table, pred, n):
                raise ConnectionError("simulated Oracle disconnect")
            yield rows[k:k + 50]

    def close(self):
        pass


# Test load_env unit behavior
tmp_env = tempfile.mktemp(suffix=".env")
with open(tmp_env, "w", encoding="utf-8") as f:
    f.write('TEST_ENV_KEY_1="value_1"\nTEST_ENV_KEY_2=value_2 # comment\nexport TEST_ENV_KEY_3=\'value_3\'\nTEST_ENV_KEY_4="=value_4"\n')
bs.load_env(tmp_env, override=True)
assert os.environ.get("TEST_ENV_KEY_1") == "value_1", "load_env failed on quoted value"
assert os.environ.get("TEST_ENV_KEY_2") == "value_2", "load_env failed on unquoted with comment"
assert os.environ.get("TEST_ENV_KEY_3") == "value_3", "load_env failed on export"
assert os.environ.get("TEST_ENV_KEY_4") == "value_4", "load_env failed on leading equal"
try:
    os.remove(tmp_env)
except Exception:
    pass

F = Fake()
bs.SOURCE_FACTORY = lambda zone, settings: F
if "PG_DSN" not in os.environ:
    raise SystemExit("PG_DSN is required for integration tests. Set it in .env or your environment.")
try:
    pg = psycopg.connect(os.environ["PG_DSN"], autocommit=True)
except Exception as e:
    raise SystemExit(
        f"\n[!] Cannot connect to PostgreSQL at {os.environ.get('PG_DSN')}: {e}\n"
        f"    To run unit checks without a PostgreSQL instance, run: python test_checks_unit.py\n"
        f"    For integration tests, please ensure PostgreSQL is started and accessible."
    )
q = lambda s, *a: pg.execute(s, a or None).fetchall()   # noqa: E731  (no params -> % is not a placeholder)


def run(*args):
    cmd_args = list(args)
    if "--workers" not in cmd_args and any(c in cmd_args for c in ("historic", "incr")):
        cmd_args.extend(["--workers", "1"])
    return bs.main(cmd_args + ["--config", CFG])


def count(t):
    return q(f"select count(*) from bronze_sz.{t}")[0][0]


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    assert cond, name


pg.execute("DROP SCHEMA IF EXISTS bronze_sz CASCADE; DROP SCHEMA IF EXISTS bronze_nz CASCADE; DROP SCHEMA IF EXISTS bronze_ctl CASCADE;")
run("init")
run("discover")
cfgs = {r[0]: r for r in q("select src_table, mode, tgt_table, partitioned from bronze_ctl.table_config")}
check("discover modes: EVT append, MST merge (pk+watermark), SMALL full_refresh",
      (cfgs["EVT"][1], cfgs["MST"][1], cfgs["SMALL"][1]) == ("append", "merge", "full_refresh"))
check("discover NZ SAP tables: IBIB merge, BUT000 merge, CRMD_ORDERADM_H merge",
      (cfgs["IBIB"][1], cfgs["BUT000"][1], cfgs["CRMD_ORDERADM_H"][1]) == ("merge", "merge", "merge"))

# ---- historic
check("historic run ok", run("historic", "--workers", "1", "--table", "EVT") == 0)
run("historic", "--workers", "1", "--table", "MST")
run("historic", "--workers", "1", "--table", "SMALL")
check("EVT 300 rows, NUL stripped", count("evt") == 300 and q("select count(*) from bronze_sz.evt where name like '%'||chr(0)||'%'" if False else "select count(*) from bronze_sz.evt where name = 'name0'")[0][0] == 1)
check("EVT partitioned monthly", q("select count(*) from pg_inherits where inhparent='bronze_sz.evt'::regclass")[0][0] >= 2)
check("MST 50 rows, SMALL 5 rows", count("mst") == 50 and count("small") == 5)
check("lineage columns filled", q("select count(*) from bronze_sz.evt where _load_id is not null and _chunk_id is not null and _loaded_at is not null")[0][0] == 300)
check("historic marked done", q("select count(*) from bronze_ctl.table_state where historic_done_at is not null")[0][0] == 3)
check("no alerts after clean load", q("select count(*) from bronze_ctl.v_alerts")[0][0] == 0)
chunks_before = q("select count(*) from bronze_ctl.load_chunk")[0][0]
for tb in ("EVT", "MST", "SMALL"):
    run("historic", "--workers", "1", "--table", tb)
check("historic re-run does nothing (already complete)", q("select count(*) from bronze_ctl.load_chunk")[0][0] == chunks_before)
check("log events written", q("select count(*) from bronze_ctl.load_event where event='chunk_done'")[0][0] >= 5)

# ---- incremental: 20 new events, 5 changed + 3 new master rows, small table changed
F.NOW = D(2026, 11, 4, 13, 0)
for i in range(300, 320):
    F.rows["EVT"].append((i, D(2026, 11, 4, 11, 0) if i < 310 else D(2026, 11, 4, 12, 58), f"new{i}", decimal.Decimal("2.00")))
F.rows["MST"] = [(i, D(2026, 11, 4, 11, 30), f"changed{i}") if i < 5 else r for i, r in enumerate(F.rows["MST"])]
F.rows["MST"] += [(i, D(2026, 11, 4, 11, 40), f"cust{i}") for i in range(50, 53)]
F.rows["SMALL"].append(("k9", 9))
check("incr run ok", run("incr", "--workers", "1") == 0)
check("EVT = 310 (rows after window end excluded: only <= now-5min)", count("evt") == 310)
check("MST merged: 53 rows, 5 updated", count("mst") == 53 and q("select count(*) from bronze_sz.mst where name like 'changed%'")[0][0] == 5)
check("SMALL refreshed", count("small") == 6)
run("incr", "--workers", "1")
check("incr re-run is idempotent (no duplicates)", count("evt") == 310 and count("mst") == 53)
F.NOW = D(2026, 11, 4, 14, 0)
run("incr", "--workers", "1")
check("late rows picked up next run, still no duplicates", count("evt") == 320)
check("no alerts", q("select count(*) from bronze_ctl.v_alerts")[0][0] == 0)

# ---- transient failure: first attempt dies mid-stream, retry succeeds, nothing duplicated
F.NOW = D(2026, 11, 4, 16, 0)
F.rows["EVT"] += [(1000 + i, D(2026, 11, 4, 15, 0), "x", decimal.Decimal("1")) for i in range(120)]
F.fail = lambda table, pred, nth: table == "EVT" and nth == 1
check("incr with transient failure ends ok", run("incr", "--workers", "1", "--table", "EVT") == 0)
check("rolled back + retried: exact row count", count("evt") == 440)
check("failure and retry both logged",
      q("select count(*) from bronze_ctl.load_event where event='chunk_failed'")[0][0] >= 1
      and q("select max(attempts) from bronze_ctl.load_chunk")[0][0] == 2)

# ---- permanent failure -> alert + check exit code
F.NOW = D(2026, 11, 4, 18, 0)
F.rows["MST"].append((99, D(2026, 11, 4, 17, 0), "zz"))
F.fail = lambda table, pred, nth: table == "MST" and pred["kind"] == "window" and F.rows["MST"] and True
bad_rows_before = count("mst")
F.rows["MST"] += [(100 + i, D(2026, 11, 4, 17, 0), "zz") for i in range(120)]
check("incr with permanent failure returns 1", run("incr", "--workers", "1", "--table", "MST") == 1)
check("failed chunk left no partial data", count("mst") == bad_rows_before)
check("v_alerts shows critical failure", q("select count(*) from bronze_ctl.v_alerts where severity='critical' and src_table='MST'")[0][0] >= 1)
check("check command exits 2", run("check") == 2)
F.fail = lambda *a: False
F.NOW = D(2026, 11, 4, 19, 0)
check("next run recovers", run("incr", "--workers", "1", "--table", "MST") == 0 and count("mst") == bad_rows_before + 121)
check("alert clears after recovery", q("select count(*) from bronze_ctl.v_alerts where src_table='MST'")[0][0] == 0)

# ---- schema drift: Oracle gains a column
F.cols["MST"].append(col("NEW_COL", "VARCHAR2", nullable=True))
F.rows["MST"] = [r + ("v",) for r in F.rows["MST"]]
F.NOW = D(2026, 11, 4, 20, 0)
F.rows["MST"].append((500, D(2026, 11, 4, 19, 30), "drift", "val"))
run("incr", "--workers", "1", "--table", "MST")
check("new Oracle column added to bronze + warning logged",
      q("select count(*) from information_schema.columns where table_schema='bronze_sz' and table_name='mst' and column_name='new_col'")[0][0] == 1
      and q("select count(*) from bronze_ctl.load_event where event='schema_drift'")[0][0] >= 1)
check("schema history recorded column addition",
      q("select count(*) from bronze_ctl.schema_history where src_table='MST' and change_type='column_added'")[0][0] >= 1)

# ---- Data Quality Check: bad rows with NULL PK are quarantined and not loaded
F.NOW = D(2026, 11, 4, 21, 0)
F.rows["MST"].append((None, D(2026, 11, 4, 20, 30), "bad_null_pk", "val"))
mst_count_before = count("mst")
run("incr", "--workers", "1", "--table", "MST")
check("row with NULL PK is routed to quarantine table",
      q("select count(*) from bronze_ctl.load_quarantine where src_table='MST' and rejection_reason like '%NULL or blank%'")[0][0] >= 1)
check("bad row is not present in target bronze table",
      q("select count(*) from bronze_sz.mst where id is null")[0][0] == 0 and count("mst") == mst_count_before)

# ---- Duplicate Check: intra-chunk duplicate PKs tracked
F.NOW = D(2026, 11, 4, 22, 0)
F.rows["MST"].append((888, D(2026, 11, 4, 21, 30), "dup_first", "val"))
F.rows["MST"].append((888, D(2026, 11, 4, 21, 35), "dup_second", "val"))
run("incr", "--workers", "1", "--table", "MST")
check("duplicate check tracked duplicate keys",
      q("select count(*) from bronze_ctl.load_chunk where src_table='MST' and duplicates_skipped > 0")[0][0] >= 1)
check("target table has no duplicate PKs for id 888",
      q("select count(*) from bronze_sz.mst where id = 888")[0][0] == 1)

# ---- resumable historic: one chunk fails, rerun loads only that chunk
F.fail = lambda table, pred, nth: table == "BIG" and pred["a"] == D(2026, 10, 3) and True
run("historic", "--workers", "1", "--table", "BIG")
check("historic partially done: 4 of 5 days loaded, not marked complete",
      count("big") == 480 and q("select historic_done_at is null from bronze_ctl.table_state where src_table='BIG'")[0][0])
prog = q("select chunks_done, chunks_total from bronze_ctl.v_historic_progress where src_table='BIG'")[0]
check("progress view shows 4/5", prog == (4, 5))
F.fail = lambda *a: False
ran_before = q("select count(*) from bronze_ctl.load_chunk where src_table='BIG'")[0][0]
run("historic", "--workers", "1", "--table", "BIG")
check("resume loaded only the missing chunk", q("select count(*) from bronze_ctl.load_chunk where src_table='BIG'")[0][0] == ran_before + 1)
check("BIG complete 600 rows, marked done", count("big") == 600 and q("select historic_done_at is not null from bronze_ctl.table_state where src_table='BIG'")[0][0])

# ---- North Zone SAP tables copied from Oracle schema to PostgreSQL bronze_nz
run("historic", "--zone", "NZ")
check("NZ SAP table IBIB copied: 25 rows in bronze_nz.ibib", q("select count(*) from bronze_nz.ibib")[0][0] == 25)
check("NZ SAP table BUT000 copied: 30 rows in bronze_nz.but000", q("select count(*) from bronze_nz.but000")[0][0] == 30)
check("NZ SAP table CRMD_ORDERADM_H copied: 20 rows in bronze_nz.crmd_orderadm_h", q("select count(*) from bronze_nz.crmd_orderadm_h")[0][0] == 20)

# Verify sensitive column redaction in quarantine for NZ tables
F.NOW = D(2026, 11, 4, 23, 0)
F.rows["BUT000"].append((None, "BAD_FIRST", "BAD_LAST", "AACFI4502K", "GSTIN12345", D(2026, 11, 4, 22, 30)))
run("incr", "--zone", "NZ", "--table", "BUT000")
quar_but000 = q("select row_data from bronze_ctl.load_quarantine where zone = 'NZ' and src_table = 'BUT000'")[0][0]
check("Sensitive columns in NZ BUT000 redacted in quarantine",
      quar_but000.get("NAME_LST2") == "[REDACTED_SENSITIVE]" and quar_but000.get("ZZFIELD10") == "[REDACTED_SENSITIVE]")

# ---- stalled chunk detection
pg.execute("insert into bronze_ctl.load_chunk (zone, src_schema, src_table, window_kind, chunk_kind, status, last_heartbeat, started_at) "
           "values ('SZ','PORTAL','EVT','incr','all','running', now() - interval '30 minutes', now() - interval '31 minutes')")
check("stalled chunk raises alert", q("select count(*) from bronze_ctl.v_alerts where issue like '%no heartbeat%'")[0][0] >= 1)
check("status command runs", run("status") == 0)
check("dq command runs", run("dq") == 0)
check("schema command runs", run("schema") == 0)
check("verify command runs", run("verify") == 0)
print("ALL TESTS PASSED")
