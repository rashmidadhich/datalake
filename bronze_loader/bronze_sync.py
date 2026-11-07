#!/usr/bin/env python3
"""
Oracle -> Postgres BRONZE loader: copies Oracle tables as-is (no business mapping), at large volume,
with every step logged to Postgres (bronze_ctl.load_*) and a log file.
Includes:
- Data Quality Checks: PK and required columns null/blank validation with quarantine routing.
- Duplicate Checks: Intra-chunk duplicate detection and append/merge deduplication.
- Idempotency Checks: Chunk-level idempotency verification and safe re-run semantics.
- Schema Checks: Additive drift handling, schema history tracking, and incompatibility alerts.

  python bronze_sync.py init      --config config.toml            # create bronze_ctl schema + views
  python bronze_sync.py discover  --config config.toml            # read Oracle catalog -> table_config, create bronze tables
  python bronze_sync.py historic  --config config.toml [--zone SZ] [--table CDR] [--workers 8] [--force]
  python bronze_sync.py incr      --config config.toml [--zone SZ] [--table CDR] [--workers 8]   # hourly/daily
  python bronze_sync.py status    --config config.toml [--watch 10]
  python bronze_sync.py check     --config config.toml            # exit 2 if v_alerts has rows (use in cron/monitoring)
  python bronze_sync.py dq        --config config.toml            # display data quality and quarantine summary
  python bronze_sync.py schema    --config config.toml            # display schema evolution and drift history
  python bronze_sync.py verify    --config config.toml            # verify duplicate and idempotency integrity

Credentials from environment or .env: PG_DSN, and per zone ORA_<ZONE>_DSN / ORA_<ZONE>_USER / ORA_<ZONE>_PASSWORD.

Load unit = chunk. A chunk is one transaction in Postgres, so a failed chunk leaves nothing behind and can be re-run.
  append        immutable/event tables. A chunk first deletes its own range, then COPYs it (re-run safe, overlap safe).
  merge         mutable tables with a primary key. COPY to a temp table, then INSERT .. ON CONFLICT DO UPDATE.
  full_refresh  small tables without a usable column. TRUNCATE + COPY in one transaction (readers wait during the load).
"""
import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import decimal
import fnmatch
import io
import json
import logging
import logging.handlers
import multiprocessing as mp
import os
import re
import socket
import sys
import time
from urllib.parse import urlparse
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]
import traceback

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

LOG = logging.getLogger("bronze")
SETTLE = dt.timedelta(minutes=5)        # incremental windows end 5 minutes ago so in-flight commits are not missed
LOAD_COLS = [
    ("_load_id", "bigint"),
    ("_chunk_id", "bigint"),
    ("_loaded_at", "timestamptz DEFAULT now()"),
    ("_source", "text"),
    ("_src_table", "text"),
]


def load_env(path=None, override=False):
    """Load environment variables from a .env file into os.environ.

    Tries python-dotenv if available, otherwise uses a built-in parser
    so no external dependency is required.
    """
    if path is None:
        path = os.environ.get("ENV_FILE") or ".env"
    if not os.path.isabs(path):
        cwd_path = os.path.abspath(path)
        if os.path.exists(cwd_path):
            path = cwd_path
        else:
            script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
            if os.path.exists(script_path):
                path = script_path

    if not os.path.exists(path):
        return False

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                if val.startswith("="):
                    val = val[1:].strip()
                if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
                    val = val[1:-1]
                else:
                    if " #" in val:
                        val = val.split(" #", 1)[0].rstrip()
                    elif "\t#" in val:
                        val = val.split("\t#", 1)[0].rstrip()
                if val.startswith("="):
                    val = val[1:].strip()
                if override or key not in os.environ:
                    os.environ[key] = val
        return True
    except Exception as e:
        LOG.warning("Failed to parse .env file at %s: %s", path, e)
        return False


# Automatically load .env on import or execution
load_env()


# =============================================================== source (Oracle) abstraction
class OracleSource:
    """All Oracle access lives here. Tests replace SOURCE_FACTORY with an in-memory fake."""

    def __init__(self, zone, settings):
        import oracledb
        oracledb.defaults.fetch_lobs = False          # CLOB -> str, BLOB -> bytes
        oracledb.defaults.fetch_decimals = True       # NUMBER -> Decimal, no float rounding
        self.zone, self.arraysize = zone, int(settings.get("arraysize", 50000))
        z = zone.upper()
        u, p, d = f"ORA_{z}_USER", f"ORA_{z}_PASSWORD", f"ORA_{z}_DSN"
        missing = [k for k in (u, p, d) if k not in os.environ]
        if missing:
            raise KeyError(
                f"Missing Oracle credentials for zone {zone}: {', '.join(missing)}. "
                f"Please define them in your .env file or environment."
            )
        self.con = oracledb.connect(user=os.environ[u], password=os.environ[p], dsn=os.environ[d])

    def _q(self, text, binds=None):
        cur = self.con.cursor()
        cur.execute(text, binds or {})
        return cur

    def tables(self, schema=None):
        if not schema or schema == "*":
            cur = self._q(
                "SELECT table_name, num_rows, owner FROM all_tables "
                "WHERE temporary = 'N' AND nested = 'NO' "
                "AND owner NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'CTXSYS', 'XDB', 'APEX_030200', "
                "'WMSYS', 'LBACSYS', 'DVSYS', 'DVF', 'GSMADMIN_INTERNAL', 'OLAPSYS', 'ORDDATA', 'ORDPLUGINS', 'ORDSYS') "
                "ORDER BY owner, table_name"
            )
            return cur.fetchall()
        cur = self._q(
            "SELECT table_name, num_rows FROM all_tables "
            "WHERE UPPER(owner) = UPPER(:o) AND temporary = 'N' AND nested = 'NO' "
            "ORDER BY table_name", {"o": schema}
        )
        return cur.fetchall()

    def schemas(self):
        cur = self._q(
            "SELECT DISTINCT owner FROM all_tables "
            "WHERE temporary = 'N' AND nested = 'NO' "
            "AND owner NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'CTXSYS', 'XDB', 'APEX_030200', "
            "'WMSYS', 'LBACSYS', 'DVSYS', 'DVF', 'GSMADMIN_INTERNAL', 'OLAPSYS', 'ORDDATA', 'ORDPLUGINS', 'ORDSYS') "
            "ORDER BY owner"
        )
        return [r[0] for r in cur.fetchall()]

    def current_schema(self):
        return self._q("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual").fetchone()[0]

    def all_accessible_tables(self):
        """Return all non-system tables accessible across all schemas directly from Oracle."""
        cur = self._q(
            "SELECT owner, table_name, num_rows FROM all_tables "
            "WHERE temporary = 'N' AND nested = 'NO' "
            "AND owner NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'CTXSYS', 'XDB', 'APEX_030200', "
            "'WMSYS', 'LBACSYS', 'DVSYS', 'DVF', 'GSMADMIN_INTERNAL', 'OLAPSYS', 'ORDDATA', 'ORDPLUGINS', 'ORDSYS') "
            "ORDER BY owner, table_name"
        )
        return [dict(owner=r[0], table_name=r[1], num_rows=r[2]) for r in cur.fetchall()]

    def columns(self, schema, table):
        cur = self._q(
            "SELECT column_name, data_type, data_precision, data_scale, nullable FROM all_tab_columns "
            "WHERE UPPER(owner) = UPPER(:o) AND UPPER(table_name) = UPPER(:t) ORDER BY column_id",
            {"o": schema, "t": table}
        )
        return [dict(name=r[0], type=r[1], precision=r[2], scale=r[3], nullable=(r[4] == "Y")) for r in cur.fetchall()]

    def pk_cols(self, schema, table):
        cur = self._q(
            "SELECT cc.column_name FROM all_constraints c JOIN all_cons_columns cc ON cc.owner = c.owner "
            "AND cc.constraint_name = c.constraint_name WHERE UPPER(c.owner) = UPPER(:o) AND UPPER(c.table_name) = UPPER(:t) "
            "AND c.constraint_type = 'P' ORDER BY cc.position",
            {"o": schema, "t": table}
        )
        return [r[0] for r in cur.fetchall()]

    def now(self):
        return self._q("SELECT CAST(SYSTIMESTAMP AS TIMESTAMP) FROM dual").fetchone()[0]

    def bounds(self, schema, table, col):
        return self._q(f'SELECT MIN("{col}"), MAX("{col}") FROM "{schema}"."{table}"').fetchone()

    @staticmethod
    def _where(pred):
        k, c = pred["kind"], pred.get("col")
        if k == "range":
            return f'WHERE "{c}" >= :a AND "{c}" < :b', {"a": pred["a"], "b": pred["b"]}
        if k == "window":
            return f'WHERE "{c}" > :a AND "{c}" <= :b', {"a": pred["a"], "b": pred["b"]}
        if k == "null":
            return f'WHERE "{c}" IS NULL', {}
        return "", {}

    def count(self, schema, table, pred):
        w, b = self._where(pred)
        return self._q(f'SELECT COUNT(*) FROM "{schema}"."{table}" {w}', b).fetchone()[0]

    def stream(self, schema, table, cols, pred, hint=None):
        w, b = self._where(pred)
        select = ", ".join(f't."{c}"' for c in cols)
        cur = self.con.cursor()
        cur.arraysize = self.arraysize
        cur.prefetchrows = self.arraysize + 1
        cur.execute(f'SELECT {hint or ""} {select} FROM "{schema}"."{table}" t {w}', b)
        while True:
            rows = cur.fetchmany()
            if not rows:
                break
            yield rows
        cur.close()

    def close(self):
        try:
            self.con.close()
        except Exception:
            pass


# =============================================================== multi-mode sources (Parquet, CSV, JDBC, Oracle)
class ParquetSource:
    """Ingests data from Parquet files or directories."""

    def __init__(self, zone, settings=None, config=None):
        self.zone = zone
        self.settings = settings or {}
        self.arraysize = int(self.settings.get("arraysize", 50000))
        self.config = config or {}
        self.base_path = self.config.get("path") or os.environ.get(f"PARQUET_{zone.upper()}_PATH", "data/parquet")
        import pyarrow.parquet as pq
        import pyarrow as pa
        self.pq = pq
        self.pa = pa

    def _resolve_table_path(self, schema, table):
        if os.path.isfile(self.base_path):
            return self.base_path
        p = os.path.join(self.base_path, table)
        if os.path.isfile(p + ".parquet"):
            return p + ".parquet"
        if os.path.isfile(p):
            return p
        if os.path.isdir(p):
            return p
        if os.path.isfile(table):
            return table
        return p + ".parquet"

    def tables(self, schema=None):
        res = []
        if os.path.isfile(self.base_path):
            name = os.path.splitext(os.path.basename(self.base_path))[0]
            try:
                nrows = self.pq.ParquetFile(self.base_path).metadata.num_rows
            except Exception:
                nrows = 0
            return [(name, nrows)]
        if os.path.isdir(self.base_path):
            for entry in os.listdir(self.base_path):
                full = os.path.join(self.base_path, entry)
                if entry.endswith(".parquet") and os.path.isfile(full):
                    name = entry[:-8]
                    try:
                        nrows = self.pq.ParquetFile(full).metadata.num_rows
                    except Exception:
                        nrows = 0
                    res.append((name, nrows))
                elif os.path.isdir(full):
                    name = entry
                    files = [os.path.join(full, f) for f in os.listdir(full) if f.endswith(".parquet")]
                    nrows = 0
                    for f in files:
                        try:
                            nrows += self.pq.ParquetFile(f).metadata.num_rows
                        except Exception:
                            pass
                    res.append((name, nrows))
        return res

    def schemas(self):
        return ["PARQUET"]

    def current_schema(self):
        return "PARQUET"

    def all_accessible_tables(self):
        return [{"owner": "PARQUET", "table_name": t, "num_rows": n} for t, n in self.tables()]

    def columns(self, schema, table):
        tpath = self._resolve_table_path(schema, table)
        if os.path.isdir(tpath):
            files = [os.path.join(tpath, f) for f in os.listdir(tpath) if f.endswith(".parquet")]
            if not files:
                return []
            tpath = files[0]
        if not os.path.exists(tpath):
            return []
        pf = self.pq.ParquetFile(tpath)
        arrow_schema = pf.schema_arrow
        cols = []
        for field in arrow_schema:
            ty_str = str(field.type).lower()
            pg_ty = "TEXT"
            if "int8" in ty_str or "int16" in ty_str or "int32" in ty_str:
                pg_ty = "INTEGER"
            elif "int64" in ty_str:
                pg_ty = "BIGINT"
            elif "float" in ty_str or "double" in ty_str:
                pg_ty = "DOUBLE PRECISION"
            elif "timestamp" in ty_str:
                pg_ty = "TIMESTAMPTZ" if ("tz" in ty_str or "zone" in ty_str) else "TIMESTAMP"
            elif "date" in ty_str:
                pg_ty = "DATE"
            elif "bool" in ty_str:
                pg_ty = "BOOLEAN"
            elif "decimal" in ty_str:
                pg_ty = "NUMERIC"
            elif "binary" in ty_str:
                pg_ty = "BYTEA"
            cols.append({
                "name": field.name,
                "type": pg_ty,
                "precision": None,
                "scale": None,
                "nullable": field.nullable
            })
        return cols

    def pk_cols(self, schema, table):
        cols = [c["name"].upper() for c in self.columns(schema, table)]
        prefix = table.upper().split("_")[0] + "_ID"
        for cand in ["ID", "ROW_ID", "GUID", "UUID", f"{table.upper()}_ID", prefix]:
            if cand in cols:
                return [cand]
        id_cols = [c for c in cols if c.endswith("_ID") or c.endswith("_PK")]
        if id_cols:
            return [id_cols[0]]
        return []

    def now(self):
        return dt.datetime.now()

    def bounds(self, schema, table, col):
        tpath = self._resolve_table_path(schema, table)
        try:
            pf = self.pq.ParquetFile(tpath)
            tbl = pf.read(columns=[col])
            arr = tbl.column(col)
            import pyarrow.compute as pc
            min_val = pc.min(arr).as_py()
            max_val = pc.max(arr).as_py()
            return (min_val, max_val)
        except Exception:
            return (None, None)

    def count(self, schema, table, pred):
        tpath = self._resolve_table_path(schema, table)
        try:
            pf = self.pq.ParquetFile(tpath)
            total = pf.metadata.num_rows
            k, c = pred.get("kind"), pred.get("col")
            if not k or k == "all" or not c:
                return total
            tbl = pf.read(columns=[c])
            arr = tbl.column(c)
            import pyarrow.compute as pc
            mask = None
            if k == "range":
                mask = pc.and_(pc.greater_equal(arr, pred["a"]), pc.less(arr, pred["b"]))
            elif k == "window":
                mask = pc.and_(pc.greater(arr, pred["a"]), pc.less_equal(arr, pred["b"]))
            elif k == "null":
                mask = pc.is_null(arr)
            if mask is not None:
                return pc.sum(pc.cast(mask, self.pa.int64())).as_py() or 0
            return total
        except Exception:
            return 0

    def stream(self, schema, table, cols, pred, hint=None):
        tpath = self._resolve_table_path(schema, table)
        files = [tpath]
        if os.path.isdir(tpath):
            files = [os.path.join(tpath, f) for f in os.listdir(tpath) if f.endswith(".parquet")]

        k, pred_col = pred.get("kind"), pred.get("col")
        req_cols = list(cols)
        if pred_col and pred_col not in req_cols:
            req_cols.append(pred_col)

        for fpath in files:
            pf = self.pq.ParquetFile(fpath)
            for batch in pf.iter_batches(batch_size=self.arraysize, columns=req_cols):
                df = batch.to_pylist()
                rows = []
                for r in df:
                    if k == "range" and pred_col:
                        val = r.get(pred_col)
                        if val is None or val < pred["a"] or val >= pred["b"]:
                            continue
                    elif k == "window" and pred_col:
                        val = r.get(pred_col)
                        if val is None or val <= pred["a"] or val > pred["b"]:
                            continue
                    elif k == "null" and pred_col:
                        if r.get(pred_col) is not None:
                            continue
                    row_tuple = tuple(r.get(c) for c in cols)
                    rows.append(row_tuple)
                if rows:
                    yield rows

    def close(self):
        pass


class CSVSource:
    """Ingests data from CSV / TSV / delimited text files (with optional .gz compression)."""

    def __init__(self, zone, settings=None, config=None):
        self.zone = zone
        self.settings = settings or {}
        self.arraysize = int(self.settings.get("arraysize", 50000))
        self.config = config or {}
        self.base_path = self.config.get("path") or os.environ.get(f"CSV_{zone.upper()}_PATH", "data/csv")
        self.delimiter = self.config.get("delimiter", ",") or ","
        self.quotechar = self.config.get("quotechar", '"') or '"'
        self.encoding = self.config.get("encoding", "utf-8") or "utf-8"
        self.has_header = bool(self.config.get("has_header", True))

    def _resolve_table_path(self, schema, table):
        if os.path.isfile(self.base_path):
            return self.base_path
        p = os.path.join(self.base_path, table)
        for ext in [".csv", ".tsv", ".csv.gz", ".txt"]:
            if os.path.isfile(p + ext):
                return p + ext
        if os.path.isfile(p):
            return p
        if os.path.isfile(table):
            return table
        return p + ".csv"

    def _open_file(self, filepath):
        if filepath.endswith(".gz"):
            import gzip
            return io.TextIOWrapper(gzip.open(filepath, "rb"), encoding=self.encoding, errors="replace")
        return open(filepath, "r", encoding=self.encoding, errors="replace")

    def tables(self, schema=None):
        res = []
        if os.path.isfile(self.base_path):
            name = os.path.splitext(os.path.basename(self.base_path))[0]
            res.append((name, self._estimate_rows(self.base_path)))
            return res
        if os.path.isdir(self.base_path):
            for entry in os.listdir(self.base_path):
                full = os.path.join(self.base_path, entry)
                if os.path.isfile(full) and any(entry.endswith(ext) for ext in [".csv", ".tsv", ".csv.gz", ".txt"]):
                    name = os.path.splitext(entry)[0]
                    if name.endswith(".csv"):
                        name = os.path.splitext(name)[0]
                    res.append((name, self._estimate_rows(full)))
        return res

    def _estimate_rows(self, filepath):
        try:
            count = 0
            with self._open_file(filepath) as f:
                for _ in f:
                    count += 1
            return max(0, count - (1 if self.has_header else 0))
        except Exception:
            return 0

    def schemas(self):
        return ["CSV"]

    def current_schema(self):
        return "CSV"

    def all_accessible_tables(self):
        return [{"owner": "CSV", "table_name": t, "num_rows": n} for t, n in self.tables()]

    def columns(self, schema, table):
        tpath = self._resolve_table_path(schema, table)
        if not os.path.exists(tpath):
            return []
        with self._open_file(tpath) as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            header = next(reader, None)
            if not header:
                return []
            header = [h.strip().lstrip("\ufeff") for h in header]
            samples = []
            for _ in range(200):
                row = next(reader, None)
                if row is None:
                    break
                samples.append(row)

        cols = []
        for col_idx, col_name in enumerate(header):
            inferred_type = "TEXT"
            vals = [s[col_idx].strip() for s in samples if col_idx < len(s) and s[col_idx] and s[col_idx].strip()]
            if vals:
                is_int = all(re.match(r"^-?\d+$", v) for v in vals)
                is_float = all(re.match(r"^-?\d+(\.\d+)?$", v) for v in vals)
                is_dt = all(re.match(r"^\d{4}-\d{2}-\d{2}", v) for v in vals)
                is_bool = all(v.lower() in ("true", "false", "t", "f", "1", "0", "y", "n") for v in vals)
                if is_int:
                    inferred_type = "BIGINT"
                elif is_float:
                    inferred_type = "NUMERIC"
                elif is_dt:
                    inferred_type = "TIMESTAMP"
                elif is_bool:
                    inferred_type = "BOOLEAN"

            cols.append({
                "name": col_name,
                "type": inferred_type,
                "precision": None,
                "scale": None,
                "nullable": True
            })
        return cols

    def pk_cols(self, schema, table):
        cols = [c["name"].upper() for c in self.columns(schema, table)]
        prefix = table.upper().split("_")[0] + "_ID"
        for cand in ["ID", "ROW_ID", "GUID", "UUID", f"{table.upper()}_ID", prefix]:
            if cand in cols:
                return [cand]
        id_cols = [c for c in cols if c.endswith("_ID") or c.endswith("_PK")]
        if id_cols:
            return [id_cols[0]]
        return []

    def now(self):
        return dt.datetime.now()

    def bounds(self, schema, table, col):
        tpath = self._resolve_table_path(schema, table)
        min_val, max_val = None, None
        try:
            with self._open_file(tpath) as f:
                reader = csv.DictReader(f, delimiter=self.delimiter, quotechar=self.quotechar)
                for r in reader:
                    v = r.get(col)
                    if v:
                        if min_val is None or v < min_val:
                            min_val = v
                        if max_val is None or v > max_val:
                            max_val = v
            return (min_val, max_val)
        except Exception:
            return (None, None)

    def count(self, schema, table, pred):
        return self._estimate_rows(self._resolve_table_path(schema, table))

    def stream(self, schema, table, cols, pred, hint=None):
        tpath = self._resolve_table_path(schema, table)
        k, pred_col = pred.get("kind"), pred.get("col")
        col_types = {c["name"]: c["type"] for c in self.columns(schema, table)}
        with self._open_file(tpath) as f:
            reader = csv.DictReader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            batch = []
            for r in reader:
                if k == "range" and pred_col:
                    val = r.get(pred_col)
                    if val is None or str(val) < str(pred["a"]) or str(val) >= str(pred["b"]):
                        continue
                elif k == "window" and pred_col:
                    val = r.get(pred_col)
                    if val is None or str(val) <= str(pred["a"]) or str(val) > str(pred["b"]):
                        continue
                elif k == "null" and pred_col:
                    if r.get(pred_col) is not None and r.get(pred_col).strip() != "":
                        continue

                row_vals = []
                for c in cols:
                    v = r.get(c)
                    if v is not None:
                        v = v.strip()
                        ty = col_types.get(c, "TEXT")
                        if v == "" and ty != "TEXT":
                            v = None
                    row_vals.append(v)
                batch.append(tuple(row_vals))
                if len(batch) >= self.arraysize:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def close(self):
        pass


class JDBCSource:
    """Ingests data via JDBC URL or standard SQL connection (PostgreSQL, SQLite, Oracle, MySQL, etc.)."""

    def __init__(self, zone, settings=None, config=None):
        self.zone = zone
        self.settings = settings or {}
        self.arraysize = int(self.settings.get("arraysize", 50000))
        self.config = config or {}
        self.url = self.config.get("jdbc_url") or self.config.get("url") or os.environ.get(f"JDBC_{zone.upper()}_URL", "")
        self.con = self._connect()

    def _connect(self):
        raw = self.url
        if raw.startswith("jdbc:"):
            raw = raw[5:]

        if raw.startswith("postgresql://") or raw.startswith("postgres://"):
            import psycopg
            return psycopg.connect(raw)
        elif raw.startswith("sqlite://") or raw.startswith("sqlite:"):
            import sqlite3
            path = raw.replace("sqlite://", "").replace("sqlite:", "")
            return sqlite3.connect(path)
        elif raw.startswith("oracle:"):
            import oracledb
            dsn = raw.split("@")[-1] if "@" in raw else raw
            u = self.config.get("user") or os.environ.get(f"JDBC_{self.zone.upper()}_USER", "")
            p = self.config.get("password") or os.environ.get(f"JDBC_{self.zone.upper()}_PASSWORD", "")
            return oracledb.connect(user=u, password=p, dsn=dsn)
        elif raw.startswith("mysql:"):
            try:
                import pymysql
                parsed = urlparse(raw if "://" in raw else "mysql://" + raw)
                return pymysql.connect(host=parsed.hostname, port=parsed.port or 3306, user=parsed.username,
                                       password=parsed.password, database=parsed.path.lstrip("/"))
            except ImportError:
                raise RuntimeError("pymysql package is required for MySQL JDBC ingestion. Install via: pip install pymysql")
        else:
            if os.path.exists(raw):
                import sqlite3
                return sqlite3.connect(raw)
            import psycopg
            return psycopg.connect(raw)

    def tables(self, schema=None):
        cur = self.con.cursor()
        try:
            if schema:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'", (schema,))
            else:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'")
            rows = cur.fetchall()
            return [(r[0], 0) for r in rows]
        except Exception:
            try:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                return [(r[0], 0) for r in cur.fetchall()]
            except Exception:
                return []

    def schemas(self):
        cur = self.con.cursor()
        try:
            cur.execute("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
            return [r[0] for r in cur.fetchall()]
        except Exception:
            return ["PUBLIC"]

    def current_schema(self):
        return "PUBLIC"

    def all_accessible_tables(self):
        return [{"owner": "JDBC", "table_name": t, "num_rows": n} for t, n in self.tables()]

    def columns(self, schema, table):
        cur = self.con.cursor()
        try:
            cur.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table,))
            rows = cur.fetchall()
            return [{
                "name": r[0],
                "type": r[1].upper(),
                "precision": None,
                "scale": None,
                "nullable": (r[2] == "YES")
            } for r in rows]
        except Exception:
            try:
                cur.execute(f"PRAGMA table_info('{table}')")
                rows = cur.fetchall()
                return [{
                    "name": r[1],
                    "type": (r[2] or "TEXT").upper(),
                    "precision": None,
                    "scale": None,
                    "nullable": not bool(r[3])
                } for r in rows]
            except Exception:
                return []

    def pk_cols(self, schema, table):
        cur = self.con.cursor()
        try:
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """, (table,))
            return [r[0] for r in cur.fetchall()]
        except Exception:
            try:
                cur.execute(f"PRAGMA table_info('{table}')")
                return [r[1] for r in cur.fetchall() if r[5] > 0]
            except Exception:
                return []

    def now(self):
        return dt.datetime.now()

    def bounds(self, schema, table, col):
        cur = self.con.cursor()
        cur.execute(f'SELECT MIN("{col}"), MAX("{col}") FROM "{table}"')
        return cur.fetchone()

    def count(self, schema, table, pred):
        cur = self.con.cursor()
        w, p = "", []
        k, c = pred.get("kind"), pred.get("col")
        if k == "range":
            w = f'WHERE "{c}" >= %s AND "{c}" < %s'
            p = [pred["a"], pred["b"]]
        elif k == "window":
            w = f'WHERE "{c}" > %s AND "{c}" <= %s'
            p = [pred["a"], pred["b"]]
        elif k == "null":
            w = f'WHERE "{c}" IS NULL'
        query = f'SELECT COUNT(*) FROM "{table}" {w}'
        if "%s" in query and "sqlite" in str(type(self.con)).lower():
            query = query.replace("%s", "?")
        cur.execute(query, tuple(p))
        return cur.fetchone()[0]

    def stream(self, schema, table, cols, pred, hint=None):
        cur = self.con.cursor()
        col_str = ", ".join(f'"{c}"' for c in cols)
        w, p = "", []
        k, c = pred.get("kind"), pred.get("col")
        if k == "range":
            w = f'WHERE "{c}" >= %s AND "{c}" < %s'
            p = [pred["a"], pred["b"]]
        elif k == "window":
            w = f'WHERE "{c}" > %s AND "{c}" <= %s'
            p = [pred["a"], pred["b"]]
        elif k == "null":
            w = f'WHERE "{c}" IS NULL'
        query = f'SELECT {col_str} FROM "{table}" {w}'
        if "%s" in query and "sqlite" in str(type(self.con)).lower():
            query = query.replace("%s", "?")
        cur.execute(query, tuple(p))
        while True:
            rows = cur.fetchmany(self.arraysize)
            if not rows:
                break
            yield rows
        cur.close()

    def close(self):
        try:
            self.con.close()
        except Exception:
            pass


def create_source(zone, settings=None, ingest_type=None, config=None):
    """Factory creating the appropriate source connector: Oracle, Parquet, CSV, or JDBC."""
    itype = (ingest_type or (config.get("type") if config else None) or "oracle").lower()
    if itype == "parquet":
        return ParquetSource(zone, settings, config=config)
    elif itype in ("csv", "tsv"):
        return CSVSource(zone, settings, config=config)
    elif itype in ("jdbc", "sql"):
        return JDBCSource(zone, settings, config=config)
    else:
        return OracleSource(zone, settings)


SOURCE_FACTORY = lambda zone, settings=None, ingest_type=None, config=None: create_source(zone, settings, ingest_type, config)  # noqa: E731


def call_source_factory(zone, settings=None, ingest_type=None, config=None):
    """Safely call SOURCE_FACTORY supporting both 4-arg multi-mode factory and legacy 2-arg test fakes."""
    try:
        return SOURCE_FACTORY(zone, settings, ingest_type=ingest_type, config=config)
    except TypeError:
        return SOURCE_FACTORY(zone, settings)


# =============================================================== helpers
def load_cfg(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def pgname(n):
    s = re.sub(r"[^a-z0-9_]", "_", n.lower())
    return ("c_" + s) if re.match(r"\d", s) else s


def pg_type(c):
    t = c["type"].upper()
    p, s = c.get("precision"), c.get("scale")
    if t in ("INTEGER", "INT", "INT4", "INT8", "BIGINT", "SMALLINT"):
        return t.lower()
    if t in ("BOOLEAN", "BOOL"):
        return "boolean"
    if t == "NUMBER":
        if s == 0 and p:
            return "integer" if p <= 9 else "bigint" if p <= 18 else "numeric"
        return "numeric"
    if t in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "DOUBLE PRECISION", "REAL"):
        return "double precision"
    if t == "DATE":
        return "timestamp"
    if t.startswith("TIMESTAMP"):
        return "timestamptz" if ("WITH TIME ZONE" in t or "TZ" in t) else "timestamp"
    if t in ("BLOB", "RAW", "LONG RAW", "BYTEA"):
        return "bytea"
    if t in ("NUMERIC", "DECIMAL"):
        return "numeric"
    return "text"            # VARCHAR2, CHAR, NVARCHAR2, CLOB, NCLOB, LONG, TEXT, anything unknown


def col_map(cols):
    """oracle columns -> [(oracle_name, pg_name, pg_type)], names made unique"""
    seen, out = {}, []
    for c in cols:
        n = pgname(c["name"])
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append((c["name"], n, pg_type(c)))
    return out


def ident(schema, table):
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


def connect(autocommit=True):
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise KeyError("Missing PG_DSN environment variable. Please define it in your .env file or environment.")
    return psycopg.connect(dsn, autocommit=autocommit)


class JsonFormatter(logging.Formatter):
    def format(self, r):
        d = {"ts": self.formatTime(r, "%Y-%m-%dT%H:%M:%S"), "level": r.levelname, "msg": r.getMessage()}
        d.update(getattr(r, "ctx", {}) or {})
        return json.dumps(d, default=str)


def setup_logging(cfg):
    path = cfg.get("settings", {}).get("log_file", "logs/bronze_sync.log")
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not LOG.handlers:
        LOG.setLevel(logging.INFO)
        fh = logging.handlers.RotatingFileHandler(path, maxBytes=100_000_000, backupCount=10, encoding="utf-8")
        fh.setFormatter(JsonFormatter())
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOG.addHandler(fh)
        LOG.addHandler(sh)


def event(ctl, level, name, msg, run_id=None, chunk_id=None, **details):
    """one line to the log file AND to bronze_ctl.load_event"""
    LOG.log(getattr(logging, level), msg, extra={"ctx": dict(event=name, run_id=run_id, chunk_id=chunk_id, **details)})
    try:
        ctl.execute("INSERT INTO bronze_ctl.load_event (run_id, chunk_id, level, event, message, details) "
                    "VALUES (%s,%s,%s,%s,%s,%s)", (run_id, chunk_id, level, name, msg, Jsonb(details)))
    except Exception as e:
        LOG.warning("Could not insert load_event into database: %s", e)


def rowdict(cur):
    names = [d.name for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


# =============================================================== bronze tables & schema checks
def ensure_table(pg, t, cmap, ctl=None, run_id=None, chunk_id=None):
    """create the bronze table, or add columns Oracle has gained (schema drift).
    Tracks schema history and validates schema evolution.
    Returns list of added columns.
    """
    full = f"{t['tgt_schema']}.{t['tgt_table']}"
    added = []
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (full,))
        pg.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(t["tgt_schema"])))
        exists = pg.execute("SELECT to_regclass(%s)", (full,)).fetchone()[0]
        if not exists:
            pcol = pgname(t["chunk_col"]) if t["partitioned"] and t["chunk_col"] and t["mode"] == "append" else None
            cols = [sql.SQL("{} {}").format(sql.Identifier(pn), sql.SQL(ty)) for _, pn, ty in cmap]
            cols += [sql.SQL("{} {}").format(sql.Identifier(n), sql.SQL(ty)) for n, ty in LOAD_COLS]
            q = sql.SQL("CREATE TABLE {} ({})").format(ident(t["tgt_schema"], t["tgt_table"]), sql.SQL(", ").join(cols))
            if pcol:
                q += sql.SQL(" PARTITION BY RANGE ({})").format(sql.Identifier(pcol))
            pg.execute(q)
            if pcol:
                pg.execute(sql.SQL("CREATE TABLE {} PARTITION OF {} DEFAULT").format(
                    ident(t["tgt_schema"], t["tgt_table"] + "_default"), ident(t["tgt_schema"], t["tgt_table"])))
            if t["mode"] == "merge" and t["pk_cols"]:
                pg.execute(sql.SQL("CREATE UNIQUE INDEX {} ON {} ({})").format(
                    sql.Identifier(f"ux_{t['tgt_table']}_pk"[:63]), ident(t["tgt_schema"], t["tgt_table"]),
                    sql.SQL(", ").join(sql.Identifier(pgname(c)) for c in t["pk_cols"])))
            wm = t["watermark_col"] or t["chunk_col"]
            if t["mode"] == "append" and wm:
                pg.execute(sql.SQL("CREATE INDEX {} ON {} ({})").format(
                    sql.Identifier(f"ix_{t['tgt_table']}_wm"[:63]), ident(t["tgt_schema"], t["tgt_table"]),
                    sql.Identifier(pgname(wm))))
            # Record initial schema snapshot in bronze_ctl.schema_history
            if ctl is not None:
                try:
                    for on, pn, ty in cmap:
                        ctl.execute(
                            "INSERT INTO bronze_ctl.schema_history (zone, src_schema, src_table, column_name, oracle_type, pg_type, change_type, run_id, chunk_id) "
                            "VALUES (%s,%s,%s,%s,%s,%s,'initial',%s,%s)",
                            (t["zone"], t["src_schema"], t["src_table"], on, ty, ty, run_id, chunk_id))
                except Exception as e:
                    LOG.debug("Could not record initial schema history: %s", e)
        else:
            have_rows = pg.execute(
                "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
                (t["tgt_schema"], t["tgt_table"])).fetchall()
            have = {r[0]: (r[1], r[2]) for r in have_rows}

            for on, pn, ty in cmap:
                if pn not in have:
                    pg.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                        ident(t["tgt_schema"], t["tgt_table"]), sql.Identifier(pn), sql.SQL(ty)))
                    added.append(pn)
                    if ctl is not None:
                        try:
                            ctl.execute(
                                "INSERT INTO bronze_ctl.schema_history (zone, src_schema, src_table, column_name, oracle_type, pg_type, change_type, run_id, chunk_id) "
                                "VALUES (%s,%s,%s,%s,%s,%s,'column_added',%s,%s)",
                                (t["zone"], t["src_schema"], t["src_table"], on, ty, ty, run_id, chunk_id))
                        except Exception as e:
                            LOG.debug("Could not record column addition in schema_history: %s", e)

            # Check for missing source columns
            load_col_names = {n for n, _ in LOAD_COLS}
            cmap_pns = {pn for _, pn, _ in cmap}
            for col_name in have:
                if col_name not in cmap_pns and col_name not in load_col_names:
                    if ctl is not None:
                        try:
                            event(ctl, "WARNING", "schema_missing_column",
                                  f"Column '{col_name}' exists in Bronze table but is absent in source Oracle schema",
                                  run_id, chunk_id, missing_column=col_name)
                            ctl.execute(
                                "INSERT INTO bronze_ctl.schema_history (zone, src_schema, src_table, column_name, oracle_type, pg_type, change_type, run_id, chunk_id) "
                                "VALUES (%s,%s,%s,%s,'UNKNOWN','UNKNOWN','column_missing',%s,%s)",
                                (t["zone"], t["src_schema"], t["src_table"], col_name, run_id, chunk_id))
                        except Exception as e:
                            LOG.debug("Could not record missing column in schema_history: %s", e)

            # Ensure all lineage metadata columns exist on pre-existing tables
            for ln, lty in LOAD_COLS:
                if ln not in have:
                    pg.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                        ident(t["tgt_schema"], t["tgt_table"]), sql.Identifier(ln), sql.SQL(lty)))

        # Validate against registered column mappings (e.g. North Zone -> CFA360)
        try:
            import mapping
            mapping.validate_schema_mapping(t["zone"], t["src_table"], [on for on, _, _ in cmap], ctl=ctl, run_id=run_id, chunk_id=chunk_id)
        except Exception as e:
            LOG.debug("Schema mapping validation skipped or failed: %s", e)
    return added


def ensure_partitions(pg, t, a, b):
    """monthly partitions covering [a, b] for partitioned append tables"""
    if not (t["partitioned"] and t["chunk_col"] and t["mode"] == "append") or a is None:
        return
    m = dt.datetime(a.year, a.month, 1)
    while m <= b:
        n = dt.datetime(m.year + (m.month == 12), m.month % 12 + 1, 1)
        child = f"{t['tgt_table']}_{m:%Y%m}"
        with pg.transaction():
            pg.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (t["tgt_schema"] + "." + t["tgt_table"],))
            if not pg.execute("SELECT to_regclass(%s)", (f"{t['tgt_schema']}.{child}",)).fetchone()[0]:
                pg.execute(sql.SQL("CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})").format(
                    ident(t["tgt_schema"], child), ident(t["tgt_schema"], t["tgt_table"]),
                    sql.Literal(m), sql.Literal(n)))          # DDL cannot take bind parameters
        m = n


# =============================================================== chunk execution & checks
def pg_predicate(pred):
    k = pred["kind"]
    if k == "range":
        return sql.SQL("{c} >= %s AND {c} < %s").format(c=sql.Identifier(pgname(pred["col"]))), [pred["a"], pred["b"]]
    if k == "window":
        return sql.SQL("{c} > %s AND {c} <= %s").format(c=sql.Identifier(pgname(pred["col"]))), [pred["a"], pred["b"]]
    if k == "null":
        return sql.SQL("{c} IS NULL").format(c=sql.Identifier(pgname(pred["col"]))), []
    return None, []


def run_chunk(chunk_id, cfg, source=None, force=False):
    """load one chunk (with retries and idempotency checks). Returns 'done' or 'failed'."""
    settings = cfg.get("settings", {})
    retries = int(settings.get("retries", 2))
    ctl = connect()
    try:
        ch = rowdict(ctl.execute("SELECT * FROM bronze_ctl.load_chunk WHERE chunk_id=%s", (chunk_id,)))[0]
        t = rowdict(ctl.execute("SELECT * FROM bronze_ctl.table_config WHERE zone=%s AND src_schema=%s AND src_table=%s",
                                (ch["zone"], ch["src_schema"], ch["src_table"])))[0]

        # Idempotency check: if an identical chunk was already completed, skip re-executing
        check_idempotency = bool(t.get("check_idempotency", True) if "check_idempotency" in t else True)
        if check_idempotency and not force:
            prior = ctl.execute(
                "SELECT chunk_id, rows_loaded FROM bronze_ctl.load_chunk "
                "WHERE zone=%s AND src_schema=%s AND src_table=%s AND window_kind=%s AND chunk_kind=%s "
                "AND chunk_from IS NOT DISTINCT FROM %s AND chunk_to IS NOT DISTINCT FROM %s "
                "AND status='done' AND chunk_id <> %s ORDER BY chunk_id DESC LIMIT 1",
                (ch["zone"], ch["src_schema"], ch["src_table"], ch["window_kind"], ch["chunk_kind"],
                 ch["chunk_from"], ch["chunk_to"], chunk_id)
            ).fetchone()
            if prior and ch["window_kind"] != "incr":
                ctl.execute(
                    "UPDATE bronze_ctl.load_chunk SET status='done', idempotent_skip=true, rows_fetched=0, "
                    "rows_loaded=%s, started_at=now(), ended_at=now(), last_heartbeat=now() WHERE chunk_id=%s",
                    (prior[1], chunk_id)
                )
                event(ctl, "INFO", "idempotent_skip",
                      f"Chunk {chunk_id} skipped: identical chunk {prior[0]} was already completed ({prior[1]} rows)",
                      ch["run_id"], chunk_id, prior_chunk_id=prior[0])
                return "done"

        for attempt in range(1, retries + 2):
            own = source is None
            src = source or call_source_factory(ch["zone"], settings, ingest_type=t.get("ingest_type"), config=t.get("ingest_config"))
            try:
                ctl.execute("UPDATE bronze_ctl.load_chunk SET status='running', attempts=%s, started_at=now(), "
                            "last_heartbeat=now(), rows_fetched=0, error=NULL WHERE chunk_id=%s", (attempt, chunk_id))
                event(ctl, "INFO", "chunk_start", f"{ch['zone']}.{ch['src_table']} {ch['chunk_kind']} "
                      f"{ch['chunk_from']}..{ch['chunk_to']} attempt {attempt}", ch["run_id"], chunk_id)
                _load_chunk(ctl, src, t, ch, chunk_id, settings)
                return "done"
            except Exception as e:                     # noqa: BLE001  any failure -> record, retry, finally mark failed
                err = f"{type(e).__name__}: {e}"
                ctl.execute("UPDATE bronze_ctl.load_chunk SET status='failed', ended_at=now(), error=%s WHERE chunk_id=%s",
                            (err[:2000], chunk_id))
                event(ctl, "ERROR", "chunk_failed", err[:500], ch["run_id"], chunk_id, attempt=attempt,
                      trace=traceback.format_exc()[-1500:])
                if attempt > retries:
                    return "failed"
                time.sleep(min(60, float(settings.get("retry_sleep", 5)) * attempt) if own else 0)
            finally:
                if own:
                    src.close()
    finally:
        ctl.close()


def _load_chunk(ctl, src, t, ch, chunk_id, settings):
    cols = src.columns(t["src_schema"], t["src_table"])
    cmap = col_map(cols)
    text_idx = [i for i, c in enumerate(cols) if pg_type(c) == "text"]
    pred = {"kind": ch["chunk_kind"], "col": (t["chunk_col"] if ch["window_kind"] == "historic"
                                              else (t["watermark_col"] or t["chunk_col"])),
            "a": ch["chunk_from"], "b": ch["chunk_to"]}
    pg = connect(autocommit=True)
    try:
        # 1. Schema check & evolution tracking
        added = ensure_table(pg, t, cmap, ctl=ctl, run_id=ch["run_id"], chunk_id=chunk_id)
        if added:
            event(ctl, "WARNING", "schema_drift", f"new Oracle columns added to bronze: {added}", ch["run_id"], chunk_id,
                  columns=added)
        ensure_partitions(pg, t, ch["chunk_from"], ch["chunk_to"] or ch["chunk_from"])
        if t["verify_counts"]:
            n = src.count(t["src_schema"], t["src_table"], pred)
            ctl.execute("UPDATE bronze_ctl.load_chunk SET oracle_count=%s WHERE chunk_id=%s", (n, chunk_id))
        tgt = ident(t["tgt_schema"], t["tgt_table"])
        names = [pn for _, pn, _ in cmap]
        ocols = [on for on, _, _ in cmap]
        merge = t["mode"] == "merge" and t["pk_cols"]

        # Setup Data Quality and Duplicate Tracking
        quarantine_records = []
        seen_pks = set()
        pk_cols = [pgname(c) for c in (t["pk_cols"] or [])]
        pk_indices = [names.index(c) for c in pk_cols if c in names]
        quarantine_enabled = bool(t.get("quarantine_enabled", True) if "quarantine_enabled" in t else True)
        check_dups = bool(t.get("check_duplicates", True) if "check_duplicates" in t else True)
        dq_rules = t.get("dq_rules") or {}
        required_cols = [pgname(c) for c in dq_rules.get("required_cols", [])]
        req_indices = [names.index(c) for c in required_cols if c in names]

        # Sensitive columns safeguard
        import mapping
        sensitive_cols = mapping.get_sensitive_columns(ctl, t["zone"], t["src_table"])
        src_source = t.get("source_system") or t.get("zone") or "UNKNOWN"
        src_tbl_name = t.get("src_table") or "UNKNOWN"

        fetched, quarantined_count, duplicates_count = 0, 0, 0
        last_beat, last_log, t0 = time.time(), time.time(), time.time()
        pg.autocommit = False
        with pg.transaction():
            cur = pg.cursor()
            if t["mode"] == "full_refresh" or ch["chunk_kind"] == "all":
                cur.execute(sql.SQL("TRUNCATE {}").format(tgt)) if not merge else None
            elif not merge:
                where, params = pg_predicate(pred)
                cur.execute(sql.SQL("DELETE FROM {} WHERE ").format(tgt) + where, params)
            if merge:
                cur.execute(sql.SQL("CREATE TEMP TABLE _c ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA").format(
                    sql.SQL(", ").join(map(sql.Identifier, names)), tgt))
                dest, dcols, extra = sql.Identifier("_c"), names, ()
            else:
                dest, dcols, extra = tgt, names + ["_load_id", "_chunk_id", "_source", "_src_table"], (ch["run_id"], chunk_id, src_source, src_tbl_name)
            cp = sql.SQL("COPY {} ({}) FROM STDIN").format(dest, sql.SQL(", ").join(map(sql.Identifier, dcols)))
            with cur.copy(cp) as copy:
                for batch in src.stream(t["src_schema"], t["src_table"], ocols, pred, t["oracle_hint"]):
                    for r in batch:
                        fetched += 1
                        r = list(r)
                        for i in text_idx:              # Postgres text cannot hold NUL bytes
                            v = r[i]
                            if v is not None and "\x00" in v:
                                r[i] = v.replace("\x00", "")

                        # 2. Data Quality Check: PK and required columns null/empty check
                        rejection_reason = None
                        if pk_indices:
                            for idx in pk_indices:
                                val = r[idx]
                                if val is None or (isinstance(val, str) and val.strip() == ""):
                                    rejection_reason = f"PK column '{names[idx]}' is NULL or blank"
                                    break
                        if not rejection_reason and req_indices:
                            for idx in req_indices:
                                val = r[idx]
                                if val is None or (isinstance(val, str) and val.strip() == ""):
                                    rejection_reason = f"Required column '{names[idx]}' is NULL or blank"
                                    break

                        if rejection_reason:
                            quarantined_count += 1
                            if quarantine_enabled:
                                row_data = {
                                    ocols[i]: (str(r[i]) if isinstance(r[i], (dt.datetime, dt.date, decimal.Decimal)) else r[i])
                                    for i in range(len(ocols))
                                }
                                if sensitive_cols:
                                    row_data = mapping.sanitize_raw_payload(row_data, sensitive_cols)
                                quarantine_records.append((
                                    ch["run_id"], chunk_id, t["zone"], t["src_schema"], t["src_table"],
                                    rejection_reason, Jsonb(row_data)
                                ))
                            continue

                        # 3. Duplicate Check: Intra-chunk duplicate detection on PK
                        if pk_indices and check_dups:
                            pk_val = tuple(r[idx] for idx in pk_indices)
                            if pk_val in seen_pks:
                                duplicates_count += 1
                                if t["mode"] == "append":
                                    continue  # Skip intra-chunk duplicate in append mode
                            else:
                                seen_pks.add(pk_val)

                        copy.write_row(r + list(extra))

                    now = time.time()
                    if now - last_beat >= 5:            # heartbeat: lets monitoring tell "slow" from "stuck"
                        ctl.execute("UPDATE bronze_ctl.load_chunk SET rows_fetched=%s, rows_quarantined=%s, "
                                    "duplicates_skipped=%s, last_heartbeat=now() WHERE chunk_id=%s",
                                    (fetched, quarantined_count, duplicates_count, chunk_id))
                        last_beat = now
                    if now - last_log >= 60:
                        event(ctl, "INFO", "progress", f"{fetched} rows ({quarantined_count} quarantined, {duplicates_count} dups), "
                              f"{int(fetched / (now - t0))} rows/s", ch["run_id"], chunk_id, rows=fetched)
                        last_log = now

            loaded = cur.rowcount if cur.rowcount >= 0 else (fetched - quarantined_count - (duplicates_count if t["mode"] == "append" else 0))
            if merge:
                pk = sql.SQL(", ").join(sql.Identifier(pgname(c)) for c in t["pk_cols"])
                nonpk = [n for n in names if n not in {pgname(c) for c in t["pk_cols"]}]
                ins = sql.SQL("INSERT INTO {} AS t ({}, _load_id, _chunk_id, _source, _src_table) SELECT DISTINCT ON ({}) {}, %s, %s, %s, %s FROM _c "
                              "ORDER BY {} ON CONFLICT ({}) ").format(
                    tgt, sql.SQL(", ").join(map(sql.Identifier, names)), pk,
                    sql.SQL(", ").join(map(sql.Identifier, names)), pk, pk)
                if nonpk:
                    sets = sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(n)) for n in nonpk)
                    old = sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(n)) for n in nonpk)
                    new = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(n)) for n in nonpk)
                    ins += sql.SQL("DO UPDATE SET {}, _load_id = EXCLUDED._load_id, _chunk_id = EXCLUDED._chunk_id, "
                                   "_loaded_at = now(), _source = EXCLUDED._source, _src_table = EXCLUDED._src_table WHERE ({}) IS DISTINCT FROM ({})").format(sets, old, new)
                else:
                    ins += sql.SQL("DO NOTHING")
                cur.execute(ins, (ch["run_id"], chunk_id, src_source, src_tbl_name))

            expected_loaded = fetched - quarantined_count - (duplicates_count if t["mode"] == "append" else 0)
            if not merge and loaded != expected_loaded:
                raise RuntimeError(f"fetched {fetched} rows (quarantined: {quarantined_count}, dups: {duplicates_count}) but COPY loaded {loaded}")
            oc = ctl.execute("SELECT oracle_count FROM bronze_ctl.load_chunk WHERE chunk_id=%s", (chunk_id,)).fetchone()[0]
            if oc is not None and oc != fetched:
                raise RuntimeError(f"Oracle COUNT(*) is {oc} but {fetched} rows were fetched")

        # Persist quarantine records to bronze_ctl.load_quarantine
        if quarantine_records:
            try:
                with ctl.cursor() as cur_ctl:
                    cur_ctl.executemany(
                        "INSERT INTO bronze_ctl.load_quarantine (run_id, chunk_id, zone, src_schema, src_table, rejection_reason, row_data) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        quarantine_records
                    )
                event(ctl, "WARNING", "dq_quarantine", f"{quarantined_count} invalid row(s) routed to quarantine",
                      ch["run_id"], chunk_id, quarantined=quarantined_count)
            except Exception as e:
                LOG.warning("Failed to persist quarantine records to bronze_ctl.load_quarantine: %s", e)

        if duplicates_count > 0:
            event(ctl, "WARNING", "duplicates_detected", f"{duplicates_count} duplicate row(s) detected in source stream",
                  ch["run_id"], chunk_id, duplicates=duplicates_count)

        secs = max(time.time() - t0, 0.001)
        ctl.execute("UPDATE bronze_ctl.load_chunk SET status='done', rows_fetched=%s, rows_loaded=%s, "
                    "rows_quarantined=%s, duplicates_skipped=%s, ended_at=now(), last_heartbeat=now() WHERE chunk_id=%s",
                    (fetched, loaded, quarantined_count, duplicates_count, chunk_id))
        event(ctl, "INFO", "chunk_done", f"{fetched} rows fetched, {loaded} loaded, {quarantined_count} quarantined, "
              f"{duplicates_count} duplicates in {secs:.1f}s ({int(fetched / secs)} rows/s)",
              ch["run_id"], chunk_id, rows=fetched, loaded=loaded, quarantined=quarantined_count,
              duplicates=duplicates_count, seconds=round(secs, 1))
    finally:
        pg.close()


# =============================================================== planning
def day_floor(d):
    return dt.datetime(d.year, d.month, d.day)


def plan_historic(ctl, src, t, force):
    """returns list of (kind, a, b) that make up the whole table"""
    key = (t["zone"], t["src_schema"], t["src_table"])
    st = ctl.execute("SELECT historic_started_at, historic_done_at FROM bronze_ctl.table_state WHERE zone=%s AND "
                     "src_schema=%s AND src_table=%s", key).fetchone()
    if st and st[1] and not force:
        return None
    if not st:
        ctl.execute("INSERT INTO bronze_ctl.table_state (zone, src_schema, src_table) VALUES (%s,%s,%s)", key)
    if not st or not st[0] or force:
        ctl.execute("UPDATE bronze_ctl.table_state SET historic_started_at=%s, historic_done_at=NULL WHERE zone=%s "
                    "AND src_schema=%s AND src_table=%s", (src.now(), *key))
    if t["mode"] == "full_refresh" or not t["chunk_col"]:
        return [("all", None, None)]
    lo, hi = src.bounds(t["src_schema"], t["src_table"], t["chunk_col"])
    plan = []
    if lo is not None:
        a, step = day_floor(lo), dt.timedelta(days=t["chunk_days"])
        while a <= hi:
            plan.append(("range", a, a + step))
            a += step
    cols = src.columns(t["src_schema"], t["src_table"])
    nullable = next((c["nullable"] for c in cols if c["name"] == t["chunk_col"]), True)
    if nullable:
        plan.append(("null", None, None))
    return plan


def plan_incr(ctl, src, t):
    key = (t["zone"], t["src_schema"], t["src_table"])
    wm = t["watermark_col"] or t["chunk_col"]
    if t["mode"] == "full_refresh" or not wm:
        return [("all", None, None)], None
    st = ctl.execute("SELECT historic_done_at, last_window_to FROM bronze_ctl.table_state WHERE zone=%s AND src_schema=%s "
                     "AND src_table=%s", key).fetchone()
    if not st or not st[0]:
        LOG.warning("%s.%s: historic load not finished, skipping incremental", t["zone"], t["src_table"])
        return None, None
    to = src.now() - SETTLE
    a = st[1] - dt.timedelta(hours=t["lookback_hours"])
    if to <= st[1]:
        return [], None
    return [("window", a, to)], to


def new_chunks(ctl, run_id, t, window_kind, plan):
    ids = []
    for kind, a, b in plan:
        r = ctl.execute(
            "INSERT INTO bronze_ctl.load_chunk (run_id, zone, src_schema, src_table, tgt_schema, tgt_table, window_kind, "
            "chunk_kind, chunk_from, chunk_to) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING chunk_id",
            (run_id, t["zone"], t["src_schema"], t["src_table"], t["tgt_schema"], t["tgt_table"], window_kind, kind, a, b))
        ids.append(r.fetchone()[0])
    return ids


# =============================================================== execution pool
_SEMAPHORES = {}


def _init_worker(sems):
    global _SEMAPHORES  # noqa: PLW0603
    _SEMAPHORES = sems


def _pool_task(chunk_id, cfg, zone):
    sem = _SEMAPHORES.get(zone)
    if sem:
        with sem:
            return chunk_id, run_chunk(chunk_id, cfg)
    return chunk_id, run_chunk(chunk_id, cfg)


def cmd_load(args, kind):
    cfg = load_cfg(args.config)
    setup_logging(cfg)
    ctl = connect()
    r = ctl.execute("INSERT INTO bronze_ctl.load_run (mode, host, pid, params) VALUES (%s,%s,%s,%s) RETURNING run_id",
                    (kind, socket.gethostname(), os.getpid(), Jsonb(vars(args))))
    run_id = r.fetchone()[0]
    event(ctl, "INFO", "run_start", f"{kind} load started", run_id)
    where = ["enabled"]
    params = []
    if args.zone:
        where.append("zone = %s")
        params.append(args.zone)
    if args.table:
        where.append("src_table = %s")
        params.append(args.table)
    q = "SELECT * FROM bronze_ctl.table_config WHERE " + " AND ".join(where) + " ORDER BY est_rows DESC NULLS LAST"
    tables = rowdict(ctl.execute(q, params))
    if not tables:
        LOG.warning("No enabled tables match the filter")
        ctl.execute("UPDATE bronze_ctl.load_run SET status='empty', ended_at=now() WHERE run_id=%s", (run_id,))
        return 0
    per_zone = int(cfg.get("settings", {}).get("max_workers_per_zone", 4))
    srcs, jobs, after = {}, [], {}
    for t in tables:
        cache_key = (t["zone"], t.get("ingest_type", "oracle"), json.dumps(t.get("ingest_config") or {}, sort_keys=True))
        src = srcs.get(cache_key) or srcs.setdefault(cache_key, call_source_factory(t["zone"], cfg.get("settings", {}), ingest_type=t.get("ingest_type"), config=t.get("ingest_config")))
        try:
            if kind == "historic":
                plan = plan_historic(ctl, src, t, args.force)
                if plan is None:
                    LOG.info("%s.%s: historic already complete (use --force to redo)", t["zone"], t["src_table"])
                    continue
                to = None
                done = {(r[0], r[1], r[2]) for r in ctl.execute(
                    "SELECT chunk_kind, chunk_from, chunk_to FROM bronze_ctl.load_chunk WHERE zone=%s AND src_schema=%s "
                    "AND src_table=%s AND window_kind='historic' AND status='done'", (t["zone"], t["src_schema"], t["src_table"]))}
                todo = [c for c in plan if args.force or c not in done]     # resume: skip chunks already done
            else:
                plan, to = plan_incr(ctl, src, t)
                if plan is None:
                    continue
                todo = plan
            ids = new_chunks(ctl, run_id, t, kind if kind == "historic" else "incr", todo)
            jobs += [(i, t["zone"]) for i in ids]
            after[(t["zone"], t["src_schema"], t["src_table"])] = (t, plan, to)
            event(ctl, "INFO", "planned", f"{t['zone']}.{t['src_table']}: {len(todo)} chunks ({len(plan) - len(todo)} already done)",
                  run_id, table=t["src_table"])
        except Exception as e:                       # noqa: BLE001  planning failure of one table must not stop the others
            event(ctl, "ERROR", "plan_failed", f"{t['zone']}.{t['src_table']}: {e}", run_id)
    results = {}
    if args.workers <= 1:
        for cid, zone in jobs:
            results[cid] = run_chunk(cid, cfg, source=srcs[zone] if getattr(args, "reuse_source", False) else None, force=args.force)
    else:
        sems = {z: mp.Semaphore(per_zone) for z in {z for _, z in jobs}}
        with cf.ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker, initargs=(sems,)) as ex:
            for cid, st in ex.map(_pool_task, [j[0] for j in jobs], [cfg] * len(jobs), [j[1] for j in jobs]):
                results[cid] = st
    for s in srcs.values():
        s.close()
    ok_all = True
    for key, (t, plan, to) in after.items():
        if kind == "historic":
            done = {(r[0], r[1], r[2]) for r in ctl.execute(
                "SELECT chunk_kind, chunk_from, chunk_to FROM bronze_ctl.load_chunk WHERE zone=%s AND src_schema=%s "
                "AND src_table=%s AND window_kind='historic' AND status='done'", key)}
            ok = all(c in done for c in plan)
            if ok:
                ctl.execute("UPDATE bronze_ctl.table_state SET historic_done_at=now(), last_window_to=historic_started_at, "
                            "last_success_at=now() WHERE zone=%s AND src_schema=%s AND src_table=%s", key)
        else:
            mine = [i for i, in ctl.execute("SELECT chunk_id FROM bronze_ctl.load_chunk WHERE run_id=%s AND zone=%s AND "
                                            "src_schema=%s AND src_table=%s", (run_id, *key))]
            ok = all(results.get(i) == "done" for i in mine)
            if ok:
                ctl.execute("INSERT INTO bronze_ctl.table_state (zone, src_schema, src_table, last_window_to, last_success_at) "
                            "VALUES (%s,%s,%s,%s,now()) ON CONFLICT (zone, src_schema, src_table) DO UPDATE SET "
                            "last_success_at = now(), last_window_to = COALESCE(EXCLUDED.last_window_to, "
                            "bronze_ctl.table_state.last_window_to)", (*key, to))
        ok_all &= ok
    status = "success" if ok_all and all(v == "done" for v in results.values()) else "failed"
    ctl.execute("UPDATE bronze_ctl.load_run SET status=%s, ended_at=now() WHERE run_id=%s", (status, run_id))
    event(ctl, "INFO" if status == "success" else "ERROR", "run_end", f"{kind} run {status}: {len(results)} chunks", run_id)
    return 0 if status == "success" else 1


# =============================================================== init / discover / status / check / dq / schema / verify / mapping
def cmd_init(args):
    ctl = connect()
    here = os.path.dirname(os.path.abspath(__file__))
    ctl.execute(open(os.path.join(here, "bronze_ctl.sql"), encoding="utf-8").read())
    import mapping
    res = mapping.sync_mapping_to_db(ctl)
    print(f"bronze_ctl schema ready (includes DQ quarantine, schema history, and {res['mappings_synced']} NZ->CFA360 mappings)")
    return 0


def cmd_discover(args):
    cfg = load_cfg(args.config)
    setup_logging(cfg)
    ctl = connect()
    over = {(o["zone"], o.get("schema"), o["name"]): o for o in cfg.get("table", [])}
    for z in cfg["zone"]:
        src = SOURCE_FACTORY(z["name"], cfg.get("settings", {}))
        target_schemas = z.get("schemas", [])
        if not target_schemas or target_schemas == ["*"] or getattr(args, "live_oracle", False):
            if hasattr(src, "schemas"):
                target_schemas = src.schemas()
            if not target_schemas and hasattr(src, "current_schema"):
                target_schemas = [src.current_schema()]
        multi = len(target_schemas) > 1
        flagged = []
        for schema in target_schemas:
            for name, nrows in src.tables(schema):
                if not any(fnmatch.fnmatch(name, p) for p in z.get("include", ["*"])):
                    continue
                if any(fnmatch.fnmatch(name, p) for p in z.get("exclude", [])):
                    continue
                o = over.get((z["name"], schema, name)) or over.get((z["name"], None, name)) or {}
                cols = src.columns(schema, name)
                col_names_upper = [c["name"].upper() for c in cols]
                pk = o.get("pk") or src.pk_cols(schema, name)
                if not pk and "ROW_ID" in col_names_upper:
                    pk = ["ROW_ID"]
                wm, ck = o.get("watermark_col"), o.get("chunk_col")
                if not wm and not ck:
                    for cand in ["LAST_UPD", "MODIFIED", "CHANGED_AT", "UPTIM", "CHDAT", "UPD_DT", "TS"]:
                        if cand in col_names_upper:
                            wm = cand
                            break
                mode = o.get("mode") or ("merge" if pk and wm else "append" if (wm or ck) else "full_refresh")
                tgt_table = pgname(f"{schema}__{name}" if multi else name)
                check_dups = o.get("check_duplicates", True)
                check_idem = o.get("check_idempotency", True)
                quar_en = o.get("quarantine_enabled", True)
                dq_rules = Jsonb(o.get("dq_rules", {})) if "dq_rules" in o else None
                src_source = o.get("source") or o.get("source_system") or z["name"]
                src_meta = Jsonb(o.get("source_metadata", o.get("meta", {}))) if ("source_metadata" in o or "meta" in o) else None
                ctl.execute(
                    "INSERT INTO bronze_ctl.table_config (zone, src_schema, src_table, tgt_schema, tgt_table, mode, pk_cols, "
                    "watermark_col, chunk_col, chunk_days, lookback_hours, partitioned, oracle_hint, verify_counts, "
                    "freshness_sla_hours, est_rows, check_duplicates, check_idempotency, quarantine_enabled, dq_rules, source_system, source_metadata) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (zone, src_schema, src_table) DO UPDATE SET mode=EXCLUDED.mode, pk_cols=EXCLUDED.pk_cols, "
                    "watermark_col=EXCLUDED.watermark_col, chunk_col=EXCLUDED.chunk_col, chunk_days=EXCLUDED.chunk_days, "
                    "lookback_hours=EXCLUDED.lookback_hours, partitioned=EXCLUDED.partitioned, "
                    "oracle_hint=EXCLUDED.oracle_hint, verify_counts=EXCLUDED.verify_counts, "
                    "freshness_sla_hours=EXCLUDED.freshness_sla_hours, est_rows=EXCLUDED.est_rows, "
                    "check_duplicates=EXCLUDED.check_duplicates, check_idempotency=EXCLUDED.check_idempotency, "
                    "quarantine_enabled=EXCLUDED.quarantine_enabled, dq_rules=EXCLUDED.dq_rules, "
                    "source_system=COALESCE(EXCLUDED.source_system, bronze_ctl.table_config.source_system), "
                    "source_metadata=COALESCE(EXCLUDED.source_metadata, bronze_ctl.table_config.source_metadata)",
                    (z["name"], schema, name, "bronze_" + z["name"].lower(), tgt_table, mode, pk or None, wm, ck,
                     o.get("chunk_days", 1), o.get("lookback_hours", 48), o.get("partitioned", False), o.get("oracle_hint"),
                     o.get("verify_counts", False), o.get("freshness_sla_hours", 26), nrows, check_dups, check_idem, quar_en, dq_rules,
                     src_source, src_meta))
                if mode == "full_refresh" and (nrows or 0) > 5_000_000:
                    flagged.append(f"{schema}.{name} (~{nrows} rows): no watermark/chunk column, would be fully reloaded")
        src.close()
        print(f"zone {z['name']}: tables registered")
        for f in flagged:
            print("  NEEDS watermark_col/chunk_col in config.toml:", f)


def cmd_tables(args):
    """Pull table list directly from the live Oracle DB connection."""
    cfg = load_cfg(args.config) if os.path.exists(args.config) else {}
    zones = [args.zone.upper()] if getattr(args, "zone", None) else [z["name"] for z in cfg.get("zone", [{"name": "NZ"}])]

    ctl = None
    try:
        ctl = connect()
    except Exception:
        pass

    registered = {}
    if ctl is not None:
        try:
            cfg_rows = rowdict(ctl.execute(
                "SELECT zone, src_schema, src_table, tgt_schema, tgt_table, mode FROM bronze_ctl.table_config"
            ))
            for r in cfg_rows:
                registered[(r["zone"], r["src_schema"].upper(), r["src_table"].upper())] = r
        except Exception:
            pass

    out_json = []
    for zone in zones:
        print(f"\n================================================================================")
        print(f" ORACLE DB CONNECTION: ZONE {zone}")
        print(f"================================================================================")
        try:
            src = SOURCE_FACTORY(zone, cfg.get("settings", {}))
            current = src.current_schema() if hasattr(src, "current_schema") else "UNKNOWN"
            schemas = src.schemas() if hasattr(src, "schemas") else []
            print(f" Connected: True | Current Schema: {current} | Accessible Schemas: {', '.join(schemas) if schemas else 'None'}")
            print("--------------------------------------------------------------------------------")

            schema_filter = getattr(args, "schema", None)
            if schema_filter:
                raw_tables = [(schema_filter.upper(), t[0], t[1]) for t in src.tables(schema_filter)]
            elif hasattr(src, "all_accessible_tables"):
                raw_tables = [(t["owner"], t["table_name"], t["num_rows"]) for t in src.all_accessible_tables()]
            else:
                raw_tables = []
                for s in (schemas or [current]):
                    for t in src.tables(s):
                        raw_tables.append((s, t[0], t[1]))

            rows_out = []
            for owner, tbl, nrows in raw_tables:
                reg = registered.get((zone, owner.upper(), tbl.upper()))
                status = f"REGISTERED ({reg['mode']})" if reg else "UNREGISTERED"
                rows_out.append([zone, owner, tbl, nrows if nrows is not None else 0, status])
                out_json.append({
                    "zone": zone,
                    "owner": owner,
                    "table_name": tbl,
                    "num_rows": nrows,
                    "registered": reg is not None,
                    "mode": reg["mode"] if reg else None,
                })

            print(f" Found {len(rows_out)} table(s) in Oracle connection:")
            print(fmt(rows_out, ["Zone", "Schema / Owner", "Table Name", "Est Rows", "Bronze Status"]))
            src.close()
        except Exception as e:
            print(f" Failed to query Oracle DB connection for zone {zone}: {e}")
    print("================================================================================\n")
    if getattr(args, "json", False):
        print(json.dumps(out_json, indent=2))
    return 0


def cmd_sources(args):
    """List all registered tables categorized by their business source system."""
    ctl = connect()
    rows = rowdict(ctl.execute("""
        SELECT COALESCE(source_system, zone) AS source_name,
               COALESCE(ingest_type, 'oracle') AS ingest_type,
               src_schema AS oracle_schema,
               src_table AS oracle_table,
               tgt_schema || '.' || tgt_table AS bronze_target_table,
               mode,
               COALESCE(est_rows, 0) AS est_rows,
               enabled,
               source_metadata
        FROM bronze_ctl.table_config
        ORDER BY source_name, src_table
    """))
    print("\n================================================================================")
    print(" SOURCE SYSTEM & TABLE METADATA CATALOG (MULTI-MODE INGEST)")
    print("================================================================================")
    fmt_rows = [[r["source_name"], r["ingest_type"].upper(), r["oracle_schema"], r["oracle_table"], r["bronze_target_table"], r["mode"], r["est_rows"], "ENABLED" if r["enabled"] else "DISABLED"] for r in rows]
    print(fmt(fmt_rows, ["Source System", "Ingest Type", "Source Schema", "Source Table", "Bronze Target Table", "Mode", "Est Rows", "Status"]))
    print("================================================================================\n")
    if getattr(args, "json", False):
        print(json.dumps(rows, default=str, indent=2))
    return 0


def cmd_tag(args):
    """Tag or mark a table's business source system (SZ_zone, EZ_zone, NZ_zone, Teerva, portal, FMS, etc.)."""
    if not getattr(args, "table", None) or not getattr(args, "source", None):
        print("Error: --table and --source are required")
        return 1
    ctl = connect()
    meta = Jsonb(json.loads(args.meta)) if getattr(args, "meta", None) else None
    res = ctl.execute("""
        UPDATE bronze_ctl.table_config
        SET source_system = %s,
            source_metadata = COALESCE(%s, source_metadata)
        WHERE UPPER(src_table) = UPPER(%s)
        RETURNING zone, src_schema, src_table, source_system;
    """, (args.source, meta, args.table.strip())).fetchall()
    ctl.commit()
    if res:
        for r in res:
            print(f"PASS: Marked table '{r[1]}.{r[2]}' with source '{r[3]}'")
        return 0
    else:
        print(f"Warning: Table '{args.table}' not found in bronze_ctl.table_config")
        return 1


def cmd_ingest(args):
    """Directly ingest a Parquet file, CSV file, or JDBC query/table into Bronze data lake."""
    itype = getattr(args, "type", None)
    file_path = getattr(args, "file", None)
    jdbc_url = getattr(args, "url", None)
    if not itype:
        if file_path:
            if file_path.endswith(".parquet"):
                itype = "parquet"
            elif file_path.endswith(".csv") or file_path.endswith(".tsv") or file_path.endswith(".csv.gz"):
                itype = "csv"
            else:
                itype = "parquet"
        elif jdbc_url:
            itype = "jdbc"
        else:
            itype = "oracle"
    itype = itype.lower()

    src_table = getattr(args, "table", None)
    if not src_table:
        if file_path:
            src_table = os.path.splitext(os.path.basename(file_path))[0]
            if src_table.endswith(".csv"):
                src_table = os.path.splitext(src_table)[0]
        elif jdbc_url:
            src_table = "EXT_JDBC_DATA"
        else:
            src_table = "DATA_INGEST"
    src_table = src_table.strip().upper()

    zone = (getattr(args, "zone", None) or "NZ").strip().upper()
    src_schema = getattr(args, "schema", None) or (itype.upper() if itype in ("parquet", "csv") else "PUBLIC")
    tgt_schema = f"bronze_{zone.lower()}"
    tgt_table = pgname(src_table)
    source_system = getattr(args, "source", None) or zone

    config = {
        "type": itype,
        "path": file_path,
        "jdbc_url": jdbc_url,
        "delimiter": getattr(args, "delimiter", ",") or ",",
    }
    if getattr(args, "meta", None):
        try:
            meta = json.loads(args.meta)
        except Exception:
            meta = {"note": args.meta}
    else:
        meta = {}

    ctl = connect()
    pg = connect(autocommit=False)
    try:
        src = call_source_factory(zone, {"arraysize": getattr(args, "arraysize", 50000)}, ingest_type=itype, config=config)
        cols = src.columns(src_schema, src_table)
        if not cols:
            print(f"Error: Could not discover columns from {itype} source ({file_path or jdbc_url})")
            return 1

        pk = [p.strip() for p in args.pk.split(",")] if getattr(args, "pk", None) else src.pk_cols(src_schema, src_table)
        watermark = getattr(args, "watermark", None)
        mode = getattr(args, "mode", None) or ("merge" if pk else "append")
        if mode == "merge" and not pk:
            LOG.warning("Merge mode requested for %s.%s but no primary key specified/detected; falling back to append", tgt_schema, tgt_table)
            mode = "append"

        # 1. Register in bronze_ctl.table_config
        ctl.execute("""
            INSERT INTO bronze_ctl.table_config (
                zone, src_schema, src_table, tgt_schema, tgt_table, mode, pk_cols, watermark_col,
                est_rows, source_system, source_metadata, ingest_type, ingest_config
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (zone, src_schema, src_table) DO UPDATE SET
                mode = EXCLUDED.mode, pk_cols = EXCLUDED.pk_cols, watermark_col = EXCLUDED.watermark_col,
                source_system = EXCLUDED.source_system, source_metadata = EXCLUDED.source_metadata,
                ingest_type = EXCLUDED.ingest_type, ingest_config = EXCLUDED.ingest_config
        """, (
            zone, src_schema, src_table, tgt_schema, tgt_table, mode, pk or None, watermark,
            0, source_system, Jsonb(meta), itype, Jsonb(config)
        ))
        ctl.commit()

        # 2. Record load run and chunk
        cur = ctl.cursor()
        cur.execute("INSERT INTO bronze_ctl.load_run (mode, host, pid, params) VALUES (%s, %s, %s, %s) RETURNING run_id",
                    ("ingest", socket.gethostname(), os.getpid(), Jsonb({"type": itype, "table": src_table, "source": source_system})))
        run_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO bronze_ctl.load_chunk (
                run_id, zone, src_schema, src_table, tgt_schema, tgt_table,
                window_kind, chunk_kind, status, started_at
            ) VALUES (%s, %s, %s, %s, %s, %s, 'historic', 'all', 'running', now()) RETURNING chunk_id
        """, (run_id, zone, src_schema, src_table, tgt_schema, tgt_table))
        chunk_id = cur.fetchone()[0]
        ctl.commit()

        # 3. Create or evolve bronze table
        cmap = col_map(cols)
        t_dict = {
            "zone": zone, "src_schema": src_schema, "src_table": src_table,
            "tgt_schema": tgt_schema, "tgt_table": tgt_table, "mode": mode,
            "pk_cols": pk, "watermark_col": watermark, "chunk_col": None, "partitioned": False,
            "source_system": source_system
        }
        ensure_table(pg, t_dict, cmap, ctl=ctl, run_id=run_id, chunk_id=chunk_id)

        # 4. Stream & Load rows
        names = [pn for _, pn, _ in cmap]
        ocols = [on for on, _, _ in cmap]
        merge = mode == "merge" and pk
        tgt = ident(tgt_schema, tgt_table)

        fetched, quarantined_count, dups_count = 0, 0, 0
        seen_pks = set()
        pk_cols = [pgname(c) for c in (pk or [])]
        pk_indices = [names.index(c) for c in pk_cols if c in names]
        extra = (run_id, chunk_id, source_system, src_table)

        # Sensitive columns guard
        import mapping
        sensitive_cols = mapping.get_sensitive_columns(ctl, zone, src_table)

        t0 = time.time()
        with pg.transaction():
            pcur = pg.cursor()
            if mode == "full_refresh":
                pcur.execute(sql.SQL("TRUNCATE {}").format(tgt))
            if merge:
                pcur.execute(sql.SQL("CREATE TEMP TABLE _c ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA").format(
                    sql.SQL(", ").join(map(sql.Identifier, names)), tgt))
                dest, dcols = sql.Identifier("_c"), names
            else:
                dest = tgt
                dcols = names + ["_load_id", "_chunk_id", "_source", "_src_table"]

            cp = sql.SQL("COPY {} ({}) FROM STDIN").format(dest, sql.SQL(", ").join(map(sql.Identifier, dcols)))
            with pcur.copy(cp) as copy:
                for batch in src.stream(src_schema, src_table, ocols, {"kind": "all"}):
                    for r in batch:
                        fetched += 1
                        r = list(r)
                        for i, v in enumerate(r):
                            if isinstance(v, str) and "\x00" in v:
                                r[i] = v.replace("\x00", "")

                        # Data Quality: PK null check
                        rej = None
                        if pk_indices:
                            for idx in pk_indices:
                                if r[idx] is None or (isinstance(r[idx], str) and r[idx].strip() == ""):
                                    rej = f"PK column '{names[idx]}' is NULL or blank"
                                    break
                        if rej:
                            quarantined_count += 1
                            raw_dict = dict(zip(ocols, [str(x) for x in r]))
                            if sensitive_cols:
                                raw_dict = mapping.sanitize_raw_payload(raw_dict, sensitive_cols)
                            ctl.execute("""
                                INSERT INTO bronze_ctl.load_quarantine (
                                    run_id, chunk_id, zone, src_schema, src_table, rejection_reason, row_data
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (run_id, chunk_id, zone, src_schema, src_table, rej, Jsonb(raw_dict)))
                            continue

                        # Duplicate check
                        if pk_indices:
                            pk_val = tuple(r[idx] for idx in pk_indices)
                            if pk_val in seen_pks:
                                dups_count += 1
                                if mode == "append":
                                    continue
                            seen_pks.add(pk_val)

                        if merge:
                            copy.write_row(r)
                        else:
                            copy.write_row(r + list(extra))

            if merge:
                pk_sql = sql.SQL(", ").join(sql.Identifier(pgname(c)) for c in pk)
                nonpk = [n for n in names if n not in {pgname(c) for c in pk}]
                ins = sql.SQL("INSERT INTO {} AS t ({}, _load_id, _chunk_id, _source, _src_table) SELECT DISTINCT ON ({}) {}, %s, %s, %s, %s FROM _c "
                              "ORDER BY {} ON CONFLICT ({}) ").format(
                    tgt, sql.SQL(", ").join(map(sql.Identifier, names)), pk_sql,
                    sql.SQL(", ").join(map(sql.Identifier, names)), pk_sql, pk_sql)
                if nonpk:
                    sets = sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(n)) for n in nonpk)
                    old = sql.SQL(", ").join(sql.SQL("t.{}").format(sql.Identifier(n)) for n in nonpk)
                    new = sql.SQL(", ").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(n)) for n in nonpk)
                    ins += sql.SQL("DO UPDATE SET {}, _load_id = EXCLUDED._load_id, _chunk_id = EXCLUDED._chunk_id, "
                                   "_loaded_at = now(), _source = EXCLUDED._source, _src_table = EXCLUDED._src_table WHERE ({}) IS DISTINCT FROM ({})").format(sets, old, new)
                else:
                    ins += sql.SQL("DO NOTHING")
                pcur.execute(ins, (run_id, chunk_id, source_system, src_table))

            loaded = fetched - quarantined_count - (dups_count if mode == "append" else 0)

        # 5. Mark chunk and run done
        ctl.execute("""
            UPDATE bronze_ctl.load_chunk
            SET status = 'done', rows_fetched = %s, rows_loaded = %s, rows_quarantined = %s,
                duplicates_skipped = %s, ended_at = now()
            WHERE chunk_id = %s
        """, (fetched, loaded, quarantined_count, dups_count, chunk_id))
        ctl.execute("UPDATE bronze_ctl.load_run SET status = 'success', ended_at = now() WHERE run_id = %s", (run_id,))
        ctl.execute("""
            INSERT INTO bronze_ctl.table_state (zone, src_schema, src_table, last_success_at, historic_done_at)
            VALUES (%s, %s, %s, now(), now())
            ON CONFLICT (zone, src_schema, src_table) DO UPDATE SET last_success_at = now(), historic_done_at = now()
        """, (zone, src_schema, src_table))
        ctl.commit()

        elapsed = round(time.time() - t0, 2)
        print("\n================================================================================")
        print(f" MULTI-MODE INGESTION SUCCESS: {itype.upper()} -> {tgt_schema}.{tgt_table}")
        print("================================================================================")
        print(f"  Ingest Type:         {itype.upper()}")
        print(f"  Source System:       {source_system}")
        print(f"  Source File / URL:   {file_path or jdbc_url or 'N/A'}")
        print(f"  Target Bronze Table: {tgt_schema}.{tgt_table}")
        print(f"  Mode:                {mode.upper()}")
        print(f"  Rows Fetched:        {fetched:,}")
        print(f"  Rows Loaded:         {loaded:,}")
        print(f"  Rows Quarantined:    {quarantined_count:,}")
        print(f"  Duplicates Skipped:  {dups_count:,}")
        print(f"  Elapsed Time:        {elapsed}s")
        print("================================================================================\n")
        if getattr(args, "return_dict", False):
            return {
                "ok": True,
                "ingest_type": itype,
                "target_schema": tgt_schema,
                "target_table": f"{tgt_schema}.{tgt_table}",
                "source_system": source_system,
                "mode": mode,
                "rows_fetched": fetched,
                "rows_loaded": loaded,
                "rows_quarantined": quarantined_count,
                "duplicates_skipped": dups_count,
                "elapsed_s": elapsed,
            }
        return 0
    finally:
        ctl.close()
        pg.close()


def fmt(rows, cols):
    if not rows:
        return "  (none)"
    w = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
    line = lambda r: "  " + "  ".join(str(v).ljust(w[i]) for i, v in enumerate(r))   # noqa: E731
    return "\n".join([line(cols), line(["-" * x for x in w])] + [line(r) for r in rows])


def cmd_status(args):
    ctl = connect()
    while True:
        a = ctl.execute("SELECT zone, src_table, chunk_kind, chunk_from::date, rows_fetched, rows_quarantined, duplicates_skipped, "
                        "rows_per_sec, elapsed::text, since_heartbeat::text, stalled FROM bronze_ctl.v_active_chunks ORDER BY zone, src_table").fetchall()
        h = ctl.execute("SELECT zone, src_table, chunks_done || '/' || chunks_total, pct_done, rows_loaded, rows_quarantined "
                        "FROM bronze_ctl.v_historic_progress WHERE pct_done < 100 ORDER BY zone, src_table").fetchall()
        s = ctl.execute("SELECT zone, src_table, mode, last_success_at, lag_hours, rows_loaded_all_runs, rows_quarantined_all_runs, chunks_failed "
                        "FROM bronze_ctl.v_table_status ORDER BY zone, src_table LIMIT 60").fetchall()
        al = ctl.execute("SELECT severity, zone, src_table, issue FROM bronze_ctl.v_alerts").fetchall()
        print(f"\n== {dt.datetime.now():%Y-%m-%d %H:%M:%S}  ALERTS ({len(al)})\n" + fmt(al, ["sev", "zone", "table", "issue"]))
        print("== RUNNING\n" + fmt(a, ["zone", "table", "kind", "from", "fetched", "quarantined", "dups", "rows/s", "elapsed", "heartbeat", "stalled"]))
        print("== HISTORIC IN PROGRESS\n" + fmt(h, ["zone", "table", "chunks", "pct", "loaded", "quarantined"]))
        print("== TABLES\n" + fmt(s, ["zone", "table", "mode", "last_success", "lag_h", "loaded", "quarantined", "failed"]))
        if not args.watch:
            return 0
        time.sleep(args.watch)


def cmd_check(args):
    ctl = connect()
    rows = ctl.execute("SELECT severity, zone, src_table, issue FROM bronze_ctl.v_alerts ORDER BY severity, zone").fetchall()
    for r in rows:
        print(" | ".join(str(x) for x in r))
    print(f"{len(rows)} alert(s)")
    return 2 if rows else 0


def cmd_dq(args):
    ctl = connect()
    summary = ctl.execute(
        "SELECT zone, src_schema, src_table, rows_fetched_total, rows_loaded_total, "
        "rows_quarantined_total, duplicates_skipped_total, quarantine_pct "
        "FROM bronze_ctl.v_dq_summary ORDER BY zone, src_table"
    ).fetchall()
    print("\n== DATA QUALITY & QUARANTINE SUMMARY ==")
    print(fmt(summary, ["zone", "schema", "table", "fetched", "loaded", "quarantined", "dups_skipped", "quarantine_%"]))

    recent = ctl.execute(
        "SELECT quarantine_id, zone, src_table, rejection_reason, quarantined_at::text "
        "FROM bronze_ctl.load_quarantine ORDER BY quarantine_id DESC LIMIT 20"
    ).fetchall()
    print("\n== RECENT QUARANTINED ROWS (latest 20) ==")
    print(fmt(recent, ["id", "zone", "table", "rejection_reason", "quarantined_at"]))
    return 0


def cmd_schema(args):
    ctl = connect()
    hist = ctl.execute(
        "SELECT history_id, zone, src_table, column_name, oracle_type, pg_type, change_type, detected_at::text "
        "FROM bronze_ctl.schema_history ORDER BY history_id DESC LIMIT 40"
    ).fetchall()
    print("\n== SCHEMA EVOLUTION & DRIFT HISTORY (latest 40) ==")
    print(fmt(hist, ["id", "zone", "table", "column", "oracle_type", "pg_type", "change_type", "detected_at"]))

    import mapping
    mapping.print_mapping_summary(ctl)
    return 0


def cmd_mapping(args):
    ctl = connect()
    import mapping
    if getattr(args, "add", False):
        if not args.table or not getattr(args, "col", None) or not getattr(args, "entity", None) or not getattr(args, "field", None):
            print("Error: --table, --col, --entity, and --field are required when using --add")
            return 1
        res = mapping.add_or_update_mapping(
            zone=args.zone,
            src_table=args.table,
            src_column=args.col,
            target_entity=args.entity,
            target_field=args.field,
            mapping_type=getattr(args, "type", "DIRECT") or "DIRECT",
            transform_rule=getattr(args, "rule", "DIRECT") or "DIRECT",
            is_sensitive=getattr(args, "sensitive", False),
            sensitive_reason=getattr(args, "reason", "") or "",
            ctl=ctl
        )
        print(f"PASS: {res['message']}")
        return 0
    elif getattr(args, "delete", False):
        if not args.table or not getattr(args, "col", None):
            print("Error: --table and --col are required when using --delete")
            return 1
        res = mapping.delete_mapping(
            zone=args.zone,
            src_table=args.table,
            src_column=args.col,
            target_entity=getattr(args, "entity", "") or "",
            target_field=getattr(args, "field", "") or "",
            ctl=ctl
        )
        print(f"PASS: {res['message']}")
        return 0

    if getattr(args, "sync", False):
        res = mapping.sync_mapping_to_db(ctl, zone=args.zone)
        print(f"Synced mapping to database: {res}")
    mapping.print_mapping_summary(ctl)
    return 0


def cmd_verify(args):
    ctl = connect()
    tables = rowdict(ctl.execute(
        "SELECT zone, src_schema, src_table, tgt_schema, tgt_table, mode, pk_cols "
        "FROM bronze_ctl.table_config WHERE enabled"
    ))
    pg = connect()
    print("\n== VERIFYING IDEMPOTENCY & DUPLICATES ACROSS TABLES ==")
    issues = 0
    for t in tables:
        full = f"{t['tgt_schema']}.{t['tgt_table']}"
        exists = pg.execute("SELECT to_regclass(%s)", (full,)).fetchone()[0]
        if not exists:
            continue
        total_rows = pg.execute(sql.SQL("SELECT count(*) FROM {}").format(ident(t["tgt_schema"], t["tgt_table"]))).fetchone()[0]
        dup_count = 0
        if t["pk_cols"]:
            pks = sql.SQL(", ").join(sql.Identifier(pgname(c)) for c in t["pk_cols"])
            dup_query = sql.SQL(
                "SELECT count(*) FROM (SELECT {} FROM {} GROUP BY {} HAVING count(*) > 1) d"
            ).format(pks, ident(t["tgt_schema"], t["tgt_table"]), pks)
            dup_count = pg.execute(dup_query).fetchone()[0]

        status_str = "OK" if dup_count == 0 else "FAIL (DUPLICATES FOUND)"
        if dup_count > 0:
            issues += 1
        print(f"  {full:<35} Mode: {t['mode']:<12} Total Rows: {total_rows:<8} Duplicate Keys: {dup_count:<4} [{status_str}]")
    pg.close()
    return 1 if issues > 0 else 0


def cmd_ui(a):
    import dashboard
    dashboard.run_server(host=a.host, port=a.port)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("init", "discover", "historic", "incr", "status", "check", "dq", "schema", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--config", default="config.toml")
        p.add_argument("--env-file", default=None, help="Path to .env file for credentials")
        if name == "discover":
            p.add_argument("--live-oracle", action="store_true", help="Auto-discover all schemas and tables directly from Oracle DB")
        if name in ("historic", "incr"):
            p.add_argument("--zone")
            p.add_argument("--table")
            p.add_argument("--workers", type=int, default=4)
            p.add_argument("--force", action="store_true")
        if name == "status":
            p.add_argument("--watch", type=int, default=0)

    p_tbl = sub.add_parser("tables", help="Pull and inspect live table list directly from Oracle DB connection")
    p_tbl.add_argument("--config", default="config.toml")
    p_tbl.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_tbl.add_argument("--zone", default=None, help="Optional zone code (NZ, SZ, WZ, EZ)")
    p_tbl.add_argument("--schema", default=None, help="Optional Oracle schema/owner filter")
    p_tbl.add_argument("--json", action="store_true", help="Output in JSON format")

    p_src = sub.add_parser("sources", help="List registered tables by business source system (SZ_zone, EZ_zone, NZ_zone, Teerva, portal, FMS)")
    p_src.add_argument("--config", default="config.toml")
    p_src.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_src.add_argument("--json", action="store_true", help="Output in JSON format")

    p_tag = sub.add_parser("tag", help="Tag a table with its source system and metadata (Teerva, portal, FMS, NZ_zone, etc.)")
    p_tag.add_argument("--config", default="config.toml")
    p_tag.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_tag.add_argument("--table", required=True, help="Table name to tag")
    p_tag.add_argument("--source", required=True, help="Source system name (e.g. Teerva, portal, FMS, SZ_zone, NZ_zone)")
    p_tag.add_argument("--meta", default=None, help="Optional JSON metadata string")

    p_ui = sub.add_parser("ui", help="Start real-time web UI dashboard")
    p_ui.add_argument("--config", default="config.toml")
    p_ui.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_ui.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    p_ui.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")

    p_map = sub.add_parser("mapping", help="Inspect, audit, add or change column mappings")
    p_map.add_argument("--config", default="config.toml")
    p_map.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_map.add_argument("--zone", default="NZ", help="Zone code (default: NZ)")
    p_map.add_argument("--table", default=None, help="Optional table filter or target table")
    p_map.add_argument("--sync", action="store_true", help="Sync mapping data from JSON into database")
    p_map.add_argument("--add", action="store_true", help="Add or update a column mapping")
    p_map.add_argument("--delete", action="store_true", help="Delete a column mapping")
    p_map.add_argument("--col", default=None, help="Source column name to map")
    p_map.add_argument("--entity", default=None, help="Target CFA entity (e.g. Customer_master_data)")
    p_map.add_argument("--field", default=None, help="Target CFA field name")
    p_map.add_argument("--type", default="DIRECT", help="Mapping type: DIRECT, DERIVED, PIPELINE")
    p_map.add_argument("--rule", default="DIRECT", help="Transformation rule / expression")
    p_map.add_argument("--sensitive", action="store_true", help="Flag column as sensitive PII / credential")
    p_map.add_argument("--reason", default="", help="Sensitive column classification reason")

    p_ing = sub.add_parser("ingest", help="Ingest Parquet, CSV, or JDBC source into Bronze PostgreSQL table")
    p_ing.add_argument("--config", default="config.toml")
    p_ing.add_argument("--env-file", default=None, help="Path to .env file for credentials")
    p_ing.add_argument("--type", choices=["parquet", "csv", "jdbc"], help="Ingestion type: parquet, csv, jdbc")
    p_ing.add_argument("--file", default=None, help="Path or glob pattern to Parquet or CSV file")
    p_ing.add_argument("--url", default=None, help="JDBC connection URL (e.g. jdbc:postgresql://..., jdbc:sqlite:...)")
    p_ing.add_argument("--table", default=None, help="Target Bronze table name")
    p_ing.add_argument("--schema", default=None, help="Source schema (for JDBC or logical schema)")
    p_ing.add_argument("--zone", default="NZ", help="Target zone code (default: NZ)")
    p_ing.add_argument("--source", default=None, help="Source system name (e.g. Teerva, portal, FMS, SZ_zone, NZ_zone)")
    p_ing.add_argument("--mode", default="merge", choices=["merge", "append", "full_refresh"], help="Ingestion mode")
    p_ing.add_argument("--pk", default=None, help="Comma-separated primary key columns")
    p_ing.add_argument("--watermark", default=None, help="Watermark column for incremental loading")
    p_ing.add_argument("--delimiter", default=",", help="Delimiter for CSV ingestion (default: ,)")
    p_ing.add_argument("--meta", default=None, help="Optional JSON metadata string")
    p_ing.add_argument("--arraysize", type=int, default=50000, help="Batch array size (default: 50000)")

    a = ap.parse_args(argv)
    if getattr(a, "env_file", None):
        load_env(a.env_file, override=True)
    if a.cmd in ("historic", "incr"):
        return cmd_load(a, a.cmd)
    return {
        "init": cmd_init,
        "discover": cmd_discover,
        "status": cmd_status,
        "check": cmd_check,
        "dq": cmd_dq,
        "schema": cmd_schema,
        "verify": cmd_verify,
        "ui": cmd_ui,
        "mapping": cmd_mapping,
        "tables": cmd_tables,
        "sources": cmd_sources,
        "tag": cmd_tag,
        "ingest": cmd_ingest,
    }[a.cmd](a) or 0


if __name__ == "__main__":
    mp.freeze_support()
    sys.exit(main())
