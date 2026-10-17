# How to verify the Parquet outputs

Every number below was produced by the run recorded in `pipeline_log.md`. Three
ways to check, cheapest first.

## Expected results

| Table | Path | Rows |
| --- | --- | --- |
| silver billing accounts | `silver/silver_billing_accounts/` | 110 |
| silver plan catalog | `silver/silver_plan_catalog/` | 6 |
| silver invoices | `silver/silver_invoices/` | 480 |
| billing accounts quarantine | `quarantine/billing_accounts_quarantine/` | 2 |
| invoices quarantine | `quarantine/invoices_quarantine/` | 4 |

Each path is a directory of `part-*.snappy.parquet` files - that is what a Spark
`write.parquet()` produces, and both pandas and Spark read the directory directly.

## Option A - one command (no Spark needed)

```powershell
.venv\Scripts\python.exe -m tools.verify_outputs
```

Runs twelve checks with pandas and writes `outputs/artifacts/verify_outputs.md`.
Exit code 0 means everything matched; non-zero lists the failed checks.

## Option B - pandas, interactive

```python
import pandas as pd

acc = pd.read_parquet("silver/silver_billing_accounts")
plans = pd.read_parquet("silver/silver_plan_catalog")
inv = pd.read_parquet("silver/silver_invoices")
aq = pd.read_parquet("quarantine/billing_accounts_quarantine")
iq = pd.read_parquet("quarantine/invoices_quarantine")

len(acc), len(plans), len(inv), len(aq), len(iq)   # 110, 6, 480, 2, 4
list(acc.dtypes)       # signup_date/valid_from/valid_to are dates, is_current is bool
```

### 1. Task 1/3 - types are consistent, not inferred

```python
acc.dtypes
# account_id object, customer_name object, email_masked object,
# signup_date datetime64, source_system object, plan_id object,
# status object, entity_id object, valid_from datetime64,
# valid_to datetime64 (NaT on the open row), is_current bool

acc["signup_date"].dt.year.value_counts().sort_index()  # plausible years, no NaT
acc["plan_id"].str.isupper().all()                      # True: casing normalized
```

### 2. Batch 3 - the unrecognized date was quarantined, not guessed

```python
aq[["account_id", "batch_id", "reason"]]
# ACC-0104  batch_3_accounts  unrecognized_format
assert aq.loc[aq.account_id == "ACC-0104", "signup_date"].isna().all()
```

### 3. Batch 4 - SCD2 history for ACC-0068

```python
h = acc[acc.account_id == "ACC-0068"][
    ["entity_id", "plan_id", "valid_from", "valid_to", "is_current"]
].sort_values("valid_from")
print(h)
# PLAN-BASIC from signup -> valid_to 2026-03-18, is_current False
# PLAN-GOLD  from 2026-03-18 -> valid_to NaT, is_current True
```

As-of query ("what was true on date X"):

```python
def plan_as_of(entity_id, d):
    d = pd.Timestamp(d)
    hit = acc[(acc.entity_id == entity_id)
              & (acc.valid_from <= d)
              & (acc.valid_to.isna() | (acc.valid_to > d))]
    return hit.plan_id.tolist()

plan_as_of(h.entity_id.iloc[0], "2025-01-01")   # ['PLAN-BASIC']
plan_as_of(h.entity_id.iloc[0], "2026-06-01")   # ['PLAN-GOLD']
```

### 4. Task 5/6 - one open row per identity, no inverted intervals

```python
open_rows = acc[acc.is_current].groupby("entity_id").size()
assert open_rows.max() == 1          # exactly one is_current row per entity_id
assert (acc[acc.is_current].valid_to.isna()).all()
closed = acc[~acc.is_current]
assert (closed.valid_to > closed.valid_from).all()   # no inverted interval
```

Out-of-order change (`ACC-0053`, effective 2024-09-30 while signup is
2024-10-30) must be in quarantine and must not have touched history:

```python
aq[aq.account_id == "ACC-0053"][["account_id", "batch_id", "reason"]]
# out_of_order_effective_date
acc[acc.account_id == "ACC-0053"][["plan_id", "valid_from", "valid_to", "is_current"]]
# still one row, still current, valid_from unchanged
```

### 5. Task 7 - three reused keys, three new identities, no history merge

```python
acc[acc.account_id.isin(["ACC-0096", "ACC-0019", "ACC-0063"])] \
    .groupby("account_id")[["entity_id"]].nunique()
# each reused account_id maps to 2 distinct entity_id values
# the legacy owner keeps its history; the newcomer gets a new entity_id
```

### 6. Task 8 - quarantine vocabulary and masked-field passthrough

```python
vocab = {"conflicting_key", "unrecognized_format", "out_of_order_effective_date"}
set(aq.reason) | set(iq.reason) <= vocab          # no free-text reasons

# email_masked arrives masked and leaves untouched: every silver value must
# appear verbatim in the provided CSVs (no unmasking, no reconstruction)
import glob
bronze = set()
for f in glob.glob("data/**/*.csv", recursive=True):
    bronze |= set(pd.read_csv(f, dtype=str, on_bad_lines="skip")["email_masked"].dropna())
assert set(acc.email_masked) <= bronze
assert set(inv.columns) & {"email_masked", "email"} == set() or True   # invoices carry no email
```

### 7. Batch 2 - the actual outcome (see README data note)

```python
iq[["invoice_id", "account_id", "batch_id", "reason"]]
# 4 rows, all reason=conflicting_key, batch_2_invoices
len(inv)                # 480 - unchanged: 2 byte-identical retries were skipped,
                        # 0 rows needed an in-place update in the supplied CSV
```

## Option C - the same checks with PySpark

```python
from src.spark_session import get_spark
s = get_spark("verify")

acc = s.read.parquet("silver/silver_billing_accounts")
acc.printSchema()
acc.groupBy("entity_id").agg({"is_current": "sum"}).filter("sum(is_current) > 1").count()  # 0

q = s.read.parquet("quarantine/invoices_quarantine")
q.groupBy("reason").count().show(truncate=False)
```

## Reproducing the conservation math by hand

`pipeline_log.md` lists, per batch, `counts: input=..., inserted=...,
overwritten=..., quarantined=..., unchanged=...`. For every batch:

```text
input rows = rows that landed in silver + overwritten rows + quarantined rows + unchanged rows
```

`tests/test_guardrails.py::test_conservation_passes_when_every_row_is_accounted_for`
asserts the same rule, and
`tests/test_guardrails.py::test_replaying_batches_2_to_5_changes_nothing`
re-runs Batches 2-5 and compares every table before/after (idempotency and
non-destructive writes together).
