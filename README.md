# Tiger Retail Billing - Silver Layer Transformation

A bronze-to-silver PySpark pipeline built skill-first, per the assignment's
skill-driven workflow. It processes a baseline load plus four sample batches and
demonstrates deduplication, type casting, SCD2 history tracking, and entity
resolution, with cross-cutting pipeline guardrails and a pytest suite.

## Setup

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### Environment (Windows + Hadoop)

PySpark on Windows needs a JDK and a Hadoop `winutils.exe` / `hadoop.dll`
discoverable on `PATH`:

1. Install a JDK (Java 17 tested) and set `JAVA_HOME`.
2. Create `C:\hadoop\bin` and put `winutils.exe` and `hadoop.dll` there
   (e.g. from the `cdarlint/winutils` Hadoop 3.3.5 build).
3. Set `HADOOP_HOME=C:\hadoop` and add `%JAVA_HOME%\bin` and
   `%HADOOP_HOME%\bin` to `PATH`.

`src/spark_session.py` fills in these defaults automatically **only if** they are
unset and the default directories exist, so it does not override a working
environment. On macOS/Linux no extra setup is needed.

## Project layout

See `CLAUDE.md` for the full structure and conventions. In short:

- `data/` - input CSVs (Batch 1 baseline + Batches 2-5 sample batches).
- `.claude/skills/` - the five skill folders. Two were modified and two authored
  from scratch for this assignment (see `CLAUDE.md`).
- `src/` - the pipeline implementation.
- `silver/`, `quarantine/` - Parquet outputs, created when the pipeline runs.
- `tests/` - the pytest suite (Task 9).
- `outputs/artifacts/` - evidence: skill-driven evidence log, pipeline run log,
  pytest report.

## Running the pipeline

```bash
python -m src.pipeline
```

This loads Batch 1 (baseline) then applies Batches 2-5 in order, checking the
guardrails after every batch and writing a line per batch to
`outputs/artifacts/pipeline_log.txt`.

Expected outcomes:

| Stage | Effect |
|---|---|
| Batch 1 | 100 accounts, 6 plans, 480 invoices loaded |
| Batch 2 | 2 true duplicates retained, 4 conflicting rows quarantined, silver invoices stay 480 |
| Batch 3 | 3 accounts inserted (format + casing normalized), 1 unrecognized date quarantined |
| Batch 4 | 4 plan changes applied (close + append), 1 out-of-order change quarantined |
| Batch 5 | 3 reused account ids resolved to 3 new identities |
| Final | silver: 110 accounts / 6 plans / 480 invoices; quarantine: 2 accounts / 4 invoices |

Re-running the pipeline is idempotent (identical tables).

## Running tests

```bash
python -m pytest -v
```

## Data note (Batch 2)

The provided `batch_2_duplicate_retry` CSV does **not** match the assignment
narrative. The narrative describes 4 duplicate retries, 1 legitimate update and
1 conflict; the sample README and a test stub describe 4 duplicates and 2
conflicts. The actual data contains **2 byte-identical duplicates and 4 rows that
disagree with the baseline on `account_id` and `invoice_date`** (so 4 conflicts,
0 updates). The pipeline handles the data as written, the tests assert the
actual outcome, and the discrepancy is recorded in
`outputs/artifacts/skill_driven_evidence_log.md`.

## What to submit

Zip the complete project folder per the assignment's Section 8 (Submission
Rule), named `empid_emp_name_DE_SilverTransformation.zip`.
