# Bronze Ingestion Pipeline

This project ingests five synthetic retail CSV entities into Parquet Bronze
tables with lineage metadata, duplicate-delivery protection, additive schema
drift handling, and row-level data-quality quarantine.

Run commands in this README from `starter-code/`. The `data/` and `.claude/`
folders must remain beside `starter-code/`, as supplied in this project.

## What the pipeline does

- Batch 1 loads `customers`, `products`, `stores`, `sales_transactions`, and
  `returns` into one `bronze/bronze_<entity>/` Parquet dataset per entity.
- Every ingested row receives `batch_id`, `source_file`, and
  `ingestion_timestamp`.
- Batch 2 uses `order_id` to skip a duplicate sales delivery.
- Batch 3 adds `loyalty_tier`; Batch 4 preserves it while adding
  `marketing_opt_in`.
- Batch 5 sends rows whose `customer_id` is null, empty, or whitespace-only to
  `quarantine/customers/batch_5/` with a `rejection_reason`.

## Prerequisites

- Python 3.10 or 3.11
- A Java JDK compatible with PySpark, with `java -version` available in the
  terminal. JDK 11 or 17 is recommended.
- On Windows only: Hadoop Windows utilities containing both `winutils.exe` and
  `hadoop.dll`. They are native executables, so they cannot be installed from
  `requirements.txt`.

### Windows Hadoop setup

Create or obtain a Hadoop utilities directory with this layout:

```text
C:\hadoop-3.3.5\
  bin\winutils.exe
  bin\hadoop.dll
```

Set it for the current PowerShell session before running Spark:

```powershell
$env:HADOOP_HOME = 'C:\hadoop-3.3.5'
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"
```

Both checks must return `True`. Alternatively, place those two files under
`%LOCALAPPDATA%\hadoop-3.3.5\bin`; the Spark factory checks that location by
default. Do not commit or distribute another computer's Hadoop binaries unless
you have permission to do so.

## Fresh setup

From the unzipped project root in PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r .\starter-code\requirements.txt
java -version
Set-Location .\starter-code
```

On macOS or Linux, use `python3 -m venv .venv`, activate it with
`source .venv/bin/activate`, install `requirements.txt`, then `cd starter-code`.
Windows Hadoop utilities are not needed on those systems.

## Run from a clean state

The repository includes generated Bronze and quarantine outputs as assignment
evidence. To reproduce the full pipeline from scratch, remove only those
generated directories while inside `starter-code/`:

```powershell
Remove-Item -LiteralPath .\bronze -Recurse -Force
Remove-Item -LiteralPath .\quarantine -Recurse -Force -ErrorAction SilentlyContinue
```

Then execute batches in order:

```powershell
python -m src.pipeline --batch batch_1
python -m src.pipeline --batch batch_2
python -m src.pipeline --batch batch_3
python -m src.pipeline --batch batch_4
python -m src.pipeline --batch batch_5
```

Expected customer row counts after the full sequence are Batch 1 = 80, Batch 3
= 3, Batch 4 = 3, and Batch 5 = 3 valid rows, for **89 Bronze customers**.
Batch 5 also creates **2 quarantined rows**. Re-running any completed batch is
safe: existing business keys are not added again, and the Batch 5 quarantine
path is overwritten for that batch rather than duplicated.

## View the Parquet outputs

Use the included inspection command to read the actual Parquet files with the
same Spark configuration as the pipeline:

```powershell
# Counts, schema, and ten sample rows for every Bronze table
python -m src.inspect_outputs --all

# A larger sample from one Bronze table
python -m src.inspect_outputs --entity customers --show 20

# Inspect the Batch 5 rejected rows and their rejection reason
python -m src.inspect_outputs --quarantine customers --batch batch_5 --show 20
```

The expected output folders are:

```text
bronze/bronze_customers/
bronze/bronze_products/
bronze/bronze_stores/
bronze/bronze_sales_transactions/
bronze/bronze_returns/
quarantine/customers/batch_5/
```

## Test the project

```powershell
python -m pytest -q
```

The test suite covers initial ingestion, metadata, idempotency, the two schema
drift events, and Batch 5 quarantine behavior. The completed project has nine
tests.

## Assignment evidence

Non-code validation evidence is stored in `outputs/artifacts/`, including the
Task 7 evidence report. The completed rubric workbook is
`outputs/artifacts/Ramya_Submission_Artifacts_Completed.xlsx` because the
original workbook was open and locked during its update.

## Troubleshooting

- **`winutils.exe not found` or Parquet write failure on Windows:** complete
  the Windows Hadoop setup above, then start a new PowerShell session and
  reactivate the virtual environment.
- **`JAVA_HOME`/Java error or Spark cannot start:** install a supported JDK,
  ensure `java -version` works, then set `JAVA_HOME` to the JDK directory if
  your Java installer did not set it.
- **`ModuleNotFoundError: src`:** run commands from `starter-code/`, not from
  the outer project root.
- **Source file not found:** preserve the supplied sibling layout:
  `starter-code/`, `data/`, and `.claude/` must all be at the same project-root
  level.
