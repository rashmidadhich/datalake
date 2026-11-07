# Ramya - Task 1 and Task 2 Validation

Date: 2026-10-06

## Windows Spark Write Setup

PySpark 3.5.1 includes Hadoop 3.3.4 client jars. On this Windows machine, Spark started but Parquet writes initially failed because Hadoop could not find `winutils.exe`; after adding that executable, the write reached Hadoop's native `NativeIO.access0` call and required `hadoop.dll` as well.

The required Windows utilities are stored outside the project at `%LOCALAPPDATA%\hadoop-3.3.5\bin`. They were fetched over certificate-verified HTTPS from the pinned `cdarlint/winutils` commit `b780884cca4f524e05b4b0000e9e2ba4119ca52e` and verified against the GitHub Git-blob hashes:

- `winutils.exe`: 112640 bytes; blob SHA-1 `4fd286dd74c2d37d73ccaf3c83007afa99f5586b`
- `hadoop.dll`: 84992 bytes; blob SHA-1 `60fb8b1336de7ec890e594c6910678e7be35786f`

These are Hadoop 3.3.5 utilities, the closest version available in that repository to the bundled Hadoop 3.3.4 runtime. The local Parquet write/read probe completed successfully and read back 3 rows. The Spark factory configures `HADOOP_HOME` before creating the session and accepts an explicit `HADOOP_HOME` override.

## Batch 1 Outputs

The Batch 1 pipeline wrote one Parquet output per entity under `starter-code/bronze/`:

| Entity | Expected rows | Read-back rows | Metadata nulls | batch_id | source_file |
|---|---:|---:|---:|---|---|
| customers | 80 | 80 | 0 | batch_1 | customers.csv |
| products | 10 | 10 | 0 | batch_1 | products.csv |
| stores | 15 | 15 | 0 | batch_1 | stores.csv |
| sales_transactions | 360 | 360 | 0 | batch_1 | sales_transactions.csv |
| returns | 46 | 46 | 0 | batch_1 | returns.csv |

## Automated Tests

Command: `pytest -q tests/test_ingestion.py tests/test_metadata.py`

Result: `3 passed in 126.17s`.

The tests cover the configured Parquet outputs, source-to-Bronze row-count parity, source-column preservation, populated metadata, and rejection of metadata-name collisions. The rubric's Batch 1 baseline counts are recorded above from the read-back validation. One Spark shutdown cleanup warning about an already-removed temporary directory appeared after a successful run; it did not affect the Parquet outputs or test exit code.
