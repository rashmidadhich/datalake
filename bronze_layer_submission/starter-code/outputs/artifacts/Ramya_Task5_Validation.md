# Ramya - Task 5 Testing and Validation Evidence

Date: 2026-10-07

## Environment

The tests were run from Command Prompt with the activated project venv. `sys.executable` resolved to `D:\projects\Broze Layer Project\.venv\Scripts\python.exe`.

A Windows Spark worker failure had previously occurred because worker startup resolved `python` to the Windows Store alias. `src/spark_session.py` now sets `PYSPARK_PYTHON` to the active driver interpreter (`sys.executable`) before creating Spark.

## Focused Metadata Test

Command: `python -m pytest tests\test_metadata.py -q`

Result: `2 passed in 45.54s`.

## Full Current Test Suite

Command: `python -m pytest -q`

Result: `6 passed, 3 skipped in 64.70s`.

The skipped tests are the Batch 4 schema-drift test, which remains deferred until Task 6 modifies the schema-drift skill, and the two Batch 5 data-quality tests, which remain deferred until Task 7 creates the new skill and implementation.

No test failures remained in this run.
