# Ramya - Task 3 Idempotency Evidence

Date: 2026-10-06

## Skill and Decision

The idempotency skill was applied as provided. The duplicate check uses the entity business key from configuration. For sales transactions, the key is `order_id`. Existing Bronze rows are retained; incoming rows are anti-joined against existing keys and only unseen rows are appended. Batch/file identity and ingestion metadata are not used to decide whether a transaction is a duplicate.

## Validation

Command: `pytest tests/test_ingestion.py tests/test_metadata.py tests/test_idempotency.py -q`

Result: `5 passed in 142.11s`.

`test_batch_1_rerun_leaves_row_count_unchanged` loads Batch 1 into an isolated temporary Bronze location, derives source row counts from the configured CSVs, reruns Batch 1, asserts zero rows are written on the second run, and verifies persisted counts remain unchanged with no duplicate business keys.

`test_batch_2_redelivered_rows_are_not_duplicated` loads Batch 1, reads its persisted `order_id` values, confirms the configured Batch 2 keys are already present, runs Batch 2, asserts zero sales transaction rows are written, and checks the output count is unchanged and business keys remain unique.

The tests use temporary output locations so results do not depend on the pre-existing local Bronze folder. Batch 2 was also run against the persistent `starter-code/bronze/bronze_sales_transactions` output: it read 3 rows, wrote 0, and skipped 3 existing `order_id` values. Read-back validation before and after showed 360 rows, 360 distinct `order_id` values, and 0 duplicate keys; `O-00001`, `O-00002`, and `O-00003` retained their original `batch_1` metadata.
