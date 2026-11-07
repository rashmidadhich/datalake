# pytest report

Saved 2026-10-17 23:26. Command:

```powershell
cmd /c ".venv\Scripts\python.exe" -m pytest -v > outputs\artifacts\pytest_report.md 2>&1
```

**Result: 26 passed in 508.88s (0:08:28)**

The suite starts one SparkSession and runs the full Batch 1-5 pipeline once (plus a Batch 2-5 replay for the idempotency check); on a 12-core Windows machine with the default `TRB_SPARK_MASTER=local[2]` a run takes ~8-12 minutes.

## Console output

~~~text
============================= test session starts =============================
platform win32 -- Python 3.11.9, pytest-8.2.0, pluggy-1.7.0 -- D:\projects\Silver Layer Project\.venv\Scripts\python.exe
rootdir: D:\projects\Silver Layer Project
configfile: pytest.ini
testpaths: tests
collecting ... collected 26 items

tests/test_dedup.py::test_true_duplicate_is_skipped_not_inserted PASSED  [  3%]
tests/test_dedup.py::test_mutable_only_change_is_an_overwrite_not_a_duplicate_or_conflict PASSED [  7%]
tests/test_dedup.py::test_conflicting_key_is_quarantined_not_inserted_or_overwritten PASSED [ 11%]
tests/test_dedup.py::test_conflicting_key_quarantine_reason_is_correct PASSED [ 15%]
tests/test_dedup.py::test_batch2_actual_outcome PASSED                   [ 19%]
tests/test_entity_resolution.py::test_reused_key_becomes_a_distinct_identity PASSED [ 23%]
tests/test_entity_resolution.py::test_new_key_keeps_account_id_as_identity PASSED [ 26%]
tests/test_entity_resolution.py::test_batch5_three_reused_keys_become_three_identities PASSED [ 30%]
tests/test_entity_resolution.py::test_masked_email_is_never_exposed_or_unmasked PASSED [ 34%]
tests/test_guardrails.py::test_conservation_passes_when_every_row_is_accounted_for PASSED [ 38%]
tests/test_guardrails.py::test_conservation_fails_when_a_row_vanishes PASSED [ 42%]
tests/test_guardrails.py::test_every_quarantine_reason_is_from_the_vocabulary PASSED [ 46%]
tests/test_guardrails.py::test_vocabulary_check_can_fail PASSED          [ 50%]
tests/test_guardrails.py::test_bronze_masked_emails_are_unchanged_in_silver PASSED [ 53%]
tests/test_guardrails.py::test_masked_integrity_check_can_fail PASSED    [ 57%]
tests/test_guardrails.py::test_replaying_batches_2_to_5_changes_nothing PASSED [ 61%]
tests/test_guardrails.py::test_earlier_batches_are_not_destroyed PASSED  [ 65%]
tests/test_scd2.py::test_normal_change_closes_and_appends PASSED         [ 69%]
tests/test_scd2.py::test_exactly_one_open_row_per_entity PASSED          [ 73%]
tests/test_scd2.py::test_out_of_order_effective_date_is_quarantined_not_inverted PASSED [ 76%]
tests/test_scd2.py::test_batch4_applies_four_and_quarantines_the_out_of_order_one PASSED [ 80%]
tests/test_scd2.py::test_plan_as_of_date_is_queryable PASSED             [ 84%]
tests/test_type_casting.py::test_mm_dd_yyyy_is_cast_and_plan_id_is_normalized PASSED [ 88%]
tests/test_type_casting.py::test_format_drift_uses_the_matching_known_pattern PASSED [ 92%]
tests/test_type_casting.py::test_unrecognized_format_is_quarantined_not_guessed PASSED [ 96%]
tests/test_type_casting.py::test_batch_3_gains_three_rows_and_quarantines_one PASSED [100%]

- generated xml file: D:\projects\Silver Layer Project\outputs\artifacts\pytest_junit.xml -
======================= 26 passed in 508.88s (0:08:28) ========================
SUCCESS: The process with PID 17980 (child process of PID 20452) has been terminated.
SUCCESS: The process with PID 20452 (child process of PID 10272) has been terminated.
SUCCESS: The process with PID 10272 (child process of PID 9688) has been terminated.
~~~
