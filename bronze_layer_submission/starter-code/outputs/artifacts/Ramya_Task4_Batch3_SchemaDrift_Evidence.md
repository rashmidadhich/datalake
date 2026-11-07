# Ramya - Task 4 Batch 3 Schema Drift Evidence

Date: 2026-10-07

## Skill Used

Used `.claude/skills/schema_drift/SKILL.md` as provided. For Batch 3, the skill calls for detecting the incoming additive field, logging the entity/batch/field, retaining old rows with null for the new field, and preserving supplied values for Batch 3 rows.

## Persistent Bronze Baseline Before Batch 3

The pre-run Command Prompt inspection showed:

- Customer row count: 80
- `loyalty_tier` was absent from the schema
- The displayed first ten records had `batch_id=batch_1` and `source_file=customers.csv`

## Automated Validation

Command: `python -m pytest tests\test_schema_drift.py -q`

User-reported result: `1 passed, 1 skipped in 14.74s`.

The passing test runs Batch 1 and Batch 3 in a temporary Bronze directory, verifies the new `loyalty_tier` column, null values for Batch 1 rows, supplied values for Batch 3 rows, and a visible drift log. The skipped test covers Batch 4 and remains deferred until Task 6 modifies the schema-drift skill.

## Persistent Batch 3 Run

After running `python -m src.pipeline --batch batch_3`, the persistent read-back showed:

- Customer row count: 83
- `loyalty_tier` present in the schema
- Null `loyalty_tier` values: 80
- Populated `loyalty_tier` values: 3
- The first ten displayed customers remained Batch 1 rows with null `loyalty_tier`

The Command Prompt read-back completed with the expected values. Spark then printed a `ShutdownHookManager` warning while deleting a temporary directory; it occurred after the query output and did not affect the Parquet results.
