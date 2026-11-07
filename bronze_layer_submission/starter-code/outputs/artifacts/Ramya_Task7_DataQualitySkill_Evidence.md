# Ramya - Task 7 Data Quality Validation Skill Evidence

Date: 2026-10-07

## Capability Gap and New Skill

The existing ingestion metadata, idempotency, and schema drift skills provide lineage, duplicate-key handling, and additive-column handling. They do not determine whether a structurally readable row has a required business key. I created `.claude/skills/data_quality_validation/SKILL.md` and its `README.md` to define reusable row-level validation and quarantine guidance.

The skill requires a required string business key to be treated as invalid when it is null, empty, or whitespace-only. It specifies that metadata must be added before validation, valid and invalid rows must be split without silently dropping either set, and every quarantined row must retain source values, lineage, and a non-empty `rejection_reason`.

## Implementation Guided by the Skill

`src/data_quality.py` implements the reusable `validate_data_quality(df, business_key)` split. `src/pipeline.py` invokes it after ingestion metadata and schema reconciliation, writes valid records through the normal idempotent Bronze path, and writes invalid records to `quarantine/<entity>/<batch_id>/` with overwrite mode so a retry does not accumulate duplicates for that batch.

## Generate Test Improve Regenerate Validate Cycle

1. The initial capability review identified that none of the supplied skills handled row-level required-key validation or a quarantine path.
2. I authored the new skill and README before completing the pipeline behavior.
3. The implementation was reviewed against the skill for blank/whitespace handling, lineage on rejected rows, rejection reasons, and job continuity.
4. Two focused tests were added: one validates the complete valid/quarantine split; the other validates Batch 5 against an already evolved customer schema.
5. After the persistent Batch 4 and Batch 5 runs, I read the Parquet outputs back and ran the focused test file again.

## Persistent Batch 5 Validation

Run sequence: Batch 1 and Batch 3 were already present; Batch 4 was ingested to add `marketing_opt_in`; then `python -m src.pipeline --batch batch_5` was run from `starter-code/`.

Batch 5 pipeline report: `customers: read 5, valid 3, wrote 3, skipped 0 existing customer_id values, quarantined 2`.

Read-back results:

- `bronze_customers` total: 89 rows (`batch_1=80`, `batch_3=3`, `batch_4=3`, `batch_5=3`).
- Batch 5 invalid customer IDs in Bronze: 0.
- `quarantine/customers/batch_5`: 2 rows.
- Quarantine reason: `customer_id is null or blank` on both rows.
- Quarantined rows with missing `batch_id`, `source_file`, or `ingestion_timestamp`: 0.
- The evolved Bronze customer schema retains `loyalty_tier` and `marketing_opt_in` while Batch 5 rows receive nulls for those fields because that file does not supply them.

## Automated Validation

Command: `python -m pytest tests\\test_data_quality.py -q`

Result: `2 passed in 19.05s`.

The tests confirm three valid rows are written, two invalid rows are quarantined with the stated reason and metadata, invalid rows are absent from Bronze, and the Batch 5 job succeeds against an already-evolved customer schema.
