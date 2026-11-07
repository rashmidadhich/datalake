# Sample Batches & Pipeline Architecture

These folders simulate later batch drops on top of the Batch 1 baseline (the five CSV files in `data/`). Each folder exercises exactly one property. Batches 2 and 3 use the three provided skills as-is; Batches 4 and 5 are the two required skill-driven learning activities (modify an existing skill, create a new skill) described in the assignment and Helper Guide.

## batch_2_duplicate_redelivery/

**Purpose: Idempotency.**

`sales_transactions_batch2_duplicate.csv` contains the exact same three rows (`O-00001`, `O-00002`, `O-00003`) already present in `data/sales_transactions.csv`. This simulates the same batch being delivered twice (e.g. a retried upload).

Expected result: after ingesting this batch, the bronze `sales_transactions` output must **not** contain duplicate rows for these order IDs — the row count must stay the same as after Batch 1.

## batch_3_schema_drift/

**Purpose: Schema drift.**

`customers_batch3_loyalty_tier.csv` contains new customer rows (`C-0006`, `C-0007`, `C-0008`) with one extra column not present in the Batch 1 schema: `loyalty_tier`.

Expected result: the pipeline must detect the new column, must not crash, and must produce a bronze `customers` output that includes `loyalty_tier` (null for rows ingested before Batch 3, populated for Batch 3 rows).

## batch4_schema_drift_skill_update/

**Purpose: Schema-drift skill modification (required learning activity — see the assignment's Task 6 and the Helper Guide's Activity 1).**

`customers_batch4_unexpected_column.csv` contains new customer rows (`C-0081`, `C-0082`, `C-0083`) with one further column not present in any prior batch: `marketing_opt_in`. Note this arrives *after* the schema has already been evolved once (Batch 3's `loyalty_tier`), so this batch tests a second, independent drift event on top of an already-drifted schema.

This batch is deliberately **not** solved by re-using `.claude/skills/schema_drift/SKILL.md` unmodified. Before implementing against it, you are expected to review that skill, identify a gap or limitation in its current guidance (e.g. it may not address a further column arriving on a schema that has already evolved, or preserving a previously-added column through a second drift event), and modify `.claude/skills/schema_drift/SKILL.md` yourself to close that gap. Do not implement this batch by simply repeating the Batch 3 logic without revisiting the skill.

Expected result: the pipeline must detect `marketing_opt_in` as a new column, must not crash, and the resulting bronze `customers` output must retain `loyalty_tier` (unaffected, still populated for Batch 3 rows and null elsewhere) while adding `marketing_opt_in` (populated for Batch 4 rows, null for all earlier rows).

## batch_5_data_quality/

**Purpose: New Data Quality Validation skill (required learning activity — see the assignment's Task 7 and the Helper Guide's Activity 2).**

`customers_batch5_null_customer_id.csv` contains 5 rows: 3 valid (`C-0084`, `C-0085`, `C-0086`) and 2 with `customer_id` left blank. `customer_id` is the documented business key for the `customers` entity, and no other column in this file is invalid — the scenario is intentionally limited to one data-quality problem.

No skill currently covers row-level content validation (as opposed to schema shape or duplicate detection). You are expected to design and create `.claude/skills/data_quality_validation/SKILL.md` yourself, then use it to guide your implementation of this batch.

Expected result: the pipeline must not crash and must not silently drop the invalid rows. The 3 valid rows must reach bronze `customers` as normal (with batch_id, source_file and ingestion_timestamp populated). The 2 rows with a null `customer_id` must be excluded from bronze and routed to a quarantine output/log that still records why they were rejected, and the ingestion evidence/log for this batch must show a non-zero count of rows quarantined for this reason.