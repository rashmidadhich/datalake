---
name: idempotent-batch-ingestion
description: >
  Guide a batch ingestion step so that redelivering a batch already
  processed does not create duplicate output records. Use whenever a batch
  (same file/date/extract) could plausibly be delivered or triggered more
  than once and a business key can be identified in the source data.
---

# Skill Name

Idempotent Batch Ingestion (Duplicate Batch Redelivery)

## Purpose

Guide the design of a batch ingestion step so that redelivering a batch that has already been processed does not create duplicate records in the output.

## Problem This Skill Solves

Source systems and delivery mechanisms occasionally send the same batch more than once (a retry after a timeout, a manual re-run, an accidental re-upload). A plain append-only write has no memory of what it already wrote, so processing the same batch twice silently doubles the affected records. This skill defines how to recognize that situation and avoid it.

## When to Use

- A batch ingestion job writes to a table or file that persists across runs.
- The same batch (same file, same date, same extract) could plausibly be delivered or triggered more than once.
- The requirement is "re-running this must not change the result," not "guarantee delivery exactly once across a distributed system."

Do not reach for exactly-once semantics, distributed locking, or transactional checkpointing here — those solve a different, broader problem than a single batch being redelivered, and are out of scope for this skill.

## Inputs / Context Required

Before applying this skill, establish:

- **The business key**: the column or combination of columns that uniquely identifies a real-world record for this entity (e.g. an order id, a transaction id). This must come from the data itself, never a value generated during ingestion.
- **The target location**: where previously ingested data for this entity is stored, so it can be checked before writing.
- **The incoming batch**: the new data to be ingested, in the same shape as previously ingested batches.

## Expected Outcome

Processing the same batch a second time leaves the target table's row count and content unchanged from after the first processing. Processing a batch containing a mix of already-seen and new records adds only the new records.

## Core Guidance

**Understand**: The problem is duplication at the record level, keyed by a business identifier — not duplication of an entire file or job execution. Design around "has this specific record already landed?" rather than "has this file run before?", because a redelivered batch may not be byte-identical to the original (e.g. it might be re-exported) even though its rows represent the same real-world events.

**Reason**: Before writing code, decide:
- What is the business key for this entity, and is it actually stable and unique in the source data?
- Does the target table already exist, or is this the first load?
- If a record's business key already exists in the target, should the existing record be kept as-is, or should it be updated with the incoming version? (For a pure redelivery scenario, keeping the existing record is normally correct — the incoming row is expected to be identical or a re-send, not a legitimate update.)

## Decision Rules

- If the target does not yet exist or is empty, treat the entire incoming batch as new.
- If a business key from the incoming batch already exists in the target, do not insert another row for it.
- If a business key does not exist in the target, insert it.
- Never use a surrogate value generated during ingestion (e.g. an auto-incrementing id or a value derived from wall-clock time) as part of the duplicate check — it will differ between the original and redelivered batch even when the underlying record is identical.

## Implementation Guidance

1. Load the incoming batch.
2. Read the set of business keys currently present in the target (if the target exists).
3. Separate incoming rows into "already present" and "new" using the business key.
4. Write only the "new" rows to the target, using an append-style write.
5. Report how many rows were read versus how many were actually new — this makes idempotent behavior observable, not just assumed.

A minimal illustration of the core check (not a full solution):

```python
new_rows_df = incoming_df.join(existing_keys_df, on=business_key, how="left_anti")
```

## Validation

- Process a batch once; record the resulting row count in the target.
- Process the exact same batch again; assert the row count is unchanged.
- Process a batch that mixes previously-seen and new business keys; assert only the new keys' rows were added.

## Common Failure Scenarios

- The redelivered batch is not byte-identical to the original (different row order, re-quoted values, re-exported timestamps) — the duplicate check must key on business identity, not file/content equality.
- The target table does not exist yet on the very first run — the check must handle "no prior data" as "everything is new," not as an error.
- A batch is empty — this should still succeed and add zero rows, not be treated as a failure.

## Common Mistakes

- Using `mode("append")` with no check against existing data.
- Using the presence of the source file, rather than the business key, to decide whether data was already processed.
- Comparing entire rows (including any ingestion metadata columns) instead of the business key, causing every redelivery to look "new" because metadata like a timestamp differs.

## Reusability

This skill applies to any batch or micro-batch ingestion step, for any entity, as long as a business key can be identified in the source data. It is independent of storage format (Parquet, Delta, a database table) and independent of any specific dataset — apply it by substituting the business key and target location for the entity at hand.
