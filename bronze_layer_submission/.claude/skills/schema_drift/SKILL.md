---
name: schema-drift-handling
description: >
  Detect and safely absorb a new column appearing in a later ingestion
  batch without breaking the pipeline or losing existing data. Use when an
  entity is ingested repeatedly over time and its schema is not
  contractually frozen. Does not cover renames, removals, or type changes.
---

# Skill Name

Schema Drift Handling (New Column in a Later Batch)

## Purpose

Guide the design of an ingestion step so that a new column appearing in a later batch is detected, accepted, and reflected in the output, without breaking the pipeline or losing existing data.

## Problem This Skill Solves

Upstream systems evolve. A source that previously sent N columns may start sending N+1 columns without warning. A pipeline written against a fixed, hardcoded schema will either crash or silently drop the new information. This skill defines how to notice a new column and absorb it safely.

## When to Use

- An entity is ingested repeatedly over time (multiple batches), and its schema is not contractually frozen.
- The specific change to prepare for is an **added column** in a later batch — the earlier batches did not have it.

This skill deliberately does not cover column renames, column removal, data type changes, or multiple simultaneous schema changes. Treat `loyalty_tier` in this assignment as one concrete example of "a new column appears" — the same guidance applies to any newly introduced column, whatever it is called or whichever entity it appears on.

## Inputs / Context Required

- **The incoming batch's schema** (its column names, and optionally types).
- **The last known schema** for this entity — either read from the existing output table, or a schema snapshot saved after the previous successful run.
- **The entity name and batch identifier**, so a detected change can be logged meaningfully.

## Expected Outcome

The job completes successfully on the batch that introduces the new column. The output for this entity includes the new column: populated for rows from the batch(es) that had it, and null for rows ingested before it existed. The detection of the new column is visible (logged), not silent.

## Core Guidance

**Understand**: Schema drift here is a comparison problem, not a parsing problem — you are not trying to guess a schema from scratch, you are comparing "what came in" against "what we expected/had before" and reacting to the difference.

**Reason**: Before implementing, decide:
- Where does "the last known schema" come from — the existing output, or a stored snapshot? Either is valid; a stored snapshot is more robust if the output can be rebuilt from scratch.
- What should happen to historical rows that predate the new column — they should gain the column with a null value, not be excluded or recomputed.
- Should the job simply continue silently, or should the new column be logged as an event? For traceability, it should be logged even though it is not treated as an error.

## Decision Rules

- If a column exists in the incoming batch but not in the last known schema, treat it as **new** — log it and include it.
- If a column exists in the last known schema but not in the incoming batch, that is a different scenario (a missing column) and is out of scope for this skill unless the assignment also requires it — do not silently invent behavior for it here.
- A newly introduced column must never cause the batch write to fail; the job must complete and the column must be present in the output.
- Rows that existed before the new column was introduced must show null for that column, not be dropped or backfilled with a guessed value.
- For a later additive drift, compare against the entity's current, fully evolved output schema, including columns added by earlier batches. Preserve those earlier columns and their existing values while adding the newly detected column.
- A previously added column may be absent from a later incoming file; retain it in the combined output, preserving existing values and using null only for incoming rows that do not supply it.

## Implementation Guidance

1. Read the incoming batch and determine its column list.
2. Determine the last known schema (from the existing output, or a saved snapshot).
3. Compute the columns present in the incoming batch but absent from the last known schema.
4. If any such columns exist, log the entity, batch identifier, and the new column name(s).
5. Combine old and new data in a way that fills in null for any column one side doesn't have, rather than requiring both sides to match exactly before combining.
6. After writing, persist/update the "last known schema" so the next run can detect further drift correctly.
7. For every later drift, use that persisted full schema as the comparison baseline. Add only the newly detected field, retain previously evolved fields and values, and backfill null only where a row predates a field.

A minimal illustration of the core idea (not a full solution):

```python
combined_df = existing_df.unionByName(incoming_df, allowMissingColumns=True)
```

## Validation

- Ingest a baseline batch, then a batch that adds one new column; assert the job completes without error.
- Assert the new column is present in the output schema.
- Assert the new column is null for rows from the baseline batch and populated for rows from the batch that introduced it.
- Assert the detection was logged (entity, batch id, column name) somewhere reviewable.
- After an earlier additive drift succeeds, process a later batch with one different new column. Assert the earlier added column and its values remain unchanged, the later column is present, and it is null for rows from before its introduction.

## Common Failure Scenarios

- The "last known schema" is never persisted or read back, so drift can never actually be detected on the next run (every run looks like the first run).
- The combine step requires exact schema equality between old and new data, which raises an error the moment a new column appears.
- The new column is accepted into the current batch but historical rows are rewritten incorrectly or dropped in the process.
- A later drift is compared with the original baseline instead of the current evolved schema, causing a previously added column or its values to be lost.

## Common Mistakes

- Hardcoding the full expected column list so that any addition is treated as invalid input rather than as drift to absorb.
- Detecting the new column but not logging it, so the change is invisible to anyone reviewing the run.
- Assuming the new column's data type in advance instead of reading what actually arrived.

## Reusability

This skill applies to any entity, in any assignment or project, where the schema can grow additively over time. It is independent of the specific column name or dataset — apply it by substituting the entity, its last-known-schema source, and its logging destination.
