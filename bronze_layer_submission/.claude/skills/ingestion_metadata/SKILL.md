---
name: ingestion-metadata-tagging
description: >
  Attach a batch identifier, source file name, and ingestion timestamp to
  every ingested record for basic traceability. Use for any batch
  ingestion step whose output rows might later need to be traced back to
  their originating run and file; apply alongside the idempotency and
  schema-drift skills.
---

# Skill Name

Ingestion Metadata Tagging (Basic Traceability)

## Purpose

Guide the addition of a small, fixed set of metadata fields to every ingested record so any row can be traced back to the batch and source file it came from.

## Problem This Skill Solves

Once a row is written to an output table, there is normally no way to answer "which file did this come from?" or "which run produced this?" unless that information was captured at ingestion time. Without it, debugging a bad batch, re-running a specific batch, or explaining an anomaly becomes guesswork. This skill defines the minimum metadata needed for basic traceability.

## When to Use

- Any batch ingestion step where rows are written to a persistent output and might later need to be traced back to their origin.
- Use this together with the idempotency and schema-drift skills — metadata columns are typically added before those other checks run, since they help make drift and duplicate-handling events attributable to a specific batch.

## Inputs / Context Required

- **A batch identifier**: however the caller identifies a run (a date string, a sequence number, a label) — this is supplied by whatever triggers ingestion, not invented inside the ingestion logic.
- **The source file name**: the name of the file (or equivalent source identifier) the batch was read from.
- **A way to capture the current time** at the moment of loading.

## Expected Outcome

Every record in the output carries three additional fields — a batch identifier, the source file name, and an ingestion timestamp — without altering or removing any original source column.

## Core Guidance

**Understand**: The goal is traceability, not a full audit framework. Three fields are sufficient to answer "which batch, which file, and roughly when" for any given row; do not expand this into a larger audit/control-table design unless the assignment or project explicitly asks for one.

**Reason**: Before implementing, decide:
- Where does the batch identifier come from? It should be passed into the ingestion step as a parameter, not derived from the data itself, so it stays consistent even if the data changes.
- Where does the source file name come from? If reading multiple files, prefer capturing the actual file each row was read from over a single value for the whole run.
- Should metadata be added before or after other transformations? Add it as early as possible — immediately after reading — so later steps (drift detection, dedup logging) can reference it.

## Decision Rules

- All three fields are always populated — none should be left null for a successfully ingested row.
- The batch identifier and source file name are supplied values, not computed from business columns.
- The ingestion timestamp reflects when the row was loaded, not any business-meaningful date in the source data.
- Metadata fields are additive: original source columns must remain unchanged and present alongside them.

## Implementation Guidance

1. Determine the batch identifier and source file name for the current ingestion run before reading the data (or while reading it, if reading per-file).
2. Immediately after reading a source file into a DataFrame, attach the three metadata fields.
3. Carry these fields through any subsequent transformation steps unchanged.
4. Write the enriched DataFrame to the output.

A minimal illustration of the core idea (not a full solution):

```python
tagged_df = (
    raw_df.withColumn("source_file", F.lit(source_file))
          .withColumn("batch_id", F.lit(batch_id))
          .withColumn("ingestion_timestamp", F.current_timestamp())
)
```

## Validation

- After ingesting a batch, confirm all three metadata columns are non-null for every row.
- Confirm `source_file` matches the actual file each row was read from.
- Confirm `batch_id` matches the batch that was run, and differs correctly across separate batches.
- Confirm original source columns are unchanged and none were overwritten by a metadata column of the same name.

## Common Failure Scenarios

- Metadata is added after a join or filter that drops rows, making it unclear which rows lost their tagging versus which were legitimately filtered.
- The batch identifier is hardcoded instead of parameterized, so every batch looks identical in the output.
- Reading multiple files at once without capturing the per-row source file, collapsing traceability to "this run" instead of "this file."

## Common Mistakes

- Deriving the ingestion timestamp from a business date column instead of the actual load time.
- Naming a metadata column the same as an existing source column, silently overwriting real data.
- Adding metadata only to some entities/files and not others, breaking consistency across the pipeline.

## Reusability

This skill applies to any ingestion pipeline, for any entity or dataset, regardless of storage format. It is independent of the specific field names chosen for a given project, as long as the same three concepts — a run/batch identifier, a source identifier, and a load timestamp — are captured consistently.
