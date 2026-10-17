---
name: silver-pipeline-guardrails
description: >
  Cross-cutting safety checks every bronze-to-silver batch must satisfy,
  regardless of which transformation skill produced the rows: row-count
  conservation, a controlled quarantine reason vocabulary, sensitive/masked
  field passthrough integrity, idempotent re-runs, and non-destructive writes.
  Use when a pipeline writes to persistent output (Parquet files, a table) and
  more than one skill or batch contributes rows to the same target.
---

# Skill Name

Pipeline Guardrails (Cross-Cutting Write Safety)

## Purpose

Define the invariants that hold across *every* batch and *every* transformation
skill, so that no capability can quietly lose rows, invent a quarantine reason,
mutate a masked field, double-write on a re-run, or destroy previously written
output. These are the properties a reviewer checks first, because they are the
ones a single happy-path batch will not reveal.

## When to Use

- A pipeline writes rows to persistent output that accumulates across batches
  or runs.
- Multiple transformation skills (type casting, deduplication, SCD2, entity
  resolution) contribute rows to the same silver and quarantine targets.
- Rows may be dropped, rewritten, or quarantined, and correctness requires that
  every such movement is accounted for.

## Inputs

- **Per-batch counts**: rows read, rows written to silver, rows written to
  quarantine, and rows intentionally skipped (e.g. true duplicates).
- **The quarantine table**, including a machine-readable reason column.
- **The sensitive/masked columns** that must not change, and the key to compare
  them by.
- **The durable output locations** (silver and quarantine), before and after a
  run.

## Expected Outcome

After every batch, all five invariants hold:

1. **Row-count conservation** - `rows_in = rows_written_to_silver +
   rows_quarantined + rows_intentionally_skipped`. No row vanishes without
   landing in exactly one of those destinations, and none is counted twice.
2. **Controlled quarantine reasons** - every quarantine row carries a reason
   from a single, declared vocabulary; never free text, never null, never a
   one-off string invented inside a transformation.
3. **Masked-field passthrough integrity** - for every row, a sensitive/masked
   column is byte-identical from source to silver; it is never recomputed,
   unmasked, re-masked with a different scheme, or used as a join key.
4. **Idempotent re-runs** - processing the same batch twice leaves silver and
   quarantine identical (same counts, same content); the second run resolves to
   skips/no-ops, not duplicate inserts.
5. **Non-destructive writes** - writing a later batch never reduces or
   overwrites an earlier batch's committed rows; output is append-only or
   versioned, and any replacement of a row is a deliberate, accounted-for
   update rather than a blind overwrite of the whole table.

## Recommended Approach

1. **Centralise the invariants.** Implement them as reusable assertions that
   take plain counts/DataFrames, not as ad-hoc checks embedded in each
   transformation. Call them after every batch.
2. **Declare the reason vocabulary in one place** and reference it from every
   skill that quarantines. Adding a reason is a deliberate, documented change -
   never a literal typed at a call site.
3. **Assert conservation explicitly.** Count the four numbers per batch and
   fail the run if they do not reconcile; an unexplained difference is a bug,
   not a rounding error.
4. **Compare masked fields by key.** After a batch, join source and output on
   the business key and assert the masked column is unchanged; treat any
   difference as a hard failure.
5. **Design writes for re-runs.** Read the target, compute the intended result,
   and write a result that is identical when recomputed from the same inputs.
   Prefer read-modify-write of the whole logical table (or a table format with
   atomic row updates) over blind `append`, so a re-run cannot duplicate rows.
6. **Never overwrite destructively.** When closing/updating a row, rebuild the
   table from existing rows plus the change; do not truncate history or drop
   rows that are not part of the current batch.

Illustrative shape only (not a full implementation):

```python
assert rows_in == rows_silver + rows_quarantine + rows_skipped
assert quarantine.select(reason).distinct().subtract(allowed_reasons).isEmpty()
assert masked_mismatch_count(before, after, key, masked_col) == 0
```

## Validation

- **Prove each check can fail.** Deliberately break one rule in a scratch copy
  and confirm its assertion trips; a test that cannot fail is not a guardrail.
- Reconcile conservation for every batch that moves rows to more than one
  destination.
- Scan the whole quarantine output (not just the latest batch) for any reason
  outside the vocabulary.
- Run a batch, then run it again, and diff the two outputs.
- Write a later batch and assert earlier batches' row counts and content are
  unchanged.

## Edge Cases

- A batch legitimately adds *zero* rows (pure retry) - conservation must still
  hold with `rows_written = 0`.
- An empty or absent target on the first run.
- A row counted as skipped must not also be counted as quarantined.
- A masked value that is a placeholder repeated across rows - still never used
  as a join key.
- Partial-failure runs: a run that fails mid-way must not leave the target in a
  state that violates conservation once retried.

## Common Mistakes

- Silently dropping malformed rows instead of quarantining them, breaking
  conservation.
- Using free-text or per-batch reason strings instead of the declared
  vocabulary.
- Re-running a batch with `mode("append")` and no existing-data check,
  duplicating rows.
- Overwriting, truncating, or rebuilding the target from only the current
  batch, discarding earlier batches.
- Normalising or "cleaning" a masked field as a side effect of another
  transformation.

## Reusability

Applies to any bronze-to-silver pipeline that writes persistent output across
multiple batches or runs and carries at least one sensitive field. Substitute
the output locations, the key, the masked columns, and the reason vocabulary;
the five invariants are otherwise independent of entity, skill, and storage
format.
