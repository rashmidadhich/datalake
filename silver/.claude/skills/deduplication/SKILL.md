---
name: silver-deduplication
description: >
  Guide a bronze-to-silver transformation step so that a redelivered batch does
  not create duplicate records, and so that a repeated business key is routed
  to the correct one of three outcomes: quarantine, overwrite-as-update, or
  retain-unchanged. Use whenever a batch could plausibly be delivered or
  triggered more than once and a business key can be identified in the
  incoming data.
---

# Skill Name

Deduplication (Retried Batch Delivery & Legitimate Updates)

## Purpose

Guide the design of a silver-layer write so that a repeated business key is
never blindly inserted, blindly overwritten, or blindly dropped. Every
repeated key must resolve to exactly one of three outcomes: quarantine,
overwrite (update), or retain unchanged.

## Problem This Skill Solves

Retries, timeouts, and manual re-runs occasionally cause the same batch to be
delivered more than once. But not every row sharing an existing business key
is a bad retry — some legitimately update the record's business state (e.g. an
invoice's payment status progressing from Pending to Paid). A plain
append-only write doubles records on retry; a plain "skip everything with an
existing key" write silently discards real updates; a plain "always overwrite"
write lets corrupted data replace good data. This skill defines how to tell
the three cases apart.

## When to Use

- A silver-layer write persists across runs.
- The same batch (same file, same date, same extract) could plausibly be
  delivered more than once.
- The entity may also legitimately receive updates to specific fields after
  its initial load (e.g. a status or amount field), without needing full
  point-in-time history.
- The requirement is "re-running this must not change the result, and a real
  update must not be lost or mistaken for a duplicate."

## Inputs / Context Required

- **The business key**: the column that uniquely identifies a real-world record
  for this entity (e.g. `invoice_id`). Must come from the data itself.
- **Identity-defining (immutable) columns**: columns that describe *what* the
  record is and should never change once written for a given business key
  (e.g. `account_id`, `invoice_date`, `currency` for an invoice).
- **Mutable / business-state columns**: columns that are expected to change
  over the record's lifecycle (e.g. `payment_status`, `amount_due` for an
  invoice).
- **The target silver table**: read before writing, so incoming rows can be
  checked against it.
- **The incoming batch**: the new data to be ingested.

## Expected Outcome

Every incoming row whose business key already exists in the target resolves
to exactly one of:

1. **Quarantine** — a row where an identity-defining column differs from the
   existing row is quarantined with a controlled reason code (e.g.
   `conflicting_key`), since this indicates corrupted or misattributed data
   rather than a real update. A byte-identical duplicate is never
   re-quarantined; it resolves to "retain unchanged" (see Decision Rules).
2. **Overwrite (update, no history)** — a row where only mutable /
   business-state columns differ from the existing row is applied to silver by
   overwriting those columns on the existing record. No `valid_from`/`valid_to`
   history is kept for this update; the prior value is not retained. (If the
   entity needs point-in-time history of the change, use the SCD2 skill
   instead — this skill only covers overwrite-without-history.)
3. **Retain unchanged** — a row that is byte-identical to the existing row on
   every non-key column is a true duplicate: no write occurs, and the existing
   row is left exactly as it is.

## Core Guidance

**Understand**: the problem is not binary ("new vs. duplicate"). A repeated
business key can mean three different things, and code must distinguish them
by which *columns* differ, not merely whether *any* column differs.

**Reason**: before writing code, decide for the entity at hand:
- What is the business key, and is it stable and unique?
- Which columns are identity-defining (must never change for that key)?
- Which columns are mutable business state (expected to change over time)?
- Does the target table already exist, or is this the first load?

## Decision Rules

- If the target does not yet exist or is empty, treat the entire incoming
  batch as new.
- If a business key does not exist in the target, insert it.
- If a business key exists in the target and the incoming row is identical to
  the existing row on every non-key column, **retain unchanged**: skip the
  write, do not quarantine, do not duplicate.
- If a business key exists in the target and the incoming row differs from the
  existing row **only** on mutable/business-state columns, **overwrite**: apply
  the new values for those columns to the existing row in silver. Do not
  preserve the prior value as a separate history row under this skill.
- If a business key exists in the target and the incoming row differs from the
  existing row on **any** identity-defining column, **quarantine** with a
  reason (e.g. `conflicting_key`) — do not insert, do not overwrite, do not
  drop silently.
- If a row differs on both an identity-defining column and a mutable column,
  quarantine takes precedence — an identity mismatch is not treated as a
  partial update.
- Never use a surrogate value generated during ingestion (e.g. an
  auto-incrementing id) as part of the duplicate/identity check.
- **Boundary and ordering with adjacent skills — check this before
  quarantining.** A repeated business key is not automatically this skill's
  concern. Classify the repeated key first, in this order:
  1. If the incoming row carries an effective date for a tracked attribute
     (e.g. an `effective_date` alongside a `plan_id`), it is a
     **tracked-attribute change**, not a retry — hand it to the SCD2 history
     skill, which closes the prior row and appends a new one.
  2. Else, if the incoming row disagrees with the existing identity on the
     identity-defining attributes (e.g. a customer name, signup date, or
     source system that cannot belong to the same real-world entity), it may
     be a **different physical entity reusing the key** — hand it to the
     entity-resolution skill, which assigns a distinct surrogate identity.
  3. Only rows that match neither case are deduplicated by this skill.
  Getting this order wrong is worse than not deduplicating: it quarantines
  legitimate updates, plan changes, and new customers as if they were
  corrupt data.
- Quarantine reason codes must come from the pipeline's controlled vocabulary
  (owned by the pipeline-guardrails skill), never free text, and never a
  value invented ad hoc for one batch.

## Implementation Guidance

1. Load the incoming batch.
2. Join incoming rows to the target table on the business key.
3. For matched rows, compare identity-defining columns and mutable columns
   separately:
   - No columns differ → retain unchanged (no-op).
   - Only mutable columns differ → overwrite those columns on the target row.
   - Any identity-defining column differs → route to quarantine with a reason.
4. For unmatched keys, insert as new.
5. Report counts for each of the four paths: new, retained-unchanged,
   overwritten, quarantined.

```python
matched = incoming_df.join(existing_df, on=business_key, how="inner")

identity_diff = matched.filter(
    reduce(lambda a, b: a | b, [F.col(f"in_{c}") != F.col(f"ex_{c}") for c in identity_cols])
)
mutable_only_diff = matched.join(identity_diff, on=business_key, how="left_anti").filter(
    reduce(lambda a, b: a | b, [F.col(f"in_{c}") != F.col(f"ex_{c}") for c in mutable_cols])
)
unchanged = matched.join(identity_diff, on=business_key, how="left_anti") \
                    .join(mutable_only_diff, on=business_key, how="left_anti")
new_rows = incoming_df.join(existing_df, on=business_key, how="left_anti")
```

## Validation

- Process a batch once; record the resulting row count and the counts for
  each outcome.
- Process the exact same batch again; assert the row count is unchanged and
  every row resolves to "retain unchanged."
- Process a batch containing a row that changes only a mutable column; assert
  the existing row's value is overwritten, no new row is added, and the row
  is not quarantined.
- Process a batch containing a row where an identity-defining column differs;
  assert it is quarantined with a reason, the target row count is unaffected,
  and the target row's original values are untouched.

## Common Failure Scenarios

- The redelivered batch is not byte-identical (different row order, re-quoted
  values) — key on business identity and column-level comparison, not
  file/content equality.
- The target table does not exist yet on the first run.
- An empty batch should succeed and add zero rows.
- A row changes both a mutable and an identity-defining column — this must
  still be quarantined, not partially overwritten.
- The repeated key actually represents a tracked-attribute change or a reused
  key belonging to a different real-world entity — applying this skill's
  quarantine rule instead of handing the row to the SCD2 or entity-resolution
  skill corrupts history or blocks a real customer.

## Common Mistakes

- Treating every "key exists, some value differs" row as a conflict and
  quarantining legitimate status/amount updates — this silently blocks real
  business updates from ever reaching silver.
- Treating every "key exists, some value differs" row as an update and
  overwriting rows where identity-defining columns differ — this lets
  corrupted or misattributed data replace correct data.
- Using `mode("append")` with no check against existing data.
- Comparing rows including ingestion-metadata columns, causing every
  redelivery to look "new" or "changed" because a timestamp differs.

## Reusability

Applies to any batch write, for any entity, as long as a business key can be
identified and the entity's columns can be split into identity-defining vs.
mutable/business-state. Independent of storage format. Distinct from the SCD2
skill: use this skill when updates should simply overwrite in place with no
retained history; use SCD2 when point-in-time history of the change must
remain queryable.

---
