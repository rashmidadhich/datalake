---
name: silver-scd2-history
description: >
  Track a tracked attribute's changes over time (e.g. a subscription plan) so
  that historical state remains queryable, rather than being overwritten on
  change. Use whenever an entity's attribute can change during its lifetime and
  the pipeline must answer "what was true as of date X."
---

# Skill Name

SCD2 History Tracking (Slowly Changing Dimension, Type 2)

## Purpose

Guide the design of a silver-layer write so that a change to a tracked
attribute (e.g. a customer's subscription plan) is recorded as history, rather
than overwriting the prior value.

## Problem This Skill Solves

A plain upsert-in-place loses the previous value the moment an attribute
changes - there is no way to answer "what plan was this account on as of last
month" afterward. This skill defines how to keep both the old and new values
queryable by time.

## When to Use

- An entity has at least one attribute (e.g. `plan_id`) that can legitimately
  change during the entity's lifetime.
- The requirement is to answer point-in-time questions about that attribute's
  value, not just its current value.

This skill assumes exactly one change event per entity per batch. It does not
cover multiple simultaneous attribute changes on the same row.

## Inputs / Context Required

- **The tracked attribute(s)**: which column(s) must have their history
  preserved (e.g. `plan_id`).
- **An effective date** for the change, supplied by the incoming event (e.g.
  `effective_date`).
- **The target silver table**, including its current `valid_from`, `valid_to`,
  and `is_current` columns for the entity being changed.

## Expected Outcome

For any entity that has undergone a tracked change, querying its attribute as
of a date before the change returns the old value, and querying it on or after
the change returns the new value. At most one row per entity has
`is_current = true` at any time.

## Core Guidance

**Understand**: this is a close-and-append operation, not an update-in-place.
The prior "current" row is closed out (its `valid_to` is set, `is_current`
becomes false) and a new row is appended representing the new value.

**Reason**: before implementing, decide:
- Which row currently represents the entity's "current" state?
- What is the effective date of the incoming change, and does it fall after
  the current row's `valid_from`?
- What are `valid_from`/`valid_to` for the new row, and for the row being
  closed?

## Decision Rules

- Exactly one row per entity may have `is_current = true` at any time.
- On a tracked-attribute change, set the previously-current row's `valid_to` to
  the incoming change's effective date, and its `is_current` to false.
- Insert a new row for the entity with the new attribute value, `valid_from`
  set to the effective date, `valid_to` set to null, and `is_current` set to
  true.
- Historical rows are never deleted or overwritten once closed.

## Implementation Guidance

1. Read the incoming change event (entity key, new attribute value, effective
   date).
2. Look up the entity's current row in the target (`is_current = true`).
3. Close the current row: set `valid_to = effective_date`, `is_current =
   false`.
4. Construct and append the new row: `valid_from = effective_date`, `valid_to
   = null`, `is_current = true`, with the new attribute value and all other
   unchanged columns carried forward.
5. Write the closed row and the new row back to the target (overwrite the
   closed row's partition/file, append the new row, or use a table format that
   supports row-level update).

```python
closed_df = current_df.withColumn(
    "valid_to", F.when(F.col("account_id") == changed_id, effective_date).otherwise(F.col("valid_to"))
).withColumn(
    "is_current", F.when(F.col("account_id") == changed_id, F.lit(False)).otherwise(F.col("is_current"))
)
new_row_df = spark.createDataFrame([...])  # new_plan_id, valid_from=effective_date, valid_to=None, is_current=True
result_df = closed_df.unionByName(new_row_df)
```

## Validation

- Ingest a baseline row, then a change event; assert exactly one `is_current =
  true` row remains for the entity afterward.
- Assert querying the entity's attribute as of a date before the change
  returns the old value, and as of a date on/after the change returns the new
  value.
- Assert the closed row's `valid_to` exactly equals the new row's `valid_from`,
  with no gap or overlap.

## Common Failure Scenarios

- The current-row lookup matches more than one row (e.g. an entity that was
  never properly closed out from a previous run), producing two rows with
  `is_current = true`.
- The new row is appended without closing the old one, leaving two open rows.
- `valid_to` and the new row's `valid_from` are computed independently and
  drift apart by even one day, creating a gap where no row is valid.

## Common Mistakes

- Updating the existing row's attribute value in place instead of closing it
  and appending a new one - this destroys history.
- Hardcoding the assumption that the incoming effective date is always later
  than the entity's current `valid_from`.
- Forgetting to carry forward unchanged columns onto the new row, leaving them
  null.

## Reusability

Applies to any entity and any tracked attribute where point-in-time history
must be queryable - substitute the entity, the tracked attribute(s), and the
effective-date source for the case at hand.

---
