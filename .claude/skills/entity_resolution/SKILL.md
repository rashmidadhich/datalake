---
name: silver-entity-resolution
description: >
  Decide whether a repeated business key refers to the same real-world entity
  already present in the silver layer, or to a different physical entity that
  reused a retired or reassigned key. Use whenever a key column is not
  guaranteed to be globally unique across source systems or eras - for example
  after a migration that reassigned legacy identifiers to new owners - and
  key-based deduplication or history logic could otherwise merge two distinct
  entities.
---

# Skill Name

Entity Resolution (Detecting Reused / Reassigned Business Keys)

## Purpose

Prevent two different real-world entities that share one business key from
being merged into a single silver-layer identity. When a key is reused, the
pipeline must assign the newcomer its own identity and leave the original
entity's existing rows and history untouched.

## When to Use

- A business key is unique *within* a source or era but not guaranteed unique
  *across* sources or *across time* (common after a migration, a renumbering,
  or a data backfill).
- Key-based deduplication or SCD2 logic is about to run, and applying it purely
  on the key would either overwrite, quarantine, or extend the wrong entity.
- Two rows share a key but their stable, identity-describing attributes do not
  agree.

Do not use this skill for the ordinary case where a key maps to one entity and
only mutable or tracked attributes change - that belongs to the deduplication
or SCD2 skill. This skill only decides *identity*.

## Inputs

- **The business key** whose reuse you must detect (e.g. an account or customer
  identifier).
- **The identity fingerprint**: the set of *stable, identity-defining*
  attributes that describe which real-world entity a row is, excluding the key
  itself, excluding mutable/business-state attributes, and excluding sensitive
  passthrough fields. Choose attributes that a genuine re-registration of the
  same entity would not change.
- **The existing silver table** for that entity (current rows and, if present,
  history rows).
- **The incoming batch.**

## Expected Outcome

For every incoming row whose key already exists:

- If its identity fingerprint is **consistent** with the existing entity, the
  row is the same entity and is handed to the normal key-based logic
  (deduplication / SCD2).
- If its identity fingerprint **disagrees** with the existing entity, the row
  is a **different entity**: it receives a **distinct surrogate identity**, is
  written as new, and never merges into, overwrites, or closes the original
  entity's rows or history.
- Masked or otherwise sensitive fields are never recomputed, unmasked,
  cross-joined, or used as an identity or join key; they pass through
  unchanged as opaque values.

Invariant: after resolution, each surrogate identity has at most one open
(current) history row, and the original entity's rows and history are
byte-identical to what they were before.

## Recommended Approach

1. **Define the fingerprint deliberately.** List the stable identity attributes
   and document why each belongs. Include attributes that survive a legitimate
   re-registration; exclude anything mutable, derived, or sensitive.
2. **Detect reuse before acting.** Join the incoming batch to the existing table
   on the business key and compare the fingerprint attribute by attribute.
   Treat a change of *source system alone* as weak evidence - the same entity
   can legitimately move to a new system - and decide identity on the stable
   attributes that cannot both belong to one entity.
3. **Branch on the comparison.**
   - Consistent fingerprint -> same entity; hand to deduplication / SCD2.
   - Inconsistent fingerprint -> assign a **distinct surrogate identity** and
     write it as a new entity; do not touch the original entity's rows.
4. **Assign the surrogate identity deterministically.** Derive it from the key
   plus enough of the distinguishing context that re-running the same batch
   produces the same identity (idempotency), without embedding sensitive data.
   Keep the original key value so the row is still traceable, but treat the
   surrogate identity - not the raw key - as the grain for history.
5. **Keep history isolated.** History rows are keyed by the surrogate identity,
   so the newcomer's changes never close the original entity's open row.

Illustrative shape only (not a full implementation):

```python
# fingerprint = columns that must agree for two rows to be the same entity
same = incoming.join(existing, on=key, how="inner").filter(
    all_equal(fingerprint_cols)      # every identity attribute agrees
)
reused = incoming.join(same, on=key, how="left_anti")   # key exists, identity differs
reused = reused.withColumn("surrogate_id", new_identity(key, *distinguishing_cols))
```

## Validation

- For each reused key, assert the incoming row becomes a **new, distinct**
  identity and does not appear among the original entity's rows.
- Assert the original entity's existing silver rows *and* its SCD2 history are
  unchanged (same count, same values, same open-row status) after the batch.
- Assert no masked/sensitive field can be joined back to its unmasked or
  original counterpart: a self-join on the sensitive field between the two
  identities returns nothing that reveals a real value.
- Re-run the same batch and assert the surrogate identities are identical
  (idempotent), not duplicated.

## Edge Cases

- Key reused but the fingerprint is *identical* on every chosen attribute -
  genuinely ambiguous; quarantine for human review rather than guessing, since
  the pipeline cannot prove it is a different entity.
- Key reused with only a *partial* fingerprint match - decide explicitly which
  attributes are decisive and document it; do not fall back to "one attribute
  differs, therefore new entity".
- Same entity legitimately arriving from a new source system - a source-system
  change alone must not create a new identity.
- Two different entities whose masked placeholders happen to be identical -
  never use the masked field to confirm or deny identity.
- A reused key that also carries a tracked-attribute change - resolve identity
  first, then apply SCD2 to the correct entity.

## Common Mistakes

- Joining or deduplicating purely on the raw key, which silently merges two
  entities and destroys the original's history.
- Using a masked or otherwise sensitive field as part of the fingerprint or as
  a join key, increasing identifiability.
- Overwriting the original entity's row instead of creating a new identity.
- Assigning a random surrogate identity that changes on every re-run, breaking
  idempotency.
- Treating any single differing attribute as proof of a different entity,
  including attributes that legitimately change.

## Reusability

Applies to any entity whose key may be reused or reassigned across sources or
eras. Substitute the business key, the identity fingerprint, and the surrogate
identity rule; the branching logic is otherwise independent of the entity,
storage format, and pipeline. It is a prerequisite concern for deduplication
and SCD2, not a replacement for either.
