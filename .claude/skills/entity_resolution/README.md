# Entity Resolution Skill - README

## Purpose & Scope

This skill decides whether a repeated business key refers to the *same*
real-world entity already in silver, or to a *different* entity that reused a
reassigned key. It is used for Batch 5, where three `account_id` values that
belonged to `legacy_v1` customers reappear under `platform_v2` with different
names, signup dates, and masked emails.

Scope note: this skill only decides identity. Once a row is confirmed to be the
same entity, hand it to the deduplication skill (mutable updates / true
duplicates) or the SCD2 history skill (tracked-attribute changes).

## When and Why to Use

Use it before any key-based dedup or SCD2 step whenever the key is not
guaranteed to be globally unique across sources or eras - a common consequence
of a migration, renumbering, or backfill. Applying key-based logic without this
step merges two customers and corrupts history.

## Expected Inputs/Outputs

Input: the incoming batch, the existing silver table (rows and history), the
business key, and the chosen identity fingerprint (stable, non-mutable,
non-sensitive attributes).

Output:
- Rows whose fingerprint agrees with the existing entity -> passed to normal
  key-based logic.
- Rows whose fingerprint disagrees -> written as a new, distinct surrogate
  identity; the original entity's rows and history are left untouched.
- Sensitive/masked fields -> passed through unchanged and never used as an
  identity or join key.

## Business Rules for This Assignment

Batch 5 contains three `account_id` values (`ACC-0096`, `ACC-0019`,
`ACC-0063`) whose incoming `customer_name`, `signup_date`, and `source_system`
disagree with the original `legacy_v1` owner. Each must become a new, distinct
silver identity; the original customers keep their rows and their SCD2 history;
`email_masked` is never unmasked or cross-joined.

A change of `source_system` alone is *not* proof of a new entity - the same
customer can migrate systems - so identity is decided on the stable attributes
(name, signup date) that cannot both belong to one customer.

## Boundary with Other Skills

- **Deduplication**: true duplicates and mutable-only updates of the *same*
  entity. Not this skill's concern.
- **SCD2 history**: tracked-attribute changes for a confirmed single entity.
- Pipeline ordering matters: type-cast -> entity resolution (identity) ->
  dedup -> SCD2 -> write, so that history is always keyed by the correct
  surrogate identity.

## Using This Skill with SKILL.md

Read SKILL.md's Recommended Approach and Validation before implementing Task 7.
This README does not repeat that guidance.
