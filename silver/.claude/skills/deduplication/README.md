# Deduplication Skill - README

## Purpose & Scope

This skill guides how to handle a repeated business key in the silver layer -
whether it comes from a redelivered batch or from a row that legitimately
updates an existing record's business state - and routes it to exactly one of
three outcomes: quarantine, overwrite (update, no history), or retain
unchanged. It is used for Batch 2 (`invoices_batch2_duplicate_retry.csv`).

## When and Why to Use

Use it any time a batch could plausibly be delivered more than once, and/or
an entity's business-state columns (e.g. a status or amount field) can
legitimately change after the record's initial load, and you need to check
incoming rows against what's already in the silver table before writing.

Scope note: this skill applies to entities whose business key is expected to
be immutable and non-repeating for a *different* real-world record (e.g.
`invoices`, keyed by `invoice_id`). It does not apply to `billing_accounts`
plan changes (handled by the SCD2 skill) or reused `account_id` values
(handled by the entity resolution skill) - those cases also present as "key
exists, non-key values differ" but are explicitly out of scope for this
skill. See "Boundary with Other Skills" below.

## Expected Inputs/Outputs

Input: an incoming batch plus the current silver table for the same entity,
with its columns split into identity-defining columns (must never change for
a given key) and mutable/business-state columns (expected to change over the
record's lifecycle).

Output: every row resolves to exactly one of:
- **New** - business key not yet in silver -> inserted.
- **Retain unchanged** - identical to the existing row on every non-key
  column -> no write.
- **Overwrite (update, no history)** - differs from the existing row only on
  mutable columns -> those columns are overwritten on the existing silver row;
  the prior value is not retained anywhere.
- **Quarantine** - differs from the existing row on any identity-defining
  column -> routed to quarantine with a reason; the existing silver row is
  left untouched.

## Business Rules & Scenarios for This Assignment

Batch 2 contains 6 invoice rows whose `invoice_id` already exists in
`invoices.csv`. Verified against the actual baseline, the rows resolve as:

- 2 are byte-identical retries (`INV-00449`, `INV-00122`) -> retain unchanged.
- 0 differ only on mutable columns -> no legitimate status update occurs in
  this batch.
- 4 disagree on identity-defining columns (`INV-00113`, `INV-00220`,
  `INV-00301`, `INV-00378` differ on `account_id` and `invoice_date`) ->
  quarantine with reason `conflicting_key`.

Data note: the assignment narrative describes 4 duplicates + 1 update + 1
conflict, and the sample-batch README describes 4 duplicates + 2 conflicts;
neither matches the CSV actually provided. The pipeline implements the data as
written and the discrepancy is recorded in
`outputs/artifacts/skill_driven_evidence_log.md`.

This skill was **modified for Task 2** before implementation. As originally
provided it already listed the three outcomes but did not define the boundary
and ordering against the SCD2 and entity-resolution skills, and its quarantine
expected-outcome item was self-contradictory. The modification added an ordered
classification rule (route tracked-attribute changes to SCD2 and reused keys to
entity resolution before quarantining), required reason codes to come from the
controlled vocabulary, and clarified that a byte-identical duplicate is never
quarantined.

## Boundary with Other Skills

- **SCD2 history skill**: a row sharing an existing `account_id` with a
  different `plan_id` and an `effective_date` (Batch 4) is a plan change, not
  a dedup case. The presence of an `effective_date` for a tracked attribute
  is the discriminator - route it to SCD2's close-and-append logic, not to
  this skill's quarantine/overwrite rules.
- **Entity resolution skill**: a row sharing an existing `account_id` but
  disagreeing on identity attributes (`customer_name`, `signup_date`,
  `source_system`) (Batch 5) represents a different physical entity reusing a
  key, not a conflicting or updated record of the same entity. Route it to
  entity resolution, not to this skill's quarantine rule.
- Pipeline ordering (type-cast -> resolve identity -> dedup -> SCD2 -> write)
  is what keeps these three skills from competing for the same rows - by the
  time a row reaches this skill, it has already been type-cast, and any row
  that truly belongs to SCD2 or entity resolution is handled downstream, not
  by this skill's identity-column check.

## Using This Skill with SKILL.md

Read SKILL.md's Decision Rules and Implementation Guidance before writing any
code for Task 2. This README does not repeat that guidance.
