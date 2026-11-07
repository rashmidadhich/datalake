# idempotency

## Overview

Guidance for making a batch ingestion step safe to re-run: redelivering a
batch that has already landed in bronze must not create duplicate records.
This is the skill behind Task 3 (Batch 2 — duplicate `sales_transactions`
redelivery) and every re-run of Batch 1.

## Purpose and Scope

This skill covers exactly one problem: **the same batch, or the same
business-key records, arriving more than once**. It defines how to detect
that a record has already been ingested and how to avoid writing it again.

It is intentionally narrow. It does **not** cover distributed exactly-once
delivery, transactional checkpointing, or multi-system locking — those solve
a different, larger problem than "don't duplicate this batch if it's
re-run." If your implementation starts reaching for those, you've stepped
outside what this skill is for.

## When and Why to Use It

Use it whenever:
- Bronze output persists across runs (a table or file, not a throwaway
  DataFrame), and
- The same batch (same file, same date, same extract) could plausibly be
  triggered more than once — a retry after a timeout, a manual re-run, an
  accidental re-upload.

Why it matters for this assignment: a plain `mode("append")` write has no
memory of what it already wrote. Without this skill applied, re-running
Batch 1 or reprocessing Batch 2 would silently double the affected rows —
which is exactly the failure mode Task 3 and Task 5 ask you to demonstrate
you've avoided.

## Expected Inputs and Outputs

| | |
|---|---|
| **Input** | The incoming batch's DataFrame, the business key for the entity (e.g. `order_id` for `sales_transactions`), and the existing bronze output for that entity (or nothing, on the very first load). |
| **Output** | Bronze contains exactly one row per business key. Re-running an already-processed batch leaves bronze's row count and content unchanged. A batch mixing already-seen and new business keys adds only the new ones. |

## Business Rules / Scenarios

- **First load (Batch 1):** target does not exist yet → every row is new.
- **Exact redelivery (Batch 1 re-run, or Batch 2):** every business key in
  the incoming batch already exists in bronze → zero rows added, bronze
  unchanged.
- **Mixed batch:** some business keys already exist, some don't → only the
  new ones are added.
- **Empty batch:** zero rows read → should succeed with zero rows written,
  not error.

Worked example (Batch 2, `sales_transactions`): `sales_transactions_batch2_duplicate.csv`
re-sends `order_id` `O-00001`, `O-00002`, `O-00003`, all already loaded in
Batch 1. Expected result: bronze `sales_transactions` row count after Batch 2
is identical to after Batch 1 — see
`data/sample_batches/batch_2_duplicate_redelivery/README.md` for the exact
pass criteria.

## How to Use `skill.md`

`skill.md` is your implementation reference, not a checklist to copy from
top to bottom. Read it in full *before* writing any code for Task 3, then:
1. Use its **Inputs / Context Required** section to identify the business
    key for the entity you're working on.
2. Use **Decision Rules** to settle what should happen in each case (new
    target, existing key, new key) before you write the join/filter logic.
3. Use **Validation** and **Common Failure Scenarios** as your test list —
    they describe exactly what Task 5's evidence needs to demonstrate.
4. If something you encounter falls outside the skill's stated scope (see
    its **When to Use** section's boundary — no distributed locking or
    exactly-once semantics), that's a signal to consult the Helper Guide's
    skill-driven workflow rather than improvising silently.

This README does not repeat the skill's decision rules or code illustration
— see `skill.md` directly for those.

## Notes for Evaluators

Check that the learner's duplicate check is keyed on the business key
(`order_id`), not on file identity, row content hash, or a
Spark-generated id — and that Batch 1's re-run and Batch 2's redelivery both
produce unchanged bronze row counts, as recorded in the rubric's "Ingestion
Batch Test Log."
