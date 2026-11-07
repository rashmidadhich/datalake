# Pipeline Guardrails Skill - README

## Purpose & Scope

This skill defines the cross-cutting safety invariants that every batch must
satisfy, whichever transformation skill produced its rows: row-count
conservation, a controlled quarantine reason vocabulary, masked-field
passthrough integrity, idempotent re-runs, and non-destructive writes. It is
authored for Task 8 and applied across Tasks 1-7.

## When and Why to Use

Use it whenever a pipeline writes to persistent, accumulating output and more
than one skill or batch contributes rows to the same target. These invariants
are the ones a single happy-path batch does not reveal, and the ones a reviewer
checks first.

## Expected Inputs/Outputs

Input: per-batch counts (read / written / quarantined / skipped), the
quarantine table with a reason column, the sensitive/masked columns with their
comparison key, and the durable silver/quarantine locations.

Output: reusable assertions that either pass or fail the run; a run that fails
an invariant is a bug, not a warning.

## Applying the Five Rules in This Project

- **Row-count conservation** - implemented in `src/guardrails.py` and called
  after every batch in `src/pipeline.py`.
- **Controlled quarantine reasons** - `src/config.py` owns
  `QUARANTINE_REASONS`; every quarantining skill references it rather than
  inventing a string.
- **Masked-field integrity** - `email_masked` is compared source-vs-silver by
  `account_id` after every accounts batch; it is never recomputed or joined on.
- **Idempotent re-runs** - each batch's write is a read-modify-write of the
  logical table, so re-running resolves to skips/no-ops.
- **Non-destructive writes** - later batches rebuild the table from existing
  rows plus the change; they never truncate or drop earlier rows.

## Boundary with Other Skills

The four transformation skills each define correctness for their own
capability; none of them defines these cross-cutting properties. When a
transformation and a guardrail disagree, the guardrail is authoritative and the
transformation must be adjusted.

## Using This Skill with SKILL.md

Read SKILL.md's Expected Outcome and Recommended Approach before implementing
Task 8, and reference it from `CLAUDE.md` so quarantine reasons stay a
controlled vocabulary. This README does not repeat that guidance.
