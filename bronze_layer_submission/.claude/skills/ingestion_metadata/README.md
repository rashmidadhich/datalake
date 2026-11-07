# ingestion_metadata

## Overview

Guidance for tagging every ingested record with a small, fixed set of
traceability fields — `batch_id`, `source_file`, `ingestion_timestamp` —
so any row in bronze can be traced back to the run and file it came from.
This is the skill behind Task 2 and underlies every other task, since
Tasks 3, 4, 6 and 7 all reference these fields when logging what happened
to a batch.

## Purpose and Scope

This skill covers exactly three fields, added once, as early as possible
in the pipeline. It is deliberately minimal — `skill.md`'s **Core Guidance**
is explicit that the goal is basic traceability, not a full audit/control-table
framework. Do not expand it into additional metadata columns unless a task
in the assignment explicitly asks for one.

## When and Why to Use It

Use it for every batch ingestion step in this assignment, for every
entity, without exception. Apply it together with the idempotency and
schema-drift skills — `skill.md` notes these metadata columns are typically
added *before* those other checks run, since drift and duplicate-handling
events need to be attributable to a specific batch when they're logged.

Why it matters: once a row is written to bronze, there is otherwise no way
to answer "which file did this come from?" or "which run produced this?" —
which is exactly what Task 5's audit evidence, and the rubric's
"Skill-Driven Evidence Log," ask you to be able to show for every batch.

## Expected Inputs and Outputs

| | |
|---|---|
| **Input** | A batch identifier (supplied by whatever triggers ingestion — not derived from the data), the source file name for the row being read, and the current time at load. |
| **Output** | Every record in bronze carries all three fields, always populated, with all original source columns unchanged and present alongside them. |

## Business Rules / Scenarios

- **Every batch, every entity:** `batch_id`, `source_file`, and
  `ingestion_timestamp` must be non-null for every successfully ingested
  row — there is no case in this assignment where one of the three is
  optional.
- **Multiple files in one batch:** capture the *actual* file each row was
  read from, not one value for the whole run.
- **Name collision:** if a source column happens to already be named
  `batch_id`, `source_file`, or `ingestion_timestamp`, `skill.md`'s
  **Common Mistakes** section flags silently overwriting it as a named
  failure mode — decide and document what your implementation does instead
  of letting it happen by accident.
- **Cross-task consistency:** the same three fields, populated the same
  way, across all five entities and all five batches (1 through 5) — Task
  7's quarantined rows (Batch 5) must carry them too, not just the rows
  that reach bronze.

## How to Use `skill.md`

Read `skill.md` in full before Task 2 — this is the first skill you'll
apply, since metadata tagging happens immediately after reading each source
file. Then:
1. Use **Inputs / Context Required** to decide where `batch_id` and
   `source_file` come from in your pipeline (passed in, not computed from
   business data).
2. Use **Decision Rules** to settle the "always populated, always additive"
   requirements before writing code.
3. Use **Validation** as your test list for Task 2, and re-use it for every
   later batch (2 through 5) since metadata correctness never becomes
   optional once idempotency, schema-drift, or data-quality logic is added
   on top.

This README does not repeat the skill's implementation steps or code
illustration — see `skill.md` directly for those.

## Notes for Evaluators

Check that all three metadata fields are populated for every bronze row
across all five batches (including quarantined rows from Batch 5), that
`source_file` and `batch_id` are supplied values rather than hardcoded
literals, and that no source column was silently overwritten by a
same-named metadata field.
