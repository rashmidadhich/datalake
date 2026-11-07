# schema_drift

## Overview

Guidance for safely absorbing a new column that appears in a later
ingestion batch, without breaking the pipeline or losing existing data.
This is the skill behind Task 4 (Batch 3 — `loyalty_tier`) and, once you
modify it, Task 6 (Batch 4 — a second, independent drift event).

## Purpose and Scope

This skill covers exactly one kind of schema change: **a column that is
present in an incoming batch but was not present in the last known
schema for that entity.** It defines how to detect that, log it, and
combine old and new data without requiring both sides to match exactly.

It deliberately does **not** cover column renames, column removal, or data
type changes — `skill.md`'s own **When to Use** section states this
boundary explicitly. If your work on Task 6 surfaces one of those other
cases, that is precisely the kind of gap you are expected to notice and
address by modifying this skill yourself, not by silently expanding scope
without documenting it.

## When and Why to Use It

Use it whenever an entity is ingested repeatedly over time and its schema
is not contractually frozen — which describes every entity in this
assignment's `data/sample_batches/`. Without it, a pipeline hardcoded
against a fixed column list will either crash the moment an upstream
source adds a field, or silently drop the new information — both outcomes
this assignment explicitly asks you to avoid (see the assignment's
"Schema drift handling" component in Section 5).

## Expected Inputs and Outputs

| | |
|---|---|
| **Input** | The incoming batch's column list (and, optionally, types), and the last known schema for the entity — read from the existing bronze output or a saved snapshot. |
| **Output** | The job completes without error. The output schema includes the new column: populated for rows from the batch(es) that had it, null for rows ingested before it existed. The detection itself is logged (entity, batch id, column name) — not silent. |

## Business Rules / Scenarios

- **Batch 3 (`loyalty_tier`):** `customers_batch3_loyalty_tier.csv` adds one
  column not present in Batch 1. Expected: the column appears in bronze
  `customers`, null for Batch 1 rows, populated for Batch 3 rows; the
  addition is logged.
- **Batch 4 (Task 6 — required activity):**
  `customers_batch4_unexpected_column.csv` adds `marketing_opt_in` on top of
  a schema that has *already* been evolved once (by `loyalty_tier`). Before
  implementing this, read `skill.md` and decide: does its current guidance,
  written for a single drift event against a stable baseline, say anything
  about a *second* independent drift event layered on an already-evolved
  schema? If not, that is the gap to close — modify `skill.md` yourself to
  state how a previously-added column (`loyalty_tier`) should be preserved
  while a further new column (`marketing_opt_in`) is absorbed. See
   `data/sample_batches/batch4_schema_drift_skill_update/README.md` for the
  exact pass criteria.
- **Out of scope, do not improvise:** a column disappearing, being renamed,
  or changing type. If you hit one of these, say so explicitly in your
  skill-driven evidence rather than quietly handling it as if it were "just
  another new column."

## How to Use `skill.md`

Read `skill.md` in full before Task 4. For Task 6, re-read it specifically
against the Batch 4 scenario above and:
1. Use **Decision Rules** and **Core Guidance** to identify exactly which
   sentence(s) don't address "a second drift event on an already-evolved
   schema."
2. Modify only what's needed to close that gap — keep the skill's existing
   structure and its explicit non-goals (renames/removals/type changes)
   intact. This is a targeted edit, not a rewrite.
3. Use **Validation** as your test list, extended to also assert the
   previously-added column (`loyalty_tier`) is unaffected by the new drift
   event.
4. Record what gap you found and what you changed in the rubric's
   "Skill-Driven Evidence Log" (Batch 4 row).

This README does not repeat the skill's decision rules, implementation
steps, or code illustration — see `skill.md` directly for those, and edit
`skill.md` itself (not this README) when you make your Task 6 change.

## Notes for Evaluators

Check that the learner's Task 6 modification is a targeted addition (not a
rewrite that discards the skill's original scope), that it is applied
*before* the Batch 4 implementation (not reverse-engineered after the code
already worked), and that bronze `customers` after Batch 4 retains
`loyalty_tier` unchanged alongside the new `marketing_opt_in` column.

The Task 6 gap is that single-column guidance did not explicitly state that a
later additive drift must compare against the full, already-evolved schema
and preserve the values of earlier added columns. The updated `SKILL.md` now
states this sequential-drift rule and adds a validation requirement for a
second distinct column. Renames, removals, and type changes remain out of
scope.
