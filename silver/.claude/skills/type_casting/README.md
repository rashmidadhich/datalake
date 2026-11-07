# Type Casting Skill - README

## Purpose & Scope

This skill guides how to normalize a field that arrives in more than one
format across source systems into one consistent silver-layer representation.
It is used for Batch 3 (`billing_accounts_batch3_date_format_v2.csv`).

## When and Why to Use

Use it whenever the same logical field (here, `signup_date` and `plan_id`)
arrives in different formats depending on `source_system`.

## Expected Inputs/Outputs

Input: an incoming batch with a source signal (`source_system`). Output: the
field cast to one canonical type/value for every row whose format is known;
rows whose format is unrecognized are flagged, not cast.

## Business Rules & Scenarios for This Assignment

Batch 3 has 3 rows in `MM/DD/YYYY` format (vs. `YYYY-MM-DD` in Batch 1) plus
inconsistent `plan_id` casing, and 1 row in a third, unrecognized long-form
textual date format. Use this skill as provided - no modification is required
- but the pipeline-level logic (outside the skill file) routes the flagged,
unrecognized-format row to a quarantine output with a reason, per Task 3. Two
data quirks are handled there: the unrecognized long-form date carries an
unquoted comma (so Spark reports the row as `_corrupt_record`), and one batch
declares `platform_v2` while writing legacy `yyyy-MM-dd` dates, so the caster
falls back to an unambiguous known pattern as the skill permits.

## Using This Skill with SKILL.md

Read SKILL.md's Decision Rules and Implementation Guidance before writing any
code for Task 3. This README does not repeat that guidance.
