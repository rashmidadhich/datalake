# Ramya - Task 6 Schema-Drift Skill Improvement

Date: 2026-10-07

## Skill Used and Gap Identified

Skill: `.claude/skills/schema_drift/SKILL.md` (modified), with its companion `README.md` updated.

The original guidance explained detecting and absorbing an additive column, but did not explicitly direct a later drift to compare against the full schema after an earlier drift or require preservation of the earlier added column and its values. Batch 4 exercises this gap: Batch 3 added `loyalty_tier`; Batch 4 adds `marketing_opt_in` afterward.

## Change Made

Added decision and implementation guidance to compare every incoming schema with the current fully evolved Bronze schema; preserve earlier added columns and values; append only the newly detected column; and use null only for rows that predate a given field. Added a second-drift validation case. The explicit exclusions for renames, removals, and type changes remain unchanged. The README records the gap and improvement.

## Validation

The Task 6 test runs Batch 1, Batch 3, and Batch 4 against a temporary Bronze directory. It asserts both columns remain in the output, Batch 3 loyalty values match the source, Batch 4 loyalty and marketing values match the source, prior batches have null for fields they did not provide, and customer business keys remain unique.

User-reported full suite result after Task 6 changes: `7 passed, 2 skipped`. The two skips are the Batch 5 data-quality tests, deferred until Task 7.

Persistent Batch 4 output inspection has not yet been captured; the passing schema test uses temporary output so it does not modify persistent Bronze.
