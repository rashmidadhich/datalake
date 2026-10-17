# SCD2 History Skill - README

## Purpose & Scope

This skill guides how to record a tracked-attribute change (here, `plan_id`)
as queryable history rather than overwriting it. It is used for Batch 4
(`billing_accounts_batch4_plan_change.csv`).

## When and Why to Use

Use it whenever a plan-change event arrives for an existing account and you
need the account's plan history to remain queryable by date.

## Expected Inputs/Outputs

Input: a plan-change event (`account_id`, `old_plan_id`, `new_plan_id`,
`effective_date`) plus the account's current silver row. Output: the prior row
closed out, a new current row appended.

## Business Rules & Scenarios for This Assignment

Batch 4 has 5 plan-change events. Four have effective dates after the
account's signup date, in normal order. One has an `effective_date` earlier
than the account's own `signup_date` (out of order). As provided, this skill
does not define what should happen in that case. Task 6 requires you to
review the skill against Batch 4, identify this gap, and modify the skill
yourself before implementing - if the modification doesn't fully resolve the
ordering, the affected row must be quarantined rather than corrupting the
account's history.

## Using This Skill with SKILL.md

Read SKILL.md's Decision Rules and Implementation Guidance before writing any
code for Task 5/Task 6. This README does not repeat that guidance.
