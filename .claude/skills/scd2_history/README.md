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
than the account's own `signup_date` (out of order). This skill was **modified
for Task 6** before implementation: as provided it did not define what to do
with an out-of-order date, so the modification added a rule requiring either a
correctly rebuilt contiguous timeline or quarantine with the controlled reason
`out_of_order_effective_date` - never an inverted interval. The Batch 4
out-of-order event is quarantined and the account's history is left intact.

## Using This Skill with SKILL.md

Read SKILL.md's Decision Rules and Implementation Guidance before writing any
code for Task 5/Task 6. This README does not repeat that guidance.
