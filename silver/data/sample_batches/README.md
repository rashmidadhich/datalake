# Sample Batches

Each folder below is one simulated bronze delivery. Use these alongside the three
baseline files in `data/` (`billing_accounts.csv`, `plan_catalog.csv`, `invoices.csv`)
which represent Batch 1, the initial full load.

## `batch_2_duplicate_retry/`

`invoices_batch2_duplicate_retry.csv` — 6 rows. 4 are byte-identical retries of rows
already in `invoices.csv` (a true redelivery). 2 reuse an existing `invoice_id` but
carry a different `amount_due` / `payment_status` than the original row (a conflicting
record, not a duplicate). Exercises Task 2 (Deduplication).

## `batch_3_date_format_v2/`

`billing_accounts_batch3_date_format_v2.csv` — 4 new accounts from the newer
`platform_v2` source. 3 rows use `MM/DD/YYYY` dates (vs. `YYYY-MM-DD` in Batch 1) and
inconsistent `plan_id` casing. 1 row uses a third, unrecognized date format (a
long-form textual date) that matches neither known pattern. Exercises Task 3 (Type
Casting) and its quarantine requirement.

## `batch_4_plan_change/`

`billing_accounts_batch4_plan_change.csv` — 5 plan-change events for existing
accounts, each with `old_plan_id`, `new_plan_id`, and `effective_date`. One row's
`effective_date` is earlier than the account's own `signup_date` (out of order).
Exercises Task 5/Task 6 (SCD2 History Tracking, skill modification).

## `batch_5_reused_account_id/`

`billing_accounts_batch5_reused_account_id.csv` — 3 rows whose `account_id` was
already assigned to a Batch 1 `legacy_v1` customer, now reused by `platform_v2` for
a different physical customer (different name, email, signup date). Exercises Task 7
(Entity Resolution).
