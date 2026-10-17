# Output verification

Run with `python -m tools.verify_outputs`. Result: **12/12 checks passed**.

| # | Check | Result | Detail |
| --- | --- | --- | --- |
| 1 | Table row counts | PASS | accounts=110, plans=6, invoices=480, accounts_quarantine=2, invoices_quarantine=4 (expected 110, 6, 480, 2, 4) |
| 2 | Explicit typed columns (no inferred strings) | PASS | signup_date/valid_from/valid_to are dates, is_current is bool, monthly_price is numeric |
| 3 | plan_id casing normalized | PASS | every plan_id is upper case |
| 4 | Batch 3 unrecognized date quarantined, not guessed | PASS | ACC-0104 -> unrecognized_format, absent from silver |
| 5 | Exactly one open row per entity, no inverted interval | PASS | max open rows per entity_id=1, inverted intervals=0 |
| 6 | SCD2 history is queryable as of a date | PASS | ACC-0068: PLAN-BASIC on 2025-01-01, PLAN-GOLD on 2026-06-01 |
| 7 | Out-of-order change quarantined, history untouched | PASS | ACC-0053 -> out_of_order_effective_date; still one current row in silver |
| 8 | Reused account ids resolved to distinct identities | PASS | two identities each for ACC-0096, ACC-0019, ACC-0063; distinct entity_id=106, distinct account_id=103 |
| 9 | Quarantine reasons come from the controlled vocabulary | PASS | observed: ['conflicting_key', 'out_of_order_effective_date', 'unrecognized_format'] |
| 10 | email_masked is passed through unchanged (never reconstructed) | PASS | 66 distinct masked values in the source CSVs; 0 silver values not found there |
| 11 | Invoices keep their business key unique | PASS | 480 distinct invoice_id over 480 rows |
| 12 | Batch 1 account ids all still present (non-destructive writes) | PASS | 100 Batch 1 account ids; 0 missing from silver |

Tables read back from disk: `silver/silver_billing_accounts`, `silver/silver_plan_catalog`, `silver/silver_invoices`, `quarantine/billing_accounts_quarantine`, `quarantine/invoices_quarantine`.
