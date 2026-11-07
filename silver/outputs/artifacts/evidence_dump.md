# Evidence dump - Tiger Retail Billing silver layer

Written by `python -m tools.evidence_dump`. Every figure below is read back from the Parquet tables in `silver/` and `quarantine/` after a full pipeline run.

## Final counts

| Table | Rows |
| --- | --- |
| silver/silver_billing_accounts | 110 |
| silver/silver_plan_catalog | 6 |
| silver/silver_invoices | 480 |
| quarantine/billing_accounts_quarantine | 2 |
| quarantine/invoices_quarantine | 4 |

## Batch 1 baseline (Task 1)

Source files as delivered, against what is in silver now:

| Source file | Data rows | Loaded into | Rows in silver now |
| --- | --- | --- | --- |
| data/billing_accounts.csv | 100 | silver/silver_billing_accounts | 110 |
| data/plan_catalog.csv | 6 | silver/silver_plan_catalog | 6 |
| data/invoices.csv | 480 | silver/silver_invoices | 480 |

`silver_billing_accounts` grew with Batches 3-5 (3 new accounts, 4 plan-change versions, 3 reused-key entities). Plans and invoices are unchanged because Batch 2 added no net rows: 2 byte-identical retries were skipped, 4 conflicting rows were quarantined, and the supplied CSV contains no in-place update rows.

## Schemas - declared per column, never inferred (Tasks 1 and 3)

`silver_billing_accounts`

| Column | Type |
| --- | --- |
| entity_id | string |
| account_id | string |
| customer_name | string |
| email_masked | string |
| signup_date | date |
| source_system | string |
| plan_id | string |
| status | string |
| valid_from | date |
| valid_to | date |
| is_current | boolean |

`silver_plan_catalog`

| Column | Type |
| --- | --- |
| plan_id | string |
| plan_name | string |
| monthly_price | decimal(10,2) |
| tier | string |

`silver_invoices`

| Column | Type |
| --- | --- |
| invoice_id | string |
| account_id | string |
| invoice_date | date |
| amount_due | decimal(10,2) |
| currency | string |
| payment_status | string |

## Plan catalog

| plan_id | plan_name | monthly_price |
| --- | --- | --- |
| PLAN-BASIC | Basic Monthly | 9.99 |
| PLAN-ENTERPRISE | Enterprise Custom | 499.00 |
| PLAN-GOLD | Gold Monthly | 29.99 |
| PLAN-PLATINUM | Platinum Annual | 249.00 |
| PLAN-SILVER | Silver Monthly | 19.99 |
| PLAN-STUDENT | Student Discount | 4.99 |

## Batch 4 - SCD2 history for ACC-0068 (PLAN-BASIC to PLAN-GOLD, effective 2026-03-18)

| entity_id | account_id | plan_id | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- | --- |
| ACC-0068 | ACC-0068 | PLAN-BASIC | 2024-04-19 | 2026-03-18 | False |
| ACC-0068 | ACC-0068 | PLAN-GOLD | 2026-03-18 |  | True |

## Batch 4 - ACC-0053 out-of-order change; history must be untouched

| entity_id | account_id | plan_id | valid_from | valid_to | is_current |
| --- | --- | --- | --- | --- | --- |
| ACC-0053 | ACC-0053 | PLAN-SILVER | 2024-10-30 |  | True |

## Batch 5 - entity resolution for the three reused account ids

| entity_id | account_id | customer_name | source_system | plan_id | is_current |
| --- | --- | --- | --- | --- | --- |
| ACC-0019 | ACC-0019 | Zoe Costa | legacy_v1 | PLAN-ENTERPRISE | True |
| ACC-0019:R | ACC-0019 | Sam Khan | platform_v2 | PLAN-GOLD | True |
| ACC-0063 | ACC-0063 | Mia Petrov | legacy_v1 | PLAN-BASIC | True |
| ACC-0063:R | ACC-0063 | Chen Kim | platform_v2 | PLAN-PLATINUM | True |
| ACC-0096 | ACC-0096 | Leo Lund | legacy_v1 | PLAN-ENTERPRISE | True |
| ACC-0096:R | ACC-0096 | Priya Okoye | platform_v2 | PLAN-STUDENT | True |

## Masked field sample - email_masked stays opaque

| entity_id | email_masked |
| --- | --- |
| ACC-0001 | m***@trb1***.com |
| ACC-0002 | j***@trb2***.com |
| ACC-0003 | e***@trb3***.com |
| ACC-0004 | s***@trb4***.com |
| ACC-0005 | a***@trb5***.com |
| ACC-0006 | a***@trb6***.com |

## Quarantine - billing accounts

| account_id | batch_id | reason |
| --- | --- | --- |
| ACC-0053 | batch_4_plan_change | out_of_order_effective_date |
| ACC-0104 | batch_3_accounts | unrecognized_format |

## Quarantine - invoices

| invoice_id | account_id | batch_id | reason |
| --- | --- | --- | --- |
| INV-00113 | ACC-0031 | batch_2_invoices | conflicting_key |
| INV-00220 | ACC-0058 | batch_2_invoices | conflicting_key |
| INV-00301 | ACC-0002 | batch_2_invoices | conflicting_key |
| INV-00378 | ACC-0090 | batch_2_invoices | conflicting_key |

## Invariants

| Check | Value | Expected |
| --- | --- | --- |
| distinct entity_id | 106 | one row per real-world account |
| distinct account_id | 103 | 3 fewer than entity_id after Batch 5 |
| entities with more than one is_current row | 0 | 0 |

