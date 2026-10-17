# Pipeline run log

Append-only log written by `src/audit.py` during a run of
`python -m src.pipeline`. One row per audited batch.

| Timestamp | Level | Batch | Entity | Detail |
| --- | --- | --- | --- | --- |
| 2026-10-17 23:24:24 | INFO | batch_1 | baseline | counts: billing_accounts=100, plan_catalog=6, invoices=480 \| initial full load (valid_from=signup_date, is_current=true) |
| 2026-10-17 23:24:29 | INFO | batch_2_invoices | invoices | counts: new=0, overwritten=0, quarantined=4, retained_unchanged=2 \| silver_invoices=480 |
| 2026-10-17 23:24:35 | INFO | batch_3_accounts | billing_accounts | counts: inserted=3, quarantined=1, retained_unchanged=0 \| format-aware cast + plan_id casing; silver_billing_accounts=103 |
| 2026-10-17 23:24:37 | INFO | batch_4_plan_change | billing_accounts | counts: applied=4, quarantined=1, already_applied=0 \| close-and-append; silver_billing_accounts=107 |
| 2026-10-17 23:24:41 | INFO | batch_5_reused_accounts | billing_accounts | counts: inserted=3, quarantined=0, retained_unchanged=0, reused_new_identity=3, same_entity=0, new=0 \| entity resolution: reused keys -> new identities; silver_billing_accounts=110 |
| 2026-10-17 23:24:41 | INFO | summary | pipeline | counts: silver_billing_accounts=110, silver_plan_catalog=6, silver_invoices=480, billing_accounts_quarantine=2, invoices_quarantine=4 \| final table counts |
