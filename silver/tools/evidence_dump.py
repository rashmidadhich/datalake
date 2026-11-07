"""Write a concrete evidence dump of the pipeline outputs.

Produces ``outputs/artifacts/evidence_dump.md`` showing, for the reviewer:

  * final table counts read back from the written Parquet files;
  * the SCD2 history for a changed account and for the out-of-order account;
  * the entity-resolution split for the three reused account ids;
  * every quarantine row with its controlled reason code;
  * the invariants a reviewer can re-check in seconds.

Run with::

    python -m tools.evidence_dump

The numbers come from ``silver/`` and ``quarantine/`` on disk, not from the
in-memory pipeline run, so they double as a verification that the writes landed.
"""

from pathlib import Path

from src import config, ingest
from src.spark_session import get_spark

OUT = config.ARTIFACTS_DIR / "evidence_dump.md"


def source_rows(relative_path: str) -> int:
    """Data rows in a source CSV (header excluded), counted as plain text."""
    path = config.ROOT / relative_path
    if not path.exists():
        return -1
    with open(path, encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip()) - 1


def md_table(df, columns) -> str:
    """Render a DataFrame as a markdown table over ``columns``."""
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in df.select(*columns).collect():
        cells = ["" if row[c] is None else str(row[c]) for c in columns]
        lines.append("| " + " | ".join(cells) + " |")
    if len(lines) == 2:
        lines.append("| (none) |" + " |" * (len(columns) - 1))
    return "\n".join(lines)


def main() -> None:
    spark = get_spark("trb-evidence-dump")
    try:
        accounts = ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS)
        plans = ingest.read_table(spark, config.SILVER_PLAN_CATALOG)
        invoices = ingest.read_table(spark, config.SILVER_INVOICES)
        account_quarantine = ingest.read_table(spark, config.QUARANTINE_BILLING_ACCOUNTS)
        invoice_quarantine = ingest.read_table(spark, config.QUARANTINE_INVOICES)

        out = []
        out.append("# Evidence dump - Tiger Retail Billing silver layer\n")
        out.append(
            "Written by `python -m tools.evidence_dump`. Every figure below is read back "
            "from the Parquet tables in `silver/` and `quarantine/` after a full pipeline run.\n"
        )

        out.append("## Final counts\n")
        counts = [
            ("silver/silver_billing_accounts", accounts.count()),
            ("silver/silver_plan_catalog", plans.count()),
            ("silver/silver_invoices", invoices.count()),
            ("quarantine/billing_accounts_quarantine", account_quarantine.count()),
            ("quarantine/invoices_quarantine", invoice_quarantine.count()),
        ]
        out.append("| Table | Rows |")
        out.append("| --- | --- |")
        for name, n in counts:
            out.append(f"| {name} | {n} |")
        out.append("")

        out.append("## Batch 1 baseline (Task 1)\n")
        out.append("Source files as delivered, against what is in silver now:\n")
        out.append("| Source file | Data rows | Loaded into | Rows in silver now |")
        out.append("| --- | --- | --- | --- |")
        out.append(
            f"| data/billing_accounts.csv | {source_rows('data/billing_accounts.csv')} "
            f"| silver/silver_billing_accounts | {accounts.count()} |"
        )
        out.append(
            f"| data/plan_catalog.csv | {source_rows('data/plan_catalog.csv')} "
            f"| silver/silver_plan_catalog | {plans.count()} |"
        )
        out.append(
            f"| data/invoices.csv | {source_rows('data/invoices.csv')} "
            f"| silver/silver_invoices | {invoices.count()} |"
        )
        out.append("")
        out.append(
            "`silver_billing_accounts` grew with Batches 3-5 (3 new accounts, 4 plan-change "
            "versions, 3 reused-key entities). Plans and invoices are unchanged because Batch 2 "
            "added no net rows: 2 byte-identical retries were skipped, 4 conflicting rows were "
            "quarantined, and the supplied CSV contains no in-place update rows.\n"
        )

        out.append("## Schemas - declared per column, never inferred (Tasks 1 and 3)\n")
        for label, table in (
            ("silver_billing_accounts", accounts),
            ("silver_plan_catalog", plans),
            ("silver_invoices", invoices),
        ):
            out.append(f"`{label}`\n")
            out.append("| Column | Type |")
            out.append("| --- | --- |")
            for name, dtype in table.dtypes:
                out.append(f"| {name} | {dtype} |")
            out.append("")

        out.append("## Plan catalog\n")
        out.append(md_table(plans.orderBy("plan_id"), ["plan_id", "plan_name", "monthly_price"]))
        out.append("")

        out.append("## Batch 4 - SCD2 history for ACC-0068 (PLAN-BASIC to PLAN-GOLD, effective 2026-03-18)\n")
        out.append(md_table(
            accounts.filter(accounts.account_id == "ACC-0068").orderBy("valid_from"),
            ["entity_id", "account_id", "plan_id", "valid_from", "valid_to", "is_current"],
        ))
        out.append("")

        out.append("## Batch 4 - ACC-0053 out-of-order change; history must be untouched\n")
        out.append(md_table(
            accounts.filter(accounts.account_id == "ACC-0053").orderBy("valid_from"),
            ["entity_id", "account_id", "plan_id", "valid_from", "valid_to", "is_current"],
        ))
        out.append("")

        out.append("## Batch 5 - entity resolution for the three reused account ids\n")
        out.append(md_table(
            accounts.filter(accounts.account_id.isin(["ACC-0096", "ACC-0019", "ACC-0063"]))
            .select("entity_id", "account_id", "customer_name", "source_system", "plan_id", "is_current")
            .orderBy("account_id", "entity_id"),
            ["entity_id", "account_id", "customer_name", "source_system", "plan_id", "is_current"],
        ))
        out.append("")

        out.append("## Masked field sample - email_masked stays opaque\n")
        out.append(md_table(
            accounts.select("entity_id", "email_masked").orderBy("entity_id").limit(6),
            ["entity_id", "email_masked"],
        ))
        out.append("")

        out.append("## Quarantine - billing accounts\n")
        out.append(md_table(
            account_quarantine.orderBy("account_id"),
            ["account_id", "batch_id", "reason"],
        ))
        out.append("")

        out.append("## Quarantine - invoices\n")
        out.append(md_table(
            invoice_quarantine.orderBy("invoice_id"),
            ["invoice_id", "account_id", "batch_id", "reason"],
        ))
        out.append("")

        entities = accounts.select("entity_id").distinct().count()
        reused = accounts.select("account_id").distinct().count()
        multi_open = (
            accounts.filter(accounts.is_current).groupBy("entity_id").count().filter("count > 1").count()
        )

        out.append("## Invariants\n")
        out.append("| Check | Value | Expected |")
        out.append("| --- | --- | --- |")
        out.append(f"| distinct entity_id | {entities} | one row per real-world account |")
        out.append(f"| distinct account_id | {reused} | 3 fewer than entity_id after Batch 5 |")
        out.append(f"| entities with more than one is_current row | {multi_open} | 0 |")
        out.append("")

        text = "\n".join(out) + "\n"
        Path(OUT).parent.mkdir(parents=True, exist_ok=True)
        Path(OUT).write_text(text, encoding="utf-8")
        print(text)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
