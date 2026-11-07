"""One-command verification of the written Parquet outputs.

Reads the five written tables with pandas (no Spark, so it runs in seconds even
on a machine that has never run the pipeline) and checks the invariants a
reviewer cares about:

  1.  table row counts (110 / 6 / 480, quarantine 2 / 4);
  2.  date and boolean columns really are typed;
  3.  plan_id casing is normalized;
  4.  the unrecognized Batch 3 date is quarantined, not present in silver;
  5.  exactly one open SCD2 row per entity and no inverted interval;
  6.  the ACC-0068 history is queryable as of a date;
  7.  the out-of-order change is quarantined and its history is untouched;
  8.  each reused account_id resolves to two distinct identities;
  9.  every quarantine reason comes from the controlled vocabulary;
  10. email_masked values appear verbatim in the provided source CSVs;
  11. the invoice business key stays unique;
  12. every Batch 1 account id survives to the final table (non-destructive writes).

Usage::

    python -m tools.verify_outputs

Writes ``outputs/artifacts/verify_outputs.md`` and exits non-zero on failure.
A crash is reported as a failed check rather than a traceback, so the artifact
is always produced.
"""

import glob
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src import config

REPORT = config.ARTIFACTS_DIR / "verify_outputs.md"
REUSED_IDS = ["ACC-0096", "ACC-0019", "ACC-0063"]
EXPECTED_COUNTS = (110, 6, 480, 2, 4)

_checks: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    _checks.append((name, bool(ok), detail))
    return bool(ok)


def normalize_dates(frame: pd.DataFrame) -> pd.DataFrame:
    """Promote ``datetime.date`` columns (how pyarrow reads Spark DATE) to datetime64."""
    for column in frame.columns:
        if frame[column].dtype != object:
            continue
        sample = frame[column].dropna()
        if len(sample) and isinstance(sample.iloc[0], date) and not isinstance(sample.iloc[0], datetime):
            frame[column] = pd.to_datetime(frame[column])
    return frame


def read_bronze_masked_emails() -> set[str]:
    values: set[str] = set()
    for path in glob.glob(str(config.ROOT / "data" / "**" / "*.csv"), recursive=True):
        frame = pd.read_csv(path, dtype=str, on_bad_lines="skip")
        if "email_masked" in frame.columns:
            values |= set(frame["email_masked"].dropna())
    return values


def run_checks() -> None:
    accounts = normalize_dates(pd.read_parquet(config.SILVER_BILLING_ACCOUNTS))
    plans = normalize_dates(pd.read_parquet(config.SILVER_PLAN_CATALOG))
    invoices = normalize_dates(pd.read_parquet(config.SILVER_INVOICES))
    account_q = normalize_dates(pd.read_parquet(config.QUARANTINE_BILLING_ACCOUNTS))
    invoice_q = normalize_dates(pd.read_parquet(config.QUARANTINE_INVOICES))

    check(
        "Table row counts",
        (len(accounts), len(plans), len(invoices), len(account_q), len(invoice_q)) == EXPECTED_COUNTS,
        f"accounts={len(accounts)}, plans={len(plans)}, invoices={len(invoices)}, "
        f"accounts_quarantine={len(account_q)}, invoices_quarantine={len(invoice_q)} "
        f"(expected {', '.join(str(n) for n in EXPECTED_COUNTS)})",
    )

    prices = pd.to_numeric(plans["monthly_price"], errors="coerce")
    typed = (accounts.dtypes["signup_date"].kind == "M"
             and accounts.dtypes["valid_from"].kind == "M"
             and accounts.dtypes["valid_to"].kind == "M"
             and accounts.dtypes["is_current"] == bool
             and not prices.isna().any())
    check("Explicit typed columns (no inferred strings)", typed,
          "signup_date/valid_from/valid_to are dates, is_current is bool, monthly_price is numeric")

    check("plan_id casing normalized", bool(accounts["plan_id"].str.isupper().all()),
          "every plan_id is upper case")

    unrecognized = account_q[account_q["reason"] == "unrecognized_format"]
    check(
        "Batch 3 unrecognized date quarantined, not guessed",
        len(unrecognized) == 1
        and unrecognized.iloc[0]["account_id"] == "ACC-0104"
        and "ACC-0104" not in set(accounts["account_id"]),
        "ACC-0104 -> unrecognized_format, absent from silver",
    )

    open_rows = accounts[accounts["is_current"]]
    max_open = int(open_rows.groupby("entity_id").size().max()) if len(open_rows) else 0
    closed = accounts[~accounts["is_current"]]
    inverted = int((closed["valid_to"] <= closed["valid_from"]).sum())
    check(
        "Exactly one open row per entity, no inverted interval",
        max_open == 1 and inverted == 0 and bool(open_rows["valid_to"].isna().all()),
        f"max open rows per entity_id={max_open}, inverted intervals={inverted}",
    )

    def plan_as_of(entity_id: str, day: str) -> list[str]:
        when = pd.Timestamp(day)
        hit = accounts[
            (accounts["entity_id"] == entity_id)
            & (accounts["valid_from"] <= when)
            & (accounts["valid_to"].isna() | (accounts["valid_to"] > when))
        ]
        return sorted(hit["plan_id"].tolist())

    history = accounts[accounts["account_id"] == "ACC-0068"].sort_values("valid_from")
    as_of_ok = False
    if len(history):
        entity = history.iloc[0]["entity_id"]
        as_of_ok = (plan_as_of(entity, "2025-01-01") == ["PLAN-BASIC"]
                    and plan_as_of(entity, "2026-06-01") == ["PLAN-GOLD"])
    check("SCD2 history is queryable as of a date", as_of_ok,
          "ACC-0068: PLAN-BASIC on 2025-01-01, PLAN-GOLD on 2026-06-01")

    out_of_order = account_q[account_q["reason"] == "out_of_order_effective_date"]
    acc53 = accounts[accounts["account_id"] == "ACC-0053"]
    check(
        "Out-of-order change quarantined, history untouched",
        len(out_of_order) == 1
        and out_of_order.iloc[0]["account_id"] == "ACC-0053"
        and len(acc53) == 1
        and bool(acc53.iloc[0]["is_current"]),
        "ACC-0053 -> out_of_order_effective_date; still one current row in silver",
    )

    reused = accounts[accounts["account_id"].isin(REUSED_IDS)]
    per_id = reused.groupby("account_id")["entity_id"].nunique().to_dict()
    check(
        "Reused account ids resolved to distinct identities",
        all(per_id.get(a) == 2 for a in REUSED_IDS)
        and accounts["entity_id"].nunique() == accounts["account_id"].nunique() + 3,
        f"two identities each for {', '.join(REUSED_IDS)}; "
        f"distinct entity_id={accounts['entity_id'].nunique()}, "
        f"distinct account_id={accounts['account_id'].nunique()}",
    )

    reasons = set(account_q["reason"]) | set(invoice_q["reason"])
    check("Quarantine reasons come from the controlled vocabulary",
          reasons <= set(config.QUARANTINE_REASONS.values()),
          f"observed: {sorted(reasons)}")

    bronze = read_bronze_masked_emails()
    leaked = set(accounts["email_masked"].dropna()) - bronze
    check("email_masked is passed through unchanged (never reconstructed)",
          not leaked and len(bronze) > 0,
          f"{len(bronze)} distinct masked values in the source CSVs; "
          f"{len(leaked)} silver values not found there")

    check("Invoices keep their business key unique", bool(invoices["invoice_id"].is_unique),
          f"{invoices['invoice_id'].nunique()} distinct invoice_id over {len(invoices)} rows")

    baseline = pd.read_csv(config.BATCH_FILES["batch_1_billing_accounts"], dtype=str)
    baseline_ids = set(baseline["account_id"].dropna())
    missing = baseline_ids - set(accounts["account_id"])
    check("Batch 1 account ids all still present (non-destructive writes)",
          not missing,
          f"{len(baseline_ids)} Batch 1 account ids; {len(missing)} missing from silver")


def write_report() -> int:
    passed = sum(1 for _, ok, _ in _checks if ok)
    lines = [
        "# Output verification\n",
        f"Run with `python -m tools.verify_outputs`. Result: **{passed}/{len(_checks)} checks passed**.\n",
        "| # | Check | Result | Detail |",
        "| --- | --- | --- | --- |",
    ]
    for i, (name, ok, detail) in enumerate(_checks, start=1):
        lines.append(f"| {i} | {name} | {'PASS' if ok else 'FAIL'} | {detail} |")
    lines.append("")
    lines.append(
        "Tables read back from disk: `silver/silver_billing_accounts`, "
        "`silver/silver_plan_catalog`, `silver/silver_invoices`, "
        "`quarantine/billing_accounts_quarantine`, `quarantine/invoices_quarantine`.\n"
    )
    text = "\n".join(lines)
    Path(REPORT).parent.mkdir(parents=True, exist_ok=True)
    Path(REPORT).write_text(text, encoding="utf-8")
    print(text)
    return 0 if passed == len(_checks) else 1


def main() -> int:
    try:
        run_checks()
    except Exception as exc:  # noqa: BLE001 - a verifier must always report
        check("Verifier ran to completion", False, f"{type(exc).__name__}: {exc}")
    return write_report()


if __name__ == "__main__":
    sys.exit(main())
