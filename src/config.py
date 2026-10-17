"""Central path configuration. Adjust if you reorganize the project."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = ROOT / "data"
BATCHES_DIR = DATA_DIR / "sample_batches"

SILVER_DIR = ROOT / "silver"
QUARANTINE_DIR = ROOT / "quarantine"

SILVER_BILLING_ACCOUNTS = SILVER_DIR / "silver_billing_accounts"
SILVER_PLAN_CATALOG = SILVER_DIR / "silver_plan_catalog"
SILVER_INVOICES = SILVER_DIR / "silver_invoices"

QUARANTINE_BILLING_ACCOUNTS = QUARANTINE_DIR / "billing_accounts_quarantine"
QUARANTINE_INVOICES = QUARANTINE_DIR / "invoices_quarantine"

# Controlled quarantine reason vocabulary - extend only if your
# pipeline_guardrails/SKILL.md documents the addition.
QUARANTINE_REASONS = {
    "CONFLICTING_KEY": "conflicting_key",
    "UNRECOGNIZED_FORMAT": "unrecognized_format",
    "OUT_OF_ORDER_EFFECTIVE_DATE": "out_of_order_effective_date",
}
