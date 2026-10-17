"""Central path, vocabulary, and column-role configuration.

Column roles (identity vs mutable, identity fingerprint) are declared here so
the transformation modules stay generic and the skills' guidance is applied in
one place, rather than hard-coded per batch.
"""

from pathlib import Path

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
BATCHES_DIR = DATA_DIR / "sample_batches"

SILVER_DIR = ROOT / "silver"
QUARANTINE_DIR = ROOT / "quarantine"
ARTIFACTS_DIR = ROOT / "outputs" / "artifacts"

SILVER_BILLING_ACCOUNTS = SILVER_DIR / "silver_billing_accounts"
SILVER_PLAN_CATALOG = SILVER_DIR / "silver_plan_catalog"
SILVER_INVOICES = SILVER_DIR / "silver_invoices"

QUARANTINE_BILLING_ACCOUNTS = QUARANTINE_DIR / "billing_accounts_quarantine"
QUARANTINE_INVOICES = QUARANTINE_DIR / "invoices_quarantine"

# --------------------------------------------------------------------------- #
# Controlled quarantine reason vocabulary. pipeline_guardrails/SKILL.md requires
# the vocabulary to be declared in exactly one place, with every quarantining
# skill referencing that declaration; this dict is that single declaration.
# Extend it only by documenting the new reason in that skill.
# --------------------------------------------------------------------------- #
QUARANTINE_REASONS = {
    "CONFLICTING_KEY": "conflicting_key",
    "UNRECOGNIZED_FORMAT": "unrecognized_format",
    "OUT_OF_ORDER_EFFECTIVE_DATE": "out_of_order_effective_date",
}

# --------------------------------------------------------------------------- #
# Column classification used by the skills
# --------------------------------------------------------------------------- #
# Deduplication skill: invoices keyed by invoice_id.
INVOICE_KEY = "invoice_id"
INVOICE_IDENTITY_COLUMNS = ["account_id", "invoice_date", "currency"]
INVOICE_MUTABLE_COLUMNS = ["amount_due", "payment_status"]

# Entity-resolution skill: accounts keyed by account_id, with a stable identity
# fingerprint. A source-system change alone is not decisive (same customer can
# migrate systems), so identity is decided on name + signup date.
ACCOUNT_KEY = "account_id"
ACCOUNT_IDENTITY_FINGERPRINT = ["customer_name", "signup_date"]
ACCOUNT_HISTORY_GRAIN = "entity_id"          # surrogate identity = SCD2 grain
ACCOUNT_TRACKED_ATTRIBUTE = "plan_id"
MASKED_COLUMN = "email_masked"

# Type-casting skill: known signup_date formats per source system.
SIGNUP_DATE_FORMATS = {
    "legacy_v1": "yyyy-MM-dd",
    "platform_v2": "MM/dd/yyyy",
}
LEGACY_DATE_FORMAT = "yyyy-MM-dd"

# --------------------------------------------------------------------------- #
# Input files
# --------------------------------------------------------------------------- #
BATCH_FILES = {
    "batch_1_billing_accounts": DATA_DIR / "billing_accounts.csv",
    "batch_1_plan_catalog": DATA_DIR / "plan_catalog.csv",
    "batch_1_invoices": DATA_DIR / "invoices.csv",
    "batch_2_invoices": BATCHES_DIR / "batch_2_duplicate_retry" / "invoices_batch2_duplicate_retry.csv",
    "batch_3_accounts": BATCHES_DIR / "batch_3_date_format_v2" / "billing_accounts_batch3_date_format_v2.csv",
    "batch_4_plan_change": BATCHES_DIR / "batch_4_plan_change" / "billing_accounts_batch4_plan_change.csv",
    "batch_5_reused_accounts": BATCHES_DIR / "batch_5_reused_account_id" / "billing_accounts_batch5_reused_account_id.csv",
}

# Batches evaluated for row-count conservation: (batch_id, entity).
CONSERVATION_BATCHES = [
    ("batch_2_invoices", "invoices"),
    ("batch_3_accounts", "billing_accounts"),
    ("batch_4_plan_change", "billing_accounts"),
    ("batch_5_reused_accounts", "billing_accounts"),
]
