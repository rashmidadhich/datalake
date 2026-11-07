"""Read bronze CSVs into DataFrames and write silver/quarantine tables.

Readers use **explicit schemas** (never ``inferSchema`` for production-style
code) and add Spark's ``_corrupt_record`` column so that a row which does not
have the expected number of fields (for example an unquoted comma inside a
date) is captured and can be quarantined instead of silently shifted or
dropped.

Writers are **read-modify-write**: the complete logical table (existing rows
plus the batch delta) is recomputed and written back. This keeps writes
non-destructive (earlier rows are preserved) and makes re-runs idempotent
(identical inputs produce an identical table).
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from . import config

# --------------------------------------------------------------------------- #
# Raw column orders
# --------------------------------------------------------------------------- #
ACCOUNT_COLS = [
    "account_id",
    "customer_name",
    "email_masked",
    "signup_date",
    "source_system",
    "plan_id",
    "status",
]
PLAN_COLS = ["plan_id", "plan_name", "monthly_price", "tier"]
INVOICE_COLS = [
    "invoice_id",
    "account_id",
    "invoice_date",
    "amount_due",
    "currency",
    "payment_status",
]

CORRUPT_COL = "_corrupt_record"

QUARANTINE_ACCOUNT_COLS = ACCOUNT_COLS + ["reason", "batch_id"]
QUARANTINE_INVOICE_COLS = INVOICE_COLS + ["reason", "batch_id"]


def _raw_schema(cols) -> StructType:
    fields = [StructField(c, StringType(), True) for c in cols]
    fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


def _read_raw(spark: SparkSession, path, cols) -> DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(_raw_schema(cols))
        .csv(str(path))
    )


# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #
def read_accounts_raw(spark: SparkSession, path=None) -> DataFrame:
    """Raw (all-string) billing_accounts rows, including ``_corrupt_record``."""
    return _read_raw(spark, path or config.BATCH_FILES["batch_1_billing_accounts"], ACCOUNT_COLS)


def read_invoices_raw(spark: SparkSession, path=None) -> DataFrame:
    """Raw (all-string) invoice rows, including ``_corrupt_record``."""
    return _read_raw(spark, path or config.BATCH_FILES["batch_1_invoices"], INVOICE_COLS)


def read_plan_catalog(spark: SparkSession, path=None) -> DataFrame:
    """Typed plan_catalog."""
    raw = _read_raw(spark, path or config.BATCH_FILES["batch_1_plan_catalog"], PLAN_COLS)
    return raw.select(
        F.trim("plan_id").alias("plan_id"),
        F.col("plan_name"),
        F.col("monthly_price").cast(DecimalType(10, 2)).alias("monthly_price"),
        F.col("tier"),
    )


def typed_account_columns(df: DataFrame) -> DataFrame:
    """Project raw account columns to their canonical silver types.

    ``signup_date`` here assumes the legacy ``yyyy-MM-dd`` pattern; batches with
    other formats must go through the type-casting step first.
    """
    return df.select(
        F.trim("account_id").alias("account_id"),
        F.col("customer_name"),
        F.col("email_masked"),
        F.to_date(F.trim("signup_date"), config.LEGACY_DATE_FORMAT).alias("signup_date"),
        F.trim("source_system").alias("source_system"),
        F.upper(F.trim("plan_id")).alias("plan_id"),
        F.trim("status").alias("status"),
    )


def typed_invoice_columns(df: DataFrame) -> DataFrame:
    """Project raw invoice columns to their canonical silver types."""
    return df.select(
        F.trim("invoice_id").alias("invoice_id"),
        F.trim("account_id").alias("account_id"),
        F.to_date(F.trim("invoice_date"), config.LEGACY_DATE_FORMAT).alias("invoice_date"),
        F.col("amount_due").cast(DecimalType(10, 2)).alias("amount_due"),
        F.upper(F.trim("currency")).alias("currency"),
        F.trim("payment_status").alias("payment_status"),
    )


def read_baseline_accounts(spark: SparkSession) -> DataFrame:
    return typed_account_columns(read_accounts_raw(spark, config.BATCH_FILES["batch_1_billing_accounts"]))


def read_baseline_invoices(spark: SparkSession) -> DataFrame:
    return typed_invoice_columns(read_invoices_raw(spark, config.BATCH_FILES["batch_1_invoices"]))


PLAN_CHANGE_COLS = ["account_id", "old_plan_id", "new_plan_id", "effective_date"]


def read_plan_change_batch(spark: SparkSession, path=None) -> DataFrame:
    """Typed plan-change events for the SCD2 step."""
    raw = _read_raw(spark, path or config.BATCH_FILES["batch_4_plan_change"], PLAN_CHANGE_COLS)
    return raw.select(
        F.trim("account_id").alias("account_id"),
        F.upper(F.trim("old_plan_id")).alias("old_plan_id"),
        F.upper(F.trim("new_plan_id")).alias("new_plan_id"),
        F.to_date(F.trim("effective_date"), config.LEGACY_DATE_FORMAT).alias("effective_date"),
    )


# --------------------------------------------------------------------------- #
# Quarantine row builders
# --------------------------------------------------------------------------- #
def quarantine_accounts(df: DataFrame, reason: str, batch_id: str) -> DataFrame:
    """Force a DataFrame onto the accounts-quarantine schema."""
    return df.select(
        *[F.col(c).cast(StringType()).alias(c) for c in ACCOUNT_COLS],
        F.lit(reason).alias("reason"),
        F.lit(batch_id).alias("batch_id"),
    )


def quarantine_invoices(df: DataFrame, reason: str, batch_id: str) -> DataFrame:
    """Force a DataFrame onto the invoices-quarantine schema."""
    return df.select(
        *[F.col(c).cast(StringType()).alias(c) for c in INVOICE_COLS],
        F.lit(reason).alias("reason"),
        F.lit(batch_id).alias("batch_id"),
    )


# --------------------------------------------------------------------------- #
# Writers (read-modify-write: non-destructive + idempotent)
# --------------------------------------------------------------------------- #
def read_table(spark: SparkSession, path) -> DataFrame | None:
    """Return the Parquet table at ``path``, or ``None`` when it does not exist."""
    if Path(path).exists():
        return spark.read.parquet(str(path))
    return None


def write_table(df: DataFrame, path, checkpoint: bool = False) -> None:
    """Write ``df`` to ``path`` (overwrite).

    ``checkpoint=True`` materializes the DataFrame first, which is required
    when ``df`` still depends on the same path being overwritten - otherwise
    Spark deletes the target directory before the write plan re-reads it.
    """
    if checkpoint:
        df = df.localCheckpoint(eager=True)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))

def materialize(df: DataFrame) -> DataFrame:
    """Materialize ``df`` locally, truncating its lineage.

    Used for any DataFrame that must survive a write to a table it was derived
    from (Spark deletes the target directory before an overwrite write, which
    would otherwise invalidate still-lazy lineage).
    """
    return df.localCheckpoint(eager=True)


def merge_append(spark: SparkSession, delta: DataFrame, path) -> DataFrame:
    """Union ``delta`` onto the existing table and de-duplicate the whole table.

    De-duplicating on every column makes the write idempotent: replaying the
    same batch adds nothing the second time.
    """
    existing = read_table(spark, path)
    combined = delta if existing is None else existing.unionByName(delta, allowMissingColumns=True)
    combined = combined.distinct()
    write_table(combined, path, checkpoint=True)
    return combined


def append_quarantine(spark: SparkSession, delta: DataFrame, path) -> DataFrame:
    """Append quarantine rows without ever duplicating an identical row."""
    return merge_append(spark, delta, path)
