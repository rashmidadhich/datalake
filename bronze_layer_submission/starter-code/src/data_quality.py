"""Data quality validation - Task 7 (Required AI-Native Activity) and Extended Checks.

Implements row-level content checks that separate records safe for Bronze
from records that need review/quarantine, including:
1. Business key null or blank validation
2. Required non-key field presence validation
3. Intra-batch duplicate checks
4. Rejection reason tagging and audit metrics
"""
from typing import Any
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def validate_data_quality(
    df: DataFrame,
    business_key: str,
    required_fields: list[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Split rows on a required business key, retaining invalid rows with a reason.

    Checks:
    - Primary rule: business key is not null, not empty, and not whitespace-only.
    - Optional rules: any required_fields are not null or blank.

    Valid rows and quarantined rows both retain all source columns and ingestion
    metadata (batch_id, source_file, ingestion_timestamp).
    Quarantined rows gain a non-empty `rejection_reason`.
    """
    key_value = F.trim(F.col(business_key).cast("string"))
    invalid_key = F.col(business_key).isNull() | (key_value == "")

    # Build condition and reason expressions
    condition = invalid_key
    reason_expr = F.when(invalid_key, F.lit(f"{business_key} is null or blank"))

    if required_fields:
        for field in required_fields:
            if field in df.columns and field != business_key:
                field_val = F.trim(F.col(field).cast("string"))
                field_invalid = F.col(field).isNull() | (field_val == "")
                condition = condition | field_invalid
                reason_expr = reason_expr.when(
                    field_invalid,
                    F.lit(f"{field} is null or blank"),
                )

    reason_expr = reason_expr.otherwise(F.lit("unspecified quality validation failure"))

    valid_df = df.filter(~condition)
    quarantined_df = df.filter(condition).withColumn("rejection_reason", reason_expr)
    return valid_df, quarantined_df


def validate_intra_batch_duplicates(
    df: DataFrame,
    business_key: str,
) -> tuple[DataFrame, DataFrame]:
    """Detect duplicate business keys inside the incoming batch.

    Separates the first occurrence of each business key (valid) from subsequent
    duplicate occurrences (quarantined with rejection reason).
    """
    if business_key not in df.columns:
        return df, df.filter(F.lit(False)).withColumn("rejection_reason", F.lit(""))

    # Use window over business_key to number occurrences
    window_spec = Window.partitionBy(business_key).orderBy(
        F.col("ingestion_timestamp").asc() if "ingestion_timestamp" in df.columns else F.monotonically_increasing_id()
    )
    ranked_df = df.withColumn("_row_num", F.row_number().over(window_spec))

    unique_df = ranked_df.filter(F.col("_row_num") == 1).drop("_row_num")
    duplicates_df = (
        ranked_df.filter(F.col("_row_num") > 1)
        .drop("_row_num")
        .withColumn("rejection_reason", F.lit(f"duplicate {business_key} in incoming batch"))
    )
    return unique_df, duplicates_df


def run_data_quality_suite(
    df: DataFrame,
    business_key: str,
    required_fields: list[str] | None = None,
    check_duplicates: bool = True,
) -> tuple[DataFrame, DataFrame, dict[str, Any]]:
    """Run full data quality suite: null/blank check, duplicate check, and metrics summary.

    Returns:
        (valid_df, combined_quarantined_df, metrics)
    """
    total_read = df.count()
    if total_read == 0:
        empty_q = df.withColumn("rejection_reason", F.lit("").cast("string")).filter(F.lit(False))
        return df, empty_q, {"total_read": 0, "valid": 0, "quarantined": 0, "duplicates": 0}

    # 1. Null / blank check on business key and required fields
    key_valid_df, key_quarantined_df = validate_data_quality(df, business_key, required_fields)
    key_quarantined_count = key_quarantined_df.count()

    # 2. Intra-batch duplicate check
    if check_duplicates:
        valid_df, dup_quarantined_df = validate_intra_batch_duplicates(key_valid_df, business_key)
        dup_count = dup_quarantined_df.count()
    else:
        valid_df = key_valid_df
        dup_quarantined_df = key_valid_df.filter(F.lit(False)).withColumn("rejection_reason", F.lit(""))
        dup_count = 0

    # Combine quarantine datasets if duplicates were flagged
    if dup_count > 0:
        quarantined_df = key_quarantined_df.unionByName(dup_quarantined_df)
    else:
        quarantined_df = key_quarantined_df

    total_quarantined = key_quarantined_count + dup_count
    valid_count = valid_df.count()

    metrics = {
        "total_read": total_read,
        "valid": valid_count,
        "quarantined": total_quarantined,
        "null_or_blank_key_count": key_quarantined_count,
        "intra_batch_duplicates_count": dup_count,
    }
    return valid_df, quarantined_df, metrics
