"""Schema-drift detection and reconciliation utilities.

Supports:
- Schema comparison: Added columns, missing columns, data type changes.
- Safe additive schema reconciliation: Backfills historical columns with NULLs.
- Structured schema-drift event logging for audit and compliance.
"""
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def compare_schemas(existing_schema: StructType, incoming_schema: StructType) -> dict[str, Any]:
    """Compare fields by name and report additions, omissions, and type changes."""
    existing_fields = {field.name: field for field in existing_schema.fields}
    incoming_fields = {field.name: field for field in incoming_schema.fields}

    added_columns = [
        name for name in incoming_fields if name not in existing_fields
    ]
    missing_columns = [
        name for name in existing_fields if name not in incoming_fields
    ]
    type_changes = {
        name: {
            "existing": existing_fields[name].dataType.simpleString(),
            "incoming": incoming_fields[name].dataType.simpleString(),
        }
        for name in existing_fields.keys() & incoming_fields.keys()
        if existing_fields[name].dataType != incoming_fields[name].dataType
    }

    return {
        "added_columns": added_columns,
        "missing_columns": missing_columns,
        "type_changes": type_changes,
    }


def validate_schema_compatibility(
    existing_schema: StructType,
    incoming_schema: StructType,
    allow_additive: bool = True,
) -> tuple[bool, str, dict[str, Any]]:
    """Validate whether incoming schema is compatible with existing schema.

    Returns:
        (is_compatible, message, diff)
    """
    diff = compare_schemas(existing_schema, incoming_schema)

    if diff["type_changes"]:
        mismatch_details = "; ".join(
            f"{col}: {c['existing']} -> {c['incoming']}"
            for col, c in diff["type_changes"].items()
        )
        return False, f"Incompatible type changes detected: {mismatch_details}", diff

    if not allow_additive and diff["added_columns"]:
        return False, f"Additive drift not allowed: new columns {diff['added_columns']}", diff

    return True, "Schema is compatible", diff


def reconcile_schema(incoming_df: DataFrame, existing_schema: StructType) -> DataFrame:
    """Align input with the evolved Bronze schema, preserving new fields too."""
    existing_columns = existing_schema.fieldNames()
    new_columns = [
        name for name in incoming_df.columns if name not in existing_columns
    ]

    for field in existing_schema.fields:
        if field.name not in incoming_df.columns:
            incoming_df = incoming_df.withColumn(
                field.name,
                F.lit(None).cast(field.dataType),
            )

    return incoming_df.select(*(existing_columns + new_columns))


def log_drift_event(entity: str, batch_id: str, diff: dict) -> dict:
    """Build a compact event identifying an additive schema change."""
    return {
        "entity": entity,
        "batch_id": batch_id,
        "added_columns": diff["added_columns"],
    }
