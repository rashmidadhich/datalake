"""Main orchestration entry point.

Wires modules together and runs one or more batches end to end with:
- Schema checks and additive drift reconciliation
- Data quality validation and quarantine routing
- Duplicate checks and cross-batch idempotency
- Audit / evidence logging
"""
import argparse
from pathlib import Path

from .audit import write_audit_record
from .config import BATCHES, BRONZE_DIR, ENTITY_BUSINESS_KEYS, QUARANTINE_DIR
from .data_quality import validate_data_quality
from .ingest import ingest_entity
from .idempotent_writer import (
    dedup_against_existing,
    replace_bronze,
    write_bronze,
)
from .schema_utils import (
    compare_schemas,
    log_drift_event,
    reconcile_schema,
)
from .spark_session import get_spark


def run_batch(
    spark,
    batch_id: str,
    bronze_dir: str | Path | None = None,
    quarantine_dir: str | Path | None = None,
) -> dict[str, int]:
    """Load only unseen business keys from one configured batch."""
    batch = next((item for item in BATCHES if item["batch_id"] == batch_id), None)
    if batch is None:
        raise ValueError(f"Unknown batch_id: {batch_id!r}")

    output_dir = Path(bronze_dir) if bronze_dir is not None else BRONZE_DIR
    quarantine_root = (
        Path(quarantine_dir) if quarantine_dir is not None else QUARANTINE_DIR
    )
    rows_written_by_entity = {}
    for entity, source_path in batch["entities"].items():
        df = ingest_entity(spark, entity, str(source_path), batch_id)
        rows_read = df.count()
        target = output_dir / f"bronze_{entity}"
        business_key = ENTITY_BUSINESS_KEYS[entity]
        existing_df = (
            spark.read.parquet(str(target)) if target.exists() else None
        )
        schema_diff = None
        drift_events = []
        if existing_df is not None:
            schema_diff = compare_schemas(existing_df.schema, df.schema)
            if schema_diff["type_changes"]:
                raise ValueError(
                    f"Unsupported schema change for {entity}: {schema_diff}"
                )
            if schema_diff["missing_columns"]:
                print(
                    f"schema alignment: {entity} batch {batch_id} lacks "
                    f"existing columns {schema_diff['missing_columns']}; "
                    "filling them with nulls for new rows"
                )
            if schema_diff["added_columns"]:
                event = log_drift_event(entity, batch_id, schema_diff)
                drift_events.append(event)
                print(f"schema drift detected: {event}")
            df = reconcile_schema(df, existing_df.schema)

        # 1. Data Quality Check
        valid_df, quarantined_df = validate_data_quality(df, business_key)
        quarantined = quarantined_df.count()
        if quarantined:
            quarantine_path = quarantine_root / entity / batch_id
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            quarantined_df.write.mode("overwrite").parquet(str(quarantine_path))

        valid_rows = valid_df.count()

        # 2. Idempotency & Duplicate Check
        existing_keys = (
            existing_df.select(business_key).distinct()
            if existing_df is not None
            else None
        )
        new_rows = dedup_against_existing(valid_df, existing_keys, business_key)
        rows_written = new_rows.count()
        duplicates_skipped = valid_rows - rows_written

        # 3. Bronze Write (additive schema update or append)
        if rows_written:
            if existing_df is not None and schema_diff and schema_diff["added_columns"]:
                combined = existing_df.unionByName(
                    new_rows,
                    allowMissingColumns=True,
                )
                replace_bronze(combined, str(target))
            else:
                write_bronze(new_rows, str(target))

        # 4. Audit Trail Logging
        write_audit_record(
            batch_id=batch_id,
            entity=entity,
            source_file=Path(source_path).name,
            rows_read=rows_read,
            rows_written=rows_written,
            duplicates_skipped=duplicates_skipped,
            drift_events=drift_events,
            quarantined=quarantined,
        )

        rows_written_by_entity[entity] = rows_written
        print(
            f"{entity}: read {rows_read}, valid {valid_rows}, "
            f"wrote {rows_written}, skipped {duplicates_skipped} existing "
            f"{business_key} values, quarantined {quarantined}"
        )

    return rows_written_by_entity


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one bronze ingestion batch.")
    parser.add_argument("--batch", required=True, help="batch_id to run, e.g. batch_1")
    args = parser.parse_args()

    spark = get_spark()
    try:
        run_batch(spark, args.batch)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
