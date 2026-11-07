"""Command-line inspection of generated Bronze and quarantine Parquet data."""
import argparse
from pathlib import Path

from .config import BRONZE_DIR, QUARANTINE_DIR
from .spark_session import get_spark


def _show_dataset(spark, label: str, path: Path, limit: int) -> None:
    """Print an accessible schema, count, and sample for one Parquet dataset."""
    if not path.is_dir():
        print(f"{label}: not found at {path}")
        return

    df = spark.read.parquet(str(path))
    print(f"\n{'=' * 72}\n{label}\nPath: {path}\nRows: {df.count()}")
    print("Schema:")
    df.printSchema()
    print(f"Sample (up to {limit} rows):")
    df.show(limit, truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Bronze or quarantine Parquet outputs."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Inspect all Bronze tables.")
    group.add_argument("--entity", help="Inspect one Bronze entity, e.g. customers.")
    group.add_argument(
        "--quarantine", metavar="ENTITY", help="Inspect one quarantined entity."
    )
    parser.add_argument(
        "--batch", help="Required with --quarantine, for example batch_5."
    )
    parser.add_argument("--show", type=int, default=10, help="Maximum rows to display.")
    args = parser.parse_args()

    if args.show < 1:
        parser.error("--show must be at least 1")
    if args.quarantine and not args.batch:
        parser.error("--batch is required with --quarantine")
    if args.batch and not args.quarantine:
        parser.error("--batch can only be used with --quarantine")

    spark = get_spark("bronze-output-inspection")
    try:
        if args.all:
            for path in sorted(BRONZE_DIR.glob("bronze_*")):
                _show_dataset(spark, path.name, path, args.show)
        elif args.entity:
            _show_dataset(
                spark,
                f"bronze_{args.entity}",
                BRONZE_DIR / f"bronze_{args.entity}",
                args.show,
            )
        else:
            path = QUARANTINE_DIR / args.quarantine / args.batch
            _show_dataset(
                spark,
                f"quarantine_{args.quarantine}_{args.batch}",
                path,
                args.show,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
