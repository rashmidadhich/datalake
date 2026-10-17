"""Orchestrates Batch 1 through Batch 5 end to end.

TODO: wire together ingest -> type_casting -> dedup -> entity_resolution ->
scd2_writer -> guardrails checks -> write to silver/ or quarantine/, per the
Required Architecture workflow in the assignment doc:

    Read bronze batch -> Cast and normalize types -> Deduplicate against
    existing silver rows -> Resolve entity identity for the business key ->
    Apply SCD2 logic for any tracked attribute change -> Write to silver
    output

Run each batch (1-5) in order, printing/logging row counts at each stage so
guardrail conservation can be checked.
"""

from src.spark_session import get_spark


def main():
    spark = get_spark()
    # TODO: Batch 1 - initial load (Task 1)
    # TODO: Batch 2 - dedup (Task 2)
    # TODO: Batch 3 - type casting (Task 3)
    # TODO: Batch 4 - SCD2 plan change (Task 5/Task 6)
    # TODO: Batch 5 - entity resolution (Task 7)
    # TODO: run guardrail checks after each batch (Task 8)
    spark.stop()


if __name__ == "__main__":
    main()
