"""Task 9: tests for Task 5 / Task 6 (SCD2 history tracking)."""

from datetime import date

from pyspark.sql import functions as F

from src import config, scd2_writer

ACCOUNT_SCHEMA = (
    "entity_id string, account_id string, customer_name string, email_masked string, "
    "signup_date date, source_system string, plan_id string, status string, "
    "valid_from date, valid_to date, is_current boolean"
)
EVENT_SCHEMA = "account_id string, old_plan_id string, new_plan_id string, effective_date date"


def _silver(spark):
    return spark.createDataFrame([
        ("ACC-1", "ACC-1", "Ann", "a***@x.com", date(2024, 1, 1), "legacy_v1",
         "PLAN-BASIC", "active", date(2024, 1, 1), None, True),
        ("ACC-2", "ACC-2", "Bob", "b***@x.com", date(2024, 1, 1), "legacy_v1",
         "PLAN-SILVER", "active", date(2024, 1, 1), None, True),
    ], ACCOUNT_SCHEMA)


def test_normal_change_closes_and_appends(spark):
    events = spark.createDataFrame(
        [("ACC-1", "PLAN-BASIC", "PLAN-GOLD", date(2025, 6, 1))], EVENT_SCHEMA)
    res = scd2_writer.apply_plan_change(events, _silver(spark))

    rows = {r["valid_from"]: r for r in res.updated_silver.filter(F.col("entity_id") == "ACC-1").collect()}
    assert len(rows) == 2
    old = rows[date(2024, 1, 1)]
    new = rows[date(2025, 6, 1)]
    assert old["plan_id"] == "PLAN-BASIC" and old["is_current"] is False
    assert old["valid_to"] == date(2025, 6, 1)
    assert new["plan_id"] == "PLAN-GOLD" and new["is_current"] is True
    assert new["valid_to"] is None


def test_exactly_one_open_row_per_entity(spark):
    events = spark.createDataFrame(
        [("ACC-1", "PLAN-BASIC", "PLAN-GOLD", date(2025, 6, 1))], EVENT_SCHEMA)
    res = scd2_writer.apply_plan_change(events, _silver(spark))
    open_per_entity = (
        res.updated_silver.filter(F.col("is_current"))
        .groupBy("entity_id").count()
        .filter(F.col("count") != 1)
    )
    assert open_per_entity.count() == 0


def test_out_of_order_effective_date_is_quarantined_not_inverted(spark):
    events = spark.createDataFrame(
        [("ACC-2", "PLAN-SILVER", "PLAN-GOLD", date(2023, 1, 1))], EVENT_SCHEMA)
    res = scd2_writer.apply_plan_change(events, _silver(spark))

    assert res.counts["applied"] == 0
    assert res.counts["quarantined"] == 1
    q = res.quarantine.collect()[0]
    assert q["account_id"] == "ACC-2"
    assert q["reason"] == config.QUARANTINE_REASONS["OUT_OF_ORDER_EFFECTIVE_DATE"]
    # ACC-2's history is untouched: one row, still open, still SILVER.
    acc2 = res.updated_silver.filter(F.col("entity_id") == "ACC-2")
    assert acc2.count() == 1
    assert acc2.collect()[0]["is_current"] is True


def test_batch4_applies_four_and_quarantines_the_out_of_order_one(spark, pipeline_run, silver):
    assert pipeline_run["batch_4"]["applied"] == 4
    assert pipeline_run["batch_4"]["quarantined"] == 1

    accounts = silver["accounts"]
    # Global invariant across the whole silver table.
    multi_open = accounts.filter(F.col("is_current")).groupBy("entity_id").count().filter(F.col("count") > 1)
    assert multi_open.count() == 0

    # ACC-0068 moved PLAN-BASIC -> PLAN-GOLD effective 2026-03-18.
    hist = accounts.filter(F.col("entity_id") == "ACC-0068").orderBy("valid_from")
    rows = hist.collect()
    assert len(rows) == 2
    assert rows[0]["plan_id"] == "PLAN-BASIC"
    assert rows[0]["valid_to"] == date(2026, 3, 18)
    assert rows[1]["plan_id"] == "PLAN-GOLD"
    assert rows[1]["is_current"] is True


def test_plan_as_of_date_is_queryable(spark, silver):
    accounts = silver["accounts"].filter(F.col("entity_id") == "ACC-0068")

    def plan_as_of(when):
        return (
            accounts
            .filter((F.col("valid_from") <= F.lit(when)) &
                    (F.col("valid_to").isNull() | (F.col("valid_to") > F.lit(when))))
            .collect()[0]["plan_id"]
        )

    assert plan_as_of(date(2025, 1, 1)) == "PLAN-BASIC"
    assert plan_as_of(date(2026, 6, 1)) == "PLAN-GOLD"
