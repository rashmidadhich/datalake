"""Task 9: tests for Task 7 (Entity resolution of reused account ids)."""

from datetime import date

from pyspark.sql import functions as F

from src import entity_resolution

EXISTING_SCHEMA = "entity_id string, account_id string, customer_name string, signup_date date"
INCOMING_SCHEMA = (
    "account_id string, customer_name string, email_masked string, signup_date date, "
    "source_system string, plan_id string, status string"
)


def test_reused_key_becomes_a_distinct_identity(spark):
    existing = spark.createDataFrame([
        ("ACC-1", "ACC-1", "Leo Lund", date(2024, 11, 25)),
    ], EXISTING_SCHEMA)
    incoming = spark.createDataFrame([
        ("ACC-1", "Priya Okoye", "p***@x.com", date(2026, 7, 23), "platform_v2", "PLAN-STUDENT", "active"),
    ], INCOMING_SCHEMA)

    res = entity_resolution.resolve_entities(incoming, existing)
    row = res.resolved.collect()[0]
    assert row["identity_status"] == entity_resolution.STATUS_REUSED
    assert row["entity_id"] == "ACC-1:R"
    assert row["entity_id"] != row["account_id"]


def test_new_key_keeps_account_id_as_identity(spark):
    existing = spark.createDataFrame([
        ("ACC-1", "ACC-1", "Leo Lund", date(2024, 11, 25)),
    ], EXISTING_SCHEMA)
    incoming = spark.createDataFrame([
        ("ACC-99", "New Person", "n***@x.com", date(2026, 1, 1), "platform_v2", "PLAN-GOLD", "active"),
    ], INCOMING_SCHEMA)

    res = entity_resolution.resolve_entities(incoming, existing)
    row = res.resolved.collect()[0]
    assert row["identity_status"] == entity_resolution.STATUS_NEW
    assert row["entity_id"] == "ACC-99"


def test_batch5_three_reused_keys_become_three_identities(spark, pipeline_run, silver):
    accounts = silver["accounts"]
    reused = accounts.filter(F.col("entity_id").endswith(":R"))
    assert {r["entity_id"] for r in reused.collect()} == {"ACC-0096:R", "ACC-0019:R", "ACC-0063:R"}

    for key, legacy_name in [("ACC-0096", "Leo Lund"), ("ACC-0019", "Zoe Costa"), ("ACC-0063", "Mia Petrov")]:
        rows = accounts.filter(F.col("account_id") == key).collect()
        by_entity = {r["entity_id"]: r for r in rows}
        assert len(by_entity) == 2, f"{key} should have exactly two identities"
        # Original legacy owner is untouched and still current.
        original = by_entity[key]
        assert original["customer_name"] == legacy_name
        assert original["is_current"] is True
        assert original["source_system"] == "legacy_v1"
        # The newcomer is a distinct identity with a different name, never merged.
        newcomer = by_entity[f"{key}:R"]
        assert newcomer["customer_name"] != legacy_name
        assert newcomer["source_system"] == "platform_v2"


def test_masked_email_is_never_exposed_or_unmasked(silver):
    emails = [r["email_masked"] for r in silver["accounts"].collect()]
    assert all(e is not None and "***" in e for e in emails)
