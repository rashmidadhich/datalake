"""Shared pytest fixtures.

A single SparkSession is reused for the whole session, and the full Batch 1-5
pipeline runs once (``pipeline_run``) so integration tests read real outputs
rather than re-running Spark per test.
"""

import hashlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Two task slots instead of one per core: Spark starts a Python worker per slot
# and worker startup dominates the runtime of this suite on Windows.
os.environ.setdefault("TRB_SPARK_MASTER", "local[2]")

from src import config, ingest  # noqa: E402
from src.pipeline import run_all  # noqa: E402
from src.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = get_spark("trb-silver-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def pipeline_run(spark):
    """Run Batch 1 through Batch 5 once; return the summary dict."""
    return run_all(spark)


@pytest.fixture
def silver(spark, pipeline_run):
    """Read every output table after the pipeline has run.

    Function-scoped on purpose. Several tests rewrite the silver tables (the
    idempotency replay re-runs Batches 2-5), and a DataFrame created from a path
    remembers the part-file names it saw when it was read. A session-scoped
    handle reused after such an overwrite would point at files that no longer
    exist, so each test gets a fresh read.
    """
    return {
        "accounts": ingest.read_table(spark, config.SILVER_BILLING_ACCOUNTS),
        "plans": ingest.read_table(spark, config.SILVER_PLAN_CATALOG),
        "invoices": ingest.read_table(spark, config.SILVER_INVOICES),
        "accounts_quarantine": ingest.read_table(spark, config.QUARANTINE_BILLING_ACCOUNTS),
        "invoices_quarantine": ingest.read_table(spark, config.QUARANTINE_INVOICES),
    }


@pytest.fixture(scope="session")
def snapshot(spark):
    """Return a callable producing a content signature of every output table."""

    def _snap():
        tables = {
            "accounts": config.SILVER_BILLING_ACCOUNTS,
            "plans": config.SILVER_PLAN_CATALOG,
            "invoices": config.SILVER_INVOICES,
            "accounts_quarantine": config.QUARANTINE_BILLING_ACCOUNTS,
            "invoices_quarantine": config.QUARANTINE_INVOICES,
        }
        signature = {}
        for name, path in tables.items():
            rows = ingest.read_table(spark, path).collect()
            normalized = sorted(tuple(str(v) for v in row) for row in rows)
            digest = hashlib.md5(repr(normalized).encode("utf-8")).hexdigest()
            signature[name] = (len(normalized), digest)
        return signature

    return _snap
