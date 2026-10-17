"""Read bronze CSVs into DataFrames.

TODO (Task 1): implement baseline loads for billing_accounts, plan_catalog,
and invoices. TODO: implement a generic batch reader used by Tasks 2-7 for the
sample_batches/ files.
"""

from pyspark.sql import DataFrame, SparkSession

from . import config


def read_billing_accounts(spark: SparkSession, path=None) -> DataFrame:
    """TODO: read data/billing_accounts.csv (or the given batch path) with an
    explicit schema (do not rely on inferSchema for production-style code)."""
    raise NotImplementedError


def read_plan_catalog(spark: SparkSession) -> DataFrame:
    """TODO: read data/plan_catalog.csv."""
    raise NotImplementedError


def read_invoices(spark: SparkSession, path=None) -> DataFrame:
    """TODO: read data/invoices.csv (or the given batch path)."""
    raise NotImplementedError
