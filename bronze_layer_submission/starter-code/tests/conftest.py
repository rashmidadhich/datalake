"""Shared pytest fixtures. This file is complete boilerplate - no need to
change it (see Helper Guide's testing tips: keep shuffle partitions low for
fast local unit tests)."""
import pytest

from src.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    spark = get_spark("bronze-ingestion-tests")
    yield spark
    spark.stop()
