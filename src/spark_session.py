"""Reusable local SparkSession factory."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "trb-silver-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
