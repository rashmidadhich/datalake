"""Reusable local SparkSession factory.

PySpark on Windows needs ``JAVA_HOME`` and a Hadoop ``winutils.exe`` /
``hadoop.dll`` discoverable on ``PATH``. To keep the project reproducible from a
clean shell, this module fills in conservative defaults *only when the values
are not already set* and the expected directories exist, so it never overrides
a working environment.
"""

import os
import sys

from pyspark.sql import SparkSession

# Documented defaults for this development machine (see README > Environment).
_DEFAULT_JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
_DEFAULT_HADOOP_HOME = r"C:\hadoop"


def _ensure_windows_hadoop_env() -> None:
    if os.name != "nt":
        return

    java_home = os.environ.get("JAVA_HOME")
    if not java_home and os.path.isdir(_DEFAULT_JAVA_HOME):
        java_home = _DEFAULT_JAVA_HOME
        os.environ["JAVA_HOME"] = java_home

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home and os.path.isdir(_DEFAULT_HADOOP_HOME):
        hadoop_home = _DEFAULT_HADOOP_HOME
        os.environ["HADOOP_HOME"] = hadoop_home

    prefix = []
    if java_home:
        prefix.append(os.path.join(java_home, "bin"))
    if hadoop_home:
        prefix.append(os.path.join(hadoop_home, "bin"))
    if prefix:
        os.environ["PATH"] = ";".join(prefix + [os.environ.get("PATH", "")])

    # Workers must use the same interpreter that launched the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(app_name: str = "trb-silver-pipeline") -> SparkSession:
    """Return (creating if needed) a local[*] SparkSession."""
    _ensure_windows_hadoop_env()
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
