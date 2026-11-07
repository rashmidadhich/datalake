"""Reusable SparkSession factory.

Keep one shared factory so every module and test gets the same local Spark
configuration.
"""
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _configure_windows_hadoop() -> None:
    if os.name != "nt":
        return

    local_app_data = Path(
        os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")
    )
    default_home = local_app_data / "hadoop-3.3.5"
    hadoop_home = Path(os.environ.get("HADOOP_HOME", default_home))
    winutils_path = hadoop_home / "bin" / "winutils.exe"
    if not winutils_path.is_file():
        raise FileNotFoundError(
            "Windows local Parquet writes require Hadoop Windows utilities. "
            f"Expected winutils.exe at {winutils_path}. Install matching "
            "winutils.exe and hadoop.dll, then set HADOOP_HOME to their "
            "Hadoop directory. See README.md, Windows prerequisites."
        )

    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = (
        str(winutils_path.parent) + os.pathsep + os.environ.get("PATH", "")
    )


def get_spark(app_name: str = "bronze-ingestion") -> SparkSession:
    """Return a local SparkSession suitable for this assignment.

    Use the local Spark engine and keep shuffle parallelism small for this
    laptop-sized assignment dataset.
    """
    _configure_windows_hadoop()
    # When the virtual environment is activated, using the command name avoids
    # an unquoted Windows path such as "Broze Layer Project" being split by a
    # Spark worker launch. Fall back to the active interpreter if activation
    # has not placed that interpreter on PATH.
    python_on_path = shutil.which("python")
    if python_on_path and Path(python_on_path).resolve() == Path(sys.executable).resolve():
        os.environ["PYSPARK_PYTHON"] = "python"
    else:
        os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()
