import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.python.worker.reuse", "false")
        .getOrCreate()
    )
    yield session
    session.stop()