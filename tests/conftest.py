"""Shared pytest fixtures including a fast session-scoped local PySpark SparkSession."""

import os
import sys
from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure project root is on sys.path for local testing across all test modules
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_SPARK_SESSION = (
    SparkSession.builder.master("local[1]")
    .appName("pytest-data-vault-kimball-dwh")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a fast, isolated local[1] SparkSession for unit testing pure PySpark transformations."""
    yield _SPARK_SESSION
