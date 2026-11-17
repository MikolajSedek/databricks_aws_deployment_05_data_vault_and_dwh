# Databricks notebook source
"""Task 03: Post-pipeline Data Quality (DQX), referential integrity, and Unity Catalog metadata validation.

Uses pure functional composition and concurrent thread-pool dispatch with Spark scheduler pools,
inspired by Mikołaj Sędek's architecture for concurrent PySpark dispatch:
"How to dispatch multiple PySpark jobs on a Databricks Notebook using Python multithreading and Spark pools".
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import TYPE_CHECKING, Final, TypeVar

from pyspark.sql import SparkSession

from src.python.modules.transformations.dq_expectations import get_dq_engine
from src.python.modules.transformations.dq_validation_registry import (
    DQXSuiteSpec,
    ReferentialIntegritySpec,
    build_dqx_suite_specs,
    build_referential_integrity_specs,
    format_dqx_suite_label,
    format_ri_spec_label,
    verify_dqx_suite_spec,
    verify_ri_spec,
)
from src.python.modules.utils.config import DEFAULT_CONFIG, EXPECTED_TABLES_BY_SCHEMA, LakehouseConfig
from src.python.modules.utils.metadata import apply_catalog_and_schema_metadata, build_alter_column_comment_sql

if TYPE_CHECKING:
    from databricks.labs.dqx.engine import DQEngine

DEFAULT_MAX_WORKERS: Final[int] = 8
DEFAULT_SCHEDULER_POOL: Final[str] = "fair_pool"

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


def _set_spark_thread_context(spark: SparkSession, description: str, pool: str = DEFAULT_SCHEDULER_POOL) -> None:
    """Assign thread-local Spark scheduler pool and descriptive job label for cluster tracking."""
    sc = getattr(spark, "sparkContext", None)
    if sc is not None:
        try:
            sc.setLocalProperty("spark.scheduler.pool", pool)
            sc.setJobDescription(description)
        except Exception:
            pass


def _dispatch_concurrent_tasks(
    items: Sequence[TItem],
    worker_fn: Callable[[TItem], TResult],
    label_fn: Callable[[TItem], str],
    max_workers: int = DEFAULT_MAX_WORKERS,
    thread_prefix: str = "SparkPoolWorker",
    error_header: str = "Parallel Spark execution",
) -> dict[str, TResult]:
    """Higher-order function dispatching independent tasks across a thread pool and aggregating results or errors."""
    results: dict[str, TResult] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=thread_prefix) as executor:
        future_to_label = {executor.submit(worker_fn, item): label_fn(item) for item in items}
        for future in as_completed(future_to_label):
            label = future_to_label[future]
            try:
                results[label] = future.result()
            except Exception as exc:
                errors.append(f"[{label}] {exc}")

    if errors:
        raise AssertionError(f"{error_header} encountered {len(errors)} failure(s):\n" + "\n".join(errors))
    return results


def _count_table_rows_in_pool(spark: SparkSession, pool: str, fqn: str) -> int:
    """Read a single table and assert a positive row count inside a thread-isolated Spark pool."""
    _set_spark_thread_context(spark, f"Row count: {fqn}", pool)
    count = spark.read.table(fqn).count()
    if count <= 0:
        raise AssertionError(f"Validation failed: Table {fqn} is empty (row_count={count}).")
    return count


def _verify_all_table_counts_parallel(
    spark: SparkSession,
    config: LakehouseConfig,
    max_workers: int = DEFAULT_MAX_WORKERS,
    pool: str = DEFAULT_SCHEDULER_POOL,
) -> dict[str, int]:
    """Verify non-empty row counts across all 37 tables concurrently via partial function application."""
    worker = partial(_count_table_rows_in_pool, spark, pool)
    return _dispatch_concurrent_tasks(
        items=config.all_expected_table_fqns,
        worker_fn=worker,
        label_fn=str,
        max_workers=max_workers,
        thread_prefix="TableCountWorker",
        error_header="Table count audit",
    )


def _build_bronze_column_comment_ddls(spark: SparkSession, config: LakehouseConfig) -> tuple[str, ...]:
    """Build all `ALTER TABLE ... ALTER COLUMN ... COMMENT` DDL strings for Bronze tables."""
    bronze_tables = EXPECTED_TABLES_BY_SCHEMA.get(config.bronze_schema, [])
    return tuple(
        build_alter_column_comment_sql(config.catalog, config.bronze_schema, table_name, col_name)
        for table_name in bronze_tables
        for col_name in spark.read.table(config.table_fqn(config.bronze_schema, table_name)).columns
    )


def _apply_bronze_column_comments(spark: SparkSession, config: LakehouseConfig) -> None:
    """Execute Unity Catalog column comment DDLs for all columns across Bronze tables."""
    for ddl in _build_bronze_column_comment_ddls(spark, config):
        spark.sql(ddl)


def _evaluate_dqx_suite_in_pool(
    spark: SparkSession,
    engine: DQEngine,
    pool: str,
    spec: DQXSuiteSpec,
) -> None:
    """Evaluate a `DQXSuiteSpec` inside a thread-isolated Spark pool."""
    _set_spark_thread_context(spark, f"DQX audit: {format_dqx_suite_label(spec)}", pool)
    verify_dqx_suite_spec(spark, engine, spec)


def _verify_dqx_suites_parallel(
    spark: SparkSession,
    config: LakehouseConfig,
    engine: DQEngine,
    max_workers: int = DEFAULT_MAX_WORKERS,
    pool: str = DEFAULT_SCHEDULER_POOL,
) -> None:
    """Execute all 20 DQX expectation suites concurrently via partial function application."""
    worker = partial(_evaluate_dqx_suite_in_pool, spark, engine, pool)
    _dispatch_concurrent_tasks(
        items=build_dqx_suite_specs(config.catalog),
        worker_fn=worker,
        label_fn=format_dqx_suite_label,
        max_workers=max_workers,
        thread_prefix="DQXWorker",
        error_header="DQX parallel audit",
    )


def _evaluate_ri_spec_in_pool(
    spark: SparkSession,
    engine: DQEngine,
    pool: str,
    spec: ReferentialIntegritySpec,
) -> None:
    """Evaluate a `ReferentialIntegritySpec` inside a thread-isolated Spark pool."""
    _set_spark_thread_context(spark, f"RI audit: {format_ri_spec_label(spec)}", pool)
    verify_ri_spec(spark, engine, spec)


def _verify_referential_integrity_parallel(
    spark: SparkSession,
    config: LakehouseConfig,
    engine: DQEngine,
    max_workers: int = DEFAULT_MAX_WORKERS,
    pool: str = DEFAULT_SCHEDULER_POOL,
) -> None:
    """Assert zero orphan foreign keys across Data Vault and Kimball models concurrently."""
    worker = partial(_evaluate_ri_spec_in_pool, spark, engine, pool)
    _dispatch_concurrent_tasks(
        items=build_referential_integrity_specs(config.catalog),
        worker_fn=worker,
        label_fn=format_ri_spec_label,
        max_workers=max_workers,
        thread_prefix="RIWorker",
        error_header="Referential integrity audit",
    )


def validate_lakehouse_deployment(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> dict[str, int]:
    """Validate that all 37 tables are populated, documented, and pass DQX and RI checks concurrently."""
    apply_catalog_and_schema_metadata(spark, config)
    dq_engine = get_dq_engine(spark)

    row_counts = _verify_all_table_counts_parallel(spark, config, max_workers=max_workers)
    _apply_bronze_column_comments(spark, config)
    _verify_dqx_suites_parallel(spark, config, dq_engine, max_workers=max_workers)
    _verify_referential_integrity_parallel(spark, config, dq_engine, max_workers=max_workers)

    return row_counts


if __name__ == "__main__":
    active_spark = SparkSession.builder.appName("03_validate_and_audit_dq").getOrCreate()
    counts = validate_lakehouse_deployment(active_spark, DEFAULT_CONFIG)
    for tbl, cnt in counts.items():
        print(f"[VERIFIED] {tbl}: {cnt} rows")
