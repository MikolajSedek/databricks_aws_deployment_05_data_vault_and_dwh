# Databricks notebook source
"""Task 01: Provision Unity Catalog descriptions and append incremental Parquet event batches to Managed Volume."""

from pyspark.sql import SparkSession

from src.python.modules.io.data_generation import write_parquet_events_to_volume
from src.python.modules.utils.config import DEFAULT_CONFIG, LakehouseConfig
from src.python.modules.utils.metadata import apply_catalog_and_schema_metadata


def run_parquet_generation_job(
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> dict[str, str]:
    """Provision catalog/schema descriptions and append a new batch of synthetic Parquet files to the Managed Volume.

    Args:
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        Dictionary of dataset names to written Managed Volume paths.
    """
    apply_catalog_and_schema_metadata(spark, config)
    written_paths = write_parquet_events_to_volume(
        spark=spark,
        config=config,
        num_users=120,
        num_products=50,
        num_orders=400,
        num_events=800,
        incremental=True,
    )
    return written_paths


if __name__ == "__main__":
    active_spark = SparkSession.builder.appName("01_generate_parquet_events").getOrCreate()
    paths = run_parquet_generation_job(active_spark, DEFAULT_CONFIG)
    for ds_name, ds_path in paths.items():
        print(f"Appended incremental Parquet dataset batch '{ds_name}' -> {ds_path}")
