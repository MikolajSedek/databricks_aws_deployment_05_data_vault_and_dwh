"""Auto Loader streaming reader functions for ingesting Parquet files from Managed Volumes."""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.python.modules.utils.config import COL_LDTS, COL_RSRC, COL_SOURCE_FILE, LakehouseConfig
from src.python.modules.utils.metadata import attach_column_descriptions


def build_audit_columns_map(record_source: str, file_col: Column) -> dict[str, Column]:
    """Construct the dictionary of Bronze audit lineage Column expressions for `.withColumns()`.

    Args:
        record_source: Identifier of the origin system.
        file_col: Column expression resolving the source file path.

    Returns:
        Dictionary mapping (_ldts, _rsrc, _source_file) to their Column expressions.
    """
    return {
        COL_LDTS: F.current_timestamp(),
        COL_RSRC: F.lit(record_source),
        COL_SOURCE_FILE: file_col,
    }


def enrich_bronze_audit_columns(
    df: DataFrame,
    record_source: str,
    source_file_expr: str = "_metadata.file_path",
) -> DataFrame:
    """Attach standard Bronze audit lineage columns (_ldts, _rsrc, _source_file) via `.withColumns()`.

    Args:
        df: Input raw DataFrame from Volume or batch source.
        record_source: Identifier of the origin system.
        source_file_expr: SQL expression or column reference for source file path.

    Returns:
        Enriched Bronze DataFrame with audit metadata and Unity Catalog column comments.
    """
    has_metadata = "_metadata" in df.columns or source_file_expr in df.columns
    file_col = F.col(source_file_expr) if has_metadata else F.lit("VOLUME_PARQUET_STREAM")
    audit_map = build_audit_columns_map(record_source, file_col)
    return df.withColumns(audit_map).transform(attach_column_descriptions)


def read_volume_parquet_stream(
    spark: SparkSession,
    config: LakehouseConfig,
    dataset_name: str,
    schema: T.StructType,
) -> DataFrame:
    """Read Parquet files incrementally from a Unity Catalog Managed Volume using Auto Loader (cloudFiles).

    Args:
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.
        dataset_name: Logical dataset directory ('users', 'products', 'orders', 'events').
        schema: Explicit PySpark StructType schema for deterministic Auto Loader ingestion.

    Returns:
        Streaming DataFrame enriched with audit columns and column descriptions.
    """
    source_path = config.dataset_volume_path(dataset_name)
    raw_stream = (
        spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").schema(schema).load(source_path)
    )
    return enrich_bronze_audit_columns(
        df=raw_stream,
        record_source=f"{config.record_source}:{dataset_name.upper()}",
        source_file_expr="_metadata.file_path",
    )
