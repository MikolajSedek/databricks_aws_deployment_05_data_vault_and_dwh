"""Composite Lakeflow pipeline registration combining Data Vault 2.0 and Kimball DWH pipeline modules."""

from typing import Any

from pyspark.sql import SparkSession

from src.python.modules.utils.config import DEFAULT_CONFIG, LakehouseConfig
from src.python.pipelines.lakeflow_data_vault_pipeline import register_data_vault_pipeline
from src.python.pipelines.lakeflow_kimball_dwh_pipeline import register_kimball_dwh_pipeline


def register_lakeflow_pipeline(
    dp_module: Any,
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    """Register both Data Vault 2.0 (20 tables) and Kimball DWH (17 tables/views) on the Lakeflow module.

    Args:
        dp_module: The `pyspark.pipelines` (or mock test) module.
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        Combined dictionary of all 37 registered pipeline functions.
    """
    dv_tables = register_data_vault_pipeline(dp_module, spark, config)
    dwh_tables = register_kimball_dwh_pipeline(dp_module, spark, config)
    return {**dv_tables, **dwh_tables}
