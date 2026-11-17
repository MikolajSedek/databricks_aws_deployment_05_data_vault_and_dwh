# Databricks notebook source
"""Lakeflow Declarative Pipeline (`pyspark.pipelines`) — Bronze Ingestion & Data Vault 2.0 (Raw + Business Vault)."""

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.python.modules.data_vault.business_vault import (
    build_br_order_fulfillment,
    build_pit_products,
    build_pit_users,
    build_sat_order_bv,
    build_sat_user_tier_bv,
)
from src.python.modules.data_vault.raw_vault import (
    build_hub_events,
    build_hub_orders,
    build_hub_products,
    build_hub_users,
    build_lnk_order_product,
    build_lnk_user_event,
    build_lnk_user_order,
    build_sat_event_details,
    build_sat_order_details,
    build_sat_product_details,
    build_sat_user_details,
)
from src.python.modules.io.data_generation import (
    EVENTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    USERS_SCHEMA,
)
from src.python.modules.io.volume_readers import read_volume_parquet_stream
from src.python.modules.transformations.cleaning import (
    clean_events_df,
    clean_orders_df,
    clean_products_df,
    clean_users_df,
)
from src.python.modules.transformations.dq_expectations import (
    BRONZE_EVENTS_EXPECTATIONS,
    BRONZE_ORDERS_EXPECTATIONS,
    BRONZE_PRODUCTS_EXPECTATIONS,
    BRONZE_USERS_EXPECTATIONS,
    BUSINESS_VAULT_ORDER_EXPECTATIONS,
    BUSINESS_VAULT_USER_TIER_EXPECTATIONS,
    DATA_VAULT_HUB_EXPECTATIONS,
    DATA_VAULT_SAT_EXPECTATIONS,
    apply_lakeflow_expectations,
    filter_valid_records,
)
from src.python.modules.utils.config import DEFAULT_CONFIG, LakehouseConfig


def register_data_vault_pipeline(
    dp_module: Any,
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    """Register Bronze streaming tables and Silver Data Vault 2.0 (Raw + Business Vault) tables on Lakeflow.

    Args:
        dp_module: The `pyspark.pipelines` (or mock test) module.
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        Dictionary mapping table name to its registered pipeline function (20 tables).
    """
    bronze_prefix = f"{config.bronze_schema}."
    silver_prefix = f"{config.silver_schema}."

    @dp_module.table(
        name=f"{bronze_prefix}raw_users",
        comment="Raw Bronze streaming table of customer registration records ingested from Managed Volume Parquet files via Auto Loader.",
        cluster_by=["user_id"],
    )
    @apply_lakeflow_expectations(dp_module, BRONZE_USERS_EXPECTATIONS)
    def raw_users() -> DataFrame:
        """Ingest raw user Parquet files from Managed Volume with DQX expectations."""
        return filter_valid_records(
            read_volume_parquet_stream(spark, config, "users", USERS_SCHEMA),
            BRONZE_USERS_EXPECTATIONS,
        )

    @dp_module.table(
        name=f"{bronze_prefix}raw_products",
        comment="Raw Bronze streaming table of product catalog SKU records ingested from Managed Volume Parquet files via Auto Loader.",
        cluster_by=["product_id"],
    )
    @apply_lakeflow_expectations(dp_module, BRONZE_PRODUCTS_EXPECTATIONS)
    def raw_products() -> DataFrame:
        """Ingest raw product Parquet files from Managed Volume with DQX expectations."""
        return filter_valid_records(
            read_volume_parquet_stream(spark, config, "products", PRODUCTS_SCHEMA),
            BRONZE_PRODUCTS_EXPECTATIONS,
        )

    @dp_module.table(
        name=f"{bronze_prefix}raw_orders",
        comment="Raw Bronze streaming table of e-commerce order transactions ingested from Managed Volume Parquet files via Auto Loader.",
        cluster_by=["order_id", "user_id"],
    )
    @apply_lakeflow_expectations(dp_module, BRONZE_ORDERS_EXPECTATIONS)
    def raw_orders() -> DataFrame:
        """Ingest raw order Parquet files from Managed Volume with DQX expectations."""
        return filter_valid_records(
            read_volume_parquet_stream(spark, config, "orders", ORDERS_SCHEMA),
            BRONZE_ORDERS_EXPECTATIONS,
        )

    @dp_module.table(
        name=f"{bronze_prefix}raw_events",
        comment="Raw Bronze streaming table of digital clickstream telemetry events ingested from Managed Volume Parquet files via Auto Loader.",
        cluster_by=["event_id", "user_id"],
    )
    @apply_lakeflow_expectations(dp_module, BRONZE_EVENTS_EXPECTATIONS)
    def raw_events() -> DataFrame:
        """Ingest raw clickstream event Parquet files from Managed Volume with DQX expectations."""
        return filter_valid_records(
            read_volume_parquet_stream(spark, config, "events", EVENTS_SCHEMA),
            BRONZE_EVENTS_EXPECTATIONS,
        )

    # --- Raw Vault Hubs ---

    @dp_module.materialized_view(
        name=f"{silver_prefix}hub_users",
        comment="Data Vault 2.0 Raw Vault Hub table storing unique customer business keys (user_id) and SHA-256 surrogate hash keys (hk_user).",
        cluster_by=["hk_user"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def hub_users() -> DataFrame:
        """Build Raw Vault hub_users from cleaned Bronze users."""
        return spark.read.table(f"{bronze_prefix}raw_users").transform(clean_users_df).transform(build_hub_users)

    @dp_module.materialized_view(
        name=f"{silver_prefix}hub_products",
        comment="Data Vault 2.0 Raw Vault Hub table storing unique product SKU business keys (product_id) and SHA-256 surrogate hash keys (hk_product).",
        cluster_by=["hk_product"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def hub_products() -> DataFrame:
        """Build Raw Vault hub_products from cleaned Bronze products."""
        return (
            spark.read.table(f"{bronze_prefix}raw_products").transform(clean_products_df).transform(build_hub_products)
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}hub_orders",
        comment="Data Vault 2.0 Raw Vault Hub table storing unique order transaction business keys (order_id) and SHA-256 surrogate hash keys (hk_order).",
        cluster_by=["hk_order"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def hub_orders() -> DataFrame:
        """Build Raw Vault hub_orders from cleaned Bronze orders."""
        return spark.read.table(f"{bronze_prefix}raw_orders").transform(clean_orders_df).transform(build_hub_orders)

    @dp_module.materialized_view(
        name=f"{silver_prefix}hub_events",
        comment="Data Vault 2.0 Raw Vault Hub table storing unique clickstream telemetry event business keys (event_id) and SHA-256 surrogate hash keys (hk_event).",
        cluster_by=["hk_event"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def hub_events() -> DataFrame:
        """Build Raw Vault hub_events from cleaned Bronze events."""
        return spark.read.table(f"{bronze_prefix}raw_events").transform(clean_events_df).transform(build_hub_events)

    # --- Raw Vault Links ---

    @dp_module.materialized_view(
        name=f"{silver_prefix}lnk_user_order",
        comment="Data Vault 2.0 Raw Vault Link table capturing the relationship between customers (hub_users) and checkout orders (hub_orders).",
        cluster_by=["hk_lnk_user_order", "hk_user"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def lnk_user_order() -> DataFrame:
        """Build Raw Vault lnk_user_order from cleaned Bronze orders."""
        return spark.read.table(f"{bronze_prefix}raw_orders").transform(clean_orders_df).transform(build_lnk_user_order)

    @dp_module.materialized_view(
        name=f"{silver_prefix}lnk_order_product",
        comment="Data Vault 2.0 Raw Vault Link table capturing the line-item association between orders (hub_orders) and products (hub_products).",
        cluster_by=["hk_lnk_order_product", "hk_order"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def lnk_order_product() -> DataFrame:
        """Build Raw Vault lnk_order_product from cleaned Bronze orders."""
        return (
            spark.read.table(f"{bronze_prefix}raw_orders").transform(clean_orders_df).transform(build_lnk_order_product)
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}lnk_user_event",
        comment="Data Vault 2.0 Raw Vault Link table connecting customers (hub_users), products (hub_products), and clickstream interactions (hub_events).",
        cluster_by=["hk_lnk_user_event", "hk_user"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_HUB_EXPECTATIONS)
    def lnk_user_event() -> DataFrame:
        """Build Raw Vault lnk_user_event from cleaned Bronze events."""
        return spark.read.table(f"{bronze_prefix}raw_events").transform(clean_events_df).transform(build_lnk_user_event)

    # --- Raw Vault Satellites ---

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_user_details",
        comment="Data Vault 2.0 Raw Vault Satellite table tracking descriptive customer profile attributes and SHA-256 hashdiff change history.",
        cluster_by=["hk_user"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_SAT_EXPECTATIONS)
    def sat_user_details() -> DataFrame:
        """Build Raw Vault sat_user_details from cleaned Bronze users."""
        return spark.read.table(f"{bronze_prefix}raw_users").transform(clean_users_df).transform(build_sat_user_details)

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_product_details",
        comment="Data Vault 2.0 Raw Vault Satellite table tracking product catalog merchandising attributes, pricing, and SHA-256 hashdiff history.",
        cluster_by=["hk_product"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_SAT_EXPECTATIONS)
    def sat_product_details() -> DataFrame:
        """Build Raw Vault sat_product_details from cleaned Bronze products."""
        return (
            spark.read.table(f"{bronze_prefix}raw_products")
            .transform(clean_products_df)
            .transform(build_sat_product_details)
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_order_details",
        comment="Data Vault 2.0 Raw Vault Satellite table storing order transaction amounts, status, quantities, and SHA-256 hashdiff history.",
        cluster_by=["hk_order"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_SAT_EXPECTATIONS)
    def sat_order_details() -> DataFrame:
        """Build Raw Vault sat_order_details from cleaned Bronze orders."""
        return (
            spark.read.table(f"{bronze_prefix}raw_orders").transform(clean_orders_df).transform(build_sat_order_details)
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_event_details",
        comment="Data Vault 2.0 Raw Vault Satellite table storing clickstream session telemetry, device context, duration, and SHA-256 hashdiff.",
        cluster_by=["hk_event"],
    )
    @apply_lakeflow_expectations(dp_module, DATA_VAULT_SAT_EXPECTATIONS)
    def sat_event_details() -> DataFrame:
        """Build Raw Vault sat_event_details from cleaned Bronze events."""
        return (
            spark.read.table(f"{bronze_prefix}raw_events").transform(clean_events_df).transform(build_sat_event_details)
        )

    # --- Business Vault (PIT, Bridge, BV Satellites) ---

    @dp_module.materialized_view(
        name=f"{silver_prefix}pit_users",
        comment="Data Vault 2.0 Business Vault Point-in-Time (PIT) table resolving active customer satellite timestamps and ghost-record safe hashdiffs.",
        cluster_by=["hk_user"],
    )
    def pit_users() -> DataFrame:
        """Build Business Vault pit_users from hub_users and sat_user_details."""
        return build_pit_users(
            spark.read.table(f"{silver_prefix}hub_users"),
            spark.read.table(f"{silver_prefix}sat_user_details"),
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}pit_products",
        comment="Data Vault 2.0 Business Vault Point-in-Time (PIT) table resolving active product satellite timestamps and ghost-record safe hashdiffs.",
        cluster_by=["hk_product"],
    )
    def pit_products() -> DataFrame:
        """Build Business Vault pit_products from hub_products and sat_product_details."""
        return build_pit_products(
            spark.read.table(f"{silver_prefix}hub_products"),
            spark.read.table(f"{silver_prefix}sat_product_details"),
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_order_bv",
        comment="Data Vault 2.0 Business Vault Satellite applying enterprise order classification rules (value tiers, high-value flags, fulfillment status).",
        cluster_by=["hk_order"],
    )
    @apply_lakeflow_expectations(dp_module, BUSINESS_VAULT_ORDER_EXPECTATIONS)
    def sat_order_bv() -> DataFrame:
        """Build Business Vault sat_order_bv from sat_order_details."""
        return filter_valid_records(
            build_sat_order_bv(spark.read.table(f"{silver_prefix}sat_order_details")),
            BUSINESS_VAULT_ORDER_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}sat_user_tier_bv",
        comment="Data Vault 2.0 Business Vault Satellite computing customer lifetime value (LTV), valid order counts, and behavioral customer segments.",
        cluster_by=["hk_user"],
    )
    @apply_lakeflow_expectations(dp_module, BUSINESS_VAULT_USER_TIER_EXPECTATIONS)
    def sat_user_tier_bv() -> DataFrame:
        """Build Business Vault sat_user_tier_bv from hub_users, lnk_user_order, and sat_order_details."""
        return filter_valid_records(
            build_sat_user_tier_bv(
                spark.read.table(f"{silver_prefix}hub_users"),
                spark.read.table(f"{silver_prefix}lnk_user_order"),
                spark.read.table(f"{silver_prefix}sat_order_details"),
            ),
            BUSINESS_VAULT_USER_TIER_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{silver_prefix}br_order_fulfillment",
        comment="Data Vault 2.0 Business Vault Bridge table pre-joining customer, order, and product Vault keys with enriched order metrics.",
        cluster_by=["hk_order", "hk_user"],
    )
    def br_order_fulfillment() -> DataFrame:
        """Build Business Vault Bridge table br_order_fulfillment."""
        return build_br_order_fulfillment(
            spark.read.table(f"{silver_prefix}hub_users"),
            spark.read.table(f"{silver_prefix}hub_orders"),
            spark.read.table(f"{silver_prefix}hub_products"),
            spark.read.table(f"{silver_prefix}lnk_user_order"),
            spark.read.table(f"{silver_prefix}lnk_order_product"),
            spark.read.table(f"{silver_prefix}sat_order_details"),
            spark.read.table(f"{silver_prefix}sat_order_bv"),
        )

    return {
        "raw_users": raw_users,
        "raw_products": raw_products,
        "raw_orders": raw_orders,
        "raw_events": raw_events,
        "hub_users": hub_users,
        "hub_products": hub_products,
        "hub_orders": hub_orders,
        "hub_events": hub_events,
        "lnk_user_order": lnk_user_order,
        "lnk_order_product": lnk_order_product,
        "lnk_user_event": lnk_user_event,
        "sat_user_details": sat_user_details,
        "sat_product_details": sat_product_details,
        "sat_order_details": sat_order_details,
        "sat_event_details": sat_event_details,
        "pit_users": pit_users,
        "pit_products": pit_products,
        "sat_order_bv": sat_order_bv,
        "sat_user_tier_bv": sat_user_tier_bv,
        "br_order_fulfillment": br_order_fulfillment,
    }


if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    try:
        from pyspark import pipelines as dp  # type: ignore[attr-defined]

        _active_spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        register_data_vault_pipeline(dp, _active_spark, DEFAULT_CONFIG)
    except ImportError:
        pass
