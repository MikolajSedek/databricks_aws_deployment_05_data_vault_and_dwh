# Databricks notebook source
"""Lakeflow Declarative Pipeline (`pyspark.pipelines`) — Gold Kimball Dimensional DWH & Analyst/ML Views."""

import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.python.modules.kimball_dwh.consumer_views import (
    build_vw_customer_360,
    build_vw_executive_sales_kpis,
    build_vw_ml_customer_churn_features,
    build_vw_ml_product_affinity_features,
    build_vw_product_performance,
)
from src.python.modules.kimball_dwh.dimensions import (
    build_dim_currency,
    build_dim_date,
    build_dim_device,
    build_dim_event_type,
    build_dim_geography,
    build_dim_order_status,
    build_dim_product_category,
    build_dim_products,
    build_dim_users,
)
from src.python.modules.kimball_dwh.facts import (
    build_fct_daily_sales_snapshot,
    build_fct_events,
    build_fct_orders,
)
from src.python.modules.transformations.dq_expectations import (
    KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS,
    KIMBALL_DIM_CURRENCY_EXPECTATIONS,
    KIMBALL_DIM_DATE_EXPECTATIONS,
    KIMBALL_DIM_DEVICE_EXPECTATIONS,
    KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS,
    KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS,
    KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS,
    KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS,
    KIMBALL_DIM_PRODUCTS_EXPECTATIONS,
    KIMBALL_DIM_USERS_EXPECTATIONS,
    KIMBALL_FACT_EVENTS_EXPECTATIONS,
    KIMBALL_FACT_ORDERS_EXPECTATIONS,
    ML_AFFINITY_FEATURES_EXPECTATIONS,
    ML_CHURN_FEATURES_EXPECTATIONS,
    apply_lakeflow_expectations,
    filter_valid_records,
)
from src.python.modules.utils.config import DEFAULT_CONFIG, LakehouseConfig


def register_kimball_dwh_pipeline(
    dp_module: Any,
    spark: SparkSession,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> dict[str, Any]:
    """Register Gold Kimball Conformed Dimensions, Fact Tables, and Analyst/ML Views on Lakeflow.

    Args:
        dp_module: The `pyspark.pipelines` (or mock test) module.
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        Dictionary mapping table name to its registered pipeline function (17 tables/views).
    """
    silver_prefix = f"{config.silver_schema}."
    gold_prefix = f"{config.gold_schema}."

    # --- Conformed Kimball Dimensions (9 Dimensions) ---

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_date",
        comment="Kimball Conformed Calendar Date Dimension (dim_date) with integer smart key sk_date (YYYYMMDD) and fiscal/calendar attributes.",
        cluster_by=["sk_date"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_DATE_EXPECTATIONS)
    def dim_date() -> DataFrame:
        """Build Kimball Conformed Date Dimension dim_date."""
        return filter_valid_records(build_dim_date(spark), KIMBALL_DIM_DATE_EXPECTATIONS)

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_users",
        comment="Kimball Conformed Customer Dimension (dim_users) with SCD Type 2 validity windows, surrogate key sk_user, and Business Vault LTV segments.",
        cluster_by=["sk_user", "user_id"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_USERS_EXPECTATIONS)
    def dim_users() -> DataFrame:
        """Build Kimball Customer Dimension dim_users."""
        return filter_valid_records(
            build_dim_users(
                spark.read.table(f"{silver_prefix}hub_users"),
                spark.read.table(f"{silver_prefix}sat_user_details"),
                spark.read.table(f"{silver_prefix}sat_user_tier_bv"),
            ),
            KIMBALL_DIM_USERS_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_products",
        comment="Kimball Conformed Product Dimension (dim_products) with unit gross margins, SCD Type 2 validity intervals, and surrogate key sk_product.",
        cluster_by=["sk_product", "product_id"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_PRODUCTS_EXPECTATIONS)
    def dim_products() -> DataFrame:
        """Build Kimball Product Dimension dim_products."""
        return filter_valid_records(
            build_dim_products(
                spark.read.table(f"{silver_prefix}hub_products"),
                spark.read.table(f"{silver_prefix}sat_product_details"),
            ),
            KIMBALL_DIM_PRODUCTS_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_geography",
        comment="Kimball Conformed Geography Dimension (dim_geography) mapping country_code to country names, commercial macro regions, subregions, and default billing currencies.",
        cluster_by=["sk_geography"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS)
    def dim_geography() -> DataFrame:
        """Build Kimball Geography Dimension dim_geography."""
        return filter_valid_records(
            build_dim_geography(spark.read.table(f"{silver_prefix}sat_user_details")),
            KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_currency",
        comment="Kimball Conformed Currency Dimension (dim_currency) mapping ISO-4217 currency codes to descriptive labels, symbols, and USD FX rates.",
        cluster_by=["sk_currency"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_CURRENCY_EXPECTATIONS)
    def dim_currency() -> DataFrame:
        """Build Kimball Currency Dimension dim_currency."""
        return filter_valid_records(
            build_dim_currency(spark.read.table(f"{silver_prefix}sat_order_details")),
            KIMBALL_DIM_CURRENCY_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_order_status",
        comment="Kimball Junk/Status Dimension (dim_order_status) capturing lifecycle statuses, value tiers, fulfillment indicators, and reversal flags.",
        cluster_by=["sk_order_status"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS)
    def dim_order_status() -> DataFrame:
        """Build Kimball Order Status Dimension dim_order_status."""
        return filter_valid_records(
            build_dim_order_status(
                spark.read.table(f"{silver_prefix}sat_order_details"),
                spark.read.table(f"{silver_prefix}sat_order_bv"),
            ),
            KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_device",
        comment="Kimball Client Device Dimension (dim_device) classifying clickstream hardware types into mobility and desktop platform groupings.",
        cluster_by=["sk_device"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_DEVICE_EXPECTATIONS)
    def dim_device() -> DataFrame:
        """Build Kimball Device Dimension dim_device."""
        return filter_valid_records(
            build_dim_device(spark.read.table(f"{silver_prefix}sat_event_details")),
            KIMBALL_DIM_DEVICE_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_event_type",
        comment="Kimball Clickstream Funnel Dimension (dim_event_type) mapping user telemetry events to ordered e-commerce conversion funnel stages.",
        cluster_by=["sk_event_type"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS)
    def dim_event_type() -> DataFrame:
        """Build Kimball Event Type Dimension dim_event_type."""
        return filter_valid_records(
            build_dim_event_type(spark.read.table(f"{silver_prefix}sat_event_details")),
            KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}dim_product_category",
        comment="Kimball Conformed Product Category Dimension (dim_product_category) enriching merchandising categories with commercial departments and strategic tiers.",
        cluster_by=["sk_category"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS)
    def dim_product_category() -> DataFrame:
        """Build Kimball Product Category Dimension dim_product_category."""
        return filter_valid_records(
            build_dim_product_category(spark.read.table(f"{silver_prefix}sat_product_details")),
            KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS,
        )

    # --- Kimball Star Schema Fact Tables (3 Facts) ---

    @dp_module.materialized_view(
        name=f"{gold_prefix}fct_orders",
        comment="Kimball Transaction Fact Table (fct_orders) at the order-line grain with conformed dimension foreign keys (sk_date, sk_user, sk_product, sk_order_status, sk_currency, sk_geography, sk_category) and additive revenue measures.",
        cluster_by=["sk_date", "sk_user"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_FACT_ORDERS_EXPECTATIONS)
    def fct_orders() -> DataFrame:
        """Build Kimball Transaction Fact table fct_orders."""
        return filter_valid_records(
            build_fct_orders(
                spark.read.table(f"{silver_prefix}br_order_fulfillment"),
                spark.read.table(f"{gold_prefix}dim_users"),
                spark.read.table(f"{gold_prefix}dim_products"),
                spark.read.table(f"{gold_prefix}dim_geography"),
                spark.read.table(f"{gold_prefix}dim_currency"),
                spark.read.table(f"{gold_prefix}dim_order_status"),
                spark.read.table(f"{gold_prefix}dim_product_category"),
            ),
            KIMBALL_FACT_ORDERS_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}fct_events",
        comment="Kimball Clickstream Interaction Fact Table (fct_events) at individual telemetry event grain linked to conformed date, customer, product, event type, device, geography, and category dimensions.",
        cluster_by=["sk_date", "sk_user"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_FACT_EVENTS_EXPECTATIONS)
    def fct_events() -> DataFrame:
        """Build Kimball Clickstream Fact table fct_events."""
        return filter_valid_records(
            build_fct_events(
                spark.read.table(f"{silver_prefix}hub_events"),
                spark.read.table(f"{silver_prefix}lnk_user_event"),
                spark.read.table(f"{silver_prefix}sat_event_details"),
                spark.read.table(f"{gold_prefix}dim_users"),
                spark.read.table(f"{gold_prefix}dim_products"),
                spark.read.table(f"{gold_prefix}dim_geography"),
                spark.read.table(f"{gold_prefix}dim_event_type"),
                spark.read.table(f"{gold_prefix}dim_device"),
                spark.read.table(f"{gold_prefix}dim_product_category"),
            ),
            KIMBALL_FACT_EVENTS_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}fct_daily_sales_snapshot",
        comment="Kimball Periodic Snapshot Fact Table (fct_daily_sales_snapshot) aggregating daily revenue, units, and order KPIs at the (sk_date, sk_category) grain.",
        cluster_by=["sk_date", "sk_category"],
    )
    @apply_lakeflow_expectations(dp_module, KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS)
    def fct_daily_sales_snapshot() -> DataFrame:
        """Build Kimball Daily Sales Periodic Snapshot Fact table."""
        return filter_valid_records(
            build_fct_daily_sales_snapshot(
                spark.read.table(f"{gold_prefix}fct_orders"),
                spark.read.table(f"{gold_prefix}dim_order_status"),
            ),
            KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS,
        )

    # --- Consumer Views for Data Analysts & ML Engineers (5 Views) ---

    @dp_module.materialized_view(
        name=f"{gold_prefix}vw_executive_sales_kpis",
        comment="Data Analyst BI View providing daily and monthly executive revenue KPIs, order completion rates, and category/department performance across calendar periods.",
    )
    def vw_executive_sales_kpis() -> DataFrame:
        """Build Data Analyst Executive Sales KPIs view."""
        return build_vw_executive_sales_kpis(
            spark.read.table(f"{gold_prefix}fct_daily_sales_snapshot"),
            spark.read.table(f"{gold_prefix}dim_date"),
            spark.read.table(f"{gold_prefix}dim_product_category"),
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}vw_customer_360",
        comment="Data Analyst Customer 360 View combining customer demographics, geography, loyalty segments, lifetime order revenue, and digital engagement metrics.",
    )
    def vw_customer_360() -> DataFrame:
        """Build Data Analyst Customer 360 view."""
        return build_vw_customer_360(
            spark.read.table(f"{gold_prefix}dim_users"),
            spark.read.table(f"{gold_prefix}dim_geography"),
            spark.read.table(f"{gold_prefix}fct_orders"),
            spark.read.table(f"{gold_prefix}dim_order_status"),
            spark.read.table(f"{gold_prefix}fct_events"),
            spark.read.table(f"{gold_prefix}dim_event_type"),
            spark.read.table(f"{gold_prefix}dim_date"),
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}vw_product_performance",
        comment="Data Analyst Merchandising View tracking product gross margins, departments, unit sales velocity, unique buyers, and clickstream cart-to-purchase conversion.",
    )
    def vw_product_performance() -> DataFrame:
        """Build Data Analyst Product Performance view."""
        return build_vw_product_performance(
            spark.read.table(f"{gold_prefix}dim_products"),
            spark.read.table(f"{gold_prefix}dim_product_category"),
            spark.read.table(f"{gold_prefix}fct_orders"),
            spark.read.table(f"{gold_prefix}fct_events"),
            spark.read.table(f"{gold_prefix}dim_event_type"),
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}vw_ml_customer_churn_features",
        comment="ML Engineer Feature View providing RFM (Recency, Frequency, Monetary) features, session behavioral ratios, and churn propensity scores for ML training.",
    )
    @apply_lakeflow_expectations(dp_module, ML_CHURN_FEATURES_EXPECTATIONS)
    def vw_ml_customer_churn_features() -> DataFrame:
        """Build ML Engineer Customer Churn Features view."""
        return filter_valid_records(
            build_vw_ml_customer_churn_features(
                spark.read.table(f"{gold_prefix}dim_users"),
                spark.read.table(f"{gold_prefix}fct_orders"),
                spark.read.table(f"{gold_prefix}dim_order_status"),
                spark.read.table(f"{gold_prefix}fct_events"),
                spark.read.table(f"{gold_prefix}dim_event_type"),
                spark.read.table(f"{gold_prefix}dim_date"),
            ),
            ML_CHURN_FEATURES_EXPECTATIONS,
        )

    @dp_module.materialized_view(
        name=f"{gold_prefix}vw_ml_product_affinity_features",
        comment="ML Engineer Feature View providing repeat purchase rates, category revenue shares, and composite product affinity scores for recommendation models.",
    )
    @apply_lakeflow_expectations(dp_module, ML_AFFINITY_FEATURES_EXPECTATIONS)
    def vw_ml_product_affinity_features() -> DataFrame:
        """Build ML Engineer Product Affinity Features view."""
        return filter_valid_records(
            build_vw_ml_product_affinity_features(
                spark.read.table(f"{gold_prefix}dim_products"),
                spark.read.table(f"{gold_prefix}fct_orders"),
                spark.read.table(f"{gold_prefix}fct_events"),
                spark.read.table(f"{gold_prefix}dim_event_type"),
            ),
            ML_AFFINITY_FEATURES_EXPECTATIONS,
        )

    return {
        "dim_date": dim_date,
        "dim_users": dim_users,
        "dim_products": dim_products,
        "dim_geography": dim_geography,
        "dim_currency": dim_currency,
        "dim_order_status": dim_order_status,
        "dim_device": dim_device,
        "dim_event_type": dim_event_type,
        "dim_product_category": dim_product_category,
        "fct_orders": fct_orders,
        "fct_events": fct_events,
        "fct_daily_sales_snapshot": fct_daily_sales_snapshot,
        "vw_executive_sales_kpis": vw_executive_sales_kpis,
        "vw_customer_360": vw_customer_360,
        "vw_product_performance": vw_product_performance,
        "vw_ml_customer_churn_features": vw_ml_customer_churn_features,
        "vw_ml_product_affinity_features": vw_ml_product_affinity_features,
    }


if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    try:
        from pyspark import pipelines as dp  # type: ignore[attr-defined]

        _active_spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        register_kimball_dwh_pipeline(dp, _active_spark, DEFAULT_CONFIG)
    except ImportError:
        pass
