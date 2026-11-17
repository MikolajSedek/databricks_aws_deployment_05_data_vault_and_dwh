"""Pure PySpark functions building Kimball Fact tables (fct_orders, fct_events, fct_daily_sales_snapshot).

Follows Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark standards:
- Column-level expression helpers (`Column -> Column`)
- Batch `.withColumns({...})` dictionary projections
- Short, modular functions composed via `.transform()`
- Strict Star Schema design with surrogate foreign keys joining conformed dimensions
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.python.modules.data_vault.business_vault import select_latest_satellite_state
from src.python.modules.utils.config import (
    COL_EVENT_ID,
    COL_HK_EVENT,
    COL_HK_ORDER,
    COL_HK_PRODUCT,
    COL_HK_USER,
    COL_LDTS,
    COL_ORDER_ID,
)
from src.python.modules.utils.metadata import attach_column_descriptions


def to_smart_date_key(date_col: Column) -> Column:
    """Convert a DateType Column to an integer smart key in `YYYYMMDD` format."""
    return F.date_format(date_col, "yyyyMMdd").cast("int")


def _resolve_fact_order_keys(df: DataFrame) -> DataFrame:
    """Resolve `sk_date` and ensure non-null dimension foreign keys on `fct_orders` via `.withColumns()`."""
    return df.withColumns(
        {
            "sk_user": F.coalesce(F.col("sk_user"), F.col(COL_HK_USER)),
            "sk_product": F.coalesce(F.col("sk_product"), F.col(COL_HK_PRODUCT)),
            "sk_date": to_smart_date_key(F.col("order_date")),
        }
    )


def build_fct_orders(
    br_order_fulfillment_df: DataFrame,
    dim_users_df: DataFrame,
    dim_products_df: DataFrame,
    dim_geography_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_order_status_df: DataFrame,
    dim_product_category_df: DataFrame,
) -> DataFrame:
    """Build Kimball Transaction Fact Table `fct_orders` at the order-line grain linked to conformed dimensions."""
    current_users = dim_users_df.filter(F.col("is_current")).select(COL_HK_USER, "sk_user", "country_code")
    current_products = dim_products_df.filter(F.col("is_current")).select(COL_HK_PRODUCT, "sk_product", "category")
    geo_dim = dim_geography_df.select("country_code", "sk_geography")
    currency_dim = dim_currency_df.select("currency_code", "sk_currency")
    status_dim = dim_order_status_df.select("order_status", "order_value_tier", "sk_order_status")
    category_dim = dim_product_category_df.select("category", "sk_category")

    joined_df = (
        br_order_fulfillment_df.join(current_users, on=COL_HK_USER, how="left")
        .join(current_products, on=COL_HK_PRODUCT, how="left")
        .join(geo_dim, on="country_code", how="left")
        .join(currency_dim, on="currency_code", how="left")
        .join(status_dim, on=["order_status", "order_value_tier"], how="left")
        .join(category_dim, on="category", how="left")
    )
    return (
        joined_df.transform(_resolve_fact_order_keys)
        .select(
            COL_HK_ORDER,
            COL_ORDER_ID,
            "sk_date",
            "sk_user",
            "sk_product",
            "sk_order_status",
            "sk_currency",
            "sk_geography",
            "sk_category",
            "quantity",
            "gross_amount",
            "discount_amount",
            "tax_amount",
            "net_amount",
            "effective_discount_pct",
            "order_timestamp",
        )
        .transform(attach_column_descriptions)
    )


def _resolve_fact_event_keys(df: DataFrame) -> DataFrame:
    """Resolve `sk_date` and additive `event_count` counter on `fct_events` via `.withColumns()`."""
    return df.withColumns(
        {
            "sk_user": F.coalesce(F.col("sk_user"), F.col(COL_HK_USER)),
            "sk_product": F.coalesce(F.col("sk_product"), F.col(COL_HK_PRODUCT)),
            "sk_date": to_smart_date_key(F.col("event_date")),
            "event_count": F.lit(1),
        }
    )


def build_fct_events(
    hub_events_df: DataFrame,
    lnk_user_event_df: DataFrame,
    sat_event_details_df: DataFrame,
    dim_users_df: DataFrame,
    dim_products_df: DataFrame,
    dim_geography_df: DataFrame,
    dim_event_type_df: DataFrame,
    dim_device_df: DataFrame,
    dim_product_category_df: DataFrame,
) -> DataFrame:
    """Build Kimball Interaction Fact Table `fct_events` at the clickstream event grain."""
    latest_sat_evt = select_latest_satellite_state(sat_event_details_df, COL_HK_EVENT).drop(COL_LDTS)
    current_users = dim_users_df.filter(F.col("is_current")).select(COL_HK_USER, "sk_user", "country_code")
    current_products = dim_products_df.filter(F.col("is_current")).select(COL_HK_PRODUCT, "sk_product", "category")
    geo_dim = dim_geography_df.select("country_code", "sk_geography")
    event_type_dim = dim_event_type_df.select("event_type", "sk_event_type")
    device_dim = dim_device_df.select("device_type", "sk_device")
    category_dim = dim_product_category_df.select("category", "sk_category")

    event_spine = (
        lnk_user_event_df.select(COL_HK_EVENT, COL_HK_USER, COL_HK_PRODUCT)
        .join(hub_events_df.select(COL_HK_EVENT, COL_EVENT_ID), on=COL_HK_EVENT, how="inner")
        .join(latest_sat_evt, on=COL_HK_EVENT, how="inner")
    )
    joined_dims = (
        event_spine.join(current_users, on=COL_HK_USER, how="left")
        .join(current_products, on=COL_HK_PRODUCT, how="left")
        .join(geo_dim, on="country_code", how="left")
        .join(event_type_dim, on="event_type", how="left")
        .join(device_dim, on="device_type", how="left")
        .join(category_dim, on="category", how="left")
    )
    return (
        joined_dims.transform(_resolve_fact_event_keys)
        .select(
            COL_HK_EVENT,
            COL_EVENT_ID,
            "sk_date",
            "sk_user",
            "sk_product",
            "sk_event_type",
            "sk_device",
            "sk_geography",
            "sk_category",
            "session_id",
            "page_url",
            "duration_seconds",
            "event_count",
            "event_timestamp",
        )
        .transform(attach_column_descriptions)
    )


def _aggregate_daily_category_sales(df: DataFrame) -> DataFrame:
    """Aggregate daily order measures by conformed surrogate keys (`sk_date`, `sk_category`)."""
    return df.groupBy("sk_date", "sk_category").agg(
        F.countDistinct(COL_ORDER_ID).cast("int").alias("total_orders"),
        F.sum(F.when(F.col("is_fulfilled"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("completed_orders"),
        F.sum(F.when(F.col("is_cancelled_or_refunded"), F.lit(1)).otherwise(F.lit(0)))
        .cast("int")
        .alias("cancelled_orders"),
        F.sum("quantity").cast("int").alias("total_units_sold"),
        F.round(F.sum("gross_amount"), 2).alias("total_gross_revenue_usd"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount_usd"),
        F.round(F.sum("tax_amount"), 2).alias("total_tax_usd"),
        F.round(F.sum("net_amount"), 2).alias("total_net_revenue_usd"),
        F.round(F.avg("net_amount"), 2).alias("avg_order_value_usd"),
        F.sum(F.when(F.col("is_high_value_order"), F.lit(1)).otherwise(F.lit(0)))
        .cast("int")
        .alias("high_value_orders_count"),
        F.countDistinct("sk_user").cast("int").alias("unique_customers_count"),
    )


def build_fct_daily_sales_snapshot(
    fct_orders_df: DataFrame,
    dim_order_status_df: DataFrame,
) -> DataFrame:
    """Build Kimball Periodic Snapshot Fact Table `fct_daily_sales_snapshot` at (`sk_date`, `sk_category`) grain."""
    status_flags = dim_order_status_df.select(
        "sk_order_status",
        "is_fulfilled",
        "is_cancelled_or_refunded",
        "is_high_value_order",
    )
    enriched_orders = fct_orders_df.join(status_flags, on="sk_order_status", how="left")
    return (
        enriched_orders.transform(_aggregate_daily_category_sales)
        .select(
            "sk_date",
            "sk_category",
            "total_orders",
            "completed_orders",
            "cancelled_orders",
            "total_units_sold",
            "total_gross_revenue_usd",
            "total_discount_usd",
            "total_tax_usd",
            "total_net_revenue_usd",
            "avg_order_value_usd",
            "high_value_orders_count",
            "unique_customers_count",
        )
        .transform(attach_column_descriptions)
    )
