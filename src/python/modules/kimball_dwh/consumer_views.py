"""Pure PySpark transformations building governed views for Data Analysts and ML Engineers.

Follows Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark standards:
- Column-level expression helpers (`Column -> Column`)
- Batch `.withColumns({...})` dictionary projections instead of chained `.withColumn()`
- Short, single-concern functions composed via `.transform()`
- Queries Fact Tables joined explicitly to Kimball Conformed Dimensions
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.python.modules.utils.config import (
    COL_EVENT_ID,
    COL_ORDER_ID,
    COL_PRODUCT_ID,
    COL_USER_ID,
)
from src.python.modules.utils.metadata import attach_column_descriptions

# =============================================================================
# Tier 1: Pure Column-Level Feature & Metric Builders (Column -> Column)
# =============================================================================


def calc_safe_ratio(numerator: Column, denominator: Column, scale: int = 4) -> Column:
    """Compute a bounded ratio safely guarding against zero or null denominators."""
    has_positive_denom = denominator.isNotNull() & (denominator > F.lit(0))
    return F.when(has_positive_denom, F.round(numerator / denominator, scale)).otherwise(F.lit(0.0))


def count_event_type(event_type_label: str) -> Column:
    """Return an aggregate integer sum Column counting occurrences of a specific `event_type`."""
    is_match = F.col("event_type") == F.lit(event_type_label)
    return F.sum(F.when(is_match, F.lit(1)).otherwise(F.lit(0))).cast("int")


def calc_churn_risk_score(
    days_since_order_col: Column,
    days_since_event_col: Column,
    total_orders_col: Column,
) -> Column:
    """Compute normalized ML churn propensity score (0.0 to 1.0) from recency and order frequency."""
    order_recency_component = (days_since_order_col / F.lit(180.0)) * F.lit(0.6)
    event_recency_component = (days_since_event_col / F.lit(180.0)) * F.lit(0.4)
    frequency_discount = F.least(total_orders_col, F.lit(10)) * F.lit(0.03)
    raw_score = order_recency_component + event_recency_component - frequency_discount
    return F.round(F.least(F.lit(1.0), F.greatest(F.lit(0.0), raw_score)), 4)


def calc_repeat_purchase_ratio(total_orders_col: Column, distinct_buyers_col: Column) -> Column:
    """Compute repeat purchase ratio as max(0, (total_orders - distinct_buyers) / total_orders)."""
    repeat_orders = F.greatest(F.lit(0.0), (total_orders_col - distinct_buyers_col) / total_orders_col)
    return F.when(total_orders_col > F.lit(0), F.round(repeat_orders, 4)).otherwise(F.lit(0.0))


def calc_affinity_index_score(
    repeat_ratio_col: Column,
    conversion_rate_col: Column,
    cat_share_pct_col: Column,
) -> Column:
    """Compute composite ML product affinity score (0.0 to 100.0)."""
    repeat_weight = repeat_ratio_col * F.lit(40.0)
    conversion_weight = F.least(conversion_rate_col, F.lit(1.0)) * F.lit(35.0)
    share_weight = F.least(cat_share_pct_col, F.lit(100.0)) * F.lit(0.25)
    return F.round(repeat_weight + conversion_weight + share_weight, 2)


def coalesce_zero_columns(df: DataFrame, int_cols: tuple[str, ...], float_cols: tuple[str, ...]) -> DataFrame:
    """Replace nulls with 0 / 0.0 across specified integer and floating-point columns via `.withColumns()`."""
    updates: dict[str, Column] = {
        **{c: F.coalesce(F.col(c), F.lit(0)) for c in int_cols},
        **{c: F.coalesce(F.col(c), F.lit(0.0)) for c in float_cols},
    }
    return df.withColumns(updates)


# =============================================================================
# Tier 2: Composable Consumer View Builders (DataFrame -> DataFrame)
# =============================================================================


def build_vw_executive_sales_kpis(
    fct_daily_sales_df: DataFrame,
    dim_date_df: DataFrame,
    dim_product_category_df: DataFrame,
) -> DataFrame:
    """Build Data Analyst View `vw_executive_sales_kpis` joining snapshot facts to calendar and category dimensions."""
    calendar_slice = dim_date_df.select(
        "sk_date",
        F.col("date_actual").alias("order_date"),
        "year_actual",
        "quarter_actual",
        "month_actual",
        "month_name",
        "day_name",
        "is_weekend",
    )
    category_slice = dim_product_category_df.select(
        "sk_category",
        "category",
        "department",
        "category_tier",
    )
    return (
        fct_daily_sales_df.join(calendar_slice, on="sk_date", how="left")
        .join(category_slice, on="sk_category", how="left")
        .select(
            "sk_date",
            "order_date",
            "year_actual",
            "quarter_actual",
            "month_actual",
            "month_name",
            "day_name",
            "is_weekend",
            "sk_category",
            "category",
            "department",
            "category_tier",
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


def _aggregate_customer_orders(
    fct_orders_df: DataFrame,
    dim_order_status_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Aggregate order count, fulfillment count, revenue, and latest order date per `sk_user`."""
    status_slice = dim_order_status_df.select("sk_order_status", "is_fulfilled")
    date_slice = dim_date_df.select("sk_date", "date_actual")
    return (
        fct_orders_df.join(status_slice, on="sk_order_status", how="left")
        .join(date_slice, on="sk_date", how="left")
        .groupBy("sk_user")
        .agg(
            F.countDistinct(COL_ORDER_ID).cast("int").alias("total_orders"),
            F.sum(F.when(F.col("is_fulfilled"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("completed_orders"),
            F.round(F.sum("net_amount"), 2).alias("total_net_revenue_usd"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value_usd"),
            F.max("date_actual").alias("_last_order_date"),
        )
    )


def _aggregate_customer_events(
    fct_events_df: DataFrame,
    dim_event_type_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Aggregate clickstream telemetry counts and session metrics per `sk_user`."""
    event_type_slice = dim_event_type_df.select("sk_event_type", "event_type")
    date_slice = dim_date_df.select("sk_date", "date_actual")
    return (
        fct_events_df.join(event_type_slice, on="sk_event_type", how="left")
        .join(date_slice, on="sk_date", how="left")
        .groupBy("sk_user")
        .agg(
            F.countDistinct(COL_EVENT_ID).cast("int").alias("total_events_count"),
            F.countDistinct("session_id").cast("int").alias("distinct_sessions_count"),
            F.round(F.avg("duration_seconds"), 2).alias("avg_session_duration_sec"),
            count_event_type("PAGE_VIEW").alias("page_view_count"),
            count_event_type("ADD_TO_CART").alias("add_to_cart_count"),
            count_event_type("CHECKOUT_START").alias("checkout_start_count"),
            count_event_type("PURCHASE").alias("purchase_event_count"),
            count_event_type("PRODUCT_SEARCH").alias("product_search_count"),
            F.max("date_actual").alias("_last_event_date"),
        )
    )


def build_vw_customer_360(
    dim_users_df: DataFrame,
    dim_geography_df: DataFrame,
    fct_orders_df: DataFrame,
    dim_order_status_df: DataFrame,
    fct_events_df: DataFrame,
    dim_event_type_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """Build Data Analyst View `vw_customer_360` providing a holistic customer profile across orders and events."""
    active_users = dim_users_df.filter(F.col("is_current")).join(
        dim_geography_df.select("country_code", "country_name", "region"),
        on="country_code",
        how="left",
    )
    order_agg = _aggregate_customer_orders(fct_orders_df, dim_order_status_df, dim_date_df)
    event_agg = _aggregate_customer_events(fct_events_df, dim_event_type_df, dim_date_df)

    int_cols = ("total_orders", "completed_orders", "total_events_count", "distinct_sessions_count")
    float_cols = ("total_net_revenue_usd", "avg_order_value_usd", "avg_session_duration_sec")

    joined_df = active_users.join(order_agg, on="sk_user", how="left").join(event_agg, on="sk_user", how="left")
    return (
        joined_df.transform(lambda d: coalesce_zero_columns(d, int_cols, float_cols))
        .select(
            "sk_user",
            COL_USER_ID,
            "email",
            "full_name",
            "country_code",
            "country_name",
            "region",
            "loyalty_tier",
            "calculated_customer_segment",
            "signup_date",
            "total_orders",
            "completed_orders",
            "total_net_revenue_usd",
            "avg_order_value_usd",
            "total_events_count",
            "distinct_sessions_count",
            "avg_session_duration_sec",
        )
        .transform(attach_column_descriptions)
    )


def _aggregate_product_orders(fct_orders_df: DataFrame) -> DataFrame:
    """Aggregate product order volume, units sold, net revenue, and distinct buyers per `sk_product`."""
    return fct_orders_df.groupBy("sk_product").agg(
        F.countDistinct(COL_ORDER_ID).cast("int").alias("total_orders"),
        F.sum("quantity").cast("int").alias("total_units_sold"),
        F.round(F.sum("net_amount"), 2).alias("total_net_revenue_usd"),
        F.countDistinct("sk_user").cast("int").alias("distinct_buyers_count"),
    )


def _aggregate_product_events(fct_events_df: DataFrame, dim_event_type_df: DataFrame) -> DataFrame:
    """Aggregate product clickstream funnel events per `sk_product`."""
    event_type_slice = dim_event_type_df.select("sk_event_type", "event_type")
    return (
        fct_events_df.join(event_type_slice, on="sk_event_type", how="left")
        .groupBy("sk_product")
        .agg(
            F.countDistinct(COL_EVENT_ID).cast("int").alias("total_events_count"),
            count_event_type("PAGE_VIEW").alias("page_view_count"),
            count_event_type("ADD_TO_CART").alias("add_to_cart_count"),
            count_event_type("PURCHASE").alias("purchase_event_count"),
        )
    )


def build_vw_product_performance(
    dim_products_df: DataFrame,
    dim_product_category_df: DataFrame,
    fct_orders_df: DataFrame,
    fct_events_df: DataFrame,
    dim_event_type_df: DataFrame,
) -> DataFrame:
    """Build Data Analyst View `vw_product_performance` analyzing product sales, margins, and funnel engagement."""
    active_products = dim_products_df.filter(F.col("is_current")).join(
        dim_product_category_df.select("category", "department", "category_tier"),
        on="category",
        how="left",
    )
    order_agg = _aggregate_product_orders(fct_orders_df)
    event_agg = _aggregate_product_events(fct_events_df, dim_event_type_df)

    int_cols = (
        "total_orders",
        "total_units_sold",
        "distinct_buyers_count",
        "page_view_count",
        "add_to_cart_count",
        "purchase_event_count",
    )
    joined_df = active_products.join(order_agg, on="sk_product", how="left").join(
        event_agg, on="sk_product", how="left"
    )
    return (
        joined_df.transform(lambda d: coalesce_zero_columns(d, int_cols, ("total_net_revenue_usd",)))
        .withColumns(
            {"cart_to_purchase_rate": calc_safe_ratio(F.col("purchase_event_count"), F.col("add_to_cart_count"))}
        )
        .select(
            "sk_product",
            COL_PRODUCT_ID,
            "product_name",
            "category",
            "department",
            "category_tier",
            "subcategory",
            "unit_price",
            "cost_price",
            "gross_margin_usd",
            "gross_margin_pct",
            "is_active",
            "total_orders",
            "total_units_sold",
            "total_net_revenue_usd",
            "distinct_buyers_count",
            "page_view_count",
            "add_to_cart_count",
            "purchase_event_count",
            "cart_to_purchase_rate",
        )
        .transform(attach_column_descriptions)
    )


def _derive_ml_churn_columns(df: DataFrame, reference_date: str) -> DataFrame:
    """Derive recency, conversion ratio, churn risk score, and binary label via `.withColumns()`."""
    ref_dt_col = F.to_date(F.lit(reference_date))
    days_order_col = F.coalesce(F.datediff(ref_dt_col, F.col("_last_order_date")), F.lit(180)).cast("int")
    days_event_col = F.coalesce(F.datediff(ref_dt_col, F.col("_last_event_date")), F.lit(180)).cast("int")
    churn_score_col = calc_churn_risk_score(days_order_col, days_event_col, F.col("total_orders"))
    return df.withColumns(
        {
            "days_since_last_order": days_order_col,
            "days_since_last_event": days_event_col,
            "cart_to_purchase_rate": calc_safe_ratio(F.col("purchase_event_count"), F.col("add_to_cart_count")),
            "churn_risk_score": churn_score_col,
            "is_churn_candidate": F.when(churn_score_col >= F.lit(0.55), F.lit(1)).otherwise(F.lit(0)),
        }
    )


def build_vw_ml_customer_churn_features(
    dim_users_df: DataFrame,
    fct_orders_df: DataFrame,
    dim_order_status_df: DataFrame,
    fct_events_df: DataFrame,
    dim_event_type_df: DataFrame,
    dim_date_df: DataFrame,
    reference_date: str = "2025-06-01",
) -> DataFrame:
    """Build ML Engineer Feature View `vw_ml_customer_churn_features` for customer churn and CLV modeling."""
    active_users = dim_users_df.filter(F.col("is_current"))
    order_features = _aggregate_customer_orders(fct_orders_df, dim_order_status_df, dim_date_df)
    event_features = _aggregate_customer_events(fct_events_df, dim_event_type_df, dim_date_df)

    int_cols = (
        "total_orders",
        "total_events_count",
        "distinct_sessions_count",
        "page_view_count",
        "add_to_cart_count",
        "checkout_start_count",
        "purchase_event_count",
        "product_search_count",
    )
    float_cols = ("total_net_revenue_usd", "avg_order_value_usd", "avg_session_duration_sec")

    joined_df = active_users.join(order_features, on="sk_user", how="left").join(
        event_features, on="sk_user", how="left"
    )
    return (
        joined_df.transform(lambda d: coalesce_zero_columns(d, int_cols, float_cols))
        .transform(lambda d: _derive_ml_churn_columns(d, reference_date))
        .select(
            "sk_user",
            COL_USER_ID,
            "country_code",
            "loyalty_tier",
            "calculated_customer_segment",
            "total_orders",
            "total_net_revenue_usd",
            "avg_order_value_usd",
            "total_events_count",
            "distinct_sessions_count",
            "avg_session_duration_sec",
            "page_view_count",
            "add_to_cart_count",
            "checkout_start_count",
            "purchase_event_count",
            "product_search_count",
            "days_since_last_order",
            "days_since_last_event",
            "cart_to_purchase_rate",
            "churn_risk_score",
            "is_churn_candidate",
        )
        .transform(attach_column_descriptions)
    )


def _derive_ml_affinity_columns(df: DataFrame) -> DataFrame:
    """Derive repeat purchase ratio, category revenue share, and affinity index score via `.withColumns()`."""
    cat_win = Window.partitionBy("category")
    cat_rev_col = F.sum("total_net_revenue_usd").over(cat_win)
    repeat_col = calc_repeat_purchase_ratio(F.col("total_orders"), F.col("distinct_buyers_count"))
    cat_share_col = calc_safe_ratio(F.col("total_net_revenue_usd") * F.lit(100.0), cat_rev_col, scale=2)
    conv_col = calc_safe_ratio(F.col("purchase_event_count"), F.col("add_to_cart_count"), scale=4)
    return df.withColumns(
        {
            "repeat_purchase_ratio": repeat_col,
            "category_revenue_share_pct": cat_share_col,
            "cart_to_purchase_rate": conv_col,
            "affinity_index_score": calc_affinity_index_score(repeat_col, conv_col, cat_share_col),
        }
    )


def build_vw_ml_product_affinity_features(
    dim_products_df: DataFrame,
    fct_orders_df: DataFrame,
    fct_events_df: DataFrame,
    dim_event_type_df: DataFrame,
) -> DataFrame:
    """Build ML Engineer Feature View `vw_ml_product_affinity_features` for recommendation systems."""
    active_products = dim_products_df.filter(F.col("is_current"))
    prod_order_metrics = _aggregate_product_orders(fct_orders_df)
    prod_event_metrics = _aggregate_product_events(fct_events_df, dim_event_type_df)

    int_cols = (
        "total_orders",
        "distinct_buyers_count",
        "total_units_sold",
        "total_events_count",
        "add_to_cart_count",
        "purchase_event_count",
    )
    joined_df = active_products.join(prod_order_metrics, on="sk_product", how="left").join(
        prod_event_metrics, on="sk_product", how="left"
    )
    return (
        joined_df.transform(lambda d: coalesce_zero_columns(d, int_cols, ("total_net_revenue_usd",)))
        .transform(_derive_ml_affinity_columns)
        .select(
            "sk_product",
            COL_PRODUCT_ID,
            "product_name",
            "category",
            "subcategory",
            "unit_price",
            "gross_margin_pct",
            "total_orders",
            "distinct_buyers_count",
            "total_units_sold",
            "total_net_revenue_usd",
            "total_events_count",
            "repeat_purchase_ratio",
            "category_revenue_share_pct",
            "cart_to_purchase_rate",
            "affinity_index_score",
        )
        .transform(attach_column_descriptions)
    )
