"""Pure PySpark functions building Kimball Conformed Dimensions.

Includes:
- `dim_date` (Conformed Calendar Date Dimension)
- `dim_users` (SCD Type 2 Customer Dimension)
- `dim_products` (SCD Type 2 Product Dimension)
- `dim_geography` (Conformed Country & Commercial Region Dimension)
- `dim_currency` (Transaction Currency & Exchange Rate Dimension)
- `dim_order_status` (Order Lifecycle Status & Value-Tier Junk Dimension)
- `dim_device` (Client Device Platform Dimension)
- `dim_event_type` (Clickstream Funnel Stage Dimension)
- `dim_product_category` (Conformed Merchandising Category Dimension)

Adheres to Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark rules:
- Column-level expression helpers (`Column -> Column`)
- Single-pass `.withColumns({...})` dictionary projections
- Short, modular functions composed via `.transform()`
"""

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.python.modules.data_vault.business_vault import select_latest_satellite_state
from src.python.modules.utils.config import (
    COL_HASHDIFF,
    COL_HK_ORDER,
    COL_HK_PRODUCT,
    COL_HK_USER,
    COL_LDTS,
    COL_PRODUCT_ID,
    COL_USER_ID,
)
from src.python.modules.utils.metadata import attach_column_descriptions

# =============================================================================
# Tier 1: Pure Column-Level Expression Helpers (Column -> Column)
# =============================================================================


def build_surrogate_key(hk_col: Column, hashdiff_col: Column) -> Column:
    """Compute a 64-character SHA-256 Kimball surrogate key from Hub hash key and Satellite hashdiff."""
    return F.sha2(F.concat_ws("||", hk_col, hashdiff_col), 256)


def build_dimension_key(*cols: Column) -> Column:
    """Compute a deterministic 64-character SHA-256 Kimball surrogate key from normalized dimension attributes."""
    normalized = [F.upper(F.trim(F.coalesce(c.cast("string"), F.lit("UNKNOWN")))) for c in cols]
    return F.sha2(F.concat_ws("||", *normalized), 256)


def calc_gross_margin_usd(unit_price_col: Column, cost_price_col: Column) -> Column:
    """Compute unit gross margin in USD rounded to 2 decimal places."""
    return F.round(unit_price_col - cost_price_col, 2)


def calc_gross_margin_pct(unit_price_col: Column, cost_price_col: Column) -> Column:
    """Compute unit gross margin percentage relative to retail unit price."""
    has_positive_price = unit_price_col > F.lit(0.0)
    margin_ratio = (unit_price_col - cost_price_col) / unit_price_col
    return F.when(has_positive_price, F.round(margin_ratio * F.lit(100.0), 2)).otherwise(F.lit(0.0))


def _map_column_value(col_expr: Column, mapping: dict[str, str], default_value: str) -> Column:
    """Translate discrete categorical codes to descriptive labels using native Spark SQL map lookup."""
    map_pairs: list[Column] = []
    for k, v in mapping.items():
        map_pairs.extend([F.lit(k), F.lit(v)])
    lookup_map = F.create_map(*map_pairs)
    return F.coalesce(lookup_map[col_expr], F.lit(default_value))


# =============================================================================
# Tier 2: Composable DataFrame Dimension Builders (DataFrame -> DataFrame)
# =============================================================================


def attach_scd2_validity_columns(sat_df: DataFrame, hk_col: str) -> DataFrame:
    """Attach SCD Type 2 `effective_from`, `effective_to`, and `is_current` columns via `.withColumns()`."""
    scd_window = Window.partitionBy(hk_col).orderBy(
        F.col("updated_at").asc_nulls_last(),
        F.col(COL_LDTS).asc_nulls_last(),
    )
    next_updated_col = F.lead(F.col("updated_at")).over(scd_window)
    high_date_col = F.to_timestamp(F.lit("9999-12-31 23:59:59"))
    return sat_df.withColumns(
        {
            "effective_from": F.col("updated_at"),
            "effective_to": F.coalesce(next_updated_col, high_date_col),
            "is_current": next_updated_col.isNull(),
        }
    )


def _derive_calendar_columns(df: DataFrame) -> DataFrame:
    """Derive all calendar date attributes in a single `.withColumns()` projection."""
    dt_col = F.col("date_actual")
    dow_col = F.dayofweek(dt_col)
    return df.withColumns(
        {
            "sk_date": F.date_format(dt_col, "yyyyMMdd").cast("int"),
            "year_actual": F.year(dt_col),
            "quarter_actual": F.quarter(dt_col),
            "month_actual": F.month(dt_col),
            "month_name": F.date_format(dt_col, "MMMM"),
            "day_of_month": F.dayofmonth(dt_col),
            "day_of_week": dow_col,
            "day_name": F.date_format(dt_col, "EEEE"),
            "is_weekend": dow_col.isin(1, 7),
        }
    )


def build_dim_date(
    spark: SparkSession,
    start_date: str = "2024-01-01",
    end_date: str = "2026-12-31",
) -> DataFrame:
    """Build Kimball Conformed Calendar Date Dimension `dim_date` using native Spark SQL sequence."""
    date_seq_df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date_actual"
    )
    return (
        date_seq_df.transform(_derive_calendar_columns)
        .select(
            "sk_date",
            "date_actual",
            "year_actual",
            "quarter_actual",
            "month_actual",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_dim_users_columns(df: DataFrame) -> DataFrame:
    """Attach surrogate key `sk_user`, `full_name`, and default LTV metrics via `.withColumns()`."""
    return df.withColumns(
        {
            "sk_user": build_surrogate_key(F.col(COL_HK_USER), F.col(COL_HASHDIFF)),
            "full_name": F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
            "lifetime_orders_count": F.coalesce(F.col("lifetime_orders_count"), F.lit(0)),
            "lifetime_spend_usd": F.coalesce(F.col("lifetime_spend_usd"), F.lit(0.0)),
            "calculated_customer_segment": F.coalesce(F.col("calculated_customer_segment"), F.lit("PROSPECT")),
        }
    )


def build_dim_users(
    hub_users_df: DataFrame,
    sat_user_details_df: DataFrame,
    sat_user_tier_bv_df: DataFrame,
) -> DataFrame:
    """Build Kimball Customer Dimension `dim_users` combining Raw Vault and Business Vault attributes."""
    sat_with_scd = attach_scd2_validity_columns(sat_user_details_df, COL_HK_USER)
    latest_bv = select_latest_satellite_state(sat_user_tier_bv_df, COL_HK_USER).select(
        COL_HK_USER,
        "lifetime_orders_count",
        "lifetime_spend_usd",
        "calculated_customer_segment",
    )
    joined_df = (
        hub_users_df.select(COL_HK_USER, COL_USER_ID)
        .join(sat_with_scd, on=COL_HK_USER, how="inner")
        .join(latest_bv, on=COL_HK_USER, how="left")
    )
    return (
        joined_df.transform(_enrich_dim_users_columns)
        .select(
            "sk_user",
            COL_HK_USER,
            COL_USER_ID,
            "email",
            "first_name",
            "last_name",
            "full_name",
            "country_code",
            "loyalty_tier",
            "calculated_customer_segment",
            "lifetime_orders_count",
            "lifetime_spend_usd",
            "signup_date",
            "effective_from",
            "effective_to",
            "is_current",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_dim_products_columns(df: DataFrame) -> DataFrame:
    """Attach surrogate key `sk_product` and unit gross margin metrics via `.withColumns()`."""
    price_col = F.col("unit_price")
    cost_col = F.col("cost_price")
    return df.withColumns(
        {
            "sk_product": build_surrogate_key(F.col(COL_HK_PRODUCT), F.col(COL_HASHDIFF)),
            "gross_margin_usd": calc_gross_margin_usd(price_col, cost_col),
            "gross_margin_pct": calc_gross_margin_pct(price_col, cost_col),
        }
    )


def build_dim_products(
    hub_products_df: DataFrame,
    sat_product_details_df: DataFrame,
) -> DataFrame:
    """Build Kimball Product Dimension `dim_products` with unit margin calculations and SCD2 attributes."""
    sat_with_scd = attach_scd2_validity_columns(sat_product_details_df, COL_HK_PRODUCT)
    joined_df = hub_products_df.select(COL_HK_PRODUCT, COL_PRODUCT_ID).join(
        sat_with_scd, on=COL_HK_PRODUCT, how="inner"
    )
    return (
        joined_df.transform(_enrich_dim_products_columns)
        .select(
            "sk_product",
            COL_HK_PRODUCT,
            COL_PRODUCT_ID,
            "product_name",
            "category",
            "subcategory",
            "unit_price",
            "cost_price",
            "gross_margin_usd",
            "gross_margin_pct",
            "is_active",
            "effective_from",
            "effective_to",
            "is_current",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_geography_columns(df: DataFrame) -> DataFrame:
    """Derive conformed geography attributes and surrogate key `sk_geography` via `.withColumns()`."""
    cc = F.col("country_code")
    names = {
        "US": "United States",
        "CA": "Canada",
        "GB": "United Kingdom",
        "DE": "Germany",
        "PL": "Poland",
        "FR": "France",
        "NL": "Netherlands",
        "SE": "Sweden",
    }
    regions = {
        "US": "NORTH_AMERICA",
        "CA": "NORTH_AMERICA",
        "GB": "EUROPE",
        "DE": "EUROPE",
        "PL": "EUROPE",
        "FR": "EUROPE",
        "NL": "EUROPE",
        "SE": "EUROPE",
    }
    subregions = {
        "US": "NORTHERN_AMERICA",
        "CA": "NORTHERN_AMERICA",
        "GB": "WESTERN_EUROPE",
        "DE": "CENTRAL_EUROPE",
        "PL": "CENTRAL_EUROPE",
        "FR": "WESTERN_EUROPE",
        "NL": "WESTERN_EUROPE",
        "SE": "NORTHERN_EUROPE",
    }
    currencies = {
        "US": "USD",
        "CA": "CAD",
        "GB": "GBP",
        "DE": "EUR",
        "PL": "PLN",
        "FR": "EUR",
        "NL": "EUR",
        "SE": "SEK",
    }
    return df.withColumns(
        {
            "sk_geography": build_dimension_key(cc),
            "country_name": _map_column_value(cc, names, "Unknown Country"),
            "region": _map_column_value(cc, regions, "OTHER"),
            "subregion": _map_column_value(cc, subregions, "OTHER"),
            "default_currency_code": _map_column_value(cc, currencies, "USD"),
        }
    )


def build_dim_geography(sat_user_details_df: DataFrame) -> DataFrame:
    """Build Kimball Conformed Geography Dimension `dim_geography` from Raw Vault user locations."""
    distinct_countries = sat_user_details_df.select(
        F.upper(F.trim(F.coalesce(F.col("country_code"), F.lit("UNKNOWN")))).alias("country_code")
    ).distinct()
    return (
        distinct_countries.transform(_enrich_geography_columns)
        .select(
            "sk_geography",
            "country_code",
            "country_name",
            "region",
            "subregion",
            "default_currency_code",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_currency_columns(df: DataFrame) -> DataFrame:
    """Derive currency names, symbols, USD conversion factors, and `sk_currency` via `.withColumns()`."""
    curr = F.col("currency_code")
    names = {"USD": "US Dollar", "EUR": "Euro", "GBP": "British Pound Sterling"}
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    usd_rate_col = (
        F.when(curr == F.lit("USD"), F.lit(1.0))
        .when(curr == F.lit("EUR"), F.lit(1.08))
        .when(curr == F.lit("GBP"), F.lit(1.27))
        .otherwise(F.lit(1.0))
    )
    return df.withColumns(
        {
            "sk_currency": build_dimension_key(curr),
            "currency_name": _map_column_value(curr, names, "Other Currency"),
            "currency_symbol": _map_column_value(curr, symbols, "$"),
            "usd_exchange_rate": usd_rate_col,
        }
    )


def build_dim_currency(sat_order_details_df: DataFrame) -> DataFrame:
    """Build Kimball Conformed Currency Dimension `dim_currency` from Raw Vault order currencies."""
    distinct_currencies = sat_order_details_df.select(
        F.upper(F.trim(F.coalesce(F.col("currency_code"), F.lit("USD")))).alias("currency_code")
    ).distinct()
    return (
        distinct_currencies.transform(_enrich_currency_columns)
        .select(
            "sk_currency",
            "currency_code",
            "currency_name",
            "currency_symbol",
            "usd_exchange_rate",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_order_status_columns(df: DataFrame) -> DataFrame:
    """Compute `sk_order_status`, reversal flags, and `status_category` in a single `.withColumns()` projection."""
    status_col = F.col("order_status")
    tier_col = F.col("order_value_tier")
    is_reversed = status_col.isin("CANCELLED", "REFUNDED")
    is_fulfilled = status_col.isin("COMPLETED", "SHIPPED")
    status_categories = {
        "COMPLETED": "FULFILLED",
        "SHIPPED": "FULFILLED",
        "PROCESSING": "IN_PROGRESS",
        "CANCELLED": "REVERSED",
        "REFUNDED": "REVERSED",
    }
    return df.withColumns(
        {
            "sk_order_status": build_dimension_key(status_col, tier_col),
            "is_fulfilled": is_fulfilled,
            "is_cancelled_or_refunded": is_reversed,
            "status_category": _map_column_value(status_col, status_categories, "OTHER"),
        }
    )


def build_dim_order_status(sat_order_details_df: DataFrame, sat_order_bv_df: DataFrame) -> DataFrame:
    """Build Kimball Junk/Status Dimension `dim_order_status` capturing status and value-tier profiles."""
    latest_details = select_latest_satellite_state(sat_order_details_df, COL_HK_ORDER).select(
        COL_HK_ORDER,
        F.upper(F.trim(F.coalesce(F.col("order_status"), F.lit("UNKNOWN")))).alias("order_status"),
    )
    latest_bv = select_latest_satellite_state(sat_order_bv_df, COL_HK_ORDER).select(
        COL_HK_ORDER,
        F.upper(F.trim(F.coalesce(F.col("order_value_tier"), F.lit("STANDARD_VALUE")))).alias("order_value_tier"),
        F.coalesce(F.col("is_high_value_order"), F.lit(False)).alias("is_high_value_order"),
    )
    distinct_profiles = (
        latest_details.join(latest_bv, on=COL_HK_ORDER, how="inner")
        .select("order_status", "order_value_tier", "is_high_value_order")
        .distinct()
    )
    return (
        distinct_profiles.transform(_enrich_order_status_columns)
        .select(
            "sk_order_status",
            "order_status",
            "order_value_tier",
            "is_high_value_order",
            "is_fulfilled",
            "is_cancelled_or_refunded",
            "status_category",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_device_columns(df: DataFrame) -> DataFrame:
    """Derive client device category, mobility flag, and `sk_device` via `.withColumns()`."""
    dev_col = F.col("device_type")
    categories = {"MOBILE": "MOBILE_APP", "DESKTOP": "WEB_DESKTOP", "TABLET": "TABLET_WEB"}
    return df.withColumns(
        {
            "sk_device": build_dimension_key(dev_col),
            "device_category": _map_column_value(dev_col, categories, "OTHER"),
            "is_mobile_platform": dev_col.isin("MOBILE", "TABLET"),
        }
    )


def build_dim_device(sat_event_details_df: DataFrame) -> DataFrame:
    """Build Kimball Client Device Dimension `dim_device` from Raw Vault clickstream telemetry."""
    distinct_devices = sat_event_details_df.select(
        F.upper(F.trim(F.coalesce(F.col("device_type"), F.lit("UNKNOWN")))).alias("device_type")
    ).distinct()
    return (
        distinct_devices.transform(_enrich_device_columns)
        .select(
            "sk_device",
            "device_type",
            "device_category",
            "is_mobile_platform",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_event_type_columns(df: DataFrame) -> DataFrame:
    """Derive clickstream funnel stage, ordinal step, conversion flag, and `sk_event_type` via `.withColumns()`."""
    evt_col = F.col("event_type")
    stages = {
        "PAGE_VIEW": "AWARENESS",
        "PRODUCT_SEARCH": "DISCOVERY",
        "ADD_TO_CART": "CONSIDERATION",
        "CHECKOUT_START": "INTENT",
        "PURCHASE": "CONVERSION",
    }
    step_order_col = (
        F.when(evt_col == F.lit("PAGE_VIEW"), F.lit(1))
        .when(evt_col == F.lit("PRODUCT_SEARCH"), F.lit(2))
        .when(evt_col == F.lit("ADD_TO_CART"), F.lit(3))
        .when(evt_col == F.lit("CHECKOUT_START"), F.lit(4))
        .when(evt_col == F.lit("PURCHASE"), F.lit(5))
        .otherwise(F.lit(99))
    )
    return df.withColumns(
        {
            "sk_event_type": build_dimension_key(evt_col),
            "funnel_stage": _map_column_value(evt_col, stages, "OTHER"),
            "funnel_step_order": step_order_col,
            "is_conversion_event": evt_col == F.lit("PURCHASE"),
        }
    )


def build_dim_event_type(sat_event_details_df: DataFrame) -> DataFrame:
    """Build Kimball Clickstream Funnel Event Type Dimension `dim_event_type`."""
    distinct_events = sat_event_details_df.select(
        F.upper(F.trim(F.coalesce(F.col("event_type"), F.lit("UNKNOWN")))).alias("event_type")
    ).distinct()
    return (
        distinct_events.transform(_enrich_event_type_columns)
        .select(
            "sk_event_type",
            "event_type",
            "funnel_stage",
            "funnel_step_order",
            "is_conversion_event",
        )
        .transform(attach_column_descriptions)
    )


def _enrich_product_category_columns(df: DataFrame) -> DataFrame:
    """Derive merchandising department, strategic tier, and `sk_category` via `.withColumns()`."""
    cat_col = F.col("category")
    departments = {
        "ELECTRONICS": "HARDLINES",
        "HOME": "HARDLINES",
        "APPAREL": "SOFTLINES",
        "SPORTS": "ACTIVE_LIVING",
        "BOOKS": "MEDIA",
    }
    tiers = {
        "ELECTRONICS": "CORE",
        "APPAREL": "CORE",
        "HOME": "GROWTH",
        "SPORTS": "GROWTH",
        "BOOKS": "SPECIALTY",
    }
    return df.withColumns(
        {
            "sk_category": build_dimension_key(cat_col),
            "department": _map_column_value(cat_col, departments, "GENERAL"),
            "category_tier": _map_column_value(cat_col, tiers, "STANDARD"),
            "is_physical_goods": F.lit(True),
        }
    )


def build_dim_product_category(sat_product_details_df: DataFrame) -> DataFrame:
    """Build Kimball Conformed Product Category Dimension `dim_product_category`."""
    distinct_categories = sat_product_details_df.select(
        F.upper(F.trim(F.coalesce(F.col("category"), F.lit("UNCLASSIFIED")))).alias("category")
    ).distinct()
    return (
        distinct_categories.transform(_enrich_product_category_columns)
        .select(
            "sk_category",
            "category",
            "department",
            "category_tier",
            "is_physical_goods",
        )
        .transform(attach_column_descriptions)
    )
