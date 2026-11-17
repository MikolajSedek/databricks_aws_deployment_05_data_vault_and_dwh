"""Pure PySpark transformations for standardizing, deduplicating, and enriching domain entities.

Follows the Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark rules:
- Column-first helper functions (`Column -> Column`)
- Batch column projections via `.withColumns({...})` instead of chained `.withColumn()`
- Short, single-concern functions composed via `.transform()` and `functools.reduce`
"""

from collections.abc import Callable
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.python.modules.utils.metadata import attach_column_descriptions

TransformStep = Callable[[DataFrame], DataFrame]


# =============================================================================
# Tier 1: Pure Column-Level Helper Functions (Column -> Column)
# =============================================================================


def norm_upper(col_name: str) -> Column:
    """Return a trimmed, uppercase string Column expression."""
    return F.upper(F.trim(F.col(col_name)))


def norm_lower(col_name: str) -> Column:
    """Return a trimmed, lowercase string Column expression."""
    return F.lower(F.trim(F.col(col_name)))


def norm_title(col_name: str) -> Column:
    """Return a trimmed, title-case string Column expression."""
    return F.initcap(F.trim(F.col(col_name)))


def is_non_empty_key(col_name: str) -> Column:
    """Return a boolean Column predicate checking that a string key is non-null and non-blank."""
    return F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != F.lit(""))


def compute_net_order_amount(gross_col: Column, discount_col: Column, tax_col: Column) -> Column:
    """Compute net transaction amount as (gross - discount + tax) rounded to 2 decimal places."""
    safe_discount = F.coalesce(discount_col, F.lit(0.0))
    safe_tax = F.coalesce(tax_col, F.lit(0.0))
    return F.round(gross_col - safe_discount + safe_tax, 2)


# =============================================================================
# Tier 2: Composable Atomic DataFrame Step Functions (DataFrame -> DataFrame)
# =============================================================================


def apply_transform_pipeline(df: DataFrame, steps: tuple[TransformStep, ...]) -> DataFrame:
    """Apply an ordered tuple of pure DataFrame transformations using `functools.reduce`."""
    return reduce(lambda acc_df, step_fn: acc_df.transform(step_fn), steps, df)


def deduplicate_latest_by_key(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Retain the most recent record per business key using an explicit window specification."""
    dedup_window = Window.partitionBy(F.col(key_col)).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumns({"_rn": F.row_number().over(dedup_window)}).filter(F.col("_rn") == 1).drop("_rn")


# --- Users Domain Steps ---


def filter_valid_user_keys(df: DataFrame) -> DataFrame:
    """Filter users to rows with a non-null, non-empty `user_id`."""
    return df.filter(is_non_empty_key("user_id"))


def standardize_user_columns(df: DataFrame) -> DataFrame:
    """Normalize user string attributes in a single `.withColumns()` Catalyst projection."""
    return df.withColumns(
        {
            "user_id": norm_upper("user_id"),
            "email": norm_lower("email"),
            "first_name": norm_title("first_name"),
            "last_name": norm_title("last_name"),
            "country_code": norm_upper("country_code"),
            "loyalty_tier": norm_upper("loyalty_tier"),
        }
    )


def deduplicate_users(df: DataFrame) -> DataFrame:
    """Deduplicate users by `user_id` ordering by `updated_at` descending."""
    return deduplicate_latest_by_key(df, "user_id", "updated_at")


def clean_users_df(df: DataFrame) -> DataFrame:
    """Standardize user attributes, deduplicate by latest update, and attach column comments."""
    steps: tuple[TransformStep, ...] = (
        filter_valid_user_keys,
        standardize_user_columns,
        deduplicate_users,
        attach_column_descriptions,
    )
    return apply_transform_pipeline(df, steps)


# --- Products Domain Steps ---


def filter_valid_product_rows(df: DataFrame) -> DataFrame:
    """Filter products to rows with a valid `product_id`, positive `unit_price`, and non-negative `cost_price`."""
    has_key = is_non_empty_key("product_id")
    has_valid_pricing = (F.col("unit_price") > F.lit(0.0)) & (F.col("cost_price") >= F.lit(0.0))
    return df.filter(has_key & has_valid_pricing)


def standardize_product_columns(df: DataFrame) -> DataFrame:
    """Normalize product strings and round monetary fields in a single `.withColumns()` projection."""
    return df.withColumns(
        {
            "product_id": norm_upper("product_id"),
            "product_name": F.trim(F.col("product_name")),
            "category": norm_upper("category"),
            "subcategory": norm_upper("subcategory"),
            "unit_price": F.round(F.col("unit_price"), 2),
            "cost_price": F.round(F.col("cost_price"), 2),
        }
    )


def deduplicate_products(df: DataFrame) -> DataFrame:
    """Deduplicate products by `product_id` ordering by `updated_at` descending."""
    return deduplicate_latest_by_key(df, "product_id", "updated_at")


def clean_products_df(df: DataFrame) -> DataFrame:
    """Standardize product catalog attributes, enforce positive prices, and deduplicate."""
    steps: tuple[TransformStep, ...] = (
        filter_valid_product_rows,
        standardize_product_columns,
        deduplicate_products,
        attach_column_descriptions,
    )
    return apply_transform_pipeline(df, steps)


# --- Orders Domain Steps ---


def filter_valid_order_rows(df: DataFrame) -> DataFrame:
    """Filter orders to rows with non-empty foreign/primary keys and positive quantity/gross amount."""
    has_keys = is_non_empty_key("order_id") & is_non_empty_key("user_id") & is_non_empty_key("product_id")
    has_measures = (F.col("quantity") > F.lit(0)) & (F.col("gross_amount") >= F.lit(0.0))
    return df.filter(has_keys & has_measures)


def standardize_and_enrich_order_columns(df: DataFrame) -> DataFrame:
    """Normalize order identifiers and compute `net_amount` and `order_date` via `.withColumns()`."""
    net_amt_col = compute_net_order_amount(
        F.col("gross_amount"),
        F.col("discount_amount"),
        F.col("tax_amount"),
    )
    return df.withColumns(
        {
            "order_id": norm_upper("order_id"),
            "user_id": norm_upper("user_id"),
            "product_id": norm_upper("product_id"),
            "order_status": norm_upper("order_status"),
            "currency_code": norm_upper("currency_code"),
            "discount_amount": F.round(F.coalesce(F.col("discount_amount"), F.lit(0.0)), 2),
            "tax_amount": F.round(F.coalesce(F.col("tax_amount"), F.lit(0.0)), 2),
            "net_amount": net_amt_col,
            "order_date": F.to_date(F.col("order_timestamp")),
        }
    )


def deduplicate_orders(df: DataFrame) -> DataFrame:
    """Deduplicate orders by `order_id` ordering by `order_timestamp` descending."""
    return deduplicate_latest_by_key(df, "order_id", "order_timestamp")


def clean_orders_df(df: DataFrame) -> DataFrame:
    """Standardize order transactions, compute net_amount and order_date, and deduplicate."""
    steps: tuple[TransformStep, ...] = (
        filter_valid_order_rows,
        standardize_and_enrich_order_columns,
        deduplicate_orders,
        attach_column_descriptions,
    )
    return apply_transform_pipeline(df, steps)


# --- Clickstream Events Domain Steps ---


def filter_valid_event_rows(df: DataFrame) -> DataFrame:
    """Filter clickstream events to rows with non-empty keys and non-negative duration."""
    has_keys = is_non_empty_key("event_id") & is_non_empty_key("user_id") & is_non_empty_key("product_id")
    has_valid_duration = F.col("duration_seconds") >= F.lit(0)
    return df.filter(has_keys & has_valid_duration)


def standardize_and_enrich_event_columns(df: DataFrame) -> DataFrame:
    """Normalize clickstream event identifiers and derive `event_date` in a single `.withColumns()` call."""
    return df.withColumns(
        {
            "event_id": norm_upper("event_id"),
            "user_id": norm_upper("user_id"),
            "product_id": norm_upper("product_id"),
            "event_type": norm_upper("event_type"),
            "session_id": F.trim(F.col("session_id")),
            "device_type": norm_upper("device_type"),
            "event_date": F.to_date(F.col("event_timestamp")),
        }
    )


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Deduplicate events by `event_id` ordering by `event_timestamp` descending."""
    return deduplicate_latest_by_key(df, "event_id", "event_timestamp")


def clean_events_df(df: DataFrame) -> DataFrame:
    """Standardize clickstream telemetry events, derive event_date, and deduplicate."""
    steps: tuple[TransformStep, ...] = (
        filter_valid_event_rows,
        standardize_and_enrich_event_columns,
        deduplicate_events,
        attach_column_descriptions,
    )
    return apply_transform_pipeline(df, steps)
