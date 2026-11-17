"""Pure PySpark transformations building Data Vault 2.0 Raw Vault Hubs, Links, and Satellites.

Adheres to Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark rules:
- Column-first expression dictionaries applied via `.withColumns({...})`
- Short, single-concern functions composed via `.transform()`
"""

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.python.modules.data_vault.hashing import sha256_hash_key, sha256_hashdiff
from src.python.modules.utils.config import (
    COL_EVENT_ID,
    COL_HASHDIFF,
    COL_HK_EVENT,
    COL_HK_LNK_ORDER_PRODUCT,
    COL_HK_LNK_USER_EVENT,
    COL_HK_LNK_USER_ORDER,
    COL_HK_ORDER,
    COL_HK_PRODUCT,
    COL_HK_USER,
    COL_LDTS,
    COL_ORDER_ID,
    COL_PRODUCT_ID,
    COL_RSRC,
    COL_USER_ID,
)
from src.python.modules.utils.metadata import attach_column_descriptions


def _ensure_audit_cols(df: DataFrame, default_rsrc: str) -> DataFrame:
    """Ensure `_ldts` and `_rsrc` columns exist on the DataFrame using a single `.withColumns()` call."""
    missing_cols: dict[str, Column] = {}
    if COL_LDTS not in df.columns:
        missing_cols[COL_LDTS] = F.current_timestamp()
    if COL_RSRC not in df.columns:
        missing_cols[COL_RSRC] = F.lit(default_rsrc)
    return df.withColumns(missing_cols) if missing_cols else df


def _add_hub_hash_key(df: DataFrame, hash_key_col: str, business_key_col: str) -> DataFrame:
    """Normalize the business key and attach the SHA-256 Hub hash key in a single `.withColumns()` projection."""
    return df.filter(F.col(business_key_col).isNotNull()).withColumns(
        {
            business_key_col: F.upper(F.trim(F.col(business_key_col))),
            hash_key_col: sha256_hash_key(business_key_col),
        }
    )


def _aggregate_earliest_vault_arrival(df: DataFrame, key_cols: list[str]) -> DataFrame:
    """Aggregate Vault keys to retain the earliest load timestamp (`_ldts`) and first record source (`_rsrc`)."""
    return (
        df.groupBy(*key_cols)
        .agg(
            F.min(COL_LDTS).alias(COL_LDTS),
            F.first(COL_RSRC, ignorenulls=True).alias(COL_RSRC),
        )
        .select(*key_cols, COL_LDTS, COL_RSRC)
    )


def build_hub(
    df: DataFrame,
    hash_key_col: str,
    business_key_col: str,
    default_rsrc: str = "RAW_VAULT",
) -> DataFrame:
    """Construct a Data Vault 2.0 Hub table retaining the earliest arrival per business key.

    Args:
        df: Cleaned source DataFrame.
        hash_key_col: Name of the target surrogate Hub hash key column (e.g., hk_user).
        business_key_col: Name of the natural business key column (e.g., user_id).
        default_rsrc: Fallback record source identifier if _rsrc is absent.

    Returns:
        Deduplicated Hub DataFrame with columns (hash_key_col, business_key_col, _ldts, _rsrc).
    """
    return (
        _ensure_audit_cols(df, default_rsrc)
        .transform(lambda d: _add_hub_hash_key(d, hash_key_col, business_key_col))
        .transform(lambda d: _aggregate_earliest_vault_arrival(d, [hash_key_col, business_key_col]))
        .transform(attach_column_descriptions)
    )


def build_hub_users(df: DataFrame) -> DataFrame:
    """Build Raw Vault `hub_users` from cleaned users dataset."""
    return build_hub(df, COL_HK_USER, COL_USER_ID, "ECOMMERCE:USERS")


def build_hub_products(df: DataFrame) -> DataFrame:
    """Build Raw Vault `hub_products` from cleaned products dataset."""
    return build_hub(df, COL_HK_PRODUCT, COL_PRODUCT_ID, "ECOMMERCE:PRODUCTS")


def build_hub_orders(df: DataFrame) -> DataFrame:
    """Build Raw Vault `hub_orders` from cleaned orders dataset."""
    return build_hub(df, COL_HK_ORDER, COL_ORDER_ID, "ECOMMERCE:ORDERS")


def build_hub_events(df: DataFrame) -> DataFrame:
    """Build Raw Vault `hub_events` from cleaned clickstream events dataset."""
    return build_hub(df, COL_HK_EVENT, COL_EVENT_ID, "ECOMMERCE:EVENTS")


def _filter_non_null_link_keys(df: DataFrame, business_keys: list[str]) -> DataFrame:
    """Filter DataFrame to rows where all participating Link business keys are non-null."""
    predicate = reduce(lambda a, b: a & b, [F.col(bk).isNotNull() for bk in business_keys])
    return df.filter(predicate)


def _attach_link_hash_keys(
    df: DataFrame,
    link_hk_col: str,
    fk_specs: list[tuple[str, str]],
) -> DataFrame:
    """Attach the composite Link hash key and all foreign Hub hash keys in a single `.withColumns()` call."""
    bks = [bk for _, bk in fk_specs]
    hash_cols_map: dict[str, Column] = {
        link_hk_col: sha256_hash_key(*bks),
        **{hk_col: sha256_hash_key(bk_col) for hk_col, bk_col in fk_specs},
    }
    return df.withColumns(hash_cols_map)


def build_link(
    df: DataFrame,
    link_hk_col: str,
    fk_specs: list[tuple[str, str]],
    default_rsrc: str = "RAW_VAULT",
) -> DataFrame:
    """Construct a Data Vault 2.0 Link table capturing many-to-many / associative relationships.

    Args:
        df: Source transactional or event DataFrame.
        link_hk_col: Name of the composite Link hash key column.
        fk_specs: List of (hub_hk_col_name, business_key_col_name) tuples in deterministic order.
        default_rsrc: Fallback record source string.

    Returns:
        Deduplicated Link DataFrame with (link_hk_col, *hub_hk_cols, _ldts, _rsrc).
    """
    bks = [bk for _, bk in fk_specs]
    group_cols = [link_hk_col, *[hk for hk, _ in fk_specs]]
    return (
        _ensure_audit_cols(df, default_rsrc)
        .transform(lambda d: _filter_non_null_link_keys(d, bks))
        .transform(lambda d: _attach_link_hash_keys(d, link_hk_col, fk_specs))
        .transform(lambda d: _aggregate_earliest_vault_arrival(d, group_cols))
        .transform(attach_column_descriptions)
    )


def build_lnk_user_order(orders_df: DataFrame) -> DataFrame:
    """Build Raw Vault `lnk_user_order` linking `hub_users` and `hub_orders`."""
    return build_link(
        orders_df,
        COL_HK_LNK_USER_ORDER,
        [(COL_HK_USER, COL_USER_ID), (COL_HK_ORDER, COL_ORDER_ID)],
        "ECOMMERCE:ORDERS",
    )


def build_lnk_order_product(orders_df: DataFrame) -> DataFrame:
    """Build Raw Vault `lnk_order_product` linking `hub_orders` and `hub_products`."""
    return build_link(
        orders_df,
        COL_HK_LNK_ORDER_PRODUCT,
        [(COL_HK_ORDER, COL_ORDER_ID), (COL_HK_PRODUCT, COL_PRODUCT_ID)],
        "ECOMMERCE:ORDERS",
    )


def build_lnk_user_event(events_df: DataFrame) -> DataFrame:
    """Build Raw Vault `lnk_user_event` linking `hub_users`, `hub_products`, and `hub_events`."""
    return build_link(
        events_df,
        COL_HK_LNK_USER_EVENT,
        [(COL_HK_USER, COL_USER_ID), (COL_HK_PRODUCT, COL_PRODUCT_ID), (COL_HK_EVENT, COL_EVENT_ID)],
        "ECOMMERCE:EVENTS",
    )


def _deduplicate_satellite_hashdiffs(df: DataFrame, parent_hk_col: str) -> DataFrame:
    """Retain the earliest arrival for each unique (parent_hk_col, hashdiff) combination."""
    change_window = Window.partitionBy(parent_hk_col, COL_HASHDIFF).orderBy(F.col(COL_LDTS).asc_nulls_last())
    return df.withColumns({"_rn": F.row_number().over(change_window)}).filter(F.col("_rn") == 1).drop("_rn")


def build_satellite(
    df: DataFrame,
    parent_hk_col: str,
    business_key_col: str,
    payload_cols: list[str],
    default_rsrc: str = "RAW_VAULT",
) -> DataFrame:
    """Construct a Data Vault 2.0 Satellite table with SHA-256 `hashdiff` change detection.

    Args:
        df: Cleaned source DataFrame.
        parent_hk_col: Parent Hub hash key column name.
        business_key_col: Natural business key column name used to derive parent_hk_col.
        payload_cols: List of descriptive payload attribute column names.
        default_rsrc: Fallback record source identifier.

    Returns:
        Satellite DataFrame with (parent_hk_col, hashdiff, *payload_cols, _ldts, _rsrc).
    """
    hash_cols_map = {
        parent_hk_col: sha256_hash_key(business_key_col),
        COL_HASHDIFF: sha256_hashdiff(*payload_cols),
    }
    return (
        _ensure_audit_cols(df, default_rsrc)
        .filter(F.col(business_key_col).isNotNull())
        .withColumns(hash_cols_map)
        .transform(lambda d: _deduplicate_satellite_hashdiffs(d, parent_hk_col))
        .select(parent_hk_col, COL_HASHDIFF, *payload_cols, COL_LDTS, COL_RSRC)
        .transform(attach_column_descriptions)
    )


def build_sat_user_details(users_df: DataFrame) -> DataFrame:
    """Build Raw Vault `sat_user_details` tracking descriptive customer attributes."""
    payload = ["email", "first_name", "last_name", "country_code", "loyalty_tier", "signup_date", "updated_at"]
    return build_satellite(users_df, COL_HK_USER, COL_USER_ID, payload, "ECOMMERCE:USERS")


def build_sat_product_details(products_df: DataFrame) -> DataFrame:
    """Build Raw Vault `sat_product_details` tracking descriptive product catalog attributes."""
    payload = ["product_name", "category", "subcategory", "unit_price", "cost_price", "is_active", "updated_at"]
    return build_satellite(products_df, COL_HK_PRODUCT, COL_PRODUCT_ID, payload, "ECOMMERCE:PRODUCTS")


def build_sat_order_details(orders_df: DataFrame) -> DataFrame:
    """Build Raw Vault `sat_order_details` tracking order line metrics and timestamps."""
    payload = [
        "order_status",
        "quantity",
        "gross_amount",
        "discount_amount",
        "tax_amount",
        "net_amount",
        "currency_code",
        "order_timestamp",
        "order_date",
    ]
    return build_satellite(orders_df, COL_HK_ORDER, COL_ORDER_ID, payload, "ECOMMERCE:ORDERS")


def build_sat_event_details(events_df: DataFrame) -> DataFrame:
    """Build Raw Vault `sat_event_details` tracking clickstream interaction context."""
    payload = [
        "event_type",
        "session_id",
        "device_type",
        "page_url",
        "duration_seconds",
        "event_timestamp",
        "event_date",
    ]
    return build_satellite(events_df, COL_HK_EVENT, COL_EVENT_ID, payload, "ECOMMERCE:EVENTS")
