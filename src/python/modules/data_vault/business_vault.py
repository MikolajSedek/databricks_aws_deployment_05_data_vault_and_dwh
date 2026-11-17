"""Pure PySpark transformations for Data Vault 2.0 Business Vault (PIT, Bridge, and BV Satellites).

Follows Palantir PySpark Style Guide and Mikołaj Sędek's Functional PySpark standards:
- Column-level expression helpers (`Column -> Column`)
- Batch `.withColumns({...})` dictionary projections
- Short, composable helper functions with explicit join and window contracts
- Deterministic timestamps derived from upstream load timestamps (`_ldts`) to enable Enzyme incremental refresh
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.python.modules.data_vault.hashing import sha256_hashdiff
from src.python.modules.utils.config import (
    COL_HASHDIFF,
    COL_HK_LNK_ORDER_PRODUCT,
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

# =============================================================================
# Tier 1: Pure Column-Level Business Rule Builders (Column -> Column)
# =============================================================================


def calc_effective_discount_pct(gross_col: Column, discount_col: Column) -> Column:
    """Compute effective percentage discount relative to gross amount."""
    has_positive_gross = gross_col > F.lit(0.0)
    return F.when(has_positive_gross, F.round((discount_col / gross_col) * F.lit(100.0), 2)).otherwise(F.lit(0.0))


def classify_order_value_tier(net_col: Column) -> Column:
    """Classify order value bracket from net transaction amount."""
    is_high = net_col >= F.lit(250.0)
    is_medium = net_col >= F.lit(100.0)
    return (
        F.when(is_high, F.lit("HIGH_VALUE")).when(is_medium, F.lit("MEDIUM_VALUE")).otherwise(F.lit("STANDARD_VALUE"))
    )


def classify_customer_ltv_segment(spend_col: Column, orders_col: Column) -> Column:
    """Classify customer behavioral segment from cumulative spend and order count."""
    is_vip = (spend_col >= F.lit(1000.0)) | (orders_col >= F.lit(5))
    is_loyal = (spend_col >= F.lit(300.0)) | (orders_col >= F.lit(2))
    is_active = orders_col == F.lit(1)
    return (
        F.when(is_vip, F.lit("VIP_CHAMPION"))
        .when(is_loyal, F.lit("LOYAL_REGULAR"))
        .when(is_active, F.lit("ACTIVE_BUYER"))
        .otherwise(F.lit("PROSPECT"))
    )


# =============================================================================
# Tier 2: Composable DataFrame Transformations (DataFrame -> DataFrame)
# =============================================================================


def select_latest_satellite_state(sat_df: DataFrame, hk_col: str) -> DataFrame:
    """Filter a Satellite DataFrame to the latest active record per parent hash key."""
    latest_win = Window.partitionBy(hk_col).orderBy(F.col(COL_LDTS).desc_nulls_last())
    return sat_df.withColumns({"_rn": F.row_number().over(latest_win)}).filter(F.col("_rn") == 1).drop("_rn")


def _build_pit_table(
    hub_df: DataFrame,
    sat_df: DataFrame,
    hk_col: str,
    bk_col: str,
    rsrc_label: str,
) -> DataFrame:
    """Construct a Point-in-Time (PIT) table joining a Hub with its latest Satellite state."""
    latest_sat = select_latest_satellite_state(sat_df, hk_col).select(
        F.col(hk_col),
        F.col(COL_LDTS).alias("sat_ldts"),
        F.col(COL_HASHDIFF),
    )
    joined = hub_df.alias("h").join(latest_sat.alias("s"), on=hk_col, how="left")
    resolved_sat_ldts = F.coalesce(F.col("s.sat_ldts"), F.col(f"h.{COL_LDTS}"))
    pit_cols: dict[str, Column] = {
        "sat_ldts": resolved_sat_ldts,
        "pit_snapshot_date": F.to_date(resolved_sat_ldts),
        COL_HASHDIFF: F.coalesce(F.col(f"s.{COL_HASHDIFF}"), F.lit("GHOST_RECORD")),
        COL_LDTS: resolved_sat_ldts,
        COL_RSRC: F.lit(rsrc_label),
    }
    return (
        joined.withColumns(pit_cols)
        .select(hk_col, bk_col, "pit_snapshot_date", "sat_ldts", COL_HASHDIFF, COL_LDTS, COL_RSRC)
        .transform(attach_column_descriptions)
    )


def build_pit_users(hub_users_df: DataFrame, sat_user_details_df: DataFrame) -> DataFrame:
    """Build Business Vault Point-in-Time (PIT) table `pit_users` resolving active satellite state per user."""
    return _build_pit_table(
        hub_users_df,
        sat_user_details_df,
        COL_HK_USER,
        COL_USER_ID,
        "BUSINESS_VAULT:PIT_USERS",
    )


def build_pit_products(hub_products_df: DataFrame, sat_product_details_df: DataFrame) -> DataFrame:
    """Build Business Vault Point-in-Time (PIT) table `pit_products` resolving active satellite state per product."""
    return _build_pit_table(
        hub_products_df,
        sat_product_details_df,
        COL_HK_PRODUCT,
        COL_PRODUCT_ID,
        "BUSINESS_VAULT:PIT_PRODUCTS",
    )


def _derive_order_bv_attributes(df: DataFrame) -> DataFrame:
    """Attach Business Vault order classification columns in a single `.withColumns()` projection."""
    return df.withColumns(
        {
            "effective_discount_pct": calc_effective_discount_pct(F.col("gross_amount"), F.col("discount_amount")),
            "is_high_value_order": F.col("net_amount") >= F.lit(250.0),
            "order_value_tier": classify_order_value_tier(F.col("net_amount")),
            "is_fulfilled": F.col("order_status").isin("COMPLETED", "SHIPPED"),
        }
    )


def build_sat_order_bv(sat_order_details_df: DataFrame) -> DataFrame:
    """Build Business Vault Satellite `sat_order_bv` applying enterprise order classification rules."""
    bv_payload = [
        "net_amount",
        "effective_discount_pct",
        "is_high_value_order",
        "order_value_tier",
        "is_fulfilled",
    ]
    audit_and_hash_cols: dict[str, Column] = {
        COL_HASHDIFF: sha256_hashdiff(*bv_payload),
        COL_LDTS: F.col(COL_LDTS),
        COL_RSRC: F.lit("BUSINESS_VAULT:ORDER_RULES"),
    }
    return (
        select_latest_satellite_state(sat_order_details_df, COL_HK_ORDER)
        .transform(_derive_order_bv_attributes)
        .withColumns(audit_and_hash_cols)
        .select(COL_HK_ORDER, COL_HASHDIFF, *bv_payload, COL_LDTS, COL_RSRC)
        .transform(attach_column_descriptions)
    )


def _aggregate_valid_user_orders(
    lnk_user_order_df: DataFrame,
    sat_order_details_df: DataFrame,
) -> DataFrame:
    """Aggregate non-cancelled orders per `hk_user` to compute lifetime order count, spend, and latest `_ldts`."""
    is_valid_status = ~F.col("order_status").isin("CANCELLED", "REFUNDED")
    valid_orders = (
        select_latest_satellite_state(sat_order_details_df, COL_HK_ORDER)
        .filter(is_valid_status)
        .select(COL_HK_ORDER, "net_amount", F.col(COL_LDTS).alias("_order_ldts"))
    )
    return (
        lnk_user_order_df.select(COL_HK_USER, COL_HK_ORDER)
        .join(valid_orders, on=COL_HK_ORDER, how="inner")
        .groupBy(COL_HK_USER)
        .agg(
            F.countDistinct(COL_HK_ORDER).cast("int").alias("lifetime_orders_count"),
            F.round(F.sum("net_amount"), 2).alias("lifetime_spend_usd"),
            F.max("_order_ldts").alias("_max_order_ldts"),
        )
    )


def _enrich_user_ltv_segment(df: DataFrame) -> DataFrame:
    """Coalesce null order metrics and compute `calculated_customer_segment` via `.withColumns()`."""
    orders_col = F.coalesce(F.col("lifetime_orders_count"), F.lit(0))
    spend_col = F.coalesce(F.col("lifetime_spend_usd"), F.lit(0.0))
    return df.withColumns(
        {
            "lifetime_orders_count": orders_col,
            "lifetime_spend_usd": spend_col,
            "calculated_customer_segment": classify_customer_ltv_segment(spend_col, orders_col),
        }
    )


def build_sat_user_tier_bv(
    hub_users_df: DataFrame,
    lnk_user_order_df: DataFrame,
    sat_order_details_df: DataFrame,
) -> DataFrame:
    """Build Business Vault Satellite `sat_user_tier_bv` deriving customer lifetime value and segments."""
    user_metrics = _aggregate_valid_user_orders(lnk_user_order_df, sat_order_details_df)
    bv_payload = ["lifetime_orders_count", "lifetime_spend_usd", "calculated_customer_segment"]
    audit_cols: dict[str, Column] = {
        COL_HASHDIFF: sha256_hashdiff(*bv_payload),
        COL_LDTS: F.greatest(F.col("_hub_ldts"), F.coalesce(F.col("_max_order_ldts"), F.col("_hub_ldts"))),
        COL_RSRC: F.lit("BUSINESS_VAULT:CUSTOMER_SEGMENTATION"),
    }
    return (
        hub_users_df.select(COL_HK_USER, F.col(COL_LDTS).alias("_hub_ldts"))
        .join(user_metrics, on=COL_HK_USER, how="left")
        .transform(_enrich_user_ltv_segment)
        .withColumns(audit_cols)
        .select(COL_HK_USER, COL_HASHDIFF, *bv_payload, COL_LDTS, COL_RSRC)
        .transform(attach_column_descriptions)
    )


def _join_order_bridge_keys(
    hub_users_df: DataFrame,
    hub_orders_df: DataFrame,
    hub_products_df: DataFrame,
    lnk_user_order_df: DataFrame,
    lnk_order_product_df: DataFrame,
) -> DataFrame:
    """Join Hub and Link keys to form the structural spine of `br_order_fulfillment`."""
    links_joined = lnk_user_order_df.select(COL_HK_LNK_USER_ORDER, COL_HK_USER, COL_HK_ORDER).join(
        lnk_order_product_df.select(COL_HK_LNK_ORDER_PRODUCT, COL_HK_ORDER, COL_HK_PRODUCT),
        on=COL_HK_ORDER,
        how="inner",
    )
    return (
        links_joined.join(hub_users_df.select(COL_HK_USER, COL_USER_ID), on=COL_HK_USER, how="inner")
        .join(hub_orders_df.select(COL_HK_ORDER, COL_ORDER_ID), on=COL_HK_ORDER, how="inner")
        .join(hub_products_df.select(COL_HK_PRODUCT, COL_PRODUCT_ID), on=COL_HK_PRODUCT, how="inner")
    )


def build_br_order_fulfillment(
    hub_users_df: DataFrame,
    hub_orders_df: DataFrame,
    hub_products_df: DataFrame,
    lnk_user_order_df: DataFrame,
    lnk_order_product_df: DataFrame,
    sat_order_details_df: DataFrame,
    sat_order_bv_df: DataFrame,
) -> DataFrame:
    """Build Business Vault Bridge table `br_order_fulfillment` pre-joining User, Order, and Product Vault keys."""
    latest_sat_ord = select_latest_satellite_state(sat_order_details_df, COL_HK_ORDER).drop(COL_RSRC, COL_HASHDIFF)
    latest_bv_ord = select_latest_satellite_state(sat_order_bv_df, COL_HK_ORDER).select(
        COL_HK_ORDER,
        "effective_discount_pct",
        "is_high_value_order",
        "order_value_tier",
        "is_fulfilled",
    )
    spine_df = _join_order_bridge_keys(
        hub_users_df, hub_orders_df, hub_products_df, lnk_user_order_df, lnk_order_product_df
    )
    enriched_df = (
        spine_df.join(latest_sat_ord, on=COL_HK_ORDER, how="inner")
        .join(latest_bv_ord, on=COL_HK_ORDER, how="left")
        .withColumns(
            {
                COL_RSRC: F.lit("BUSINESS_VAULT:BR_ORDER_FULFILLMENT"),
            }
        )
    )
    return enriched_df.select(
        COL_HK_ORDER,
        COL_HK_USER,
        COL_HK_PRODUCT,
        COL_HK_LNK_USER_ORDER,
        COL_HK_LNK_ORDER_PRODUCT,
        COL_ORDER_ID,
        COL_USER_ID,
        COL_PRODUCT_ID,
        "order_status",
        "quantity",
        "gross_amount",
        "discount_amount",
        "tax_amount",
        "net_amount",
        "effective_discount_pct",
        "is_high_value_order",
        "order_value_tier",
        "is_fulfilled",
        "currency_code",
        "order_timestamp",
        "order_date",
        COL_LDTS,
        COL_RSRC,
    ).transform(attach_column_descriptions)
