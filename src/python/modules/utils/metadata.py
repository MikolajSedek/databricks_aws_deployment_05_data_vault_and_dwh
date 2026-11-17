"""Pure functions and helpers for column metadata, comments, and Unity Catalog DDL generation."""

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.python.modules.utils.config import CATALOG_DESCRIPTIONS, SCHEMA_DESCRIPTIONS, LakehouseConfig

# Comprehensive Column Descriptions Registry covering every column across all layers
COLUMN_DESCRIPTIONS: Final[dict[str, str]] = {
    # Technical & Audit Columns
    "_ldts": "Load Date Timestamp (UTC) recording when the record was ingested or computed in the Lakehouse.",
    "_rsrc": "Record Source system identifier providing full audit lineage back to the origin feed.",
    "_source_file": "Full Unity Catalog Volume file path from which the raw Parquet record was ingested.",
    "hashdiff": "SHA-256 hash diff of descriptive payload attributes used for deterministic change detection in Satellites.",
    # Business Keys
    "user_id": "Natural business key uniquely identifying a customer/user in the source e-commerce platform.",
    "product_id": "Natural business key uniquely identifying a catalog product SKU.",
    "order_id": "Natural business key uniquely identifying a customer checkout order transaction.",
    "event_id": "Natural business key uniquely identifying a digital clickstream telemetry event.",
    # Data Vault Hash Keys
    "hk_user": "SHA-256 surrogate Hub hash key derived from the normalized user_id business key.",
    "hk_product": "SHA-256 surrogate Hub hash key derived from the normalized product_id business key.",
    "hk_order": "SHA-256 surrogate Hub hash key derived from the normalized order_id business key.",
    "hk_event": "SHA-256 surrogate Hub hash key derived from the normalized event_id business key.",
    "hk_lnk_user_order": "SHA-256 composite Link hash key representing the relationship between user_id and order_id.",
    "hk_lnk_order_product": "SHA-256 composite Link hash key representing the relationship between order_id and product_id.",
    "hk_lnk_user_event": "SHA-256 composite Link hash key representing the relationship between user_id and event_id.",
    # User Attributes
    "email": "Normalized lowercase customer email address used for communication and identity resolution.",
    "first_name": "Customer given first name as captured during account registration.",
    "last_name": "Customer family last name as captured during account registration.",
    "full_name": "Concatenated customer first and last name for reporting and display.",
    "country_code": "ISO-3166 alpha-2 uppercase country code representing customer residence.",
    "loyalty_tier": "Customer loyalty program tier (BRONZE, SILVER, GOLD, PLATINUM).",
    "signup_date": "Calendar date when the customer registered on the platform.",
    "updated_at": "Source system timestamp when the entity record was last created or modified.",
    # Product Attributes
    "product_name": "Commercial display title of the product in the storefront catalog.",
    "category": "High-level merchandising product category (e.g., ELECTRONICS, APPAREL, HOME, BOOKS, SPORTS).",
    "subcategory": "Granular merchandising product subcategory within the parent category.",
    "unit_price": "Standard retail list price per unit of the product in USD.",
    "cost_price": "Wholesale acquisition or manufacturing cost per unit of the product in USD.",
    "is_active": "Boolean flag indicating whether the product is currently active and available for sale.",
    # Order Attributes
    "order_status": "Lifecycle fulfillment status of the order (COMPLETED, SHIPPED, PROCESSING, CANCELLED, REFUNDED).",
    "quantity": "Total number of product units purchased in the order line.",
    "gross_amount": "Total gross transaction monetary value in USD before discounts or tax.",
    "discount_amount": "Promotional discount monetary amount deducted from the order in USD.",
    "tax_amount": "Sales or value-added tax monetary amount applied to the order in USD.",
    "net_amount": "Net order revenue in USD calculated as (gross_amount - discount_amount + tax_amount).",
    "currency_code": "ISO-4217 three-letter currency code for the transaction (e.g., USD, EUR, GBP).",
    "order_timestamp": "Timestamp when the customer submitted the order transaction.",
    "order_date": "Calendar date derived from order_timestamp for date-grain partitioning and joining.",
    # Event Attributes
    "event_type": "Clickstream interaction classification (PAGE_VIEW, ADD_TO_CART, CHECKOUT_START, PURCHASE, PRODUCT_SEARCH).",
    "session_id": "Browser or mobile application session identifier grouping sequential user interactions.",
    "device_type": "Client hardware category used during the session (MOBILE, DESKTOP, TABLET).",
    "page_url": "Target storefront URL path visited during the clickstream event.",
    "duration_seconds": "Time spent on the interaction or page view measured in integer seconds.",
    "event_timestamp": "Exact UTC timestamp when the client telemetry event occurred.",
    "event_date": "Calendar date derived from event_timestamp.",
    "event_count": "Additive unit fact counter equal to 1 per recorded clickstream interaction event.",
    # Business Vault Derived Attributes
    "is_high_value_order": "Business Vault boolean indicator set to true when net_amount >= 250.00 USD.",
    "order_value_tier": "Business Vault revenue bracket classification (HIGH_VALUE, MEDIUM_VALUE, STANDARD_VALUE).",
    "effective_discount_pct": "Calculated percentage discount relative to gross_amount (0.0 to 100.0).",
    "is_fulfilled": "Business Vault boolean indicator showing whether the order reached COMPLETED or SHIPPED status.",
    "lifetime_orders_count": "Cumulative count of valid orders placed by the user across history.",
    "lifetime_spend_usd": "Cumulative net revenue in USD generated by the user across all valid orders.",
    "calculated_customer_segment": "Business Vault behavioral customer segment (VIP_CHAMPION, LOYAL_REGULAR, ACTIVE_BUYER, PROSPECT).",
    "pit_snapshot_date": "Point-in-Time (PIT) snapshot reference date for historical satellite resolution.",
    "sat_ldts": "Effective load timestamp of the active Satellite record as of the PIT snapshot.",
    # Kimball Dimensional DWH Surrogate Keys & Conformed Dimension Attributes
    "sk_user": "Kimball surrogate key uniquely identifying a dimension record in dim_users.",
    "sk_product": "Kimball surrogate key uniquely identifying a dimension record in dim_products.",
    "sk_date": "Integer smart date key in YYYYMMDD format joining to dim_date.",
    "sk_geography": "SHA-256 Kimball surrogate key uniquely identifying a country/region record in dim_geography.",
    "sk_currency": "SHA-256 Kimball surrogate key uniquely identifying a currency record in dim_currency.",
    "sk_order_status": "SHA-256 Kimball surrogate key uniquely identifying an order status and value-tier profile in dim_order_status.",
    "sk_device": "SHA-256 Kimball surrogate key uniquely identifying a client device platform in dim_device.",
    "sk_event_type": "SHA-256 Kimball surrogate key uniquely identifying a clickstream event classification in dim_event_type.",
    "sk_category": "SHA-256 Kimball surrogate key uniquely identifying a product merchandising category in dim_product_category.",
    "date_actual": "Actual calendar date value represented by the dim_date row.",
    "year_actual": "Four-digit calendar year number (e.g., 2025, 2026).",
    "quarter_actual": "Calendar quarter number (1 through 4).",
    "month_actual": "Calendar month number (1 through 12).",
    "month_name": "Full English name of the calendar month (e.g., January).",
    "day_of_month": "Day number within the calendar month (1 through 31).",
    "day_of_week": "ISO day of week number (1=Monday through 7=Sunday).",
    "day_name": "Full English name of the weekday (e.g., Monday).",
    "is_weekend": "Boolean indicator set to true when the calendar date falls on Saturday or Sunday.",
    "country_name": "Official English country name corresponding to country_code.",
    "region": "Macro commercial sales region (NORTH_AMERICA, EUROPE, OTHER).",
    "subregion": "Geographic subregion classification for regional BI breakdowns.",
    "default_currency_code": "Primary ISO-4217 billing currency associated with the country.",
    "currency_name": "Full descriptive name of the transaction currency (e.g., US Dollar, Euro).",
    "currency_symbol": "Display currency symbol (e.g., $, €, £).",
    "usd_exchange_rate": "Reference conversion multiplier from the transaction currency to USD.",
    "status_group": "High-level order status classification (FULFILLED, IN_PROGRESS, EXCEPTION).",
    "is_cancelled_or_refunded": "Boolean indicator set to true when order_status is CANCELLED or REFUNDED.",
    "device_category": "Platform form-factor classification (MOBILE_APP, DESKTOP_WEB, TABLET_WEB).",
    "os_family": "Representative operating system family associated with the device platform.",
    "is_mobile_platform": "Boolean indicator set to true for handheld mobile or tablet devices.",
    "funnel_stage": "E-commerce conversion funnel stage (AWARENESS, CONSIDERATION, INTENT, CONVERSION).",
    "funnel_step_order": "Ordinal progression step number in the conversion funnel (1 through 5).",
    "is_conversion_event": "Boolean indicator set to true when the event represents a completed purchase.",
    "department": "Executive merchandising department grouping (HARDLINES, SOFTLINES, MEDIA).",
    "category_tier": "Strategic merchandising assortment tier (CORE, GROWTH, SPECIALTY).",
    # Kimball Fact Measures & Snapshot Metrics
    "unit_margin": "Gross profit margin per unit calculated as (unit_price - cost_price).",
    "margin_pct": "Gross profit margin percentage relative to retail unit_price.",
    "daily_orders_count": "Total count of orders recorded on the snapshot date for the category.",
    "daily_units_sold": "Total quantity of product units sold on the snapshot date for the category.",
    "daily_gross_revenue": "Total gross revenue in USD generated on the snapshot date for the category.",
    "daily_discount_amount": "Total promotional discounts in USD granted on the snapshot date for the category.",
    "daily_net_revenue": "Total net revenue in USD generated on the snapshot date for the category.",
    "daily_fulfilled_orders": "Count of orders reaching fulfilled status on the snapshot date for the category.",
    "cumulative_category_revenue": "Running cumulative net revenue in USD for the product category over time.",
    # Gold Consumer Views & ML Feature Store Columns
    "fulfilled_revenue_share_pct": "Percentage of daily net revenue derived from fulfilled orders.",
    "avg_order_value_usd": "Average net order value in USD across the aggregated grain.",
    "total_events_count": "Total digital clickstream interactions recorded for the entity.",
    "total_page_views": "Total storefront page view events recorded for the entity.",
    "total_add_to_cart": "Total add-to-cart intent events recorded for the entity.",
    "total_purchases": "Total purchase conversion events recorded for the entity.",
    "total_session_duration_sec": "Cumulative time spent across sessions measured in seconds.",
    "cart_to_purchase_rate_pct": "Conversion rate percentage from add_to_cart events to completed purchases.",
    "days_since_signup": "Tenure in calendar days from customer signup_date to the reference date.",
    "recency_days": "Calendar days elapsed since the customer's most recent order or interaction.",
    "frequency_orders": "Total historical order count used in RFM / churn feature engineering.",
    "monetary_total_spend": "Total historical net spend in USD used in RFM / churn feature engineering.",
    "avg_session_duration_sec": "Average interaction duration in seconds per clickstream event.",
    "engagement_velocity_score": "Composite digital engagement score weighting purchases, cart additions, and browsing.",
    "is_churn_candidate": "Binary ML target label indicator (1 if high churn risk, 0 otherwise).",
    "distinct_buyers_count": "Count of unique customers who purchased the specific product.",
    "repeat_purchase_ratio": "Proportion of product orders originating from repeat customers.",
    "category_revenue_share_pct": "Percentage share of category net revenue contributed by this product.",
    "affinity_index_score": "ML product affinity score combining conversion rate, repeat purchases, and revenue velocity.",
}


def resolve_column_comment(col_name: str, custom_descriptions: dict[str, str] | None = None) -> str:
    """Resolve the Unity Catalog description string for a given column name.

    Args:
        col_name: Column identifier.
        custom_descriptions: Optional dictionary of custom column comment overrides.

    Returns:
        Non-empty column comment string.
    """
    if custom_descriptions and col_name in custom_descriptions:
        return custom_descriptions[col_name]
    return COLUMN_DESCRIPTIONS.get(
        col_name,
        f"Governed Lakehouse attribute '{col_name}' maintained by the Data Vault & Kimball DWH pipeline.",
    )


def build_alter_column_comment_sql(catalog: str, schema_name: str, table_name: str, col_name: str) -> str:
    """Build an idempotent `ALTER TABLE ... ALTER COLUMN ... COMMENT` SQL statement."""
    col_desc = resolve_column_comment(col_name).replace("'", "")
    return f"ALTER TABLE `{catalog}`.`{schema_name}`.`{table_name}` ALTER COLUMN `{col_name}` COMMENT '{col_desc}'"


def attach_column_descriptions(
    df: DataFrame,
    custom_descriptions: dict[str, str] | None = None,
) -> DataFrame:
    """Attach Unity Catalog column comments via a single `.select()` list comprehension projection.

    Follows the Palantir PySpark Style Guide and Mikołaj Sędek's rule avoiding `.withColumn` inside `for` loops.

    Args:
        df: Input PySpark DataFrame.
        custom_descriptions: Optional dictionary overriding or extending default column descriptions.

    Returns:
        DataFrame with `{"comment": description}` metadata attached to every column in one Catalyst projection.
    """
    annotated_cols = [
        F.col(col_name).alias(
            col_name,
            metadata={"comment": resolve_column_comment(col_name, custom_descriptions)},
        )
        for col_name in df.columns
    ]
    return df.select(*annotated_cols)


def build_catalog_and_schema_ddl(config: LakehouseConfig) -> list[str]:
    """Generate idempotent SQL DDL statements to document catalogs, schemas, and Managed Volumes in Unity Catalog.

    Args:
        config: Immutable LakehouseConfig instance.

    Returns:
        Ordered list of SQL DDL strings for catalog, schema, and Managed Volume creation and commenting.
    """
    catalog_comment = CATALOG_DESCRIPTIONS.get(
        config.catalog,
        f"Governed Lakehouse catalog {config.catalog} for Data Vault 2.0 and Kimball DWH.",
    )
    statements: list[str] = [
        f"COMMENT ON CATALOG `{config.catalog}` IS '{catalog_comment}'",
    ]
    target_schemas = (
        config.raw_ingestion_schema,
        config.bronze_schema,
        config.silver_schema,
        config.gold_schema,
    )
    for schema_name in target_schemas:
        schema_comment = SCHEMA_DESCRIPTIONS.get(
            schema_name,
            f"Governed schema {schema_name} in catalog {config.catalog}.",
        )
        statements.append(f"CREATE SCHEMA IF NOT EXISTS `{config.catalog}`.`{schema_name}`")
        statements.append(f"COMMENT ON SCHEMA `{config.catalog}`.`{schema_name}` IS '{schema_comment}'")
    statements.append(
        f"CREATE VOLUME IF NOT EXISTS `{config.catalog}`.`{config.raw_ingestion_schema}`.`{config.volume_name}` "
        "COMMENT 'Managed Volume for landing raw synthetic e-commerce Parquet event feeds.'"
    )
    return statements


def apply_catalog_and_schema_metadata(spark: SparkSession, config: LakehouseConfig) -> list[str]:
    """Execute SQL DDL statements to ensure catalog, schemas, and Managed Volume exist with full descriptions.

    Args:
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        List of executed SQL statements.
    """
    executed: list[str] = []
    for stmt in build_catalog_and_schema_ddl(config):
        spark.sql(stmt)
        executed.append(stmt)
    return executed
