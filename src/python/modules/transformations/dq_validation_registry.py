"""Registry builders and immutable specification tuples for Data Quality (DQX) and Referential Integrity audits."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from pyspark.sql import SparkSession

from src.python.modules.transformations.dq_expectations import (
    BRONZE_EVENTS_DQX_RULES,
    BRONZE_ORDERS_DQX_RULES,
    BRONZE_PRODUCTS_DQX_RULES,
    BRONZE_USERS_DQX_RULES,
    BUSINESS_VAULT_ORDER_DQX_RULES,
    BUSINESS_VAULT_USER_TIER_DQX_RULES,
    KIMBALL_DAILY_SNAPSHOT_DQX_RULES,
    KIMBALL_DIM_CURRENCY_DQX_RULES,
    KIMBALL_DIM_DATE_DQX_RULES,
    KIMBALL_DIM_DEVICE_DQX_RULES,
    KIMBALL_DIM_EVENT_TYPE_DQX_RULES,
    KIMBALL_DIM_GEOGRAPHY_DQX_RULES,
    KIMBALL_DIM_ORDER_STATUS_DQX_RULES,
    KIMBALL_DIM_PRODUCT_CATEGORY_DQX_RULES,
    KIMBALL_DIM_PRODUCTS_DQX_RULES,
    KIMBALL_DIM_USERS_DQX_RULES,
    KIMBALL_FACT_EVENTS_DQX_RULES,
    KIMBALL_FACT_ORDERS_DQX_RULES,
    ML_AFFINITY_FEATURES_DQX_RULES,
    ML_CHURN_FEATURES_DQX_RULES,
    check_referential_integrity,
    evaluate_dq_expectations,
)

if TYPE_CHECKING:
    from databricks.labs.dqx.engine import DQEngine


class DQXSuiteSpec(NamedTuple):
    """Immutable value tuple binding a fully-qualified table name to its DQX expectation rules."""

    table_fqn: str
    rules: Any


class ReferentialIntegritySpec(NamedTuple):
    """Immutable value tuple binding a child table, parent table, and foreign key join column."""

    child_fqn: str
    parent_fqn: str
    join_key: str


def format_dqx_suite_label(spec: DQXSuiteSpec) -> str:
    """Return the human-readable identifier label for a `DQXSuiteSpec`."""
    return spec.table_fqn


def format_ri_spec_label(spec: ReferentialIntegritySpec) -> str:
    """Return a human-readable foreign key relationship label (`child -> parent (key)`)."""
    return f"{spec.child_fqn} -> {spec.parent_fqn} ({spec.join_key})"


def verify_dqx_suite_spec(spark: SparkSession, engine: DQEngine, spec: DQXSuiteSpec) -> None:
    """Evaluate a `DQXSuiteSpec` against its live table and assert zero invalid rows."""
    summary = evaluate_dq_expectations(spark.read.table(spec.table_fqn), spec.rules, spark=spark, engine=engine)
    if summary.invalid_rows > 0:
        raise AssertionError(f"DQX validation failed on {spec.table_fqn}: {summary.failed_rules}")


def verify_ri_spec(spark: SparkSession, engine: DQEngine, spec: ReferentialIntegritySpec) -> None:
    """Evaluate a `ReferentialIntegritySpec` via DQX and assert zero orphan foreign key records."""
    orphan_count = check_referential_integrity(
        spark.read.table(spec.child_fqn),
        spark.read.table(spec.parent_fqn),
        spec.join_key,
        spark=spark,
        engine=engine,
    )
    if orphan_count > 0:
        raise AssertionError(f"Referential integrity violation: {orphan_count} orphans in {format_ri_spec_label(spec)}")


def build_dqx_suite_specs(catalog: str) -> tuple[DQXSuiteSpec, ...]:
    """Construct immutable `DQXSuiteSpec` value tuples for all 20 validated tables and views."""
    return tuple(DQXSuiteSpec(table_fqn=fqn, rules=rules) for fqn, rules in build_dqx_suites(catalog))


def build_referential_integrity_specs(catalog: str) -> tuple[ReferentialIntegritySpec, ...]:
    """Construct immutable `ReferentialIntegritySpec` value tuples across Data Vault and Kimball models."""
    return tuple(
        ReferentialIntegritySpec(child_fqn=child, parent_fqn=parent, join_key=key)
        for child, parent, key in build_referential_integrity_pairs(catalog)
    )


def build_dqx_suites(catalog: str) -> tuple[tuple[str, Any], ...]:
    """Construct fully-qualified table names and their respective DQX rules for all 20 target tables and views."""
    return (
        (f"{catalog}.bronze.raw_users", BRONZE_USERS_DQX_RULES),
        (f"{catalog}.bronze.raw_products", BRONZE_PRODUCTS_DQX_RULES),
        (f"{catalog}.bronze.raw_orders", BRONZE_ORDERS_DQX_RULES),
        (f"{catalog}.bronze.raw_events", BRONZE_EVENTS_DQX_RULES),
        (f"{catalog}.silver.sat_order_bv", BUSINESS_VAULT_ORDER_DQX_RULES),
        (f"{catalog}.silver.sat_user_tier_bv", BUSINESS_VAULT_USER_TIER_DQX_RULES),
        (f"{catalog}.gold.dim_date", KIMBALL_DIM_DATE_DQX_RULES),
        (f"{catalog}.gold.dim_users", KIMBALL_DIM_USERS_DQX_RULES),
        (f"{catalog}.gold.dim_products", KIMBALL_DIM_PRODUCTS_DQX_RULES),
        (f"{catalog}.gold.dim_geography", KIMBALL_DIM_GEOGRAPHY_DQX_RULES),
        (f"{catalog}.gold.dim_currency", KIMBALL_DIM_CURRENCY_DQX_RULES),
        (f"{catalog}.gold.dim_order_status", KIMBALL_DIM_ORDER_STATUS_DQX_RULES),
        (f"{catalog}.gold.dim_device", KIMBALL_DIM_DEVICE_DQX_RULES),
        (f"{catalog}.gold.dim_event_type", KIMBALL_DIM_EVENT_TYPE_DQX_RULES),
        (f"{catalog}.gold.dim_product_category", KIMBALL_DIM_PRODUCT_CATEGORY_DQX_RULES),
        (f"{catalog}.gold.fct_orders", KIMBALL_FACT_ORDERS_DQX_RULES),
        (f"{catalog}.gold.fct_events", KIMBALL_FACT_EVENTS_DQX_RULES),
        (f"{catalog}.gold.fct_daily_sales_snapshot", KIMBALL_DAILY_SNAPSHOT_DQX_RULES),
        (f"{catalog}.gold.vw_ml_customer_churn_features", ML_CHURN_FEATURES_DQX_RULES),
        (f"{catalog}.gold.vw_ml_product_affinity_features", ML_AFFINITY_FEATURES_DQX_RULES),
    )


def build_referential_integrity_pairs(catalog: str) -> tuple[tuple[str, str, str], ...]:
    """Construct child table, parent table, and join key tuples across Data Vault and Kimball models."""
    return (
        (f"{catalog}.silver.lnk_user_order", f"{catalog}.silver.hub_users", "hk_user"),
        (f"{catalog}.silver.lnk_user_order", f"{catalog}.silver.hub_orders", "hk_order"),
        (f"{catalog}.silver.lnk_order_product", f"{catalog}.silver.hub_orders", "hk_order"),
        (f"{catalog}.silver.lnk_order_product", f"{catalog}.silver.hub_products", "hk_product"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_users", "sk_user"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_products", "sk_product"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_date", "sk_date"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_order_status", "sk_order_status"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_currency", "sk_currency"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_geography", "sk_geography"),
        (f"{catalog}.gold.fct_orders", f"{catalog}.gold.dim_product_category", "sk_category"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_users", "sk_user"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_products", "sk_product"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_date", "sk_date"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_event_type", "sk_event_type"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_device", "sk_device"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_geography", "sk_geography"),
        (f"{catalog}.gold.fct_events", f"{catalog}.gold.dim_product_category", "sk_category"),
        (f"{catalog}.gold.fct_daily_sales_snapshot", f"{catalog}.gold.dim_date", "sk_date"),
        (f"{catalog}.gold.fct_daily_sales_snapshot", f"{catalog}.gold.dim_product_category", "sk_category"),
    )
