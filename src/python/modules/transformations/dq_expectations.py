"""Business-oriented Databricks Labs DQX Data Quality Expectations registry and evaluators.

Integrates Databricks Labs DQX (`databricks.labs.dqx`) to enforce data quality rules,
quarantine invalid records, compute validation metrics, and assert referential integrity
across Bronze, Silver (Data Vault 2.0), and Gold (Kimball DWH) layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from databricks.labs.dqx import check_funcs
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, DQRule
else:
    try:
        from databricks.labs.dqx import check_funcs
        from databricks.labs.dqx.engine import DQEngine
        from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, DQRule
    except ImportError:
        check_funcs = None

        class DQEngine:  # type: ignore[no-redef]
            """Fallback stub when databricks.labs.dqx is not available."""

            def __init__(self, workspace_client: Any = None, spark: Any = None) -> None:
                self.workspace_client = workspace_client
                self.spark = spark

            def apply_checks_and_split(
                self,
                df: DataFrame,
                rules: list[Any],
                ref_dfs: dict[str, DataFrame] | None = None,
            ) -> tuple[DataFrame, DataFrame]:
                return _filter_valid_spark_sql(df, rules), _quarantine_invalid_spark_sql(df, rules)

        class DQDatasetRule:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.name = kwargs.get("name", "rule")

        class DQRowRule:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.name = kwargs.get("name", "rule")

        class DQRule:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.name = kwargs.get("name", "rule")


_DQX_AVAILABLE: Final[bool] = check_funcs is not None

# =============================================================================
# 1. BRONZE LAYER — Domain Ingestion & Format Expectations
# =============================================================================

BRONZE_USERS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_user_id_pattern": "user_id IS NOT NULL AND user_id RLIKE '^USR-[0-9]{5,}$'",
    "valid_email_rfc_format": "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'",
    "valid_iso_country_code": "country_code IS NOT NULL AND country_code RLIKE '^[A-Z]{2}$'",
    "valid_loyalty_tier_domain": "loyalty_tier IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM')",
    "valid_signup_chronology": "signup_date IS NOT NULL AND signup_date >= DATE '2020-01-01' AND updated_at >= CAST(signup_date AS TIMESTAMP)",
}

BRONZE_PRODUCTS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_product_sku_pattern": "product_id IS NOT NULL AND product_id RLIKE '^PRD-[0-9]{4,}$'",
    "valid_product_name_length": "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) >= 3",
    "valid_merchandising_category": "category IN ('ELECTRONICS', 'APPAREL', 'HOME', 'SPORTS', 'BOOKS')",
    "positive_retail_unit_price": "unit_price > 0.0 AND unit_price <= 50000.0",
    "non_negative_wholesale_cost": "cost_price >= 0.0",
    "positive_unit_gross_margin": "unit_price >= cost_price",
}

BRONZE_ORDERS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_order_id_pattern": "order_id IS NOT NULL AND order_id RLIKE '^ORD-[0-9]{6,}$'",
    "valid_order_user_fk": "user_id IS NOT NULL AND user_id RLIKE '^USR-[0-9]{5,}$'",
    "valid_order_product_fk": "product_id IS NOT NULL AND product_id RLIKE '^PRD-[0-9]{4,}$'",
    "reasonable_order_quantity": "quantity >= 1 AND quantity <= 1000",
    "non_negative_gross_amount": "gross_amount > 0.0",
    "discount_capped_at_50_pct": "discount_amount >= 0.0 AND discount_amount <= ROUND(gross_amount * 0.50, 2)",
    "valid_tax_bounds": "tax_amount >= 0.0 AND tax_amount <= ROUND(gross_amount * 0.30, 2)",
    "valid_order_lifecycle_status": "order_status IN ('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED', 'REFUNDED')",
    "valid_iso_currency_code": "currency_code IN ('USD', 'EUR', 'GBP')",
}

BRONZE_EVENTS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_event_id_pattern": "event_id IS NOT NULL AND event_id RLIKE '^EVT-[0-9]{7,}$'",
    "valid_event_user_fk": "user_id IS NOT NULL AND user_id RLIKE '^USR-[0-9]{5,}$'",
    "valid_event_product_fk": "product_id IS NOT NULL AND product_id RLIKE '^PRD-[0-9]{4,}$'",
    "valid_clickstream_event_type": "event_type IN ('PAGE_VIEW', 'PRODUCT_SEARCH', 'ADD_TO_CART', 'CHECKOUT_START', 'PURCHASE')",
    "valid_session_id_format": "session_id IS NOT NULL AND session_id RLIKE '^SES-[0-9]{5,}-[0-9]{3,}$'",
    "valid_device_category": "device_type IN ('DESKTOP', 'MOBILE', 'TABLET')",
    "bounded_interaction_duration": "duration_seconds >= 0 AND duration_seconds <= 86400",
}

# =============================================================================
# 2. SILVER LAYER — Data Vault 2.0 (Raw & Business Vault) Expectations
# =============================================================================

DATA_VAULT_HUB_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_load_timestamp": "_ldts IS NOT NULL",
    "valid_record_source": "_rsrc IS NOT NULL AND LENGTH(TRIM(_rsrc)) >= 5",
}

DATA_VAULT_SAT_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sha256_hashdiff": "hashdiff IS NOT NULL AND LENGTH(hashdiff) = 64",
    "valid_load_timestamp": "_ldts IS NOT NULL",
    "valid_record_source": "_rsrc IS NOT NULL AND LENGTH(TRIM(_rsrc)) >= 5",
}

BUSINESS_VAULT_ORDER_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_hk_order": "hk_order IS NOT NULL AND LENGTH(hk_order) = 64",
    "valid_discount_percentage_range": "effective_discount_pct >= 0.0 AND effective_discount_pct <= 100.0",
    "consistent_high_value_tier": "(is_high_value_order = true AND order_value_tier = 'HIGH_VALUE' AND net_amount >= 250.0) OR (is_high_value_order = false AND order_value_tier IN ('MEDIUM_VALUE', 'STANDARD_VALUE') AND net_amount < 250.0)",
}

BUSINESS_VAULT_USER_TIER_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_hk_user": "hk_user IS NOT NULL AND LENGTH(hk_user) = 64",
    "non_negative_lifetime_metrics": "lifetime_orders_count >= 0 AND lifetime_spend_usd >= 0.0",
    "consistent_customer_segment_rule": "(calculated_customer_segment = 'VIP_CHAMPION' AND (lifetime_spend_usd >= 1000.0 OR lifetime_orders_count >= 5)) OR (calculated_customer_segment = 'LOYAL_REGULAR' AND (lifetime_spend_usd >= 300.0 OR lifetime_orders_count >= 2)) OR (calculated_customer_segment = 'ACTIVE_BUYER' AND lifetime_orders_count = 1) OR (calculated_customer_segment = 'PROSPECT' AND lifetime_orders_count = 0)",
}

# =============================================================================
# 3. GOLD LAYER — Kimball Dimensional DWH & ML Feature Expectations
# =============================================================================

KIMBALL_DIM_DATE_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_date": "sk_date IS NOT NULL AND sk_date >= 20200101 AND sk_date <= 20301231",
    "valid_calendar_month": "month_actual >= 1 AND month_actual <= 12 AND quarter_actual >= 1 AND quarter_actual <= 4",
}

KIMBALL_DIM_USERS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_user": "sk_user IS NOT NULL AND LENGTH(sk_user) = 64",
    "valid_scd2_window": "effective_from IS NOT NULL AND effective_to IS NOT NULL AND effective_to >= effective_from",
    "valid_customer_segment": "calculated_customer_segment IN ('VIP_CHAMPION', 'LOYAL_REGULAR', 'ACTIVE_BUYER', 'PROSPECT')",
}

KIMBALL_DIM_PRODUCTS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_product": "sk_product IS NOT NULL AND LENGTH(sk_product) = 64",
    "non_negative_gross_margin": "gross_margin_usd >= 0.0 AND gross_margin_pct >= 0.0 AND gross_margin_pct <= 100.0",
    "valid_scd2_window": "effective_from IS NOT NULL AND effective_to IS NOT NULL AND effective_to >= effective_from",
}

KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_geography": "sk_geography IS NOT NULL AND LENGTH(sk_geography) = 64",
    "valid_country_code": "country_code IS NOT NULL AND LENGTH(country_code) = 2",
    "valid_region_domain": "region IN ('NORTH_AMERICA', 'EUROPE', 'OTHER')",
}

KIMBALL_DIM_CURRENCY_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_currency": "sk_currency IS NOT NULL AND LENGTH(sk_currency) = 64",
    "valid_currency_code": "currency_code IN ('USD', 'EUR', 'GBP')",
    "positive_usd_exchange_rate": "usd_exchange_rate > 0.0",
}

KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_order_status": "sk_order_status IS NOT NULL AND LENGTH(sk_order_status) = 64",
    "valid_status_category": "status_category IN ('FULFILLED', 'IN_PROGRESS', 'REVERSED', 'OTHER')",
    "consistent_status_flags": "NOT (is_fulfilled = true AND is_cancelled_or_refunded = true)",
}

KIMBALL_DIM_DEVICE_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_device": "sk_device IS NOT NULL AND LENGTH(sk_device) = 64",
    "valid_device_category": "device_category IN ('MOBILE_APP', 'WEB_DESKTOP', 'TABLET_WEB', 'OTHER')",
}

KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_event_type": "sk_event_type IS NOT NULL AND LENGTH(sk_event_type) = 64",
    "valid_funnel_step_order": "funnel_step_order >= 1 AND funnel_step_order <= 99",
    "consistent_conversion_flag": "(event_type = 'PURCHASE' AND is_conversion_event = true) OR (event_type != 'PURCHASE' AND is_conversion_event = false)",
}

KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_sk_category": "sk_category IS NOT NULL AND LENGTH(sk_category) = 64",
    "valid_category_tier": "category_tier IN ('CORE', 'GROWTH', 'SPECIALTY', 'STANDARD')",
}

KIMBALL_FACT_ORDERS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_fact_order_id": "order_id IS NOT NULL AND order_id RLIKE '^ORD-[0-9]{6,}$'",
    "valid_fact_sk_user": "sk_user IS NOT NULL AND LENGTH(sk_user) = 64",
    "valid_fact_sk_product": "sk_product IS NOT NULL AND LENGTH(sk_product) = 64",
    "valid_fact_sk_order_status": "sk_order_status IS NOT NULL AND LENGTH(sk_order_status) = 64",
    "valid_fact_sk_currency": "sk_currency IS NOT NULL AND LENGTH(sk_currency) = 64",
    "valid_fact_sk_geography": "sk_geography IS NOT NULL AND LENGTH(sk_geography) = 64",
    "valid_fact_sk_category": "sk_category IS NOT NULL AND LENGTH(sk_category) = 64",
    "valid_fact_sk_date": "sk_date IS NOT NULL AND sk_date >= 20200101 AND sk_date <= 20301231",
    "accounting_identity_reconciliation": "ABS(net_amount - ROUND(gross_amount - discount_amount + tax_amount, 2)) <= 0.01",
    "positive_net_revenue": "net_amount >= 0.0 AND quantity > 0",
}

KIMBALL_FACT_EVENTS_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_fact_event_id": "event_id IS NOT NULL AND event_id RLIKE '^EVT-[0-9]{7,}$'",
    "valid_fact_sk_user": "sk_user IS NOT NULL AND LENGTH(sk_user) = 64",
    "valid_fact_sk_product": "sk_product IS NOT NULL AND LENGTH(sk_product) = 64",
    "valid_fact_sk_event_type": "sk_event_type IS NOT NULL AND LENGTH(sk_event_type) = 64",
    "valid_fact_sk_device": "sk_device IS NOT NULL AND LENGTH(sk_device) = 64",
    "valid_fact_sk_geography": "sk_geography IS NOT NULL AND LENGTH(sk_geography) = 64",
    "valid_fact_sk_category": "sk_category IS NOT NULL AND LENGTH(sk_category) = 64",
    "valid_fact_sk_date": "sk_date IS NOT NULL AND sk_date >= 20200101 AND sk_date <= 20301231",
    "valid_event_unit_count": "event_count = 1 AND duration_seconds >= 0",
}

KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS: Final[dict[str, str]] = {
    "valid_snapshot_sk_date": "sk_date IS NOT NULL AND sk_date >= 20200101 AND sk_date <= 20301231",
    "valid_snapshot_sk_category": "sk_category IS NOT NULL AND LENGTH(sk_category) = 64",
    "order_status_partition_balance": "total_orders >= (completed_orders + cancelled_orders)",
    "positive_snapshot_revenue": "total_net_revenue_usd >= 0.0 AND total_units_sold >= total_orders",
    "valid_unique_customers_bound": "unique_customers_count >= 1 AND unique_customers_count <= total_orders",
}

ML_CHURN_FEATURES_EXPECTATIONS: Final[dict[str, str]] = {
    "normalized_churn_probability": "churn_risk_score >= 0.0 AND churn_risk_score <= 1.0",
    "consistent_binary_churn_label": "(churn_risk_score >= 0.55 AND is_churn_candidate = 1) OR (churn_risk_score < 0.55 AND is_churn_candidate = 0)",
    "non_negative_recency_days": "days_since_last_order >= 0 AND days_since_last_event >= 0",
}

ML_AFFINITY_FEATURES_EXPECTATIONS: Final[dict[str, str]] = {
    "bounded_repeat_purchase_ratio": "repeat_purchase_ratio >= 0.0 AND repeat_purchase_ratio <= 1.0",
    "bounded_category_revenue_share": "category_revenue_share_pct >= 0.0 AND category_revenue_share_pct <= 100.0",
    "bounded_affinity_index": "affinity_index_score >= 0.0 AND affinity_index_score <= 100.0",
}


# =============================================================================
# 4. DQX Native Rule Conversion & Engine Factory
# =============================================================================


def dict_to_dqx_rules(expectations: dict[str, str], criticality: str = "error") -> list[DQRule]:
    """Convert an expectation dictionary into a list of DQX DQRowRule instances using `check_funcs.sql_expression`.

    Args:
        expectations: Dictionary mapping rule name to SQL boolean expression.
        criticality: DQX criticality level ('error' or 'warn').

    Returns:
        List of DQX DQRowRule instances.
    """
    if not _DQX_AVAILABLE or DQRowRule is None or check_funcs is None:
        return []
    _ensure_spark_session()
    return [
        DQRowRule(
            name=rule_name,
            criticality=criticality,
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": predicate},
        )
        for rule_name, predicate in expectations.items()
    ]


def _ensure_spark_session() -> SparkSession:
    """Ensure an active SparkSession is available for DQX expression initialization."""
    session = SparkSession.getActiveSession()
    if session is None:
        import os
        import sys

        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        session = (
            SparkSession.builder.master("local[1]")
            .appName("dqx-init")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    return session


def _build_bronze_users_rules() -> list[Any]:
    if not _DQX_AVAILABLE or DQRowRule is None or check_funcs is None:
        return []
    _ensure_spark_session()
    return [
        DQRowRule(
            name="valid_user_id_pattern",
            criticality="error",
            check_func=check_funcs.regex_match,
            column="user_id",
            check_func_kwargs={"regex": "^USR-[0-9]{5,}$"},
        ),
        DQRowRule(
            name="valid_email_rfc_format",
            criticality="error",
            check_func=check_funcs.regex_match,
            column="email",
            check_func_kwargs={"regex": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"},
        ),
        DQRowRule(
            name="valid_iso_country_code",
            criticality="error",
            check_func=check_funcs.regex_match,
            column="country_code",
            check_func_kwargs={"regex": r"^[A-Z]{2}$"},
        ),
        DQRowRule(
            name="valid_loyalty_tier_domain",
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="loyalty_tier",
            check_func_kwargs={"allowed": ["'BRONZE'", "'SILVER'", "'GOLD'", "'PLATINUM'"]},
        ),
        DQRowRule(
            name="valid_signup_chronology",
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={
                "expression": "signup_date IS NOT NULL AND signup_date >= DATE '2020-01-01' AND updated_at >= CAST(signup_date AS TIMESTAMP)"
            },
        ),
    ]


def _build_bronze_products_rules() -> list[Any]:
    if not _DQX_AVAILABLE or DQRowRule is None or check_funcs is None:
        return []
    _ensure_spark_session()
    return [
        DQRowRule(
            name="valid_product_sku_pattern",
            criticality="error",
            check_func=check_funcs.regex_match,
            column="product_id",
            check_func_kwargs={"regex": "^PRD-[0-9]{4,}$"},
        ),
        DQRowRule(
            name="valid_product_name_length",
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) >= 3"},
        ),
        DQRowRule(
            name="valid_merchandising_category",
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="category",
            check_func_kwargs={"allowed": ["'ELECTRONICS'", "'APPAREL'", "'HOME'", "'SPORTS'", "'BOOKS'"]},
        ),
        DQRowRule(
            name="positive_retail_unit_price",
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="unit_price",
            check_func_kwargs={"min_limit": 0.01, "max_limit": 50000.0},
        ),
        DQRowRule(
            name="non_negative_wholesale_cost",
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": "cost_price >= 0.0"},
        ),
        DQRowRule(
            name="positive_unit_gross_margin",
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": "unit_price >= cost_price"},
        ),
    ]


BRONZE_USERS_DQX_RULES: Final[list[Any]] = _build_bronze_users_rules()
BRONZE_PRODUCTS_DQX_RULES: Final[list[Any]] = _build_bronze_products_rules()
BRONZE_ORDERS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(BRONZE_ORDERS_EXPECTATIONS)
BRONZE_EVENTS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(BRONZE_EVENTS_EXPECTATIONS)
DATA_VAULT_HUB_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(DATA_VAULT_HUB_EXPECTATIONS)
DATA_VAULT_SAT_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(DATA_VAULT_SAT_EXPECTATIONS)
BUSINESS_VAULT_ORDER_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(BUSINESS_VAULT_ORDER_EXPECTATIONS)
BUSINESS_VAULT_USER_TIER_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(BUSINESS_VAULT_USER_TIER_EXPECTATIONS)
KIMBALL_DIM_DATE_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_DATE_EXPECTATIONS)
KIMBALL_DIM_USERS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_USERS_EXPECTATIONS)
KIMBALL_DIM_PRODUCTS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_PRODUCTS_EXPECTATIONS)
KIMBALL_DIM_GEOGRAPHY_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS)
KIMBALL_DIM_CURRENCY_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_CURRENCY_EXPECTATIONS)
KIMBALL_DIM_ORDER_STATUS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS)
KIMBALL_DIM_DEVICE_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_DEVICE_EXPECTATIONS)
KIMBALL_DIM_EVENT_TYPE_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS)
KIMBALL_DIM_PRODUCT_CATEGORY_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS)
KIMBALL_FACT_ORDERS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_FACT_ORDERS_EXPECTATIONS)
KIMBALL_FACT_EVENTS_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_FACT_EVENTS_EXPECTATIONS)
KIMBALL_DAILY_SNAPSHOT_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS)
ML_CHURN_FEATURES_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(ML_CHURN_FEATURES_EXPECTATIONS)
ML_AFFINITY_FEATURES_DQX_RULES: Final[list[Any]] = dict_to_dqx_rules(ML_AFFINITY_FEATURES_EXPECTATIONS)


def get_dq_engine(spark: SparkSession | None = None, workspace_client: Any = None) -> DQEngine:
    """Create or retrieve a DQX DQEngine configured with an active SparkSession and WorkspaceClient.

    Args:
        spark: Optional active SparkSession. Defaults to active or newly created session.
        workspace_client: Optional Databricks WorkspaceClient. Fallbacks to mock if unauthenticated.

    Returns:
        Configured DQEngine instance.
    """
    active_spark = spark or SparkSession.getActiveSession()
    if active_spark is None:
        active_spark = SparkSession.builder.getOrCreate()

    if workspace_client is None:
        try:
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient()
        except Exception:
            from unittest.mock import MagicMock

            workspace_client = MagicMock()

    return DQEngine(workspace_client=workspace_client, spark=active_spark)


@dataclass(frozen=True)
class DQValidationSummary:
    """Immutable result summary of evaluating DQX expectations on a DataFrame."""

    total_rows: int
    valid_rows: int
    invalid_rows: int
    failed_rules: dict[str, int]

    @property
    def pass_rate(self) -> float:
        """Calculate the proportion of rows passing all expectations (0.0 to 1.0)."""
        if self.total_rows == 0:
            return 1.0
        return round(self.valid_rows / self.total_rows, 4)


def apply_lakeflow_expectations(dp_module: Any, expectations: dict[str, str]) -> Any:
    """Apply `@dp.expect_all_or_drop` when available on Databricks runtime, and attach metadata for local tests.

    Args:
        dp_module: The `pyspark.pipelines` (or mock test) module.
        expectations: Dictionary mapping constraint name to SQL boolean expression.

    Returns:
        Decorator wrapping a Lakeflow pipeline table/view function.
    """
    expect_fn = getattr(dp_module, "expect_all_or_drop", None)
    if callable(expect_fn):
        return expect_fn(expectations)

    def _passthrough_decorator(fn: Any) -> Any:
        fn._expectations = expectations
        return fn

    return _passthrough_decorator


def _filter_valid_spark_sql(df: DataFrame, expectations: dict[str, str] | list[Any]) -> DataFrame:
    """Filter records using pure Spark SQL boolean expressions when DQX engine is unavailable."""
    if not expectations:
        return df
    if isinstance(expectations, dict):
        cond = " AND ".join(f"({expr})" for expr in expectations.values())
        return df.filter(F.expr(cond))
    return df


def _quarantine_invalid_spark_sql(df: DataFrame, expectations: dict[str, str] | list[Any]) -> DataFrame:
    """Quarantine records using pure Spark SQL boolean expressions when DQX engine is unavailable."""
    if not expectations or not isinstance(expectations, dict):
        return df.limit(0)
    cond = " OR ".join(f"NOT ({expr})" for expr in expectations.values())
    return df.filter(F.expr(cond))


def filter_valid_records(
    df: DataFrame,
    expectations: dict[str, str] | list[DQRule],
    spark: SparkSession | None = None,
    engine: DQEngine | None = None,
) -> DataFrame:
    """Filter a DataFrame using Databricks Labs DQX engine to retain only valid records passing all checks.

    Args:
        df: Input PySpark DataFrame.
        expectations: Either a list of DQX DQRule instances or a dictionary of rule name -> SQL expression.
        spark: Optional active SparkSession.
        engine: Optional pre-configured DQEngine instance.

    Returns:
        Filtered DataFrame containing only valid rows, with DQX audit columns stripped.
    """
    if not expectations:
        return df

    if not _DQX_AVAILABLE or DQEngine is None or DQEngine is Any:
        return _filter_valid_spark_sql(df, expectations)

    rules: list[DQRule] = dict_to_dqx_rules(expectations) if isinstance(expectations, dict) else expectations
    dq_engine = engine or get_dq_engine(spark or df.sparkSession)
    valid_df, _ = dq_engine.apply_checks_and_split(df, rules)
    cols_to_drop = [c for c in ("_errors", "_warnings", "_info") if c in valid_df.columns]
    return cast(DataFrame, valid_df.drop(*cols_to_drop) if cols_to_drop else valid_df)


def quarantine_invalid_records(
    df: DataFrame,
    expectations: dict[str, str] | list[DQRule],
    spark: SparkSession | None = None,
    engine: DQEngine | None = None,
) -> DataFrame:
    """Isolate invalid records that violate one or more DQX rules using the DQX engine.

    Args:
        df: Input PySpark DataFrame.
        expectations: Either a list of DQX DQRule instances or a dictionary of rule name -> SQL expression.
        spark: Optional active SparkSession.
        engine: Optional pre-configured DQEngine instance.

    Returns:
        DataFrame of quarantined rows with DQX `_errors` diagnostics attached.
    """
    if not expectations:
        return df.limit(0)

    if not _DQX_AVAILABLE or DQEngine is None or DQEngine is Any:
        return _quarantine_invalid_spark_sql(df, expectations)

    rules: list[DQRule] = dict_to_dqx_rules(expectations) if isinstance(expectations, dict) else expectations
    dq_engine = engine or get_dq_engine(spark or df.sparkSession)
    _, invalid_df = dq_engine.apply_checks_and_split(df, rules)
    if "_errors" in invalid_df.columns:
        invalid_df = invalid_df.withColumns({"violated_rules": F.col("_errors.name")})
    return cast(DataFrame, invalid_df)


def check_referential_integrity(
    child_df: DataFrame,
    parent_df: DataFrame,
    join_key: str,
    spark: SparkSession | None = None,
    engine: DQEngine | None = None,
) -> int:
    """Assert referential integrity using DQX `DQDatasetRule` and `check_funcs.foreign_key`.

    Args:
        child_df: Child table DataFrame (e.g., Link, Satellite, or Fact table).
        parent_df: Parent table DataFrame (e.g., Hub or Dimension table).
        join_key: Foreign key column name common to both DataFrames.
        spark: Optional active SparkSession.
        engine: Optional pre-configured DQEngine instance.

    Returns:
        Integer count of orphan rows violating the foreign key check (0 = 100% referential integrity).
    """
    if not _DQX_AVAILABLE or DQEngine is None or DQEngine is Any or check_funcs is None:
        return int(child_df.join(parent_df, on=join_key, how="left_anti").count())

    ref_name = f"ref_{join_key}_parent"
    fk_rule = DQDatasetRule(
        name=f"ri_{join_key}_fk_check",
        criticality="error",
        check_func=check_funcs.foreign_key,
        check_func_kwargs={
            "columns": [join_key],
            "ref_columns": [join_key],
            "ref_df_name": ref_name,
        },
    )
    dq_engine = engine or get_dq_engine(spark or child_df.sparkSession)
    _, invalid_df = dq_engine.apply_checks_and_split(child_df, [fk_rule], ref_dfs={ref_name: parent_df})
    return int(invalid_df.count())


def evaluate_dq_expectations(
    df: DataFrame,
    expectations: dict[str, str] | list[DQRule],
    spark: SparkSession | None = None,
    ref_dfs: dict[str, DataFrame] | None = None,
    engine: DQEngine | None = None,
) -> DQValidationSummary:
    """Evaluate quality rules using the Databricks Labs DQX engine and return a `DQValidationSummary`.

    Args:
        df: Input PySpark DataFrame to validate.
        expectations: Either a list of DQX DQRule instances or a dictionary of rule name -> SQL expression.
        spark: Optional active SparkSession.
        ref_dfs: Optional reference DataFrames for dataset-level rules (e.g. foreign keys).
        engine: Optional pre-configured DQEngine instance.

    Returns:
        DQValidationSummary containing total, valid, invalid row counts and per-rule failure counts.
    """
    if not _DQX_AVAILABLE or DQEngine is None or DQEngine is Any:
        total = df.count()
        if total == 0 or not expectations or not isinstance(expectations, dict):
            return DQValidationSummary(total_rows=total, valid_rows=total, invalid_rows=0, failed_rules={})
        valid_df = _filter_valid_spark_sql(df, expectations)
        valid_count = valid_df.count()
        invalid_count = total - valid_count
        sql_failed_counts: dict[str, int] = {}
        for rule_name, predicate in expectations.items():
            failed_n = df.filter(F.expr(f"NOT ({predicate})")).count()
            if failed_n > 0:
                sql_failed_counts[rule_name] = failed_n
        return DQValidationSummary(
            total_rows=total, valid_rows=valid_count, invalid_rows=invalid_count, failed_rules=sql_failed_counts
        )

    total = df.count()
    if total == 0 or not expectations:
        return DQValidationSummary(total_rows=total, valid_rows=total, invalid_rows=0, failed_rules={})

    rules: list[DQRule] = dict_to_dqx_rules(expectations) if isinstance(expectations, dict) else expectations
    dq_engine = engine or get_dq_engine(spark or df.sparkSession)
    valid_df, invalid_df = dq_engine.apply_checks_and_split(df, rules, ref_dfs=ref_dfs)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    failed_counts: dict[str, int] = {}
    if invalid_count > 0 and "_errors" in invalid_df.columns:
        err_df = invalid_df.select(F.explode(F.col("_errors.name")).alias("rule_name")).groupBy("rule_name").count()
        for row in err_df.collect():
            failed_counts[row["rule_name"]] = row["count"]

    return DQValidationSummary(
        total_rows=total,
        valid_rows=valid_count,
        invalid_rows=invalid_count,
        failed_rules=failed_counts,
    )
