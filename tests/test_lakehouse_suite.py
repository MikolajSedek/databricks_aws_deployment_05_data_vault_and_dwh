"""Comprehensive unit tests for utils, io, transformations, data_vault, kimball_dwh, DQX rules, and pipelines."""

from datetime import date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from pyspark.sql import DataFrame, SparkSession
from src.python.modules.data_vault.business_vault import (
    build_br_order_fulfillment,
    build_pit_products,
    build_pit_users,
    build_sat_order_bv,
    build_sat_user_tier_bv,
)
from src.python.modules.data_vault.hashing import sha256_hash_key, sha256_hashdiff
from src.python.modules.data_vault.raw_vault import (
    build_hub_events,
    build_hub_orders,
    build_hub_products,
    build_hub_users,
    build_lnk_order_product,
    build_lnk_user_event,
    build_lnk_user_order,
    build_sat_event_details,
    build_sat_order_details,
    build_sat_product_details,
    build_sat_user_details,
)
from src.python.modules.io.data_generation import (
    EVENTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    USERS_SCHEMA,
    build_sample_event_dataframes,
    generate_events_records,
    generate_orders_records,
    generate_products_records,
    generate_users_records,
    resolve_next_batch_offsets,
    write_parquet_events_to_volume,
)
from src.python.modules.io.genie_client import (
    build_genie_space_create_payload,
    extract_mcp_text,
    find_space_id_by_title,
    format_genie_attachments,
    parse_mcp_structured_content,
)
from src.python.modules.io.volume_readers import enrich_bronze_audit_columns
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
from src.python.modules.transformations.cleaning import (
    clean_events_df,
    clean_orders_df,
    clean_products_df,
    clean_users_df,
)
from src.python.modules.transformations.dq_expectations import (
    BRONZE_EVENTS_EXPECTATIONS,
    BRONZE_ORDERS_EXPECTATIONS,
    BRONZE_PRODUCTS_EXPECTATIONS,
    BRONZE_USERS_EXPECTATIONS,
    BUSINESS_VAULT_ORDER_EXPECTATIONS,
    BUSINESS_VAULT_USER_TIER_EXPECTATIONS,
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
    check_referential_integrity,
    evaluate_dq_expectations,
    filter_valid_records,
    get_dq_engine,
    quarantine_invalid_records,
)
from src.python.modules.transformations.dq_validation_registry import (
    build_dqx_suite_specs,
    build_dqx_suites,
    build_referential_integrity_pairs,
    build_referential_integrity_specs,
    format_dqx_suite_label,
    format_ri_spec_label,
)
from src.python.modules.utils.config import DEFAULT_CONFIG, EXPECTED_TABLES_BY_SCHEMA, LakehouseConfig
from src.python.modules.utils.metadata import (
    apply_catalog_and_schema_metadata,
    attach_column_descriptions,
    build_catalog_and_schema_ddl,
)
from src.python.pipelines.lakeflow_data_vault_dwh_pipeline import register_lakeflow_pipeline
from src.python.pipelines.lakeflow_data_vault_pipeline import register_data_vault_pipeline
from src.python.pipelines.lakeflow_kimball_dwh_pipeline import register_kimball_dwh_pipeline
from src.python.pipelines.validate_and_audit_dq import (
    _dispatch_concurrent_tasks,
    _set_spark_thread_context,
)
from src.python.pipelines.verify_live_warehouse import (
    build_column_comment_coverage_query,
    build_missing_column_comments_query,
    build_table_comment_coverage_query,
    select_preferred_warehouse_id,
)


def _assert_all_columns_have_comments(df: DataFrame) -> None:
    """Assert every field in the DataFrame schema has a non-empty comment in metadata."""
    for field in df.schema.fields:
        assert "comment" in field.metadata, f"Column '{field.name}' is missing 'comment' metadata!"
        assert len(str(field.metadata["comment"]).strip()) > 5


def _materialize_table(df: DataFrame) -> DataFrame:
    """Cache and trigger a fast count action to mirror materialized Lakeflow Delta tables."""
    cached = df.cache()
    assert cached.count() > 0
    return cached


def test_struct_type_nullability_and_metadata_contracts() -> None:
    """Verify explicit nullability (`nullable=False` vs `nullable=True`) and field comments on all schemas."""
    users_nullability = {f.name: f.nullable for f in USERS_SCHEMA.fields}
    assert users_nullability["user_id"] is False
    assert users_nullability["email"] is False
    assert users_nullability["country_code"] is False
    assert users_nullability["first_name"] is True
    assert users_nullability["last_name"] is True

    products_nullability = {f.name: f.nullable for f in PRODUCTS_SCHEMA.fields}
    assert products_nullability["product_id"] is False
    assert products_nullability["unit_price"] is False
    assert products_nullability["subcategory"] is True

    orders_nullability = {f.name: f.nullable for f in ORDERS_SCHEMA.fields}
    assert orders_nullability["order_id"] is False
    assert orders_nullability["user_id"] is False
    assert orders_nullability["product_id"] is False
    assert orders_nullability["discount_amount"] is True
    assert orders_nullability["tax_amount"] is True

    events_nullability = {f.name: f.nullable for f in EVENTS_SCHEMA.fields}
    assert events_nullability["event_id"] is False
    assert events_nullability["session_id"] is False
    assert events_nullability["device_type"] is True
    assert events_nullability["page_url"] is True


def test_config_and_ddl_generation() -> None:
    """Test LakehouseConfig properties and Unity Catalog DDL generation including Managed Volume."""
    cfg = LakehouseConfig(catalog="test_cat")
    assert cfg.volume_base_path == "/Volumes/test_cat/raw_ingestion/raw_events_landing"
    assert cfg.dataset_volume_path("orders") == (
        "/Volumes/test_cat/raw_ingestion/raw_events_landing/parquet_events/orders"
    )
    ddls = build_catalog_and_schema_ddl(cfg)
    assert len(ddls) == 10
    assert any("COMMENT ON CATALOG `test_cat`" in s for s in ddls)
    assert any("COMMENT ON SCHEMA `test_cat`.`silver`" in s for s in ddls)
    assert any("CREATE VOLUME IF NOT EXISTS `test_cat`.`raw_ingestion`.`raw_events_landing`" in s for s in ddls)

    mock_spark = MagicMock()
    executed = apply_catalog_and_schema_metadata(mock_spark, cfg)
    assert len(executed) == 10
    assert mock_spark.sql.call_count == 10


def test_pure_record_generators() -> None:
    """Test pure Python synthetic record generators with custom incremental `start_idx` offsets."""
    users = generate_users_records(10)
    products = generate_products_records(8)
    orders = generate_orders_records(15, 10, 8)
    events = generate_events_records(20, 10, 8)

    assert len(users) == 10
    assert len(products) == 8
    assert len(orders) == 15
    assert len(events) == 20
    assert users[0][0] == "USR-00001"
    assert products[0][0] == "PRD-0001"

    users_batch2 = generate_users_records(5, start_idx=11)
    products_batch2 = generate_products_records(4, start_idx=9)
    orders_batch2 = generate_orders_records(6, 15, 12, start_idx=16)
    events_batch2 = generate_events_records(7, 15, 12, start_idx=21)

    assert users_batch2[0][0] == "USR-00011"
    assert users_batch2[-1][0] == "USR-00015"
    assert products_batch2[0][0] == "PRD-0009"
    assert orders_batch2[0][0] == "ORD-000016"
    assert events_batch2[0][0] == "EVT-0000021"


def test_data_generation_and_parquet_write(spark: SparkSession, tmp_path: Path) -> None:
    """Test sample DataFrame creation, column comments, and incremental multi-batch Parquet writing."""
    dfs = build_sample_event_dataframes(spark, num_users=10, num_products=5, num_orders=15, num_events=20)
    assert set(dfs.keys()) == {"users", "products", "orders", "events"}
    for df in dfs.values():
        _assert_all_columns_have_comments(df)

    enriched_users = enrich_bronze_audit_columns(dfs["users"], "UNIT_TEST_SRC")
    assert "_ldts" in enriched_users.columns
    assert "_rsrc" in enriched_users.columns
    assert "_source_file" in enriched_users.columns
    _assert_all_columns_have_comments(enriched_users)

    mock_cfg = MagicMock(spec=LakehouseConfig)
    mock_cfg.dataset_volume_path.side_effect = lambda name: str(tmp_path / name)

    # Initial empty directory offsets default to 1
    initial_offsets = resolve_next_batch_offsets(spark, mock_cfg)
    assert initial_offsets == {"users": 1, "products": 1, "orders": 1, "events": 1}

    # First incremental batch write
    written = write_parquet_events_to_volume(
        spark,
        mock_cfg,
        num_users=6,
        num_products=4,
        num_orders=8,
        num_events=10,
        max_workers=2,
        incremental=True,
    )
    assert len(written) == 4
    assert spark.read.parquet(written["users"]).count() == 6
    assert spark.read.parquet(written["orders"]).count() == 8

    # Verify next batch offsets advance past batch 1
    second_offsets = resolve_next_batch_offsets(spark, mock_cfg)
    assert second_offsets == {"users": 7, "products": 5, "orders": 9, "events": 11}

    # Second incremental batch write appends without overwriting batch 1
    write_parquet_events_to_volume(
        spark,
        mock_cfg,
        num_users=6,
        num_products=4,
        num_orders=8,
        num_events=10,
        max_workers=2,
        incremental=True,
    )
    assert spark.read.parquet(written["users"]).count() == 12
    assert spark.read.parquet(written["products"]).count() == 8
    assert spark.read.parquet(written["orders"]).count() == 16
    assert spark.read.parquet(written["events"]).count() == 20


def test_detailed_business_dq_expectations_and_quarantine(spark: SparkSession) -> None:
    """Test business-oriented DQX expectations across users, products, and orders, plus quarantine isolation."""
    dq_engine = get_dq_engine(spark, MagicMock())
    user_rows = [
        (
            "USR-00001",
            "alice.smith@example.com",
            "Alice",
            "Smith",
            "US",
            "GOLD",
            date(2025, 1, 1),
            datetime(2025, 1, 2, 10, 0),
        ),
        (
            "BAD-ID",
            "invalid-email",
            "Bob",
            "Jones",
            "USA",
            "UNKNOWN_TIER",
            date(2025, 2, 10),
            datetime(2025, 1, 1, 0, 0),
        ),
    ]
    user_df = spark.createDataFrame(user_rows, schema=USERS_SCHEMA)

    user_summary = evaluate_dq_expectations(user_df, BRONZE_USERS_EXPECTATIONS, spark=spark, engine=dq_engine)
    assert user_summary.total_rows == 2
    assert user_summary.valid_rows == 1
    assert user_summary.invalid_rows == 1
    assert user_summary.pass_rate == 0.5
    assert "valid_user_id_pattern" in user_summary.failed_rules
    assert "valid_email_rfc_format" in user_summary.failed_rules
    assert "valid_iso_country_code" in user_summary.failed_rules
    assert "valid_loyalty_tier_domain" in user_summary.failed_rules
    assert "valid_signup_chronology" in user_summary.failed_rules

    quarantined_users = quarantine_invalid_records(user_df, BRONZE_USERS_EXPECTATIONS, spark=spark, engine=dq_engine)
    assert quarantined_users.count() == 1
    violated_rules = quarantined_users.collect()[0]["violated_rules"]
    assert len(violated_rules) == 5

    prod_rows = [
        ("PRD-0001", "Valid Headphone", "ELECTRONICS", "AUDIO", 150.0, 80.0, True, datetime(2025, 1, 1, 12, 0)),
        ("PRD-0002", "Loss Leader", "INVALID_CAT", "SUB", 50.0, 90.0, True, datetime(2025, 1, 1, 12, 0)),
    ]
    prod_df = spark.createDataFrame(prod_rows, schema=PRODUCTS_SCHEMA)
    prod_summary = evaluate_dq_expectations(prod_df, BRONZE_PRODUCTS_EXPECTATIONS, spark=spark, engine=dq_engine)
    assert prod_summary.valid_rows == 1
    assert "valid_merchandising_category" in prod_summary.failed_rules
    assert "positive_unit_gross_margin" in prod_summary.failed_rules

    ord_rows = [
        (
            "ORD-000001",
            "USR-00001",
            "PRD-0001",
            "COMPLETED",
            2,
            100.0,
            10.0,
            7.2,
            "USD",
            datetime(2025, 2, 1, 10, 0),
        ),
        (
            "ORD-000002",
            "USR-00001",
            "PRD-0001",
            "COMPLETED",
            2,
            100.0,
            75.0,
            45.0,
            "INVALID_CCY",
            datetime(2025, 2, 1, 10, 0),
        ),
    ]
    ord_df = spark.createDataFrame(ord_rows, schema=ORDERS_SCHEMA)
    ord_summary = evaluate_dq_expectations(ord_df, BRONZE_ORDERS_EXPECTATIONS, spark=spark, engine=dq_engine)
    assert ord_summary.valid_rows == 1
    assert "discount_capped_at_50_pct" in ord_summary.failed_rules
    assert "valid_tax_bounds" in ord_summary.failed_rules
    assert "valid_iso_currency_code" in ord_summary.failed_rules

    assert evaluate_dq_expectations(user_df, {}, spark=spark, engine=dq_engine).pass_rate == 1.0
    assert filter_valid_records(user_df, {}, spark=spark, engine=dq_engine).count() == 2
    assert quarantine_invalid_records(user_df, {}, spark=spark, engine=dq_engine).count() == 0


def test_conformed_kimball_dimensions_attributes(spark: SparkSession) -> None:
    """Verify domain enrichments and surrogate key generation across all conformed Kimball dimensions."""
    dfs = build_sample_event_dataframes(spark, num_users=12, num_products=6, num_orders=24, num_events=30)
    sat_users = _materialize_table(
        build_sat_user_details(clean_users_df(enrich_bronze_audit_columns(dfs["users"], "TEST")))
    )
    sat_products = _materialize_table(
        build_sat_product_details(clean_products_df(enrich_bronze_audit_columns(dfs["products"], "TEST")))
    )
    sat_orders = _materialize_table(
        build_sat_order_details(clean_orders_df(enrich_bronze_audit_columns(dfs["orders"], "TEST")))
    )
    sat_orders_bv = _materialize_table(build_sat_order_bv(sat_orders))
    sat_events = _materialize_table(
        build_sat_event_details(clean_events_df(enrich_bronze_audit_columns(dfs["events"], "TEST")))
    )

    dim_geo = build_dim_geography(sat_users)
    geo_map = {r["country_code"]: r.asDict() for r in dim_geo.collect()}
    if "US" in geo_map:
        assert geo_map["US"]["region"] == "NORTH_AMERICA"
        assert geo_map["US"]["default_currency_code"] == "USD"

    dim_ccy = build_dim_currency(sat_orders)
    ccy_map = {r["currency_code"]: r.asDict() for r in dim_ccy.collect()}
    if "EUR" in ccy_map:
        assert ccy_map["EUR"]["usd_exchange_rate"] == 1.08
        assert ccy_map["EUR"]["currency_symbol"] == "€"

    dim_status = build_dim_order_status(sat_orders, sat_orders_bv)
    for row in dim_status.collect():
        assert len(row["sk_order_status"]) == 64
        assert not (row["is_fulfilled"] and row["is_cancelled_or_refunded"])

    dim_dev = build_dim_device(sat_events)
    dev_map = {r["device_type"]: r.asDict() for r in dim_dev.collect()}
    if "MOBILE" in dev_map:
        assert dev_map["MOBILE"]["is_mobile_platform"] is True
        assert dev_map["MOBILE"]["device_category"] == "MOBILE_APP"

    dim_evt = build_dim_event_type(sat_events)
    evt_map = {r["event_type"]: r.asDict() for r in dim_evt.collect()}
    if "PURCHASE" in evt_map:
        assert evt_map["PURCHASE"]["funnel_step_order"] == 5
        assert evt_map["PURCHASE"]["is_conversion_event"] is True

    dim_cat = build_dim_product_category(sat_products)
    cat_map = {r["category"]: r.asDict() for r in dim_cat.collect()}
    if "ELECTRONICS" in cat_map:
        assert cat_map["ELECTRONICS"]["department"] == "HARDLINES"
        assert cat_map["ELECTRONICS"]["category_tier"] == "CORE"


def test_end_to_end_data_vault_and_kimball_pipeline_with_dq_and_ri(spark: SparkSession) -> None:
    """Test full functional pipeline from Bronze -> Raw Vault -> Business Vault -> 9 Dims + 3 Facts -> Views + DQX."""
    dq_engine = get_dq_engine(spark, MagicMock())
    dfs = build_sample_event_dataframes(spark, num_users=15, num_products=8, num_orders=30, num_events=45)

    # 1. Clean transformations
    users_clean = _materialize_table(clean_users_df(enrich_bronze_audit_columns(dfs["users"], "SRC:USERS")))
    products_clean = _materialize_table(clean_products_df(enrich_bronze_audit_columns(dfs["products"], "SRC:PRODUCTS")))
    orders_clean = _materialize_table(clean_orders_df(enrich_bronze_audit_columns(dfs["orders"], "SRC:ORDERS")))
    events_clean = _materialize_table(clean_events_df(enrich_bronze_audit_columns(dfs["events"], "SRC:EVENTS")))

    assert (
        evaluate_dq_expectations(users_clean, BRONZE_USERS_EXPECTATIONS, spark=spark, engine=dq_engine).pass_rate == 1.0
    )
    assert (
        evaluate_dq_expectations(products_clean, BRONZE_PRODUCTS_EXPECTATIONS, spark=spark, engine=dq_engine).pass_rate
        == 1.0
    )
    assert (
        evaluate_dq_expectations(orders_clean, BRONZE_ORDERS_EXPECTATIONS, spark=spark, engine=dq_engine).pass_rate
        == 1.0
    )
    assert (
        evaluate_dq_expectations(events_clean, BRONZE_EVENTS_EXPECTATIONS, spark=spark, engine=dq_engine).pass_rate
        == 1.0
    )

    # 2. Raw Vault: Hubs, Links, Satellites
    hub_users = _materialize_table(build_hub_users(users_clean))
    hub_products = _materialize_table(build_hub_products(products_clean))
    hub_orders = _materialize_table(build_hub_orders(orders_clean))
    hub_events = _materialize_table(build_hub_events(events_clean))

    lnk_user_order = _materialize_table(build_lnk_user_order(orders_clean))
    lnk_order_product = _materialize_table(build_lnk_order_product(orders_clean))
    lnk_user_event = _materialize_table(build_lnk_user_event(events_clean))

    sat_user_details = _materialize_table(build_sat_user_details(users_clean))
    sat_product_details = _materialize_table(build_sat_product_details(products_clean))
    sat_order_details = _materialize_table(build_sat_order_details(orders_clean))
    sat_event_details = _materialize_table(build_sat_event_details(events_clean))

    # Verify Data Vault Referential Integrity (0 orphans)
    assert check_referential_integrity(lnk_user_order, hub_users, "hk_user", spark=spark, engine=dq_engine) == 0
    assert check_referential_integrity(lnk_user_order, hub_orders, "hk_order", spark=spark, engine=dq_engine) == 0
    assert check_referential_integrity(lnk_order_product, hub_orders, "hk_order", spark=spark, engine=dq_engine) == 0
    assert (
        check_referential_integrity(lnk_order_product, hub_products, "hk_product", spark=spark, engine=dq_engine) == 0
    )
    assert check_referential_integrity(sat_user_details, hub_users, "hk_user", spark=spark, engine=dq_engine) == 0

    # 3. Business Vault: PITs, BV Satellites, Bridge
    pit_users = _materialize_table(build_pit_users(hub_users, sat_user_details))
    pit_products = _materialize_table(build_pit_products(hub_products, sat_product_details))
    sat_order_bv = _materialize_table(build_sat_order_bv(sat_order_details))
    sat_user_tier_bv = _materialize_table(build_sat_user_tier_bv(hub_users, lnk_user_order, sat_order_details))
    br_order_fulfillment = _materialize_table(
        build_br_order_fulfillment(
            hub_users,
            hub_orders,
            hub_products,
            lnk_user_order,
            lnk_order_product,
            sat_order_details,
            sat_order_bv,
        )
    )

    assert (
        evaluate_dq_expectations(
            sat_order_bv, BUSINESS_VAULT_ORDER_EXPECTATIONS, spark=spark, engine=dq_engine
        ).pass_rate
        == 1.0
    )
    assert (
        evaluate_dq_expectations(
            sat_user_tier_bv, BUSINESS_VAULT_USER_TIER_EXPECTATIONS, spark=spark, engine=dq_engine
        ).pass_rate
        == 1.0
    )

    # 4. Kimball Dimensional DWH: 9 Conformed Dimensions & 3 Facts
    dim_date = _materialize_table(build_dim_date(spark, "2025-01-01", "2025-06-30"))
    dim_users = _materialize_table(build_dim_users(hub_users, sat_user_details, sat_user_tier_bv))
    dim_products = _materialize_table(build_dim_products(hub_products, sat_product_details))
    dim_geography = _materialize_table(build_dim_geography(sat_user_details))
    dim_currency = _materialize_table(build_dim_currency(sat_order_details))
    dim_order_status = _materialize_table(build_dim_order_status(sat_order_details, sat_order_bv))
    dim_device = _materialize_table(build_dim_device(sat_event_details))
    dim_event_type = _materialize_table(build_dim_event_type(sat_event_details))
    dim_product_category = _materialize_table(build_dim_product_category(sat_product_details))

    fct_orders = _materialize_table(
        build_fct_orders(
            br_order_fulfillment,
            dim_users,
            dim_products,
            dim_geography,
            dim_currency,
            dim_order_status,
            dim_product_category,
        )
    )
    fct_events = _materialize_table(
        build_fct_events(
            hub_events,
            lnk_user_event,
            sat_event_details,
            dim_users,
            dim_products,
            dim_geography,
            dim_event_type,
            dim_device,
            dim_product_category,
        )
    )
    fct_daily_sales = _materialize_table(build_fct_daily_sales_snapshot(fct_orders, dim_order_status))

    for dim_df, exp in (
        (dim_date, KIMBALL_DIM_DATE_EXPECTATIONS),
        (dim_users, KIMBALL_DIM_USERS_EXPECTATIONS),
        (dim_products, KIMBALL_DIM_PRODUCTS_EXPECTATIONS),
        (dim_geography, KIMBALL_DIM_GEOGRAPHY_EXPECTATIONS),
        (dim_currency, KIMBALL_DIM_CURRENCY_EXPECTATIONS),
        (dim_order_status, KIMBALL_DIM_ORDER_STATUS_EXPECTATIONS),
        (dim_device, KIMBALL_DIM_DEVICE_EXPECTATIONS),
        (dim_event_type, KIMBALL_DIM_EVENT_TYPE_EXPECTATIONS),
        (dim_product_category, KIMBALL_DIM_PRODUCT_CATEGORY_EXPECTATIONS),
        (fct_orders, KIMBALL_FACT_ORDERS_EXPECTATIONS),
        (fct_events, KIMBALL_FACT_EVENTS_EXPECTATIONS),
        (fct_daily_sales, KIMBALL_DAILY_SNAPSHOT_EXPECTATIONS),
    ):
        assert evaluate_dq_expectations(dim_df, exp, spark=spark, engine=dq_engine).pass_rate == 1.0

    # Verify strict Star Schema absence of degenerate dimensional columns on Facts
    assert "order_status" not in fct_orders.columns
    assert "currency_code" not in fct_orders.columns
    assert "event_type" not in fct_events.columns
    assert "device_type" not in fct_events.columns
    assert "category" not in fct_daily_sales.columns

    # Verify Kimball Star Schema Referential Integrity across all Fact -> Dimension FKs (0 orphans)
    for child, parent, key in (
        (fct_orders, dim_users, "sk_user"),
        (fct_orders, dim_products, "sk_product"),
        (fct_orders, dim_date, "sk_date"),
        (fct_orders, dim_order_status, "sk_order_status"),
        (fct_orders, dim_currency, "sk_currency"),
        (fct_orders, dim_geography, "sk_geography"),
        (fct_orders, dim_product_category, "sk_category"),
        (fct_events, dim_users, "sk_user"),
        (fct_events, dim_products, "sk_product"),
        (fct_events, dim_date, "sk_date"),
        (fct_events, dim_event_type, "sk_event_type"),
        (fct_events, dim_device, "sk_device"),
        (fct_events, dim_geography, "sk_geography"),
        (fct_events, dim_product_category, "sk_category"),
        (fct_daily_sales, dim_date, "sk_date"),
        (fct_daily_sales, dim_product_category, "sk_category"),
    ):
        assert check_referential_integrity(child, parent, key, spark=spark, engine=dq_engine) == 0

    # 5. Consumer Views for Data Analysts & ML Engineers
    vw_exec_kpis = _materialize_table(build_vw_executive_sales_kpis(fct_daily_sales, dim_date, dim_product_category))
    vw_cust_360 = _materialize_table(
        build_vw_customer_360(
            dim_users,
            dim_geography,
            fct_orders,
            dim_order_status,
            fct_events,
            dim_event_type,
            dim_date,
        )
    )
    vw_prod_perf = _materialize_table(
        build_vw_product_performance(
            dim_products,
            dim_product_category,
            fct_orders,
            fct_events,
            dim_event_type,
        )
    )
    vw_ml_churn = _materialize_table(
        build_vw_ml_customer_churn_features(
            dim_users,
            fct_orders,
            dim_order_status,
            fct_events,
            dim_event_type,
            dim_date,
        )
    )
    vw_ml_affinity = _materialize_table(
        build_vw_ml_product_affinity_features(
            dim_products,
            fct_orders,
            fct_events,
            dim_event_type,
        )
    )

    assert (
        evaluate_dq_expectations(vw_ml_churn, ML_CHURN_FEATURES_EXPECTATIONS, spark=spark, engine=dq_engine).pass_rate
        == 1.0
    )
    assert (
        evaluate_dq_expectations(
            vw_ml_affinity, ML_AFFINITY_FEATURES_EXPECTATIONS, spark=spark, engine=dq_engine
        ).pass_rate
        == 1.0
    )

    for df_obj in (
        pit_users,
        pit_products,
        sat_order_bv,
        sat_user_tier_bv,
        br_order_fulfillment,
        dim_date,
        dim_users,
        dim_products,
        dim_geography,
        dim_currency,
        dim_order_status,
        dim_device,
        dim_event_type,
        dim_product_category,
        fct_orders,
        fct_events,
        fct_daily_sales,
        vw_exec_kpis,
        vw_cust_360,
        vw_prod_perf,
        vw_ml_churn,
        vw_ml_affinity,
    ):
        assert df_obj.count() > 0
        _assert_all_columns_have_comments(df_obj)


def test_separate_data_vault_and_kimball_dwh_pipeline_registrations(spark: SparkSession) -> None:
    """Verify separate Data Vault (20 tables) and Kimball DWH (17 tables) Lakeflow pipeline files."""
    table_registry: dict[str, dict[str, Any]] = {}

    class DummyDP:
        """Mock pyspark.pipelines decorator provider."""

        @staticmethod
        def table(name: str, comment: str = "", cluster_by: list[str] | None = None) -> Any:
            def decorator(fn: Any) -> Any:
                table_registry[name] = {"type": "table", "comment": comment, "cluster_by": cluster_by, "fn": fn}
                return fn

            return decorator

        @staticmethod
        def materialized_view(name: str, comment: str = "", cluster_by: list[str] | None = None) -> Any:
            def decorator(fn: Any) -> Any:
                table_registry[name] = {"type": "mv", "comment": comment, "cluster_by": cluster_by, "fn": fn}
                return fn

            return decorator

        @staticmethod
        def expect_all_or_drop(rules: dict[str, str]) -> Any:
            def decorator(fn: Any) -> Any:
                fn._expectations = rules
                return fn

            return decorator

    dv_fns = register_data_vault_pipeline(DummyDP, spark, DEFAULT_CONFIG)
    assert len(dv_fns) == 20

    dwh_fns = register_kimball_dwh_pipeline(DummyDP, spark, DEFAULT_CONFIG)
    assert len(dwh_fns) == 17

    combined_fns = register_lakeflow_pipeline(DummyDP, spark, DEFAULT_CONFIG)
    assert len(combined_fns) == 37
    assert len(table_registry) == 37

    for tbl_name, meta in table_registry.items():
        assert len(meta["comment"].strip()) > 15, f"Pipeline table {tbl_name} must have a detailed comment"

    total_expected = sum(len(v) for v in EXPECTED_TABLES_BY_SCHEMA.values())
    assert total_expected == 37


def test_hashing_determinism_and_metadata_override(spark: SparkSession) -> None:
    """Test SHA-256 hash key case/whitespace normalization and custom column description overrides."""
    df1 = spark.createDataFrame([("  usr-100 ", "Alice")], ["user_id", "first_name"])
    df2 = spark.createDataFrame([("USR-100", "Alice")], ["user_id", "first_name"])

    hk1 = df1.select(sha256_hash_key("user_id").alias("hk")).collect()[0]["hk"]
    hk2 = df2.select(sha256_hash_key("user_id").alias("hk")).collect()[0]["hk"]
    assert hk1 == hk2
    assert len(hk1) == 64

    hd1 = df1.select(sha256_hashdiff("user_id", "first_name").alias("hd")).collect()[0]["hd"]
    hd2 = df1.select(sha256_hashdiff("first_name", "user_id").alias("hd")).collect()[0]["hd"]
    assert hd1 == hd2

    custom_df = attach_column_descriptions(df1, {"user_id": "Custom user identifier description"})
    _assert_all_columns_have_comments(custom_df)


def test_parallel_dq_validation_registry_and_thread_context(spark: SparkSession) -> None:
    """Verify DQX suites registry, RI pairs completeness, and thread-local Spark scheduler context."""
    suites = build_dqx_suites("data_engineering_dev")
    assert len(suites) == 20
    for fqn, rules in suites:
        assert fqn.startswith("data_engineering_dev.")
        assert len(rules) > 0

    ri_pairs = build_referential_integrity_pairs("data_engineering_dev")
    assert len(ri_pairs) == 20
    for child, parent, key in ri_pairs:
        assert child.startswith("data_engineering_dev.")
        assert parent.startswith("data_engineering_dev.")
        assert key in (
            "hk_user",
            "hk_order",
            "hk_product",
            "sk_user",
            "sk_product",
            "sk_date",
            "sk_order_status",
            "sk_currency",
            "sk_geography",
            "sk_category",
            "sk_event_type",
            "sk_device",
        )

    _set_spark_thread_context(spark, "Test Job Description", pool="test_fair_pool")

    suite_specs = build_dqx_suite_specs("data_engineering_dev")
    assert len(suite_specs) == 20
    assert format_dqx_suite_label(suite_specs[0]) == "data_engineering_dev.bronze.raw_users"

    ri_specs = build_referential_integrity_specs("data_engineering_dev")
    assert len(ri_specs) == 20
    assert "->" in format_ri_spec_label(ri_specs[0])

    cfg = LakehouseConfig(catalog="test_cat")
    assert cfg.table_fqn("gold", "dim_users") == "test_cat.gold.dim_users"
    assert cfg.quoted_table_fqn("gold", "dim_users") == "`test_cat`.`gold`.`dim_users`"
    assert len(cfg.schema_table_fqns("bronze")) == 4
    assert len(cfg.all_expected_table_fqns) == 37
    assert len(cfg.all_expected_table_names) == 37
    assert "'dim_users'" in cfg.expected_tables_sql_in_clause

    dispatched = _dispatch_concurrent_tasks(
        items=["a", "b", "c"],
        worker_fn=lambda x: len(x) * 10,
        label_fn=str,
        max_workers=2,
    )
    assert dispatched == {"a": 10, "b": 10, "c": 10}

    assert select_preferred_warehouse_id([{"id": "w1", "state": "STOPPED"}, {"id": "w2", "state": "RUNNING"}]) == "w2"
    assert select_preferred_warehouse_id([{"id": "w1", "state": "STOPPED"}]) == "w1"
    assert select_preferred_warehouse_id([], fallback_id="fb_wh") == "fb_wh"

    assert "information_schema.columns" in build_missing_column_comments_query(cfg)
    assert "information_schema.tables" in build_table_comment_coverage_query(cfg)
    assert "information_schema.columns" in build_column_comment_coverage_query(cfg)

    assert find_space_id_by_title([{"title": "My Space", "space_id": "sp_1"}], "My Space") == "sp_1"
    assert find_space_id_by_title([{"title": "Other", "space_id": "sp_2"}], "My Space") is None

    space_payload = build_genie_space_create_payload("wh_123", config=cfg, title="Test Space")
    assert space_payload["warehouse_id"] == "wh_123"
    assert space_payload["title"] == "Test Space"
    assert "test_cat.gold.dim_users" in space_payload["serialized_space"]

    att_lines = format_genie_attachments([{"query": {"query": "SELECT 1"}}, {"text": {"content": "Done"}}])
    assert len(att_lines) == 2

    mock_mcp_res = MagicMock()
    mock_item = MagicMock()
    mock_item.text = '{"conversation_id": "c1", "response_id": "r1"}'
    mock_mcp_res.content = [mock_item]
    mock_mcp_res.structured_content = None
    assert extract_mcp_text(mock_mcp_res) == '{"conversation_id": "c1", "response_id": "r1"}'
    assert parse_mcp_structured_content(mock_mcp_res) == {"conversation_id": "c1", "response_id": "r1"}
