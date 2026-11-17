"""Immutable domain configurations, column constants, and schema descriptions."""

from dataclasses import dataclass
from typing import Final

# Canonical Lakehouse table/view inventory (37 tables across Bronze, Silver, Gold)
EXPECTED_TABLES_BY_SCHEMA: Final[dict[str, list[str]]] = {
    "bronze": [
        "raw_users",
        "raw_products",
        "raw_orders",
        "raw_events",
    ],
    "silver": [
        "hub_users",
        "hub_products",
        "hub_orders",
        "hub_events",
        "lnk_user_order",
        "lnk_order_product",
        "lnk_user_event",
        "sat_user_details",
        "sat_product_details",
        "sat_order_details",
        "sat_event_details",
        "pit_users",
        "pit_products",
        "sat_order_bv",
        "sat_user_tier_bv",
        "br_order_fulfillment",
    ],
    "gold": [
        "dim_date",
        "dim_users",
        "dim_products",
        "dim_geography",
        "dim_currency",
        "dim_order_status",
        "dim_device",
        "dim_event_type",
        "dim_product_category",
        "fct_orders",
        "fct_events",
        "fct_daily_sales_snapshot",
        "vw_executive_sales_kpis",
        "vw_customer_360",
        "vw_product_performance",
        "vw_ml_customer_churn_features",
        "vw_ml_product_affinity_features",
    ],
}


@dataclass(frozen=True)
class LakehouseConfig:
    """Immutable configuration for catalogs, schemas, and managed volume paths."""

    catalog: str = "data_engineering_dev"
    raw_ingestion_schema: str = "raw_ingestion"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    volume_name: str = "raw_events_landing"
    record_source: str = "ECOMMERCE_EVENTS_PARQUET_VOLUME"

    @property
    def volume_base_path(self) -> str:
        """Return the Unity Catalog Managed Volume root path."""
        return f"/Volumes/{self.catalog}/{self.raw_ingestion_schema}/{self.volume_name}"

    def dataset_volume_path(self, dataset_name: str) -> str:
        """Return the landing directory path for a specific parquet event dataset."""
        return f"{self.volume_base_path}/parquet_events/{dataset_name}"

    def table_fqn(self, schema_name: str, table_name: str) -> str:
        """Return the dot-separated fully-qualified table name (`catalog.schema.table`)."""
        return f"{self.catalog}.{schema_name}.{table_name}"

    def quoted_table_fqn(self, schema_name: str, table_name: str) -> str:
        """Return the backtick-quoted fully-qualified table name (`` `catalog`.`schema`.`table` ``)."""
        return f"`{self.catalog}`.`{schema_name}`.`{table_name}`"

    def schema_table_fqns(self, schema_name: str) -> tuple[str, ...]:
        """Return all dot-separated fully-qualified table names for a given schema."""
        return tuple(self.table_fqn(schema_name, tbl) for tbl in EXPECTED_TABLES_BY_SCHEMA.get(schema_name, []))

    @property
    def all_expected_table_fqns(self) -> tuple[str, ...]:
        """Return all 37 dot-separated fully-qualified table names across Bronze, Silver, and Gold."""
        return tuple(self.table_fqn(schema, tbl) for schema, tbls in EXPECTED_TABLES_BY_SCHEMA.items() for tbl in tbls)

    @property
    def all_expected_table_names(self) -> tuple[str, ...]:
        """Return all 37 unqualified table/view names across Bronze, Silver, and Gold."""
        return tuple(tbl for tbls in EXPECTED_TABLES_BY_SCHEMA.values() for tbl in tbls)

    @property
    def expected_tables_sql_in_clause(self) -> str:
        """Return a comma-separated SQL string literal list of all 37 expected table names."""
        return ", ".join(f"'{tbl}'" for tbl in self.all_expected_table_names)


DEFAULT_CONFIG: Final[LakehouseConfig] = LakehouseConfig()

# Metadata audit columns
COL_LDTS: Final[str] = "_ldts"
COL_RSRC: Final[str] = "_rsrc"
COL_SOURCE_FILE: Final[str] = "_source_file"
COL_HASHDIFF: Final[str] = "hashdiff"

# Business keys
COL_USER_ID: Final[str] = "user_id"
COL_PRODUCT_ID: Final[str] = "product_id"
COL_ORDER_ID: Final[str] = "order_id"
COL_EVENT_ID: Final[str] = "event_id"

# Hash keys
COL_HK_USER: Final[str] = "hk_user"
COL_HK_PRODUCT: Final[str] = "hk_product"
COL_HK_ORDER: Final[str] = "hk_order"
COL_HK_EVENT: Final[str] = "hk_event"
COL_HK_LNK_USER_ORDER: Final[str] = "hk_lnk_user_order"
COL_HK_LNK_ORDER_PRODUCT: Final[str] = "hk_lnk_order_product"
COL_HK_LNK_USER_EVENT: Final[str] = "hk_lnk_user_event"

# Catalog and Schema Descriptions for Unity Catalog Governance
CATALOG_DESCRIPTIONS: Final[dict[str, str]] = {
    "data_engineering_dev": (
        "Enterprise Data Engineering Lakehouse Catalog hosting Raw Ingestion Volumes, "
        "Bronze Streaming Tables, Data Vault 2.0 (Raw & Business Vault), Kimball Dimensional DWH, "
        "and governed Analytical & ML Feature Views."
    ),
}

SCHEMA_DESCRIPTIONS: Final[dict[str, str]] = {
    "raw_ingestion": "Landing schema containing Unity Catalog Managed Volumes for raw Parquet event ingestion.",
    "bronze": "Raw Bronze layer containing Auto Loader streaming tables ingested from Managed Volumes with audit metadata.",
    "silver": "Enterprise Data Vault 2.0 layer containing Raw Vault (Hubs, Links, Satellites) and Business Vault (PIT, Bridge, BV Satellites).",
    "gold": "Kimball Dimensional Data Warehouse layer (Conformed Dimensions, Fact Tables) and governed Consumer Views for Data Analysts and ML Engineers.",
}
