"""Deterministic synthetic e-commerce event generation and concurrent Parquet volume writer."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.python.modules.utils.config import LakehouseConfig
from src.python.modules.utils.metadata import COLUMN_DESCRIPTIONS, attach_column_descriptions


def _field(name: str, data_type: T.DataType, nullable: bool) -> T.StructField:
    """Create a StructField with explicit nullability and Unity Catalog comment metadata."""
    return T.StructField(
        name=name,
        dataType=data_type,
        nullable=nullable,
        metadata={"comment": COLUMN_DESCRIPTIONS.get(name, f"Governed field {name}.")},
    )


# Explicit StructType contracts with deliberate nullability (PKs/FKs/measures nullable=False, optional attributes nullable=True)
USERS_SCHEMA: Final[T.StructType] = T.StructType(
    [
        _field("user_id", T.StringType(), nullable=False),
        _field("email", T.StringType(), nullable=False),
        _field("first_name", T.StringType(), nullable=True),
        _field("last_name", T.StringType(), nullable=True),
        _field("country_code", T.StringType(), nullable=False),
        _field("loyalty_tier", T.StringType(), nullable=False),
        _field("signup_date", T.DateType(), nullable=False),
        _field("updated_at", T.TimestampType(), nullable=False),
    ]
)

PRODUCTS_SCHEMA: Final[T.StructType] = T.StructType(
    [
        _field("product_id", T.StringType(), nullable=False),
        _field("product_name", T.StringType(), nullable=False),
        _field("category", T.StringType(), nullable=False),
        _field("subcategory", T.StringType(), nullable=True),
        _field("unit_price", T.DoubleType(), nullable=False),
        _field("cost_price", T.DoubleType(), nullable=False),
        _field("is_active", T.BooleanType(), nullable=False),
        _field("updated_at", T.TimestampType(), nullable=False),
    ]
)

ORDERS_SCHEMA: Final[T.StructType] = T.StructType(
    [
        _field("order_id", T.StringType(), nullable=False),
        _field("user_id", T.StringType(), nullable=False),
        _field("product_id", T.StringType(), nullable=False),
        _field("order_status", T.StringType(), nullable=False),
        _field("quantity", T.IntegerType(), nullable=False),
        _field("gross_amount", T.DoubleType(), nullable=False),
        _field("discount_amount", T.DoubleType(), nullable=True),
        _field("tax_amount", T.DoubleType(), nullable=True),
        _field("currency_code", T.StringType(), nullable=False),
        _field("order_timestamp", T.TimestampType(), nullable=False),
    ]
)

EVENTS_SCHEMA: Final[T.StructType] = T.StructType(
    [
        _field("event_id", T.StringType(), nullable=False),
        _field("user_id", T.StringType(), nullable=False),
        _field("product_id", T.StringType(), nullable=False),
        _field("event_type", T.StringType(), nullable=False),
        _field("session_id", T.StringType(), nullable=False),
        _field("device_type", T.StringType(), nullable=True),
        _field("page_url", T.StringType(), nullable=True),
        _field("duration_seconds", T.IntegerType(), nullable=False),
        _field("event_timestamp", T.TimestampType(), nullable=False),
    ]
)


def _extract_max_numeric_id(spark: SparkSession, path: str, id_col: str) -> int:
    """Inspect existing Parquet files in a Volume path and return the highest integer suffix for `id_col`."""
    try:
        existing_df = spark.read.parquet(path)
        max_row = (
            existing_df.select(F.regexp_extract(F.col(id_col), r"(\d+)$", 1).cast("int").alias("max_id"))
            .agg(F.max("max_id").alias("max_id"))
            .collect()
        )
        if max_row and max_row[0]["max_id"] is not None:
            return int(max_row[0]["max_id"])
    except Exception:  # nosec B110
        pass
    return 0


def resolve_next_batch_offsets(
    spark: SparkSession,
    config: LakehouseConfig,
) -> dict[str, int]:
    """Determine the starting 1-based record offsets for incremental Parquet batch generation.

    Inspects existing Parquet files in the Managed Volume for each entity domain so every workflow
    execution appends a fresh, non-overlapping batch of business keys and transactions.

    Args:
        spark: Active SparkSession.
        config: Immutable LakehouseConfig instance.

    Returns:
        Dictionary mapping dataset name ('users', 'products', 'orders', 'events') to its next `start_idx`.
    """
    id_columns = {
        "users": "user_id",
        "products": "product_id",
        "orders": "order_id",
        "events": "event_id",
    }
    return {
        ds_name: _extract_max_numeric_id(spark, config.dataset_volume_path(ds_name), id_col) + 1
        for ds_name, id_col in id_columns.items()
    }


def generate_users_records(
    num_users: int = 120,
    start_idx: int = 1,
) -> list[tuple[object, ...]]:
    """Generate deterministic synthetic user profile records starting at `start_idx`.

    Args:
        num_users: Number of distinct users to generate in this batch.
        start_idx: 1-based starting user index for incremental batch generation.

    Returns:
        List of user record tuples matching USERS_SCHEMA.
    """
    countries = ("US", "GB", "DE", "PL", "FR", "CA", "NL", "SE")
    tiers = ("BRONZE", "SILVER", "GOLD", "PLATINUM")
    first_names = ("Alice", "Bob", "Carlos", "Diana", "Elena", "Filip", "Grace", "Henrik", "Ines", "Jakub")
    last_names = ("Smith", "Kowalski", "Mueller", "Dubois", "Lindqvist", "Novak", "Silva", "Chen", "Patel", "Garcia")
    base_time = datetime(2025, 1, 1, 8, 0, 0)

    records: list[tuple[object, ...]] = []
    for idx in range(start_idx, start_idx + num_users):
        user_id = f"USR-{idx:05d}"
        fname = first_names[idx % len(first_names)]
        lname = last_names[(idx // 3) % len(last_names)]
        email = f"{fname.lower()}.{lname.lower()}.{idx}@example-lakehouse.com"
        country = countries[idx % len(countries)]
        tier = tiers[idx % len(tiers)]
        signup_dt = (base_time + timedelta(days=idx % 180)).date()
        updated_ts = base_time + timedelta(days=idx % 180, hours=idx % 24)
        records.append((user_id, email, fname, lname, country, tier, signup_dt, updated_ts))
    return records


def generate_products_records(
    num_products: int = 50,
    start_idx: int = 1,
) -> list[tuple[object, ...]]:
    """Generate deterministic synthetic product catalog records starting at `start_idx`.

    Args:
        num_products: Number of distinct products to generate in this batch.
        start_idx: 1-based starting product index for incremental batch generation.

    Returns:
        List of product record tuples matching PRODUCTS_SCHEMA.
    """
    catalog_hierarchy = (
        ("ELECTRONICS", "AUDIO", 149.99, 75.00),
        ("ELECTRONICS", "COMPUTERS", 899.99, 540.00),
        ("APPAREL", "OUTERWEAR", 129.50, 48.00),
        ("HOME", "KITCHEN", 79.99, 32.00),
        ("SPORTS", "FITNESS", 59.99, 22.50),
        ("BOOKS", "TECHNICAL", 44.99, 12.00),
    )
    base_time = datetime(2025, 1, 1, 6, 0, 0)

    records: list[tuple[object, ...]] = []
    for idx in range(start_idx, start_idx + num_products):
        product_id = f"PRD-{idx:04d}"
        cat, subcat, base_price, base_cost = catalog_hierarchy[idx % len(catalog_hierarchy)]
        price_multiplier = 1.0 + ((idx % 7) * 0.08)
        unit_price = round(base_price * price_multiplier, 2)
        cost_price = round(base_cost * price_multiplier, 2)
        product_name = f"Lakehouse {subcat.title()} Pro Model {idx}"
        is_active = (idx % 15) != 0
        updated_ts = base_time + timedelta(days=idx % 60, hours=idx % 12)
        records.append((product_id, product_name, cat, subcat, unit_price, cost_price, is_active, updated_ts))
    return records


def generate_orders_records(
    num_orders: int = 400,
    num_users: int = 120,
    num_products: int = 50,
    start_idx: int = 1,
) -> list[tuple[object, ...]]:
    """Generate deterministic synthetic order transaction records referencing valid users and products.

    Args:
        num_orders: Number of order records to generate in this batch.
        num_users: Total number of valid users in the domain (`1..num_users`).
        num_products: Total number of valid products in the domain (`1..num_products`).
        start_idx: 1-based starting order index for incremental batch generation.

    Returns:
        List of order record tuples matching ORDERS_SCHEMA.
    """
    statuses = ("COMPLETED", "SHIPPED", "COMPLETED", "PROCESSING", "COMPLETED", "CANCELLED", "REFUNDED")
    currencies = ("USD", "USD", "USD", "EUR", "GBP")
    base_time = datetime(2025, 2, 1, 10, 0, 0)

    records: list[tuple[object, ...]] = []
    for idx in range(start_idx, start_idx + num_orders):
        order_id = f"ORD-{idx:06d}"
        user_id = f"USR-{((idx * 7) % num_users) + 1:05d}"
        product_id = f"PRD-{((idx * 3) % num_products) + 1:04d}"
        status = statuses[idx % len(statuses)]
        quantity = (idx % 4) + 1
        unit_base = 35.0 + ((idx % 12) * 25.0)
        gross_amount = round(quantity * unit_base, 2)
        discount_amount = round(gross_amount * (0.10 if (idx % 3 == 0) else 0.0), 2)
        tax_amount = round((gross_amount - discount_amount) * 0.08, 2)
        currency = currencies[idx % len(currencies)]
        order_ts = base_time + timedelta(days=idx % 90, hours=idx % 24, minutes=(idx * 5) % 60)
        records.append(
            (
                order_id,
                user_id,
                product_id,
                status,
                quantity,
                gross_amount,
                discount_amount,
                tax_amount,
                currency,
                order_ts,
            )
        )
    return records


def generate_events_records(
    num_events: int = 800,
    num_users: int = 120,
    num_products: int = 50,
    start_idx: int = 1,
) -> list[tuple[object, ...]]:
    """Generate deterministic synthetic clickstream telemetry event records starting at `start_idx`.

    Args:
        num_events: Number of clickstream events to generate in this batch.
        num_users: Total number of valid users in the domain (`1..num_users`).
        num_products: Total number of valid products in the domain (`1..num_products`).
        start_idx: 1-based starting event index for incremental batch generation.

    Returns:
        List of clickstream event tuples matching EVENTS_SCHEMA.
    """
    event_types = ("PAGE_VIEW", "PRODUCT_SEARCH", "PAGE_VIEW", "ADD_TO_CART", "CHECKOUT_START", "PURCHASE")
    devices = ("DESKTOP", "MOBILE", "MOBILE", "TABLET")
    base_time = datetime(2025, 2, 1, 9, 0, 0)

    records: list[tuple[object, ...]] = []
    for idx in range(start_idx, start_idx + num_events):
        event_id = f"EVT-{idx:07d}"
        user_idx = ((idx * 5) % num_users) + 1
        prod_idx = ((idx * 11) % num_products) + 1
        user_id = f"USR-{user_idx:05d}"
        product_id = f"PRD-{prod_idx:04d}"
        evt_type = event_types[idx % len(event_types)]
        session_id = f"SES-{user_idx:05d}-{(idx % 20) + 1:03d}"
        device = devices[idx % len(devices)]
        page_url = f"/store/catalog/{product_id.lower()}?action={evt_type.lower()}"
        duration_sec = 5 + ((idx * 13) % 240)
        evt_ts = base_time + timedelta(days=idx % 90, hours=idx % 24, minutes=(idx * 3) % 60, seconds=idx % 60)
        records.append(
            (
                event_id,
                user_id,
                product_id,
                evt_type,
                session_id,
                device,
                page_url,
                duration_sec,
                evt_ts,
            )
        )
    return records


def build_sample_event_dataframes(
    spark: SparkSession,
    num_users: int = 120,
    num_products: int = 50,
    num_orders: int = 400,
    num_events: int = 800,
    user_start_idx: int = 1,
    product_start_idx: int = 1,
    order_start_idx: int = 1,
    event_start_idx: int = 1,
) -> dict[str, DataFrame]:
    """Create governed PySpark DataFrames for all four raw event domains with explicit nullability and comments.

    Args:
        spark: Active SparkSession.
        num_users: Count of new users to generate in this batch.
        num_products: Count of new products to generate in this batch.
        num_orders: Count of new orders to generate in this batch.
        num_events: Count of new clickstream events to generate in this batch.
        user_start_idx: 1-based starting index for user IDs.
        product_start_idx: 1-based starting index for product IDs.
        order_start_idx: 1-based starting index for order IDs.
        event_start_idx: 1-based starting index for event IDs.

    Returns:
        Dictionary mapping dataset name ('users', 'products', 'orders', 'events') to its DataFrame.
    """
    total_valid_users = (user_start_idx - 1) + num_users
    total_valid_products = (product_start_idx - 1) + num_products

    users_df = spark.createDataFrame(
        generate_users_records(num_users, start_idx=user_start_idx),
        schema=USERS_SCHEMA,
    ).transform(attach_column_descriptions)
    products_df = spark.createDataFrame(
        generate_products_records(num_products, start_idx=product_start_idx),
        schema=PRODUCTS_SCHEMA,
    ).transform(attach_column_descriptions)
    orders_df = spark.createDataFrame(
        generate_orders_records(
            num_orders,
            num_users=total_valid_users,
            num_products=total_valid_products,
            start_idx=order_start_idx,
        ),
        schema=ORDERS_SCHEMA,
    ).transform(attach_column_descriptions)
    events_df = spark.createDataFrame(
        generate_events_records(
            num_events,
            num_users=total_valid_users,
            num_products=total_valid_products,
            start_idx=event_start_idx,
        ),
        schema=EVENTS_SCHEMA,
    ).transform(attach_column_descriptions)
    return {
        "users": users_df,
        "products": products_df,
        "orders": orders_df,
        "events": events_df,
    }


def write_parquet_events_to_volume(
    spark: SparkSession,
    config: LakehouseConfig,
    num_users: int = 120,
    num_products: int = 50,
    num_orders: int = 400,
    num_events: int = 800,
    max_workers: int = 4,
    incremental: bool = True,
) -> dict[str, str]:
    """Generate and append new e-commerce event Parquet files concurrently into the Managed Volume.

    When `incremental=True` (default), inspects existing Parquet files in the Volume to compute the next
    non-overlapping starting IDs (`start_idx`) and writes with `.mode("append")` so Auto Loader (`cloudFiles`)
    discovers and ingests only the newly added Parquet files on each pipeline run.

    Args:
        spark: Active SparkSession.
        config: Immutable LakehouseConfig defining target volume paths.
        num_users: Count of new users to generate in this batch.
        num_products: Count of new products to generate in this batch.
        num_orders: Count of new orders to generate in this batch.
        num_events: Count of new events to generate in this batch.
        max_workers: Thread pool concurrency level for parallel Parquet writes.
        incremental: If True, append new records after the current max ID; otherwise overwrite from index 1.

    Returns:
        Dictionary mapping each dataset name to the target Volume path written.
    """
    offsets = (
        resolve_next_batch_offsets(spark, config)
        if incremental
        else {
            "users": 1,
            "products": 1,
            "orders": 1,
            "events": 1,
        }
    )
    write_mode = "append" if incremental else "overwrite"

    datasets = build_sample_event_dataframes(
        spark=spark,
        num_users=num_users,
        num_products=num_products,
        num_orders=num_orders,
        num_events=num_events,
        user_start_idx=offsets["users"],
        product_start_idx=offsets["products"],
        order_start_idx=offsets["orders"],
        event_start_idx=offsets["events"],
    )

    def _write_single_dataset(item: tuple[str, DataFrame]) -> tuple[str, str]:
        name, df = item
        target_path = config.dataset_volume_path(name)
        df.coalesce(1).write.mode(write_mode).parquet(target_path)
        return name, target_path

    written_paths: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for ds_name, ds_path in executor.map(_write_single_dataset, datasets.items()):
            written_paths[ds_name] = ds_path
    return written_paths
