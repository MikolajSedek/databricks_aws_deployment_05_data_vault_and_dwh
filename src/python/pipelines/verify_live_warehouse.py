"""CLI utility to query live Unity Catalog row counts, metadata comments, Databricks Genie Spaces, and Genie One MCP."""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable
from functools import partial
from typing import Any, Final, cast

from databricks.sdk import WorkspaceClient

from src.python.modules.io.genie_client import (
    DEFAULT_MCP_QUESTION,
    async_ask_genie_one_mcp,
    ensure_genie_space_id,
    extract_mcp_text,
    poll_genie_conversation,
    run_databricks_api,
)
from src.python.modules.utils.config import DEFAULT_CONFIG, EXPECTED_TABLES_BY_SCHEMA, LakehouseConfig
from src.python.modules.utils.metadata import build_alter_column_comment_sql

DEFAULT_WAREHOUSE_ID: Final[str] = "1ddbe98f7fb6ca9d"
SqlRunnerFn = Callable[[str], list[list[Any]]]


def select_preferred_warehouse_id(
    warehouses: list[Any],
    fallback_id: str = DEFAULT_WAREHOUSE_ID,
) -> str:
    """Pure function selecting the first RUNNING SQL Warehouse ID, or the first available, or `fallback_id`."""
    for wh in warehouses:
        state = getattr(wh, "state", None) or (wh.get("state") if isinstance(wh, dict) else None)
        wh_id = getattr(wh, "id", None) or (wh.get("id") if isinstance(wh, dict) else None)
        state_val = getattr(state, "value", state)
        state_str = str(state_val) if state_val is not None else "UNKNOWN"
        if state_str == "RUNNING" and wh_id:
            return str(wh_id)
    if warehouses:
        first = warehouses[0]
        wh_id = getattr(first, "id", None) or (first.get("id") if isinstance(first, dict) else None)
        if wh_id:
            return str(wh_id)
    return fallback_id


def resolve_warehouse_id(profile: str = "dev", client: WorkspaceClient | None = None) -> str:
    """Resolve an active SQL Warehouse ID using databricks-sdk WorkspaceClient, falling back to DEFAULT_WAREHOUSE_ID."""
    try:
        ws_client = client or WorkspaceClient(profile=profile)
        warehouses: list[Any] = list(ws_client.warehouses.list())
        return select_preferred_warehouse_id(warehouses)
    except Exception:
        return DEFAULT_WAREHOUSE_ID


def execute_warehouse_sql(
    statement: str,
    warehouse_id: str | None = None,
    profile: str = "dev",
    client: WorkspaceClient | None = None,
) -> list[list[Any]]:
    """Execute a SQL statement on the Databricks SQL Warehouse using databricks-sdk and return rows."""
    ws_client = client or WorkspaceClient(profile=profile)
    target_wh = warehouse_id or resolve_warehouse_id(profile=profile, client=ws_client)
    try:
        resp = ws_client.statement_execution.execute_statement(
            warehouse_id=target_wh,
            statement=statement,
            wait_timeout="50s",
        )
        state_obj = getattr(resp.status, "state", None) if resp.status else None
        state_val = getattr(state_obj, "value", state_obj)
        state_str = str(state_val) if state_val is not None else "UNKNOWN"
        if state_str != "SUCCEEDED":
            error_msg = getattr(resp.status, "error", None) if resp.status else None
            raise RuntimeError(f"SQL execution failed ({state_str}): {error_msg}")
        if resp.result and resp.result.data_array:
            return cast(list[list[Any]], resp.result.data_array)
        return []
    except Exception as exc:
        if "SQL execution failed" in str(exc):
            raise
        # Fallback to api_client execution if statement_execution threw transport exception
        payload = {"warehouse_id": target_wh, "statement": statement, "wait_timeout": "50s"}
        resp_dict = run_databricks_api("post", "/api/2.0/sql/statements", payload, profile=profile, client=ws_client)
        state = resp_dict.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise RuntimeError(f"SQL execution failed ({state}): {json.dumps(resp_dict)}") from exc
        return cast(list[list[Any]], resp_dict.get("result", {}).get("data_array", []))


def make_warehouse_sql_runner(
    warehouse_id: str,
    profile: str = "dev",
    client: WorkspaceClient | None = None,
) -> SqlRunnerFn:
    """Return a partially-applied SQL execution function `str -> list[list[Any]]` bound to `warehouse_id`."""
    return partial(execute_warehouse_sql, warehouse_id=warehouse_id, profile=profile, client=client)


def build_missing_column_comments_query(config: LakehouseConfig = DEFAULT_CONFIG) -> str:
    """Pure function returning the SQL query to discover undocumented columns across Bronze, Silver, and Gold."""
    return f"""
    SELECT table_schema, table_name, column_name
    FROM {config.catalog}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND table_name IN ({config.expected_tables_sql_in_clause})
      AND (comment IS NULL OR LENGTH(TRIM(comment)) = 0)
    """


def build_table_comment_coverage_query(config: LakehouseConfig = DEFAULT_CONFIG) -> str:
    """Pure function returning the SQL query to compute table comment coverage in `information_schema.tables`."""
    return f"""
    SELECT
        COUNT(DISTINCT table_name) AS documented_tables,
        SUM(CASE WHEN comment IS NOT NULL AND LENGTH(TRIM(comment)) > 0 THEN 1 ELSE 0 END) AS tables_with_comments
    FROM {config.catalog}.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND table_name IN ({config.expected_tables_sql_in_clause})
    """


def build_column_comment_coverage_query(config: LakehouseConfig = DEFAULT_CONFIG) -> str:
    """Pure function returning the SQL query to compute column comment coverage in `information_schema.columns`."""
    return f"""
    SELECT
        COUNT(*) AS total_columns,
        SUM(CASE WHEN comment IS NOT NULL AND LENGTH(TRIM(comment)) > 0 THEN 1 ELSE 0 END) AS columns_with_comments
    FROM {config.catalog}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND table_name IN ({config.expected_tables_sql_in_clause})
    """


def verify_live_table_counts(
    sql_runner: SqlRunnerFn,
    warehouse_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
) -> dict[str, int]:
    """Query live row counts for all 37 expected tables and exit if any table is empty."""
    expected_total = len(config.all_expected_table_names)
    print(f"=== VERIFYING NON-EMPTY ROW COUNTS ACROSS ALL {expected_total} TABLES (Warehouse: {warehouse_id}) ===")
    counts: dict[str, int] = {}
    for schema_name, tables in EXPECTED_TABLES_BY_SCHEMA.items():
        for table_name in tables:
            fqn = config.quoted_table_fqn(schema_name, table_name)
            row_cnt = int(sql_runner(f"SELECT COUNT(*) FROM {fqn}")[0][0])
            print(f"  [LIVE] {schema_name}.{table_name}: {row_cnt} rows")
            if row_cnt <= 0:
                sys.exit(f"ERROR: Table {config.table_fqn(schema_name, table_name)} is empty!")
            counts[config.table_fqn(schema_name, table_name)] = row_cnt
    return counts


def backfill_missing_column_comments(sql_runner: SqlRunnerFn, config: LakehouseConfig = DEFAULT_CONFIG) -> int:
    """Find any columns missing Unity Catalog comments and execute `ALTER COLUMN ... COMMENT` DDLs."""
    missing_rows = sql_runner(build_missing_column_comments_query(config))
    for schema_name, table_name, col_name in missing_rows:
        sql_runner(build_alter_column_comment_sql(config.catalog, schema_name, table_name, col_name))
    return len(missing_rows)


def verify_live_metadata_coverage(sql_runner: SqlRunnerFn, config: LakehouseConfig = DEFAULT_CONFIG) -> None:
    """Backfill any missing column comments and assert 100% table and column metadata coverage."""
    expected_total = len(config.all_expected_table_names)
    backfill_missing_column_comments(sql_runner, config)

    meta_row = sql_runner(build_table_comment_coverage_query(config))[0]
    total_tbls, commented_tbls = int(meta_row[0]), int(meta_row[1])
    print(f"=== UNITY CATALOG TABLE METADATA COVERAGE ({expected_total} PROJECT TABLES) ====")
    print(f"  Project Tables: {total_tbls}, Tables with Comments: {commented_tbls}")

    col_row = sql_runner(build_column_comment_coverage_query(config))[0]
    total_cols, commented_cols = int(col_row[0]), int(col_row[1])
    print(f"=== UNITY CATALOG COLUMN METADATA COVERAGE ({expected_total} PROJECT TABLES) ===")
    print(f"  Total Columns: {total_cols}, Columns with Comments: {commented_cols}")

    if total_tbls != expected_total or commented_tbls != expected_total:
        sys.exit(f"ERROR: Not all {expected_total} tables have descriptions!")
    if total_cols != commented_cols:
        sys.exit(f"ERROR: Not all columns in the {expected_total} tables have descriptions!")


def _extract_mcp_text(result: Any) -> str:
    """Extract concatenated text content blocks from an MCP CallToolResult."""
    return extract_mcp_text(result)


async def _async_query_genie_one_mcp(question: str) -> str:
    """Query Databricks Managed Genie One MCP server (`system.ai.genie_one_mcp`) via Streamable HTTP."""
    return await async_ask_genie_one_mcp(question)


def query_genie_one_mcp(question: str = DEFAULT_MCP_QUESTION) -> str:
    """Synchronous wrapper invoking Databricks Genie One MCP (`system.ai.genie_one_mcp`)."""
    return asyncio.run(_async_query_genie_one_mcp(question))


def ensure_and_query_genie_space(warehouse_id: str | None = None, profile: str = "dev") -> None:
    """Create or locate a Databricks Genie Space over all 17 Gold tables/views and validate via Genie API."""
    ws_client = WorkspaceClient(profile=profile)
    target_wh = warehouse_id or resolve_warehouse_id(profile=profile, client=ws_client)
    space_id = ensure_genie_space_id(target_wh, config=DEFAULT_CONFIG, profile=profile, client=ws_client)
    poll_genie_conversation(space_id, profile=profile, client=ws_client)


def verify_all_live_tables_and_genie() -> None:
    """Query SQL Warehouse, Genie Space API, and Genie One MCP to verify all 37 tables, comments, and KPIs."""
    ws_client = WorkspaceClient(profile="dev")
    warehouse_id = resolve_warehouse_id(client=ws_client)
    sql_runner = make_warehouse_sql_runner(warehouse_id, client=ws_client)

    verify_live_table_counts(sql_runner, warehouse_id, DEFAULT_CONFIG)
    verify_live_metadata_coverage(sql_runner, DEFAULT_CONFIG)
    ensure_and_query_genie_space(warehouse_id=warehouse_id)
    query_genie_one_mcp(DEFAULT_MCP_QUESTION)


if __name__ == "__main__":
    verify_all_live_tables_and_genie()
