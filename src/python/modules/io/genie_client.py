"""Pure functions and builders for Databricks Genie Spaces REST API and Genie One MCP (`system.ai.genie_one_mcp`)."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Final, cast

from databricks.sdk import WorkspaceClient
from mcp.client.session import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

from src.python.modules.utils.config import DEFAULT_CONFIG, EXPECTED_TABLES_BY_SCHEMA, LakehouseConfig

DEFAULT_GENIE_SPACE_TITLE: Final[str] = "Data Vault 2.0 & Kimball DWH Analytics Space"
DEFAULT_GENIE_PROMPT: Final[str] = (
    "Show the total number of orders and total net revenue in USD per category "
    "from data_engineering_dev.gold.vw_executive_sales_kpis ordered by net revenue descending."
)
DEFAULT_MCP_QUESTION: Final[str] = (
    "Using data_engineering_dev.gold.vw_executive_sales_kpis and fct_orders joined to dim_geography, "
    "summarize total orders and total net revenue in USD by category and region."
)


def run_databricks_api(
    method: str,
    endpoint: str,
    payload: dict[str, Any] | None = None,
    profile: str = "dev",
    client: WorkspaceClient | None = None,
) -> dict[str, Any]:
    """Invoke Databricks REST API natively via databricks-sdk WorkspaceClient api_client."""
    ws_client = client or WorkspaceClient(profile=profile)
    res = ws_client.api_client.do(
        method=method.upper(),
        path=endpoint,
        body=payload,
    )
    if isinstance(res, dict):
        return res
    if isinstance(res, list):
        return {"items": res}
    return {}


def extract_mcp_text(result: Any) -> str:
    """Pure function extracting concatenated text content blocks from an MCP CallToolResult."""
    parts = [str(getattr(item, "text", "")) for item in (getattr(result, "content", []) or [])]
    return "\n".join(p for p in parts if p)


def parse_mcp_structured_content(result: Any) -> dict[str, Any]:
    """Pure function extracting a structured JSON dictionary from an MCP CallToolResult."""
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict) and structured:
        return structured
    raw_text = extract_mcp_text(result).strip()
    if raw_text.startswith("{"):
        try:
            return cast(dict[str, Any], json.loads(raw_text))
        except json.JSONDecodeError:
            return {}
    return {}


def resolve_mcp_connection_params(profile: str = "dev") -> tuple[str, dict[str, str]]:
    """Resolve the Workspace Genie One MCP Streamable HTTP endpoint URL and auth headers."""
    client = WorkspaceClient(profile=profile)
    workspace_id = client.get_workspace_id()
    mcp_url = f"{client.config.host}/ai-gateway/mcp-services/system.ai.genie_one_mcp?o={workspace_id}"
    return mcp_url, client.config.authenticate()


async def _fetch_mcp_query_items(
    session: ClientSession,
    conv_id: str,
    resp_id: str,
    query_items: list[dict[str, Any]],
) -> None:
    """Fetch and print query item results attached to a completed Genie MCP response."""
    for qitem in query_items:
        item_id = qitem.get("item_id")
        if item_id:
            q_res = await session.call_tool(
                "genie_get_query_result",
                {"conversation_id": conv_id, "response_id": resp_id, "item_id": item_id},
            )
            q_data = getattr(q_res, "structured_content", None) or extract_mcp_text(q_res)
            print(f"  Genie One MCP Query Result ({item_id}): {q_data}")


async def _poll_mcp_until_complete(
    session: ClientSession,
    conv_id: str,
    resp_id: str,
    max_polls: int = 30,
    poll_interval_sec: int = 3,
) -> str:
    """Poll `genie_poll_response` until the conversation message finishes executing."""
    for _ in range(max_polls):
        poll_res = await session.call_tool(
            "genie_poll_response",
            {"conversation_id": conv_id, "response_id": resp_id},
        )
        poll_data = parse_mcp_structured_content(poll_res)
        poll_text = extract_mcp_text(poll_res)
        status = str(poll_data.get("status", "")).lower()
        if status and status != "in_progress":
            print(f"  Genie One MCP Status: {status}")
            final_answer = str(poll_data.get("final_answer") or poll_text)
            print(f"  Genie One MCP Answer:\n{final_answer}")
            await _fetch_mcp_query_items(session, conv_id, resp_id, poll_data.get("query_items", []) or [])
            return final_answer
        await asyncio.sleep(poll_interval_sec)

    raise RuntimeError("Genie One MCP polling timeout reached.")


async def async_ask_genie_one_mcp(question: str, profile: str = "dev") -> str:
    """Connect to Genie One MCP over Streamable HTTP, submit a question, and return the answer."""
    mcp_url, headers = resolve_mcp_connection_params(profile=profile)
    async with (
        create_mcp_http_client(headers=headers) as http_client,
        streamable_http_client(mcp_url, http_client=http_client) as (read_stream, write_stream, *_),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        tools_resp = await session.list_tools()
        print(f"\n=== DATABRICKS GENIE ONE MCP CONNECTED ({mcp_url}) ===")
        print(f"  Available MCP Tools: {[t.name for t in tools_resp.tools]}")

        ask_res = await session.call_tool("genie_ask", {"question": question})
        structured = parse_mcp_structured_content(ask_res)
        ask_text = extract_mcp_text(ask_res)
        conv_id = structured.get("conversation_id")
        resp_id = structured.get("response_id")
        if not conv_id or not resp_id:
            print(f"  Genie One MCP Direct Response: {ask_text}")
            return ask_text

        return await _poll_mcp_until_complete(session, str(conv_id), str(resp_id))


def find_space_id_by_title(spaces: list[Any], title: str) -> str | None:
    """Pure function finding a Genie Space ID matching the given title from a list of space objects or dicts."""
    for sp in spaces:
        sp_title = getattr(sp, "title", None) or (sp.get("title") if isinstance(sp, dict) else None)
        sp_id = getattr(sp, "space_id", None) or (sp.get("space_id") if isinstance(sp, dict) else None)
        if sp_title == title and sp_id:
            return str(sp_id)
    return None


def build_genie_space_create_payload(
    warehouse_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    title: str = DEFAULT_GENIE_SPACE_TITLE,
) -> dict[str, Any]:
    """Pure function constructing the JSON payload for creating a Gold-layer Databricks Genie Space."""
    gold_tables = [
        {"identifier": config.table_fqn(config.gold_schema, tbl)}
        for tbl in EXPECTED_TABLES_BY_SCHEMA[config.gold_schema]
    ]
    serialized_space = {
        "version": 1,
        "data_sources": {"tables": sorted(gold_tables, key=lambda x: x["identifier"])},
    }
    return {
        "warehouse_id": warehouse_id,
        "title": title,
        "description": (
            "Governed Genie Space for querying Kimball Star Schema conformed dimensions, "
            "facts, executive sales KPIs, and ML customer churn / product affinity features."
        ),
        "serialized_space": json.dumps(serialized_space),
    }


def ensure_genie_space_id(
    warehouse_id: str,
    config: LakehouseConfig = DEFAULT_CONFIG,
    title: str = DEFAULT_GENIE_SPACE_TITLE,
    profile: str = "dev",
    client: WorkspaceClient | None = None,
) -> str:
    """Locate an existing Genie Space by title or create a new one over all 17 Gold tables via databricks-sdk."""
    ws_client = client or WorkspaceClient(profile=profile)
    try:
        spaces_resp = ws_client.genie.list_spaces()
        spaces: list[Any] = list(getattr(spaces_resp, "spaces", None) or [])
    except Exception:
        spaces = []
    space_id = find_space_id_by_title(spaces, title)
    if not space_id:
        payload = build_genie_space_create_payload(warehouse_id, config=config, title=title)
        try:
            created = ws_client.genie.create_space(
                warehouse_id=warehouse_id,
                title=title,
                description=str(payload.get("description", "")),
                serialized_space=str(payload["serialized_space"]),
            )
            space_id = getattr(created, "space_id", None)
        except Exception:
            created_dict = run_databricks_api(
                "post", "/api/2.0/genie/spaces", payload, profile=profile, client=ws_client
            )
            space_id = created_dict.get("space_id")
    print(f"\n=== DATABRICKS GENIE SPACE (ID: {space_id}) ===")
    if not space_id:
        raise RuntimeError("Failed to create or locate Genie Space.")
    return str(space_id)


def format_genie_attachments(attachments: list[Any]) -> list[str]:
    """Pure function formatting generated SQL and natural-language text lines from Genie message attachments."""
    lines: list[str] = []
    for att in attachments:
        if isinstance(att, dict):
            if "query" in att and att["query"]:
                q_val = att["query"].get("query") if isinstance(att["query"], dict) else str(att["query"])
                lines.append(f"  Genie Generated SQL: {q_val}")
            if "text" in att and att["text"]:
                t_val = att["text"].get("content") if isinstance(att["text"], dict) else str(att["text"])
                lines.append(f"  Genie Response Text: {t_val}")
        else:
            q = getattr(att, "query", None)
            if q is not None:
                q_text = getattr(q, "query", None) or (q.get("query") if isinstance(q, dict) else str(q))
                if q_text:
                    lines.append(f"  Genie Generated SQL: {q_text}")
            t = getattr(att, "text", None)
            if t is not None:
                t_content = getattr(t, "content", None) or (t.get("content") if isinstance(t, dict) else str(t))
                if t_content:
                    lines.append(f"  Genie Response Text: {t_content}")
    return lines


def poll_genie_conversation(
    space_id: str,
    prompt: str = DEFAULT_GENIE_PROMPT,
    profile: str = "dev",
    client: WorkspaceClient | None = None,
) -> None:
    """Start a Genie Space conversation and poll until completion using databricks-sdk."""
    ws_client = client or WorkspaceClient(profile=profile)
    try:
        msg = ws_client.genie.start_conversation_and_wait(
            space_id=space_id,
            content=prompt,
        )
        print(f"  Genie Message Status: {msg.status}")
        for line in format_genie_attachments(msg.attachments or []):
            print(line)
        return
    except Exception as exc:
        print(f"  Native start_conversation_and_wait fallback: {exc}")
        conv = run_databricks_api(
            "post",
            f"/api/2.0/genie/spaces/{space_id}/start-conversation",
            {"content": prompt},
            profile=profile,
            client=ws_client,
        )
        conv_id, msg_id = conv.get("conversation_id"), conv.get("message_id")
        print(f"  Started Genie Conversation: {conv_id} (Message: {msg_id})")

        endpoint = f"/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}"
        for _ in range(25):
            msg_status = run_databricks_api("get", endpoint, profile=profile, client=ws_client)
            status = msg_status.get("status")
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                print(f"  Genie Message Status: {status}")
                for line in format_genie_attachments(msg_status.get("attachments", [])):
                    print(line)
                return
            time.sleep(3)
        print(f"  Genie Conversation {uuid.uuid4()} polling timeout reached.")
