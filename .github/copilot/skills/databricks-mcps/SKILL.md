---
name: databricks-mcps
description: "Authoritative reference and setup guide for Model Context Protocol (MCP) servers operating on Databricks Lakehouse: Genie One MCP, Databricks AI Dev Kit MCP (44 tools), Databricks Labs MCP, Databricks Agent Kit (dak), DQX MCP, GitHub MCP, and AWS Docs MCP."
compatibility: "Model Context Protocol (MCP), FastMCP, Databricks Unity Gateway, Python"
metadata:
  version: "1.0.0"
category: AI & Agent Tooling
tags:
  - mcp
  - model-context-protocol
  - genie-one
  - ai-dev-kit
  - databricks
  - unity-catalog
  - github-mcp
  - aws-mcp
---

# Authoritative MCP Servers for Databricks Lakehouse Projects

This guide documents the authoritative **Model Context Protocol (MCP)** servers that act as primary knowledge and operational bridges for AI coding agents (Claude Code, Cursor, Gemini, Copilot, Antigravity) working on Databricks Lakehouse projects.

---

## 1. Databricks Genie One MCP (`system.ai.genie_one_mcp`)

### Overview
The **Genie One MCP server** is an official Databricks-provided MCP hosted on **Unity Gateway**. It connects AI agents directly to Databricks Genie, allowing natural language queries grounded in your enterprise **Genie Ontology** (governed semantic layer, metric definitions, and entity relationships).

### Available Tools:
| Tool | Description |
|---|---|
| `genie_ask` | Ask Genie a natural-language data question. Initiates asynchronous query generation and execution. |
| `genie_poll_response` | Polls in-flight execution status, progress narration, and query results. |
| `genie_get_query_result` | Retrieves complete SQL result rows and schema when answers are truncated. |
| `genie_cancel_response` | Cancels an in-flight Genie execution. |
| `view_ask` | Interactive view ask for clients supporting MCP Apps UI extensions. |

### Connection Configuration:
- **Unity Gateway Endpoint**:
  `https://<workspace-hostname>/ai-gateway/mcp-services/system.ai.genie_one_mcp?o=<workspace-id>`
- **Authentication**: OAuth on-behalf-of user token or Service Principal OAuth with `ai-gateway` scope.

---

## 2. Databricks AI Dev Kit MCP Server (`databricks-mcp-server`)

### Overview
Provided by `github.com/databricks-solutions/ai-dev-kit`, this FastMCP server exposes **44 authoritative tools** directly into your IDE/agent, enabling the agent to inspect UC volumes, query SQL warehouses, manage Lakeflow pipelines, and check table schemas.

### Key Tool Categories:
1. **SQL & Data Querying**:
   - `execute_sql`: Runs queries on a Databricks SQL Warehouse.
   - `get_table_stats_and_schema`: Retrieves row counts, column schemas, and data statistics.
   - `get_volume_folder_details`: Inspects files and structure inside Unity Catalog Volumes (critical for raw event ingestion).
2. **Spark Declarative Pipelines (Lakeflow)**:
   - `manage_pipeline` (`create`, `create_or_update`, `get`, `update`, `delete`, `find_by_name`).
   - `manage_pipeline_run` (`start`, `get`, `stop`, `get_events`).
3. **Unity Catalog Governance**:
   - `manage_uc_objects`: Catalogs, schemas, volumes, external locations.
   - `manage_uc_grants`: Access controls, permissions.
   - `manage_uc_monitors`: Data quality monitoring in Lakehouse.
4. **Workflows & Jobs**:
   - `manage_jobs`: Scaffolds and modifies Multi-Task Jobs.
   - `manage_job_runs`: Triggers, monitors, and validates job runs.

### Configuration with Project Service Principal:
Example configuration:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["<path-to-ai-dev-kit>/databricks-mcp-server/run_server.py"],
      "env": {
        "DATABRICKS_HOST": "https://<your-databricks-workspace-url>",
        "DATABRICKS_CLIENT_ID": "<service-principal-client-id>",
        "DATABRICKS_CLIENT_SECRET": "<service-principal-secret>",
        "DATABRICKS_ACCOUNT_ID": "<databricks-account-id>"
      },
      "defer_loading": true
    }
  }
}
```

---

## 3. Databricks Labs MCP ([`databrickslabs/mcp`](https://github.com/databrickslabs/mcp))

### Overview
The official Databricks Labs repository for Model Context Protocol integrations. It provides:
- Unity Catalog functions and metadata tooling.
- SQL query execution endpoints over Databricks SQL Warehouses.
- Templates for hosting custom internal MCP servers directly on **Databricks Apps**.

---

## 4. Databricks Agent Kit ([`alexmarco/databricks-agent-kit`](https://github.com/alexmarco/databricks-agent-kit) – `dak`)

### Overview
A lightweight, security-hardened MCP server designed for coding agents that connects over `stdio` or Streamable HTTP.
- **Why it is useful**: It operates strictly over public REST APIs (SQL statement execution and Unity Catalog APIs), enforcing read-only permissions and identity boundaries defined in Unity Catalog so an agent can inspect schemas, volumes, and sample rows without risk.

---

## 5. Databricks Labs DQX MCP Server

### Overview
Provided by **Databricks Labs DQX** ([databrickslabs.github.io/dqx](https://databrickslabs.github.io/dqx/)), this server equips agents with automated tools for:
- Auto-profiling raw event data in UC volumes to propose quality rule candidates.
- Validating Open Data Contract Standard (ODCS) YAML specifications against schemas.
- Inspecting data quality metrics and pipeline failure logs.

---

## 6. Official GitHub MCP Server ([`@modelcontextprotocol/server-github`](https://github.com/modelcontextprotocol/servers/tree/main/src/github))

### Overview
Enables coding agents to interact with GitHub issues, branches, and GitHub Actions CI/CD workflows directly:
- Inspect test run results (`lint`, `test-pure`, `test-spark`, `deploy`) from `.github/workflows/ci.yml`.
- Triage failing unit tests in PRs without leaving the editor.

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "env": {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "<your-pat-with-repo-scope>"
      }
    }
  }
}
```

---

## 7. AWS Documentation & Architecture MCP ([`awslabs/mcp-server-aws`](https://github.com/awslabs/mcp-server-aws))

### Overview
Provides semantic search across official AWS documentation and AWS Well-Architected Framework guidance.
- Essential for verifying S3 bucket policies, IAM cross-account roles, KMS key access, and storage credential configurations supporting Databricks Unity Catalog on AWS.

---

## 8. Data Vault & Dimensional Modeling Agent Skills

Two specialized domain modeling packages provide authoritative knowledge:
1. **Data Vault 2.0 (`ScalefreeCOM/datavault4dbt-agent-skills`)**:
   - Macro standards for Hubs, Links, and Satellites.
   - PIT table generation and Point-in-Time joins.
   - Automated hashkey / hashdiff rules and ghost record patterns.
2. **Dimensional Modeler (`curiositech/windags-skills/dimensional-modeler`)**:
   - Kimball methodology, Star schema design, and Fact table grains.
   - Slowly Changing Dimensions (SCD Type 1 & Type 2).
   - Conformed dimensions and ML Feature Store table preparation.
