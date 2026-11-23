# Gemini Developer & Agent Guidelines

Project: **Enterprise Data Vault 2.0 & Kimball Dimensional DWH on Databricks AWS**

This document provides architectural standards, critical engineering constraints, and development workflows for Gemini agents working in this repository.

---

## TOP PRIORITY: Mandatory Security Guardrails (Zero Secret Leakage)

> [!CAUTION]
> ### STRICT PROHIBITION ON CREDENTIAL LEAKS & MANDATORY SECRET SCANNING
>
> 1. **NEVER Place Databricks Service Principal Secrets or AWS Secret Keys in Any File**:
>    - **Absolute Prohibition**: Never write, hardcode, paste, commit, or reference plaintext Databricks Service Principal secrets (`DATABRICKS_CLIENT_SECRET`, `ARM_CLIENT_SECRET`), AWS secret access keys (`AWS_SECRET_ACCESS_KEY`), session tokens (`AWS_SESSION_TOKEN`), Databricks Personal Access Tokens (PAT), passwords, or private keys into any newly created or updated files (e.g., `databricks.yml`, Terraform configs, GitHub Actions workflows, Python modules, shell scripts, notebooks, or documentation).
>    - **Runtime & Vault Injection Only**: All sensitive credentials must be supplied exclusively through secure runtime environment variables or vault secret managers (e.g., GitHub Actions Secrets, Databricks Secret Scopes `dbutils.secrets.get()`, or AWS Secrets Manager).
>
> 2. **Service Principal IDs for Deployment Require Explicit Purpose Comments**:
>    - **Deployment Configuration**: When a Databricks Service Principal Application/Client ID (`service_principal_name`, `client_id`, `DATABRICKS_CLIENT_ID`) or AWS Account ID / IAM Role ARN is legitimately required for deployment configurations (e.g., `databricks.yml` `run_as` blocks, Terraform provider definitions, or CI/CD job configurations), you **MUST** accompany it with a clear, descriptive comment explaining its purpose and explicitly stating that the corresponding secret must never be committed to code or configuration files.
>    - **Compliant Pattern**:
>      ```yaml
>      # Databricks Service Principal Application ID used for automated headless bundle deployment and pipeline execution.
>      # NOTE: The client secret must NEVER be placed in code or config; provide via DATABRICKS_CLIENT_SECRET environment variable.
>      run_as:
>        service_principal_name: "<service-principal-client-id>"
>      ```
>
> 3. **Proactive Secret Scanning on All Newly Created or Updated Files**:
>    - **Pre-Save & Pre-Commit Verification**: Always thoroughly scan newly created or updated files for accidental secrets, tokens, or credential patterns before finalizing edits, staging, or committing.
>    - **Patterns to Inspect**:
>      - Databricks tokens (e.g., `dapi...`)
>      - AWS Access Key IDs (e.g., `AKIA...`, `ASIA...`) and Secret Access Keys
>      - OAuth client secrets, private keys, high-entropy tokens, or password strings
>    - **Verification**: Run pre-commit hooks (`detect-secrets`, `detect-aws-credentials`, `bandit -c bandit.yaml`) and secret scanning checks. Immediately sanitize, scrub, and replace any detected credentials with environment variable bindings before saving.

---

## 1. Architecture Overview
- **Storage / Volume**: Incremental Parquet ingestion into Unity Catalog Managed Volume (`/Volumes/<catalog>/<schema>/<volume>/`).
- **Bronze Layer**: Streaming ingestion via Auto Loader (`spark.readStream.format("cloudFiles")` or `read_files`) into append-only Delta tables with `_metadata`.
- **Silver Layer (Data Vault 2.0)**:
  - **Raw Vault**: Hubs (`hub_*`), Links (`lnk_*`), and Satellites (`sat_*`). Deterministic SHA-256 hash keys, insert-only.
  - **Business Vault**: Point-in-Time (`pit_*`) snapshots, Bridge (`br_*`) tables, and Business Satellites (`sat_*_bv`).
- **Gold Layer (Kimball Dimensional DWH)**:
  - **Dimensions (`dim_*`)**: Conformed dimensions with SCD Type 1 (in-place) and SCD Type 2 (`__START_AT` / `__END_AT`).
  - **Facts (`fct_*`)**: Transaction, Periodic Snapshot, and Accumulating Snapshot facts with explicitly declared grain.
  - **Consumer Views (`vw_*`)**: Tailored for BI / SQL Analysts and ML Feature Store.

---

## 2. Critical Engineering Constraints
0. **Zero Credential Leakage & Proactive Secret Scanning**:
   - Strict adherence to the top-priority security guardrail: never commit secrets, always comment Service Principal IDs, and scan all modified/new files.
1. **Modern Lakeflow Pipelines**:
   - Always use `from pyspark import pipelines as dp` (never import deprecated `dlt`).
   - Use `CREATE OR REFRESH STREAMING TABLE` or `CREATE OR REFRESH MATERIALIZED VIEW` (never `CREATE OR REPLACE`).
2. **Data Vault 2.0 Hashing Standard**:
   - Primary and foreign hash keys (`hk_*`) must be SHA-256 over trimmed, uppercase strings concatenated with delimiter `^^`.
   - Coalesce null natural keys to `-1` sentinel values.
   - Hash diffs: SHA-256 over alphabetically sorted payload columns coalesced with `'^^NULL^^'`.
3. **Kimball Dimensional Modeling**:
   - Fact tables must have an explicitly documented business grain.
   - Use Databricks native `__START_AT` / `__END_AT` columns for SCD Type 2 dimension historical tracking.
4. **Functional PySpark Principles**:
   - Pure, single-responsibility functions residing in `src/python/modules/`.
   - Composable transforms via `DataFrame.transform()`.
   - Explicit column projections with `.select()` contracts; avoid row-by-row Python UDFs.
5. **Lakehouse SQL Conventions**:
   - Modular CTEs, `QUALIFY` clause for window rank/row_number filters, `GROUP BY ALL`.
   - Avoid `ORDER BY` shuffles in intermediate queries.
6. **Data Quality & DQX**:
   - Implement expectations using `@dp.expect`, `@dp.expect_or_drop`, or `@dp.expect_or_fail`.
   - Validate datasets against data contracts via Databricks Labs DQX.

---

## 3. Local Testing & Verification
- Test all pure transformation logic locally without requiring a remote cluster:
  ```bash
  pytest -v
  ```
- Measure and enforce test coverage:
  ```bash
  pytest --cov=src/python/modules --cov-report=term-missing
  ```
- Run formatting and type-checking:
  ```bash
  ruff check .
  ruff format --check .
  mypy src/python/modules
  ```

---

## 4. Deployment via Databricks Asset Bundles (DAB)
- Deployment configuration: `databricks.yml`.
- **Service Principal Deployment**: When configuring `run_as.service_principal_name` or deployment targets, always include a descriptive comment documenting the Service Principal ID's purpose and stating explicitly that the client secret must come from environment variables (`DATABRICKS_CLIENT_SECRET`), never written into configuration files.
- Standard bundle commands:
  ```bash
  databricks bundle validate
  databricks bundle deploy --target dev
  databricks bundle run data_vault_and_kimball_dwh_pipeline
  ```

---

## 5. Skills Reference
Domain skills are available in `.gemini/skills/`:
- `databricks-spark-declarative-pipelines`
- `databricks-data-vault-2-0`
- `kimball-dimensional-modeling`
- `pyspark-style-guide`
- `lakehouse-sql-style-guide`
- `databricks-dqx`
- `functional-programming`
- `databricks-bundles`
- `databricks-jobs`
- `pytest`
- `pytest-coverage`
- `databricks-mcps`
- `databricks-aws-unity-catalog`
- `databricks-aws-workspace`
- `github-workflows`
