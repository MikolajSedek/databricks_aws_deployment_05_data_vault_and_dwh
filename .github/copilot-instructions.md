# GitHub Copilot Instructions

Project: **Enterprise Data Vault 2.0 & Kimball Dimensional DWH on Databricks AWS**

Follow these authoritative guidelines when suggesting or generating code in this repository:

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
- **Bronze**: Streaming ingestion from Unity Catalog Managed Volume via Auto Loader (`cloudFiles` / `read_files`) with `_metadata`.
- **Silver (Data Vault 2.0)**:
  - Raw Vault: Hubs (`hub_*`), Links (`lnk_*`), Satellites (`sat_*`). Deterministic SHA-256 keys, insert-only.
  - Business Vault: Point-in-Time (`pit_*`), Bridge (`br_*`), and Business Satellites (`sat_*_bv`).
- **Gold (Kimball DWH)**:
  - 9 Conformed Dimensions (`dim_*`) with SCD Type 1 and SCD Type 2 (`__START_AT` / `__END_AT`).
  - 3 Fact Tables (`fct_*`): Explicitly declared grains for orders, events, and daily snapshot.
  - Consumer Views (`vw_*`) for BI reporting and ML Feature Store.

---

## 2. Key Engineering Constraints
0. **Zero Credential Leakage & Proactive Secret Scanning**:
   - Strict adherence to the top-priority security guardrail: never commit secrets, always comment Service Principal IDs, and scan all modified/new files.
1. **Lakeflow Pipelines**:
   - Always import modern syntax: `from pyspark import pipelines as dp` (never use `dlt`).
   - Use `CREATE OR REFRESH STREAMING TABLE` or `CREATE OR REFRESH MATERIALIZED VIEW` (never `CREATE OR REPLACE`).
2. **Data Vault 2.0 Hashing**:
   - Primary and foreign hash keys (`hk_*`): SHA-256 over uppercase trimmed natural keys concatenated with `^^`. Coalesce nulls with `-1`.
   - Hash diff: SHA-256 over alphabetically sorted payload columns coalesced with `'^^NULL^^'`.
3. **Kimball DWH**:
   - Explicitly declare and document the grain of every fact table.
   - Use `__START_AT` and `__END_AT` timestamp columns for SCD Type 2 historical validity.
4. **PySpark Style**:
   - Composable pure functions in `src/python/modules/` chained via `.transform()`.
   - Explicit `.select()` column contracts. Avoid Python row-by-row UDFs.
5. **SQL Style**:
   - Modular CTEs, `QUALIFY` clause for window filters, `GROUP BY ALL`, avoid `ORDER BY` shuffles in intermediate tables.
6. **Data Quality**:
   - Lakeflow expectations (`@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`) and Databricks Labs DQX checks.
7. **Testing**:
   - Test transformation functions locally with `pytest` using an in-memory `SparkSession` without remote cluster dependencies.

---

## 3. Skills Reference
Domain skills are configured in `.github/copilot/skills/` (matching `.gemini/skills/`).
