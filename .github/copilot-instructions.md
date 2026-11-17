# GitHub Copilot Instructions

Project: **Enterprise Data Vault 2.0 & Kimball Dimensional DWH on Databricks AWS**

Follow these authoritative guidelines when suggesting or generating code in this repository:

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
