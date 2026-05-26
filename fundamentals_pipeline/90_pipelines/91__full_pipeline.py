# Databricks notebook source
# MAGIC %md
# MAGIC # 90_pipelines / 91__full_pipeline
# MAGIC
# MAGIC **Entry point for the Databricks Job.**
# MAGIC Runs the full ingestion → transformation → analysis pipeline in sequence.
# MAGIC
# MAGIC ```
# MAGIC favorites.json              editar favoritos (00_config/favorites.json en el repo)
# MAGIC concept_hierarchy.json      editar jerarquía de conceptos (00_config/ en el repo)
# MAGIC metrics_hierarchy.json      editar jerarquía de derived metrics (00_config/ en el repo)
# MAGIC valuation_assumptions.json  editar supuestos de valoración (00_config/ en el repo)
# MAGIC       ↓
# MAGIC 02__tickers_master              build main.config.tickers
# MAGIC       ↓
# MAGIC 03__concept_hierarchy_master    build main.config.concept_hierarchy
# MAGIC       ↓
# MAGIC 04__metrics_hierarchy_master    build main.config.metrics_hierarchy
# MAGIC       ↓
# MAGIC 11__fetch_sec_xbrl              fetch from SEC API → financials_raw
# MAGIC       ↓
# MAGIC 12__fetch_market_data           fetch from Yahoo Finance → market_data
# MAGIC       ↓
# MAGIC 21__clean_and_merge             deduplicate → MERGE into financials (FY rows)
# MAGIC       ↓
# MAGIC 21b__derive_quarterly           derive Q1–Q4 standalone rows (Q4 = FY − YTD_Q3 fallback)
# MAGIC       ↓
# MAGIC 22__derived_metrics             margins, FCF, YoY, leverage, valuation ratios
# MAGIC       ↓
# MAGIC 23__intrinsic_value             Graham, Graham revised, DCF, Owner Earnings (FY + TTM)
# MAGIC       ↓
# MAGIC 31__company_analysis            validation queries
# MAGIC       ↓
# MAGIC 51__export_dashboard_data       slice + write parquet artifacts to /tmp/
# MAGIC       ↓
# MAGIC 52__publish_to_github           upload artifacts as GitHub Release assets (latest tag)
# MAGIC ```
# MAGIC
# MAGIC > **Nota:** `02__tickers_master`, `03__concept_hierarchy_master` y `04__metrics_hierarchy_master` viven en `00_config/`.
# MAGIC > En este pipeline asumimos que `main.config.tickers` ya existe — se construye
# MAGIC > manualmente con `02__tickers_master`. Las jerarquías (`concept_hierarchy` y `metrics_hierarchy`)
# MAGIC > se reconstruyen automáticamente en cada run desde sus respectivos JSONs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Pipeline parameters
# MAGIC
# MAGIC Override at runtime via Databricks Job parameters:
# MAGIC `{"tickers_override": "AAPL,TSLA", "run_optimization": "false"}`
# MAGIC
# MAGIC - `tickers_override`: lista de tickers separados por coma (omite `main.config.tickers`)
# MAGIC - `run_optimization`: `true` para correr OPTIMIZE + VACUUM al final
# MAGIC
# MAGIC > El universo de tickers (`main.config.tickers`) se mantiene manualmente con
# MAGIC > `02__tickers_master`. Las jerarquías de conceptos y métricas
# MAGIC > (`main.config.concept_hierarchy` y `main.config.metrics_hierarchy`)
# MAGIC > sí se reconstruyen automáticamente en cada run desde sus respectivos JSONs.

# COMMAND ----------

dbutils.widgets.text("tickers_override",  "",      "tickers_override")
dbutils.widgets.text("run_optimization",  "false", "run_optimization")
dbutils.widgets.text("rebuild_config",    "false", "rebuild_config")

tickers_override = dbutils.widgets.get("tickers_override")
run_optimization = dbutils.widgets.get("run_optimization")
rebuild_config   = dbutils.widgets.get("rebuild_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load config

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/01__tickers"

# COMMAND ----------

from datetime import datetime

if tickers_override:
    ACTIVE_TICKERS = [t.strip() for t in tickers_override.split(",") if t.strip()]
    print(f"✓ Override mode — {len(ACTIVE_TICKERS)} tickers: {ACTIVE_TICKERS}")
else:
    tickers_df = spark.table(f"{CATALOG}.config.tickers")
    ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} tickers from main.config.tickers")

if not ACTIVE_TICKERS:
    raise ValueError("No tickers found — run 02__tickers_master first.")

pipeline_start = datetime.utcnow()
print(f"Pipeline started at {pipeline_start.isoformat()} UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Status de config tables
# MAGIC
# MAGIC `main.config.tickers` debe existir antes de ejecutar este pipeline — se construye
# MAGIC manualmente con `02__tickers_master` (cuando edites `favorites.json`).
# MAGIC `main.config.concept_hierarchy` y `main.config.metrics_hierarchy` se reconstruyen
# MAGIC automáticamente en los pasos 3 y 4.

# COMMAND ----------

# El rebuild de main.config.tickers no se ejecuta automáticamente porque hace
# llamadas a Wikipedia (S&P 500) y iShares (Russell 3000) que rara vez cambian.
#
# Para refrescar el universo de tickers, ejecuta 02__tickers_master manualmente
# (por ejemplo, después de editar favorites.json en el repo).
#
# Las jerarquías (siguientes pasos) sí se reconstruyen siempre — son baratas.

if rebuild_config.lower() == "true":
    print("⚠ rebuild_config=true detectado, pero el rebuild de tickers se hace")
    print("  manualmente — ejecuta 02__tickers_master por separado.")
else:
    print("⊘ Skipping ticker rebuild — usa main.config.tickers tal como está")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Concept hierarchy
# MAGIC `main.config.concept_hierarchy` — jerarquía de conceptos para agrupación y orden en el dashboard.
# MAGIC
# MAGIC Siempre se reconstruye (es barato — ~48 filas, <5s) para asegurar que la tabla
# MAGIC refleja el estado actual de `concept_hierarchy.json` en el repo.

# COMMAND ----------

print("=" * 55)
print("STEP 1 / 12 — Concept Hierarchy")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/03__concept_hierarchy_master"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metrics hierarchy
# MAGIC `main.config.metrics_hierarchy` — jerarquía de derived metrics (category → subcategory)
# MAGIC para agrupación y orden en el dashboard.
# MAGIC
# MAGIC Siempre se reconstruye (es barato — ~40 filas, <2s) para asegurar que la tabla
# MAGIC refleja el estado actual de `metrics_hierarchy.json` en el repo.

# COMMAND ----------

print("=" * 55)
print("STEP 2 / 12 — Metrics Hierarchy")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/04__metrics_hierarchy_master"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion — fetch from SEC EDGAR
# MAGIC `financials_raw` — append-only audit log

# COMMAND ----------

print("=" * 55)
print("STEP 3 / 12 — SEC Ingestion")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/10_ingestion/11__fetch_sec_xbrl"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion — fetch market data from Yahoo Finance
# MAGIC `market_data` — year-end prices + market cap

# COMMAND ----------

print("=" * 55)
print("STEP 4 / 12 — Market Data")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/10_ingestion/12__fetch_market_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Clean & merge → fact table
# MAGIC `financials` — deduplicated, clean long-format fact table

# COMMAND ----------

print("=" * 55)
print("STEP 5 / 12 — Clean & Merge")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/20_transformation/21__clean_and_merge"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Derive quarterly rows
# MAGIC `financials` — añade filas Q1–Q4 standalone por concepto.
# MAGIC
# MAGIC Por `kind`:
# MAGIC - `flow_additive`: Q1–Q3 standalone (~90d) o `YTD_n − YTD_(n-1)`. Q4 standalone si existe en el 10-K, si no `FY − YTD_Q3`.
# MAGIC - `flow_nonadditive`: solo standalone (~90d); NULL si no.
# MAGIC - `stock`: snapshot al `period_end` (dedup latest `filed`).
# MAGIC
# MAGIC Requiere que `21__clean_and_merge` haya escrito las filas FY antes.

# COMMAND ----------

print("=" * 55)
print("STEP 6 / 12 — Derive Quarterly")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/20_transformation/21b__derive_quarterly"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Derived metrics
# MAGIC `financials_metrics` — margins, FCF, YoY growth, leverage, valuation ratios

# COMMAND ----------

print("=" * 55)
print("STEP 7 / 12 — Derived Metrics")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/20_transformation/22__derived_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Intrinsic Value
# MAGIC `financials_intrinsic_value` — Graham Number, Graham Revised, DCF y Owner Earnings
# MAGIC para cada fiscal year histórico y TTM (rolling 4 quarters). También expone las
# MAGIC métricas resultantes con sufijos `(FY)` y `(TTM)` en `financials_metrics` para que
# MAGIC el dashboard pueda filtrarlas como cualquier otra métrica.
# MAGIC
# MAGIC Los supuestos (WACC, growth, etc.) viven en `00_config/valuation_assumptions.json`
# MAGIC con overrides por ticker. Requiere que `22__derived_metrics` y `12__fetch_market_data`
# MAGIC se hayan ejecutado antes.

# COMMAND ----------

print("=" * 55)
print("STEP 8 / 12 — Intrinsic Value")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/20_transformation/23__intrinsic_value"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Analysis
# MAGIC Runs analysis queries — useful for validation after pipeline runs

# COMMAND ----------

print("=" * 55)
print("STEP 9 / 12 — Analysis")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/30_analysis/31__company_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Coverage check
# MAGIC Verifies that all favorite tickers made it through the full pipeline
# MAGIC (config → financials → financials_metrics). Also surfaces any ingestion
# MAGIC failures from the latest run. Hard fails if >5% of favorites are missing.

# COMMAND ----------

print("=" * 55)
print("STEP 10 / 12 — Coverage Check")
print("=" * 55)

# COMMAND ----------

try:
    dbutils.notebook.run(
        "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/30_analysis/32__coverage_check",
        timeout_seconds=120,
    )
    coverage_ok = True
except Exception as e:
    print(f"⚠ Coverage check failed: {e}")
    coverage_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Export dashboard data
# MAGIC `dashboard_data.parquet` + `dashboard_metrics.parquet` + `dashboard_meta.json`
# MAGIC written to `/tmp/` on the driver. Consumed by step 13 (GitHub publish).
# MAGIC
# MAGIC Wrapped in try/except: a failed export should not fail the nightly run,
# MAGIC since the upstream Delta tables are already updated.
# MAGIC
# MAGIC Uses `dbutils.notebook.run` instead of `%run` (which the earlier steps
# MAGIC use) precisely so we can catch exceptions — `%run` inlines the child
# MAGIC into the same scope and aborts the whole notebook on error.

# COMMAND ----------

print("=" * 55)
print("STEP 11 / 12 — Export dashboard data")
print("=" * 55)

# COMMAND ----------

try:
    dbutils.notebook.run(
        "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/50_publish/51__export_dashboard_data",
        timeout_seconds=600,
    )
    export_ok = True
except Exception as e:
    print(f"⚠ Export failed (non-fatal): {e}")
    export_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Publish to GitHub Release
# MAGIC Uploads the three `/tmp/` artifacts as assets on the `latest` GitHub
# MAGIC release. Skipped if step 12 failed. Requires the `github/github_pat`
# MAGIC Databricks secret — see `50_publish/README.md`.

# COMMAND ----------

print("=" * 55)
print("STEP 12 / 12 — Publish to GitHub")
print("=" * 55)

# COMMAND ----------

if export_ok:
    try:
        dbutils.notebook.run(
            "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/50_publish/52__publish_to_github",
            timeout_seconds=300,
        )
    except Exception as e:
        print(f"⚠ Publish failed (non-fatal): {e}")
else:
    print("⊘ Skipping GitHub publish — export step failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print(f"  Pipeline completed ✓")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC")
print(f"  Finished : {pipeline_end.isoformat()} UTC")
print(f"  Duration : {duration:.1f}s ({duration/60:.1f} min)")
print(f"  Tickers  : {len(ACTIVE_TICKERS):,}")
print()

summary_tables = [
    ("config",      "tickers"),
    ("config",      "concept_hierarchy"),
    ("config",      "metrics_hierarchy"),
    (SCHEMA,        "financials_raw"),
    (SCHEMA,        "financials"),
    (SCHEMA,        "market_data"),
    (SCHEMA,        "financials_metrics"),
    (SCHEMA,        "financials_intrinsic_value"),
]

for schema, tbl in summary_tables:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        n = spark.table(full).count()
        print(f"  {full}: {n:,} rows")
    except Exception:
        print(f"  {full}: (not found)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Optional: optimize Delta tables
# MAGIC Set `run_optimization=true` in Job params to enable. Run at most once a week.

# COMMAND ----------

if run_optimization.lower() == "true":
    print("Running OPTIMIZE + VACUUM...")
    for tbl in ["financials", "market_data", "financials_metrics", "financials_intrinsic_value"]:
        full = f"{CATALOG}.{SCHEMA}.{tbl}"
        try:
            spark.sql(f"OPTIMIZE {full}")
            spark.sql(f"VACUUM   {full} RETAIN 168 HOURS")
            print(f"  ✓ {full}")
        except Exception as e:
            print(f"  ✗ {full}: {e}")
    print("Done.")
else:
    print("⊘ Skipping OPTIMIZE/VACUUM (set run_optimization=true to enable)")