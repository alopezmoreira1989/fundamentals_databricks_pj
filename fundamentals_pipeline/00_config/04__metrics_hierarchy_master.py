# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 04__metrics_hierarchy_master
# MAGIC
# MAGIC Reads the hierarchy tree from `00_config/metrics_hierarchy.json`, flattens it,
# MAGIC and writes the `main.config.metrics_hierarchy` table.
# MAGIC
# MAGIC **JSON structure (2 fixed levels):**
# MAGIC - **`category`** — high-level grouper: Profitability, Cash Flow, Growth, Financial Health, Valuation
# MAGIC - **`subcategory`** — fine grouper within each category: Margins, YoY, Leverage, etc.
# MAGIC - **`metric`** — exact name as it appears in `financials_metrics.metric`
# MAGIC
# MAGIC Each metric also carries `unit` (`percent` / `usd` / `ratio`) and `requires_market_data`
# MAGIC (true for valuation metrics that depend on `market_data`).
# MAGIC
# MAGIC **Salida (ejemplo):**
# MAGIC ```
# MAGIC category         | subcategory      | metric              | unit    | requires_market_data | sort_order
# MAGIC Profitability    | Margins          | Gross Margin %      | percent | false                | 10
# MAGIC Profitability    | Margins          | Operating Margin %  | percent | false                | 20
# MAGIC Profitability    | Margins          | Net Margin %        | percent | false                | 30
# MAGIC Profitability    | Margins          | FCF Margin %        | percent | false                | 40
# MAGIC Cash Flow        | Absolute         | Free Cash Flow      | usd     | false                | 50
# MAGIC Growth           | YoY              | Revenue YoY %       | percent | false                | 60
# MAGIC ...
# MAGIC Valuation        | Enterprise Value | EV/EBITDA           | ratio   | true                 | 180
# MAGIC ```
# MAGIC
# MAGIC ### ✏️ How to modify the hierarchy
# MAGIC 1. Edit `00_config/metrics_hierarchy.json` in the repo
# MAGIC 2. Commit + push
# MAGIC 3. Re-run this notebook (or the full pipeline)
# MAGIC
# MAGIC > ⚠️ The `metric` field must match EXACTLY the name used in
# MAGIC > `22__derived_metrics` (including spaces and symbols: `Gross Margin %`,
# MAGIC > `Debt / Equity`, `P/E`, etc.). If it doesn't match, the join in the dashboard will fail.

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

HIERARCHY_JSON_PATH = "../00_config/metrics_hierarchy.json"
TARGET_TABLE        = f"{CATALOG}.config.metrics_hierarchy"

print(f"Source : {HIERARCHY_JSON_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and flatten the JSON

# COMMAND ----------

with open(HIERARCHY_JSON_PATH, "r", encoding="utf-8") as f:
    tree = json.load(f)

# Keys starting with '_' are metadata/comments — discard them
tree = {k: v for k, v in tree.items() if not k.startswith("_")}

print(f"Categories found: {list(tree.keys())}")

# COMMAND ----------

# Flat 2-level structure — no recursion needed.
# We iterate: category → subcategory → list of metrics.
# sort_order is assigned globally (10, 20, 30, ...) respecting the
# JSON order. This gives a stable order in the dashboard.

rows = []
counter = 0

for category, subcategories in tree.items():
    for subcategory, metrics in subcategories.items():
        for m in metrics:
            counter += 10
            rows.append({
                "category":             category,
                "subcategory":          subcategory,
                "metric":               m["metric"],
                "unit":                 m.get("unit", "ratio"),
                "requires_market_data": bool(m.get("requires_market_data", False)),
                "sort_order":           counter,
            })

print(f"Total flattened metrics: {len(rows)}")
for r in rows:
    print(f"  {r['sort_order']:>4}  {r['category']:<18} | {r['subcategory']:<18} | {r['metric']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write to Delta (overwrite — the hierarchy is always the single source)

# COMMAND ----------

schema = StructType([
    StructField("category",             StringType(),  False),
    StructField("subcategory",          StringType(),  False),
    StructField("metric",               StringType(),  False),
    StructField("unit",                 StringType(),  False),
    StructField("requires_market_data", BooleanType(), False),
    StructField("sort_order",           IntegerType(), False),
])

sdf = spark.createDataFrame(rows, schema=schema)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ {sdf.count():,} rows written → {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preview

# COMMAND ----------

spark.sql(f"""
    SELECT
        category,
        subcategory,
        metric,
        unit,
        requires_market_data,
        sort_order
    FROM {TARGET_TABLE}
    ORDER BY sort_order
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation — all metrics in `financials_metrics` are mapped
# MAGIC
# MAGIC Detects metrics that exist in `financials_metrics` but do not appear in the hierarchy
# MAGIC (orphan metrics) or vice versa (hierarchy with metrics that are never computed).
# MAGIC If everything is mapped correctly, both queries should return 0 rows.

# COMMAND ----------

metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"

try:
    orphans = spark.sql(f"""
        SELECT DISTINCT metric
        FROM {metrics_tbl}
        WHERE metric NOT IN (SELECT metric FROM {TARGET_TABLE})
        ORDER BY metric
    """)
    n_orph = orphans.count()
    if n_orph == 0:
        print("✓ All metrics in financials_metrics are mapped in the hierarchy")
    else:
        print(f"⚠ {n_orph} metrics in financials_metrics WITHOUT an entry in the hierarchy:")
        orphans.show(truncate=False)

    unused = spark.sql(f"""
        SELECT metric
        FROM {TARGET_TABLE}
        WHERE metric NOT IN (SELECT DISTINCT metric FROM {metrics_tbl})
        ORDER BY metric
    """)
    n_unused = unused.count()
    if n_unused == 0:
        print("✓ All hierarchy metrics exist in financials_metrics")
    else:
        print(f"⚠ {n_unused} hierarchy metrics WITHOUT data in financials_metrics:")
        unused.show(truncate=False)
except Exception as e:
    print(f"⊘ Validation skipped — {metrics_tbl} does not exist yet (run 22__derived_metrics first)")
    print(f"  Detalle: {e}")