# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 04__metrics_hierarchy_master
# MAGIC
# MAGIC Lee el árbol de jerarquía desde `00_config/metrics_hierarchy.json`, lo aplana,
# MAGIC y escribe la tabla `main.config.metrics_hierarchy`.
# MAGIC
# MAGIC **Estructura del JSON (2 niveles fijos):**
# MAGIC - **`category`** — agrupador de alto nivel: Profitability, Cash Flow, Growth, Financial Health, Valuation
# MAGIC - **`subcategory`** — agrupador fino dentro de cada categoría: Margins, YoY, Leverage, etc.
# MAGIC - **`metric`** — nombre exacto tal como aparece en `financials_metrics.metric`
# MAGIC
# MAGIC Cada métrica también lleva `unit` (`percent` / `usd` / `ratio`) y `requires_market_data`
# MAGIC (true para las métricas de valuación, que dependen de `market_data`).
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
# MAGIC ### ✏️ Cómo modificar la jerarquía
# MAGIC 1. Edita `00_config/metrics_hierarchy.json` en el repo
# MAGIC 2. Commit + push
# MAGIC 3. Re-ejecuta este notebook (o el pipeline completo)
# MAGIC
# MAGIC > ⚠️ El campo `metric` debe coincidir EXACTAMENTE con el nombre usado en
# MAGIC > `22__derived_metrics` (incluyendo espacios y símbolos: `Gross Margin %`,
# MAGIC > `Debt / Equity`, `P/E`, etc.). Si no coincide, el join en el dashboard fallará.

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

HIERARCHY_JSON_PATH = "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/metrics_hierarchy.json"
TARGET_TABLE        = f"{CATALOG}.config.metrics_hierarchy"

print(f"Source : {HIERARCHY_JSON_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar y aplanar el JSON

# COMMAND ----------

with open(HIERARCHY_JSON_PATH, "r", encoding="utf-8") as f:
    tree = json.load(f)

# Las claves que empiezan por '_' son metadata/comentarios — descartarlas
tree = {k: v for k, v in tree.items() if not k.startswith("_")}

print(f"Categorías encontradas: {list(tree.keys())}")

# COMMAND ----------

# Estructura plana de 2 niveles — no necesita recursión.
# Recorremos: category → subcategory → lista de metrics.
# El sort_order se asigna globalmente (10, 20, 30, ...) respetando el
# orden del JSON. Esto da un orden estable en el dashboard.

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

print(f"Total métricas aplanadas: {len(rows)}")
for r in rows:
    print(f"  {r['sort_order']:>4}  {r['category']:<18} | {r['subcategory']:<18} | {r['metric']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escribir a Delta (overwrite — la jerarquía es siempre fuente única)

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

print(f"✓ {sdf.count():,} filas escritas → {TARGET_TABLE}")

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
# MAGIC ## 4. Validación — todas las métricas en `financials_metrics` están mapeadas
# MAGIC
# MAGIC Detecta métricas que existen en `financials_metrics` pero no aparecen en la jerarquía
# MAGIC (orphan metrics) o al revés (jerarquía con métricas que nunca se computan).
# MAGIC Si todo está bien mapeado, ambas consultas deben devolver 0 filas.

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
        print("✓ Todas las métricas en financials_metrics están mapeadas en la jerarquía")
    else:
        print(f"⚠ {n_orph} métricas en financials_metrics SIN entrada en la jerarquía:")
        orphans.show(truncate=False)

    unused = spark.sql(f"""
        SELECT metric
        FROM {TARGET_TABLE}
        WHERE metric NOT IN (SELECT DISTINCT metric FROM {metrics_tbl})
        ORDER BY metric
    """)
    n_unused = unused.count()
    if n_unused == 0:
        print("✓ Todas las métricas de la jerarquía existen en financials_metrics")
    else:
        print(f"⚠ {n_unused} métricas en la jerarquía SIN datos en financials_metrics:")
        unused.show(truncate=False)
except Exception as e:
    print(f"⊘ Validación omitida — {metrics_tbl} aún no existe (ejecuta 22__derived_metrics primero)")
    print(f"  Detalle: {e}")