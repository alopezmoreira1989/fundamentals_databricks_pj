# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 33__fy_staleness_check
# MAGIC
# MAGIC Detecta **conceptos core con FY ausente u obsoleto** en `main.financials.financials`
# MAGIC — el síntoma de un gap de sinónimo XBRL (un emisor que cambió, p.ej., `NetIncomeLoss`
# MAGIC por `ProfitLoss`, o `NetCashProvidedByUsedInOperatingActivities` por su variante
# MAGIC `…ContinuingOperations`). Esos huecos producen ROE / Net Margin / P-E **obsoletos o
# MAGIC nulos** en el screener y la página de detalle.
# MAGIC
# MAGIC **Método:** para cada ticker, el último fiscal year con `Revenue` FY es la referencia
# MAGIC de "empresa viva". Un concepto cuyo último FY va por detrás de esa referencia (o falta
# MAGIC del todo) se marca como *stale*/*missing*. Es **informativo** (no hace fail) — sirve
# MAGIC para validar el efecto de añadir sinónimos en `00_config/01__tickers.py` y para vigilar
# MAGIC regresiones futuras.
# MAGIC
# MAGIC **Nota:** algunos "missing" son legítimos (bancos/REITs sin Gross Profit / Operating
# MAGIC Income). El objetivo es ver la TENDENCIA del recuento tras cada cambio de sinónimos,
# MAGIC no perseguir el cero absoluto.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Conceptos core a vigilar (label canónico tal como queda en `financials`).
# La referencia de "empresa viva" es el último FY con Revenue.
CORE_CONCEPTS = [
    ("Income Statement", "Net Income"),
    ("Income Statement", "Operating Income"),
    ("Cash Flow",        "Operating Cash Flow"),
    ("Balance Sheet",    "Total Stockholders Equity"),
    ("Balance Sheet",    "Total Assets"),
    ("Cash Flow",        "CapEx"),
]

# COMMAND ----------

# MAGIC %md ## 1. Referencia por ticker: último FY con Revenue

# COMMAND ----------

fy = spark.table(full_tbl).filter(F.col("period_type") == "FY")

ref = (
    fy.filter((F.col("stmt") == "Income Statement") & (F.col("concept") == "Revenue"))
      .groupBy("ticker")
      .agg(F.max("fiscal_year").alias("ref_fy"))
)

# COMMAND ----------

# MAGIC %md ## 2. Por concepto: último FY vs referencia → missing / stale

# COMMAND ----------

summary_rows = []
detail = None

for stmt, concept in CORE_CONCEPTS:
    c_max = (
        fy.filter((F.col("stmt") == stmt) & (F.col("concept") == concept))
          .groupBy("ticker")
          .agg(F.max("fiscal_year").alias("concept_fy"))
    )
    j = ref.join(c_max, on="ticker", how="left")

    missing = j.filter(F.col("concept_fy").isNull())
    stale   = j.filter(F.col("concept_fy").isNotNull() & (F.col("concept_fy") < F.col("ref_fy")))

    n_missing = missing.count()
    n_stale   = stale.count()
    summary_rows.append((stmt, concept, n_missing, n_stale, n_missing + n_stale))

    affected = (
        j.filter(F.col("concept_fy").isNull() | (F.col("concept_fy") < F.col("ref_fy")))
         .withColumn("stmt", F.lit(stmt))
         .withColumn("concept", F.lit(concept))
         .withColumn("status", F.when(F.col("concept_fy").isNull(), F.lit("missing")).otherwise(F.lit("stale")))
         .withColumn("years_behind", F.col("ref_fy") - F.coalesce(F.col("concept_fy"), F.lit(0)))
         .select("ticker", "stmt", "concept", "ref_fy", "concept_fy", "status", "years_behind")
    )
    detail = affected if detail is None else detail.unionByName(affected)

# COMMAND ----------

# MAGIC %md ## 3. Report

# COMMAND ----------

summary = spark.createDataFrame(
    summary_rows, ["stmt", "concept", "missing", "stale", "total_affected"]
).orderBy(F.col("total_affected").desc())

print("FY staleness por concepto core (vs último FY con Revenue):")
summary.display()

print("\nDetalle — peores casos (más años por detrás):")
detail.orderBy(F.col("years_behind").desc()).display()
