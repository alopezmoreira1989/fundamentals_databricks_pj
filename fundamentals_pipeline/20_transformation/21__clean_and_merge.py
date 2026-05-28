# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 21_clean_and_merge
# MAGIC
# MAGIC Reads from `financials_raw` (append-only) and merges **annual rows only**
# MAGIC into the clean `financials` fact table.
# MAGIC
# MAGIC Quarterly rows are handled by `21b__derive_quarterly` afterwards.
# MAGIC
# MAGIC **Logic for FY:**
# MAGIC - Filter raw to `form IN ('10-K','10-K/A')` AND `fp = 'FY'` AND `period_shape = 'FY_or_TTM'`
# MAGIC   (for flows) OR `kind = 'stock'` AND snapshot at fiscal year-end
# MAGIC - Dedupe by `(ticker, stmt, concept, fy)` keeping the current-year fact =
# MAGIC   the one with the latest `period_end` (10-K comparatives end earlier)
# MAGIC - UPDATE if value/period_end changed (SEC restated), INSERT if new, leave rest untouched
# MAGIC
# MAGIC Re-running this notebook is always safe — fully idempotent.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

print(f"✓ Config loaded — target: {DB}.{TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Process only the latest scrape (re-run safe — old scrapes still in raw)
latest_scrape = spark.sql(f"SELECT MAX(scraped_at) AS ts FROM {raw_full}").collect()[0]["ts"]
print(f"Latest scrape: {latest_scrape}")

# COMMAND ----------

# MAGIC %md ## Create clean fact table if first run
# MAGIC
# MAGIC Schema includes `period_type`, `period_end`, `is_derived` — populated by both
# MAGIC this notebook (FY rows) and `21b__derive_quarterly` (Q rows).

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_tbl} (
        ticker       STRING    NOT NULL,
        company      STRING,
        stmt         STRING    NOT NULL,
        concept      STRING    NOT NULL,
        fiscal_year  INT       NOT NULL,
        period_type  STRING    NOT NULL,
        period_end   DATE      NOT NULL,
        value        DOUBLE,
        is_derived   BOOLEAN,
        scraped_at   TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker, stmt)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md ## 1. Extract FY rows from raw
# MAGIC
# MAGIC Two paths:
# MAGIC - **Flow concepts** (IS / CF): rows from 10-K filings with `fp='FY'` and `period_shape='FY_or_TTM'`
# MAGIC - **Stock concepts** (BS): snapshot rows from 10-K filings (`form` in 10-K family, `fp='FY'`)
# MAGIC   — these are the year-end snapshots

# COMMAND ----------

raw = spark.table(raw_full).filter(F.col("scraped_at") == latest_scrape)

# Flow FY rows: 10-K, fp='FY'. Quitamos el filtro estricto period_shape='FY_or_TTM'
# para no descartar años de transición/stub con duración fuera de 350–380d (típico
# tras una conversión MLP→C-corp o un cambio de fiscal year-end). El Window de
# dedup más abajo se queda con la fila de period_end MÁS RECIENTE por
# (ticker, stmt, concept, fy) — el fact del año en curso (los comparativos que el 10-K
# trae etiquetados con el mismo fy terminan antes). Esto también captura bien el stub de
# un año de transición (su period_end es el más reciente del grupo). Edge case conocido:
# si un emisor solo reporta Q4 standalone y nada anual, se escogerá ese — degenerado pero
# mejor que descartar el año entero.
# Snapshot se excluye por defensa (no debería aparecer para flows).
flow_fy = (
    raw
    .filter(F.col("kind").isin("flow_additive", "flow_nonadditive"))
    .filter(F.col("form").isin("10-K", "10-K/A"))
    .filter(F.col("fp") == "FY")
    .filter(F.col("period_shape") != "snapshot")
    .filter(F.col("value").isNotNull())
    .withColumn("duration_days", F.datediff(F.col("period_end"), F.col("period_start")))
)

# Stock FY rows: snapshot from 10-K
# Añadimos duration_days = NULL para que el unionByName con flow_fy cuadre el schema.
# Para stocks no hay criterio de duración (todos son snapshots), así que el window
# de dedup cae al siguiente criterio (filed desc) — comportamiento idéntico al previo.
stock_fy = (
    raw
    .filter(F.col("kind") == "stock")
    .filter(F.col("form").isin("10-K", "10-K/A"))
    .filter(F.col("fp") == "FY")
    .filter(F.col("period_shape") == "snapshot")
    .filter(F.col("value").isNotNull())
    .withColumn("duration_days", F.lit(None).cast("int"))
)

incoming = flow_fy.unionByName(stock_fy)
print(f"FY rows incoming: {incoming.count():,}")

# COMMAND ----------

# MAGIC %md ## 2. Normalize & dedupe
# MAGIC
# MAGIC - Revenue (contract) → Revenue when no plain Revenue exists for that fy
# MAGIC - Latest `filed` wins per (ticker, stmt, concept, fy) — handles restatements

# COMMAND ----------

# Normalise: colapsa sinónimos XBRL al concepto canónico via CONCEPT_SYNONYMS
# (heredado del %run de 01__tickers). Si ambos reportan el mismo (ticker, stmt, fy),
# la dedup posterior se queda con uno solo — latest filed, mayor value.
for _alt, _canon in CONCEPT_SYNONYMS.items():
    incoming = incoming.withColumn(
        "concept",
        F.when(F.col("concept") == _alt, _canon).otherwise(F.col("concept"))
    )

# Dedup: nos quedamos con el fact del AÑO EN CURSO por (ticker, stmt, concept, fy).
# Un 10-K etiqueta los comparativos de años previos con el `fy` DEL FILING, así que una
# partición fy contiene varios facts FY; el año en curso es el de `period_end` MÁS RECIENTE
# (los comparativos terminan antes). Ordenar por duración era INCORRECTO — un comparativo
# de 53 semanas (370d) o de año bisiesto (366d) ganaba al año en curso de 52 semanas/365d,
# arrastrando el valor equivocado y un period_end cuyo año ≠ fiscal_year. Misma regla
# MAX(period_end) que usa 21b. `filed` desc resuelve restatements; `value` desc es desempate
# final. Para stocks (BS) period_end desc también elige el snapshot del año en curso (antes
# caía a `value` desc y podía elegir el snapshot comparativo mayor).
w = Window.partitionBy("ticker", "stmt", "concept", "fy").orderBy(
    F.col("period_end").desc_nulls_last(),
    F.col("filed").desc_nulls_last(),
    F.col("value").desc_nulls_last()
)
incoming = (
    incoming
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "duration_days")
    .withColumn("company", F.initcap(F.col("company")))
)

# Project to clean schema
clean_fy = incoming.select(
    F.col("ticker"),
    F.col("company"),
    F.col("stmt"),
    F.col("concept"),
    F.col("fy").alias("fiscal_year"),
    F.lit("FY").alias("period_type"),
    F.col("period_end"),
    F.col("value"),
    F.lit(False).alias("is_derived"),
    F.col("scraped_at"),
)

print(f"After dedupe & normalize: {clean_fy.count():,} FY rows ready for MERGE")

# COMMAND ----------

# MAGIC %md ## 3. MERGE — upsert FY rows into clean table

# COMMAND ----------

clean_fy.createOrReplaceTempView("incoming_fy")

spark.sql(f"""
    MERGE INTO {full_tbl} AS target
    USING incoming_fy AS source
    ON  target.ticker      = source.ticker
    AND target.stmt        = source.stmt
    AND target.concept     = source.concept
    AND target.fiscal_year = source.fiscal_year
    AND target.period_type = source.period_type

    WHEN MATCHED AND (target.value != source.value OR target.period_end != source.period_end) THEN
        UPDATE SET
            target.value      = source.value,
            target.period_end = source.period_end,
            target.company    = source.company,
            target.scraped_at = source.scraped_at

    WHEN NOT MATCHED THEN
        INSERT (ticker, company, stmt, concept, fiscal_year, period_type,
                period_end, value, is_derived, scraped_at)
        VALUES (source.ticker, source.company, source.stmt, source.concept,
                source.fiscal_year, source.period_type, source.period_end,
                source.value, source.is_derived, source.scraped_at)
""")

print(f"✓ MERGE complete → {full_tbl} (FY rows)")

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        COUNT(DISTINCT stmt)           AS statements,
        COUNT(DISTINCT fiscal_year)    AS years,
        MIN(fiscal_year)               AS first_year,
        MAX(fiscal_year)               AS last_year,
        COUNT(*)                       AS total_rows
    FROM {full_tbl}
    WHERE period_type = 'FY'
    GROUP BY ticker
    ORDER BY ticker
    LIMIT 20
""").display()
