# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 21e__derive_fy_from_quarterly
# MAGIC
# MAGIC **Fallback de último recurso:** sintetiza una fila `FY` para conceptos
# MAGIC `flow_additive` (Income Statement / Cash Flow) **solo cuando NO existe ya** una FY
# MAGIC real, sumando los cuatro trimestres `Q1+Q2+Q3+Q4` que `21b__derive_quarterly` haya
# MAGIC dejado en `financials`. La fila se marca `is_derived=True`.
# MAGIC
# MAGIC **Por qué existe:** algunos emisores no exponen el total anual sin dimensión en su
# MAGIC 10-K (companyfacts de SEC descarta facts dimensionales), así que `21` no encuentra FY.
# MAGIC Si los cuatro trimestres sí están, `ΣQ` reconstruye el año.
# MAGIC
# MAGIC **Rendimiento real hoy: bajo.** El conjunto afectado suele carecer de **Q4** (Q4 a su
# MAGIC vez se deriva de `FY − YTD_Q3`, que necesita el anual — circular), así que esta etapa
# MAGIC dispara para muy pocos tickers. Se ejecuta de todas formas por robustez y para cubrir
# MAGIC casos futuros. El `print` final reporta cuántas filas FY se sintetizaron.
# MAGIC
# MAGIC **Guards (no romper la lógica existente):**
# MAGIC - **Nunca** sobrescribe una FY real → `MERGE … WHEN NOT MATCHED` (insert) y un
# MAGIC   `WHEN MATCHED AND target.is_derived = true` que solo refresca filas ya derivadas.
# MAGIC - Solo `flow_additive`. **No** `flow_nonadditive` (EPS, shares — no son aditivos) ni
# MAGIC   `stock` (el balance no es aditivo; además la tabla limpia no guarda snapshot de
# MAGIC   cierre de año para los casos-gap — `21b` solo emite snapshots Q1–Q3).
# MAGIC - **No** recalcula Q4 (eso es responsabilidad de `21b`); solo crea la fila FY ausente
# MAGIC   a partir de trimestres ya presentes → sin doble conteo.
# MAGIC
# MAGIC Idempotente: re-ejecutar es seguro.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
fin = spark.table(full_tbl)

# COMMAND ----------

# MAGIC %md ## 1. Conceptos flow_additive (desde STATEMENTS)
# MAGIC
# MAGIC Solo estos son sumables Q1..Q4 = FY. La tabla limpia usa el label canónico, que
# MAGIC coincide con las keys de STATEMENTS; los labels-sinónimo no aparecen en `financials`
# MAGIC (ya colapsados por `21`/`21b`), así que el join simplemente no los matchea.

# COMMAND ----------

_flow_pairs = [
    (stmt, label)
    for stmt, cmap in STATEMENTS.items()
    for label, (xbrl, kind) in cmap.items()
    if kind == "flow_additive"
]
flow_concepts = spark.createDataFrame(_flow_pairs, ["stmt", "concept"]).distinct()

# COMMAND ----------

# MAGIC %md ## 2. Σ Q1..Q4 por (ticker, stmt, concept, fiscal_year) — exige los 4 trimestres

# COMMAND ----------

q = (
    fin.filter(F.col("period_type").isin("Q1", "Q2", "Q3", "Q4"))
       .join(flow_concepts, on=["stmt", "concept"], how="inner")
)

def _qval(qn):  # valor del trimestre (NULL si no está)
    return F.max(F.when(F.col("period_type") == qn, F.col("value"))).alias(f"v_{qn}")

agg = (
    q.groupBy("ticker", "company", "stmt", "concept", "fiscal_year")
     .agg(
        _qval("Q1"), _qval("Q2"), _qval("Q3"), _qval("Q4"),
        # period_end de la FY sintética = cierre del Q4 (fin del año fiscal)
        F.max(F.when(F.col("period_type") == "Q4", F.col("period_end"))).alias("q4_end"),
     )
)

# Solo años con los CUATRO trimestres presentes (y un period_end de Q4 válido)
synth = (
    agg.filter(
        F.col("v_Q1").isNotNull() & F.col("v_Q2").isNotNull()
        & F.col("v_Q3").isNotNull() & F.col("v_Q4").isNotNull()
        & F.col("q4_end").isNotNull()
    )
    .withColumn("value", F.col("v_Q1") + F.col("v_Q2") + F.col("v_Q3") + F.col("v_Q4"))
)

# COMMAND ----------

# MAGIC %md ## 3. Quédate solo con (ticker, stmt, concept, fy) que NO tienen ya una FY real

# COMMAND ----------

existing_fy = (
    fin.filter(F.col("period_type") == "FY")
       .filter(~F.coalesce(F.col("is_derived"), F.lit(False)))   # FY real (no derivada)
       .select("ticker", "stmt", "concept", "fiscal_year")
       .distinct()
)

clean_fy = (
    synth.join(existing_fy, on=["ticker", "stmt", "concept", "fiscal_year"], how="left_anti")
    .select(
        F.col("ticker"),
        F.initcap(F.col("company")).alias("company"),
        F.col("stmt"),
        F.col("concept"),
        F.col("fiscal_year"),
        F.lit("FY").alias("period_type"),
        F.col("q4_end").alias("period_end"),
        F.col("value"),
        F.lit(True).alias("is_derived"),
        F.current_timestamp().alias("scraped_at"),
    )
)

n_synth = clean_fy.count()
print(f"FY sintetizadas desde Σ Q1..Q4 (conceptos sin FY real): {n_synth:,}")
if n_synth:
    print("Muestra:")
    clean_fy.select("ticker", "stmt", "concept", "fiscal_year", "value").orderBy("ticker", "concept").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. MERGE — insertar FY sintéticas (nunca tocar una FY real)

# COMMAND ----------

clean_fy.createOrReplaceTempView("incoming_fy_from_q")

spark.sql(f"""
    MERGE INTO {full_tbl} AS target
    USING incoming_fy_from_q AS source
    ON  target.ticker      = source.ticker
    AND target.stmt        = source.stmt
    AND target.concept     = source.concept
    AND target.fiscal_year = source.fiscal_year
    AND target.period_type = source.period_type

    -- Solo refresca filas YA derivadas por esta etapa (nunca una FY real reportada).
    WHEN MATCHED AND target.is_derived = true
                 AND (target.value != source.value OR target.period_end != source.period_end) THEN
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

print(f"✓ MERGE complete → {full_tbl} (FY sintéticas desde trimestres)")
