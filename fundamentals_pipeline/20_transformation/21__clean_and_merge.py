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
# MAGIC - Filter raw to `form IN ('10-K','10-K/A')` AND `fp = 'FY'`, EXCLUDING the sub-annual
# MAGIC   shapes a 10-K re-reports under `fp='FY'` (`Q_standalone`, `YTD_6M`, `YTD_9M`) — so only
# MAGIC   annual shapes (`FY_or_TTM` + `other_Nd` transition stubs) compete (for flows); OR
# MAGIC   `kind = 'stock'` AND snapshot at fiscal year-end
# MAGIC - Dedupe by `(ticker, stmt, concept, fy)` keeping the current-year fact = latest
# MAGIC   `period_end`, preferring the `FY_or_TTM`/longest-duration row before any `value` tiebreak
# MAGIC - UPDATE if value/period_end changed (SEC restated), INSERT if new, leave rest untouched
# MAGIC - DELETE FY-flow rows the patched selection no longer emits (a sub-annual form previously
# MAGIC   fabricated as the annual) — the MERGE itself never deletes; see step 4
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
# `raw` (el slice de la última scrape de financials_raw, ~81M filas) se re-escanea 3×: flow_fy,
# stock_fy y el _flow_fy_all del orphan-DELETE de §4, cada uno con su propio predicate pushdown
# sobre el Delta completo. localCheckpoint(eager) lo materializa una vez y los filtros aguas
# abajo leen del checkpoint en disco local en vez de re-escanear la tabla. .cache()/.persist()
# NO van en serverless ([NOT_SUPPORTED_WITH_SERVERLESS]); se libera al cerrar la sesión.
raw = raw.localCheckpoint(eager=True)

# Flow FY rows: 10-K, fp='FY'. NO usamos el filtro estricto period_shape='FY_or_TTM'
# (descartaría años de transición/stub con duración fuera de 350–380d — p.ej. tras una
# conversión MLP→C-corp o un cambio de fiscal year-end —, que el clasificador etiqueta
# como `other_Nd`). Pero SÍ excluimos las formas SUB-ANUALES que un 10-K re-reporta con
# fp='FY': el desglose trimestral del año (Q1..Q4 standalone, ~90d) y los YTD de 6M/9M.
# El Q4 standalone (`Q_standalone`, ~91d, period_end == fy_end) es indistinguible de la fila
# FY por (fp, period_end, filed) → empataba en el Window de dedup hasta `value desc`, que
# elegía el MAYOR valor (a menudo el Q4, no el anual) y corrompía la FY (~9k filas, p.ej.
# ALNY fy2019: −276.19 del Q4 en vez de −886.12 del año). Excluyéndolas, solo compiten las
# formas anuales (`FY_or_TTM` 365d + `other_Nd` stubs). Snapshot se excluye por defensa.
# Edge case: un fy cuyo ÚNICO fact fp='FY' es sub-anual ya no produce fila FY aquí (se borra
# como huérfana más abajo); `21e__derive_fy_from_quarterly` reconstruye el anual desde ΣQ
# para los flow_additive con los 4 trimestres.
flow_fy = (
    raw
    .filter(F.col("kind").isin("flow_additive", "flow_nonadditive"))
    .filter(F.col("form").isin("10-K", "10-K/A"))
    .filter(F.col("fp") == "FY")
    .filter(~F.col("period_shape").isin("snapshot", "Q_standalone", "YTD_6M", "YTD_9M"))
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

# Prioridad de sinónimo (menor = preferido) calculada ANTES del rename, desde el label
# de ORIGEN. Net Income coexiste como NetIncomeLoss + ProfitLoss + (to common) en el
# mismo fy, así que el desempate no puede ser `value desc`. CONCEPT_PRIORITY fuerza
# attributable-first; labels no listados → 0 (comportamiento previo de Revenue intacto).
_prio = F.lit(0)
for _label, _rank in CONCEPT_PRIORITY.items():
    _prio = F.when(F.col("concept") == _label, F.lit(_rank)).otherwise(_prio)
incoming = incoming.withColumn("prio", _prio)

# Normalise: colapsa sinónimos XBRL al concepto canónico via CONCEPT_SYNONYMS
# (heredado del %run de 01__tickers). Si ambos reportan el mismo (ticker, stmt, fy),
# la dedup posterior se queda con uno solo — prio asc, latest filed, mayor value.
for _alt, _canon in CONCEPT_SYNONYMS.items():
    incoming = incoming.withColumn(
        "concept",
        F.when(F.col("concept") == _alt, _canon).otherwise(F.col("concept"))
    )

# Drop comparativos mal etiquetados: un fact del año en curso SIEMPRE termina en un
# año calendario >= su fiscal_year (cierre en dic = año fy; Jan-enders cierran en fy+1).
# Un period_end cuyo año < fy es un comparativo de un filing posterior etiquetado con el
# fy de ese filing — nunca un dato genuino del año en curso (ningún emisor del universo
# etiqueta su fiscal year por ADELANTE; verificado vía modal offset por ticker). Sin esto,
# un fy cuyo único fact disponible es un comparativo produce una fila desalineada que el
# MERGE no borra y se acumula.
incoming = incoming.filter(F.year(F.col("period_end")) >= F.col("fy"))

# Dedup: nos quedamos con el fact del AÑO EN CURSO por (ticker, stmt, concept, fy).
# Un 10-K etiqueta los comparativos de años previos con el `fy` DEL FILING, así que una
# partición fy contiene varios facts FY; el año en curso es el de `period_end` MÁS RECIENTE
# (los comparativos terminan antes). Ordenar por duración era INCORRECTO — un comparativo
# de 53 semanas (370d) o de año bisiesto (366d) ganaba al año en curso de 52 semanas/365d,
# arrastrando el valor equivocado y un period_end cuyo año ≠ fiscal_year. Misma regla
# MAX(period_end) que usa 21b. `filed` desc resuelve restatements; `value` desc es desempate
# final. Para stocks (BS) period_end desc también elige el snapshot del año en curso (antes
# caía a `value` desc y podía elegir el snapshot comparativo mayor).
# `prio asc` va DESPUÉS de period_end (elegir año en curso vs comparativo) pero ANTES
# de value: entre tags que coexisten en el mismo fy/period_end gana el preferido.
# La forma ANUAL gana ANTES de cualquier desempate por valor: si tras filtrar las formas
# sub-anuales aún coexisten un `FY_or_TTM` y un stub `other_Nd` en el mismo period_end,
# preferimos el anual de 365d y, en su defecto, el de mayor duración — para que `value desc`
# (último recurso determinista) NUNCA vuelva a elegir una forma más corta por ser mayor.
w = Window.partitionBy("ticker", "stmt", "concept", "fy").orderBy(
    F.col("period_end").desc_nulls_last(),
    (F.col("period_shape") == "FY_or_TTM").desc(),   # prefiere la forma anual real
    F.col("prio").asc_nulls_last(),
    F.col("filed").desc_nulls_last(),
    F.col("duration_days").desc_nulls_last(),         # luego el período más largo (anual > stub)
    F.col("value").desc_nulls_last()                  # desempate final determinista
)
incoming = (
    incoming
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "duration_days", "prio")
    .withColumn("company", F.initcap(F.col("company")))
)

# Guard: un period_end pertenece a EXACTAMENTE un fiscal year. Tras la dedup por fy,
# el mismo period_end puede seguir apareciendo bajo varios fy (un 10-K reciente trae el
# año en curso como current y los previos como comparativos, cada uno con su propio fy
# en su filing original). Nos quedamos con el fy más pequeño (el filing del año en curso)
# y descartamos el resto, para que un comparativo no se haga pasar por un año posterior.
# El MERGE no borra, así que sin esto los duplicados cross-fy se acumulan en `financials`.
w2 = Window.partitionBy("ticker", "stmt", "concept", "period_end").orderBy(F.col("fy").asc())
incoming = (
    incoming
    .withColumn("rn2", F.row_number().over(w2))
    .filter(F.col("rn2") == 1)
    .drop("rn2")
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

# clean_fy se consume 3× aguas abajo: count() aquí, el MERGE de §3 y `_kept_keys` del
# orphan-DELETE de §4. Su linaje es caro (scan de `raw` + union + synonym-loops + dos
# window functions de dedup) — sin materializar se recomputa entero cada vez. localCheckpoint
# (eager) lo calcula UNA vez y trunca el linaje. OJO serverless: .cache()/.persist() NO van
# ([NOT_SUPPORTED_WITH_SERVERLESS]); localCheckpoint sí. Se libera al cerrar la sesión.
clean_fy = clean_fy.localCheckpoint(eager=True)

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

# MAGIC %md ## 4. Borrar FY de FLOW huérfanas (anual fabricado a partir de una forma sub-anual)
# MAGIC
# MAGIC El MERGE de arriba hace UPDATE/INSERT pero **nunca borra**. Antes, una forma sub-anual con
# MAGIC `fp='FY'` (Q4 standalone ~90d, o YTD 6M/9M) ganaba el desempate `value desc` y se escribía
# MAGIC como la fila `FY` (`is_derived=False`) de ese `(ticker, stmt, concept, fy)`. Con el filtro
# MAGIC nuevo de `flow_fy`, los años cuyo ÚNICO fact `fp='FY'` es sub-anual (o un comparativo mal
# MAGIC etiquetado que cae por el guard `year(period_end) >= fy`) YA NO producen fila FY, así que la
# MAGIC vieja queda huérfana y stale. La borramos aquí (~2.5k filas). `21e__derive_fy_from_quarterly`
# MAGIC (más adelante en el pipeline) reconstruye el anual desde `ΣQ1..Q4` para los `flow_additive`
# MAGIC con los 4 trimestres presentes. **Idempotente:** solo toca `is_derived=False`; las FY
# MAGIC reconstruidas (`is_derived=True`) y las FY reales nunca se borran. Acotado al universo de
# MAGIC claves del scrape en curso → nunca toca histórico de tickers/conceptos ausentes del scrape.

# COMMAND ----------

# Universo de claves FLOW con ALGÚN fact fp='FY' en este scrape, colapsado al concepto canónico
# (para casar con `financials`). Incluye las formas sub-anuales que `flow_fy` ahora excluye.
_flow_fy_all = (
    raw
    .filter(F.col("kind").isin("flow_additive", "flow_nonadditive"))
    .filter(F.col("form").isin("10-K", "10-K/A"))
    .filter(F.col("fp") == "FY")
    .filter(F.col("period_shape") != "snapshot")
    .filter(F.col("value").isNotNull())
)
for _alt, _canon in CONCEPT_SYNONYMS.items():
    _flow_fy_all = _flow_fy_all.withColumn(
        "concept", F.when(F.col("concept") == _alt, _canon).otherwise(F.col("concept"))
    )
_all_keys = _flow_fy_all.select(
    "ticker", "stmt", "concept", F.col("fy").alias("fiscal_year")
).distinct()

# Claves que el 21 parcheado SÍ (re)emite = tienen un anual genuino (FY_or_TTM / other_Nd del
# año en curso). Las demás del universo flow → su FY publicada es un fabricado a borrar.
_kept_keys = clean_fy.select("ticker", "stmt", "concept", "fiscal_year").distinct()

orphan_keys = _all_keys.join(
    _kept_keys, on=["ticker", "stmt", "concept", "fiscal_year"], how="left_anti"
)
orphan_keys.createOrReplaceTempView("orphan_fy_keys")

n_orphan = orphan_keys.count()
print(f"FY de flow huérfanas a borrar (anual fabricado / sin anual genuino): {n_orphan:,}")

# DELETE puntual vía MERGE; solo filas FY reportadas (is_derived=False), nunca derivadas/reales-OK.
spark.sql(f"""
    MERGE INTO {full_tbl} AS t
    USING orphan_fy_keys AS s
    ON  t.ticker      = s.ticker
    AND t.stmt        = s.stmt
    AND t.concept     = s.concept
    AND t.fiscal_year = s.fiscal_year
    AND t.period_type = 'FY'
    WHEN MATCHED AND t.is_derived = false THEN DELETE
""")
print(f"✓ DELETE de FY huérfanas completo → {full_tbl}")

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
