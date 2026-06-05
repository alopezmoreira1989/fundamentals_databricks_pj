# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 21f__dedup_balance_sheet
# MAGIC
# MAGIC **Paso RECURRENTE del pipeline** (a diferencia de `21d`, que es una remediación puntual).
# MAGIC
# MAGIC Refuerza la invariante de Balance Sheet de CLAUDE.md: **una sola fila por
# MAGIC `(ticker, stmt, concept, period_end)`** — un balance a una fecha es UN snapshot. El
# MAGIC `MERGE` de `21__clean_and_merge` / `21b__derive_quarterly` upserta sobre la clave
# MAGIC `(ticker, stmt, concept, fiscal_year, period_type)` y **nunca borra**, así que cuando un
# MAGIC scrape previo etiquetó un snapshot con un `fiscal_year` o `period_type` equivocado, el
# MAGIC scrape corregido INSERTA una fila nueva (clave distinta) en vez de sobrescribir → la vieja
# MAGIC queda huérfana para siempre. Dos clases observadas (ver skill `financials-invariants`):
# MAGIC - **A** mismo `(ticker, concept, period_end, period_type)` con `fiscal_year` distinto
# MAGIC   (el mismo Q3 etiquetado con dos años fiscales).
# MAGIC - **B** mismo `period_end` etiquetado como dos `period_type` (un cierre fiscal marcado como
# MAGIC   trimestre, p.ej. FY y Q2 a la misma fecha).
# MAGIC
# MAGIC `21d` (puntual) solo cubría cross-labels FY-vs-Q de OTRO `fiscal_year` y duplicados de clave
# MAGIC exacta — no estas clases. Este paso las colapsa TODAS por `period_end`.
# MAGIC
# MAGIC **Regla de superviviente** por `(ticker, stmt, concept, period_end)` en BS:
# MAGIC 1. `scraped_at` desc — gana el run más reciente. Tras un `force_full_refresh` el run nuevo
# MAGIC    re-deriva con el `21b` ya parcheado, así que el más reciente es el correcto.
# MAGIC 2. `period_type = 'FY'` primero — un cierre fiscal es FY; el label trimestral es el mislabel.
# MAGIC 3. `is_derived` asc — preferimos el valor reportado por SEC sobre el derivado.
# MAGIC 4. `value` desc — desempate final determinista.
# MAGIC
# MAGIC **Solo toca Balance Sheet.** Income Statement / Cash Flow usan `period_type` para
# MAGIC desambiguar el mismo `period_end` (Q1..Q4/FY) — colapsarlos por `period_end` sería un bug;
# MAGIC se pasan intactos. **Idempotente:** si no hay duplicados, no reescribe nada.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_bsdedup_staging"

# Clave verdadera del snapshot de BS: un balance a una fecha = una fila.
BS_KEY = ["ticker", "stmt", "concept", "period_end"]

# COMMAND ----------

# MAGIC %md ## 1. ¿Hay duplicados de BS por (ticker, stmt, concept, period_end)?

# COMMAND ----------

bs_dups = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups, COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT {', '.join(BS_KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        WHERE stmt = 'Balance Sheet'
        GROUP BY {', '.join(BS_KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]

total_before = spark.table(full_tbl).count()
print(f"Filas totales                 : {total_before:,}")
print(f"Grupos BS duplicados (period) : {bs_dups['dup_groups']:,}")
print(f"Filas BS sobrantes a borrar   : {bs_dups['extra_rows']:,}")

# COMMAND ----------

# MAGIC %md ## 2. Colapsar — quedarse con un snapshot por fecha (regla de superviviente)

# COMMAND ----------

if bs_dups["dup_groups"] > 0:
    w = Window.partitionBy(*BS_KEY).orderBy(
        F.col("scraped_at").desc_nulls_last(),         # run más reciente (post-fix) gana
        (F.col("period_type") == "FY").desc(),         # FY > trimestre en un cierre fiscal
        F.col("is_derived").asc_nulls_last(),          # reportado > derivado
        F.col("value").desc_nulls_last(),              # desempate determinista
    )

    fin = spark.table(full_tbl)
    # stmt es NOT NULL → el filtro reparte exhaustivamente entre BS y no-BS.
    non_bs = fin.filter(F.col("stmt") != "Balance Sheet")
    bs_deduped = (
        fin.filter(F.col("stmt") == "Balance Sheet")
           .withColumn("rn", F.row_number().over(w))
           .filter(F.col("rn") == 1)
           .drop("rn")
    )
    result = non_bs.unionByName(bs_deduped)

    # Materializar en staging para no leer y sobrescribir la misma tabla a la vez.
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging)
    )

    staged_count = spark.table(staging).count()
    print(f"Filas tras dedup (staging): {staged_count:,}  (se eliminarán {total_before - staged_count:,})")

    # INSERT OVERWRITE conserva la definición de la tabla (NOT NULL, partición, autoOptimize).
    target_cols = list(spark.table(full_tbl).columns)
    spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_bsdedup")
    spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_bsdedup")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    print(f"✓ INSERT OVERWRITE completo → {full_tbl}")
else:
    print("✓ 0 duplicados de BS por period_end — nada que colapsar (idempotente).")

# COMMAND ----------

# MAGIC %md ## 3. Verificación — 0 duplicados de BS por (ticker, stmt, concept, period_end)

# COMMAND ----------

dup_after = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT {', '.join(BS_KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        WHERE stmt = 'Balance Sheet'
        GROUP BY {', '.join(BS_KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]["dup_groups"]

total_after = spark.table(full_tbl).count()
print(f"Filas totales        : {total_after:,}")
print(f"Grupos BS duplicados : {dup_after:,}  (esperado 0)")
assert dup_after == 0, "Aún quedan duplicados de BS por period_end — revisar la ventana de dedup."
print("✓ Balance Sheet único por (ticker, stmt, concept, period_end)")
