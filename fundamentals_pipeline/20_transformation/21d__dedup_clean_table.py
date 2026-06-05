# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 20_transformation / 21d__dedup_clean_table
# MAGIC
# MAGIC **Remediación puntual** — NO forma parte del pipeline recurrente.
# MAGIC
# MAGIC > **Superseded para el caso BS:** `21f__dedup_balance_sheet` es el paso RECURRENTE que
# MAGIC > refuerza "un snapshot de BS por `(ticker, stmt, concept, period_end)`" en cada run y cubre
# MAGIC > las clases que el predicado de §1/§2 de aquí NO veía (mismo `period_end` con `fiscal_year`
# MAGIC > distinto, y cierres fiscales mal etiquetados como trimestre del MISMO `fiscal_year`). Este
# MAGIC > notebook se mantiene solo como remediación histórica de cross-labels.
# MAGIC
# MAGIC El `MERGE` de `21__clean_and_merge` / `21b__derive_quarterly` hace upsert pero
# MAGIC **nunca borra**. Cuando una versión previa de `21b` escribió una clave de MERGE con
# MAGIC un `period_end` equivocado y la versión parcheada ya no re-emite esa clave, la fila
# MAGIC queda huérfana en `financials` y el MERGE no la limpia.
# MAGIC
# MAGIC **Caso principal (cross-label de Balance Sheet):** el bug del desfase de `fiscal_year`
# MAGIC en `21b` etiquetaba el snapshot del CIERRE FISCAL de un año como un trimestre (Q3 casi
# MAGIC siempre) del año siguiente — un period_end que en realidad pertenece a una fila `FY`.
# MAGIC Tras parchear `21b`, re-correr el pipeline corrige (UPDATE) la mayoría de esas claves
# MAGIC in situ; este notebook borra las que el re-run ya no re-emite.
# MAGIC
# MAGIC **Invariante usado:** una fila de BS con `period_type ∈ {Q1..Q4}` cuyo `period_end`
# MAGIC coincide con el `period_end` de una fila `FY` del mismo `(ticker, stmt, concept)` pero
# MAGIC de OTRO `fiscal_year` es, por definición, un cierre fiscal mal etiquetado como
# MAGIC trimestre (un trimestre nunca cae en el límite del año fiscal). Esas filas se borran.
# MAGIC
# MAGIC **Orden de ejecución recomendado:** corre primero el pipeline ya parcheado (21 → 21b)
# MAGIC para que el MERGE haga UPDATE de las claves que sí re-emite; LUEGO este notebook para
# MAGIC borrar las huérfanas restantes.
# MAGIC
# MAGIC También colapsa, como red de seguridad, cualquier duplicado en la clave del MERGE
# MAGIC `(ticker, stmt, concept, fiscal_year, period_type)` (hoy 0; el fix de `21b` evita que
# MAGIC se generen nuevos). Idempotente: re-ejecutar sobre una tabla ya limpia no cambia nada.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_dedup_staging"

KEY = ["ticker", "stmt", "concept", "fiscal_year", "period_type"]

# COMMAND ----------

# MAGIC %md ## 1. Borrar cross-labels de Balance Sheet (cierre fiscal mal etiquetado como Q)

# COMMAND ----------

fin = spark.table(full_tbl)

# period_end de cada FY real/derivada por (ticker, stmt, concept)
fy_ends = (
    fin.filter(F.col("period_type") == "FY")
       .select("ticker", "stmt", "concept",
               F.col("fiscal_year").alias("fy_fy"), "period_end")
       .distinct()
)

# Filas de BS trimestrales cuyo period_end es un cierre fiscal de OTRO fiscal_year.
bad = (
    fin.filter((F.col("stmt") == "Balance Sheet")
               & F.col("period_type").isin("Q1", "Q2", "Q3", "Q4"))
       .alias("q")
       .join(fy_ends.alias("f"),
             on=["ticker", "stmt", "concept", "period_end"], how="inner")
       .filter(F.col("q.fiscal_year") != F.col("f.fy_fy"))
       .select("q.ticker", "q.stmt", "q.concept",
               "q.fiscal_year", "q.period_type", "q.period_end")
       .distinct()
)

n_bad = bad.count()
print(f"Cross-labels de BS a borrar (cierre fiscal etiquetado como trimestre): {n_bad:,}")
if n_bad:
    bad.groupBy("period_type").count().orderBy("period_type").show()

bad.createOrReplaceTempView("bad_xlabel")

# DELETE puntual vía MERGE (la clave + period_end identifica la fila de forma única;
# 0 duplicados en la clave del MERGE — verificado en el paso 2).
spark.sql(f"""
    MERGE INTO {full_tbl} AS t
    USING bad_xlabel AS s
    ON  t.ticker      = s.ticker
    AND t.stmt        = s.stmt
    AND t.concept     = s.concept
    AND t.fiscal_year = s.fiscal_year
    AND t.period_type = s.period_type
    AND t.period_end  = s.period_end
    WHEN MATCHED THEN DELETE
""")
print(f"✓ DELETE de cross-labels completo → {full_tbl}")

# COMMAND ----------

# MAGIC %md ## 2. Red de seguridad — colapsar duplicados en la clave del MERGE
# MAGIC
# MAGIC Una fila por `(ticker, stmt, concept, fiscal_year, period_type)`. Hoy debería haber 0
# MAGIC duplicados (el fix de `21b` los evita), así que esto es idempotente. Desempate:
# MAGIC 1. `scraped_at` desc — gana el run más reciente.
# MAGIC 2. `period_end` desc — gana el fact del año en curso (no el comparativo desfasado).
# MAGIC 3. `is_derived` asc — preferimos el valor reportado por SEC sobre el derivado.
# MAGIC 4. `value` desc — desempate final determinista.

# COMMAND ----------

dup_before = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups, COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT {', '.join(KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        GROUP BY {', '.join(KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]
total_before = spark.table(full_tbl).count()
print(f"Filas totales            : {total_before:,}")
print(f"Grupos duplicados (clave): {dup_before['dup_groups']:,}")
print(f"Filas sobrantes a borrar : {dup_before['extra_rows']:,}")

# COMMAND ----------

if dup_before["dup_groups"] > 0:
    w = Window.partitionBy(*KEY).orderBy(
        F.col("scraped_at").desc_nulls_last(),
        F.col("period_end").desc_nulls_last(),
        F.col("is_derived").asc_nulls_last(),
        F.col("value").desc_nulls_last(),
    )

    deduped = (
        spark.table(full_tbl)
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Materializamos en staging para no leer y sobrescribir la misma tabla a la vez.
    (
        deduped.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging)
    )

    staged_count = spark.table(staging).count()
    print(f"Filas tras dedup (staging): {staged_count:,}  (se eliminarán {total_before - staged_count:,})")

    # INSERT OVERWRITE conserva la definición de la tabla (NOT NULL, partición, autoOptimize).
    target_cols = [c for c in spark.table(full_tbl).columns]
    spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_dedup")
    spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_dedup")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    print(f"✓ INSERT OVERWRITE completo → {full_tbl}")
else:
    print("✓ 0 duplicados en la clave del MERGE — nada que colapsar.")

# COMMAND ----------

# MAGIC %md ## 3. Verificación — 0 cross-labels y 0 duplicados de clave

# COMMAND ----------

dup_after = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT {', '.join(KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        GROUP BY {', '.join(KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]["dup_groups"]

xlabel_after = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {full_tbl} q
    JOIN (
        SELECT ticker, stmt, concept, fiscal_year AS fy_fy, period_end
        FROM {full_tbl} WHERE period_type = 'FY'
    ) f
      ON q.ticker = f.ticker AND q.stmt = f.stmt AND q.concept = f.concept
     AND q.period_end = f.period_end
    WHERE q.stmt = 'Balance Sheet'
      AND q.period_type IN ('Q1','Q2','Q3','Q4')
      AND q.fiscal_year <> f.fy_fy
""").collect()[0]["n"]

total_after = spark.table(full_tbl).count()
print(f"Filas totales        : {total_after:,}")
print(f"Grupos duplicados    : {dup_after:,}  (esperado 0)")
print(f"Cross-labels BS      : {xlabel_after:,}  (esperado 0)")
assert dup_after == 0,    "Aún quedan duplicados en la clave del MERGE."
assert xlabel_after == 0, "Aún quedan cross-labels de BS — revisar el predicado."
print("✓ tabla limpia")
