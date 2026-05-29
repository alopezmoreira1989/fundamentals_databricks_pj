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
# MAGIC El `MERGE` de `21__clean_and_merge` / `21b__derive_quarterly` hace upsert pero
# MAGIC **nunca borra**. Versiones previas del pipeline (antes del fix del desfase de
# MAGIC `fiscal_year` en 21b) dejaron filas huérfanas: la tabla limpia tiene >100k grupos
# MAGIC duplicados en `(ticker, stmt, concept, fiscal_year, period_type)` —la clave del
# MAGIC MERGE, que debería ser única—.
# MAGIC
# MAGIC Este notebook colapsa cada clave a UNA fila y reescribe la tabla por
# MAGIC `INSERT OVERWRITE` (preserva esquema, NOT NULL, particionado y TBLPROPERTIES;
# MAGIC no usa `CREATE OR REPLACE`, que los perdería).
# MAGIC
# MAGIC **Orden de ejecución recomendado:** corre primero el pipeline ya parcheado
# MAGIC (21 → 21b) para que el MERGE haga UPDATE y corrija los valores desfasados de las
# MAGIC claves existentes; LUEGO este dedup para eliminar las filas duplicadas sobrantes.
# MAGIC El criterio de desempate (period_end desc) hace que, si quedara una versión vieja
# MAGIC y una nueva bajo la misma clave, gane la del año en curso.
# MAGIC
# MAGIC Idempotente: re-ejecutar sobre una tabla ya limpia no cambia nada.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_dedup_staging"

KEY = ["ticker", "stmt", "concept", "fiscal_year", "period_type"]

# COMMAND ----------

# MAGIC %md ## 1. Reporte ANTES — cuántos duplicados hay

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

# MAGIC %md ## 2. Dedup
# MAGIC
# MAGIC Una fila por `(ticker, stmt, concept, fiscal_year, period_type)`. Desempate:
# MAGIC 1. `scraped_at` desc — gana el run más reciente.
# MAGIC 2. `period_end` desc — gana el fact del año en curso (no el comparativo desfasado).
# MAGIC 3. `is_derived` asc — preferimos el valor reportado por SEC sobre el derivado.
# MAGIC 4. `value` desc — desempate final determinista.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ## 3. Sobrescribir la tabla limpia desde staging
# MAGIC
# MAGIC `INSERT OVERWRITE` conserva la definición de `{full_tbl}` (NOT NULL, partición
# MAGIC por `(ticker, stmt)`, autoOptimize). Reordenamos las columnas al esquema destino.

# COMMAND ----------

target_cols = [c for c in spark.table(full_tbl).columns]
spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_dedup")

spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_dedup")
print(f"✓ INSERT OVERWRITE completo → {full_tbl}")

# COMMAND ----------

# MAGIC %md ## 4. Verificación DESPUÉS — debe dar 0 grupos duplicados

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
total_after = spark.table(full_tbl).count()
print(f"Filas totales        : {total_after:,}")
print(f"Grupos duplicados    : {dup_after:,}  (esperado 0)")
assert dup_after == 0, "Aún quedan duplicados — revisar la clave/desempate antes de seguir."

# COMMAND ----------

# MAGIC %md ## 5. Limpieza del staging

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {staging}")
print(f"✓ staging eliminado: {staging}")
