# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 20__transformation / 21d__dedup_clean_table
# MAGIC
# MAGIC **One-off remediation** — NOT part of the recurring pipeline.
# MAGIC
# MAGIC > **Superseded for the BS case:** `21f__dedup_balance_sheet` is the RECURRING step that
# MAGIC > enforces "one BS snapshot per `(ticker, stmt, concept, period_end)`" on every run and covers
# MAGIC > the classes that the §1/§2 predicate here did NOT catch (same `period_end` with different
# MAGIC > `fiscal_year`, and fiscal closes mislabeled as a quarter within the SAME `fiscal_year`). This
# MAGIC > notebook is kept only as a historical remediation for cross-labels.
# MAGIC
# MAGIC The `MERGE` in `21__clean_and_merge` / `21b__derive_quarterly` upserts but
# MAGIC **never deletes**. When a prior version of `21b` wrote a MERGE key with a wrong `period_end`
# MAGIC and the patched version no longer emits that key, the row becomes an orphan in `financials`
# MAGIC and the MERGE never cleans it up.
# MAGIC
# MAGIC **Main case (Balance Sheet cross-label):** the `fiscal_year` offset bug in `21b` labeled
# MAGIC the FISCAL-CLOSE snapshot of one year as a quarter (almost always Q3) of the following year —
# MAGIC a period_end that actually belongs to an `FY` row. After patching `21b`, re-running the
# MAGIC pipeline corrects (UPDATE) most of those keys in place; this notebook deletes the ones the
# MAGIC re-run no longer emits.
# MAGIC
# MAGIC **Invariant used:** a BS row with `period_type ∈ {Q1..Q4}` whose `period_end` matches the
# MAGIC `period_end` of an `FY` row for the same `(ticker, stmt, concept)` but a DIFFERENT
# MAGIC `fiscal_year` is, by definition, a fiscal close mislabeled as a quarter (a quarter never
# MAGIC falls on a fiscal year boundary). Those rows are deleted.
# MAGIC
# MAGIC **Recommended execution order:** run the already-patched pipeline (21 → 21b) first so the
# MAGIC MERGE UPDATEs the keys it does re-emit; THEN run this notebook to delete the remaining orphans.
# MAGIC
# MAGIC Also collapses, as a safety net, any duplicate on the MERGE key
# MAGIC `(ticker, stmt, concept, fiscal_year, period_type)` (currently 0; the `21b` fix prevents new
# MAGIC ones from being generated). Idempotent: re-running on an already-clean table changes nothing.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_dedup_staging"

KEY = ["ticker", "stmt", "concept", "fiscal_year", "period_type"]

# COMMAND ----------

# MAGIC %md ## 1. Delete Balance Sheet cross-labels (fiscal close mislabeled as Q)

# COMMAND ----------

fin = spark.table(full_tbl)

# period_end of each real/derived FY per (ticker, stmt, concept)
fy_ends = (
    fin.filter(F.col("period_type") == "FY")
       .select("ticker", "stmt", "concept",
               F.col("fiscal_year").alias("fy_fy"), "period_end")
       .distinct()
)

# Quarterly BS rows whose period_end is a fiscal close from a DIFFERENT fiscal_year.
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
print(f"BS cross-labels to delete (fiscal close labeled as a quarter): {n_bad:,}")
if n_bad:
    bad.groupBy("period_type").count().orderBy("period_type").show()

bad.createOrReplaceTempView("bad_xlabel")

# Targeted DELETE via MERGE (key + period_end uniquely identifies the row;
# 0 duplicates on the MERGE key — verified in step 2).
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
print(f"✓ Cross-label DELETE complete → {full_tbl}")

# COMMAND ----------

# MAGIC %md ## 2. Safety net — collapse duplicates on the MERGE key
# MAGIC
# MAGIC One row per `(ticker, stmt, concept, fiscal_year, period_type)`. There should be 0
# MAGIC duplicates today (the `21b` fix prevents them), so this is idempotent. Tiebreak order:
# MAGIC 1. `scraped_at` desc — most recent run wins.
# MAGIC 2. `period_end` desc — current-year fact wins (not the stale comparative).
# MAGIC 3. `is_derived` asc — prefer SEC-reported value over derived.
# MAGIC 4. `value` desc — deterministic final tiebreak.

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
print(f"Total rows               : {total_before:,}")
print(f"Duplicate groups (key)   : {dup_before['dup_groups']:,}")
print(f"Extra rows to delete     : {dup_before['extra_rows']:,}")

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

    # Materialize into staging to avoid reading and overwriting the same table simultaneously.
    (
        deduped.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging)
    )

    staged_count = spark.table(staging).count()
    print(f"Rows after dedup (staging): {staged_count:,}  ({total_before - staged_count:,} will be removed)")

    # INSERT OVERWRITE preserves the table definition (NOT NULL, partition, autoOptimize).
    target_cols = [c for c in spark.table(full_tbl).columns]
    spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_dedup")
    spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_dedup")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    print(f"✓ INSERT OVERWRITE complete → {full_tbl}")
else:
    print("✓ 0 duplicates on the MERGE key — nothing to collapse.")

# COMMAND ----------

# MAGIC %md ## 3. Verification — 0 cross-labels and 0 key duplicates

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
print(f"Total rows           : {total_after:,}")
print(f"Duplicate groups     : {dup_after:,}  (expected 0)")
print(f"Cross-labels BS      : {xlabel_after:,}  (expected 0)")
assert dup_after == 0,    "Duplicates still remain on the MERGE key."
assert xlabel_after == 0, "BS cross-labels still remain — review the predicate."
print("✓ clean table")
