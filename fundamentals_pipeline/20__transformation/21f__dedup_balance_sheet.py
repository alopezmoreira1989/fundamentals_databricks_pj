# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 21f__dedup_balance_sheet
# MAGIC
# MAGIC **RECURRING pipeline step** (unlike `21d`, which is a one-off remediation).
# MAGIC
# MAGIC Enforces the Balance Sheet invariant from CLAUDE.md: **one row per
# MAGIC `(ticker, stmt, concept, period_end)`** — a balance at a date is ONE snapshot. The
# MAGIC `MERGE` in `21__clean_and_merge` / `21b__derive_quarterly` upserts on the key
# MAGIC `(ticker, stmt, concept, fiscal_year, period_type)` and **never deletes**, so when a
# MAGIC previous scrape labeled a snapshot with a wrong `fiscal_year` or `period_type`, the
# MAGIC corrected scrape INSERTS a new row (different key) instead of overwriting → the old one
# MAGIC becomes a permanent orphan. Two observed classes (see skill `financials-invariants`):
# MAGIC - **A** same `(ticker, concept, period_end, period_type)` with different `fiscal_year`
# MAGIC   (same Q3 labeled under two fiscal years).
# MAGIC - **B** same `period_end` labeled as two `period_type` values (a fiscal close marked as
# MAGIC   a quarter, e.g. FY and Q2 at the same date).
# MAGIC
# MAGIC `21d` (one-off) only covered FY-vs-Q cross-labels from a DIFFERENT `fiscal_year` and exact
# MAGIC key duplicates — not these classes. This step collapses ALL of them by `period_end`.
# MAGIC
# MAGIC **Survivor rule** per `(ticker, stmt, concept, period_end)` in BS:
# MAGIC 1. `scraped_at` desc — most recent run wins. After a `force_full_refresh` the new run
# MAGIC    re-derives with the already-patched `21b`, so the most recent is correct.
# MAGIC 2. `period_type = 'FY'` first — a fiscal close is FY; the quarter label is the mislabel.
# MAGIC 3. `is_derived` asc — prefer SEC-reported value over derived.
# MAGIC 4. `value` desc — deterministic final tiebreak.
# MAGIC
# MAGIC **Only touches Balance Sheet.** Income Statement / Cash Flow use `period_type` to
# MAGIC disambiguate the same `period_end` (Q1..Q4/FY) — collapsing them by `period_end` would
# MAGIC be a bug; they are passed through unchanged. **Idempotent:** if there are no duplicates,
# MAGIC nothing is rewritten.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_bsdedup_staging"

# True key of a BS snapshot: one balance at one date = one row.
BS_KEY = ["ticker", "stmt", "concept", "period_end"]

# COMMAND ----------

# MAGIC %md ## 1. Are there BS duplicates on (ticker, stmt, concept, period_end)?

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
print(f"Total rows                    : {total_before:,}")
print(f"Duplicate BS groups (period)  : {bs_dups['dup_groups']:,}")
print(f"Extra BS rows to delete       : {bs_dups['extra_rows']:,}")

# COMMAND ----------

# MAGIC %md ## 2. Collapse — keep one snapshot per date (survivor rule)

# COMMAND ----------

if bs_dups["dup_groups"] > 0:
    w = Window.partitionBy(*BS_KEY).orderBy(
        F.col("scraped_at").desc_nulls_last(),         # most recent run (post-fix) wins
        (F.col("period_type") == "FY").desc(),         # FY > quarter at a fiscal close
        F.col("is_derived").asc_nulls_last(),          # reported > derived
        F.col("value").desc_nulls_last(),              # deterministic tiebreak
    )

    fin = spark.table(full_tbl)
    # stmt is NOT NULL → the filter exhaustively partitions into BS and non-BS.
    non_bs = fin.filter(F.col("stmt") != "Balance Sheet")
    bs_deduped = (
        fin.filter(F.col("stmt") == "Balance Sheet")
           .withColumn("rn", F.row_number().over(w))
           .filter(F.col("rn") == 1)
           .drop("rn")
    )
    result = non_bs.unionByName(bs_deduped)

    # Materialize into staging to avoid reading and overwriting the same table simultaneously.
    (
        result.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging)
    )

    staged_count = spark.table(staging).count()
    print(f"Rows after dedup (staging): {staged_count:,}  ({total_before - staged_count:,} will be removed)")

    # INSERT OVERWRITE preserves the table definition (NOT NULL, partition, autoOptimize).
    target_cols = list(spark.table(full_tbl).columns)
    spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_bsdedup")
    spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_bsdedup")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    print(f"✓ INSERT OVERWRITE complete → {full_tbl}")
else:
    print("✓ 0 BS duplicates by period_end — nothing to collapse (idempotent).")

# COMMAND ----------

# MAGIC %md ## 3. Verification — 0 BS duplicates per (ticker, stmt, concept, period_end)

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
print(f"Total rows           : {total_after:,}")
print(f"Duplicate BS groups  : {dup_after:,}  (expected 0)")
assert dup_after == 0, "BS duplicates by period_end still remain — review the dedup window."
print("✓ Balance Sheet unique by (ticker, stmt, concept, period_end)")
