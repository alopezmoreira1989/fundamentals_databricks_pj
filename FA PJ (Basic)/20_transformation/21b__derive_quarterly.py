# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 20_transformation / 21b__derive_quarterly
# MAGIC
# MAGIC Derives standalone quarter values from raw SEC filings and merges into
# MAGIC the clean `financials` table.
# MAGIC
# MAGIC **Logic per concept kind:**
# MAGIC
# MAGIC | Kind | Q1, Q2, Q3 | Q4 |
# MAGIC |---|---|---|
# MAGIC | `flow_additive` | Standalone (~90d) if exists, else `YTD_n − YTD_(n-1)` | Standalone (~90d) in 10-K if exists, else `FY − YTD_Q3` |
# MAGIC | `flow_nonadditive` | Standalone (~90d) only; NULL otherwise | Standalone (~90d) in 10-K if exists; NULL otherwise |
# MAGIC | `stock` | Snapshot at `period_end` (dedup latest `filed`) | Snapshot at FY end |
# MAGIC
# MAGIC **`is_derived`** is set to `True` whenever the value was computed (Q4 fallback from
# MAGIC `FY − YTD_Q3` for additive flows, or Q1/Q2/Q3 derived from YTD differences).
# MAGIC `False` for SEC-reported standalone values (including Q4 standalone when published
# MAGIC in the 10-K) and BS snapshots.
# MAGIC
# MAGIC **Q4 standalone detection:** XBRL does not have a `fp="Q4"`. When an issuer
# MAGIC files a 10-K, it includes its full four-quarter breakdown as comparative data:
# MAGIC each quarter's standalone (Q1, Q2, Q3, Q4) appears as a ~90d row with
# MAGIC `fp="FY"` and `period_shape="Q_standalone"`. The Q4 standalone is the one
# MAGIC whose `period_end` equals `fy_end` (i.e. the quarter ending with the fiscal
# MAGIC year). The other three (Q1/Q2/Q3 re-reported in the 10-K) are ignored here —
# MAGIC we already capture them from their original 10-Q filings via the
# MAGIC `pick("Q1"/"Q2"/"Q3", "Q_standalone")` calls below.
# MAGIC We prefer the Q4 standalone over `FY − YTD_Q3` (more faithful to what the
# MAGIC issuer actually reported, especially after divestitures or restatements
# MAGIC where YTD figures from old 10-Q filings may be inconsistent with the
# MAGIC restated FY in the 10-K).
# MAGIC
# MAGIC **Stock dedup:** SEC re-reports prior FY snapshots in subsequent 10-Q filings as
# MAGIC comparatives. We dedupe by `(ticker, concept, period_end)` keeping `MAX(filed)`.

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Process only the latest scrape (idempotent re-run)
latest_scrape = spark.sql(f"SELECT MAX(scraped_at) AS ts FROM {raw_full}").collect()[0]["ts"]
print(f"Latest scrape: {latest_scrape}")

raw = spark.table(raw_full).filter(F.col("scraped_at") == latest_scrape)

# Normalise: collapse "Revenue (contract)" → "Revenue"
raw = raw.withColumn(
    "concept",
    F.when(F.col("concept") == "Revenue (contract)", "Revenue").otherwise(F.col("concept"))
)

# COMMAND ----------

# MAGIC %md ## 1. FLOW concepts — quarterly derivation
# MAGIC
# MAGIC Strategy:
# MAGIC 1. For each `(ticker, concept, fy, fp)`, dedupe by latest `filed` for each `period_shape`
# MAGIC 2. Pivot wide: get standalone Q value, YTD value at each fp, FY value, AND Q4 standalone if reported
# MAGIC 3. Derive standalone Q if missing: `YTD_now − YTD_prev`
# MAGIC 4. Q4: prefer standalone (10-K, ~90d); fallback to `FY − YTD_Q3`

# COMMAND ----------

flow = (
    raw
    .filter(F.col("kind").isin("flow_additive", "flow_nonadditive"))
    .filter(F.col("form").isin("10-K", "10-Q", "10-K/A", "10-Q/A"))
    .filter(F.col("value").isNotNull())
)

# For each (ticker, stmt, concept, fy, fp, period_shape) keep latest filed.
# NOTE: (fy, fp, period_shape) uniquely identifies what kind of fact this is.
# Q4 standalone (fp="FY" + period_shape="Q_standalone") is distinct from
# FY total (fp="FY" + period_shape="FY_or_TTM") thanks to period_shape,
# so this dedup window correctly preserves both as separate rows.
w = Window.partitionBy(
    "ticker", "stmt", "concept", "fy", "fp", "period_shape"
).orderBy(F.col("filed").desc_nulls_last())

flow_dedup = (
    flow
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# MAGIC %md ### 1a. Build wide table: one row per (ticker, stmt, concept, fy)
# MAGIC With columns for each available shape, including Q4 standalone when reported.

# COMMAND ----------

# We need, for each fy:
#   q1_std (90d in fp=Q1), q2_std, q3_std
#   q4_std (90d in fp=FY inside 10-K) — only present if the issuer publishes Q4 standalone
#   ytd_q1 (90d in fp=Q1 — same as q1_std actually), ytd_q2 (180d in Q2), ytd_q3 (270d in Q3)
#   fy_val (365d in fp=FY, form=10-K)
#   period_end at each fp (for tagging period_end on derived rows)
#
# Build a wide pivot manually since pivot in Spark is awkward with composite keys.

def pick(condition_col_val, condition_shape, value_col="value"):
    """Helper to pick value where fp=col_val AND period_shape=condition_shape."""
    return F.first(
        F.when(
            (F.col("fp") == condition_col_val) & (F.col("period_shape") == condition_shape),
            F.col(value_col)
        ),
        ignorenulls=True,
    )

# ── First pass: aggregate everything EXCEPT Q4 standalone ────────────────
# We need fy_end first, then we can filter Q4 standalone rows where
# period_end == fy_end (the only way to disambiguate Q4 from the Q1-Q3
# standalones that 10-Ks re-report with fp='FY').

agg_base = (
    flow_dedup
    .groupBy("ticker", "company", "stmt", "concept", "kind", "fy")
    .agg(
        # Standalone Q values (~90d) from 10-Q filings
        pick("Q1", "Q_standalone").alias("q1_std"),
        pick("Q2", "Q_standalone").alias("q2_std"),
        pick("Q3", "Q_standalone").alias("q3_std"),
        # YTD values
        pick("Q1", "Q_standalone").alias("ytd_q1"),   # YTD at Q1 = standalone Q1
        pick("Q2", "YTD_6M").alias("ytd_q2"),
        pick("Q3", "YTD_9M").alias("ytd_q3"),
        # FY value (from 10-K)
        F.first(
            F.when(
                (F.col("fp") == "FY")
                & (F.col("period_shape") == "FY_or_TTM")
                & (F.col("form").isin("10-K", "10-K/A")),
                F.col("value")
            ),
            ignorenulls=True,
        ).alias("fy_val"),
        # period_end for each
        pick("Q1", "Q_standalone", "period_end").alias("q1_end"),
        pick("Q2", "Q_standalone", "period_end").alias("q2_std_end"),
        pick("Q3", "Q_standalone", "period_end").alias("q3_std_end"),
        pick("Q2", "YTD_6M",      "period_end").alias("q2_ytd_end"),
        pick("Q3", "YTD_9M",      "period_end").alias("q3_ytd_end"),
        F.first(
            F.when(
                (F.col("fp") == "FY")
                & (F.col("period_shape") == "FY_or_TTM")
                & (F.col("form").isin("10-K", "10-K/A")),
                F.col("period_end")
            ),
            ignorenulls=True,
        ).alias("fy_end"),
    )
)

# ── Second pass: extract Q4 standalone using fy_end as anchor ────────────
# A 10-K re-reports ALL four quarter standalones (Q1, Q2, Q3, Q4 of that FY)
# with fp='FY' and period_shape='Q_standalone'. We can only tell which is
# the Q4 by matching period_end == fy_end.

q4_std_rows = (
    flow_dedup
    .filter(F.col("fp") == "FY")
    .filter(F.col("period_shape") == "Q_standalone")
    .filter(F.col("form").isin("10-K", "10-K/A"))
    .select(
        "ticker", "stmt", "concept", "fy",
        F.col("period_end").alias("q4_std_end"),
        F.col("value").alias("q4_std"),
        F.col("filed").alias("_q4_std_filed"),
    )
)

# Join with fy_end from agg_base; keep only rows where the standalone ends
# at the FY end. If multiple match (restatements with different filed dates),
# keep latest filed.
q4_std_pick = (
    q4_std_rows
    .join(
        agg_base.select("ticker", "stmt", "concept", "fy", "fy_end"),
        on=["ticker", "stmt", "concept", "fy"],
        how="inner",
    )
    .filter(F.col("q4_std_end") == F.col("fy_end"))
)

# Dedup by latest filed (in case of restatement)
w_q4 = Window.partitionBy("ticker", "stmt", "concept", "fy").orderBy(
    F.col("_q4_std_filed").desc_nulls_last()
)
q4_std_pick = (
    q4_std_pick
    .withColumn("rn", F.row_number().over(w_q4))
    .filter(F.col("rn") == 1)
    .select("ticker", "stmt", "concept", "fy", "q4_std", "q4_std_end")
)

# Merge q4_std back into agg
agg = (
    agg_base.join(
        q4_std_pick,
        on=["ticker", "stmt", "concept", "fy"],
        how="left",
    )
)

# COMMAND ----------

# MAGIC %md ### 1b. Derive Q1..Q4 standalone values
# MAGIC
# MAGIC - **`flow_additive`**: full derivation with fallback for all quarters.
# MAGIC - **`flow_nonadditive`**: only standalone values pass through (no YTD-diff fallback).
# MAGIC - **Q4 (both kinds)**: prefer standalone (10-K, ~90d) over derivation. For additive,
# MAGIC   `FY − YTD_Q3` is the fallback. For nonadditive, no fallback (NULL).

# COMMAND ----------

derived = agg.select(
    "ticker", "company", "stmt", "concept", "kind", "fy",

    # ─── Q1 ───
    # standalone if exists, else YTD_Q1 (which is ~same thing)
    F.coalesce(F.col("q1_std"), F.col("ytd_q1")).alias("q1_value"),
    F.col("q1_end").alias("q1_period_end"),
    (F.col("q1_std").isNull() & F.col("ytd_q1").isNotNull()).alias("q1_derived"),

    # ─── Q2 ───
    # standalone if exists (additive only), else YTD_Q2 − YTD_Q1 (additive only)
    F.when(
        F.col("kind") == "flow_additive",
        F.coalesce(F.col("q2_std"), F.col("ytd_q2") - F.col("ytd_q1"))
    ).otherwise(
        F.col("q2_std")  # nonadditive: only standalone
    ).alias("q2_value"),
    F.coalesce(F.col("q2_std_end"), F.col("q2_ytd_end")).alias("q2_period_end"),
    (F.col("kind") == "flow_additive").__and__(F.col("q2_std").isNull()).alias("q2_derived"),

    # ─── Q3 ───
    # standalone if exists (additive only), else YTD_Q3 − YTD_Q2 (additive only)
    F.when(
        F.col("kind") == "flow_additive",
        F.coalesce(F.col("q3_std"), F.col("ytd_q3") - F.col("ytd_q2"))
    ).otherwise(
        F.col("q3_std")
    ).alias("q3_value"),
    F.coalesce(F.col("q3_std_end"), F.col("q3_ytd_end")).alias("q3_period_end"),
    (F.col("kind") == "flow_additive").__and__(F.col("q3_std").isNull()).alias("q3_derived"),

    # ─── Q4 ───
    # Prefer standalone (10-K, ~90d) for BOTH kinds.
    # additive: if no standalone → fallback to FY − YTD_Q3 (derived)
    # nonadditive: if no standalone → NULL (cannot derive sensibly)
    F.when(
        F.col("q4_std").isNotNull(),
        F.col("q4_std")
    ).when(
        F.col("kind") == "flow_additive",
        F.col("fy_val") - F.col("ytd_q3")
    ).otherwise(F.lit(None).cast("double")).alias("q4_value"),

    # period_end for Q4: standalone end if used, else fy_end
    F.coalesce(F.col("q4_std_end"), F.col("fy_end")).alias("q4_period_end"),

    # is_derived for Q4: False if standalone was used, True if fallback (FY−YTD_Q3) was used
    F.when(F.col("q4_std").isNotNull(), F.lit(False))
     .otherwise(F.lit(True))
     .alias("q4_derived"),
)

# COMMAND ----------

# MAGIC %md ### 1b-bis. Cross-check Q4: standalone vs FY−YTD_Q3
# MAGIC
# MAGIC When both vías are available, compare and log discrepancies. We KEEP the standalone
# MAGIC (decision: trust what the issuer actually reported), but flag mismatches >0.1% so
# MAGIC we can investigate restatements or concept-tagging inconsistencies between 10-K and 10-Q.

# COMMAND ----------

q4_xcheck = (
    agg
    .filter(F.col("kind") == "flow_additive")
    .filter(F.col("q4_std").isNotNull())
    .filter(F.col("fy_val").isNotNull() & F.col("ytd_q3").isNotNull())
    .withColumn("q4_derived_val", F.col("fy_val") - F.col("ytd_q3"))
    .withColumn("diff_abs", F.abs(F.col("q4_std") - F.col("q4_derived_val")))
    .withColumn(
        "diff_pct",
        F.when(
            F.col("q4_derived_val") != 0,
            (F.col("q4_std") - F.col("q4_derived_val")) / F.abs(F.col("q4_derived_val")) * 100
        ).otherwise(F.lit(None).cast("double"))
    )
    .select(
        "ticker", "stmt", "concept", "fy",
        F.col("q4_std").alias("q4_standalone"),
        F.col("q4_derived_val").alias("q4_via_fy_minus_ytd_q3"),
        "diff_abs", "diff_pct",
    )
)

n_xcheck   = q4_xcheck.count()
n_mismatch = q4_xcheck.filter(F.abs(F.col("diff_pct")) > 0.1).count()
print(f"Q4 cross-check: {n_xcheck:,} (ticker, concept, fy) tuples have both vías")
print(f"  → {n_mismatch:,} with |diff| > 0.1%  (preferring standalone in all cases)")

if n_mismatch > 0:
    print("\nTop 20 worst mismatches (standalone is kept; this is informational):")
    q4_xcheck.filter(F.abs(F.col("diff_pct")) > 0.1) \
             .orderBy(F.abs(F.col("diff_pct")).desc()) \
             .limit(20).display()

# COMMAND ----------

# MAGIC %md ### 1c. Unpivot to long format

# COMMAND ----------

def unpivot_quarter(df, q_num):
    """Extract one quarter's rows from the wide-form derived df."""
    return df.select(
        "ticker", "company", "stmt", "concept",
        F.col("fy").alias("fiscal_year"),
        F.lit(f"Q{q_num}").alias("period_type"),
        F.col(f"q{q_num}_period_end").alias("period_end"),
        F.col(f"q{q_num}_value").alias("value"),
        F.col(f"q{q_num}_derived").alias("is_derived"),
    ).filter(F.col("value").isNotNull() & F.col("period_end").isNotNull())

flow_quarterly = (
    unpivot_quarter(derived, 1)
    .unionByName(unpivot_quarter(derived, 2))
    .unionByName(unpivot_quarter(derived, 3))
    .unionByName(unpivot_quarter(derived, 4))
)

flow_quarterly = flow_quarterly.withColumn("scraped_at", F.lit(latest_scrape).cast("timestamp"))

print(f"Flow quarterly rows derived: {flow_quarterly.count():,}")

# COMMAND ----------

# MAGIC %md ## 2. STOCK concepts — snapshot-based quarterly
# MAGIC
# MAGIC SEC re-reports prior-period BS snapshots in later 10-Qs as comparatives.
# MAGIC We dedupe by `(ticker, concept, period_end)` keeping `MAX(filed)`.
# MAGIC
# MAGIC Then assign `period_type` based on `fp` from the filing it appears in.
# MAGIC The FY snapshot from the 10-K (already in clean table via `21__clean_and_merge`)
# MAGIC is excluded here.

# COMMAND ----------

stock = (
    raw
    .filter(F.col("kind") == "stock")
    .filter(F.col("form").isin("10-Q", "10-Q/A"))   # Q snapshots only; FY snapshots come from 10-K via 21
    .filter(F.col("fp").isin("Q1", "Q2", "Q3"))
    .filter(F.col("value").isNotNull())
)

# Dedup: (ticker, concept, period_end) → MAX(filed)
w_stock = Window.partitionBy("ticker", "stmt", "concept", "period_end").orderBy(
    F.col("filed").desc_nulls_last()
)

stock_dedup = (
    stock
    .withColumn("rn", F.row_number().over(w_stock))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stock_quarterly = stock_dedup.select(
    F.col("ticker"),
    F.col("company"),
    F.col("stmt"),
    F.col("concept"),
    F.col("fy").alias("fiscal_year"),
    F.col("fp").alias("period_type"),
    F.col("period_end"),
    F.col("value"),
    F.lit(False).alias("is_derived"),
    F.lit(latest_scrape).cast("timestamp").alias("scraped_at"),
)

print(f"Stock quarterly rows: {stock_quarterly.count():,}")

# COMMAND ----------

# MAGIC %md ## 3. Combine flow + stock → MERGE into clean table

# COMMAND ----------

all_quarterly = (
    flow_quarterly
    .withColumn("company", F.initcap(F.col("company")) if False else F.col("company"))
    .unionByName(stock_quarterly)
    .withColumn("company", F.initcap(F.col("company")))
)

# Filter: drop rows where fiscal_year is null (shouldn't happen, defensive)
all_quarterly = all_quarterly.filter(F.col("fiscal_year").isNotNull())

print(f"Total quarterly rows to merge: {all_quarterly.count():,}")
all_quarterly.createOrReplaceTempView("incoming_quarterly")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {full_tbl} AS target
    USING incoming_quarterly AS source
    ON  target.ticker      = source.ticker
    AND target.stmt        = source.stmt
    AND target.concept     = source.concept
    AND target.fiscal_year = source.fiscal_year
    AND target.period_type = source.period_type

    WHEN MATCHED AND (target.value != source.value
                   OR target.period_end != source.period_end
                   OR target.is_derived != source.is_derived) THEN
        UPDATE SET
            target.value      = source.value,
            target.period_end = source.period_end,
            target.is_derived = source.is_derived,
            target.company    = source.company,
            target.scraped_at = source.scraped_at

    WHEN NOT MATCHED THEN
        INSERT (ticker, company, stmt, concept, fiscal_year, period_type,
                period_end, value, is_derived, scraped_at)
        VALUES (source.ticker, source.company, source.stmt, source.concept,
                source.fiscal_year, source.period_type, source.period_end,
                source.value, source.is_derived, source.scraped_at)
""")

print(f"✓ MERGE complete → {full_tbl} (quarterly rows)")

# COMMAND ----------

# MAGIC %md ## 4. Sanity check — Σ Q1..Q4 ≈ FY for flow_additive

# COMMAND ----------

spark.sql(f"""
    WITH q AS (
        SELECT ticker, concept, fiscal_year,
               SUM(CASE WHEN period_type IN ('Q1','Q2','Q3','Q4') THEN value END) AS quarterly_sum,
               MAX(CASE WHEN period_type = 'FY' THEN value END)                    AS fy_value
        FROM {full_tbl}
        WHERE stmt IN ('Income Statement', 'Cash Flow')
        GROUP BY ticker, concept, fiscal_year
        HAVING COUNT(DISTINCT period_type) = 5    -- has all 5: FY + Q1..Q4
    )
    SELECT
        ticker,
        concept,
        fiscal_year,
        ROUND(quarterly_sum / 1e6, 2)  AS quarterly_sum_mn,
        ROUND(fy_value      / 1e6, 2)  AS fy_value_mn,
        ROUND((quarterly_sum - fy_value) / NULLIF(fy_value, 0) * 100, 4) AS pct_diff
    FROM q
    WHERE ABS((quarterly_sum - fy_value) / NULLIF(fy_value, 0)) > 0.001  -- >0.1% mismatch
    ORDER BY ABS(pct_diff) DESC
    LIMIT 50
""").display()
