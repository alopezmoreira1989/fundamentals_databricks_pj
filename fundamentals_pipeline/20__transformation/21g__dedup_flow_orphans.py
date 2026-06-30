# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 21g__dedup_flow_orphans
# MAGIC
# MAGIC **RECURRING pipeline step** (the Income-Statement / Cash-Flow analogue of `21f__dedup_balance_sheet`).
# MAGIC
# MAGIC Enforces one row per **`(ticker, stmt, concept, period_end, period_type)`** for flow
# MAGIC statements. The `MERGE` in `21__clean_and_merge` / `21b__derive_quarterly` upserts on
# MAGIC `(ticker, stmt, concept, fiscal_year, period_type)` and **never deletes**, so when a scrape
# MAGIC relabels a `period_end`'s `fiscal_year` (the residual `+1yr` shift — a 10-K/10-Q re-reports
# MAGIC the prior period as a comparative under the filing's `fy`), the corrected scrape INSERTS a new
# MAGIC row under a different key instead of overwriting → the old one becomes a permanent orphan. The
# MAGIC same economic fact (one `period_end`, one `period_type`) then exists under two `fiscal_year`
# MAGIC values. `21b`'s FY-anchor current-year guard stops NEW duplicates being generated; this step is
# MAGIC the structural cleanup that collapses the stale ones already accumulated (and any straggler).
# MAGIC
# MAGIC **Why not the Balance-Sheet rule.** `21f` collapses BS by `period_end` (a balance at a date is
# MAGIC one snapshot, period_type-agnostic). Flows are the opposite: the SAME `period_end` legitimately
# MAGIC carries different `period_type` values (Q4 and FY both end at the fiscal close; a YTD and a
# MAGIC standalone can share an end date across labels) — so the flow key MUST include `period_type`.
# MAGIC
# MAGIC **Survivor rule** per `(ticker, stmt, concept, period_end, period_type)` — IS / CF only:
# MAGIC 1. `fiscal_year == implied_fy` first. `implied_fy` = the fiscal year whose CLOSE is the nearest
# MAGIC    on-or-after this `period_end`, read from the issuer's OWN reported-FY calendar (the
# MAGIC    `period_type='FY'` rows in `financials`). A quarter belongs to exactly one fiscal year — the
# MAGIC    one whose Q4/close it rolls up into — so this is the provably-correct label, independent of
# MAGIC    the companyfacts `fy` that produced the `+1yr` shift. We only ever USE it to pick which
# MAGIC    EXISTING row survives; we never relabel a surviving row's `fiscal_year`.
# MAGIC 2. `scraped_at` desc — after a `force_full_refresh` the freshly re-derived (guarded) row is
# MAGIC    newest; resolves groups where neither row matches `implied_fy` (issuer FY-calendar gap).
# MAGIC 3. `is_derived` asc — prefer the SEC-reported value over a derived one.
# MAGIC 4. `value` desc — deterministic final tiebreak.
# MAGIC
# MAGIC **Scope & safety.** Touches ONLY Income Statement / Cash Flow rows; Balance Sheet (already
# MAGIC handled by `21f`) and everything else pass through byte-for-byte. The survivor rule only DELETES
# MAGIC the losing rows of a genuine duplicate group — a singleton row (the overwhelming majority) is
# MAGIC always its own survivor and is never relabelled or dropped. **Idempotent:** if no flow
# MAGIC duplicates exist, nothing is rewritten. Must run AFTER all flow writers (`21`, `21b`, `21e`)
# MAGIC and after `21f`, before metrics.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
staging  = f"{CATALOG}.{SCHEMA}.{TABLE}_flowdedup_staging"

# True key of a flow fact: one value per (period_end, period_type).
FLOW_KEY    = ["ticker", "stmt", "concept", "period_end", "period_type"]
FLOW_STMTS  = ("Income Statement", "Cash Flow")
FLOW_PTYPES = ("Q1", "Q2", "Q3", "Q4", "FY")

# COMMAND ----------

# MAGIC %md ## 1. Are there flow duplicates on (ticker, stmt, concept, period_end, period_type)?

# COMMAND ----------

flow_dups = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups, COALESCE(SUM(n - 1), 0) AS extra_rows
    FROM (
        SELECT {', '.join(FLOW_KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        WHERE stmt IN {FLOW_STMTS}
          AND period_type IN {FLOW_PTYPES}
        GROUP BY {', '.join(FLOW_KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]

total_before = spark.table(full_tbl).count()
print(f"Total rows                     : {total_before:,}")
print(f"Duplicate flow groups (key)    : {flow_dups['dup_groups']:,}")
print(f"Extra flow rows to delete      : {flow_dups['extra_rows']:,}")

# COMMAND ----------

# MAGIC %md ## 2. Collapse — keep one row per flow key (survivor rule)

# COMMAND ----------

if flow_dups["dup_groups"] > 0:
    fin = spark.table(full_tbl)

    # Issuer's own fiscal-close calendar: one close (max period_end) per reported fy.
    fy_close = (
        fin.filter(F.col("period_type") == "FY")
           .groupBy("ticker", "fiscal_year")
           .agg(F.max("period_end").alias("fy_end"))
    )

    # implied_fy per distinct (ticker, period_end): the smallest fy whose close is on-or-after
    # the period_end (the fiscal year this period rolls up into). Computed on the DISTINCT
    # (ticker, period_end) universe and joined back, so the non-equi join stays small.
    flow = (
        fin.filter(F.col("stmt").isin(*FLOW_STMTS))
           .filter(F.col("period_type").isin(*FLOW_PTYPES))
    )
    pe = flow.select("ticker", "period_end").distinct().alias("pe")
    fc = fy_close.alias("fc")
    implied = (
        pe.join(
            fc,
            (F.col("pe.ticker") == F.col("fc.ticker")) & (F.col("fc.fy_end") >= F.col("pe.period_end")),
            how="left",
        )
        .groupBy(F.col("pe.ticker").alias("ticker"), F.col("pe.period_end").alias("period_end"))
        .agg(F.min(F.col("fc.fiscal_year")).alias("implied_fy"))
    )

    flow = flow.join(implied, on=["ticker", "period_end"], how="left")

    w = Window.partitionBy(*FLOW_KEY).orderBy(
        (F.col("fiscal_year") == F.col("implied_fy")).desc_nulls_last(),  # provably-correct fy first
        F.col("scraped_at").desc_nulls_last(),                            # most recent (post-fix) run
        F.col("is_derived").asc_nulls_last(),                             # reported > derived
        F.col("value").desc_nulls_last(),                                 # deterministic tiebreak
    )

    flow_deduped = (
        flow.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn", "implied_fy")
    )

    # stmt is NOT NULL. A row is a flow-dedup candidate iff (stmt in FLOW_STMTS AND period_type in
    # FLOW_PTYPES); everything else (Balance Sheet, plus any flow row with an out-of-set period_type)
    # passes through untouched.
    passthrough = fin.filter(
        ~(F.col("stmt").isin(*FLOW_STMTS) & F.col("period_type").isin(*FLOW_PTYPES))
    )
    result = passthrough.unionByName(flow_deduped)

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
    spark.table(staging).select(*target_cols).createOrReplaceTempView("incoming_flowdedup")
    spark.sql(f"INSERT OVERWRITE TABLE {full_tbl} SELECT {', '.join(target_cols)} FROM incoming_flowdedup")
    spark.sql(f"DROP TABLE IF EXISTS {staging}")
    print(f"✓ INSERT OVERWRITE complete → {full_tbl}")
else:
    print("✓ 0 flow duplicates by key — nothing to collapse (idempotent).")

# COMMAND ----------

# MAGIC %md ## 3. Verification — 0 flow duplicates per (ticker, stmt, concept, period_end, period_type)

# COMMAND ----------

dup_after = spark.sql(f"""
    SELECT COUNT(*) AS dup_groups
    FROM (
        SELECT {', '.join(FLOW_KEY)}, COUNT(*) AS n
        FROM {full_tbl}
        WHERE stmt IN {FLOW_STMTS}
          AND period_type IN {FLOW_PTYPES}
        GROUP BY {', '.join(FLOW_KEY)}
        HAVING COUNT(*) > 1
    )
""").collect()[0]["dup_groups"]

total_after = spark.table(full_tbl).count()
print(f"Total rows           : {total_after:,}")
print(f"Duplicate flow groups: {dup_after:,}  (expected 0)")
assert dup_after == 0, "Flow duplicates by key still remain — review the dedup window."
print("✓ Income Statement / Cash Flow unique by (ticker, stmt, concept, period_end, period_type)")
