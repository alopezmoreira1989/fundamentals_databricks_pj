# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 21e__derive_fy_from_quarterly
# MAGIC
# MAGIC **Last-resort fallback:** synthesizes an `FY` row for `flow_additive` concepts
# MAGIC (Income Statement / Cash Flow) **only when a real FY does not already exist**, by
# MAGIC summing the four quarters `Q1+Q2+Q3+Q4` that `21b__derive_quarterly` left in
# MAGIC `financials`. The row is flagged `is_derived=True`.
# MAGIC
# MAGIC **Why it exists:** some filers do not expose the undimensioned annual total in their
# MAGIC 10-K (SEC companyfacts drops dimensional facts), so `21` finds no FY. If all four
# MAGIC quarters are present, `ΣQ` reconstructs the year.
# MAGIC
# MAGIC **Actual coverage today: low.** The affected set typically lacks **Q4** (Q4 is itself
# MAGIC derived from `FY − YTD_Q3`, which needs the annual — circular), so this stage fires
# MAGIC for very few tickers. It runs anyway for robustness and to cover future cases. The
# MAGIC final `print` reports how many FY rows were synthesized.
# MAGIC
# MAGIC **Guards (do not break existing logic):**
# MAGIC - **Never** overwrites a real FY → `MERGE … WHEN NOT MATCHED` (insert) and a
# MAGIC   `WHEN MATCHED AND target.is_derived = true` that only refreshes already-derived rows.
# MAGIC - Only `flow_additive`. **Not** `flow_nonadditive` (EPS, shares — not additive) nor
# MAGIC   `stock` (balance sheet is not additive; also the clean table does not store a
# MAGIC   year-end snapshot for gap cases — `21b` only emits Q1–Q3 snapshots).
# MAGIC - **Does not** recalculate Q4 (that is `21b`'s responsibility); only creates the
# MAGIC   missing FY row from already-present quarters → no double-counting.
# MAGIC
# MAGIC Idempotent: re-running is safe.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"
fin = spark.table(full_tbl)

# COMMAND ----------

# MAGIC %md ## 1. flow_additive concepts (from STATEMENTS)
# MAGIC
# MAGIC Only these are summable Q1..Q4 = FY. The clean table uses the canonical label, which
# MAGIC matches the STATEMENTS keys; synonym labels do not appear in `financials`
# MAGIC (already collapsed by `21`/`21b`), so the join simply does not match them.

# COMMAND ----------

_flow_pairs = [
    (stmt, label)
    for stmt, cmap in STATEMENTS.items()
    for label, (xbrl, kind) in cmap.items()
    if kind == "flow_additive"
]
flow_concepts = spark.createDataFrame(_flow_pairs, ["stmt", "concept"]).distinct()

# COMMAND ----------

# MAGIC %md ## 2. Σ Q1..Q4 per (ticker, stmt, concept, fiscal_year) — requires all 4 quarters

# COMMAND ----------

q = (
    fin.filter(F.col("period_type").isin("Q1", "Q2", "Q3", "Q4"))
       .join(flow_concepts, on=["stmt", "concept"], how="inner")
)

def _qval(qn):  # quarter value (NULL if absent)
    return F.max(F.when(F.col("period_type") == qn, F.col("value"))).alias(f"v_{qn}")

agg = (
    q.groupBy("ticker", "company", "stmt", "concept", "fiscal_year")
     .agg(
        _qval("Q1"), _qval("Q2"), _qval("Q3"), _qval("Q4"),
        # period_end of the synthetic FY = Q4 close (end of fiscal year)
        F.max(F.when(F.col("period_type") == "Q4", F.col("period_end"))).alias("q4_end"),
     )
)

# Only years with ALL FOUR quarters present (and a valid Q4 period_end)
synth = (
    agg.filter(
        F.col("v_Q1").isNotNull() & F.col("v_Q2").isNotNull()
        & F.col("v_Q3").isNotNull() & F.col("v_Q4").isNotNull()
        & F.col("q4_end").isNotNull()
    )
    .withColumn("value", F.col("v_Q1") + F.col("v_Q2") + F.col("v_Q3") + F.col("v_Q4"))
)

# COMMAND ----------

# MAGIC %md ## 3. Keep only (ticker, stmt, concept, fy) that do NOT already have a real FY

# COMMAND ----------

existing_fy = (
    fin.filter(F.col("period_type") == "FY")
       .filter(~F.coalesce(F.col("is_derived"), F.lit(False)))   # real FY (not derived)
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
print(f"FY synthesized from Σ Q1..Q4 (concepts without a real FY): {n_synth:,}")
if n_synth:
    print("Muestra:")
    clean_fy.select("ticker", "stmt", "concept", "fiscal_year", "value").orderBy("ticker", "concept").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. MERGE — insert synthetic FY rows (never touch a real FY)

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

    -- Only refreshes rows ALREADY derived by this stage (never a real reported FY).
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

print(f"✓ MERGE complete → {full_tbl} (synthetic FY from quarters)")
