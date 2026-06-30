# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 21_clean_and_merge
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

# MAGIC %run "../00__config/01__tickers"

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
# `raw` (the slice of the latest financials_raw scrape, ~81M rows) is re-scanned 3×: flow_fy,
# stock_fy, and the _flow_fy_all for the orphan-DELETE in §4, each with its own predicate pushdown
# over the full Delta table. localCheckpoint(eager) materializes it once and downstream filters
# read from the on-disk local checkpoint instead of re-scanning the table. .cache()/.persist()
# do NOT work on serverless ([NOT_SUPPORTED_WITH_SERVERLESS]); released when the session closes.
raw = raw.localCheckpoint(eager=True)

# Flow FY rows: 10-K, fp='FY'. We do NOT use the strict period_shape='FY_or_TTM' filter
# (it would drop transition/stub years with duration outside 350–380d — e.g. after an
# MLP→C-corp conversion or a fiscal year-end change — which the classifier labels
# `other_Nd`). We DO exclude the SUB-ANNUAL shapes that a 10-K re-reports with
# fp='FY': the quarterly breakdown of the year (Q1..Q4 standalone, ~90d) and the 6M/9M YTDs.
# The Q4 standalone (`Q_standalone`, ~91d, period_end == fy_end) is indistinguishable from the
# FY row by (fp, period_end, filed) → it tied in the dedup Window until `value desc`, which
# picked the LARGEST value (often the Q4, not the annual) and corrupted the FY (~9k rows, e.g.
# ALNY fy2019: −276.19 from Q4 instead of −886.12 for the year). Excluding them, only annual
# shapes compete (`FY_or_TTM` 365d + `other_Nd` stubs). Snapshot excluded as a safeguard.
# Edge case: a fy whose ONLY fact fp='FY' is sub-annual no longer produces an FY row here (it
# is deleted as an orphan below); `21e__derive_fy_from_quarterly` reconstructs the annual from ΣQ
# for flow_additive concepts that have all 4 quarters.
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
# We add duration_days = NULL so that the unionByName with flow_fy aligns the schema.
# For stocks there is no duration criterion (all are snapshots), so the dedup window
# falls through to the next criterion (filed desc) — identical behaviour to before.
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

# Statement-aware synonym priority (lower = preferred), computed BEFORE the rename from the
# SOURCE label. Net Income coexists as NetIncomeLoss + ProfitLoss + (to common) in the same fy,
# so the tiebreak cannot be `value desc`. The global CONCEPT_PRIORITY forces attributable-first
# (correct for the Income Statement); CONCEPT_PRIORITY_BY_STMT then INVERTS the Net Income order
# for `Cash Flow` so the consolidated ProfitLoss — the indirect-method reconciliation start —
# wins there (NCI-heavy filers like VNOM). Mirrors concept_priority() in 01__tickers; unlisted
# labels → 0 (previous Revenue behaviour preserved). Apply the global map first, then the
# statement-scoped overrides (added last → outermost F.when → take precedence for matching pairs).
_prio = F.lit(0)
for _label, _rank in CONCEPT_PRIORITY.items():
    _prio = F.when(F.col("concept") == _label, F.lit(_rank)).otherwise(_prio)
for _stmt, _over in CONCEPT_PRIORITY_BY_STMT.items():
    for _label, _rank in _over.items():
        _prio = F.when(
            (F.col("stmt") == _stmt) & (F.col("concept") == _label), F.lit(_rank)
        ).otherwise(_prio)
incoming = incoming.withColumn("prio", _prio)

# Normalise: collapse XBRL synonyms to the canonical concept via CONCEPT_SYNONYMS
# (inherited from the %run of 01__tickers). If both report the same (ticker, stmt, fy),
# the downstream dedup keeps one — prio asc, latest filed, largest value.
for _alt, _canon in CONCEPT_SYNONYMS.items():
    incoming = incoming.withColumn(
        "concept",
        F.when(F.col("concept") == _alt, _canon).otherwise(F.col("concept"))
    )

# Drop mislabelled comparatives: a current-year fact ALWAYS ends in a calendar year
# >= its fiscal_year (Dec-closers = fy year; Jan-closers close in fy+1).
# A period_end whose year < fy is a comparative from a later filing labelled with
# that filing's fy — never a genuine current-year figure (no issuer in the universe
# labels its fiscal year AHEAD; verified via modal offset per ticker). Without this,
# a fy whose only available fact is a comparative produces a misaligned row that the
# MERGE never deletes and that accumulates.
incoming = incoming.filter(F.year(F.col("period_end")) >= F.col("fy"))

# Dedup: keep the CURRENT-YEAR fact per (ticker, stmt, concept, fy).
# A 10-K labels prior-year comparatives with the FILING's `fy`, so a fy partition
# contains multiple FY facts; the current year has the MOST RECENT `period_end`
# (comparatives end earlier). Ordering by duration was INCORRECT — a 53-week (370d)
# or leap-year (366d) comparative beat the current 52-week/365d year, pulling in the
# wrong value and a period_end whose year ≠ fiscal_year. Same MAX(period_end) rule
# as 21b. `filed` desc resolves restatements; `value` desc is the final tiebreak.
# For stocks (BS) period_end desc also picks the current-year snapshot (previously
# fell through to `value` desc and could pick the larger comparative snapshot).
# `prio asc` goes AFTER period_end (pick current year vs comparative) but BEFORE
# value: among tags that coexist in the same fy/period_end the preferred one wins.
# The ANNUAL shape wins BEFORE any value tiebreak: if after filtering sub-annual shapes
# a `FY_or_TTM` and an `other_Nd` stub coexist at the same period_end, we prefer the
# 365d annual and, failing that, the longest duration — so `value desc`
# (last deterministic resort) NEVER again picks a shorter shape just because it is larger.
w = Window.partitionBy("ticker", "stmt", "concept", "fy").orderBy(
    F.col("period_end").desc_nulls_last(),
    (F.col("period_shape") == "FY_or_TTM").desc(),   # prefer the true annual shape
    F.col("prio").asc_nulls_last(),
    F.col("filed").desc_nulls_last(),
    F.col("duration_days").desc_nulls_last(),         # then the longest period (annual > stub)
    F.col("value").desc_nulls_last()                  # final deterministic tiebreak
)
incoming = (
    incoming
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "duration_days", "prio")
    .withColumn("company", F.initcap(F.col("company")))
)

# Guard: a period_end belongs to EXACTLY one fiscal year. After dedup by fy,
# the same period_end can still appear under multiple fy values (a recent 10-K brings
# the current year as current and prior years as comparatives, each with its own fy
# from its original filing). We keep the smallest fy (the current-year filing)
# and discard the rest, so a comparative cannot masquerade as a later year.
# The MERGE never deletes, so without this cross-fy duplicates accumulate in `financials`.
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

# clean_fy is consumed 3× downstream: count() here, the MERGE in §3, and `_kept_keys` for
# the orphan-DELETE in §4. Its lineage is expensive (scan of `raw` + union + synonym-loops
# + two dedup window functions) — without materializing it is fully recomputed each time.
# localCheckpoint(eager) computes it ONCE and truncates the lineage. NOTE serverless:
# .cache()/.persist() do NOT work ([NOT_SUPPORTED_WITH_SERVERLESS]); localCheckpoint does.
# Released when the session closes.
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

# MAGIC %md ## 4. Delete orphan FLOW FY rows (annual fabricated from a sub-annual shape)
# MAGIC
# MAGIC The MERGE above does UPDATE/INSERT but **never deletes**. Previously, a sub-annual shape with
# MAGIC `fp='FY'` (Q4 standalone ~90d, or YTD 6M/9M) won the `value desc` tiebreak and was written
# MAGIC as the `FY` row (`is_derived=False`) for that `(ticker, stmt, concept, fy)`. With the new
# MAGIC `flow_fy` filter, years whose ONLY fact `fp='FY'` is sub-annual (or a mislabelled comparative
# MAGIC that is dropped by the `year(period_end) >= fy` guard) NO LONGER produce an FY row, so the
# MAGIC old one becomes stale and orphaned. We delete it here (~2.5k rows). `21e__derive_fy_from_quarterly`
# MAGIC (later in the pipeline) reconstructs the annual from `ΣQ1..Q4` for `flow_additive` concepts
# MAGIC that have all 4 quarters present. **Idempotent:** only touches `is_derived=False`; reconstructed
# MAGIC FY rows (`is_derived=True`) and genuine FY rows are never deleted. Scoped to the key universe
# MAGIC of the current scrape → never touches historical tickers/concepts absent from the scrape.

# COMMAND ----------

# Universe of FLOW keys with ANY fact fp='FY' in this scrape, collapsed to the canonical concept
# (to match against `financials`). Includes the sub-annual shapes that `flow_fy` now excludes.
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

# Keys that the patched 21 DOES (re)emit = they have a genuine annual (FY_or_TTM / other_Nd for
# the current year). The rest of the flow universe → their published FY is a fabrication to delete.
_kept_keys = clean_fy.select("ticker", "stmt", "concept", "fiscal_year").distinct()

orphan_keys = _all_keys.join(
    _kept_keys, on=["ticker", "stmt", "concept", "fiscal_year"], how="left_anti"
)
orphan_keys.createOrReplaceTempView("orphan_fy_keys")

n_orphan = orphan_keys.count()
print(f"Orphan flow FY rows to delete (fabricated annual / no genuine annual): {n_orphan:,}")

# Targeted DELETE via MERGE; only reported FY rows (is_derived=False), never derived/genuine ones.
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
print(f"✓ Orphan FY DELETE complete → {full_tbl}")

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
