# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 21h__shares_diluted_plausibility
# MAGIC
# MAGIC A confirmed, real-world XBRL failure mode (found 2026-07 via a live NCAV/Share bug
# MAGIC report — Badger Meter's Net-Net Finder and Valuation pages showed wildly different,
# MAGIC opposite-signed numbers): a filer's own submission software tags
# MAGIC `WeightedAverageNumberOfDilutedSharesOutstanding` ("Shares Diluted") **"in thousands"
# MAGIC without rescaling it back to raw share units**, sometimes for a contiguous run of
# MAGIC periods while OTHER periods from the SAME filer are correctly scaled, sometimes for
# MAGIC that filer's ENTIRE reported history. SEC's `companyfacts` API reproduces whatever was
# MAGIC actually tagged — it does not retroactively correct filer-side errors.
# MAGIC
# MAGIC This corrupts every downstream metric that divides by Shares Diluted for the affected
# MAGIC periods — NCAV/Share, Market Cap (= price × shares), P/E, P/B, and more.
# MAGIC
# MAGIC **Must run here — AFTER `21b__derive_quarterly`/`21e`/`21f`/`21g` have all finished
# MAGIC writing quarterly + dedup data, BEFORE `22__derived_metrics` consumes Shares Diluted —
# MAGIC not inside `21__clean_and_merge` itself.** `21__clean_and_merge` only ever writes FY
# MAGIC rows; a version of this guard that lived there (broadened to scan all `period_type`s)
# MAGIC was found, via a real pipeline run, to silently see zero quarterly candidates, because
# MAGIC no quarterly rows exist in `financials` yet at that point in the pipeline. Confirmed
# MAGIC concretely: MCD/BRKR/SYNA/UCTT's bad quarters (e.g. MCD's Q1 2024 Shares Diluted tagged
# MAGIC `725.9` instead of `725,900,000`) survived a full pipeline run untouched when this guard
# MAGIC was placed in `21__clean_and_merge` — moving it here (after quarterly data actually
# MAGIC exists) is the fix.
# MAGIC
# MAGIC Two independent checks, run in this specific order:
# MAGIC
# MAGIC **1. Absolute plausibility check (runs FIRST).** Catches a ticker whose Shares Diluted
# MAGIC is wrong in EVERY reported period (found 2026-07: FULC, IOVA, NUVB, CLDX, GOGO, NTNX,
# MAGIC LOAR, WOOF, EVER, ACAD, TEM — every FY row wrong ~1000x, so there is no genuinely correct
# MAGIC period anywhere in the ticker's own history for a neighbor-relative check to compare
# MAGIC against) — plus isolated bad QUARTERS in otherwise-correct tickers (MCD, BRKR, SYNA,
# MAGIC UCTT) that silently poisoned `22__derived_metrics.py`'s Market Cap as-of carry-forward
# MAGIC join (e.g. MCD FY2024 Market Cap computed as $208,721 instead of ~$200B). Independent,
# MAGIC absolute signal, no neighboring period needed: a row's own
# MAGIC `Total Current Assets / Shares Diluted` ("assets per share") compared against that
# MAGIC ticker's own real traded price. Flagged when ALL of:
# MAGIC   - `Shares Diluted < 2,000,000` — a real US-listed common stock this small is a rare,
# MAGIC     famous exception (Seaboard Corp ~964K-971K, Daily Journal ~1.38M), not the normal
# MAGIC     case
# MAGIC   - `Total Current Assets > 20,000,000` — only companies with real scale; a genuine
# MAGIC     tiny micro-cap with both a tiny float and tiny assets is self-consistent, not flagged
# MAGIC   - `(Total Current Assets / Shares Diluted) / price > 10` — the discriminator that
# MAGIC     separates bugs from genuine low float: Seaboard/Daily Journal land at ~0.7-0.8x
# MAGIC     (their unusually high real price already explains the low share count), while every
# MAGIC     confirmed bug lands at 12x-1800x+. Verified against real published data before
# MAGIC     shipping.
# MAGIC
# MAGIC **2. Neighbor-based iterative check (runs SECOND, after check 1's deletions).** The
# MAGIC original guard for a contiguous bad run sandwiched between correct periods (BMI's own
# MAGIC case: 7 straight wrong FY years, FY2018-2024, between correct FY2017 and FY2025) —
# MAGIC scans ALL `period_type`s (FY, Q1-Q4, TTM), ordered by `period_end` instead of
# MAGIC `fiscal_year` (quarters and FY dates interleave on one timeline per ticker), with FY
# MAGIC preferred on a same-date tie. Treats "the current best guess at which periods are
# MAGIC trustworthy" as a pool, evaluates every period against its nearest POOL neighbor IN TIME
# MAGIC (via last/first-non-null carried across an ordered window — no self-join needed),
# MAGIC removes newly-implausible periods from the pool, and repeats to a fixed point (this
# MAGIC round's flagged set exactly matches the previous round's, not a monotonically-growing
# MAGIC accumulation).
# MAGIC
# MAGIC **Also runs against `Shares Outstanding (Cover Page)` (`dei:EntityCommonStockSharesOutstanding`,
# MAGIC added 2026-07 for the "live" Market Cap / NCAV features — see 00__config/01__tickers.py).**
# MAGIC A universe-wide scan comparing cover-page shares against Shares Diluted (triggered by a
# MAGIC GENI market-cap bug report) found the SAME isolated single-quarter scale-error failure
# MAGIC mode already fixed above for Shares Diluted — confirmed real cases: ADV, AGL, PTCT, BKNG,
# MAGIC MCHB. The check is identical in mechanism (same ratio thresholds, same iterative pool
# MAGIC convergence) — only the concept being scanned differs — so it is implemented as one
# MAGIC shared function, `_run_neighbor_plausibility_check(concept, error_type)`, called once per
# MAGIC concept below. Check 1 (the absolute TCA-based check) is NOT extended to cover-page
# MAGIC shares — it is specific to Shares Diluted's own "assets per share vs. traded price"
# MAGIC signal, and this scope was deliberately left for a later pass.
# MAGIC
# MAGIC **Why check 1 must run before check 2, not after or merged into one pass (found via
# MAGIC pre-implementation simulation against real data, not just reasoned about):** a ticker
# MAGIC with SEVERAL consecutive wrong periods (e.g. ACAD's wrong FY2022/FY2023 AND wrong Q2/Q3
# MAGIC 2023) has those wrong periods mutually validate each other in check 2's neighbor
# MAGIC comparison — both wrong by the same factor, so their ratio to each other looks perfectly
# MAGIC normal — and check 2's preference for comparing against the PREVIOUS still-trusted
# MAGIC period then never looks past them to find a genuinely correct reference. Left unfixed,
# MAGIC check 2 would instead wrongly flag the correctly-scaled LATER quarters as the "outlier",
# MAGIC actively deleting real, correct data. Removing the entire wrong cluster in check 1 first
# MAGIC (it needs no "good" reference period at all) avoids this.
# MAGIC
# MAGIC Flagged rows are DELETED from `financials` (never "corrected" by guessing a scale factor
# MAGIC — this repo's convention is always "real gap reads NULL/absent", never a guessed fix)
# MAGIC and logged to `ingestion_failures`. `financials_raw` (the audit table) is never touched —
# MAGIC it stays a true, unfiltered record of what was actually reported, warts and all.
# MAGIC
# MAGIC **Idempotent:** re-running with no new implausible values is a no-op.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# MAGIC %md ## Shared DELETE + ingestion_failures logging

# COMMAND ----------

_sd_fail_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {_sd_fail_tbl} (
        ticker         STRING    NOT NULL,
        error_type     STRING    NOT NULL,
        error_message  STRING,
        step           STRING    NOT NULL,
        scraped_at     TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")
_sd_fail_schema = StructType([
    StructField("ticker",        StringType(),    False),
    StructField("error_type",    StringType(),    False),
    StructField("error_message", StringType(),    True),
    StructField("step",          StringType(),    False),
    StructField("scraped_at",    TimestampType(), False),
])


def _delete_implausible_rows(concept: str, excluded_keys: set, records: list) -> None:
    """Shared DELETE + ingestion_failures logging for every plausibility check in this file.
    `concept`: the exact `financials.concept` value to delete rows for (e.g. 'Shares Diluted',
    'Shares Outstanding (Cover Page)'). `excluded_keys`: {(ticker, period_type, fiscal_year), ...}.
    `records`: dict rows matching `_sd_fail_schema`, already filtered to `excluded_keys`."""
    if not excluded_keys:
        print(f"✓ No implausible {concept} values found.")
        return
    _bad_df = spark.createDataFrame(
        list(excluded_keys), schema=["ticker", "period_type", "fiscal_year"]
    )
    _bad_df.createOrReplaceTempView("implausible_concept_rows")
    spark.sql(f"""
        MERGE INTO {full_tbl} AS t
        USING implausible_concept_rows AS s
        ON  t.ticker      = s.ticker
        AND t.fiscal_year = s.fiscal_year
        AND t.period_type = s.period_type
        AND t.concept     = '{concept}'
        WHEN MATCHED THEN DELETE
    """)
    print(f"✓ Deleted {len(excluded_keys):,} implausible {concept} row(s) from {full_tbl}")
    spark.createDataFrame(records, schema=_sd_fail_schema) \
         .write.mode("append").saveAsTable(_sd_fail_tbl)
    print(f"✓ Logged {len(records):,} row(s) to {_sd_fail_tbl}")

# COMMAND ----------

# MAGIC %md ## 1. Absolute plausibility check

# COMMAND ----------

_SD_ABS_SHARES_FLOOR  = 2_000_000
_SD_ABS_TCA_FLOOR     = 20_000_000
_SD_ABS_RATIO_CEILING = 10.0

_shares_allper_small = (
    spark.table(full_tbl)
    .filter(
        (F.col("concept") == "Shares Diluted")
        & (F.col("value") > 0) & (F.col("value") < _SD_ABS_SHARES_FLOOR)
    )
    .select("ticker", "period_type", "fiscal_year", "period_end", F.col("value").alias("shares"))
)
_tca_allper_large = (
    spark.table(full_tbl)
    .filter((F.col("concept") == "Total Current Assets") & (F.col("value") > _SD_ABS_TCA_FLOOR))
    .select("ticker", "period_type", "fiscal_year", F.col("value").alias("tca"))
)
_sd_abs_base = _shares_allper_small.join(
    _tca_allper_large, on=["ticker", "period_type", "fiscal_year"], how="inner"
)

# As-of price: latest close ≤ period_end, falling back to the ticker's latest available close
# at all (covers a candidate row older than the ticker's earliest priced date) — same as-of
# pattern as 22__derived_metrics.py's asof_close/asof_shares joins.
_sd_abs_prices = spark.table(f"{CATALOG}.{SCHEMA}.market_prices_daily").select("ticker", "date", "close")

_w_price_asof = Window.partitionBy("ticker", "period_type", "fiscal_year").orderBy(F.col("date").desc())
_price_asof = (
    _sd_abs_base.select("ticker", "period_type", "fiscal_year", "period_end").distinct()
    .join(_sd_abs_prices, on="ticker", how="inner")
    .filter(F.col("date") <= F.col("period_end"))
    .withColumn("_rn", F.row_number().over(_w_price_asof))
    .filter(F.col("_rn") == 1)
    .select("ticker", "period_type", "fiscal_year", F.col("close").alias("price_asof"))
)
_w_price_latest = Window.partitionBy("ticker").orderBy(F.col("date").desc())
_price_latest = (
    _sd_abs_prices
    .withColumn("_rn", F.row_number().over(_w_price_latest))
    .filter(F.col("_rn") == 1)
    .select("ticker", F.col("close").alias("price_latest"))
)

_sd_abs_evaluated = (
    _sd_abs_base
    .join(_price_asof, on=["ticker", "period_type", "fiscal_year"], how="left")
    .join(_price_latest, on="ticker", how="left")
    .withColumn("price", F.coalesce(F.col("price_asof"), F.col("price_latest")))
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    .withColumn("asset_per_share", F.col("tca") / F.col("shares"))
    .withColumn("ratio", F.col("asset_per_share") / F.col("price"))
    .filter(F.col("ratio") > _SD_ABS_RATIO_CEILING)
)
_sd_abs_rows = _sd_abs_evaluated.select(
    "ticker", "period_type", "fiscal_year", "shares", "tca", "price", "ratio"
).collect()

_sd_abs_excluded = {(r.ticker, r.period_type, r.fiscal_year) for r in _sd_abs_rows}
print(f"Absolute-plausibility implausible Shares Diluted values found: {len(_sd_abs_excluded):,}")

_sd_abs_scraped_at = datetime.utcnow()
_sd_abs_records = [{
    "ticker": r.ticker,
    "error_type": "implausible_shares_diluted_absolute",
    "error_message": (
        f"Shares Diluted {r.period_type} FY{r.fiscal_year}={r.shares:,.0f} implies Total "
        f"Current Assets/Share=${r.tca / r.shares:,.2f}, {r.ratio:.4g}x the ticker's own traded "
        f"price (${r.price:,.2f}) — outside any plausible real capital structure, consistent "
        f"with a filer-side 'reported in thousands' XBRL tagging error. Excluded from "
        f"financials this run."
    ),
    "step": "shares_diluted_plausibility",
    "scraped_at": _sd_abs_scraped_at,
} for r in _sd_abs_rows]

_delete_implausible_rows("Shares Diluted", _sd_abs_excluded, _sd_abs_records)

# COMMAND ----------

# MAGIC %md ## 2. Neighbor-based iterative check (all period_types, runs after check 1)
# MAGIC
# MAGIC Shared across both `Shares Diluted` and `Shares Outstanding (Cover Page)` — same
# MAGIC mechanism, only the scanned concept and the logged `error_type` differ (see the header
# MAGIC doc above).

# COMMAND ----------

_SD_RATIO_LOW  = 1.0 / 500.0
_SD_RATIO_HIGH = 500.0
_SD_MAX_ITERATIONS = 15


def _run_neighbor_plausibility_check(concept: str, error_type: str) -> None:
    """Iterative neighbor-relative plausibility check for a single stock/count concept.
    Re-reads from `financials` so this reflects any prior check's deletions, not a stale
    in-memory frame. See the header doc's "2. Neighbor-based iterative check" for the
    algorithm description."""
    _sd_concept_rows = (
        spark.table(full_tbl)
        .filter(F.col("concept") == concept)
        .select("ticker", "period_type", "fiscal_year", "period_end", "value")
        .localCheckpoint(eager=True)
    )
    _w_sd_ticker = Window.partitionBy("ticker").orderBy("period_end", (F.col("period_type") != "FY"))

    _sd_excluded: set = set()      # {(ticker, period_type, fiscal_year), ...} — "presumed bad" set
    _sd_prev_excluded: set = set()  # the set one iteration back — used for the non-convergence
                                     # per-ticker stability check below (see its own comment)
    _sd_converged = False
    for _sd_iter in range(_SD_MAX_ITERATIONS):
        if _sd_excluded:
            _sd_excl_df = spark.createDataFrame(
                list(_sd_excluded), schema=["ticker", "period_type", "fiscal_year"]
            )
            _sd_marked = (
                _sd_concept_rows
                .join(_sd_excl_df.withColumn("_excl", F.lit(True)),
                      on=["ticker", "period_type", "fiscal_year"], how="left")
                .withColumn("pool_value", F.when(F.col("_excl").isNull(), F.col("value")))
                .drop("_excl")
            )
        else:
            _sd_marked = _sd_concept_rows.withColumn("pool_value", F.col("value"))

        _sd_evaluated = (
            _sd_marked
            .withColumn("prev_value", F.last("pool_value", ignorenulls=True)
                        .over(_w_sd_ticker.rowsBetween(Window.unboundedPreceding, -1)))
            .withColumn("next_value", F.first("pool_value", ignorenulls=True)
                        .over(_w_sd_ticker.rowsBetween(1, Window.unboundedFollowing)))
            .withColumn("neighbor_value", F.coalesce(F.col("prev_value"), F.col("next_value")))
            .withColumn("neighbor_side", F.when(F.col("prev_value").isNotNull(), F.lit("prev")).otherwise(F.lit("next")))
        )
        _sd_bad_df = (
            _sd_evaluated
            .filter(F.col("neighbor_value").isNotNull() & (F.col("neighbor_value") != 0) & (F.col("value") != 0))
            .withColumn("ratio", F.col("value") / F.col("neighbor_value"))
            .filter((F.col("ratio") <= _SD_RATIO_LOW) | (F.col("ratio") >= _SD_RATIO_HIGH))
        )
        _sd_bad_rows = _sd_bad_df.select(
            "ticker", "period_type", "fiscal_year", "value", "neighbor_value", "neighbor_side", "ratio"
        ).collect()
        _sd_new_excluded = {(r.ticker, r.period_type, r.fiscal_year) for r in _sd_bad_rows}

        if _sd_new_excluded == _sd_excluded:
            print(f"✓ {concept} neighbor plausibility check converged after {_sd_iter + 1} iteration(s)")
            _sd_converged = True
            _sd_prev_excluded = _sd_excluded
            break
        _sd_prev_excluded = _sd_excluded
        _sd_excluded = _sd_new_excluded
    else:
        print(f"⚠ {concept} neighbor plausibility check did not fully converge after "
              f"{_SD_MAX_ITERATIONS} iterations for every ticker")

    if not _sd_converged:
        # A handful of tickers with very little history (confirmed real case: a 4-year-old ticker
        # with genuinely volatile real share counts) can make the flag/unflag decision for their
        # OWN periods oscillate forever between two states, without ever settling — comparing a
        # newly-excluded period against whatever's left in the pool can swing in and out of the
        # implausible-ratio band each pass. This does NOT mean every other ticker's answer is in
        # doubt (tickers are evaluated independently; one ticker's oscillation can't change another
        # ticker's own row values or comparisons) — only the specific periods still changing
        # between this iteration and the last are actually undetermined. Drop exactly those (never
        # guess which of the two oscillating states is right); every other ticker's already-stable
        # answer is unaffected and kept as computed.
        _sd_unstable = _sd_excluded.symmetric_difference(_sd_prev_excluded)
        if _sd_unstable:
            print(f"⚠ {len(_sd_unstable):,} (ticker, period_type, fiscal_year) row(s) never "
                  f"stabilized — excluding from this run's action, will be re-evaluated next run: "
                  f"{sorted(_sd_unstable)[:10]}{'...' if len(_sd_unstable) > 10 else ''}")
            _sd_excluded = _sd_excluded - _sd_unstable

    print(f"Neighbor-relative implausible {concept} values found: {len(_sd_excluded):,}")

    _sd_scraped_at = datetime.utcnow()
    # _sd_bad_rows is from the loop's LAST (converged) iteration — its neighbor/ratio values
    # already reflect the final, stable excluded set, so no re-evaluation is needed here.
    _sd_records = [{
        "ticker": r.ticker,
        "error_type": error_type,
        "error_message": (
            f"{concept} {r.period_type} FY{r.fiscal_year}={r.value:,.0f} is {r.ratio:.4g}x its "
            f"nearest trustworthy neighbor ({r.neighbor_side}, {r.neighbor_value:,.0f}) — outside "
            f"the range any real split/buyback/offering produces, consistent with a filer-side "
            f"'reported in thousands' XBRL tagging error. Excluded from financials this run."
        ),
        "step": "shares_diluted_plausibility",
        "scraped_at": _sd_scraped_at,
    } for r in _sd_bad_rows if (r.ticker, r.period_type, r.fiscal_year) in _sd_excluded]

    _delete_implausible_rows(concept, _sd_excluded, _sd_records)


_run_neighbor_plausibility_check("Shares Diluted", "implausible_shares_diluted")

# COMMAND ----------

# MAGIC %md ## 3. Neighbor-based iterative check — Shares Outstanding (Cover Page)
# MAGIC
# MAGIC Same mechanism as check 2 above, applied to the cover-page share count instead of the
# MAGIC weighted-average one. See the header doc's cover-page-shares note for the confirmed
# MAGIC real cases (ADV, AGL, PTCT, BKNG, MCHB).

# COMMAND ----------

_run_neighbor_plausibility_check(
    "Shares Outstanding (Cover Page)", "implausible_shares_outstanding_cover_page"
)
