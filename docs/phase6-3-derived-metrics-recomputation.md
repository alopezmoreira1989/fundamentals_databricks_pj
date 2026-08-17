# Phase 6.3 — Safe European derived-metrics recomputation (WIP)

**Status as of this commit: IN PROGRESS.** `22__derived_metrics.py` has been run once, live,
successfully, and validated. `23__intrinsic_value.py`, the idempotency re-run (22+23 a second
time), and `51__export_dashboard_data.py`/`52__publish_to_github.py` have **not** been run yet
in this phase. This document will be updated with the remaining phases and a final
classification (`SUCCESS` / `BLOCKED` / `PARTIAL`) once that work resumes and completes.

## Objective

The 8 admitted European companies (FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP) have had 21
canonical ESEF concepts flowing through `financials` since Phase 6.1 (2026-08-17), but
`financials_metrics`/`financials_intrinsic_value` had not been recomputed since that mapping
expansion — so the European companies had raw financial-statement data but little/no derived
ratios, growth metrics, or valuation output. This phase safely determines and executes
`22 → 23 → 51 → 52` so the new EU facts flow through to derived metrics → `dashboard_data` →
the production website artifact, without damaging US/Canada/Europe/existing historical data.

Full context on the prior incident this phase's caution is grounded in — the 2026-08-16
`market_prices_daily` write-mode bug and its cascading impact across 6 tables — and the two
rounds of hardening that followed are in
`docs/phase5-6-european-dashboard-data-integration.md` §10–§15. This phase does not repeat that
history; it proves, against the *current* code and *current* production data, that the
hardening actually holds before making any write.

## Phase A — code trace (complete)

Read the actual current source (not just the documentation) of every file in the write path,
on the `main` branch, confirming it matches what §10–§15 describe:

- **`fundamentals_pipeline/write_safety.py`** + `tests/test_write_safety.py` — `is_full_universe_run`
  (semantic primary guard), `assert_orphan_delete_safe` (secondary percentage guard),
  `assert_full_overwrite_safe`, `scoped_orphan_keys` (the joint-ownership allow-list filter).
  All four functions and their test coverage match the documented design exactly.
- **`fundamentals_pipeline/20__transformation/22__derived_metrics.py`** — full read. Confirmed:
  - No ticker-scoping mechanism anywhere (always reads the full `financials` table) — this
    notebook cannot be run "scoped to Europe only."
  - `market_cap_asof`/`market_cap_live` full-table overwrites are gated behind `_has_prices`,
    which is itself gated by `is_full_universe_run(price_ticker_count, fundamentals_ticker_count)`
    — the exact primary defense that would have prevented the 2026-08-16 incident.
  - `financials_metrics`'s `WHEN NOT MATCHED BY SOURCE THEN DELETE` orphan cleanup is scoped to
    `_22_owned_metrics` (this notebook's own `base_metric_cols ∪ val_metric_cols` allow-list) —
    never touches `23`'s intrinsic-value-labeled rows — and is guarded by
    `assert_orphan_delete_safe` as a secondary check before the MERGE runs.
  - A real, purpose-built `DRY_RUN` mode exists: classifies the exact `incoming_metrics`
    DataFrame the MERGE would consume against the current table, then exits via
    `dbutils.notebook.exit()` before any write.
- **`fundamentals_pipeline/20__transformation/23__intrinsic_value.py`** — full read. Confirmed:
  - Also has no ticker-scoping mechanism (always full-universe).
  - The `financials_intrinsic_value` MERGE has **two** `WHEN MATCHED` clauses: the first (safe
    cases — FY rows, or a TTM row with a genuine non-NULL price, or a TTM row when
    `_ttm_full_universe_run` is true) updates every column including `price_close`/
    `margin_of_safety_pct`; the second (a TTM row whose price computed NULL AND price coverage
    was not representative) updates everything *except* those two columns — this is the exact
    fix for the 2026-08-16 AAPL TTM-price-wiped-to-NULL failure mode.
  - Two independent orphan-cleanup MERGEs (`financials_intrinsic_value` itself, and the
    IV-labeled rows it exposes into `financials_metrics`), each scoped (to
    `iv_processed_tickers` / the `_iv_labels` allow-list respectively) and each independently
    guarded by `assert_orphan_delete_safe`.
- **`fundamentals_pipeline/50__publish/51__export_dashboard_data.py`** — full read. Confirmed
  **zero Delta table writes** of any kind — reads every source table read-only, writes only
  local parquet + a copy to the `main.financials._publish` Volume.
- **`fundamentals_pipeline/50__publish/52__publish_to_github.py`** — confirmed zero Spark/Delta
  interaction at all (reads local files, uploads to the GitHub Release via the GitHub API).
- **`databricks.yml`** + `91b__pipeline_metrics.py`/`91d__intrinsic_value.py`/
  `91h__export_dashboard_data.py`/`91i__publish_github.py` — confirmed the real production DAG
  order (`pipeline_pre22 → pipeline_metrics(22) → {intrinsic_value(23), backtest, forecasting}
  in parallel → analysis_and_checks → export_dashboard_data(51) → publish_github(52) →
  delta_maintenance`) and that `52`'s real filename is `52__publish_to_github.py` (not
  `52__publish_dashboard_data.py`).

## Phase B — execution scope (answered)

Confirmed directly from the code (not inferred): neither `22` nor `23` has any ticker-scoping
mechanism. A "run scoped to just the 8 EU tickers" is not possible and was not invented — both
notebooks are structurally full-universe-only by design. Any live run recomputes the entire
~2,700-ticker universe.

## Phase C — pre-run baseline (captured live, read-only)

Via Databricks Connect (`profile("fundamentals").serverless(True)`), before any write:

| Table | Row count | Distinct tickers |
|---|---|---|
| `financials` | 4,750,782 | 2,716 |
| `financials_metrics` | 2,039,737 | 2,715 |
| `financials_intrinsic_value` | 221,225 | 2,375 |
| `market_prices_daily` | 15,315,168 | 2,652 |
| `market_cap_asof` | 27,230 | 2,488 |
| `market_cap_live` | 2,175 | 2,175 |
| `stock_splits` | 4,330 | 1,255 |

`market_prices_daily` covers 2,652 of 2,716 fundamentals tickers = **97.6%** — comfortably above
the 90% `is_full_universe_run` coverage threshold.

Per-ticker baseline (`financials_metrics` row count):

- **US/CA regression set**: AAPL=1,470, MSFT=1,470, TSLA=896, AEM=533, AQN=521, BN=190.
- **EU set** (pre-existing rows, leftovers from Phase 5.6 §15's earlier live run under the
  *old* 5-concept mapping — not zero, as later corrected from an initial misreading): FCC=32,
  ALO=16, NAI=12, FCT=12, IBE=32, SGO=18, RAND=32, ISP=8.

Option B architecture confirmed still holding: 0 rows with `market = 'EU'` in
`main.config.tickers`; 8 rows with `admission_status = 'admitted'` in
`main.config.eu_admission_candidates`.

## Phase D — safety-gate review (complete, folded into Phase A)

Every guard traced above was confirmed present in the **actual executing code** on `main`
(commit `60c7cb1`, the exact commit the personal Databricks Repo clone
`/Repos/al.lopez.moreira@gmail.com/phase6-validation` was pointed at and pulled before any run),
not merely cited from documentation or inferred from tests.

## Phase E — dry run (complete, clean)

Submitted `22__derived_metrics.py` as a real Databricks Job run with `DRY_RUN=true` (exits via
`dbutils.notebook.exit()` before any write — verified no table was touched). Real result against
live production data:

- `target_financials_metrics_count`: 2,039,737 · `source_incoming_metrics_count`: 1,565,652
- `matched_count`: 1,565,134 · `new_count`: 518 · `modified_count`: 39
- **`orphaned_count` (raw, unscoped)**: 474,603 (23.3% of target) — would have failed the old,
  pre-hardening naive check.
- **`scoped_orphaned_count`**: **0** (0.0% of target). `orphan_by_metric` confirms every one of
  those 474,603 "orphans" is a `23`-owned intrinsic-value label (`Owner Earnings (FY)`,
  `Graham Revised Value (FY)`, `MoS % (...)`, etc.) — none belong to `22`'s own vocabulary. This
  is the joint-ownership fix (§14 of the Phase 5.6 doc) working correctly against real, current
  data, reproducing the same pattern the original 2026-08-17 dry run found.
- **`new_by_market`**: `[{"market": "EU", "n": 518}]` — 100% of the 518 new rows are European,
  exactly the expected effect of the Phase 6.1 concept-mapping expansion (Current Ratio,
  Operating Margin %, Tangible Book Value, Goodwill/Total Assets %, ROE %, ROIC %, NCAV, etc.
  becoming computable for FCC/ALO/FCT/IBE for the first time).
- `sample_modified_rows`: all plausible (e.g. Piotroski F-Score improving for several EU
  tickers now that more concepts are available; Total Payout Ratio becoming non-zero for FCC/
  ALO/FCT/NAI now that Dividends Paid is mapped) — nothing resembling corruption.

Scoped orphan percentage (0.0%) is far under the 10% `assert_orphan_delete_safe` threshold, so
the live MERGE would pass the guard cleanly.

## Phase F — live-run authorization (granted)

All conditions were met: code traced and confirmed safe (Phase A/D), execution scope confirmed
full-universe-only with no safer alternative (Phase B), a real pre-run baseline captured (Phase
C), and a clean real `DRY_RUN` result against current production data (Phase E). Explicit
go-ahead was given by the repo owner before any write was made.

## Phase G — live run 1 of `22__derived_metrics.py` (complete, validated)

Submitted for real (no `DRY_RUN` parameter) against the personal Databricks Repo clone pinned to
`main`@`60c7cb1`. **Result: `SUCCESS`.**

Post-run validation (read-only):

| Table | Before | After |
|---|---|---|
| `financials_metrics` | 2,039,737 rows / 2,715 tickers | 2,040,255 rows / 2,715 tickers |
| `market_cap_asof` | 27,230 rows / 2,488 tickers | 27,230 rows / 2,488 tickers (unchanged — correct, not touched by 22 unless coverage changed) |
| `market_cap_live` | 2,175 rows / 2,175 tickers | 2,175 rows / 2,175 tickers (unchanged) |

**EU tickers — real growth, confirmed**:

| Ticker | `financials_metrics` rows before | after | Sample: Current Ratio |
|---|---|---|---|
| FCC | 32 | 137 | 1.62 (FY2024) |
| ALO | 16 | 72 | 0.82 (FY2023) |
| NAI | 12 | 58 | 1.68 (FY2025) |
| FCT | 12 | 38 | *(still absent — FCT genuinely has no current/noncurrent split, matches Phase 6.1's own documented finding)* |
| IBE | 32 | 132 | 0.69 (FY2024) |
| SGO | 18 | 96 | 1.27 (FY2025) |
| RAND | 32 | 132 | 1.21 (FY2025) |
| ISP | 8 | 15 | *(still absent — ISP's bank structure, no current/noncurrent split, matches Phase 6.1's own documented finding)* |

Every ratio value matches the Phase 6.1 "Final validation before merge" section's own
hand-computed read-only SQL check (FCC 1.62 / IBE 0.69 / SGO 1.27 / RAND 1.21) — now confirmed
as the real value the MERGE actually wrote, not a projection.

**US/CA regression — byte-identical row counts, confirmed clean**:

| Ticker | Before | After | Match |
|---|---|---|---|
| AAPL | 1,470 | 1,470 | ✓ |
| MSFT | 1,470 | 1,470 | ✓ |
| TSLA | 896 | 896 | ✓ |
| AEM | 533 | 533 | ✓ |
| AQN | 521 | 521 | ✓ |
| BN | 190 | 190 | ✓ |

No US/Canada regression. `market_cap_asof`/`market_cap_live` untouched (as expected — this
run's price coverage didn't change, so no overwrite was even attempted beyond the normal
full recompute, and the row/ticker counts came back identical).

## Remaining work (not yet done)

Per explicit instruction mid-session, this phase is being committed as WIP at this checkpoint.
**Not yet run or validated**:

- Live run of `23__intrinsic_value.py` (Phase H) — the two-clause NULL-preserving MERGE and
  its own orphan cleanups have not yet been exercised against real production data this phase.
- Idempotency check (Phase J) — running `22` and `23` a second time and confirming byte-identical
  row counts.
- `51__export_dashboard_data.py` / `52__publish_to_github.py` (Phase I) — not run; the public
  GitHub Release / website have **not** been updated with the new EU metrics yet.
- European coverage validation table (Phase L) and real website validation (Phase M).
- Final classification (`SUCCESS` / `BLOCKED` / `PARTIAL`).

**No production code was changed in this phase** — this was purely an execution/validation pass
against the existing, already-hardened `22`/`23`/`51`/`52`. No defect was found that would
require a code fix.

## Explicit non-goals (unchanged, still honored)

Tier 2 ESEF concepts, ESEF mapping changes, `financials` schema changes, `financials_raw`
changes, `fundamentals_screener`/Django/Streamlit changes, valuation formula/ratio redesign,
Periods/Filings UI changes, interim ESEF ingestion, and inventing missing financial concepts are
all explicitly out of scope for this phase and were not touched.
