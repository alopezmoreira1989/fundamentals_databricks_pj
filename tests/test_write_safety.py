"""Tests for the write-safety guards (fundamentals_pipeline/write_safety.py).

Reproduces the exact failure mode of the 2026-08-16 market_prices_daily incident (see
docs/phase5-6-european-dashboard-data-integration.md §10) at the pure-decision-logic level.
"""

from __future__ import annotations

import pytest

from fundamentals_pipeline.write_safety import (
    UnsafeFullOverwriteError,
    UnsafeOrphanDeleteError,
    assert_full_overwrite_safe,
    assert_orphan_delete_safe,
    is_full_universe_run,
    scoped_orphan_keys,
)

# ── assert_full_overwrite_safe ──────────────────────────────────────────────────────────────


def test_full_universe_scope_passes():
    # The normal, unscoped production caller: ACTIVE_TICKERS == config.tickers.
    universe = {"AAPL", "MSFT", "TSLA"}
    assert_full_overwrite_safe(active_tickers=universe, full_universe=universe)


def test_full_universe_scope_with_benchmark_passes():
    # Benchmark tickers (e.g. SPY) are additive and never in config.tickers — must not count
    # against coverage.
    universe = {"AAPL", "MSFT", "TSLA"}
    assert_full_overwrite_safe(
        active_tickers=universe | {"SPY"},
        full_universe=universe,
        benchmark_tickers={"SPY"},
    )


def test_narrow_scope_raises():
    # The exact 2026-08-16 incident: 18__fetch_eu_market_data.py pre-seeded ACTIVE_TICKERS to
    # 6 EU tickers (disjoint from config.tickers entirely) while MODE=="full" was about to
    # overwrite the whole table.
    with pytest.raises(UnsafeFullOverwriteError, match="does not cover the full"):
        assert_full_overwrite_safe(
            active_tickers={"ALO", "FCT", "IBE", "ISP", "NAI", "RAND"},
            full_universe={"AAPL", "MSFT", "TSLA"},
        )


def test_single_ticker_override_raises():
    # A plausible, legitimate-looking future misuse: force_full_refresh=true with
    # tickers_override=AAPL (re-fetch AAPL's complete history). Still unsafe for a full-table
    # overwrite — must route through the scoped MERGE path instead.
    with pytest.raises(UnsafeFullOverwriteError, match="1 ticker"):
        assert_full_overwrite_safe(
            active_tickers={"AAPL"},
            full_universe={"AAPL", "MSFT", "TSLA"},
        )


def test_active_superset_of_universe_passes():
    # ACTIVE_TICKERS may legitimately include MORE than config.tickers (e.g. a brand-new
    # ticker not yet re-read) — only genuine under-coverage is unsafe.
    assert_full_overwrite_safe(
        active_tickers={"AAPL", "MSFT", "TSLA", "NEWCO"},
        full_universe={"AAPL", "MSFT", "TSLA"},
    )


def test_empty_full_universe_is_a_noop():
    # No production universe to protect (e.g. a fresh/test environment) — never raise.
    assert_full_overwrite_safe(active_tickers={"AAPL"}, full_universe=set())


# ── assert_orphan_delete_safe ───────────────────────────────────────────────────────────────


def test_small_orphan_drift_passes():
    # A healthy run's normal, occasional stale-value retirement.
    assert_orphan_delete_safe(would_delete=50, existing=10_000)


def test_large_orphan_delete_raises():
    # The exact 2026-08-16 cascade: financials_metrics dropped from 2,039,873 to 1,380,672
    # rows (32.3%) when market_cap_asof came back empty.
    with pytest.raises(UnsafeOrphanDeleteError, match="32%"):
        assert_orphan_delete_safe(would_delete=659_201, existing=2_039_873)


def test_exactly_at_threshold_passes():
    # 10.0% exactly does not exceed the (strict >) 10% threshold.
    assert_orphan_delete_safe(would_delete=1_000, existing=10_000, max_fraction=0.10)


def test_just_over_threshold_raises():
    with pytest.raises(UnsafeOrphanDeleteError):
        assert_orphan_delete_safe(would_delete=1_001, existing=10_000, max_fraction=0.10)


def test_empty_existing_table_is_a_noop():
    # Nothing to protect on a fresh table.
    assert_orphan_delete_safe(would_delete=0, existing=0)


def test_custom_max_fraction_respected():
    assert_orphan_delete_safe(would_delete=400, existing=1_000, max_fraction=0.5)
    with pytest.raises(UnsafeOrphanDeleteError):
        assert_orphan_delete_safe(would_delete=600, existing=1_000, max_fraction=0.5)


# ── is_full_universe_run (2026-08-17: semantic primary guard) ──────────────────────────────


def test_full_coverage_is_full_universe_run():
    assert is_full_universe_run(source_ticker_count=2_662, reference_ticker_count=2_662) is True


def test_the_exact_incident_is_not_a_full_universe_run():
    # market_prices_daily covered 7 tickers against a ~2,662-ticker fundamentals universe
    # during the 2026-08-16 incident — nowhere near the coverage threshold.
    assert is_full_universe_run(source_ticker_count=7, reference_ticker_count=2_662) is False


def test_coverage_exactly_at_threshold_passes():
    assert is_full_universe_run(source_ticker_count=900, reference_ticker_count=1_000,
                                 min_coverage=0.90) is True


def test_coverage_just_under_threshold_fails():
    assert is_full_universe_run(source_ticker_count=899, reference_ticker_count=1_000,
                                 min_coverage=0.90) is False


def test_zero_reference_universe_is_a_noop():
    # Nothing to compare against (e.g. a fresh/test environment) — never block on this alone.
    assert is_full_universe_run(source_ticker_count=0, reference_ticker_count=0) is True


def test_source_can_exceed_reference():
    # The source legitimately covering MORE tickers than the reference (e.g. price data for
    # some tickers not yet in the fundamentals universe) is still a full-universe run.
    assert is_full_universe_run(source_ticker_count=3_000, reference_ticker_count=2_662) is True


# ── scoped_orphan_keys (2026-08-17: financials_metrics joint-ownership fix) ─────────────────
# Reproduces the real 2026-08-17 dry-run finding: 474,698 raw orphans, 100% belonging to
# 23__intrinsic_value.py's own metric vocabulary, 0% to 22__derived_metrics.py's own -- and
# proves the fix doesn't weaken protection against the ORIGINAL 2026-08-16 incident pattern
# (a genuine, large loss of 22-owned metrics).

_22_OWNED = {"Net Margin %", "ROA %", "Piotroski F-Score", "P/E", "Altman Z-Score"}
_23_OWNED = {"Owner Earnings (FY)", "Graham Number (FY)", "MoS % (DCF, FY)"}


def test_the_real_dry_run_scenario_scopes_to_zero():
    # All "orphans" are 23-owned metrics missing from 22's own source -- because 22 never
    # produces them in the first place, not because anything is actually stale.
    orphans = {("AAPL", 2024, m) for m in _23_OWNED} | {("MSFT", 2024, m) for m in _23_OWNED}
    assert scoped_orphan_keys(orphans, _22_OWNED) == set()


def test_genuinely_stale_22_owned_metric_is_still_flagged():
    # A real, legitimate cleanup case: 22 itself no longer produces a metric it used to (e.g.
    # a retired formula) -- this must still be caught, not silently protected by the fix.
    orphans = {("AAPL", 2020, "Altman Z-Score")}
    assert scoped_orphan_keys(orphans, _22_OWNED) == {("AAPL", 2020, "Altman Z-Score")}


def test_mixed_orphan_set_keeps_only_the_owned_subset():
    orphans = (
        {("AAPL", 2024, m) for m in _23_OWNED}          # not 22's business
        | {("AAPL", 2020, "Altman Z-Score")}             # genuinely 22's stale metric
    )
    assert scoped_orphan_keys(orphans, _22_OWNED) == {("AAPL", 2020, "Altman Z-Score")}


def test_original_incident_pattern_still_fully_protected():
    # The 2026-08-16 incident: a corrupted price input collapsed long_val (22's OWN valuation
    # metrics) for the entire universe. Simulate a large, genuine loss of 22-owned rows and
    # confirm scoped_orphan_keys still surfaces all of it -- the fix must not mask this.
    large_genuine_loss = {(f"TICK{i}", 2024, "P/E") for i in range(1000)}
    scoped = scoped_orphan_keys(large_genuine_loss, _22_OWNED)
    assert len(scoped) == 1000
    assert scoped == large_genuine_loss


def test_combined_with_assert_orphan_delete_safe_real_scenario_passes():
    # End-to-end: the real dry-run's raw count (474,698) would have tripped the OLD, unscoped
    # guard; the ownership-scoped count (0, since all are 23-owned) correctly passes.
    orphans = {(f"TICK{i}", 2024, m) for i in range(50_000) for m in _23_OWNED}
    scoped = scoped_orphan_keys(orphans, _22_OWNED)
    assert_orphan_delete_safe(len(scoped), existing=2_039_873)  # does not raise


def test_combined_with_assert_orphan_delete_safe_original_incident_still_raises():
    # End-to-end: a genuine large loss of 22-owned rows must still trip the guard after scoping.
    large_genuine_loss = {(f"TICK{i}", 2024, "P/E") for i in range(300_000)}
    scoped = scoped_orphan_keys(large_genuine_loss, _22_OWNED)
    with pytest.raises(UnsafeOrphanDeleteError):
        assert_orphan_delete_safe(len(scoped), existing=2_039_873)


def test_empty_owned_metrics_scopes_to_nothing():
    orphans = {("AAPL", 2024, "P/E")}
    assert scoped_orphan_keys(orphans, set()) == set()


def test_empty_orphan_set_is_a_noop():
    assert scoped_orphan_keys(set(), _22_OWNED) == set()
