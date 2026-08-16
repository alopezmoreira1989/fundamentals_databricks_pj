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
