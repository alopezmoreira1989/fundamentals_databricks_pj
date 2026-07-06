"""Unit tests for the pure backtest helpers (_core/backtest.py)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from fundamentals_pipeline import backtest as bt


# ── as_of_date / as_of_eligible (no look-ahead) ──────────────────────────────────
def test_as_of_date_prefers_filed():
    assert bt.as_of_date(date(2024, 2, 15), date(2023, 12, 31)) == date(2024, 2, 15)


def test_as_of_date_falls_back_to_period_end_plus_lag():
    assert bt.as_of_date(None, date(2023, 12, 31), lag_days=90) == date(2023, 12, 31) + timedelta(days=90)


def test_as_of_date_none_when_no_dates():
    assert bt.as_of_date(None, None) is None


def test_as_of_eligible():
    reb = date(2024, 3, 1)
    assert bt.as_of_eligible(date(2024, 2, 15), reb) is True   # filed before rebalance
    assert bt.as_of_eligible(date(2024, 3, 1), reb) is True    # filed on rebalance (inclusive)
    assert bt.as_of_eligible(date(2024, 3, 2), reb) is False   # filed AFTER → look-ahead, excluded
    assert bt.as_of_eligible(None, reb) is False


def test_as_of_eligible_monotonic_in_rebalance_date():
    # Once knowable, always knowable going forward — eligibility can't flip back to False as the
    # rebalance date advances (the core no-look-ahead invariant, stated as a property).
    as_of = date(2024, 2, 15)
    assert bt.as_of_eligible(as_of, date(2024, 2, 15)) is True
    assert bt.as_of_eligible(as_of, date(2024, 6, 1)) is True
    assert bt.as_of_eligible(as_of, date(2030, 1, 1)) is True


# ── latest_price_asof (no look-ahead, reference for 71's Spark entry/exit pricing) ────
def test_latest_price_asof_picks_latest_not_after_as_of():
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 5)]
    prices = [100.0, 101.0, 103.0]
    # as_of falls between 1/3 and 1/5 → 1/5's price is unknowable yet, must not be picked.
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 4)) == 101.0


def test_latest_price_asof_exact_as_of_date_is_inclusive():
    dates = [date(2024, 1, 2), date(2024, 1, 4)]
    prices = [100.0, 102.0]
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 4)) == 102.0


def test_latest_price_asof_all_dates_after_as_of_is_none():
    dates = [date(2024, 2, 1), date(2024, 2, 2)]
    prices = [100.0, 101.0]
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 1)) is None


def test_latest_price_asof_none_as_of_is_none():
    assert bt.latest_price_asof([date(2024, 1, 2)], [100.0], None) is None


def test_latest_price_asof_unsorted_input():
    dates = [date(2024, 1, 5), date(2024, 1, 2), date(2024, 1, 3)]
    prices = [103.0, 100.0, 101.0]
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 4)) == 101.0


def test_latest_price_asof_skips_missing_price():
    dates = [date(2024, 1, 2), date(2024, 1, 3)]
    prices = [100.0, float("nan")]
    # The nearest-in-time candidate has a NaN price → falls back to the next-latest valid one.
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 3)) == 100.0


def test_latest_price_asof_none_date_entries_ignored():
    dates = [None, date(2024, 1, 2)]
    prices = [999.0, 100.0]
    assert bt.latest_price_asof(dates, prices, date(2024, 1, 4)) == 100.0


# ── passes_predicates ────────────────────────────────────────────────────────────
def test_passes_predicates_all_true():
    m = {"P/E": 12.0, "P/B": 1.2, "Current Ratio": 2.5}
    preds = [["P/E", "<", 15], ["P/B", "<", 1.5], ["Current Ratio", ">", 2.0]]
    assert bt.passes_predicates(m, preds) is True


def test_passes_predicates_one_false():
    m = {"P/E": 18.0, "P/B": 1.2}
    assert bt.passes_predicates(m, [["P/E", "<", 15], ["P/B", "<", 1.5]]) is False


def test_passes_predicates_missing_metric_fails():
    m = {"P/E": 12.0}  # no P/B
    assert bt.passes_predicates(m, [["P/E", "<", 15], ["P/B", "<", 1.5]]) is False


def test_passes_predicates_nan_fails():
    m = {"P/E": float("nan")}
    assert bt.passes_predicates(m, [["P/E", "<", 15]]) is False


def test_passes_predicates_unknown_operator_raises():
    with pytest.raises(ValueError):
        bt.passes_predicates({"P/E": 12.0}, [["P/E", "≈", 15]])


def test_passes_predicates_empty_is_true():
    assert bt.passes_predicates({"P/E": 99.0}, []) is True


@pytest.mark.parametrize(
    "op,threshold,passing_value,failing_value",
    [
        (">=", 10.0, 10.0, 9.999),
        ("<=", 10.0, 10.0, 10.001),
        ("==", 10.0, 10.0, 10.001),
        ("!=", 10.0, 9.0, 10.0),
    ],
)
def test_passes_predicates_all_operators(op, threshold, passing_value, failing_value):
    assert bt.passes_predicates({"m": passing_value}, [["m", op, threshold]]) is True
    assert bt.passes_predicates({"m": failing_value}, [["m", op, threshold]]) is False


# ── cagr ──────────────────────────────────────────────────────────────────────────
def test_cagr_basic():
    # 100 → 121 over 2y = 10%
    assert bt.cagr(100, 121, 2) == pytest.approx(0.10)


def test_cagr_invalid_inputs():
    assert bt.cagr(100, 0, 2) is None       # wipeout
    assert bt.cagr(0, 100, 2) is None       # zero base
    assert bt.cagr(100, 121, 0) is None     # zero horizon
    assert bt.cagr(None, 121, 2) is None


# ── max_drawdown ───────────────────────────────────────────────────────────────────
def test_max_drawdown_basic():
    # peak 120 → trough 90 = -25%
    assert bt.max_drawdown([100, 120, 90, 110]) == pytest.approx(-0.25)


def test_max_drawdown_monotonic_up_is_zero():
    assert bt.max_drawdown([100, 110, 130]) == pytest.approx(0.0)


def test_max_drawdown_empty_is_none():
    assert bt.max_drawdown([]) is None


def test_max_drawdown_skips_embedded_nan():
    # A NaN mid-series (e.g. a missing valuation year) must be skipped, not treated as 0 or as
    # breaking the peak-tracking — the drawdown is computed over the remaining real points only.
    curve = [100.0, 120.0, float("nan"), 90.0, 110.0]
    assert bt.max_drawdown(curve) == pytest.approx(-0.25)  # peak 120 → trough 90


# ── annualized_vol / sharpe ─────────────────────────────────────────────────────────
def test_annualized_vol_basic():
    # returns with a known sample std
    assert bt.annualized_vol([0.10, 0.20]) == pytest.approx(0.0707106781, abs=1e-6)


def test_annualized_vol_too_few():
    assert bt.annualized_vol([0.1]) is None


def test_sharpe_basic():
    r = [0.10, 0.20]
    expected = (0.15 - 0.04) / bt.annualized_vol(r)
    assert bt.sharpe(r, risk_free=0.04) == pytest.approx(expected)


def test_sharpe_zero_vol_is_none():
    assert bt.sharpe([0.1, 0.1, 0.1]) is None
