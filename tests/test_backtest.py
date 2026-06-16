"""Unit tests for the pure backtest helpers (_core/backtest.py)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from fundamentals_pipeline._core import backtest as bt


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
