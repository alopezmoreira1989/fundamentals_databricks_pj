"""Unit tests for the scalar valuation reference implementations (_core/valuation.py)."""

from __future__ import annotations

import math

import pytest

from fundamentals_core import valuation as v


# ── safe_div ──────────────────────────────────────────────────────────────────
def test_safe_div_basic():
    assert v.safe_div(10, 2) == 5.0


def test_safe_div_zero_denominator_is_none():
    assert v.safe_div(1, 0) is None


def test_safe_div_missing_operand_is_none():
    assert v.safe_div(5, None) is None
    assert v.safe_div(None, 2) is None
    assert v.safe_div(5, float("nan")) is None


def test_safe_div_tiny_denominator_is_finite_not_explosion():
    # KLIC-style near-zero earnings: a multiple gets large but stays finite (no raise,
    # no inf). The pipeline's job is to surface the big number, not crash.
    out = v.safe_div(1e9, 1e-6)
    assert out == pytest.approx(1e15)
    assert math.isfinite(out)


# ── pe_ratio ──────────────────────────────────────────────────────────────────
def test_pe_ratio_positive():
    assert v.pe_ratio(1_000_000, 100_000) == pytest.approx(10.0)


def test_pe_ratio_zero_ni_returns_none():
    assert v.pe_ratio(1_000_000, 0) is None


def test_pe_ratio_negative_ni_returns_none():
    assert v.pe_ratio(1_000_000, -50_000) is None


def test_pe_ratio_missing_returns_none():
    assert v.pe_ratio(None, 100_000) is None
    assert v.pe_ratio(1_000_000, None) is None
    assert v.pe_ratio(1_000_000, float("nan")) is None


# ── earnings_yield ──────────────────────────────────────────────────────────────
def test_earnings_yield_positive():
    # 100_000 / 1_000_000 * 100 = 10.0
    assert v.earnings_yield(100_000, 1_000_000) == pytest.approx(10.0)


def test_earnings_yield_zero_ni_returns_none():
    assert v.earnings_yield(0, 1_000_000) is None


def test_earnings_yield_negative_ni_returns_none():
    assert v.earnings_yield(-50_000, 1_000_000) is None


def test_earnings_yield_missing_or_zero_mcap_returns_none():
    assert v.earnings_yield(100_000, None) is None
    assert v.earnings_yield(100_000, 0) is None
    assert v.earnings_yield(None, 1_000_000) is None


def test_pe_ratio_and_earnings_yield_are_inverses():
    # EY% = 100 / (P/E) — both guard NI > 0 identically.
    pe = v.pe_ratio(1_000_000, 80_000)
    ey = v.earnings_yield(80_000, 1_000_000)
    assert pe is not None and ey is not None
    assert ey == pytest.approx(100.0 / pe)


# ── graham_number ─────────────────────────────────────────────────────────────
def test_graham_number_basic():
    # sqrt(22.5 * 5 * 20) = sqrt(2250)
    assert v.graham_number(5, 20) == pytest.approx(math.sqrt(2250.0))


def test_graham_number_negative_eps_is_none():
    assert v.graham_number(-1, 20) is None


def test_graham_number_negative_or_zero_bvps_is_none():
    assert v.graham_number(5, -3) is None
    assert v.graham_number(5, 0) is None


def test_graham_number_missing_is_none():
    assert v.graham_number(None, 20) is None
    assert v.graham_number(5, float("nan")) is None


def test_graham_number_near_zero_eps_does_not_explode():
    out = v.graham_number(1e-6, 20)
    assert out is not None and out < 0.1 and math.isfinite(out)


# ── graham_revised ────────────────────────────────────────────────────────────
def test_graham_revised_basic():
    # 5 * (8.5 + 2*0.08*100) * 4.4 / (0.055*100) = 5 * 24.5 * 4.4 / 5.5 = 98.0
    assert v.graham_revised(5, 0.08, 0.055) == pytest.approx(98.0)


def test_graham_revised_growth_capped_at_15pct():
    # growth 0.50 clipped to 0.15: 5 * (8.5 + 30) * 4.4 / 5.5 = 154.0
    assert v.graham_revised(5, 0.50, 0.055) == pytest.approx(154.0)


def test_graham_revised_negative_growth_floored_to_zero():
    # growth -0.20 floored to 0: 5 * 8.5 * 4.4 / 5.5 = 34.0
    assert v.graham_revised(5, -0.20, 0.055) == pytest.approx(34.0)


def test_graham_revised_negative_eps_is_none():
    assert v.graham_revised(-2, 0.08, 0.055) is None


def test_graham_revised_missing_growth_treated_as_zero():
    # NaN growth -> 0 (the Spark fallback to dcf_g1 is a pipeline concern, not the scalar)
    assert v.graham_revised(5, None, 0.055) == pytest.approx(34.0)


# ── dcf_value ─────────────────────────────────────────────────────────────────
def test_dcf_value_one_year_no_growth():
    # year 1: cf=100, pv1=100/1.1; terminal tv=100/0.10=1000, pv_term=1000/1.1
    # total = (100 + 1000)/1.1 = 1000.0
    assert v.dcf_value(100, 0.0, 0.10, 0.0, 1) == pytest.approx(1000.0)


def test_dcf_value_non_positive_fcf_is_none():
    assert v.dcf_value(-5, 0.08, 0.09, 0.025, 10) is None
    assert v.dcf_value(0, 0.08, 0.09, 0.025, 10) is None


def test_dcf_value_discount_not_above_terminal_is_none():
    assert v.dcf_value(100, 0.08, 0.025, 0.025, 10) is None
    assert v.dcf_value(100, 0.08, 0.02, 0.025, 10) is None


def test_dcf_value_zero_years_is_none():
    assert v.dcf_value(100, 0.08, 0.09, 0.025, 0) is None


# ── owner_earnings ────────────────────────────────────────────────────────────
def test_owner_earnings_basic():
    # 100 + 20 + 5 - 30 - 10 = 85
    assert v.owner_earnings(100, 20, 5, 30, 10) == pytest.approx(85.0)


def test_owner_earnings_missing_components_treated_as_zero():
    assert v.owner_earnings(100, None, None, 30, None) == pytest.approx(70.0)
    assert v.owner_earnings(None, None, None, None, None) == pytest.approx(0.0)


# ── eps_cagr ──────────────────────────────────────────────────────────────────
def test_eps_cagr_basic():
    # (121/100) ** (1/2) - 1 = 0.10
    assert v.eps_cagr(100, 121, 2) == pytest.approx(0.10)


def test_eps_cagr_flat_is_zero():
    assert v.eps_cagr(100, 100, 1) == pytest.approx(0.0)


def test_eps_cagr_non_positive_endpoint_is_none():
    assert v.eps_cagr(-1, 5, 3) is None
    assert v.eps_cagr(5, -1, 3) is None
    assert v.eps_cagr(0, 5, 3) is None


def test_eps_cagr_zero_years_is_none():
    assert v.eps_cagr(100, 121, 0) is None


def test_eps_cagr_near_zero_base_is_large_but_finite():
    out = v.eps_cagr(0.01, 5, 3)
    assert out is not None and out > 1.0 and math.isfinite(out)
