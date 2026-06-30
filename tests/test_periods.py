"""Unit tests for the scalar Q4 period arithmetic (_core/periods.py)."""

from __future__ import annotations

import pytest

from fundamentals_core import periods as p


def test_flow_additive_subtracts_ytd_from_fy():
    # Q4 = FY - YTD_Q3 (intentional: captures year-end audit adjustments)
    assert p.q4_from_fy_ytd(100.0, 70.0, "flow_additive") == pytest.approx(30.0)


def test_flow_additive_missing_input_is_none():
    assert p.q4_from_fy_ytd(None, 70.0, "flow_additive") is None
    assert p.q4_from_fy_ytd(100.0, None, "flow_additive") is None
    assert p.q4_from_fy_ytd(float("nan"), 70.0, "flow_additive") is None


def test_flow_nonadditive_is_always_none():
    # Cannot derive a non-additive flow by subtraction; 21b keeps only reported standalones.
    assert p.q4_from_fy_ytd(100.0, 70.0, "flow_nonadditive") is None


def test_stock_returns_fy_snapshot_unchanged():
    # A balance-sheet snapshot at fiscal-year-end == the FY snapshot; never subtracted.
    assert p.q4_from_fy_ytd(500.0, 480.0, "stock") == pytest.approx(500.0)


def test_stock_missing_fy_is_none():
    assert p.q4_from_fy_ytd(None, 480.0, "stock") is None


def test_unknown_kind_raises():
    with pytest.raises(ValueError):
        p.q4_from_fy_ytd(100.0, 70.0, "flow_weird")
