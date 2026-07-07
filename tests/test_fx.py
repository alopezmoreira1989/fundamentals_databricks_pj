"""Tests for the FX conversion helper (fundamentals_pipeline/fx.py)."""

from __future__ import annotations

import inspect

import pytest

from fundamentals_pipeline.fx import MissingFxRateError, convert_price


def test_matching_currency_is_a_noop():
    # No conversion needed — `rate` may legitimately be None; it must be ignored.
    assert convert_price(100.0, "USD", "USD", None) == 100.0
    assert convert_price(55.5, "CAD", "CAD", None) == 55.5


def test_mismatched_currency_converts():
    # CADUSD=X rate ~0.70 (1 CAD = 0.70 USD) — converting a CAD-quoted price into USD.
    result = convert_price(100.0, "CAD", "USD", 0.70)
    assert result == pytest.approx(70.0)


def test_missing_rate_raises():
    with pytest.raises(MissingFxRateError, match="CAD.*USD"):
        convert_price(100.0, "CAD", "USD", None)


def test_convert_price_signature_has_no_date_parameter():
    """convert_price() intentionally takes no date argument — date selection (a fiscal
    period_end, or a live price's own trade date; never `filed`, never "today") is entirely
    the CALLER's responsibility (the Spark as-of join in 22__derived_metrics.py /
    23__intrinsic_value.py), so it can never silently drift inside this pure function. This
    test locks down that contract — see fx.py's module docstring for the full rationale.
    """
    params = inspect.signature(convert_price).parameters
    assert "date" not in params
    assert "filed" not in params
