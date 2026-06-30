"""Tests for the cumulative split-factor reference (_core/splits.py).

Mirrors the Spark factor used by 22 (Net Buyback Yield % / Piotroski) and 23 (EPS-CAGR
growth input). NVDA is the canonical multi-split case: 4:1 on 2021-07-20, 10:1 on 2024-06-10.
"""

from __future__ import annotations

from datetime import date

from fundamentals_core import splits as s

NVDA = [(date(2021, 7, 20), 4.0), (date(2024, 6, 10), 10.0)]


def test_no_splits_factor_one():
    assert s.cumulative_split_factor(date(2024, 1, 28), []) == 1.0


def test_before_both_splits_compounds():
    # A period ending before both NVDA splits must be scaled by 4 × 10 = 40.
    assert s.cumulative_split_factor(date(2021, 1, 31), NVDA) == 40.0


def test_between_splits_uses_only_later():
    # After the 4:1 but before the 10:1 → only the 10:1 applies.
    assert s.cumulative_split_factor(date(2023, 1, 29), NVDA) == 10.0


def test_after_all_splits_factor_one():
    assert s.cumulative_split_factor(date(2025, 1, 26), NVDA) == 1.0


def test_reverse_split():
    # A 1-for-10 reverse split is ratio 0.1 and rescales the same way.
    assert s.cumulative_split_factor(date(2019, 1, 1), [(date(2020, 1, 1), 0.1)]) == 0.1


def test_ex_date_on_period_end_is_excluded():
    # A split dated exactly on period_end already affected that period → not applied (strict >).
    assert s.cumulative_split_factor(date(2024, 6, 10), NVDA) == 1.0


def test_order_independent_and_skips_bad_ratios():
    out_of_order = [(date(2024, 6, 10), 10.0), (date(2021, 7, 20), 4.0)]
    assert s.cumulative_split_factor(date(2021, 1, 31), out_of_order) == 40.0
    # zero / None ratios are bad data and skipped
    assert s.cumulative_split_factor(date(2021, 1, 31), [(date(2022, 1, 1), 0.0)]) == 1.0
