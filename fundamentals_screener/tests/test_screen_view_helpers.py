"""Tests for pure helper functions in `views.py` supporting the General Screener's "Metric
filters" panel — specifically `_count_bound_filters`, which fixes a real regression (2026-08-23,
caught live): the panel/badge showed "active" forever after ANY interaction (Sector dropdown,
Currency, Scale, ...), not just after the user actually bounded a metric filter.

Pure function, no Django/DuckDB/network dependency.
"""

from __future__ import annotations

from fundamentals_screener.repositories.company_listing import MetricFilter
from fundamentals_screener.views import _count_bound_filters


def test_bound_less_filters_are_not_counted():
    # The exact regression: every Columns-picker selection (including the DEFAULT columns,
    # pre-checked with zero user interaction) mirrors into a blank Metric-filters row, and
    # those blank rows' real (if empty) fmetric/fmin/fmax fields get submitted on ANY filter
    # change -- but a row with no bound at all doesn't actually constrain anything.
    filters = [
        MetricFilter(metric="Market Cap (Live)"),
        MetricFilter(metric="P/E (TTM, live)"),
        MetricFilter(metric="Current Ratio"),
        MetricFilter(metric="Debt / Equity"),
    ]
    assert _count_bound_filters(filters) == 0


def test_a_min_only_bound_counts():
    filters = [MetricFilter(metric="Current Ratio", min_value=1.0)]
    assert _count_bound_filters(filters) == 1


def test_a_max_only_bound_counts():
    filters = [MetricFilter(metric="P/E", max_value=15.0)]
    assert _count_bound_filters(filters) == 1


def test_mixed_bound_and_bound_less_rows_counts_only_the_bound_ones():
    filters = [
        MetricFilter(metric="Market Cap (Live)"),  # blank, from the Columns mirror
        MetricFilter(metric="P/E", max_value=15.0),  # genuinely bounded
        MetricFilter(metric="Current Ratio", min_value=1.0, max_value=3.0),  # genuinely bounded
        MetricFilter(metric="Debt / Equity"),  # blank, from the Columns mirror
    ]
    assert _count_bound_filters(filters) == 2


def test_empty_list_counts_zero():
    assert _count_bound_filters([]) == 0
