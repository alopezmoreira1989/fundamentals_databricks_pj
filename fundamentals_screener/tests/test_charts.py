"""Tests for fundamentals_screener/charts.py's balance_sheet_compositions().

Pure dataclass derivation, no Django/DuckDB needed -- same style as pricechart's own tests.
"""

from __future__ import annotations

import pytest
from fundamentals_screener.charts import balance_sheet_compositions
from fundamentals_screener.dtos import Statement, StatementLine


def _line(display_name, section, group, value):
    return StatementLine(display_name=display_name, section=section, group=group, values=(value,))


def _statement(lines):
    return Statement(name="Balance Sheet", years=(2026,), lines=lines, period_ends=("2026-05-31",))


def _segment_names(stack):
    return [s.name for s in stack.segments]


def _pct_sum(stack):
    return sum(s.pct for s in stack.segments)


def test_both_stacks_reach_100_percent_when_total_liabilities_concept_is_missing():
    # Real bug (Nike, 2026-10-02 screenshot): the filer doesn't tag an aggregate `Liabilities`
    # XBRL concept, so "Total Liabilities" is NULL for this fiscal year. Accounts Payable
    # ($3.6B) + LT Debt ($5.9B) + Equity ($14.9B) summed to well under le_total ($38.4B) with
    # no "Other liabilities" segment to make up the gap -- the Liabilities & Equity bar
    # rendered visibly shorter than the Assets bar even though both stacks' own totals (38.4B)
    # were equal. No "Total Liabilities" line is included here at all, reproducing the gap.
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 7.6e9),
        _line("Accounts Receivable", "Assets", "Current Assets", 5.9e9),
        _line("PP&E Net", "Assets", "Non-Current Assets", 4.8e9),
        _line("Total Assets", "Assets", None, 38.4e9),
        _line("Accounts Payable", "Liabilities & Equity", "Current Liabilities", 3.6e9),
        _line("LT Debt", "Liabilities & Equity", "Non-Current Liabilities", 5.9e9),
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 14.9e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 38.4e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    assert len(comps) == 1
    le = comps[0].liabilities_equity
    assert le.total == 38.4e9
    # The derived "Other liabilities" remainder must appear, closing the gap.
    assert "Other liabilities" in _segment_names(le)
    assert _pct_sum(le) == pytest.approx(100.0)


def test_explicit_total_liabilities_used_when_present():
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 10.0e9),
        _line("Total Assets", "Assets", None, 10.0e9),
        _line("Accounts Payable", "Liabilities & Equity", "Current Liabilities", 3.0e9),
        _line("Total Liabilities", "Liabilities & Equity", None, 4.0e9),
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 6.0e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 10.0e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    le = comps[0].liabilities_equity
    # Other liabilities = Total Liabilities (4.0B, explicit) - Accounts Payable (3.0B) = 1.0B.
    assert "Other liabilities" in _segment_names(le)
    other = next(s for s in le.segments if s.name == "Other liabilities")
    assert other.value == pytest.approx(1.0e9)
    assert _pct_sum(le) == pytest.approx(100.0)


def test_total_assets_falls_back_to_le_total_when_missing():
    # "Total Assets" itself NULL -- derived from Total Liabilities & Equity (equal by definition).
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 8.0e9),
        _line("Accounts Payable", "Liabilities & Equity", "Current Liabilities", 2.0e9),
        _line("Total Liabilities", "Liabilities & Equity", None, 2.0e9),
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 8.0e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 10.0e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    assert len(comps) == 1
    assets = comps[0].assets
    assert assets.total == pytest.approx(10.0e9)
    assert "Other assets" in _segment_names(assets)
    assert _pct_sum(assets) == pytest.approx(100.0)


def test_no_composition_when_both_totals_are_missing():
    lines = (_line("Cash & Equivalents", "Assets", "Current Assets", 8.0e9),)
    assert balance_sheet_compositions(_statement(lines)) == ()
