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


def test_total_liabilities_is_derived_from_le_total_even_when_explicit_concept_disagrees():
    # The explicit "Total Liabilities" tag (4.0B) is deliberately WRONG here (doesn't reconcile
    # with le_total - equity = 10.0 - 6.0 = 4.0... use a mismatched value instead so this test
    # actually proves the explicit concept is ignored, not just coincidentally consistent with
    # it): set it to 2.5B, well below the true 4.0B gap. The derived remainder must still close
    # the stack to 100% using le_total/equity, not the (understated) explicit tag.
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 10.0e9),
        _line("Total Assets", "Assets", None, 10.0e9),
        _line("Accounts Payable", "Liabilities & Equity", "Current Liabilities", 3.0e9),
        _line("Total Liabilities", "Liabilities & Equity", None, 2.5e9),  # understated on purpose
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 6.0e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 10.0e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    le = comps[0].liabilities_equity
    # Derived total_liabilities = le_total - equity = 10.0 - 6.0 = 4.0B.
    # Other liabilities = 4.0B - Accounts Payable (3.0B) = 1.0B -- NOT 2.5B - 3.0B (negative,
    # which _with_other would have silently dropped, leaving the stack short).
    assert "Other liabilities" in _segment_names(le)
    other = next(s for s in le.segments if s.name == "Other liabilities")
    assert other.value == pytest.approx(1.0e9)
    assert _pct_sum(le) == pytest.approx(100.0)


def test_noncontrolling_interest_closes_the_gap_for_up_c_structures():
    # Real bug (Viper Energy / VNOM, 2026-10-27 screenshot): an Up-C structure where the parent
    # retains only a controlling stake in the operating LLC -- "Total Stockholders Equity" is
    # parent-only ($4.448B) and the pipeline already ingests "Non-Controlling Interests"
    # ($5.92B) as its own row, but balance_sheet_compositions() never read it, so le_total
    # ($12.671B) minus the tracked leaves/equity left a ~$5.9B gap with no segment to fill it.
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 0.013e9),
        _line("PP&E Net", "Assets", "Non-Current Assets", 12.209e9),
        _line("Total Assets", "Assets", None, 12.671e9),
        _line("LT Debt", "Liabilities & Equity", "Non-Current Liabilities", 2.186e9),
        _line("Total Liabilities", "Liabilities & Equity", None, 2.308e9),
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 4.448e9),
        _line("Non-Controlling Interests", "Liabilities & Equity", "Stockholders Equity", 5.92e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 12.671e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    le = comps[0].liabilities_equity
    names = _segment_names(le)
    assert "Noncontrolling Interest" in names
    nci = next(s for s in le.segments if s.name == "Noncontrolling Interest")
    assert nci.value == pytest.approx(5.92e9)
    # Both bars must now reach (approximately) the same 100% -- the whole point of the chart.
    assert _pct_sum(le) == pytest.approx(100.0, abs=0.1)
    assert _pct_sum(comps[0].assets) == pytest.approx(100.0, abs=0.1)


def test_no_noncontrolling_interest_segment_when_concept_is_absent():
    # The overwhelming majority of companies have no NCI at all -- confirm the new segment
    # simply doesn't appear (not a zero-value row) and nothing else regresses.
    lines = (
        _line("Cash & Equivalents", "Assets", "Current Assets", 10.0e9),
        _line("Total Assets", "Assets", None, 10.0e9),
        _line("Accounts Payable", "Liabilities & Equity", "Current Liabilities", 4.0e9),
        _line("Total Stockholders Equity", "Liabilities & Equity", None, 6.0e9),
        _line("Total Liabilities & Equity", "Liabilities & Equity", None, 10.0e9),
    )
    comps = balance_sheet_compositions(_statement(lines))
    le = comps[0].liabilities_equity
    assert "Noncontrolling Interest" not in _segment_names(le)
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
