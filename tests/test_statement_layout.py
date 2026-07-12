"""Tests for fundamentals_pipeline/statement_layout.py — shared row classification used by
both the Streamlit and Django frontends' financial-statement tables."""

from __future__ import annotations

from fundamentals_pipeline.statement_layout import resolve_indent, row_class


def test_grand_total_concepts_are_classified():
    assert row_class("Balance Sheet", "Total Assets", "Total Assets") == "grand-total"
    assert row_class("Balance Sheet", "Total Liabilities & Equity", "Total Liabilities & Equity") == "grand-total"


def test_headline_concepts_are_statement_scoped():
    assert row_class("Income Statement", "Net Income", "Net Income") == "headline"
    assert row_class("Cash Flow", "Net Change in Cash", "Net Change in Cash") == "headline"
    # Same concept name on a different statement isn't a headline — the pair is scoped.
    assert row_class("Balance Sheet", "Net Income", "Net Income") != "headline"


def test_subtotal_concepts_are_classified():
    for concept in ("Gross Profit", "Operating Income", "Operating Cash Flow"):
        assert row_class("Income Statement", concept, concept) == "subtotal"


def test_total_prefix_fallback_catches_unlisted_subtotals():
    # Total Current Assets / Total Liabilities aren't in SUBTOTAL_CONCEPTS explicitly —
    # caught by the "Total " display-name prefix instead.
    assert row_class("Balance Sheet", "Total Current Assets", "Total Current Assets") == "subtotal"
    assert row_class("Balance Sheet", "Total Liabilities", "Total Liabilities") == "subtotal"


def test_plain_line_item_has_no_class():
    assert row_class("Income Statement", "Revenue", "Revenue") == ""
    assert row_class("Balance Sheet", "Cash & Equivalents", "Cash & Equivalents") == ""


def test_indent_applies_only_inside_a_distinct_inner_group():
    # Nested under a real inner group (Current Assets != Assets section) -> indented.
    assert resolve_indent("Assets", "Current Assets", "") == 1
    # No group, or group == section -> not indented (Income Statement has no inner group level).
    assert resolve_indent("Revenue", None, "") == 0
    assert resolve_indent("Assets", "Assets", "") == 0
    assert resolve_indent("Assets", "", "") == 0


def test_styled_rows_never_indent_even_inside_a_group():
    # A subtotal/grand-total/headline row always renders at the outer level, regardless of
    # which inner group it summarizes.
    assert resolve_indent("Assets", "Current Assets", "subtotal") == 0
    assert resolve_indent("Assets", "Current Assets", "grand-total") == 0
    assert resolve_indent("Income Statement", "Bottom Line", "headline") == 0
