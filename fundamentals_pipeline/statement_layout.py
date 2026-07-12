"""Shared financial-statement row classification.

Both frontends (Streamlit's ``lib/tables.py``/``lib/colors.py`` and the Django web app's
``repositories/companies.py``) render the same ``00__config/concept_hierarchy.json`` tree and
must agree on which rows get visual emphasis (subtotal / grand-total / headline) and which are
indented as a nested child — this is the single source of truth so the two can't drift.

Import-safe: no Spark/Streamlit/Django dependency, unit-tested like ``valuation.py``/``fx.py``.
"""

from __future__ import annotations

# Concepts that render as a grand-total row (dark band, white text).
GRAND_TOTAL_CONCEPTS = {
    "Total Assets",
    "Total Liabilities & Equity",
}

# (stmt, concept) pairs that render as a headline row (accent-soft band).
HEADLINE_CONCEPTS = {
    ("Income Statement", "Net Income"),
    ("Cash Flow", "Net Change in Cash"),
}

# Concepts that render as a subtotal row (light bg, bold, "▸" marker).
SUBTOTAL_CONCEPTS = {
    "Gross Profit",
    "Operating Income",
    "Operating Cash Flow",
    "Investing Cash Flow",
    "Financing Cash Flow",
}

# row_class() return values that get visual emphasis (as opposed to a plain "" line item).
STYLED_ROW_CLASSES = frozenset({"subtotal", "grand-total", "headline"})


def row_class(stmt: str, concept: str, display_name: str) -> str:
    """CSS-class label for a statement row: ``"grand-total"`` | ``"headline"`` | ``"subtotal"``
    | ``""`` (plain line item). Concept-name-keyed so both frontends can't drift — extend the
    sets above when a new concept needs emphasis, never re-derive this classification locally.
    """
    if concept in GRAND_TOTAL_CONCEPTS:
        return "grand-total"
    if (stmt, concept) in HEADLINE_CONCEPTS:
        return "headline"
    if concept in SUBTOTAL_CONCEPTS:
        return "subtotal"
    if display_name.startswith("Total "):
        return "subtotal"
    return ""


def resolve_indent(section: str | None, group: str | None, cls: str) -> int:
    """1 if this row sits inside a nested ``group`` distinct from its ``section`` (a genuine
    concept_hierarchy.json sub-level, e.g. the Balance Sheet's "Current Assets" group under the
    "Assets" section) — 0 for section-level rows and for any subtotal/grand-total/headline row
    (those always render at the outer level regardless of which group they summarize).
    """
    has_inner_group = bool(group) and group != section
    return 1 if has_inner_group and cls not in STYLED_ROW_CLASSES else 0
