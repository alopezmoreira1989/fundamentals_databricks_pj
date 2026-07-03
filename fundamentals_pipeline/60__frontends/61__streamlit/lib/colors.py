"""Sparkline stroke color per row, plus CSS-class helpers."""

from __future__ import annotations

# Palette — must match the CSS variables in styles.css.
GREEN   = "#0F6E56"   # --positive
BLUE    = "#185FA5"   # --accent
GRAY    = "#888780"   # --ink-3
CORAL   = "#993C1D"   # --negative
AMBER   = "#BA7517"   # --warning
PURPLE  = "#534AB7"   # per-share rows
CREAM   = "#FAF8F4"   # --bg (for grand-total rows on dark background)

# Concepts that get a specific color regardless of statement.
_CONCEPT_COLORS: dict[str, str] = {
    # Headlines / subtotals → green (trend-positive) or blue (volume).
    "Revenue":                  GREEN,
    "Gross Profit":             GREEN,
    "Operating Income":         GREEN,
    "Operating Cash Flow":      GREEN,
    "Net Income":               BLUE,
    "Total Assets":             BLUE,
    "Total Liabilities & Equity": BLUE,
    "Net Change in Cash":       BLUE,

    # R&D = investment, blue.
    "R&D Expense":              BLUE,

    # Per-share rows.
    "EPS Basic":                PURPLE,
    "EPS Diluted":              PURPLE,
    "Shares Diluted":           GRAY,

    # Negative-trending balance sheet items.
    "Short-term Debt":          CORAL,
    "Long-term Debt":           CORAL,
    "Retained Earnings":        CORAL,  # AAPL case — buybacks drove this negative
    "Income Tax":               CORAL,

    # Investing.
    "CapEx":                    AMBER,
    "Acquisitions":             AMBER,
    "Investing Cash Flow":      AMBER,
    "Purchases of Investments": AMBER,
    "Sales of Investments":     AMBER,

    # Financing.
    "Debt Repayment":           CORAL,
    "Share Repurchases":        CORAL,
    "Financing Cash Flow":      CORAL,
    "Dividends Paid":           GRAY,
    "Debt Issuance":            GRAY,
}


def row_color(stmt: str, concept: str) -> str:
    """Pick a sparkline stroke color for a row."""
    if concept in _CONCEPT_COLORS:
        return _CONCEPT_COLORS[concept]
    # Default: gray for everything else (cost lines, generic BS items).
    return GRAY


# Concepts that render as a grand-total row (dark band, white text).
GRAND_TOTAL_CONCEPTS = {
    "Total Assets",
    "Total Liabilities & Equity",
}

# Concepts that render as a headline row (accent-soft band).
HEADLINE_CONCEPTS = {
    ("Income Statement", "Net Income"),
    ("Cash Flow", "Net Change in Cash"),
}

# Concepts that render as a subtotal row (light bg, bold ▸ label).
SUBTOTAL_CONCEPTS = {
    "Gross Profit",
    "Operating Income",
    "Operating Cash Flow",
    "Investing Cash Flow",
    "Financing Cash Flow",
}


def row_class(stmt: str, concept: str, display_name: str) -> str:
    """Pick the CSS class for a regular concept row."""
    if concept in GRAND_TOTAL_CONCEPTS:
        return "grand-total"
    if (stmt, concept) in HEADLINE_CONCEPTS:
        return "headline"
    if concept in SUBTOTAL_CONCEPTS:
        return "subtotal"
    if display_name.startswith("Total "):
        return "subtotal"
    return ""


# Section-row color modifier per (stmt, section).
_SECTION_CLASSES: dict[tuple[str, str], str] = {
    ("Balance Sheet", "Assets"):              "assets",
    ("Balance Sheet", "Liabilities & Equity"): "liab",
    ("Cash Flow", "Operating Activities"):     "operating",
    ("Cash Flow", "Investing Activities"):     "investing",
    ("Cash Flow", "Financing Activities"):     "financing",
    ("Cash Flow", "Net Change"):               "netchange",
}


def section_class(stmt: str, section: str) -> str:
    return _SECTION_CLASSES.get((stmt, section), "")


# Per-share concepts — render with .num.eps (different format, 2 decimals).
PER_SHARE_CONCEPTS = {"EPS Basic", "EPS Diluted"}


def is_negative_concept(concept: str) -> bool:
    """Concepts that the SEC reports as positives but conceptually represent outflows.

    We render these with muted .num (parens) to match the editorial style — costs,
    capex, tax, etc. Returns True if values should be displayed as negatives.
    """
    return concept in {
        "Cost of Revenue", "Operating Expenses", "R&D Expense", "SG&A Expense",
        "Interest Expense", "Income Tax",
        "CapEx", "Acquisitions", "Purchases of Investments",
        "Debt Repayment", "Dividends Paid", "Share Repurchases",
    }
