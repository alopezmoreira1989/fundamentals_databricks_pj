"""Build per-statement wide dataframes ready for render_table_html.

The export parquet is long-format (one row per concept × period). This module
pivots it into wide format (one row per concept, columns = fiscal years) and
decorates each row with indent / row_class metadata so the renderer is dumb.
"""

from __future__ import annotations

import pandas as pd

from .colors import row_class as derive_row_class, section_class


def _pivot_financials(
    data: pd.DataFrame,
    ticker: str,
    stmt: str,
    period_type_filter: str = "FY",
) -> pd.DataFrame:
    """Filter + pivot from long to wide on fiscal_year (or quarter label)."""
    mask = (
        (data["ticker"] == ticker)
        & (data["stmt"] == stmt)
        & (data["period_type"] == period_type_filter)
    )
    sub = data[mask].copy()
    if sub.empty:
        return pd.DataFrame()

    # For FY: pivot columns are fiscal_year integers.
    # Sort by sort_order (hierarchy position) within the pivoted result.
    pivot = sub.pivot_table(
        index=["stmt", "section", "group", "concept", "display_name", "sort_order"],
        columns="fiscal_year",
        values="value",
        aggfunc="first",
    ).reset_index()

    pivot = pivot.sort_values("sort_order", na_position="last").reset_index(drop=True)
    return pivot


def _decorate(pivot: pd.DataFrame) -> pd.DataFrame:
    """Add indent and row_class columns for the renderer."""
    if pivot.empty:
        return pivot

    indents = []
    classes = []
    for _, row in pivot.iterrows():
        stmt = row["stmt"]
        concept = row["concept"]
        display_name = row["display_name"]
        section = row.get("section")
        group = row.get("group")

        cls = derive_row_class(stmt, concept, display_name)
        classes.append(cls)

        # Indent rule: if the concept is inside a nested group (group != section
        # and group is not null), it gets indent-1 — UNLESS it's a subtotal/grand-total.
        has_inner_group = pd.notna(group) and group != section and group != ""
        is_styled_row = cls in ("subtotal", "grand-total", "headline")
        indent = 1 if has_inner_group and not is_styled_row else 0
        indents.append(indent)

    pivot["indent"] = indents
    pivot["row_class"] = classes
    return pivot


def _year_columns(pivot: pd.DataFrame) -> list[int]:
    """Extract the fiscal-year integer columns (sorted ascending)."""
    year_cols = [c for c in pivot.columns if isinstance(c, (int, float)) and c > 1900]
    return sorted(int(y) for y in year_cols)


def income_statement_df(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    pivot = _pivot_financials(data, ticker, "Income Statement")
    return _decorate(pivot)


def balance_sheet_df(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    pivot = _pivot_financials(data, ticker, "Balance Sheet")
    return _decorate(pivot)


def cash_flow_df(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    pivot = _pivot_financials(data, ticker, "Cash Flow")
    return _decorate(pivot)


def quarterly_df(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Wide dataframe for the quarterly mini-table + combo chart.

    Columns are quarter labels like '22-Q1' ... '24-Q4' in chronological order.
    Only Income Statement concepts are included (matching the reference HTML).
    """
    mask = (
        (data["ticker"] == ticker)
        & (data["stmt"] == "Income Statement")
        & (data["period_type"].isin(["Q1", "Q2", "Q3", "Q4"]))
    )
    sub = data[mask].copy()
    if sub.empty:
        return pd.DataFrame()

    # Build a sort-friendly label: fiscal_year * 10 + quarter_num.
    q_num_map = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    sub["q_sort"] = sub["fiscal_year"] * 10 + sub["period_type"].map(q_num_map)
    sub["q_label"] = sub.apply(
        lambda r: f"'{str(int(r['fiscal_year']))[-2:]}-{r['period_type']}", axis=1
    )

    # Keep last 12 distinct quarter labels.
    ordered_qs = (
        sub[["q_sort", "q_label"]]
        .drop_duplicates()
        .sort_values("q_sort")
        .tail(12)["q_label"]
        .tolist()
    )
    sub = sub[sub["q_label"].isin(ordered_qs)]

    pivot = sub.pivot_table(
        index=["stmt", "section", "group", "concept", "display_name", "sort_order"],
        columns="q_label",
        values="value",
        aggfunc="first",
    ).reset_index()

    # Reorder columns to match chronological quarter order.
    meta_cols = ["stmt", "section", "group", "concept", "display_name", "sort_order"]
    q_cols_in_pivot = [c for c in ordered_qs if c in pivot.columns]
    pivot = pivot[meta_cols + q_cols_in_pivot]

    pivot = pivot.sort_values("sort_order", na_position="last").reset_index(drop=True)
    return _decorate(pivot)


def get_year_columns(df: pd.DataFrame) -> list:
    """Public helper — return the ordered value columns (FY ints or quarter label strings)."""
    meta = {"stmt", "section", "group", "concept", "display_name", "sort_order", "indent", "row_class"}
    cols = [c for c in df.columns if c not in meta]
    # If integer (FY years), sort ascending; if string (quarter labels), sort by original order.
    if cols and isinstance(cols[0], (int, float)):
        return sorted(int(c) for c in cols)
    return cols  # already in order from pivot
