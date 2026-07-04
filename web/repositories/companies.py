"""Company read repository — descriptive facts + latest-FY metrics for a ticker."""

from __future__ import annotations

from typing import Any

import duckdb
from infrastructure.storage import meta as load_meta

from .base import DuckDBRepository
from .dtos import (
    CompanyStatements,
    CompanySummary,
    MetricPoint,
    PricePoint,
    QuarterGrid,
    Statement,
    StatementLine,
)

# Latest available value per metric (the most recent FY is often a partial/TTM year that only
# carries intrinsic-value metrics, so per-metric — not single-year — gives the full picture).
# The QUALIFY picks each metric's newest FY inside DuckDB; ordered by the hierarchy's
# sort_order so categories come out as contiguous blocks the template can regroup.
_LATEST_METRICS_SQL = """
    SELECT ticker, metric, unit, fiscal_year, value, category, subcategory, sort_order
    FROM metrics
    WHERE ticker = ?
      AND period_type = 'FY'
      AND category IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY metric ORDER BY fiscal_year DESC) = 1
    ORDER BY sort_order, metric
    LIMIT ?
"""

# Reported financial-statement line items across fiscal years (the raw statements, not the
# derived metrics). Ordered by the reporting hierarchy so the pivot below preserves line order.
_STATEMENTS_SQL = """
    SELECT stmt, section, "group", display_name, sort_order, fiscal_year, value
    FROM financials
    WHERE ticker = ?
      AND period_type = 'FY'
      AND stmt IS NOT NULL
      AND value IS NOT NULL
    ORDER BY stmt, sort_order, display_name, fiscal_year
"""

# The three statements, in the order the tabs present them.
_STATEMENT_ORDER = ("Income Statement", "Balance Sheet", "Cash Flow")


def _clean(value: Any) -> str | None:
    """Meta strings arrive stringified — treat empty / literal 'None' as missing."""
    text = str(value).strip() if value is not None else ""
    return text or None if text and text.lower() != "none" else None


def _as_int(value: Any) -> int | None:
    text = str(value).strip() if value is not None else ""
    return int(text) if text.isdigit() else None


def _as_bool(value: Any) -> bool | None:
    text = str(value).strip().lower() if value is not None else ""
    return True if text == "true" else False if text == "false" else None

# Quarterly line items for one statement (newest quarter first is applied after the fetch).
_QUARTERLY_SQL = """
    SELECT section, display_name, sort_order, period_type, period_end, fiscal_year, value
    FROM financials
    WHERE ticker = ?
      AND stmt = ?
      AND period_type <> 'FY'
      AND value IS NOT NULL
    ORDER BY sort_order, display_name, period_end
"""

# Daily close series for the price chart.
_PRICE_SERIES_SQL = "SELECT date, close FROM prices WHERE ticker = ? AND close IS NOT NULL ORDER BY date"


class CompanyRepository(DuckDBRepository):
    def get_summary(self, ticker: str) -> CompanySummary | None:
        """Descriptive facts from the meta artifact (``None`` if the ticker is unknown)."""
        records: list[dict[str, Any]] = load_meta().get("tickers", [])
        for rec in records:
            if rec.get("ticker") == ticker:
                return CompanySummary(
                    ticker=rec["ticker"],
                    name=rec.get("company", ""),
                    sector=rec.get("sector"),
                    industry=rec.get("industry"),
                    exchange=rec.get("exchange"),
                    country=rec.get("country"),
                    description=rec.get("description"),
                    website=_clean(rec.get("website")),
                    employees=_as_int(rec.get("employees")),
                    founded=_clean(rec.get("founded")),
                    has_logo=_as_bool(rec.get("has_logo")),
                )
        return None

    def latest_metrics(self, ticker: str, *, limit: int = 400) -> tuple[MetricPoint, ...]:
        """The latest available value of each derived metric, ordered for grouped display."""
        return self._fetch(_LATEST_METRICS_SQL, [ticker, limit], MetricPoint)

    def get_statements(self, ticker: str, *, max_years: int = 8) -> CompanyStatements:
        """The ticker's reported statements as line-item × fiscal-year grids (newest year first).

        One query over the ``financials`` view; the pivot into per-statement grids aligned to a
        common set of the most recent ``max_years`` fiscal years is assembled here, so the
        service/view receive ready DTOs and never touch a raw row or SQL column name.
        """
        with self._connection() as con:
            rows = con.execute(_STATEMENTS_SQL, [ticker]).fetchall()
        if not rows:
            return CompanyStatements(statements=())

        # Most recent `max_years` fiscal years, newest first → the display columns.
        years = sorted({int(r[5]) for r in rows}, reverse=True)[:max_years]
        col = {year: i for i, year in enumerate(years)}

        statements: list[Statement] = []
        for stmt in _STATEMENT_ORDER:
            values: dict[str, list[float | None]] = {}
            sections: dict[str, str | None] = {}
            groups: dict[str, str | None] = {}
            for _stmt, section, group, name, _sort, fiscal_year, value in rows:
                if _stmt != stmt or int(fiscal_year) not in col:
                    continue
                line = values.get(name)
                if line is None:
                    line = [None] * len(years)
                    values[name] = line
                    sections[name] = section
                    groups[name] = group
                line[col[int(fiscal_year)]] = value
            if not values:
                continue
            lines = tuple(
                StatementLine(display_name=name, section=sections[name], values=tuple(vals), group=groups[name])
                for name, vals in values.items()
            )
            statements.append(Statement(name=stmt, years=tuple(years), lines=lines))
        return CompanyStatements(statements=tuple(statements))

    def get_quarterly(
        self, ticker: str, *, statement: str = "Income Statement", max_quarters: int = 8
    ) -> QuarterGrid:
        """One statement's line items across the most recent ``max_quarters`` fiscal quarters
        (newest first), pivoted here into DTOs. Columns are quarter labels (``"Q2 2026"``)."""
        with self._connection() as con:
            rows = con.execute(_QUARTERLY_SQL, [ticker, statement]).fetchall()
        if not rows:
            return QuarterGrid(name=statement, columns=(), lines=())

        # Most recent quarters by period_end; label each fiscal-quarter + fiscal year (so
        # Apple's Q1 ending Dec 2025 reads "Q1 2026", ordering cleanly after Q4 2025).
        labels = {
            period_end: f"{period_type} {int(fiscal_year)}"
            for _s, _n, _o, period_type, period_end, fiscal_year, _v in rows
        }
        recent = sorted(labels, reverse=True)[:max_quarters]
        col = {period_end: i for i, period_end in enumerate(recent)}

        values: dict[str, list[float | None]] = {}
        sections: dict[str, str | None] = {}
        for section, name, _sort, _ptype, period_end, _fy, value in rows:
            if period_end not in col:
                continue
            line = values.get(name)
            if line is None:
                line = [None] * len(recent)
                values[name] = line
                sections[name] = section
            line[col[period_end]] = value
        lines = tuple(
            StatementLine(display_name=name, section=sections[name], values=tuple(vals))
            for name, vals in values.items()
        )
        return QuarterGrid(name=statement, columns=tuple(labels[q] for q in recent), lines=lines)

    def price_series(self, ticker: str, *, max_points: int = 260) -> tuple[PricePoint, ...]:
        """Daily close series (ascending), downsampled to at most ``max_points`` for charting.

        Returns ``()`` when the ticker has no prices or the optional prices view is absent —
        the price tab then degrades to a "no data" state rather than erroring."""
        try:
            with self._connection() as con:
                rows = con.execute(_PRICE_SERIES_SQL, [ticker]).fetchall()
        except duckdb.Error:
            return ()  # optional prices view not registered
        if not rows:
            return ()
        count = len(rows)
        if count > max_points:
            stride = -(-count // max_points)  # ceil, so len(rows[::stride]) <= max_points
            sampled = rows[::stride]
            if sampled[-1] != rows[-1]:
                sampled.append(rows[-1])  # always keep the latest close
            rows = sampled
        return tuple(PricePoint(date=str(date), close=float(close)) for date, close in rows)
