"""Company read repository — descriptive facts + latest-FY metrics for a ticker."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import duckdb
from fundamentals_pipeline.statement_layout import resolve_indent
from fundamentals_pipeline.statement_layout import row_class as _row_class
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

# Market Cap's category is NULL in the artifact by design (screener-only, invisible in the
# metrics grid — mirrors the Streamlit app; see 51__export_dashboard_data.py's injection
# comment), so it needs its own targeted fetch rather than _LATEST_METRICS_SQL's
# category-IS-NOT-NULL filter. period_end (a real date) is included for the USD-lens toggle's
# date-anchored FX lookup (see usd_fx_rate below) — the only MetricPoint use case that needs it.
_MARKET_CAP_SQL = """
    SELECT ticker, 'Market Cap' AS metric, unit, fiscal_year, period_end, value
    FROM metrics
    WHERE ticker = ? AND metric = 'Market Cap' AND period_type = 'FY' AND value IS NOT NULL
    ORDER BY fiscal_year DESC
    LIMIT 1
"""

# Most recent currency->USD rate on or before `as_of` — the date-anchoring rule from
# fundamentals_pipeline/fx.py (never today's spot rate, never the SEC filed timestamp).
_FX_RATE_SQL = """
    SELECT rate FROM fx WHERE base = ? AND quote = 'USD' AND date <= ? ORDER BY date DESC LIMIT 1
"""

# Reported financial-statement line items across fiscal years (the raw statements, not the
# derived metrics). Ordered by the reporting hierarchy so the pivot below preserves line order.
# `concept` (the raw XBRL-derived concept name, not the possibly-overridden display_name) drives
# fundamentals_pipeline.statement_layout's row classification (subtotal/grand-total/headline) —
# see get_statements()/get_quarterly() below.
_STATEMENTS_SQL = """
    SELECT stmt, section, "group", concept, display_name, sort_order, fiscal_year, value
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
    SELECT section, "group", concept, display_name, sort_order, period_type, period_end, fiscal_year, value
    FROM financials
    WHERE ticker = ?
      AND stmt = ?
      AND period_type <> 'FY'
      AND value IS NOT NULL
    ORDER BY sort_order, display_name, period_end
"""

# Daily close series for the price chart, with SMA 20/50/200 computed on adj_close (split-safe)
# over the ticker's FULL history — the rolling window must see real trading days before any
# window/downsample filtering, or an SMA near the start of a short window would be wrong
# (starved of the lookback it needs). NULL until `rn` real trading days exist, matching the
# Streamlit app's pandas .rolling(win).mean() semantics exactly (never a partial-window average).
_PRICE_SERIES_SQL = """
    WITH ordered AS (
        SELECT date, close, adj_close, row_number() OVER (ORDER BY date) AS rn
        FROM prices
        WHERE ticker = ? AND close IS NOT NULL AND adj_close IS NOT NULL
    )
    SELECT date, close, adj_close,
        CASE WHEN rn >= 20  THEN AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) END AS sma20,
        CASE WHEN rn >= 50  THEN AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) END AS sma50,
        CASE WHEN rn >= 200 THEN AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) END AS sma200
    FROM ordered
    ORDER BY date
"""

# Quick-range windows (#231) — trailing day-count from the SERIES' OWN latest date (not "today",
# which may lag real publish dates). "Max" is a pass-through (no filter). Approximate calendar
# day-counts, not exact months/years — fine for a UI range button, avoids a date-arithmetic lib.
PRICE_WINDOW_DAYS: dict[str, int | None] = {
    "1M": 30, "3M": 91, "6M": 182, "1Y": 365, "5Y": 365 * 5, "Max": None,
}
PRICE_WINDOW_DEFAULT = "1Y"


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
                    market=rec.get("market"),
                    reporting_currency=_clean(rec.get("reporting_currency")),
                    accounting_standard=_clean(rec.get("accounting_standard")),
                    in_tsx_composite=_as_bool(rec.get("in_tsx_composite")),
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

    def market_cap(self, ticker: str) -> MetricPoint | None:
        """The ticker's latest-FY Market Cap, or ``None`` if unavailable (see _MARKET_CAP_SQL
        for why this bypasses latest_metrics())."""
        rows = self._fetch(_MARKET_CAP_SQL, [ticker], MetricPoint)
        return rows[0] if rows else None

    def usd_fx_rate(self, currency: str, as_of: str) -> float | None:
        """Most recent `currency`->USD rate on or before `as_of`, or ``None`` if the `fx` view
        is absent or carries no rate dated early enough — callers must keep the value in its
        native currency then, never silently guess (fundamentals_pipeline.fx's date-anchoring
        rule)."""
        try:
            rates = self._fetch_column(_FX_RATE_SQL, [currency.upper(), as_of])
        except duckdb.Error:
            return None
        return float(rates[0]) if rates and rates[0] is not None else None

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
        years = sorted({int(r[6]) for r in rows}, reverse=True)[:max_years]
        col = {year: i for i, year in enumerate(years)}

        statements: list[Statement] = []
        for stmt in _STATEMENT_ORDER:
            values: dict[str, list[float | None]] = {}
            sections: dict[str, str | None] = {}
            groups: dict[str, str | None] = {}
            concepts: dict[str, str] = {}
            for _stmt, section, group, concept, name, _sort, fiscal_year, value in rows:
                if _stmt != stmt or int(fiscal_year) not in col:
                    continue
                line = values.get(name)
                if line is None:
                    line = [None] * len(years)
                    values[name] = line
                    sections[name] = section
                    groups[name] = group
                    concepts[name] = concept
                line[col[int(fiscal_year)]] = value
            if not values:
                continue
            lines = tuple(
                StatementLine(
                    display_name=name, section=sections[name], values=tuple(vals), group=groups[name],
                    row_class=(cls := _row_class(stmt, concepts[name], name)),
                    indent=resolve_indent(sections[name], groups[name], cls),
                )
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
            for _s, _g, _c, _n, _o, period_type, period_end, fiscal_year, _v in rows
        }
        recent = sorted(labels, reverse=True)[:max_quarters]
        col = {period_end: i for i, period_end in enumerate(recent)}

        values: dict[str, list[float | None]] = {}
        sections: dict[str, str | None] = {}
        groups: dict[str, str | None] = {}
        concepts: dict[str, str] = {}
        for section, group, concept, name, _sort, _ptype, period_end, _fy, value in rows:
            if period_end not in col:
                continue
            line = values.get(name)
            if line is None:
                line = [None] * len(recent)
                values[name] = line
                sections[name] = section
                groups[name] = group
                concepts[name] = concept
            line[col[period_end]] = value
        lines = tuple(
            StatementLine(
                display_name=name, section=sections[name], values=tuple(vals), group=groups[name],
                row_class=(cls := _row_class(statement, concepts[name], name)),
                indent=resolve_indent(sections[name], groups[name], cls),
            )
            for name, vals in values.items()
        )
        return QuarterGrid(name=statement, columns=tuple(labels[q] for q in recent), lines=lines)

    def price_series(
        self, ticker: str, *, window: str = PRICE_WINDOW_DEFAULT, max_points: int = 500
    ) -> tuple[PricePoint, ...]:
        """Daily close series (ascending) with SMA 20/50/200, trimmed to the trailing ``window``
        (#231) and downsampled to at most ``max_points`` for charting.

        SMAs are computed over the ticker's FULL history (see _PRICE_SERIES_SQL) before the
        window/downsample filters below run — a rolling average needs real lookback data, which
        a pre-filtered slice wouldn't have near its own start. Returns ``()`` when the ticker has
        no prices or the optional prices view is absent — the price tab then degrades to a "no
        data" state rather than erroring."""
        try:
            with self._connection() as con:
                rows = con.execute(_PRICE_SERIES_SQL, [ticker]).fetchall()
        except duckdb.Error:
            return ()  # optional prices view not registered
        if not rows:
            return ()

        days = PRICE_WINDOW_DAYS.get(window)
        if days is not None:
            cutoff = max(r[0] for r in rows) - timedelta(days=days)
            rows = [r for r in rows if r[0] >= cutoff]

        count = len(rows)
        if count > max_points:
            stride = -(-count // max_points)  # ceil, so len(rows[::stride]) <= max_points
            sampled = rows[::stride]
            if sampled[-1] != rows[-1]:
                sampled.append(rows[-1])  # always keep the latest close
            rows = sampled
        return tuple(
            PricePoint(
                date=str(date), close=float(close),
                adj_close=float(adj) if adj is not None else None,
                sma20=float(s20) if s20 is not None else None,
                sma50=float(s50) if s50 is not None else None,
                sma200=float(s200) if s200 is not None else None,
            )
            for date, close, adj, s20, s50, s200 in rows
        )
