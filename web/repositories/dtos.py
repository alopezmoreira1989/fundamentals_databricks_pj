"""Immutable DTOs returned by the repository tier.

These frozen dataclasses are the read model the services/views consume — the boundary that
keeps SQL column names out of the upper layers. A repository maps query rows to these; if a
DuckDB column is renamed, only the repository's SELECT changes, not these types or anything
above them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CompanySummary:
    """Descriptive company facts (from the meta artifact)."""

    ticker: str
    name: str
    sector: str | None = None
    industry: str | None = None
    exchange: str | None = None
    country: str | None = None
    description: str | None = None
    website: str | None = None
    employees: int | None = None
    founded: str | None = None
    has_logo: bool | None = None


@dataclass(frozen=True, slots=True)
class MetricPoint:
    """One derived-metric value for a ticker at a fiscal year.

    ``category``/``subcategory``/``sort_order`` come from the metrics hierarchy and drive the
    grouped company view; they default to ``None`` for callers (e.g. valuation) that don't
    select them.
    """

    ticker: str
    metric: str
    unit: str | None
    fiscal_year: int
    value: float | None
    category: str | None = None
    subcategory: str | None = None
    sort_order: float | None = None


@dataclass(frozen=True, slots=True)
class CompanyDetail:
    """A company summary plus its latest-FY metrics (service-level aggregate)."""

    summary: CompanySummary
    metrics: tuple[MetricPoint, ...]


@dataclass(frozen=True, slots=True)
class StatementLine:
    """One financial-statement line item: its value across the displayed fiscal years, aligned
    to :attr:`Statement.years` (``None`` where a year has no reported value)."""

    display_name: str
    section: str | None
    values: tuple[float | None, ...]
    group: str | None = None


@dataclass(frozen=True, slots=True)
class Statement:
    """One financial statement (Income Statement / Balance Sheet / Cash Flow) as a line-item ×
    fiscal-year grid, ordered by the reporting hierarchy."""

    name: str
    years: tuple[int, ...]
    lines: tuple[StatementLine, ...]


@dataclass(frozen=True, slots=True)
class CompanyStatements:
    """A ticker's financial statements, ready for tabbed display."""

    statements: tuple[Statement, ...]


@dataclass(frozen=True, slots=True)
class QuarterGrid:
    """A statement's line items across recent fiscal quarters (newest first). Like
    :class:`Statement` but the columns are quarter labels (e.g. ``"Q2 2026"``), not years."""

    name: str
    columns: tuple[str, ...]
    lines: tuple[StatementLine, ...]


@dataclass(frozen=True, slots=True)
class PricePoint:
    """One point on the price series: an ISO date and its close."""

    date: str
    close: float


@dataclass(frozen=True, slots=True)
class HeadlineKpi:
    """A single headline figure (latest fiscal year) for the company overview strip."""

    label: str
    value: float | None
    fiscal_year: int | None


@dataclass(frozen=True, slots=True)
class ScreenRow:
    """One screener hit: a ticker's latest-FY value for the screened metric."""

    ticker: str
    fiscal_year: int
    value: float | None


@dataclass(frozen=True, slots=True)
class CompanyListRow:
    """One row of the paginated company table: descriptive facts (from the meta artifact) plus,
    when a metric filter is active, that ticker's latest-FY value of the screened metric."""

    ticker: str
    name: str
    sector: str | None = None
    industry: str | None = None
    metric_value: float | None = None
    fiscal_year: int | None = None
    has_logo: bool | None = None


@dataclass(frozen=True, slots=True)
class CompanyPage:
    """A page of the company table plus the total match count (for pagination in the view)."""

    rows: tuple[CompanyListRow, ...]
    total: int


@dataclass(frozen=True, slots=True)
class FootballBar:
    """One intrinsic-value estimate as a bear→bull range with a mid point (per-share, USD)."""

    method: str
    bear: float
    mid: float
    bull: float
    fiscal_year: int


@dataclass(frozen=True, slots=True)
class MosScenario:
    """A method's Margin-of-Safety across the three scenarios, on one basis (TTM or FY).

    Collapses the flat ``MoS % (<method>, <basis>)`` / ``… — Bear`` / ``… — Bull`` metrics into a
    single row so the view can render aligned Bear / Mid / Bull columns."""

    method: str  # "Graham Revised", "DCF", "Owner Earnings", "Graham Number"
    basis: str   # "TTM" or "FY"
    bear: float | None
    mid: float | None
    bull: float | None
    fiscal_year: int | None


@dataclass(frozen=True, slots=True)
class FootballField:
    """The valuation "football field": per-method IV ranges plus the current market price."""

    bars: tuple[FootballBar, ...]
    price: float | None
