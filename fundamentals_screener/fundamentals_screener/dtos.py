"""Immutable DTOs returned by the repository tier.

These frozen dataclasses are the read model the views consume — the boundary that keeps SQL
column names out of the upper layers. A repository maps query rows to these; if a DuckDB
column is renamed, only the repository's SELECT changes, not these types or anything above
them.

Ported from fundamentals_databricks_pj's web/repositories/dtos.py. ``slots=True`` was dropped
from every dataclass here (Python 3.9 compatibility on the target hosting — the ``slots``
keyword argument to ``@dataclass`` was only added in Python 3.10).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class CompanySummary:
    """Descriptive company facts (from the meta artifact)."""

    ticker: str
    name: str
    sector: str | None = None
    industry: str | None = None
    exchange: str | None = None
    country: str | None = None
    market: str | None = None
    reporting_currency: str | None = None
    accounting_standard: str | None = None
    in_tsx_composite: bool | None = None
    description: str | None = None
    website: str | None = None
    employees: int | None = None
    founded: str | None = None
    has_logo: bool | None = None


@dataclass(frozen=True)
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
    period_end: str | None = None


@dataclass(frozen=True)
class CompanyDetail:
    """A company summary plus its latest-FY metrics (service-level aggregate)."""

    summary: CompanySummary
    metrics: tuple[MetricPoint, ...]


@dataclass(frozen=True)
class MetricSeries:
    """One derived metric's recent fiscal-year history for a ticker, for the Derived-metrics
    tab's sparkline + industry/sector benchmark columns.

    ``fiscal_years``/``values`` are newest-first and index-aligned (``values[0]`` is the latest
    value, at ``fiscal_years[0]``) — pass ``values`` straight to the ``|sparkline`` filter,
    which reverses to chronological order itself. ``peer_median``/``peer_count`` are ``None``/0
    until the service layer merges in the industry-or-sector benchmark (this DTO alone doesn't
    know the ticker's industry/sector — see ``services.get_metric_history``).
    """

    ticker: str
    metric: str
    unit: str | None
    category: str | None
    subcategory: str | None
    sort_order: float | None
    fiscal_years: tuple[int, ...]
    values: tuple[float | None, ...]
    peer_median: float | None = None
    peer_count: int = 0


@dataclass(frozen=True)
class PeerBenchmark:
    """One metric's peer-group median (across a ticker's industry-or-sector peers, each at
    their own latest reported fiscal year) — see ``CompanyRepository.industry_benchmark``."""

    metric: str
    peer_median: float
    peer_count: int


@dataclass(frozen=True)
class StatementLine:
    """One financial-statement line item: its value across the displayed fiscal years, aligned
    to :attr:`Statement.years` (``None`` where a year has no reported value).

    ``row_class``/``indent`` come from ``fundamentals_pipeline.statement_layout`` — ``row_class``
    is ``"grand-total"``/``"headline"``/``"subtotal"``/``""`` (plain line item); ``indent`` is 1
    for a line nested under a concept_hierarchy.json group, 0 for section-level rows and for any
    styled row.
    """

    display_name: str
    section: str | None
    values: tuple[float | None, ...]
    group: str | None = None
    row_class: str = ""
    indent: int = 0


@dataclass(frozen=True)
class Statement:
    """One financial statement (Income Statement / Balance Sheet / Cash Flow) as a line-item ×
    fiscal-year grid, ordered by the reporting hierarchy."""

    name: str
    years: tuple[int, ...]
    lines: tuple[StatementLine, ...]


@dataclass(frozen=True)
class CompanyStatements:
    """A ticker's financial statements, ready for tabbed display."""

    statements: tuple[Statement, ...]


@dataclass(frozen=True)
class QuarterGrid:
    """A statement's line items across recent fiscal quarters (newest first). Like
    :class:`Statement` but the columns are quarter labels (e.g. ``"Q2 2026"``), not years."""

    name: str
    columns: tuple[str, ...]
    lines: tuple[StatementLine, ...]


@dataclass(frozen=True)
class PricePoint:
    """One point on the price series: an ISO date, its raw close, and (for the chart) the
    split-adjusted close + SMA 20/50/200 — ``None`` until enough trading days exist to seed
    that average."""

    date: str
    close: float
    adj_close: float | None = None
    sma20: float | None = None
    sma50: float | None = None
    sma200: float | None = None


@dataclass(frozen=True)
class HeadlineKpi:
    """A single headline figure (latest fiscal year) for the company overview strip.

    ``currency`` is ``None`` for the statement-derived KPIs (Revenue, Net Income, …) — always
    the ticker's ``reporting_currency`` and rendered with the default ``$`` prefix; Market Cap
    sets it explicitly (native currency, or ``"USD"`` once converted by the USD-lens toggle) so
    the template can badge a non-USD value instead of assuming ``$``."""

    label: str
    value: float | None
    fiscal_year: int | None
    currency: str | None = None


@dataclass(frozen=True)
class ScreenRow:
    """One screener hit: a ticker's latest-FY value for the screened metric."""

    ticker: str
    fiscal_year: int
    value: float | None


@dataclass(frozen=True)
class CompanyListRow:
    """One row of the paginated company table: descriptive facts (from the meta artifact) plus,
    when a metric filter is active, that ticker's latest-FY value of the screened metric."""

    ticker: str
    name: str
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    market: str | None = None
    metric_value: float | None = None
    fiscal_year: int | None = None
    has_logo: bool | None = None


@dataclass(frozen=True)
class CompanyPage:
    """A page of the company table plus the total match count (for pagination in the view)."""

    rows: tuple[CompanyListRow, ...]
    total: int


@dataclass(frozen=True)
class ScreenColumn:
    """One selectable metric column of the multi-metric screener table.

    ``key`` is the metric name (the DuckDB/​artifact identifier); ``unit`` drives display
    formatting (``percent`` / ``usd`` / ``ratio`` …); ``category`` groups the metric in the
    column picker (from the metrics hierarchy)."""

    key: str
    unit: str | None = None
    category: str | None = None


@dataclass(frozen=True)
class ScreenTableRow:
    """One row of the multi-metric screener table: descriptive facts (from the meta artifact)
    plus this ticker's latest-FY value of each selected metric column.

    ``values`` maps a :class:`ScreenColumn` key → the ticker's latest-FY value (``None`` where
    the ticker has no reported value for that metric), so the view renders cells by column key
    without ever touching a SQL column name. ``units`` is the same row's own per-metric unit —
    most metrics carry one fixed unit column-wide (``ScreenColumn.unit`` covers those), but
    Market Cap's unit is each ticker's own native reporting currency, so it must be read per
    row, never assumed from the column."""

    ticker: str
    name: str
    sector: str | None
    industry: str | None
    country: str | None
    market: str | None
    has_logo: bool | None
    values: Mapping[str, float | None]
    units: Mapping[str, str | None]


@dataclass(frozen=True)
class ScreenTablePage:
    """A page of the multi-metric screener table: the rows, the total match count (for
    pagination), and the ordered metric columns those rows carry values for."""

    rows: tuple[ScreenTableRow, ...]
    total: int
    columns: tuple[ScreenColumn, ...]


@dataclass(frozen=True)
class FootballBar:
    """One intrinsic-value estimate as a bear→bull range with a mid point (per-share, USD)."""

    method: str
    bear: float
    mid: float
    bull: float
    fiscal_year: int


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class FootballField:
    """The valuation "football field": per-method IV ranges plus the current market price."""

    bars: tuple[FootballBar, ...]
    price: float | None
