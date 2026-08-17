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
class FilingRow:
    """One real SEC filing (10-K/10-Q) for the company page's Filings tab — issue #318.

    Sourced from the ``dashboard_filings`` artifact, written by the pipeline's
    ``15__fetch_sec_filings.py`` — no live SEC call of our own; see
    ``CompanyRepository.get_filings``. ``fiscal_year``/``period_type`` are joined in from
    ``dashboard_data`` (matched on this filing's own ``report_date``), not derived from the
    date ourselves — ``None`` when no matching period is published yet.
    """

    form: str
    filing_date: str | None
    report_date: str | None
    description: str | None
    url: str
    fiscal_year: int | None = None
    period_type: str | None = None


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
class PeerCompany:
    """A ticker + display name pair — used for the peer-chip roster, the selected "compare to"
    company, and the Compare-picker's <datalist> options."""

    ticker: str
    name: str


@dataclass(frozen=True)
class BenchmarkContext:
    """Everything the Derived-metrics tab's "Benchmark against" switch needs beyond the
    per-metric peer_median/peer_count already merged into each MetricSeries row (see
    services.get_metric_history).

    ``mode`` is always "industry"/"sector"/"compare" — which pill is shown active, even on a
    plain page load with no ?bench= (the historic silent auto-cascade still runs then; ``mode``
    reflects whichever basis it resolved to, defaulting to "industry" if neither basis had any
    peers). ``basis`` is what was actually merged into the metric rows — same three values, or
    ``None`` when there's nothing to show (a forced basis with zero peers, or "compare" with no
    ticker chosen / an unknown ticker) — callers degrade to a plain empty caption then, never a
    crash. ``industry_peer_count``/``sector_peer_count`` are ALWAYS both populated so both pills
    can show their own badge. ``peers`` (the chip roster) is populated only when
    ``basis == "industry"``; ``compare`` carries the single Compare target instead of duplicating
    it into ``peers``.
    """

    mode: str
    basis: str | None
    peer_count: int = 0
    industry_peer_count: int = 0
    sector_peer_count: int = 0
    peers: tuple[PeerCompany, ...] = ()
    compare: PeerCompany | None = None


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
class NetNetRow:
    """One Net-Net Finder screener row: a ticker's latest close price, NCAV/Share at all three
    liquidation-conservatism levels (relaxed/moderate/strict — see
    CompanyListingRepository.net_net_screen), and its Piotroski/Altman quality overlay.

    ``f_score`` is Piotroski's 0–9 composite (higher = healthier). ``z_score_zone`` is
    ``"safe"`` (Altman Z > 3.0) / ``"grey"`` (1.8–3.0) / ``"distress"`` (< 1.8) / ``None`` (no
    Z-Score) — the same two-threshold classification `fundamentals_pipeline`'s Streamlit lib
    (``60__frontends/61__streamlit/lib/signals.py``) already uses, duplicated as constants in
    the repository rather than imported (that directory is digit-prefixed — not a valid Python
    module path — and isn't part of the installed `fundamentals_pipeline` distribution anyway).
    """

    ticker: str
    name: str
    sector: str | None
    industry: str | None
    country: str | None
    market: str | None
    price: float | None
    market_cap: float | None
    ncav_per_share_relaxed: float | None
    ncav_per_share_moderate: float | None
    ncav_per_share_strict: float | None
    f_score: float | None
    z_score: float | None
    z_score_zone: str | None
    has_logo: bool | None = None
    # Phase 5.7a: the ticker's own reporting currency (meta artifact — always the real,
    # FIRDS/SEC-sourced currency, never inferred from country/market), since `price`/
    # `market_cap`/every `ncav_per_share_*` field here are all denominated in it. Deliberately
    # NOT read from dashboard_metrics' own per-metric `unit` column: confirmed during Phase
    # 5.7a that "NCAV / Share (Live)" (and its Moderate/Strict siblings) are exported with a
    # hardcoded `'usd'` unit literal (fundamentals_pipeline/50__publish/
    # 51__export_dashboard_data.py's `_NCAV_LIVE_COLUMNS`), unlike "Market Cap (Live)" which
    # already uses the real per-row currency — so the metric's own unit is not a reliable
    # source for this DTO. See docs/phase5-7-fundamentals-screener-multi-market-audit.md's
    # implementation section for the full note.
    currency: str | None = None


@dataclass(frozen=True)
class NetNetStats:
    """Hero-stat counts for the Net-Net Finder's selected level, all computed under the SAME
    descriptive filters (and ``hide_value_traps``, if on) as the rows they accompany — so the
    numbers always agree with what's on screen, never a stale/unfiltered count next to a
    filtered table.

    ``universe_size``: companies matching the descriptive filters, before any net-net-specific
    filtering — the denominator for "how rare is this". ``eligible_count``: the subset with a
    positive NCAV at this level (``len(rows)`` before sorting). ``below_value_count``: the
    subset actually trading below their NCAV/share (price/NCAV-per-share < 1). ``classic_net_
    net_count``: the subset at or below Graham's classic net-net rule-of-thumb margin of safety,
    trading at ≤ 0.67× NCAV/share.
    """

    universe_size: int
    eligible_count: int
    below_value_count: int
    classic_net_net_count: int


@dataclass(frozen=True)
class NetNetScreen:
    """The Net-Net Finder's full result for one level: rows sorted by discount to NCAV
    (descending — cheapest first), plus the hero stats for that same filtered set."""

    rows: tuple[NetNetRow, ...]
    stats: NetNetStats


@dataclass(frozen=True)
class PresetCriterion:
    """One line of an Investor Presets school's criteria list — a human-readable rule plus
    whether it's enforced today (``"live"``, latest-FY data) or still pending the multi-year
    historical-depth work (``"pending"`` — rendered in the UI, never filtered on)."""

    label: str
    status: str  # "live" | "pending"


@dataclass(frozen=True)
class PresetDefinition:
    """Copy for one Investor Presets school (Graham/Buffett/Lynch). Everything except
    ``criteria`` is static (the same for every request); ``criteria`` is level-dependent (its
    label text embeds that conservatism level's actual threshold numbers — see
    ``services.get_preset_definition``) and is filled in at call time via
    ``dataclasses.replace``, not stored directly in the static registry. ``dot_color`` names a
    CSS custom property (e.g. ``"--accent"``) for the pill selector's per-school dot."""

    key: str  # "graham" | "buffett" | "lynch"
    dot_color: str
    eyebrow: str
    headline: str
    description: str
    school: str
    name: str
    tagline: str
    criteria: tuple[PresetCriterion, ...]


@dataclass(frozen=True)
class PresetStats:
    """Hero-stat counts for the selected preset, computed under the SAME descriptive filters
    as the rows they accompany."""

    universe_size: int
    live_criteria_count: int
    pending_criteria_count: int


@dataclass(frozen=True)
class PresetScreen:
    """The Investor Presets screener's full result for one school: the matching rows (generic
    ticker + metric-values shape, reused from the multi-metric screener table), the columns
    those rows carry values for, and the hero stats for that same filtered set."""

    rows: tuple[ScreenTableRow, ...]
    columns: tuple[ScreenColumn, ...]
    total: int
    stats: PresetStats


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


@dataclass(frozen=True)
class ForecastPoint:
    """One raw forecast row: a ticker's forecast for ``metric`` at ``horizon`` years out, at
    one quantile scenario. Sourced from the ``dashboard_forecast`` artifact, written by the
    pipeline's ``24__forecasting.py`` (issue #332) — this DTO never recomputes a forecast."""

    metric: str
    horizon: int
    quantile_level: float
    forecast_value: float | None


@dataclass(frozen=True)
class ForecastSeries:
    """One ``(metric, quantile level)`` forecast path, ready to plot as one line of the
    Forecasting fan chart — ``horizons``/``values`` are index-aligned, ascending by horizon."""

    metric: str
    quantile_level: float
    horizons: tuple[int, ...]
    values: tuple[float | None, ...]


@dataclass(frozen=True)
class ForecastHistoryPoint:
    """One historical (reported, not forecasted) fiscal-year value for a Forecasting metric."""

    fiscal_year: int
    value: float | None


@dataclass(frozen=True)
class ForecastMetricChart:
    """One target metric's (Revenue/Net Income/Free Cash Flow) full Forecasting fan-chart
    data: recent reported history plus every quantile scenario's forward path.

    ``historical`` is chronological (oldest first, ending at the ticker's latest reported
    fiscal year — "FY0"). When history exists, each ``scenarios`` entry's own ``horizons``/
    ``values`` are pre-pended with ``(0, historical[-1].value)`` — the FY0 anchor — as their
    first element, so every line the chart draws — historical AND every scenario — shares one
    array origin point; this is what makes a single shared y-scale/domain across all lines
    trivial on the JS side (issue #336's explicit requirement) rather than something the chart
    code has to engineer itself. With no history, scenarios are left un-anchored (raw horizons
    1-10).
    """

    metric: str
    label: str
    unit: str | None
    historical: tuple[ForecastHistoryPoint, ...]
    scenarios: tuple[ForecastSeries, ...]


@dataclass(frozen=True)
class ForwardMultipleRow:
    """One PV-discounted forward multiple (``"forward_pe"`` or ``"forward_fcf_yield"``) at one
    horizon, for the mid/"Crab" (quantile 0.50) scenario only — see
    ``ForecastRepository.forward_multiples``."""

    metric: str
    horizon: int
    value: float | None


@dataclass(frozen=True)
class ForecastChart:
    """The Forecasting page's full data for one ticker: every target metric's fan-chart data
    plus the forward-multiples table. ``metrics``/``forward_multiples`` are both empty (not
    ``None``) when the ticker is known but has no published forecast yet — the page renders a
    "not available" state rather than 404ing."""

    ticker: str
    metrics: tuple[ForecastMetricChart, ...]
    forward_multiples: tuple[ForwardMultipleRow, ...]
