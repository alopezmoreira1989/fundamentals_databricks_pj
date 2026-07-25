"""Application services — coordinate the repository tier for the views.

Storage-agnostic: these functions ask the repositories for DTOs and compose them; none of
them know the data comes from DuckDB/parquet. Consolidates what were three separate
per-app ``services.py`` modules (companies/screener/valuation) in the source project this
package was extracted from, since this package ships as one Django app.

Not ported: the favorites/watchlist/history personalization the source app's
``company_page`` view had — those depend on login-scoped apps this package doesn't assume
the host project has. See the package README for the full list of what v1 does and doesn't
cover.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from fundamentals_pipeline.fx import convert_price

from .dtos import (
    BenchmarkContext,
    CompanyDetail,
    CompanyPage,
    CompanyStatements,
    CompanySummary,
    HeadlineKpi,
    MetricPoint,
    MetricSeries,
    NetNetRow,
    NetNetScreen,
    NetNetStats,
    PeerBenchmark,
    PeerCompany,
    PresetCriterion,
    PresetDefinition,
    PresetScreen,
    PresetStats,
    PricePoint,
    QuarterGrid,
    ScreenRow,
    ScreenTablePage,
)
from .news import NewsItem, fetch_yahoo_news
from .repositories.companies import PRICE_WINDOW_DAYS, PRICE_WINDOW_DEFAULT, CompanyRepository
from .repositories.company_listing import CompanyListingRepository, MetricFilter, SortSpec
from .repositories.screener import ScreenerRepository
from .repositories.valuation import ValuationRepository

# Headline overview figures: (statement, line item, display label). The latest fiscal year's
# value of each is shown in the overview KPI strip.
_HEADLINE: tuple[tuple[str, str, str], ...] = (
    ("Income Statement", "Revenue", "Revenue"),
    ("Income Statement", "Net Income", "Net income"),
    ("Balance Sheet", "Total Assets", "Total assets"),
    ("Cash Flow", "Operating CF", "Operating cash flow"),
)

# Metric categories that belong to the Valuation tab, not the Derived-metrics tab:
#   "Intrinsic Value" — the Graham/DCF/Owner-Earnings estimates + MoS, already shown as the
#                       valuation football field + MoS table → dropped here to avoid redundancy.
#   "Valuation"       — the price multiples/yields (P/E, P/B, …) → surfaced in the Valuation tab.
_INTRINSIC_CATEGORY = "Intrinsic Value"
_VALUATION_CATEGORY = "Valuation"


# ── company detail ──────────────────────────────────────────────────────────────────────
def get_company_detail(ticker: str) -> CompanyDetail | None:
    """Company summary + latest-FY metrics, or ``None`` if the ticker is unknown."""
    repo = CompanyRepository()
    summary = repo.get_summary(ticker)
    if summary is None:
        return None
    return CompanyDetail(summary=summary, metrics=repo.latest_metrics(ticker))


def get_company_summary(ticker: str) -> CompanySummary | None:
    """Descriptive facts for a ticker, or ``None`` if unknown (no metrics fetch)."""
    return CompanyRepository().get_summary(ticker)


def get_company_statements(ticker: str) -> CompanyStatements:
    """The ticker's reported statements (Income / Balance Sheet / Cash Flow) as year grids."""
    return CompanyRepository().get_statements(ticker)


def get_quarterly(ticker: str) -> QuarterGrid:
    """The ticker's Income Statement across recent fiscal quarters."""
    return CompanyRepository().get_quarterly(ticker)


def get_price_series(ticker: str, *, window: str = PRICE_WINDOW_DEFAULT) -> tuple[PricePoint, ...]:
    """The ticker's daily close series (downsampled) for the price chart, trimmed to the
    trailing `window` (e.g. "1Y") with SMA 20/50/200 already computed."""
    return CompanyRepository().price_series(ticker, window=window)


def price_windows() -> tuple[str, ...]:
    """The quick-range window labels the Price tab's buttons offer, e.g. ``("1M", ..., "Max")``."""
    return tuple(PRICE_WINDOW_DAYS)


def get_company_news(ticker: str) -> tuple[NewsItem, ...]:
    """Latest Yahoo Finance headlines for the ticker (cached; empty on any error)."""
    return fetch_yahoo_news(ticker)


def _merge_peer_medians(
    series: tuple[MetricSeries, ...], benchmarks: tuple[PeerBenchmark, ...]
) -> tuple[MetricSeries, ...]:
    """Fold each metric's peer-group median/count into its MetricSeries row (a metric with no
    matching benchmark keeps its default peer_median=None/peer_count=0 — still shows up, just
    without a benchmark column value)."""
    by_metric = {b.metric: b for b in benchmarks}
    return tuple(
        replace(s, peer_median=bm.peer_median, peer_count=bm.peer_count) if (bm := by_metric.get(s.metric))
        else s
        for s in series
    )


def get_metric_history(
    ticker: str, *, years: int = 5, bench: str = "", compare: str = ""
) -> tuple[tuple[MetricSeries, ...], BenchmarkContext]:
    """The Derived-metrics tab's data: each metric's recent `years`-year history (for the
    sparkline), merged with the requested benchmark.

    Valuation/Intrinsic-Value categories are excluded here for the same reason as
    ``split_metrics`` (they're portrayed by the Valuation tab instead).

    `bench`: ``""`` (auto — today's silent industry-then-sector cascade) | ``"industry"`` |
    ``"sector"`` | ``"compare"``. `compare`: a ticker symbol, consulted only when
    ``bench == "compare"``. Returns ``(series, BenchmarkContext)`` — preserves today's exact
    output when `bench`/`compare` are both left blank.
    """
    repo = CompanyRepository()
    series = tuple(
        s for s in repo.metric_history(ticker, years=years)
        if s.category not in (_INTRINSIC_CATEGORY, _VALUATION_CATEGORY)
    )
    summary = repo.get_summary(ticker)
    if summary is None or not series:
        return series, BenchmarkContext(mode="industry", basis=None, peer_count=0)

    industry_n, sector_n = repo.peer_counts(ticker, summary.industry, summary.sector)

    if bench == "compare":
        benchmarks, compare_company = repo.compare_benchmark(ticker, compare)
        merged = _merge_peer_medians(series, benchmarks)
        ctx = BenchmarkContext(
            mode="compare",
            basis="compare" if compare_company else None,
            peer_count=1 if compare_company else 0,
            industry_peer_count=industry_n,
            sector_peer_count=sector_n,
            compare=compare_company,
        )
        return merged, ctx

    forced = bench if bench in ("industry", "sector") else "auto"
    benchmarks, basis, peer_count, peers = repo.industry_benchmark(
        ticker, summary.industry, summary.sector, basis=forced,
    )
    merged = _merge_peer_medians(series, benchmarks)
    mode = basis or (forced if forced in ("industry", "sector") else "industry")
    ctx = BenchmarkContext(
        mode=mode, basis=basis, peer_count=peer_count,
        industry_peer_count=industry_n, sector_peer_count=sector_n, peers=peers,
    )
    return merged, ctx


def all_companies() -> tuple[PeerCompany, ...]:
    """Every ticker+name in the universe, for the "Compare to a company" <datalist>."""
    return CompanyRepository().all_companies()


def split_metrics(
    metrics: tuple[MetricPoint, ...],
) -> tuple[tuple[MetricPoint, ...], tuple[MetricPoint, ...]]:
    """Split latest metrics into ``(derived, valuation_multiples)`` for the tabbed company view.

    Intrinsic-value estimates are omitted from both — they're portrayed by the valuation
    football field + MoS table, so repeating them as a metrics table would be redundant.
    """
    derived = tuple(
        m for m in metrics if m.category not in (_INTRINSIC_CATEGORY, _VALUATION_CATEGORY)
    )
    valuation = tuple(m for m in metrics if m.category == _VALUATION_CATEGORY)
    return derived, valuation


def headline_kpis(statements: CompanyStatements) -> tuple[HeadlineKpi, ...]:
    """Pick the overview headline figures (latest FY) from already-fetched statements."""
    latest: dict[tuple[str, str], tuple[float | None, int | None]] = {}
    for statement in statements.statements:
        year = statement.years[0] if statement.years else None
        for line in statement.lines:
            value = line.values[0] if line.values else None
            latest[(statement.name, line.display_name)] = (value, year)
    kpis = []
    for stmt, concept, label in _HEADLINE:
        value, year = latest.get((stmt, concept), (None, None))
        kpis.append(HeadlineKpi(label=label, value=value, fiscal_year=year))
    return tuple(kpis)


def get_market_cap_kpi(ticker: str, *, usd_lens: bool) -> HeadlineKpi | None:
    """Market Cap headline card (``None`` if the ticker has no Market Cap row).

    Native currency by default; converts to USD only when `usd_lens` is on AND a same-date FX
    rate exists — mirrors the "no rate → stay native, still badge" fallback (never silently
    guessed).
    """
    repo = CompanyRepository()
    mc = repo.market_cap(ticker)
    if mc is None:
        return None
    currency = (mc.unit or "usd").upper()
    value = mc.value
    if usd_lens and currency != "USD" and mc.period_end:
        rate = repo.usd_fx_rate(currency, mc.period_end)
        if rate is not None:
            value = convert_price(value, currency, "USD", rate)
            currency = "USD"
    return HeadlineKpi(label="Market Cap", value=value, fiscal_year=mc.fiscal_year, currency=currency)


# ── screener ─────────────────────────────────────────────────────────────────────────────
def list_companies(
    *,
    search: str = "",
    sector: str = "",
    index: str = "",
    country: str = "",
    market: str = "",
    industry: str = "",
    metric: str = "",
    min_value: float | None = None,
    max_value: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> CompanyPage:
    """One page of the company table under the active filters, plus the total match count."""
    rows, total = CompanyListingRepository().list_page(
        search=search, sector=sector, index=index, country=country, market=market,
        industry=industry, metric=metric, min_value=min_value, max_value=max_value,
        page=page, page_size=page_size,
    )
    return CompanyPage(rows=rows, total=total)


def screen_table(
    *,
    search: str = "",
    sector: str = "",
    index: str = "",
    country: str = "",
    market: str = "",
    industry: str = "",
    columns: Sequence[str] = (),
    filters: Sequence[MetricFilter] = (),
    sort: SortSpec | None = None,
    page: int = 1,
    page_size: int = 50,
    usd_lens: bool = False,
) -> ScreenTablePage:
    """One page of the multi-metric screener table: the descriptive scope narrowed by the
    metric ``filters``, each selected ``columns`` metric pivoted to its latest-FY value.
    ``usd_lens`` converts a displayed Market Cap column to USD."""
    return CompanyListingRepository().screen_table(
        search=search, sector=sector, index=index, country=country, market=market,
        industry=industry, columns=columns, filters=filters, sort=sort,
        page=page, page_size=page_size, usd_lens=usd_lens,
    )


# ── Net-Net Finder ──────────────────────────────────────────────────────────────────────
_NET_NET_LEVELS = ("relaxed", "moderate", "strict")
_NET_NET_NCAV_FIELD = {
    "relaxed": "ncav_per_share_relaxed",
    "moderate": "ncav_per_share_moderate",
    "strict": "ncav_per_share_strict",
}
# Graham's classic net-net rule-of-thumb margin of safety: buy at <= 2/3 of NCAV/share.
_CLASSIC_NET_NET_THRESHOLD = 0.67
# Piotroski F-Score floor below which "hide value traps" excludes a row (0-9 scale; < 4 is
# the commonly-cited weak-quality cutoff — same threshold fundamentals_pipeline's Streamlit
# lib uses for its own F-Score "weak" classification).
_VALUE_TRAP_F_SCORE_FLOOR = 4.0


def _price_to_ncav_ratio(row: NetNetRow, level: str) -> float | None:
    """price / NCAV-per-share for `level` — None when either side is missing, or the level's
    own NCAV/share isn't positive (shouldn't happen for a row that already passed the
    repository's `NCAV Ratio IS NOT NULL` eligibility filter, but stay defensive rather than
    divide by a non-positive value)."""
    ncav_per_share = getattr(row, _NET_NET_NCAV_FIELD.get(level, "ncav_per_share_relaxed"))
    if row.price is None or ncav_per_share is None or ncav_per_share <= 0:
        return None
    return row.price / ncav_per_share


def get_net_net_screen(
    *, level: str = "relaxed", hide_value_traps: bool = False,
    sector: str = "", country: str = "", market: str = "",
) -> NetNetScreen:
    """The Net-Net Finder's full result for `level` ("relaxed"/"moderate"/"strict", falls back
    to "relaxed" for anything else): rows sorted by discount to NCAV (descending — cheapest
    first, i.e. ascending price/NCAV-per-share), optionally excluding likely value traps
    (Piotroski F-Score < 4), plus hero stats for that SAME filtered set — so the counts always
    agree with what's on screen, never a stale/unfiltered number next to a filtered table.
    """
    if level not in _NET_NET_LEVELS:
        level = "relaxed"
    repo = CompanyListingRepository()
    universe_size = repo.scope_size(sector=sector, country=country, market=market)
    rows = repo.net_net_screen(level=level, sector=sector, country=country, market=market)
    if hide_value_traps:
        rows = tuple(r for r in rows if r.f_score is None or r.f_score >= _VALUE_TRAP_F_SCORE_FLOOR)

    ratios = {row.ticker: _price_to_ncav_ratio(row, level) for row in rows}
    sorted_rows = tuple(
        sorted(rows, key=lambda r: ratios[r.ticker] if ratios[r.ticker] is not None else float("inf"))
    )

    valid_ratios = [r for r in ratios.values() if r is not None]
    stats = NetNetStats(
        universe_size=universe_size,
        eligible_count=len(rows),
        below_value_count=sum(1 for r in valid_ratios if r < 1.0),
        classic_net_net_count=sum(1 for r in valid_ratios if r <= _CLASSIC_NET_NET_THRESHOLD),
    )
    return NetNetScreen(rows=sorted_rows, stats=stats)


def get_net_net_snapshot(ticker: str) -> NetNetRow | None:
    """A single ticker's own price, NCAV/Share at all three levels, and quality overlay, for
    the company Valuation page's Net-Net card (issue #262) — ``None`` for an unknown ticker.
    Unlike ``get_net_net_screen``, this never filters on eligibility: a company with negative
    NCAV at every level still gets a snapshot (with those fields ``None``), since the Valuation
    page always shows a company's own numbers rather than screening a universe.
    """
    return CompanyRepository().net_net_snapshot(ticker)


# ── Investor Presets ─────────────────────────────────────────────────────────────────────
# Static copy + criteria for each school (Spanish UI copy is a deliberate, confirmed exception
# to this package's otherwise English-only/no-i18n convention — approved specifically for this
# mode's mockup, not a precedent for the rest of the app; see the docs issue in the "Investor
# Presets Screener" milestone). Live criteria are the 10 latest-FY filters `preset_screen`
# actually applies; pending ones render in the UI (visually disabled) but are never filtered on
# — they depend on the milestone's separate Phase 0 historical-depth investigation and shared
# multi-year repository method issue.
_PRESET_DEFINITIONS: dict[str, PresetDefinition] = {
    "graham": PresetDefinition(
        key="graham",
        dot_color="--accent",
        eyebrow="GRAHAM · DEFENSIVE INVESTOR",
        headline="Precio, margen de seguridad y poco más.",
        description=(
            "Los criterios clásicos de The Intelligent Investor aplicados al universo actual: "
            "balance sólido, múltiplos moderados y, en cuanto el histórico lo permita, "
            "estabilidad de beneficios y dividendo."
        ),
        school="GRAHAM · DEFENSIVE INVESTOR",
        name="Benjamin Graham",
        tagline="El padre del análisis de valor y autor de El Inversor Inteligente.",
        criteria=(
            PresetCriterion("Ratio corriente ≥ 2", "live"),
            PresetCriterion("P/E ≤ 15", "live"),
            PresetCriterion("P/B ≤ 1,5 (o P/E × P/B ≤ 22,5)", "live"),
            PresetCriterion("Beneficios positivos, varios años", "pending"),
            PresetCriterion("Dividendo ininterrumpido, varios años", "pending"),
            PresetCriterion("Crecimiento de BPA ≥ 33%, varios años", "pending"),
        ),
    ),
    "buffett": PresetDefinition(
        key="buffett",
        dot_color="--orange",
        eyebrow="BUFFETT · QUALITY COMPOUNDER",
        headline="Negocios con foso, no gangas de balance.",
        description=(
            "El giro de Graham a Munger: pagar un precio justo por un negocio excelente. "
            "Márgenes altos, poca deuda y un margen de seguridad calculado sobre el valor "
            "del negocio, no solo sobre su precio en libros."
        ),
        school="BUFFETT · QUALITY COMPOUNDER",
        name="Warren Buffett",
        tagline="CEO de Berkshire Hathaway; el mayor discípulo de Graham, evolucionado.",
        criteria=(
            PresetCriterion("Deuda/Patrimonio ≤ 0,5", "live"),
            PresetCriterion("Margen bruto > 40%", "live"),
            PresetCriterion("Margen neto > 20%", "live"),
            PresetCriterion("Margen de seguridad (Owner Earnings) ≥ 25%", "live"),
            PresetCriterion("ROE ≥ 15% sostenido, varios años", "pending"),
        ),
    ),
    "lynch": PresetDefinition(
        key="lynch",
        dot_color="--violet",
        eyebrow="LYNCH · GROWTH AT A REASONABLE PRICE",
        headline="Crecimiento razonable, pagado a un precio razonable.",
        description=(
            "El enfoque GARP de Peter Lynch: negocios en crecimiento con balances sanos, "
            "comprados sin pagar de más por ese crecimiento — el equilibrio entre "
            "Graham y el puro momentum."
        ),
        school="LYNCH · GROWTH AT A REASONABLE PRICE",
        name="Peter Lynch",
        tagline="Gestor del Fidelity Magellan Fund; popularizó el análisis GARP.",
        criteria=(
            PresetCriterion("Deuda/Patrimonio < 0,6", "live"),
            PresetCriterion("Ratio corriente ≥ 1", "live"),
            PresetCriterion("ROE > 15%", "live"),
            PresetCriterion("CAGR de BPA a 5 años", "pending"),
            PresetCriterion("PEG < 1", "pending"),
        ),
    ),
}
_PRESETS: tuple[str, ...] = ("graham", "buffett", "lynch")


def preset_keys() -> tuple[str, ...]:
    """The valid preset keys, in display order (for the pill selector)."""
    return _PRESETS


def get_preset_definition(preset: str) -> PresetDefinition:
    """The static copy/criteria for one school, falling back to "graham" for an unrecognized
    value."""
    return _PRESET_DEFINITIONS.get(preset, _PRESET_DEFINITIONS["graham"])


def get_preset_screen(
    preset: str, *, sector: str = "", country: str = "", market: str = "",
    page: int = 1, page_size: int = 50,
) -> PresetScreen:
    """The Investor Presets screener's full result for one school ("graham"/"buffett"/"lynch",
    falls back to "graham" for anything else): one page of matching companies (paginated in
    DuckDB — see ``CompanyListingRepository.preset_screen``), plus hero stats computed under
    the SAME descriptive filters (universe size, and the definition's own live/pending
    criteria counts, so the numbers always agree with what's on screen).
    """
    if preset not in _PRESET_DEFINITIONS:
        preset = "graham"
    repo = CompanyListingRepository()
    universe_size = repo.scope_size(sector=sector, country=country, market=market)
    rows, total, columns = repo.preset_screen(
        preset=preset, sector=sector, country=country, market=market,
        page=page, page_size=page_size,
    )
    definition = _PRESET_DEFINITIONS[preset]
    stats = PresetStats(
        universe_size=universe_size,
        live_criteria_count=sum(1 for c in definition.criteria if c.status == "live"),
        pending_criteria_count=sum(1 for c in definition.criteria if c.status == "pending"),
    )
    return PresetScreen(rows=rows, columns=columns, total=total, stats=stats)


def available_sectors() -> tuple[str, ...]:
    """Sector names the user can filter on (for the picker)."""
    return CompanyListingRepository().available_sectors()


def available_countries() -> tuple[str, ...]:
    """Country names the user can filter on (for the picker)."""
    return CompanyListingRepository().available_countries()


def available_markets() -> tuple[str, ...]:
    """Listing markets the user can filter on (for the picker), e.g. ``("CA", "US")``."""
    return CompanyListingRepository().available_markets()


def available_industries(*, sector: str = "") -> tuple[str, ...]:
    """Industry names the user can filter on (for the picker), scoped to `sector` when given."""
    return CompanyListingRepository().available_industries(sector=sector)


def run_screen(
    *,
    metric: str,
    min_value: float | None = None,
    max_value: float | None = None,
    limit: int = 50,
) -> tuple[ScreenRow, ...]:
    """Latest-FY screen of ``metric`` within optional inclusive ``[min_value, max_value]``."""
    return ScreenerRepository().screen(
        metric=metric, min_value=min_value, max_value=max_value, limit=limit
    )


def available_metrics() -> tuple[str, ...]:
    """Metric names the user can screen on (for the picker)."""
    return ScreenerRepository().available_metrics()


# ── valuation ────────────────────────────────────────────────────────────────────────────
def get_margin_of_safety(ticker: str) -> tuple[MetricPoint, ...]:
    """Latest Margin-of-Safety metrics for the ticker (empty if none/unknown)."""
    return ValuationRepository().margin_of_safety(ticker)


def get_margin_of_safety_scenarios(ticker: str):
    """MoS organized per (method, basis) with Bear / Mid / Bull columns (empty if none)."""
    return ValuationRepository().margin_of_safety_scenarios(ticker)


def get_intrinsic_value_field(ticker: str):
    """Per-method TTM intrinsic-value ranges + market price (the football field)."""
    return ValuationRepository().intrinsic_value_field(ticker)
