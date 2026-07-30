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
from typing import Any

from fundamentals_pipeline.fx import convert_price

from .dtos import (
    BenchmarkContext,
    CompanyDetail,
    CompanyPage,
    CompanyStatements,
    CompanySummary,
    FilingRow,
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
from .repositories.company_listing import (
    PRESET_LEVELS,
    CompanyListingRepository,
    MetricFilter,
    SortSpec,
    preset_thresholds,
)
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


def get_company_filings(ticker: str) -> tuple[FilingRow, ...]:
    """The ticker's real SEC 10-K/10-Q filings (newest first), or ``()`` if none are
    published — see ``CompanyRepository.get_filings``."""
    return CompanyRepository().get_filings(ticker)


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

# Net-Net Finder table's sortable column keys, in header order. "discount" is the default
# (price/NCAV-per-share ratio, ascending — biggest discount first, this table's original and
# only behaviour before column-header sorting existed); every other key lets a column-header
# click override it. Sorting happens here in Python (not DuckDB, unlike the general screener
# and Investor Presets) because `net_net_screen` already returns its whole filtered set
# unpaginated — see its own docstring on why ("genuine net-nets are a small fraction of the
# universe").
_NET_NET_SORT_KEYS = (
    "ticker", "name", "sector", "price", "ncav_per_share", "discount", "f_score", "z_score",
    "market_cap",
)


def _price_to_ncav_ratio(row: NetNetRow, level: str) -> float | None:
    """price / NCAV-per-share for `level` — None when either side is missing, or the level's
    own NCAV/share isn't positive (shouldn't happen for a row that already passed the
    repository's `NCAV Ratio IS NOT NULL` eligibility filter, but stay defensive rather than
    divide by a non-positive value)."""
    ncav_per_share = getattr(row, _NET_NET_NCAV_FIELD.get(level, "ncav_per_share_relaxed"))
    if row.price is None or ncav_per_share is None or ncav_per_share <= 0:
        return None
    return row.price / ncav_per_share


def _net_net_sort_value(row: NetNetRow, ratio: float | None, level: str, key: str) -> Any:
    if key == "ticker":
        return row.ticker
    if key == "name":
        return row.name or ""
    if key == "sector":
        return row.sector or ""
    if key == "price":
        return row.price
    if key == "ncav_per_share":
        return getattr(row, _NET_NET_NCAV_FIELD.get(level, "ncav_per_share_relaxed"))
    if key == "f_score":
        return row.f_score
    if key == "z_score":
        return row.z_score
    if key == "market_cap":
        return row.market_cap
    return ratio  # "discount" (default) and any unrecognized key


def _sort_net_net_rows(
    rows: tuple[NetNetRow, ...], ratios: dict[str, float | None], level: str,
    sort_key: str, descending: bool,
) -> tuple[NetNetRow, ...]:
    """Sort by `sort_key` (a column a user clicked), nulls always last regardless of direction —
    the same convention a database `ORDER BY ... NULLS LAST` would apply, needed here because
    Python's `sorted()` can't compare `None` to a number/string and `reverse=True` would put
    nulls first if they were merely coalesced to +/-inf."""
    if sort_key not in _NET_NET_SORT_KEYS:
        sort_key = "discount"
    values = {row.ticker: _net_net_sort_value(row, ratios.get(row.ticker), level, sort_key) for row in rows}
    with_value = sorted((r for r in rows if values[r.ticker] is not None),
                         key=lambda r: values[r.ticker], reverse=descending)
    without_value = [r for r in rows if values[r.ticker] is None]
    return tuple(with_value + without_value)


def get_net_net_screen(
    *, level: str = "relaxed", hide_value_traps: bool = False,
    min_discount_pct: float | None = None,
    sector: str = "", index: str = "", country: str = "", market: str = "", industry: str = "",
    sort_key: str = "discount", descending: bool = False,
) -> NetNetScreen:
    """The Net-Net Finder's full result for `level` ("relaxed"/"moderate"/"strict", falls back
    to "relaxed" for anything else): rows sorted by `sort_key` (defaults to discount to NCAV,
    ascending — cheapest first, i.e. ascending price/NCAV-per-share, this table's original
    behaviour before column-header sorting existed), optionally excluding likely value traps
    (Piotroski F-Score < 4) and/or rows below a minimum discount, plus hero stats for that SAME
    filtered set — so the counts always agree with what's on screen, never a stale/unfiltered
    number next to a filtered table.

    `min_discount_pct`: `None` (issue #317's "All") applies no discount filter at all, keeping
    every eligible row regardless of ratio — including rows with no computable ratio (e.g. a
    missing live price). A float (0.0/15.0/33.0, issue #317's ">0%"/">15%"/">33%") keeps only
    rows with a STRICTLY greater discount than that threshold; a row with no computable ratio
    is dropped in this case (unlike `hide_value_traps`'s lenient "unknown passes" convention) —
    we can't confirm an unknown discount clears a specific bar the user explicitly asked for.
    """
    if level not in _NET_NET_LEVELS:
        level = "relaxed"
    repo = CompanyListingRepository()
    universe_size = repo.scope_size(sector=sector, index=index, country=country, market=market,
                                     industry=industry)
    rows = repo.net_net_screen(level=level, sector=sector, index=index, country=country,
                                market=market, industry=industry)
    if hide_value_traps:
        rows = tuple(r for r in rows if r.f_score is None or r.f_score >= _VALUE_TRAP_F_SCORE_FLOOR)

    ratios = {row.ticker: _price_to_ncav_ratio(row, level) for row in rows}

    if min_discount_pct is not None:
        max_ratio = 1 - min_discount_pct / 100
        rows = tuple(r for r in rows if (ratio := ratios.get(r.ticker)) is not None and ratio < max_ratio)
        ratios = {row.ticker: ratios[row.ticker] for row in rows}

    sorted_rows = _sort_net_net_rows(rows, ratios, level, sort_key, descending)

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
# Static copy for each school; `criteria` below is always a `()` placeholder — the real,
# level-parameterized criteria list is built by `get_preset_definition` via the
# `_PRESET_CRITERIA_BUILDERS` functions further down, reading `company_listing.preset_thresholds`
# (the single source of truth also `CompanyListingRepository.preset_screen`'s WHERE/multi-year
# checks read from — never a second, duplicated set of numbers). All 3 schools are fully live at
# every conservatism level: each preset's own `_PRESET_WHERE` latest-FY predicate plus — for
# Buffett/Graham — the multi-year checks in `_preset_multi_year_checks` (sustained ROE;
# positive-earnings/uninterrupted-dividend/EPS-growth). Lynch's PEG is a plain latest-FY
# predicate too (not a multi-year check) — it just happens to filter on "PEG", a metric the
# pipeline computes from multi-year EPS history (issue #280); "5-year EPS CAGR" stays
# display-only (shown as a column) at every level — it has no threshold of its own.
_PRESET_DEFINITIONS: dict[str, PresetDefinition] = {
    "graham": PresetDefinition(
        key="graham",
        dot_color="--accent",
        eyebrow="GRAHAM · DEFENSIVE INVESTOR",
        headline="Price, margin of safety, and little else.",
        description=(
            "The Intelligent Investor's classic criteria applied to today's universe: a solid "
            "balance sheet, moderate multiples, earnings stability, and dividend record."
        ),
        school="GRAHAM · DEFENSIVE INVESTOR",
        name="Benjamin Graham",
        tagline="The father of value investing and author of The Intelligent Investor.",
        criteria=(),
    ),
    "buffett": PresetDefinition(
        key="buffett",
        dot_color="--orange",
        eyebrow="BUFFETT · QUALITY COMPOUNDER",
        headline="Businesses with a moat, not balance-sheet bargains.",
        description=(
            "Graham's turn toward Munger: pay a fair price for an excellent business. High "
            "margins, little debt, and a margin of safety computed against the business's "
            "value, not just its book price."
        ),
        school="BUFFETT · QUALITY COMPOUNDER",
        name="Warren Buffett",
        tagline="CEO of Berkshire Hathaway; Graham's greatest disciple, evolved.",
        criteria=(),
    ),
    "lynch": PresetDefinition(
        key="lynch",
        dot_color="--violet",
        eyebrow="LYNCH · GROWTH AT A REASONABLE PRICE",
        headline="Reasonable growth, paid at a reasonable price.",
        description=(
            "Peter Lynch's GARP approach: growing businesses with healthy balance sheets, "
            "bought without overpaying for that growth — the balance between Graham and pure "
            "momentum."
        ),
        school="LYNCH · GROWTH AT A REASONABLE PRICE",
        name="Peter Lynch",
        tagline="Manager of the Fidelity Magellan Fund; popularized GARP analysis.",
        criteria=(),
    ),
}
_PRESETS: tuple[str, ...] = ("graham", "buffett", "lynch")


def _graham_criteria(t: dict[str, Any]) -> tuple[PresetCriterion, ...]:
    div_label = (
        f"Uninterrupted dividend, {t['div_years']:g} straight years"
        if t["div_min_years"] == t["div_years"] else
        f"Uninterrupted dividend, {t['div_min_years']:g}+ of last {t['div_years']:g} years"
    )
    growth_pct = (t["eps_min_growth"] - 1) * 100
    eps_label = f"EPS growth ≥ {growth_pct:.0f}% over {t['eps_years']:g}y"
    if t["eps_use_3y_avg"]:
        eps_label += " (3-year averages)"
    return (
        PresetCriterion(f"Current ratio ≥ {t['cr_min']:g}", "live"),
        PresetCriterion(f"P/E ≤ {t['pe_max']:g}", "live"),
        PresetCriterion(
            f"P/B ≤ {t['pb_max']:g} (or P/E × P/B ≤ {t['pe_pb_max']:g})", "live"),
        PresetCriterion(f"Positive earnings, {t['earnings_years']:g} straight years", "live"),
        PresetCriterion(div_label, "live"),
        PresetCriterion(eps_label, "live"),
    )


def _buffett_criteria(t: dict[str, Any]) -> tuple[PresetCriterion, ...]:
    return (
        PresetCriterion(f"Debt / Equity ≤ {t['de_max']:g}", "live"),
        PresetCriterion(f"Gross margin > {t['gm_min']:g}%", "live"),
        PresetCriterion(f"Net margin > {t['nm_min']:g}%", "live"),
        PresetCriterion(f"Margin of safety (Owner Earnings) ≥ {t['mos_min']:g}%", "live"),
        PresetCriterion(f"ROE ≥ {t['roe_min']:g}% sustained, {t['roe_years']:g} years", "live"),
    )


def _lynch_criteria(t: dict[str, Any]) -> tuple[PresetCriterion, ...]:
    return (
        PresetCriterion(f"Debt / Equity < {t['de_max']:g}", "live"),
        PresetCriterion(f"Current ratio ≥ {t['cr_min']:g}", "live"),
        PresetCriterion(f"ROE > {t['roe_min']:g}%", "live"),
        PresetCriterion("5-year EPS CAGR", "live"),
        PresetCriterion(f"PEG < {t['peg_max']:g}", "live"),
    )


_PRESET_CRITERIA_BUILDERS: dict[str, Any] = {
    "graham": _graham_criteria, "buffett": _buffett_criteria, "lynch": _lynch_criteria,
}


def preset_keys() -> tuple[str, ...]:
    """The valid preset keys, in display order (for the pill selector)."""
    return _PRESETS


def preset_levels() -> tuple[str, ...]:
    """The valid conservatism-level keys, in display order (for the level pill) — "strict" is
    the toughest and the default."""
    return PRESET_LEVELS


def get_preset_definition(preset: str, level: str = "strict") -> PresetDefinition:
    """The copy for one school, with `criteria` built for this conservatism level (falls back to
    "graham"/"strict" for unrecognized values) — see `_PRESET_CRITERIA_BUILDERS` and
    `company_listing.preset_thresholds`, the single source of truth for the actual numbers."""
    if preset not in _PRESET_DEFINITIONS:
        preset = "graham"
    thresholds = preset_thresholds(preset, level)
    criteria = _PRESET_CRITERIA_BUILDERS[preset](thresholds)
    return replace(_PRESET_DEFINITIONS[preset], criteria=criteria)


def get_preset_screen(
    preset: str, *, level: str = "strict", sector: str = "", index: str = "", country: str = "",
    market: str = "", industry: str = "", sort: SortSpec | None = None, page: int = 1,
    page_size: int = 50,
) -> PresetScreen:
    """The Investor Presets screener's full result for one school ("graham"/"buffett"/"lynch",
    falls back to "graham" for anything else) at one conservatism `level` ("strict"/"moderate"/
    "relaxed", falls back to "strict"): one page of matching companies (paginated AND sorted in
    DuckDB — see ``CompanyListingRepository.preset_screen``), plus hero stats computed under the
    SAME descriptive filters (universe size, and the level-resolved definition's own live/
    pending criteria counts, so the numbers always agree with what's on screen).
    """
    if preset not in _PRESET_DEFINITIONS:
        preset = "graham"
    repo = CompanyListingRepository()
    universe_size = repo.scope_size(sector=sector, index=index, country=country, market=market,
                                     industry=industry)
    rows, total, columns = repo.preset_screen(
        preset=preset, level=level, sector=sector, index=index, country=country, market=market,
        industry=industry, sort=sort, page=page, page_size=page_size,
    )
    definition = get_preset_definition(preset, level)
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
