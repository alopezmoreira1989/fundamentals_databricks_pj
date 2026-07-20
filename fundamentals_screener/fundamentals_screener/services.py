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

from fundamentals_pipeline.fx import convert_price

from .dtos import (
    CompanyDetail,
    CompanyPage,
    CompanyStatements,
    CompanySummary,
    HeadlineKpi,
    MetricPoint,
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
