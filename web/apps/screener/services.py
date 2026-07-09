"""Screener application service — coordinates the screener/company-listing repositories.

Storage-agnostic: it forwards validated query parameters to the repositories and returns the
immutable read model. The HTML screener renders a paginated *company table* (``list_companies``)
that optional filters narrow; ``run_screen`` remains the single-metric JSON read model.
"""

from __future__ import annotations

from collections.abc import Sequence

from repositories.company_listing import CompanyListingRepository, MetricFilter, SortSpec
from repositories.dtos import CompanyPage, ScreenRow, ScreenTablePage
from repositories.screener import ScreenerRepository


def list_companies(
    *,
    search: str = "",
    sector: str = "",
    index: str = "",
    country: str = "",
    market: str = "",
    metric: str = "",
    min_value: float | None = None,
    max_value: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> CompanyPage:
    """One page of the company table under the active filters, plus the total match count."""
    rows, total = CompanyListingRepository().list_page(
        search=search,
        sector=sector,
        index=index,
        country=country,
        market=market,
        metric=metric,
        min_value=min_value,
        max_value=max_value,
        page=page,
        page_size=page_size,
    )
    return CompanyPage(rows=rows, total=total)


def screen_table(
    *,
    search: str = "",
    sector: str = "",
    index: str = "",
    country: str = "",
    market: str = "",
    columns: Sequence[str] = (),
    filters: Sequence[MetricFilter] = (),
    sort: SortSpec | None = None,
    page: int = 1,
    page_size: int = 50,
    usd_lens: bool = False,
) -> ScreenTablePage:
    """One page of the multi-metric screener table: the descriptive scope narrowed by the
    metric ``filters``, each selected ``columns`` metric pivoted to its latest-FY value.
    ``usd_lens`` converts a displayed Market Cap column to USD (#220's toggle)."""
    return CompanyListingRepository().screen_table(
        search=search,
        sector=sector,
        index=index,
        country=country,
        market=market,
        columns=columns,
        filters=filters,
        sort=sort,
        page=page,
        page_size=page_size,
        usd_lens=usd_lens,
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
