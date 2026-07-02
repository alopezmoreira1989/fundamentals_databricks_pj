"""Screener application service — coordinates the screener/company-listing repositories.

Storage-agnostic: it forwards validated query parameters to the repositories and returns the
immutable read model. The HTML screener renders a paginated *company table* (``list_companies``)
that optional filters narrow; ``run_screen`` remains the single-metric JSON read model.
"""

from __future__ import annotations

from repositories.company_listing import CompanyListingRepository
from repositories.dtos import CompanyPage, ScreenRow
from repositories.screener import ScreenerRepository


def list_companies(
    *,
    search: str = "",
    sector: str = "",
    index: str = "",
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
        metric=metric,
        min_value=min_value,
        max_value=max_value,
        page=page,
        page_size=page_size,
    )
    return CompanyPage(rows=rows, total=total)


def available_sectors() -> tuple[str, ...]:
    """Sector names the user can filter on (for the picker)."""
    return CompanyListingRepository().available_sectors()


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
