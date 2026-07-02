"""Companies application service — coordinates the company repository.

Storage-agnostic: it asks ``CompanyRepository`` for DTOs and composes them; it has no idea
the data comes from DuckDB/parquet.
"""

from __future__ import annotations

from repositories.companies import CompanyRepository
from repositories.dtos import CompanyDetail, CompanySummary


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
