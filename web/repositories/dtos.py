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


@dataclass(frozen=True, slots=True)
class MetricPoint:
    """One derived-metric value for a ticker at a fiscal year."""

    ticker: str
    metric: str
    unit: str | None
    fiscal_year: int
    value: float | None


@dataclass(frozen=True, slots=True)
class CompanyDetail:
    """A company summary plus its latest-FY metrics (service-level aggregate)."""

    summary: CompanySummary
    metrics: tuple[MetricPoint, ...]


@dataclass(frozen=True, slots=True)
class ScreenRow:
    """One screener hit: a ticker's latest-FY value for the screened metric."""

    ticker: str
    fiscal_year: int
    value: float | None
