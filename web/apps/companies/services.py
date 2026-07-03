"""Companies application service — coordinates the company repository.

Storage-agnostic: it asks ``CompanyRepository`` for DTOs and composes them; it has no idea
the data comes from DuckDB/parquet.
"""

from __future__ import annotations

from repositories.companies import CompanyRepository
from repositories.dtos import (
    CompanyDetail,
    CompanyStatements,
    CompanySummary,
    HeadlineKpi,
    MetricPoint,
    PricePoint,
    QuarterGrid,
)

# Headline overview figures: (statement, line item, display label). The latest fiscal year's
# value of each is shown in the overview KPI strip.
_HEADLINE: tuple[tuple[str, str, str], ...] = (
    ("Income Statement", "Revenue", "Revenue"),
    ("Income Statement", "Net Income", "Net income"),
    ("Balance Sheet", "Total Assets", "Total assets"),
    ("Cash Flow", "Operating CF", "Operating cash flow"),
)


def get_company_detail(ticker: str) -> CompanyDetail | None:
    """Company summary + latest-FY metrics, or ``None`` if the ticker is unknown."""
    repo = CompanyRepository()
    summary = repo.get_summary(ticker)
    if summary is None:
        return None
    return CompanyDetail(summary=summary, metrics=repo.latest_metrics(ticker))


def get_company_statements(ticker: str) -> CompanyStatements:
    """The ticker's reported statements (Income / Balance Sheet / Cash Flow) as year grids."""
    return CompanyRepository().get_statements(ticker)


def get_quarterly(ticker: str) -> QuarterGrid:
    """The ticker's Income Statement across recent fiscal quarters."""
    return CompanyRepository().get_quarterly(ticker)


def get_price_series(ticker: str) -> tuple[PricePoint, ...]:
    """The ticker's daily close series (downsampled) for the price chart."""
    return CompanyRepository().price_series(ticker)


# Metric categories that belong to the Valuation tab, not the Derived-metrics tab:
#   "Intrinsic Value" — the Graham/DCF/Owner-Earnings estimates + MoS, already shown as the
#                       valuation football field + MoS table → dropped here to avoid redundancy.
#   "Valuation"       — the price multiples/yields (P/E, P/B, …) → surfaced in the Valuation tab.
_INTRINSIC_CATEGORY = "Intrinsic Value"
_VALUATION_CATEGORY = "Valuation"


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


def get_company_summary(ticker: str) -> CompanySummary | None:
    """Descriptive facts for a ticker, or ``None`` if unknown (no metrics fetch)."""
    return CompanyRepository().get_summary(ticker)
