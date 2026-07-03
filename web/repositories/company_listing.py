"""Company-listing read repository — the paginated, filterable company table behind the screener.

The company *universe* (name, sector, industry, index membership) lives in the meta JSON
artifact, loaded in memory; the per-ticker *metric* values live in the ``metrics`` parquet
(DuckDB). This repository owns both sources and joins them:

* Descriptive filters (text search, sector, index membership) are applied over the in-memory
  meta list — the same artifact ``CompanyRepository.get_summary`` already reads. This is not a
  parquet, so filtering it in Python (not DuckDB) is by design.
* When a metric filter is active, the scoped ticker set is pushed into DuckDB, which does the
  min/max filter, latest-FY pick, ordering and LIMIT/OFFSET pagination — never loading the
  whole metrics frame into Python.

Everything above (services/views) sees only immutable DTOs.
"""

from __future__ import annotations

from typing import Any

from infrastructure.storage import meta as load_meta

from .base import DuckDBRepository
from .dtos import CompanyListRow

# Latest-FY value per scoped ticker for one metric, bounded + ordered + paged inside DuckDB.
# ``list_contains(?, ticker)`` restricts to the meta-filtered scope; the optional min/max are
# appended by the caller. The QUALIFY picks each ticker's newest FY before the bounds apply.
_METRIC_PAGE_SQL = """
    WITH latest AS (
        SELECT ticker, fiscal_year, value
        FROM metrics
        WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL
          AND list_contains(?, ticker)
        QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) = 1
    )
    SELECT ticker, fiscal_year, value
    FROM latest
    WHERE TRUE
"""


def _index_flag(index: str) -> str | None:
    """Map an index-membership filter value to its meta boolean key (``None`` ⇒ no filter)."""
    return {"sp500": "in_sp500", "r3000": "in_r3000"}.get(index)


def _has_logo(rec: dict[str, Any]) -> bool | None:
    """Normalize the (stringified) meta ``has_logo`` to a bool (None if unknown)."""
    text = str(rec.get("has_logo", "")).strip().lower()
    return True if text == "true" else False if text == "false" else None


class CompanyListingRepository(DuckDBRepository):
    def available_sectors(self) -> tuple[str, ...]:
        """Distinct non-empty sector names in the universe, alphabetical (for the filter picker)."""
        sectors = {
            s for rec in load_meta().get("tickers", []) if (s := rec.get("sector"))
        }
        return tuple(sorted(sectors))

    def _scope(
        self, *, search: str, sector: str, index: str
    ) -> list[dict[str, Any]]:
        """Universe rows passing the descriptive filters, sorted by ticker (deterministic paging)."""
        needle = search.strip().upper()
        flag = _index_flag(index)
        rows = []
        for rec in load_meta().get("tickers", []):
            ticker = rec.get("ticker", "")
            if sector and rec.get("sector") != sector:
                continue
            if flag and not rec.get(flag):
                continue
            if needle and needle not in ticker.upper() and needle not in rec.get("company", "").upper():
                continue
            rows.append(rec)
        rows.sort(key=lambda r: r.get("ticker", ""))
        return rows

    def list_page(
        self,
        *,
        search: str = "",
        sector: str = "",
        index: str = "",
        metric: str = "",
        min_value: float | None = None,
        max_value: float | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[tuple[CompanyListRow, ...], int]:
        """One page of the company table plus the total match count.

        With no ``metric`` the page is the descriptive universe (sorted by ticker); with a
        ``metric`` the scoped tickers are filtered/ordered/paged by that metric's latest-FY
        value inside DuckDB. Returns ``(rows, total)``.
        """
        scope = self._scope(search=search, sector=sector, index=index)
        offset = max(0, (page - 1) * page_size)

        if not metric:
            total = len(scope)
            window = scope[offset : offset + page_size]
            rows = tuple(
                CompanyListRow(
                    ticker=rec.get("ticker", ""),
                    name=rec.get("company", ""),
                    sector=rec.get("sector"),
                    industry=rec.get("industry"),
                    has_logo=_has_logo(rec),
                )
                for rec in window
            )
            return rows, total

        return self._metric_page(
            scope=scope,
            metric=metric,
            min_value=min_value,
            max_value=max_value,
            offset=offset,
            page_size=page_size,
        )

    def _metric_page(
        self,
        *,
        scope: list[dict[str, Any]],
        metric: str,
        min_value: float | None,
        max_value: float | None,
        offset: int,
        page_size: int,
    ) -> tuple[tuple[CompanyListRow, ...], int]:
        by_ticker = {rec.get("ticker", ""): rec for rec in scope}
        tickers = list(by_ticker)
        if not tickers:
            return (), 0

        where = ""
        bound_params: list[Any] = []
        if min_value is not None:
            where += " AND value >= ?"
            bound_params.append(min_value)
        if max_value is not None:
            where += " AND value <= ?"
            bound_params.append(max_value)

        base_params: list[Any] = [metric, tickers]
        count_sql = f"SELECT count(*) AS n FROM ({_METRIC_PAGE_SQL + where})"
        with self._connection() as con:
            count_row = con.execute(count_sql, base_params + bound_params).fetchone()
            total = int(count_row[0]) if count_row else 0
            page_sql = _METRIC_PAGE_SQL + where + " ORDER BY value DESC LIMIT ? OFFSET ?"
            cursor = con.execute(page_sql, base_params + bound_params + [page_size, offset])
            hits = cursor.fetchall()

        rows = tuple(
            CompanyListRow(
                ticker=ticker,
                name=by_ticker[ticker].get("company", ""),
                sector=by_ticker[ticker].get("sector"),
                industry=by_ticker[ticker].get("industry"),
                metric_value=value,
                fiscal_year=fiscal_year,
                has_logo=_has_logo(by_ticker[ticker]),
            )
            for ticker, fiscal_year, value in hits
        )
        return rows, total
