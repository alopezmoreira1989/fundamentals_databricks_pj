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

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

import duckdb
from fundamentals_pipeline.fx import convert_price

from infrastructure.storage import meta as load_meta

from .base import DuckDBRepository
from .dtos import CompanyListRow, ScreenColumn, ScreenTablePage, ScreenTableRow

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


# Descriptive columns of the screener table that can be sorted on (they live on the in-memory
# scope table, not the metrics pivot). Any other sort key must name a selected metric column.
_SORTABLE_DESCRIPTIVE = ("ticker", "name", "sector", "industry", "country", "market")

# Market Cap's FX rate for tickers whose native unit isn't already USD, for the USD-lens
# toggle (#220) — date-anchored to each ticker's own Market Cap period_end, mirroring the
# Streamlit app's usd_lens_convert()/convert_to_usd(). Scoped to just the page's tickers.
_MARKET_CAP_FX_SQL = """
    WITH mc AS (
        SELECT ticker, unit, period_end
        FROM metrics
        WHERE metric = 'Market Cap' AND period_type = 'FY' AND value IS NOT NULL
          AND UPPER(unit) != 'USD' AND list_contains(?, ticker)
        QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) = 1
    )
    SELECT mc.ticker, fx.rate
    FROM mc
    LEFT JOIN fx ON fx.base = UPPER(mc.unit) AND fx.quote = 'USD' AND fx.date <= mc.period_end
    QUALIFY row_number() OVER (PARTITION BY mc.ticker ORDER BY fx.date DESC) = 1
"""


@dataclass(frozen=True, slots=True)
class MetricFilter:
    """One metric criterion of the multi-metric screen: keep tickers whose latest-FY ``metric``
    is within the inclusive ``[min_value, max_value]`` (either bound optional)."""

    metric: str
    min_value: float | None = None
    max_value: float | None = None


@dataclass(frozen=True, slots=True)
class SortSpec:
    """How to order the screener table: by a descriptive column (``ticker``/``name``/``sector``/
    ``industry``) or a selected metric name, ascending or descending."""

    key: str = "ticker"
    descending: bool = False


def _index_flag(index: str) -> str | None:
    """Map an index-membership filter value to its meta boolean key (``None`` ⇒ no filter)."""
    return {"sp500": "in_sp500", "r3000": "in_r3000", "tsx": "in_tsx_composite"}.get(index)


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

    def available_countries(self) -> tuple[str, ...]:
        """Distinct non-empty country names in the universe, alphabetical (for the filter picker)."""
        countries = {
            c for rec in load_meta().get("tickers", []) if (c := rec.get("country"))
        }
        return tuple(sorted(countries))

    def available_markets(self) -> tuple[str, ...]:
        """Distinct non-empty listing markets in the universe, alphabetical (for the filter
        picker) — e.g. ``("CA", "US")``. Independent of ``country`` (incorporation/HQ) and of
        the Universe index-membership filter (a dual-listed ticker's `market` can be "US" while
        it's also `in_tsx_composite`)."""
        markets = {
            m for rec in load_meta().get("tickers", []) if (m := rec.get("market"))
        }
        return tuple(sorted(markets))

    def available_industries(self, *, sector: str = "") -> tuple[str, ...]:
        """Distinct non-empty industry names in the universe, alphabetical (for the filter
        picker). Scoped to `sector` when given (#231) — Yahoo's ~145-value industry taxonomy is
        too large to show unscoped, and industries are logically nested one level under sector,
        so picking a sector first narrows this to the relevant subset (mirrors the Streamlit
        screener's own `industry_options(wide[sector_mask(...)])`)."""
        industries = {
            i for rec in load_meta().get("tickers", [])
            if (i := rec.get("industry")) and (not sector or rec.get("sector") == sector)
        }
        return tuple(sorted(industries))

    def _scope(
        self, *, search: str, sector: str, index: str, country: str = "", market: str = "",
        industry: str = ""
    ) -> list[dict[str, Any]]:
        """Universe rows passing the descriptive filters, sorted by ticker (deterministic paging)."""
        needle = search.strip().upper()
        flag = _index_flag(index)
        rows = []
        for rec in load_meta().get("tickers", []):
            ticker = rec.get("ticker", "")
            if sector and rec.get("sector") != sector:
                continue
            if industry and rec.get("industry") != industry:
                continue
            if country and rec.get("country") != country:
                continue
            if market and rec.get("market") != market:
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
        country: str = "",
        market: str = "",
        industry: str = "",
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
        scope = self._scope(search=search, sector=sector, index=index, country=country,
                            market=market, industry=industry)
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
                    country=rec.get("country"),
                    market=rec.get("market"),
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
                country=by_ticker[ticker].get("country"),
                market=by_ticker[ticker].get("market"),
                metric_value=value,
                fiscal_year=fiscal_year,
                has_logo=_has_logo(by_ticker[ticker]),
            )
            for ticker, fiscal_year, value in hits
        )
        return rows, total

    # ── multi-metric screener table ─────────────────────────────────────────────────────
    def screen_table(
        self,
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
        """One page of the multi-metric screener table.

        The descriptive scope (search/sector/index/country/market/industry over the meta
        universe) is pushed into a DuckDB temp table; each selected/​filtered metric's latest-FY
        value is pivoted per ticker in DuckDB, the metric filters and the sort/pagination are
        all applied there. Only the page's rows (≤ ``page_size``) ever cross back into Python.
        ``usd_lens`` converts the Market Cap column to USD (only, date-anchored per ticker's own
        Market Cap period_end) when it's a displayed column — #220's USD-lens toggle, mirroring
        the Streamlit app; every other ``$``-unit column stays native regardless. Returns the
        rows, the total match count, and the ordered display columns (with units, for
        formatting)."""
        scope = self._scope(search=search, sector=sector, index=index, country=country,
                            market=market, industry=industry)
        offset = max(0, (page - 1) * page_size)
        sort = sort or SortSpec()

        display_metrics = list(dict.fromkeys(columns))
        # Every metric that must exist in the pivot: the display columns plus any metric a
        # filter constrains (a filter needs its metric's value available to bound on).
        all_metrics = list(dict.fromkeys([*display_metrics, *(f.metric for f in filters)]))
        alias = {metric: f"m{i}" for i, metric in enumerate(all_metrics)}

        if not scope:
            columns_meta = tuple(ScreenColumn(key=m) for m in display_metrics)
            return ScreenTablePage(rows=(), total=0, columns=columns_meta)

        scope_rows = [
            (
                rec.get("ticker", ""),
                rec.get("company", ""),
                rec.get("sector"),
                rec.get("industry"),
                rec.get("country"),
                rec.get("market"),
                _has_logo(rec),
            )
            for rec in scope
        ]

        with self._connection() as con:
            con.execute("DROP TABLE IF EXISTS scoped")
            con.execute(
                "CREATE TEMP TABLE scoped"
                " (ticker VARCHAR, name VARCHAR, sector VARCHAR, industry VARCHAR,"
                " country VARCHAR, market VARCHAR, has_logo BOOLEAN)"
            )
            con.executemany("INSERT INTO scoped VALUES (?, ?, ?, ?, ?, ?, ?)", scope_rows)

            units = self._metric_units(con, all_metrics)
            cte, cte_params = self._pivot_cte(all_metrics, alias)
            where, where_params = self._filter_clause(filters, alias)
            select_cols = ", ".join(f"p.{alias[m]}, p.{alias[m]}_u" for m in display_metrics)
            projection = (
                "s.ticker, s.name, s.sector, s.industry, s.country, s.market, s.has_logo"
                + (f", {select_cols}" if select_cols else "")
            )
            from_join = (
                "scoped s LEFT JOIN pivoted p USING (ticker)" if all_metrics else "scoped s"
            )

            count_sql = f"{cte}SELECT count(*) AS n FROM {from_join}{where}"
            count_row = con.execute(count_sql, cte_params + where_params).fetchone()
            total = int(count_row[0]) if count_row else 0

            order_sql = self._order_clause(sort, alias)
            page_sql = (
                f"{cte}SELECT {projection} FROM {from_join}{where}{order_sql}"
                " LIMIT ? OFFSET ?"
            )
            cursor = con.execute(
                page_sql, cte_params + where_params + [page_size, offset]
            )
            hits = cursor.fetchall()

            rows = tuple(
                ScreenTableRow(
                    ticker=row[0],
                    name=row[1],
                    sector=row[2],
                    industry=row[3],
                    country=row[4],
                    market=row[5],
                    has_logo=row[6],
                    values={m: row[7 + 2 * i] for i, m in enumerate(display_metrics)},
                    units={m: row[7 + 2 * i + 1] for i, m in enumerate(display_metrics)},
                )
                for row in hits
            )
            if usd_lens and "Market Cap" in display_metrics and rows:
                rows = self._apply_usd_lens(con, rows)

        columns_meta = tuple(
            ScreenColumn(key=m, unit=units.get(m)) for m in display_metrics
        )
        return ScreenTablePage(rows=rows, total=total, columns=columns_meta)

    @staticmethod
    def _apply_usd_lens(con: Any, rows: tuple[ScreenTableRow, ...]) -> tuple[ScreenTableRow, ...]:
        """Convert each row's Market Cap to USD where a same-date FX rate exists; rows with no
        rate (missing `fx` view, or no rate dated early enough) are returned unchanged — still
        native-currency, still badged, never silently guessed."""
        try:
            rate_rows = con.execute(_MARKET_CAP_FX_SQL, [[r.ticker for r in rows]]).fetchall()
        except duckdb.Error:
            return rows  # `fx` view not registered (dashboard_fx.parquet unavailable) — degrade
        rate_by_ticker = {ticker: rate for ticker, rate in rate_rows if rate is not None}
        if not rate_by_ticker:
            return rows
        converted = []
        for row in rows:
            rate = rate_by_ticker.get(row.ticker)
            native_value = row.values.get("Market Cap")
            native_unit = row.units.get("Market Cap")
            if rate is None or native_value is None or not native_unit:
                converted.append(row)
                continue
            usd_value = convert_price(native_value, native_unit.upper(), "USD", rate)
            converted.append(replace(
                row,
                values={**row.values, "Market Cap": usd_value},
                units={**row.units, "Market Cap": "usd"},
            ))
        return tuple(converted)

    @staticmethod
    def _metric_units(con: Any, metrics: list[str]) -> dict[str, str | None]:
        """Map each metric → its unit (for display formatting), read once from the parquet."""
        if not metrics:
            return {}
        rows = con.execute(
            "SELECT metric, any_value(unit) AS unit FROM metrics"
            " WHERE list_contains(?, metric) GROUP BY metric",
            [metrics],
        ).fetchall()
        return {metric: unit for metric, unit in rows}

    @staticmethod
    def _pivot_cte(metrics: list[str], alias: dict[str, str]) -> tuple[str, list[Any]]:
        """The ``WITH latest, pivoted`` CTE that gives each scoped ticker its latest-FY value of
        every needed metric, one value column (``m0``, ``m1``, …) plus its own unit column
        (``m0_u``, ``m1_u``, …) per metric. Most metrics carry one fixed unit across every
        ticker (``ScreenColumn.unit`` covers those), but Market Cap's unit is each ticker's own
        native reporting currency — read per row here rather than assumed column-wide (see
        CLAUDE.md's currency-alignment convention). Empty when no metrics."""
        if not metrics:
            return "", []
        parts: list[str] = []
        params: list[Any] = [metrics]  # the IN-list param for list_contains(?, metric)
        for m in metrics:
            parts.append(f"max(value) FILTER (WHERE metric = ?) AS {alias[m]}")
            params.append(m)
            parts.append(f"any_value(unit) FILTER (WHERE metric = ?) AS {alias[m]}_u")
            params.append(m)
        filters_sql = ", ".join(parts)
        cte = (
            "WITH latest AS ("
            "  SELECT ticker, metric, value, unit FROM metrics"
            "  WHERE period_type = 'FY' AND value IS NOT NULL AND list_contains(?, metric)"
            "    AND ticker IN (SELECT ticker FROM scoped)"
            "  QUALIFY row_number() OVER (PARTITION BY ticker, metric ORDER BY fiscal_year DESC) = 1"
            "), pivoted AS ("
            f"  SELECT ticker, {filters_sql} FROM latest GROUP BY ticker"
            ") "
        )
        return cte, params

    @staticmethod
    def _filter_clause(
        filters: Sequence[MetricFilter], alias: dict[str, str]
    ) -> tuple[str, list[Any]]:
        """The ``WHERE`` bounds for the metric filters, referencing the pivot aliases. A ticker
        with a NULL pivot value for a bounded metric is excluded (NULL fails the comparison)."""
        clauses: list[str] = []
        params: list[Any] = []
        for f in filters:
            col = alias.get(f.metric)
            if col is None:
                continue
            if f.min_value is not None:
                clauses.append(f"p.{col} >= ?")
                params.append(f.min_value)
            if f.max_value is not None:
                clauses.append(f"p.{col} <= ?")
                params.append(f.max_value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    @staticmethod
    def _order_clause(sort: SortSpec, alias: dict[str, str]) -> str:
        """The ``ORDER BY`` expression. Sort keys are whitelisted (a descriptive column or a
        pivot alias) so inlining the identifier is injection-safe; ``s.ticker`` breaks ties for
        stable pagination."""
        direction = "DESC" if sort.descending else "ASC"
        if sort.key in _SORTABLE_DESCRIPTIVE:
            expr = f"s.{sort.key}"
        elif sort.key in alias:
            expr = f"p.{alias[sort.key]}"
        else:
            expr = "s.ticker"
        if expr == "s.ticker":
            return f" ORDER BY {expr} {direction}"
        return f" ORDER BY {expr} {direction} NULLS LAST, s.ticker ASC"
