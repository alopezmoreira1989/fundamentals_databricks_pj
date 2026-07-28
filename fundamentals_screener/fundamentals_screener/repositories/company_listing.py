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

Everything above (views) sees only immutable DTOs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

import duckdb

from fundamentals_pipeline.fx import convert_price

from ..data_source import get_meta as load_meta
from ..dtos import CompanyListRow, NetNetRow, ScreenColumn, ScreenTablePage, ScreenTableRow
from .base import DuckDBRepository

# Latest-FY value per scoped ticker for one metric, bounded + ordered + paged inside DuckDB.
# ``list_contains(?, ticker)`` restricts to the meta-filtered scope; the optional min/max are
# appended by the caller. The QUALIFY picks each ticker's newest FY before the bounds apply.
_METRIC_PAGE_SQL = """
    WITH latest AS (
        SELECT ticker, fiscal_year, value
        FROM dashboard_metrics
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
# toggle — date-anchored to each ticker's own Market Cap period_end. Scoped to just the
# page's tickers.
_MARKET_CAP_FX_SQL = """
    WITH mc AS (
        SELECT ticker, unit, period_end
        FROM dashboard_metrics
        WHERE metric = 'Market Cap' AND period_type = 'FY' AND value IS NOT NULL
          AND UPPER(unit) != 'USD' AND list_contains(?, ticker)
        QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) = 1
    )
    SELECT mc.ticker, fx.rate
    FROM mc
    LEFT JOIN dashboard_fx AS fx ON fx.base = UPPER(mc.unit) AND fx.quote = 'USD' AND fx.date <= mc.period_end
    QUALIFY row_number() OVER (PARTITION BY mc.ticker ORDER BY fx.date DESC) = 1
"""


# Net-Net Finder: which NCAV Ratio (Live) metric gates inclusion for each conservatism level
# (the per-share value columns below are always all three, regardless of which level is
# active), and Piotroski/Altman for the quality overlay — each fetched as that TICKER's own
# independently-latest value (see net_net_screen's query: a plain "latest per (ticker, metric)"
# pivot, no shared anchor year). "Market Cap (Live)" and price are fetched separately (bulk,
# ticker-only — see _LATEST_MARKET_CAP_LIVE_SQL/_LATEST_PRICE_SQL below), NOT through this
# pivot: both are single-row-per-ticker metrics with no meaningful fiscal_year, so joining them
# through a fiscal-year-keyed CTE the way an earlier version of this code did silently returned
# NULL market_cap for any ticker whose eligibility year differed from its own latest FY —
# confirmed as a real bug (2026-07) affecting 271 tickers before this fix.
_NET_NET_VALUE_METRICS = (
    "NCAV / Share (Live)", "NCAV (Moderate) / Share (Live)", "NCAV (Strict) / Share (Live)",
    "Piotroski F-Score", "Altman Z-Score",
)
_NET_NET_LEVEL_RATIO = {
    "relaxed":  "NCAV Ratio (Live)",
    "moderate": "NCAV (Moderate) Ratio (Live)",
    "strict":   "NCAV (Strict) Ratio (Live)",
}

# Latest close per ticker, scoped to the tickers that already passed the NCAV-ratio filter (a
# small subset of the universe — genuine net-nets are rare) — cheaper than pricing the whole
# scope up front. dashboard_prices is the daily OHLC-ish store (see CompanyRepository's own
# _PRICE_SERIES_SQL for the single-ticker equivalent); this is the bulk "latest value" variant.
_LATEST_PRICE_SQL = """
    SELECT ticker, close FROM dashboard_prices
    WHERE list_contains(?, ticker)
    QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
"""

# Market Cap (Live) — a single row per ticker (see 22__derived_metrics.py's market_cap_live),
# so this is a flat ticker-keyed lookup, deliberately NOT joined through the fiscal-year-keyed
# `vals` CTE below (see _NET_NET_VALUE_METRICS' own comment on why that broke for 271 tickers).
_LATEST_MARKET_CAP_LIVE_SQL = """
    SELECT ticker, value FROM dashboard_metrics
    WHERE metric = 'Market Cap (Live)' AND list_contains(?, ticker)
"""

# Altman Z-Score traffic-light thresholds — duplicated from fundamentals_pipeline's Streamlit
# lib (60__frontends/61__streamlit/lib/signals.py's _HIGHER_IS_BETTER["Altman Z-Score"] =
# (3.0, 1.8)), not imported: that directory is digit-prefixed (not a valid Python module path)
# and isn't part of the installed fundamentals_pipeline distribution anyway. Keep these two
# numbers in sync with signals.py by hand if that ever changes.
_ALTMAN_SAFE_MIN = 3.0
_ALTMAN_DISTRESS_MAX = 1.8

# Investor Presets: each school's latest-FY-only display/filter columns (exact names confirmed
# byte-for-byte against metrics_hierarchy.json) and its own WHERE predicate over the pivot
# aliases (see _pivot_cte). Graham's P/B-or-P/E×P/B rule and Buffett's NULL-passthrough MoS
# rule both need custom SQL, not the independent AND'd bounds `MetricFilter`/`_filter_clause`
# express — hence a dedicated predicate per preset rather than reusing that generic mechanism.
_PRESET_COLUMNS: dict[str, tuple[str, ...]] = {
    "graham": ("Current Ratio", "P/E", "P/B"),
    "buffett": ("Debt / Equity", "Gross Margin %", "Net Margin %", "MoS % (Owner Earnings, FY)",
                "ROE % (5Y Avg)"),
    # "EPS CAGR (5Y) %" is display-only (no threshold of its own — see _lynch_where, which
    # doesn't reference it); "PEG" is both displayed and the actual filter.
    "lynch": ("Debt / Equity", "Current Ratio", "ROE %", "EPS CAGR (5Y) %", "PEG"),
}


# Display/iteration order matches the Net-Net Finder's own level pill (views._NET_NET_LEVELS) —
# increasing conservatism left to right, both features agreeing on one order. The actual
# default ("strict") is a separate concern, hardcoded at each fallback site below and in
# views._presets_screen — not derived from this tuple's first element.
PRESET_LEVELS: tuple[str, ...] = ("relaxed", "moderate", "strict")

# Per-preset, per-level threshold values for the latest-FY WHERE predicate and the multi-year
# checks below — the single source of truth both this module's query builders AND services.py's
# criteria-label copy read from (never duplicated). "strict" is Graham's own literal numbers
# where one genuinely exists (The Intelligent Investor: Current Ratio >= 2, P/E <= 15,
# P/B <= 1.5 or P/E*P/B <= 22.5, 10y positive earnings, 20y uninterrupted dividends, 33% EPS
# growth over 10y using 3-year averages at each end) — except the dividend window, capped at
# 10y (the published export's own retention limit) rather than the literal 20y, which issue
# #276's Phase-0 finding confirmed is infeasible against real data (dividend payers rarely have
# 20y on record) and would return ~0 matches always. Buffett/Lynch have no single quotable rule
# set (this app's 5 criteria for each are already our own synthesis), so their "moderate" tier IS
# today's live values — the natural, already-tuned center — with strict/relaxed tightening/
# loosening from there.
_GRAHAM_THRESHOLDS: dict[str, dict[str, Any]] = {
    "strict":   {"cr_min": 2.0,  "pe_max": 15.0, "pb_max": 1.5, "pe_pb_max": 22.5,
                 "earnings_years": 10, "div_years": 10, "div_min_years": 10,
                 "eps_years": 10, "eps_min_growth": 1.33, "eps_use_3y_avg": True},
    "moderate": {"cr_min": 1.75, "pe_max": 17.5, "pb_max": 2.0, "pe_pb_max": 26.0,
                 "earnings_years": 7, "div_years": 7, "div_min_years": 5,
                 "eps_years": 7, "eps_min_growth": 1.33, "eps_use_3y_avg": False},
    "relaxed":  {"cr_min": 1.5,  "pe_max": 20.0, "pb_max": 2.5, "pe_pb_max": 30.0,
                 "earnings_years": 5, "div_years": 5, "div_min_years": 3,
                 "eps_years": 5, "eps_min_growth": 1.33, "eps_use_3y_avg": False},
}
_BUFFETT_THRESHOLDS: dict[str, dict[str, Any]] = {
    "strict":   {"de_max": 0.35, "gm_min": 50.0, "nm_min": 25.0, "mos_min": 33.0,
                 "roe_min": 15.0, "roe_years": 10},
    "moderate": {"de_max": 0.5,  "gm_min": 40.0, "nm_min": 20.0, "mos_min": 25.0,
                 "roe_min": 15.0, "roe_years": 5},
    "relaxed":  {"de_max": 0.75, "gm_min": 30.0, "nm_min": 12.0, "mos_min": 15.0,
                 "roe_min": 12.0, "roe_years": 3},
}
_LYNCH_THRESHOLDS: dict[str, dict[str, Any]] = {
    "strict":   {"de_max": 0.4, "cr_min": 1.5, "roe_min": 20.0, "peg_max": 0.75},
    "moderate": {"de_max": 0.6, "cr_min": 1.0, "roe_min": 15.0, "peg_max": 1.0},
    "relaxed":  {"de_max": 1.0, "cr_min": 0.8, "roe_min": 10.0, "peg_max": 1.5},
}
_PRESET_THRESHOLDS: dict[str, dict[str, dict[str, Any]]] = {
    "graham": _GRAHAM_THRESHOLDS, "buffett": _BUFFETT_THRESHOLDS, "lynch": _LYNCH_THRESHOLDS,
}


def preset_thresholds(preset: str, level: str) -> dict[str, Any]:
    """Public accessor for one preset's resolved threshold dict at one conservatism level — the
    exact same values `preset_screen`'s WHERE/multi-year-check building uses (via
    `_PRESET_WHERE`/`_preset_multi_year_checks`), exposed so `services.py`'s criteria-label copy
    can render the numbers the query is actually using without a second, duplicated registry.
    Falls back to "strict" for an unrecognized `level`, mirroring `preset_screen`'s own fallback;
    an unrecognized `preset` returns an empty dict (no thresholds to resolve)."""
    if level not in PRESET_LEVELS:
        level = "strict"
    return _PRESET_THRESHOLDS.get(preset, {}).get(level, {})


def _graham_where(alias: dict[str, str], t: dict[str, Any]) -> str:
    cr, pe, pb = alias["Current Ratio"], alias["P/E"], alias["P/B"]
    return (
        f"p.{cr} >= {t['cr_min']} AND p.{pe} <= {t['pe_max']}"
        f" AND (p.{pb} <= {t['pb_max']}"
        f" OR (p.{pe} IS NOT NULL AND p.{pb} IS NOT NULL AND p.{pe} * p.{pb} <= {t['pe_pb_max']}))"
    )


def _buffett_where(alias: dict[str, str], t: dict[str, Any]) -> str:
    # MoS % (Owner Earnings, FY) is NULL for Energy/Financials/Real Estate by design (the Owner
    # Earnings model is sector-gated upstream — see 23__intrinsic_value.py) — "not applicable",
    # not "fails", so those sectors are still evaluated on the other three criteria rather than
    # excluded outright by an AND'd NULL comparison.
    de, gm, nm, mos = (
        alias["Debt / Equity"], alias["Gross Margin %"], alias["Net Margin %"],
        alias["MoS % (Owner Earnings, FY)"],
    )
    return (
        f"p.{de} <= {t['de_max']} AND p.{gm} > {t['gm_min']} AND p.{nm} > {t['nm_min']}"
        f" AND (p.{mos} IS NULL OR p.{mos} >= {t['mos_min']})"
    )


def _lynch_where(alias: dict[str, str], t: dict[str, Any]) -> str:
    de, cr, roe, peg = (
        alias["Debt / Equity"], alias["Current Ratio"], alias["ROE %"], alias["PEG"],
    )
    return (
        f"p.{de} < {t['de_max']} AND p.{cr} >= {t['cr_min']} AND p.{roe} > {t['roe_min']}"
        f" AND p.{peg} < {t['peg_max']}"
    )


_PRESET_WHERE: dict[str, Any] = {
    "graham": _graham_where,
    "buffett": _buffett_where,
    "lynch": _lynch_where,
}


def _preset_multi_year_checks(preset: str, level: str) -> tuple[tuple, ...]:
    """Multi-year checks layered on top of `_PRESET_WHERE`'s latest-FY predicate, for one preset
    at one conservatism level — each a tagged check-spec tuple dispatched to the matching
    repository method via `CompanyListingRepository._qualifying_tickers`:
      ("metric", metric, min_value, years)              -> sustained_metric_tickers
      ("concept", concept, min_value, years)             -> sustained_concept_tickers
      ("dividend", metric, min_value, years, min_years)  -> uninterrupted_dividend_tickers
      ("eps_growth", concept, years, min_growth)         -> eps_growth_tickers (single-year
                                                             endpoints — moderate/relaxed)
      ("eps_growth_avg", concept, years, min_growth)     -> eps_growth_avg_tickers (3-year
                                                             averages at each end — strict only,
                                                             Graham's own literal phrasing)
    Lynch's PEG is a plain latest-FY WHERE predicate (`_lynch_where`), not a multi-year check,
    even though it filters on a metric the pipeline computes from multi-year EPS history — so
    Lynch has no entry here.
    """
    if preset == "buffett":
        t = _BUFFETT_THRESHOLDS[level]
        return (("metric", "ROE %", t["roe_min"], t["roe_years"]),)
    if preset == "graham":
        t = _GRAHAM_THRESHOLDS[level]
        eps_tag = "eps_growth_avg" if t["eps_use_3y_avg"] else "eps_growth"
        return (
            ("concept", "Net Income", 0.0, t["earnings_years"]),
            ("dividend", "Dividend Yield %", 0.0, t["div_years"], t["div_min_years"]),
            (eps_tag, "EPS Diluted", t["eps_years"], t["eps_min_growth"]),
        )
    return ()


def _altman_zone(z_score: float | None) -> str | None:
    """Classify an Altman Z-Score into "safe" (> 3.0) / "grey" (1.8-3.0) / "distress" (< 1.8),
    or None when the score itself is missing."""
    if z_score is None:
        return None
    if z_score > _ALTMAN_SAFE_MIN:
        return "safe"
    if z_score < _ALTMAN_DISTRESS_MAX:
        return "distress"
    return "grey"


@dataclass(frozen=True)
class MetricFilter:
    """One metric criterion of the multi-metric screen: keep tickers whose latest-FY ``metric``
    is within the inclusive ``[min_value, max_value]`` (either bound optional)."""

    metric: str
    min_value: float | None = None
    max_value: float | None = None


@dataclass(frozen=True)
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
        picker). Scoped to `sector` when given — Yahoo's ~145-value industry taxonomy is too
        large to show unscoped, and industries are logically nested one level under sector, so
        picking a sector first narrows this to the relevant subset."""
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

    def scope_size(
        self, *, search: str = "", sector: str = "", index: str = "", country: str = "",
        market: str = "", industry: str = "",
    ) -> int:
        """Count of tickers passing the descriptive filters, with no metric fetch at all (pure
        in-memory meta filtering via `_scope`) — for hero stats like "N net-nets out of M
        companies in scope" without paying for a DuckDB round trip just to count."""
        return len(self._scope(search=search, sector=sector, index=index, country=country,
                                market=market, industry=industry))

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
        Market Cap period_end) when it's a displayed column; every other ``$``-unit column stays
        native regardless. Returns the rows, the total match count, and the ordered display
        columns (with units, for formatting)."""
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

    # ── Net-Net Finder ───────────────────────────────────────────────────────────────────
    def net_net_screen(
        self,
        *,
        level: str,
        search: str = "",
        sector: str = "",
        index: str = "",
        country: str = "",
        market: str = "",
        industry: str = "",
    ) -> tuple[NetNetRow, ...]:
        """Every ticker in scope whose `level` NCAV Ratio (Live) is non-null (i.e. its own
        latest-FY NCAV for that level is positive, priced against today's real market cap —
        genuine, CURRENT liquidation-value candidates only), with NCAV/Share (Live) at all
        three levels, today's close, live market cap, and the Piotroski/Altman quality overlay.

        `level`: ``"relaxed"`` | ``"moderate"`` | ``"strict"`` — picks which NCAV Ratio (Live)
        metric gates inclusion; falls back to ``"relaxed"`` for an unrecognized value. The three
        NCAV/Share (Live) values are always all three, regardless of `level`.

        Unlike the old FY-anchored Ratio this replaced, every "live" metric here (NCAV/Share,
        NCAV Ratio, Market Cap) is already a single current snapshot per ticker (see
        22__derived_metrics.py's market_cap_live) — no shared-fiscal-year anchoring is needed
        or done: each of NCAV/Share/F-Score/Z-Score below is simply that ticker's own
        independently-latest value, and Market Cap (Live) is fetched separately (a flat
        ticker-only lookup, see _LATEST_MARKET_CAP_LIVE_SQL — NOT joined through this pivot,
        since it has no fiscal_year that would reliably match one). Confirmed as a real bug
        (2026-07): DMRA showed an 83% "discount to liquidation value" that was a pure division
        artifact of a stale, pre-reverse-merger weighted-average share count — using each
        ticker's actual current share count instead (Shares Outstanding (Cover Page)) fixes it.

        No pagination — genuine net-nets are a small fraction of the universe (unlike the
        general screener, which can match thousands of rows), so the whole filtered set is
        returned; the caller (``services.get_net_net_screen``) sorts in Python.
        """
        ratio_metric = _NET_NET_LEVEL_RATIO.get(level, _NET_NET_LEVEL_RATIO["relaxed"])

        scope = self._scope(search=search, sector=sector, index=index, country=country,
                             market=market, industry=industry)
        if not scope:
            return ()
        by_ticker = {rec.get("ticker", ""): rec for rec in scope}
        tickers = list(by_ticker)

        value_filters = ", ".join(
            f"max(value) FILTER (WHERE metric = ?) AS m{i}"
            for i in range(len(_NET_NET_VALUE_METRICS))
        )
        sql = f"""
            WITH eligible AS (
                SELECT ticker FROM dashboard_metrics
                WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL
                  AND list_contains(?, ticker)
            ), vals AS (
                SELECT m.ticker, m.metric, m.value
                FROM dashboard_metrics m
                JOIN eligible e ON e.ticker = m.ticker
                WHERE m.period_type = 'FY' AND list_contains(?, m.metric)
                QUALIFY row_number() OVER (
                    PARTITION BY m.ticker, m.metric ORDER BY m.fiscal_year DESC
                ) = 1
            )
            SELECT ticker, {value_filters}
            FROM vals GROUP BY ticker
        """
        params = [
            ratio_metric, tickers,               # eligible
            list(_NET_NET_VALUE_METRICS),        # vals's list_contains(?, m.metric)
            *_NET_NET_VALUE_METRICS,              # the 5 FILTER (WHERE metric = ?) params
        ]

        with self._connection() as con:
            hits = con.execute(sql, params).fetchall()
            if not hits:
                return ()

            matched_tickers = [row[0] for row in hits]
            price_rows = con.execute(_LATEST_PRICE_SQL, [matched_tickers]).fetchall()
            price_by_ticker = dict(price_rows)
            mc_rows = con.execute(_LATEST_MARKET_CAP_LIVE_SQL, [matched_tickers]).fetchall()
            market_cap_by_ticker = dict(mc_rows)

        rows = []
        for ticker, ncav_relaxed, ncav_moderate, ncav_strict, f_score, z_score in hits:
            rec = by_ticker.get(ticker, {})
            rows.append(NetNetRow(
                ticker=ticker,
                name=rec.get("company", ""),
                sector=rec.get("sector"),
                industry=rec.get("industry"),
                country=rec.get("country"),
                market=rec.get("market"),
                has_logo=_has_logo(rec),
                price=price_by_ticker.get(ticker),
                market_cap=market_cap_by_ticker.get(ticker),
                ncav_per_share_relaxed=ncav_relaxed,
                ncav_per_share_moderate=ncav_moderate,
                ncav_per_share_strict=ncav_strict,
                f_score=f_score,
                z_score=z_score,
                z_score_zone=_altman_zone(z_score),
            ))
        return tuple(rows)

    def sustained_metric_tickers(
        self, *, tickers: Sequence[str], metric: str, min_value: float, years: int,
    ) -> frozenset[str]:
        """Tickers (out of `tickers`) whose `metric` FY value was >= `min_value` in EVERY ONE
        of their most recent `years` fiscal years. A ticker with fewer than `years` non-null FY
        values on record is excluded, not passed — "sustained over N years" requires N years of
        proof, the same reasoning the Net-Net Finder's own eligibility filters already apply
        elsewhere. Shared across whichever Investor Presets criteria need an every-year-
        threshold test (currently only Buffett's ROE via `_PRESET_MULTI_YEAR`; Graham's
        earnings-stability/dividend-record and Lynch's future criteria are separate, not-yet-
        built issues that can reuse this same method with a different metric/threshold/years —
        not a comparator this method doesn't support yet, `>=` only, add one if/when one of
        those needs something else).
        """
        if not tickers:
            return frozenset()
        sql = """
            WITH recent AS (
                SELECT ticker, value
                FROM dashboard_metrics
                WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL
                  AND list_contains(?, ticker)
                QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) <= ?
            )
            SELECT ticker
            FROM recent
            GROUP BY ticker
            HAVING count(*) = ?
               AND count(*) FILTER (WHERE value >= ?) = ?
        """
        return frozenset(
            self._fetch_column(sql, [metric, list(tickers), years, years, min_value, years])
        )

    def sustained_concept_tickers(
        self, *, tickers: Sequence[str], concept: str, min_value: float, years: int,
    ) -> frozenset[str]:
        """Same "every one of the most recent `years` FY values >= `min_value`" test as
        `sustained_metric_tickers`, but over `dashboard_data` (raw statement `concept`/`value`,
        e.g. "Net Income") rather than `dashboard_metrics` (derived `metric`/`value`) — a raw
        line item is never published as a derived metric, so a criterion built on one (Graham's
        "positive earnings, several years") needs this table instead. `period_type = 'FY'`
        scoping and the exact-`years`-count requirement match `sustained_metric_tickers` exactly;
        kept as a separate method rather than a table/column parameter on that one since the two
        source tables have different column names (`concept` vs `metric`).
        """
        if not tickers:
            return frozenset()
        sql = """
            WITH recent AS (
                SELECT ticker, value
                FROM dashboard_data
                WHERE concept = ? AND period_type = 'FY' AND value IS NOT NULL
                  AND list_contains(?, ticker)
                QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) <= ?
            )
            SELECT ticker
            FROM recent
            GROUP BY ticker
            HAVING count(*) = ?
               AND count(*) FILTER (WHERE value >= ?) = ?
        """
        return frozenset(
            self._fetch_column(sql, [concept, list(tickers), years, years, min_value, years])
        )

    def uninterrupted_dividend_tickers(
        self, *, tickers: Sequence[str], metric: str, min_value: float, years: int, min_years: int,
    ) -> frozenset[str]:
        """Tickers (out of `tickers`) with a *contiguous*, *currently active* run of >= `min_value`
        `metric` (e.g. "Dividend Yield %") FY values of at least `min_years`, counting back from
        each ticker's own most recent fiscal year on record — capped at looking back `years` years.

        Deliberately NOT `sustained_metric_tickers` (which just requires the latest `years`
        non-null rows to all clear the bar): dividend metrics are only ever published for years a
        dividend was actually paid (a non-payer/cut year has no row at all, not a zero row — see
        this repository's other dividend-metric callers), so `sustained_metric_tickers`'s "latest
        N non-null rows" window can silently reach past a real gap into a company's paying years
        from *before* it stopped, incorrectly treating a lapsed payer as "sustained." Confirmed
        against real fixture data before building this: an anchor a company's own most recent
        reported fiscal year (over `dashboard_metrics` generally, not just this metric) is
        required to catch that; anchoring to the *dataset-wide* max fiscal year instead was tried
        first and wrongly excluded ~480 genuinely current payers (a lone outlier ticker with a
        later fiscal year skewed the dataset-wide max).

        `min_years` (not `years`) is the actual pass bar — a floor lower than `years` per the
        milestone's Phase 0 historical-depth finding, which flagged that "uninterrupted dividend"
        specifically should tolerate a newer payer's shorter-but-real record (e.g. 3 real years)
        rather than hard-requiring the full window the way Buffett's ROE/Graham's earnings
        criteria do — those two have no equivalent "omitted, not zeroed" row-shape and so don't
        need this tolerance.
        """
        if not tickers:
            return frozenset()
        sql = """
            WITH ticker_latest AS (
                SELECT ticker, max(fiscal_year) AS latest_fy
                FROM dashboard_metrics
                WHERE period_type = 'FY' AND list_contains(?, ticker)
                GROUP BY ticker
            ),
            div_rows AS (
                SELECT ticker, fiscal_year,
                    row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) AS rn
                FROM dashboard_metrics
                WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL AND value >= ?
                  AND list_contains(?, ticker)
                QUALIFY rn <= ?
            ),
            runs AS (
                SELECT ticker, fiscal_year, rn, (fiscal_year + rn) AS run_id
                FROM div_rows
            )
            SELECT r1.ticker
            FROM runs r1
            JOIN runs r2 ON r2.ticker = r1.ticker AND r2.run_id = r1.run_id
            JOIN ticker_latest t ON t.ticker = r1.ticker
            WHERE r1.rn = 1 AND r1.fiscal_year >= t.latest_fy - 1
            GROUP BY r1.ticker
            HAVING count(*) >= ?
        """
        tickers_list = list(tickers)
        return frozenset(
            self._fetch_column(
                sql,
                [tickers_list, metric, min_value, tickers_list, years, min_years],
            )
        )

    def eps_growth_tickers(
        self, *, tickers: Sequence[str], concept: str, years: int, min_growth: float,
    ) -> frozenset[str]:
        """Tickers (out of `tickers`) whose latest-FY `concept` value (e.g. "EPS Diluted") is >=
        `min_growth` times its value from exactly `years` fiscal years earlier — Graham's actual
        "EPS growth >= 33% over several years" test (`min_growth=1.33`) is an endpoint-to-endpoint
        comparison over a window, NOT a per-year compounded rate (that would be an absurdly high,
        almost-never-met bar) and NOT the single-year "Net Income YoY %" metric already published
        (a different, single-period figure, and Net Income rather than EPS specifically).

        Both endpoints must be strictly positive: mirrors `23__intrinsic_value.py`'s own `eps > 0`
        gate on its (unpublished, internal-only) EPS CAGR calc for the Graham Revised model — a
        company recovering from a loss to any positive EPS isn't "33% growth", it's a different
        (turnaround) situation this test isn't meant to capture. A ticker missing the exact
        FY-`years`-ago row is excluded (insufficient history), matching this repository's existing
        "no proof, no pass" convention (e.g. `sustained_metric_tickers`'s own exact-count
        requirement).
        """
        if not tickers:
            return frozenset()
        sql = """
            WITH eps_rows AS (
                SELECT ticker, fiscal_year, value,
                    row_number() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) AS rn
                FROM dashboard_data
                WHERE concept = ? AND period_type = 'FY' AND value IS NOT NULL
                  AND list_contains(?, ticker)
            ),
            latest AS (
                SELECT ticker, value AS latest_eps, fiscal_year AS latest_fy
                FROM eps_rows WHERE rn = 1
            ),
            base AS (
                SELECT e.ticker, e.value AS base_eps
                FROM eps_rows e
                JOIN latest l ON l.ticker = e.ticker
                WHERE e.fiscal_year = l.latest_fy - ?
            )
            SELECT l.ticker
            FROM latest l
            JOIN base b ON b.ticker = l.ticker
            WHERE l.latest_eps > 0 AND b.base_eps > 0 AND l.latest_eps >= b.base_eps * ?
        """
        return frozenset(
            self._fetch_column(sql, [concept, list(tickers), years, min_growth])
        )

    def eps_growth_avg_tickers(
        self, *, tickers: Sequence[str], concept: str, years: int, min_growth: float,
    ) -> frozenset[str]:
        """Graham's own literal EPS-growth test (The Intelligent Investor): "an increase of at
        least one-third... using three-year averages at the beginning and end" of a `years`-year
        span (Graham's own span is 10). Distinct from `eps_growth_tickers`'s single-year endpoint
        comparison: averaging smooths over one bad year at either end — the whole point of
        Graham's own phrasing — but both resulting averages still have to be `> 0` (a smoothed
        "growth" figure from a net-negative base isn't meaningful either, same reasoning
        `eps_growth_tickers` already applies to its own single-year endpoints). Each 3-year
        window requires all 3 real FY rows present — no partial-average passes, matching this
        repository's existing "no proof, no pass" convention.

        For a `years`-year span ending at a ticker's own latest FY: the END window is the most
        recent 3 years of the span; the BASE window is the earliest 3 years of the span (i.e.
        `years - 1` to `years - 3` years before latest, so the two 3-year windows sit at opposite
        ends of the same `years`-year span, not overlapping and not adjacent).
        """
        if not tickers:
            return frozenset()
        sql = """
            WITH eps_rows AS (
                SELECT ticker, fiscal_year, value
                FROM dashboard_data
                WHERE concept = ? AND period_type = 'FY' AND value IS NOT NULL
                  AND list_contains(?, ticker)
            ),
            latest AS (
                SELECT ticker, max(fiscal_year) AS latest_fy FROM eps_rows GROUP BY ticker
            ),
            end_avg AS (
                SELECT e.ticker, avg(e.value) AS avg_value, count(*) AS n
                FROM eps_rows e JOIN latest l ON l.ticker = e.ticker
                WHERE e.fiscal_year BETWEEN l.latest_fy - 2 AND l.latest_fy
                GROUP BY e.ticker
            ),
            base_avg AS (
                SELECT e.ticker, avg(e.value) AS avg_value, count(*) AS n
                FROM eps_rows e JOIN latest l ON l.ticker = e.ticker
                WHERE e.fiscal_year BETWEEN l.latest_fy - (? - 1) AND l.latest_fy - (? - 3)
                GROUP BY e.ticker
            )
            SELECT en.ticker
            FROM end_avg en
            JOIN base_avg b ON b.ticker = en.ticker
            WHERE en.n = 3 AND b.n = 3
              AND en.avg_value > 0 AND b.avg_value > 0
              AND en.avg_value >= b.avg_value * ?
        """
        return frozenset(
            self._fetch_column(sql, [concept, list(tickers), years, years, min_growth])
        )

    def _qualifying_tickers(self, check: tuple, tickers: Sequence[str]) -> frozenset[str]:
        """Dispatch one `_preset_multi_year_checks` check-spec tuple to its repository method —
        see that function's own comment for the tag/shape of each kind."""
        kind = check[0]
        if kind == "metric":
            _, metric, min_value, years = check
            return self.sustained_metric_tickers(
                tickers=tickers, metric=metric, min_value=min_value, years=years,
            )
        if kind == "concept":
            _, concept, min_value, years = check
            return self.sustained_concept_tickers(
                tickers=tickers, concept=concept, min_value=min_value, years=years,
            )
        if kind == "dividend":
            _, metric, min_value, years, min_years = check
            return self.uninterrupted_dividend_tickers(
                tickers=tickers, metric=metric, min_value=min_value, years=years,
                min_years=min_years,
            )
        if kind == "eps_growth":
            _, concept, years, min_growth = check
            return self.eps_growth_tickers(
                tickers=tickers, concept=concept, years=years, min_growth=min_growth,
            )
        if kind == "eps_growth_avg":
            _, concept, years, min_growth = check
            return self.eps_growth_avg_tickers(
                tickers=tickers, concept=concept, years=years, min_growth=min_growth,
            )
        raise ValueError(f"unknown multi-year check kind: {kind!r}")

    # ── Investor Presets ─────────────────────────────────────────────────────────────────
    def preset_screen(
        self,
        *,
        preset: str,
        level: str = "strict",
        sector: str = "",
        index: str = "",
        country: str = "",
        market: str = "",
        industry: str = "",
        sort: SortSpec | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[tuple[ScreenTableRow, ...], int, tuple[ScreenColumn, ...]]:
        """One page of an Investor Presets school's matching companies, the total match count,
        and the display columns — same pivot/pagination machinery as ``screen_table``, but with
        each preset's own fixed filter predicate (``_PRESET_WHERE``) rather than a user-composed
        ``MetricFilter`` list: Graham's combined P/E×P/B-or-P/B rule and Buffett's NULL-
        passthrough Margin-of-Safety rule aren't expressible as independent AND'd bounds (a
        NULL pivot value always fails a plain ``MetricFilter`` bound — see ``_filter_clause`` —
        which is wrong for Buffett's MoS: Energy/Financials/Real Estate tickers have it NULL by
        design, not failing, so they must still be evaluated on their other three criteria).
        Falls back to an empty result for an unrecognized `preset`. ``level`` is the conservatism
        tier ("strict"/"moderate"/"relaxed" — see `_PRESET_THRESHOLDS`), falling back to
        "strict" (the toughest, and the default) for anything unrecognized — resolves the
        preset's own threshold dict, fed to both `_PRESET_WHERE` and `_preset_multi_year_checks`
        so the latest-FY predicate and the multi-year checks always agree on which level is
        active. ``sort`` reuses ``_order_clause`` — a descriptive column or one of the preset's
        own display columns; defaults to ``s.ticker`` for anything else (same safe fallback
        ``screen_table`` relies on).
        """
        columns = list(_PRESET_COLUMNS.get(preset, ()))
        if not columns:
            return (), 0, ()
        if level not in PRESET_LEVELS:
            level = "strict"
        thresholds = _PRESET_THRESHOLDS.get(preset, {}).get(level, {})
        scope = self._scope(search="", sector=sector, index=index, country=country, market=market,
                             industry=industry)
        for check in _preset_multi_year_checks(preset, level):
            qualifying = self._qualifying_tickers(
                check, [rec.get("ticker", "") for rec in scope],
            )
            scope = [rec for rec in scope if rec.get("ticker", "") in qualifying]
        offset = max(0, (page - 1) * page_size)
        sort = sort or SortSpec()
        if not scope:
            return (), 0, tuple(ScreenColumn(key=m) for m in columns)

        scope_rows = [
            (
                rec.get("ticker", ""), rec.get("company", ""), rec.get("sector"),
                rec.get("industry"), rec.get("country"), rec.get("market"), _has_logo(rec),
            )
            for rec in scope
        ]
        alias = {metric: f"m{i}" for i, metric in enumerate(columns)}

        with self._connection() as con:
            con.execute("DROP TABLE IF EXISTS scoped")
            con.execute(
                "CREATE TEMP TABLE scoped"
                " (ticker VARCHAR, name VARCHAR, sector VARCHAR, industry VARCHAR,"
                " country VARCHAR, market VARCHAR, has_logo BOOLEAN)"
            )
            con.executemany("INSERT INTO scoped VALUES (?, ?, ?, ?, ?, ?, ?)", scope_rows)

            units = self._metric_units(con, columns)
            cte, cte_params = self._pivot_cte(columns, alias)
            where = f" WHERE {_PRESET_WHERE[preset](alias, thresholds)}"
            select_cols = ", ".join(f"p.{alias[m]}, p.{alias[m]}_u" for m in columns)
            projection = (
                "s.ticker, s.name, s.sector, s.industry, s.country, s.market, s.has_logo, "
                + select_cols
            )
            from_join = "scoped s LEFT JOIN pivoted p USING (ticker)"

            count_sql = f"{cte}SELECT count(*) AS n FROM {from_join}{where}"
            count_row = con.execute(count_sql, cte_params).fetchone()
            total = int(count_row[0]) if count_row else 0

            order_sql = self._order_clause(sort, alias)
            page_sql = f"{cte}SELECT {projection} FROM {from_join}{where}{order_sql} LIMIT ? OFFSET ?"
            cursor = con.execute(page_sql, cte_params + [page_size, offset])
            hits = cursor.fetchall()

            rows = tuple(
                ScreenTableRow(
                    ticker=row[0], name=row[1], sector=row[2], industry=row[3],
                    country=row[4], market=row[5], has_logo=row[6],
                    values={m: row[7 + 2 * i] for i, m in enumerate(columns)},
                    units={m: row[7 + 2 * i + 1] for i, m in enumerate(columns)},
                )
                for row in hits
            )

        columns_meta = tuple(ScreenColumn(key=m, unit=units.get(m)) for m in columns)
        return rows, total, columns_meta

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
            "SELECT metric, any_value(unit) AS unit FROM dashboard_metrics"
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
        native reporting currency — read per row here rather than assumed column-wide. Empty
        when no metrics."""
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
            "  SELECT ticker, metric, value, unit FROM dashboard_metrics"
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
