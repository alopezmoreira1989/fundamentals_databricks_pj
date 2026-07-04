"""Read-model tests: repositories → services → views, offline against fixture artifacts.

No database (no ORM here); the ``artifacts_from_fixtures`` fixture points storage at the
committed Streamlit fixtures and DuckDB queries the parquet directly.
"""

from __future__ import annotations

import dataclasses

import pytest
from apps.companies import services as company_services
from apps.valuation.football import build_chart
from repositories.companies import CompanyRepository
from repositories.company_listing import CompanyListingRepository, MetricFilter, SortSpec
from repositories.dtos import (
    CompanyListRow,
    CompanySummary,
    MetricPoint,
    ScreenColumn,
    ScreenRow,
    ScreenTablePage,
    ScreenTableRow,
)
from repositories.screener import ScreenerRepository
from repositories.valuation import ValuationRepository

TICKER = "AAPL"


def _a_metric_of(ticker: str) -> str:
    """A metric name the ticker actually has a non-null latest-FY value for."""
    for m in CompanyRepository().latest_metrics(ticker):
        if m.value is not None:
            return m.metric
    raise AssertionError(f"no non-null metric found for {ticker} in fixtures")


# ── repositories ─────────────────────────────────────────────────────────────────────
def test_company_summary(artifacts_from_fixtures):
    summary = CompanyRepository().get_summary(TICKER)
    assert isinstance(summary, CompanySummary)
    assert summary.ticker == TICKER
    assert summary.name  # non-empty company name
    assert CompanyRepository().get_summary("NOTAREALTICKER") is None


def test_latest_metrics_grouped_and_ordered(artifacts_from_fixtures):
    metrics = CompanyRepository().latest_metrics(TICKER)
    assert metrics and all(isinstance(m, MetricPoint) for m in metrics)
    # latest value per metric ⇒ each metric appears once
    names = [m.metric for m in metrics]
    assert len(names) == len(set(names))
    # every row carries hierarchy fields (None-category rows are filtered out) and is
    # ordered by sort_order so categories come out as contiguous blocks
    assert all(m.category is not None and m.sort_order is not None for m in metrics)
    orders = [m.sort_order for m in metrics]
    assert orders == sorted(orders)
    seen: list[str] = []
    for m in metrics:
        if not seen or seen[-1] != m.category:
            assert m.category not in seen, "categories must be contiguous for regroup"
            seen.append(m.category)


def test_get_statements_pivots_line_items_by_year(artifacts_from_fixtures):
    result = CompanyRepository().get_statements(TICKER, max_years=6)
    names = [s.name for s in result.statements]
    assert names == ["Income Statement", "Balance Sheet", "Cash Flow"]  # fixed reporting order
    income = result.statements[0]
    assert 1 <= len(income.years) <= 6
    assert income.years == tuple(sorted(income.years, reverse=True))  # newest first
    # Every line's values align to the year columns; Revenue is present and positive.
    assert all(len(line.values) == len(income.years) for line in income.lines)
    revenue = next(line for line in income.lines if line.display_name == "Revenue")
    assert revenue.values[0] and revenue.values[0] > 0


def test_get_statements_unknown_ticker_is_empty(artifacts_from_fixtures):
    assert CompanyRepository().get_statements("NOTAREALTICKER").statements == ()


def test_get_quarterly_pivots_income_statement(artifacts_from_fixtures):
    grid = CompanyRepository().get_quarterly(TICKER, max_quarters=6)
    assert grid.name == "Income Statement"
    assert 1 <= len(grid.columns) <= 6
    assert all(c[0] == "Q" for c in grid.columns)  # labels like "Q2 2026"
    revenue = next(line for line in grid.lines if line.display_name == "Revenue")
    assert len(revenue.values) == len(grid.columns)
    assert revenue.values[0] and revenue.values[0] > 0


def test_price_series_downsampled_and_chart_geometry(artifacts_from_fixtures):
    from apps.companies.pricechart import build_price_chart

    series = CompanyRepository().price_series(TICKER, max_points=120)
    assert 2 <= len(series) <= 121  # downsampled (+ the always-kept latest close)
    dates = [p.date for p in series]
    assert dates == sorted(dates)  # ascending
    chart = build_price_chart(series)
    assert chart is not None
    assert chart.min_close <= chart.last_close <= chart.max_close
    assert chart.polyline and chart.area.endswith("Z")
    assert build_price_chart(()) is None  # empty series → no chart


def test_headline_kpis_from_statements(artifacts_from_fixtures):
    from apps.companies import services

    kpis = services.headline_kpis(CompanyRepository().get_statements(TICKER))
    labels = [k.label for k in kpis]
    assert labels == ["Revenue", "Net income", "Total assets", "Operating cash flow"]
    assert all(k.value is not None for k in kpis)  # AAPL reports all four


def test_company_summary_carries_profile_fields(artifacts_from_fixtures):
    summary = CompanyRepository().get_summary(TICKER)
    assert summary is not None
    assert summary.description and "Apple" in summary.description
    assert summary.website and "apple.com" in summary.website
    assert isinstance(summary.employees, int) and summary.employees > 0  # stringified → int
    assert summary.has_logo is True  # "True" → bool
    assert summary.founded != "None"  # literal "None" (if present) normalized away


def test_meta_field_normalizers():
    from repositories.companies import _as_bool, _as_int, _clean

    assert _clean("None") is None and _clean("") is None and _clean(None) is None
    assert _clean("Apple Inc.") == "Apple Inc."
    assert _as_int("166000") == 166000 and _as_int("None") is None and _as_int(None) is None
    assert _as_bool("True") is True and _as_bool("False") is False and _as_bool(None) is None


def test_dtos_are_immutable(artifacts_from_fixtures):
    summary = CompanyRepository().get_summary(TICKER)
    assert summary is not None
    with pytest.raises(dataclasses.FrozenInstanceError):
        summary.name = "mutated"


def test_screener_respects_bounds_ordering_and_limit(artifacts_from_fixtures):
    metric = _a_metric_of(TICKER)
    rows = ScreenerRepository().screen(metric=metric, limit=5)
    assert rows and all(isinstance(r, ScreenRow) for r in rows)
    assert len(rows) <= 5
    values = [r.value for r in rows if r.value is not None]
    assert values == sorted(values, reverse=True)  # ordered by value desc

    # A min bound only returns rows at/above it.
    floor = values[-1]
    bounded = ScreenerRepository().screen(metric=metric, min_value=floor, limit=50)
    assert all(r.value is not None and r.value >= floor for r in bounded)


def test_screener_available_metrics(artifacts_from_fixtures):
    metrics = ScreenerRepository().available_metrics()
    assert metrics and all(isinstance(m, str) for m in metrics)
    assert list(metrics) == sorted(metrics)  # distinct, alphabetical
    assert len(set(metrics)) == len(metrics)


def test_valuation_returns_only_mos_metrics(artifacts_from_fixtures):
    points = ValuationRepository().margin_of_safety(TICKER)
    assert points  # AAPL has MoS metrics in the fixtures
    assert all(p.metric.startswith("MoS ") for p in points)


def test_sparkline_svg_geometry():
    from apps.companies.charts import sparkline_svg

    svg = sparkline_svg([1.0, 2.0, 1.5, 3.0])
    assert svg.startswith("<svg") and "<polyline" in svg
    assert sparkline_svg([1.0]) == ""  # fewer than 2 points → no sparkline
    assert sparkline_svg([]) == ""
    assert sparkline_svg([1.0, None, 3.0]).startswith("<svg")  # skips the gap, still draws


def test_tab_charts_build_from_statements(artifacts_from_fixtures):
    from apps.companies import charts

    repo = CompanyRepository()
    by_name = {s.name: s for s in repo.get_statements(TICKER).statements}
    income = charts.income_statement_chart(by_name["Income Statement"])
    assert income and "<rect" in income.svg and "polyline" in income.svg  # revenue bars + NI line
    assert dict(income.legend).get("Revenue")
    cash = charts.cash_flow_chart(by_name["Cash Flow"])
    assert cash and {n for n, _ in cash.legend} == {"Operating", "Investing", "Financing"}
    quarterly = charts.quarterly_chart(repo.get_quarterly(TICKER))
    assert quarterly and "<rect" in quarterly.svg


def test_balance_sheet_compositions(artifacts_from_fixtures):
    from apps.companies import charts

    balance = next(s for s in CompanyRepository().get_statements(TICKER).statements if s.name == "Balance Sheet")
    comps = charts.balance_sheet_compositions(balance)
    assert comps  # one per fiscal year
    latest = comps[0]
    assert latest.year == max(balance.years)  # newest first
    for stack in (latest.assets, latest.liabilities_equity):
        assert stack.segments
        assert abs(sum(s.pct for s in stack.segments) - 100) < 0.5  # segments fill the bar
    # assets == liabilities + equity (the accounting identity)
    assert abs(latest.assets.total - latest.liabilities_equity.total) < 1


def test_mos_scenarios_pivot_by_method_and_basis(artifacts_from_fixtures):
    scenarios = ValuationRepository().margin_of_safety_scenarios(TICKER)
    assert scenarios  # AAPL has MoS scenarios
    # Each row collapses one method+basis into Bear/Mid/Bull; each key appears once.
    keys = [(s.method, s.basis) for s in scenarios]
    assert len(keys) == len(set(keys))
    # Ordered: grouped by method, TTM basis before FY within a method.
    dcf = [s for s in scenarios if s.method == "DCF"]
    assert [s.basis for s in dcf] == ["TTM", "FY"]
    # Bear <= Mid <= Bull for a populated row (MoS rises from bear to bull scenario).
    populated = next(s for s in scenarios if None not in (s.bear, s.mid, s.bull))
    assert populated.bear <= populated.mid <= populated.bull


def test_intrinsic_value_field_pivots_scenarios(artifacts_from_fixtures):
    field = ValuationRepository().intrinsic_value_field(TICKER)
    assert field.bars  # AAPL has TTM IV methods
    assert field.price is not None and field.price > 0
    # every bar is a well-formed range and no total-dollar row leaked in (per-share values)
    for b in field.bars:
        assert b.bear <= b.mid <= b.bull
        assert "(TTM)" in b.method and b.bull < 100_000
    # sorted by mid descending
    mids = [b.mid for b in field.bars]
    assert mids == sorted(mids, reverse=True)


def test_football_chart_geometry(artifacts_from_fixtures):
    chart = build_chart(ValuationRepository().intrinsic_value_field(TICKER))
    assert chart is not None
    for b in chart.bars:
        assert 0.0 <= b.left_pct <= 100.0
        assert 0.0 <= b.left_pct + b.width_pct <= 100.0 + 1e-6
        assert b.left_pct <= b.mid_pct <= b.left_pct + b.width_pct + 1e-6
    assert chart.price_pct is not None and 0.0 <= chart.price_pct <= 100.0


# ── service ──────────────────────────────────────────────────────────────────────────
def test_company_service_composes_detail(artifacts_from_fixtures):
    detail = company_services.get_company_detail(TICKER)
    assert detail is not None
    assert detail.summary.ticker == TICKER
    assert detail.metrics  # non-empty
    assert company_services.get_company_detail("NOTAREALTICKER") is None


# ── views ────────────────────────────────────────────────────────────────────────────
def test_company_page_html_200_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"/companies/{TICKER.lower()}/")  # lower-case → view upper-cases it
    assert resp.status_code == 200
    assert resp["Content-Type"].startswith("text/html")
    html = resp.content.decode()
    # ticker + a fundamentals category heading in the Derived-metrics tab. Intrinsic-value
    # metrics are intentionally NOT here — they live in the Valuation tab (no redundancy).
    assert TICKER in html and "Profitability" in html
    assert "Intrinsic Value" not in html  # dropped from Derived metrics

    assert client.get("/companies/notareal/").status_code == 404


def test_company_data_json_200_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"/companies/{TICKER.lower()}/data/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == TICKER
    assert isinstance(body["metrics"], list) and body["metrics"]

    assert client.get("/companies/notareal/data/").status_code == 404


def test_screener_page_renders_form(artifacts_from_fixtures, client):
    resp = client.get("/screener/")  # no metric ⇒ just the form, still 200
    assert resp.status_code == 200
    assert resp["Content-Type"].startswith("text/html")
    assert "Screener" in resp.content.decode()


def test_screener_page_shows_results_and_links(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    resp = client.get("/screener/", {"metric": metric, "limit": 5})
    assert resp.status_code == 200
    html = resp.content.decode()
    assert metric in html
    assert 'href="/companies/' in html  # each hit links to its company page


def test_screener_data_json_requires_metric(artifacts_from_fixtures, client):
    assert client.get("/screener/data/").status_code == 400
    assert client.get("/screener/data/?metric=X&min=abc").status_code == 400


def test_screener_data_json_returns_results(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    resp = client.get("/screener/data/", {"metric": metric, "limit": 5})
    assert resp.status_code == 200
    body = resp.json()
    assert body["metric"] == metric
    assert body["count"] == len(body["results"]) <= 5


def test_valuation_page_html_200(artifacts_from_fixtures, client):
    resp = client.get(f"/valuation/{TICKER.lower()}/")  # lower-case → view upper-cases it
    assert resp.status_code == 200
    assert resp["Content-Type"].startswith("text/html")
    html = resp.content.decode()
    assert TICKER in html and "Margin of Safety" in html
    assert "Intrinsic Value" in html  # football field rendered


def test_valuation_data_json_200(artifacts_from_fixtures, client):
    resp = client.get(f"/valuation/{TICKER}/data/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == TICKER
    assert all(row["metric"].startswith("MoS ") for row in body["margin_of_safety"])


# ── company listing (screener table) ─────────────────────────────────────────────────
def test_listing_default_is_whole_universe(artifacts_from_fixtures):
    repo = CompanyListingRepository()
    rows, total = repo.list_page(page=1, page_size=50)
    assert total > 100  # the full universe, not a single-metric slice
    assert len(rows) == 50 and all(isinstance(r, CompanyListRow) for r in rows)
    assert all(r.name for r in rows) and all(r.metric_value is None for r in rows)
    tickers = [r.ticker for r in rows]
    assert tickers == sorted(tickers)  # ticker-ordered for stable paging


def test_listing_pagination_is_disjoint_and_sized(artifacts_from_fixtures):
    repo = CompanyListingRepository()
    (p1, total), (p2, _) = repo.list_page(page=1, page_size=25), repo.list_page(page=2, page_size=25)
    assert len(p1) == 25 and len(p2) == 25
    assert not ({r.ticker for r in p1} & {r.ticker for r in p2})  # no overlap between pages
    assert total > 50


def test_listing_search_and_sector_and_index_narrow(artifacts_from_fixtures):
    repo = CompanyListingRepository()
    _, all_total = repo.list_page(page_size=1)
    _, aapl_total = repo.list_page(search="aapl", page_size=1)
    assert 0 < aapl_total < all_total
    sectors = repo.available_sectors()
    assert sectors  # non-empty picker
    _, sector_total = repo.list_page(sector=sectors[0], page_size=1)
    assert 0 < sector_total <= all_total
    _, sp_total = repo.list_page(index="sp500", page_size=1)
    _, r3_total = repo.list_page(index="r3000", page_size=1)
    assert 0 < sp_total <= r3_total <= all_total


def test_listing_metric_filter_bounds_and_orders(artifacts_from_fixtures):
    metric = _a_metric_of(TICKER)
    repo = CompanyListingRepository()
    rows, total = repo.list_page(metric=metric, page_size=50)
    assert rows and all(r.metric_value is not None for r in rows)
    values = [r.metric_value for r in rows]
    assert values == sorted(values, reverse=True)  # ordered by value desc

    floor = min(v for v in values if v is not None)
    bounded, bounded_total = repo.list_page(metric=metric, min_value=floor, page_size=50)
    assert bounded_total <= total
    assert all(r.metric_value is not None and r.metric_value >= floor for r in bounded)


# ── multi-metric screener table ───────────────────────────────────────────────────────
def _two_metrics_of(ticker: str) -> list[str]:
    """Two metric names the ticker has non-null latest-FY values for."""
    got = [m.metric for m in CompanyRepository().latest_metrics(ticker) if m.value is not None]
    return got[:2]


def test_screen_table_pivots_columns_over_the_universe(artifacts_from_fixtures):
    cols = _two_metrics_of(TICKER)
    page = CompanyListingRepository().screen_table(columns=cols, page=1, page_size=10)
    assert isinstance(page, ScreenTablePage)
    assert page.total > 100  # the whole universe, not a single-metric slice
    assert len(page.rows) == 10 and all(isinstance(r, ScreenTableRow) for r in page.rows)
    assert tuple(c.key for c in page.columns) == tuple(cols)
    assert all(isinstance(c, ScreenColumn) for c in page.columns)
    for r in page.rows:
        assert set(r.values) == set(cols)  # every row carries a slot per column
    tickers = [r.ticker for r in page.rows]
    assert tickers == sorted(tickers)  # default sort: ticker asc


def test_screen_table_filters_and_sorts_by_metric(artifacts_from_fixtures):
    cols = _two_metrics_of(TICKER)
    metric = cols[0]
    repo = CompanyListingRepository()
    full = repo.screen_table(columns=cols, page_size=500)
    vals = sorted(v for r in full.rows for v in [r.values[metric]] if v is not None)
    floor = vals[len(vals) // 2]  # median-ish bound

    filtered = repo.screen_table(
        columns=cols,
        filters=[MetricFilter(metric=metric, min_value=floor)],
        sort=SortSpec(key=metric, descending=True),
        page_size=500,
    )
    assert filtered.total <= full.total
    got = [r.values[metric] for r in filtered.rows]
    assert all(v is not None and v >= floor for v in got)  # NULL/below-floor excluded
    assert got == sorted(got, reverse=True)  # ordered by the metric, desc


def test_screen_table_empty_scope_returns_no_rows(artifacts_from_fixtures):
    page = CompanyListingRepository().screen_table(
        columns=_two_metrics_of(TICKER), search="___no_such_ticker___"
    )
    assert page.total == 0 and page.rows == ()


def test_screener_page_default_lists_companies(artifacts_from_fixtures, client):
    resp = client.get("/screener/")  # no filters ⇒ the whole company table, page 1
    assert resp.status_code == 200
    html = resp.content.decode()
    assert "match" in html  # the total-count line
    assert 'href="/companies/' in html  # rows link to company pages


def test_screener_page_metric_adds_value_column(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    html = client.get("/screener/", {"metric": metric}).content.decode()
    assert metric in html  # metric column header
    assert 'href="/companies/' in html


def test_screener_page_multi_columns_and_sort(artifacts_from_fixtures, client):
    a, b = _two_metrics_of(TICKER)
    resp = client.get("/screener/", {"col": [a, b], "sort": a, "dir": "desc"})
    assert resp.status_code == 200
    html = resp.content.decode()
    assert a in html and b in html  # both metric columns rendered
    assert "sort=" in html and "dir=" in html  # sortable header links carry sort state
    assert 'href="/companies/' in html


def test_screener_page_metric_filter_narrows(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    unfiltered = client.get("/screener/", {"col": metric}).content.decode()
    # An impossible lower bound must shrink the match count to zero.
    filtered = client.get(
        "/screener/", {"col": metric, "fmetric": metric, "fmin": "1e30"}
    ).content.decode()
    assert "No companies match" in filtered
    assert "No companies match" not in unfiltered


def test_screener_page_bad_bound_shows_error_not_500(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    resp = client.get("/screener/", {"fmetric": metric, "fmin": "abc"})
    assert resp.status_code == 200
    assert "must be numbers" in resp.content.decode()
