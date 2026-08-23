"""Template-render smoke test for the Company Detail page's currency-lens changes.

Renders `company_detail.html` directly (Django's template engine, real DTOs, no DuckDB/network)
with a context shaped exactly like `views.company_detail()` builds it — the same "render the
template directly through Django's engine before considering it fixed" check used earlier for
the leaked `{# #}` comment bug. Catches template syntax errors and filter/tag misuse (e.g. the
new `statement_panes` 3-tuple unpacking, the masthead currency-selector form, `st_ccy`/
`quarterly_currency`/`iv_currency`) that a pure-Python unit test can't see, without needing real
production parquet data.
"""

from __future__ import annotations

import pytest
from django.template.loader import render_to_string
from fundamentals_screener.dtos import (
    CompanyDetail,
    CompanyStatements,
    FootballBar,
    FootballField,
    HeadlineKpi,
    MetricPoint,
    MetricSeries,
    NetNetRow,
    QuarterGrid,
    Statement,
    StatementLine,
)
from fundamentals_screener.dtos import CompanySummary as _CompanySummary
from fundamentals_screener.football import build_chart

pytestmark = pytest.mark.django_db


def _summary(**overrides):
    base = dict(
        ticker="CADCO", name="CADCO Corp", sector="Industrials", industry="Machinery",
        exchange="TSX", country="Canada", market="CA", reporting_currency="CAD", has_logo=False,
    )
    base.update(overrides)
    return _CompanySummary(**base)


def _statement():
    line = StatementLine(display_name="Revenue", section="Top", values=(400.0, 380.0))
    return Statement(name="Income Statement", years=(2024, 2023), lines=(line,),
                      period_ends=("2024-12-31", "2023-12-31"))


def _quarterly():
    line = StatementLine(display_name="Revenue", section=None, values=(100.0, 90.0))
    return QuarterGrid(name="Income Statement", columns=("Q4 2024", "Q3 2024"), lines=(line,),
                        period_ends=("2024-12-31", "2024-09-30"))


def _base_context() -> dict:
    summary = _summary()
    statements = CompanyStatements(statements=(_statement(),))
    quarterly = _quarterly()
    iv_field = FootballField(
        bars=(FootballBar(method="DCF", bear=10.0, mid=20.0, bull=30.0, fiscal_year=2024,
                           period_end="2024-12-31"),),
        price=25.0,
    )
    net_net = NetNetRow(
        ticker="CADCO", name="CADCO Corp", sector="Industrials", industry="Machinery",
        country="Canada", market="CA", price=25.0, market_cap=1000.0,
        ncav_per_share_relaxed=5.0, ncav_per_share_moderate=4.0, ncav_per_share_strict=3.0,
        f_score=7.0, z_score=3.5, z_score_zone="safe", has_logo=False, currency="cad",
    )
    statement_panes = [(_statement(), None, "CAD")]
    derived_metrics = (
        MetricSeries(ticker="CADCO", metric="Free Cash Flow", unit="cad", category="Cash Flow",
                     subcategory=None, sort_order=1.0, fiscal_years=(2024, 2023),
                     values=(120.0, 110.0), period_ends=("2024-12-31", "2023-12-31")),
    )
    valuation_metrics = (
        MetricPoint(ticker="CADCO", metric="P/S", unit="ratio", fiscal_year=2024, value=2.5),
    )
    headline = (
        HeadlineKpi(label="Revenue", value=400.0, fiscal_year=2024, currency="CAD"),
        HeadlineKpi(label="Market Cap", value=1000.0, fiscal_year=2024, currency="CAD"),
    )
    return {
        "detail": CompanyDetail(summary=summary, metrics=valuation_metrics),
        "statements": statements.statements,
        "statement_panes": statement_panes,
        "headline": headline,
        "price_chart": None,
        "price_tab_data": None,
        "price_windows": (),
        "price_window": "",
        "price_currency": "cad",
        "chart_currency": "CAD",
        "show_currency_selector": True,
        "target_currencies": ("CAD", "USD"),
        "selected_currency": "",
        "selected_scale": "auto",
        "value_scale": "auto",
        "display_currency": "CAD",
        "quarterly": quarterly,
        "quarterly_currency": "CAD",
        "quarterly_chart_data": None,
        "statement_chart_data": {},
        "bs_compositions": (),
        "bs_compositions_data": None,
        "derived_metrics": derived_metrics,
        "bench_ctx": None,
        "bench": "",
        "compare_query": "",
        "summary": summary,
        "compare_options": (),
        "valuation_metrics": valuation_metrics,
        "iv_chart": build_chart(iv_field),
        "iv_currency": "CAD",
        "mos_scenarios": (),
        "filings": (),
        "forecast_chart": None,
        "forecast_chart_data": None,
        "net_net": net_net,
        "net_net_levels": [
            {"label": "Relaxed", "ncav_per_share": 5.0, "ratio": 5.0, "discount_pct": -400.0, "bar_pct": 0.0},
        ],
    }


def test_renders_native_no_lens_without_error():
    html = render_to_string("fundamentals_screener/company_detail.html", _base_context())
    assert "CADCO Corp" in html
    assert "{%" not in html and "{#" not in html  # no leaked template syntax


def test_renders_with_currency_selected_and_shows_dropdown():
    ctx = _base_context()
    ctx.update(selected_currency="USD", display_currency="USD", quarterly_currency="USD", iv_currency="USD")
    ctx["statement_panes"] = [(_statement(), None, "USD")]
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert 'id="co-ccy"' in html
    assert 'value="USD" selected' in html
    assert "{%" not in html and "{#" not in html


def test_currency_selector_hidden_when_only_one_currency_available():
    ctx = _base_context()
    ctx.update(show_currency_selector=False, target_currencies=())
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert 'id="co-ccy"' not in html


def test_statement_pane_badges_own_currency_not_reporting_currency():
    # A statement that degraded back to native CAD while the page-wide display_currency is USD
    # -- the per-statement badge must show CAD, not USD (proves st_ccy, not display_currency,
    # drives the "Figures in ..." badge).
    ctx = _base_context()
    ctx.update(selected_currency="USD", display_currency="USD")
    ctx["statement_panes"] = [(_statement(), None, "CAD")]
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert "Figures in" in html


def test_derived_metrics_fragment_renders_without_error():
    from fundamentals_screener.dtos import BenchmarkContext

    ctx = {
        "derived_metrics": (
            MetricSeries(ticker="CADCO", metric="Free Cash Flow", unit="cad", category="Cash Flow",
                         subcategory=None, sort_order=1.0, fiscal_years=(2024,), values=(120.0,),
                         period_ends=("2024-12-31",)),
        ),
        "bench_ctx": BenchmarkContext(mode="industry", basis="industry", peer_count=3,
                                       industry_peer_count=3, sector_peer_count=8),
        "bench": "",
        "compare_query": "",
        "summary": _summary(),
        "selected_currency": "USD",
        "value_scale": "M",
    }
    html = render_to_string("fundamentals_screener/_derived_metrics.html", ctx)
    assert "ccy=USD" in html
    assert "scale=M" in html
    assert "{%" not in html and "{#" not in html


def test_masthead_scale_select_renders_with_a_visible_label():
    html = render_to_string("fundamentals_screener/company_detail.html", _base_context())
    assert '<label class="scr-ccy-select-label mb-0" for="co-scale">Scale</label>' in html
    assert 'id="co-scale"' in html
    assert '<option value="">Auto</option>' in html


def test_masthead_scale_select_always_renders_even_with_one_currency():
    # Scale never depends on FX data availability (unlike Currency) -- must render regardless.
    ctx = _base_context()
    ctx.update(show_currency_selector=False, target_currencies=())
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert 'id="co-ccy"' not in html
    assert 'id="co-scale"' in html


def test_default_scale_auto_compacts_a_large_kpi_value():
    ctx = _base_context()
    ctx["headline"] = (HeadlineKpi(label="Market Cap", value=1_234_567_890.0, fiscal_year=2024, currency="CAD"),)
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert "1.23B" in html
    assert "1,234,567,890.00" not in html


def test_forced_normal_scale_shows_the_full_raw_kpi_number():
    ctx = _base_context()
    ctx.update(selected_scale="normal", value_scale="normal")
    ctx["headline"] = (HeadlineKpi(label="Market Cap", value=1_234_567_890.0, fiscal_year=2024, currency="CAD"),)
    html = render_to_string("fundamentals_screener/company_detail.html", ctx)
    assert "1,234,567,890.00" in html
    assert "1.23B" not in html


def test_default_render_is_unchanged_from_before_the_scale_feature():
    # Company Detail's statement/KPI figures already auto-compacted before this feature existed
    # -- the default ("auto", nothing selected) must render byte-for-byte the same numbers:
    # 400.0 (Revenue) stays under the 1e3 auto-compaction threshold; 1000.0 (Market Cap) has
    # always crossed it and compacted to "1.0K", both before and after this feature.
    html = render_to_string("fundamentals_screener/company_detail.html", _base_context())
    assert "400.00" in html
    assert "1.0K" in html
