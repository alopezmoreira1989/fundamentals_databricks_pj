"""Tests for the Company Detail page's currency-lens converters (detail_currency.py) — pure
functions, no database. Each converter/collector pair is exercised directly against hand-built
DTOs and a hand-built `rates` dict (never a real resolve_rates call — that engine is already
covered by test_currency_lens.py, shared with the General Screener).

Deliberate scope, matching the module's own docstring: MetricPoint converts independently per
point; MetricSeries/Statement/QuarterGrid/FootballField convert ALL-OR-NOTHING (one missing
rate degrades the whole DTO back to native); relabel (the "usd"-mislabeling fix) is
unconditional, independent of any conversion.
"""

from __future__ import annotations

from datetime import date

from fundamentals_screener.dtos import (
    FootballBar,
    FootballField,
    MetricPoint,
    MetricSeries,
    NetNetRow,
    QuarterGrid,
    Statement,
    StatementLine,
)
from fundamentals_screener.repositories.fx_lens import RateKey

from fundamentals_screener import detail_currency

_CAD_2024 = RateKey("CAD", date(2024, 12, 31))
_CAD_2023 = RateKey("CAD", date(2023, 6, 30))
_RATES = {_CAD_2024: 0.78, _CAD_2023: 0.72}


# ── relabel ────────────────────────────────────────────────────────────────────────────────

def test_relabel_metric_points_fixes_mislabeled_usd():
    points = (MetricPoint(ticker="C", metric="Free Cash Flow", unit="usd", fiscal_year=2024, value=100.0),)
    out = detail_currency.relabel_metric_points(points, "CAD")
    assert out[0].unit == "cad"
    assert out[0].value == 100.0  # relabel never touches the number


def test_relabel_metric_points_excludes_market_cap():
    points = (MetricPoint(ticker="C", metric="Market Cap", unit="usd", fiscal_year=2024, value=100.0),)
    out = detail_currency.relabel_metric_points(points, "CAD")
    assert out[0].unit == "usd"  # already correct, genuine per-row currency -- left alone


def test_relabel_metric_points_leaves_usd_reporter_untouched():
    points = (MetricPoint(ticker="C", metric="Free Cash Flow", unit="usd", fiscal_year=2024, value=100.0),)
    out = detail_currency.relabel_metric_points(points, "USD")
    assert out[0].unit == "usd"


def test_relabel_metric_points_never_guesses_missing_reporting_currency():
    points = (MetricPoint(ticker="C", metric="Free Cash Flow", unit="usd", fiscal_year=2024, value=100.0),)
    out = detail_currency.relabel_metric_points(points, None)
    assert out[0].unit == "usd"


def test_relabel_metric_series_fixes_mislabeled_usd():
    series = (MetricSeries(ticker="C", metric="Free Cash Flow", unit="usd", category="x", subcategory=None,
                            sort_order=1.0, fiscal_years=(2024,), values=(100.0,)),)
    out = detail_currency.relabel_metric_series(series, "CAD")
    assert out[0].unit == "cad"


# ── MetricPoint: independent per-point conversion ─────────────────────────────────────────────

def test_metric_point_converts_when_rate_available():
    points = (MetricPoint(ticker="C", metric="P/S", unit="cad", fiscal_year=2024, value=100.0,
                           period_end="2024-12-31"),)
    keys = detail_currency.metric_point_keys(points, "USD")
    assert keys == frozenset({_CAD_2024})
    out = detail_currency.convert_metric_points(points, _RATES, "USD")
    assert out[0].value == 100.0 * 0.78
    assert out[0].unit == "usd"


def test_metric_point_stays_native_when_rate_missing():
    points = (MetricPoint(ticker="C", metric="P/S", unit="cad", fiscal_year=2024, value=100.0,
                           period_end="2019-01-01"),)  # no rate for this date
    out = detail_currency.convert_metric_points(points, _RATES, "USD")
    assert out[0].value == 100.0
    assert out[0].unit == "cad"


def test_metric_point_ratio_unit_never_touched():
    points = (MetricPoint(ticker="C", metric="P/E", unit="ratio", fiscal_year=2024, value=15.0,
                           period_end="2024-12-31"),)
    keys = detail_currency.metric_point_keys(points, "USD")
    assert keys == frozenset()
    out = detail_currency.convert_metric_points(points, _RATES, "USD")
    assert out[0].value == 15.0
    assert out[0].unit == "ratio"


def test_metric_point_already_target_currency_is_noop():
    points = (MetricPoint(ticker="C", metric="P/S", unit="usd", fiscal_year=2024, value=100.0,
                           period_end="2024-12-31"),)
    out = detail_currency.convert_metric_points(points, {}, "USD")
    assert out[0].value == 100.0
    assert out[0].unit == "usd"


def test_metric_point_missing_period_end_stays_native():
    points = (MetricPoint(ticker="C", metric="P/S", unit="cad", fiscal_year=2024, value=100.0, period_end=None),)
    keys = detail_currency.metric_point_keys(points, "USD")
    assert keys == frozenset()
    out = detail_currency.convert_metric_points(points, _RATES, "USD")
    assert out[0].value == 100.0


def test_metric_point_period_end_as_real_date_object_not_just_string():
    # Regression: CompanyRepository.market_cap()/latest_metrics() route through the generic
    # _fetch() row-mapper, which hands a raw datetime.date straight from DuckDB into this
    # str-typed field without stringifying it -- a real production 500 (2026-08-23, `?ccy=USD`
    # on a CAD-reporting ticker) from the old `_to_date` assuming `s[:10]` always works on a
    # string. period_end must be accepted as either shape.
    points = (MetricPoint(ticker="C", metric="P/S", unit="cad", fiscal_year=2024, value=100.0,
                           period_end=date(2024, 12, 31)),)
    keys = detail_currency.metric_point_keys(points, "USD")
    assert keys == frozenset({_CAD_2024})
    out = detail_currency.convert_metric_points(points, _RATES, "USD")
    assert out[0].value == 100.0 * 0.78


# ── MetricSeries: all-or-nothing per series ───────────────────────────────────────────────────

def test_metric_series_converts_every_value_when_all_rates_available():
    s = MetricSeries(ticker="C", metric="Free Cash Flow", unit="cad", category="x", subcategory=None,
                      sort_order=1.0, fiscal_years=(2024, 2023), values=(100.0, 50.0),
                      period_ends=("2024-12-31", "2023-06-30"))
    keys = detail_currency.metric_series_keys((s,), "USD")
    assert keys == frozenset({_CAD_2024, _CAD_2023})
    out = detail_currency.convert_metric_series((s,), _RATES, "USD")
    assert out[0].values == (100.0 * 0.78, 50.0 * 0.72)
    assert out[0].unit == "usd"


def test_metric_series_degrades_whole_series_when_one_rate_missing():
    s = MetricSeries(ticker="C", metric="Free Cash Flow", unit="cad", category="x", subcategory=None,
                      sort_order=1.0, fiscal_years=(2024, 2019), values=(100.0, 50.0),
                      period_ends=("2024-12-31", "2019-01-01"))
    out = detail_currency.convert_metric_series((s,), _RATES, "USD")
    assert out[0].values == (100.0, 50.0)  # unchanged
    assert out[0].unit == "cad"


def test_metric_series_null_values_dont_block_conversion():
    s = MetricSeries(ticker="C", metric="Free Cash Flow", unit="cad", category="x", subcategory=None,
                      sort_order=1.0, fiscal_years=(2024, 2023), values=(100.0, None),
                      period_ends=("2024-12-31", "2023-06-30"))
    out = detail_currency.convert_metric_series((s,), _RATES, "USD")
    assert out[0].values == (100.0 * 0.78, None)
    assert out[0].unit == "usd"


# ── Statement / QuarterGrid: all-or-nothing per grid ──────────────────────────────────────────

def _statement(period_ends, values_2024, values_2023):
    line = StatementLine(display_name="Revenue", section="Top", values=(values_2024, values_2023))
    return Statement(name="Income Statement", years=(2024, 2023), lines=(line,), period_ends=period_ends)


def test_statement_converts_every_column_when_all_rates_available():
    st = _statement(("2024-12-31", "2023-06-30"), 100.0, 50.0)
    keys = detail_currency.statement_keys(st, "CAD", "USD")
    assert keys == frozenset({_CAD_2024, _CAD_2023})
    out, ccy = detail_currency.convert_statement(st, "CAD", _RATES, "USD")
    assert out.lines[0].values == (100.0 * 0.78, 50.0 * 0.72)
    assert ccy == "USD"


def test_statement_degrades_to_native_when_one_column_rate_missing():
    st = _statement(("2024-12-31", "2019-01-01"), 100.0, 50.0)
    out, ccy = detail_currency.convert_statement(st, "CAD", _RATES, "USD")
    assert out is st  # untouched
    assert ccy == "CAD"


def test_statement_column_with_no_reported_values_doesnt_block_conversion():
    # A column with an unresolvable date but nothing reported in it shouldn't sink the whole
    # statement -- only columns that actually carry a value matter.
    line = StatementLine(display_name="Revenue", section="Top", values=(100.0, None))
    st = Statement(name="Income Statement", years=(2024, 2023), lines=(line,),
                   period_ends=("2024-12-31", "2019-01-01"))
    out, ccy = detail_currency.convert_statement(st, "CAD", _RATES, "USD")
    assert out.lines[0].values == (100.0 * 0.78, None)
    assert ccy == "USD"


def test_statement_native_equals_target_is_noop_no_keys_needed():
    st = _statement(("2024-12-31", "2023-06-30"), 100.0, 50.0)
    assert detail_currency.statement_keys(st, "USD", "USD") == frozenset()
    out, ccy = detail_currency.convert_statement(st, "USD", {}, "USD")
    assert out is st
    assert ccy == "USD"


def test_quarter_grid_all_or_nothing_mirrors_statement():
    line = StatementLine(display_name="Revenue", section=None, values=(100.0, 50.0))
    grid = QuarterGrid(name="Income Statement", columns=("Q2 2025", "Q1 2025"), lines=(line,),
                        period_ends=("2024-12-31", "2023-06-30"))
    out, ccy = detail_currency.convert_grid(grid, "CAD", _RATES, "USD")
    assert out.lines[0].values == (100.0 * 0.78, 50.0 * 0.72)
    assert ccy == "USD"


# ── FootballField: all-or-nothing across every bar ────────────────────────────────────────────

def test_football_converts_all_bars_together():
    bars = (
        FootballBar(method="DCF", bear=10.0, mid=20.0, bull=30.0, fiscal_year=2024, period_end="2024-12-31"),
        FootballBar(method="Graham", bear=15.0, mid=25.0, bull=35.0, fiscal_year=2024, period_end="2024-12-31"),
    )
    field = FootballField(bars=bars, price=100.0)
    keys = detail_currency.football_keys(field, "CAD", "USD")
    assert keys == frozenset({_CAD_2024})
    out, ccy = detail_currency.convert_football(field, "CAD", _RATES, "USD")
    assert out.bars[0].bear == 10.0 * 0.78
    assert out.bars[1].bull == 35.0 * 0.78
    assert out.price == 100.0  # price line deliberately untouched -- see module docstring
    assert ccy == "USD"


def test_football_any_bar_failing_reverts_whole_field_to_native():
    bars = (
        FootballBar(method="DCF", bear=10.0, mid=20.0, bull=30.0, fiscal_year=2024, period_end="2024-12-31"),
        FootballBar(method="Graham", bear=15.0, mid=25.0, bull=35.0, fiscal_year=2019, period_end="2019-01-01"),
    )
    field = FootballField(bars=bars, price=100.0)
    out, ccy = detail_currency.convert_football(field, "CAD", _RATES, "USD")
    assert out is field
    assert ccy == "CAD"


def test_football_no_bars_is_native_noop():
    field = FootballField(bars=(), price=None)
    out, ccy = detail_currency.convert_football(field, "CAD", _RATES, "USD")
    assert out is field
    assert ccy == "CAD"


# ── NetNetRow: one shared "live snapshot" date ────────────────────────────────────────────────

def _net_net_row(**overrides):
    base = dict(
        ticker="C", name="C Corp", sector=None, industry=None, country=None, market=None,
        price=10.0, market_cap=1000.0, ncav_per_share_relaxed=5.0, ncav_per_share_moderate=4.0,
        ncav_per_share_strict=3.0, f_score=7.0, z_score=3.5, z_score_zone="safe", currency="cad",
    )
    base.update(overrides)
    return NetNetRow(**base)


def test_net_net_converts_every_field_together():
    row = _net_net_row()
    keys = detail_currency.net_net_keys(row, "CAD", "USD", "2024-12-31")
    assert keys == frozenset({_CAD_2024})
    out, ccy = detail_currency.convert_net_net(row, "CAD", _RATES, "USD", "2024-12-31")
    assert out.price == 10.0 * 0.78
    assert out.market_cap == 1000.0 * 0.78
    assert out.ncav_per_share_relaxed == 5.0 * 0.78
    assert out.currency == "usd"
    assert ccy == "USD"


def test_net_net_stays_native_when_as_of_unresolvable():
    row = _net_net_row()
    out, ccy = detail_currency.convert_net_net(row, "CAD", _RATES, "USD", None)
    assert out is row
    assert ccy == "CAD"


def test_net_net_none_row_passes_through():
    out, ccy = detail_currency.convert_net_net(None, "CAD", _RATES, "USD", "2024-12-31")
    assert out is None
    assert ccy == "CAD"
