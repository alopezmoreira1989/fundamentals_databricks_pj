"""Tests for the General Screener's multi-currency lens: fx_lens.resolve_rates (the shared,
batched, date-anchored, triangulating FX-rate primitive) and CompanyListingRepository's
available_target_currencies / _relabel_native_currency / _apply_currency_lens / screen_table
wiring on top of it.

Self-contained: an in-memory DuckDB connection with hand-written dashboard_metrics/dashboard_fx
tables, injected into CompanyListingRepository(connection=...), plus a monkeypatched
repositories.company_listing.load_meta -- same pattern as test_investor_presets.py.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest
from fundamentals_screener.repositories import company_listing as company_listing_module
from fundamentals_screener.repositories.company_listing import CompanyListingRepository
from fundamentals_screener.repositories.fx_lens import RateKey, resolve_rates

_META = {
    "tickers": [
        {"ticker": "USDCO", "company": "USDCO Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US",
         "reporting_currency": "USD"},
        {"ticker": "CADCO", "company": "CADCO Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "Canada", "market": "CA",
         "reporting_currency": "CAD"},
        {"ticker": "NOCCY", "company": "NOCCY Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "Canada", "market": "CA",
         "reporting_currency": None},
    ]
}

# ticker, metric, unit, fiscal_year, value, period_type, period_end
_METRIC_ROWS = [
    # Market Cap: genuinely per-row native currency, already correct -- CADCO's is "cad".
    ("USDCO", "Market Cap", "usd", 2024, 1000.0, "FY", "2024-12-31"),
    ("CADCO", "Market Cap", "cad", 2024, 500.0, "FY", "2024-12-31"),
    # Free Cash Flow: mislabeled "usd" for every ticker (the real pre-existing bug), even
    # though CADCO's is really in CAD -- same period_end as Market Cap here.
    ("USDCO", "Free Cash Flow", "usd", 2024, 200.0, "FY", "2024-12-31"),
    ("CADCO", "Free Cash Flow", "usd", 2024, 100.0, "FY", "2024-12-31"),
    ("NOCCY", "Free Cash Flow", "usd", 2024, 50.0, "FY", "2024-12-31"),
    # A second currency-denominated metric with a DIFFERENT period_end than Market Cap/FCF for
    # CADCO, to prove per-cell (not per-ticker) date-anchoring.
    ("CADCO", "EV", "usd", 2023, 900.0, "FY", "2023-06-30"),
    # A ratio metric -- must never be touched by relabel or conversion.
    ("CADCO", "P/E", "ratio", 2024, 15.0, "FY", "2024-12-31"),
]

# base, quote, date, rate -- CAD/USD only, with a rate CHANGE between 2023-06-30 and
# 2024-12-31 so date-anchoring can be proven, not just assumed.
_FX_ROWS = [
    ("CAD", "USD", "2023-01-01", 0.70),
    ("CAD", "USD", "2023-06-30", 0.72),
    ("CAD", "USD", "2024-01-01", 0.75),
    ("CAD", "USD", "2024-12-31", 0.78),
    ("USD", "CAD", "2024-12-31", 1.0 / 0.78),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " period_type VARCHAR, period_end DATE)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?,?)", _METRIC_ROWS)
    conn.execute(
        "CREATE TABLE dashboard_fx (base VARCHAR, quote VARCHAR, date DATE, rate DOUBLE)"
    )
    conn.executemany("INSERT INTO dashboard_fx VALUES (?,?,?,?)", _FX_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    return CompanyListingRepository(connection=con)


# ── resolve_rates ──────────────────────────────────────────────────────────────────────────

def test_resolve_rates_direct_pair(con):
    rates = resolve_rates(con, frozenset({RateKey("CAD", date(2024, 12, 31))}), "USD")
    assert rates == {RateKey("CAD", date(2024, 12, 31)): pytest.approx(0.78)}


def test_resolve_rates_triangulates_via_usd_when_no_direct_pair(con):
    # No direct EUR->CAD pair exists -- only EUR->USD and USD->CAD (planted just for this test).
    con.execute("INSERT INTO dashboard_fx VALUES ('EUR', 'USD', '2024-12-31', 1.10)")
    rates = resolve_rates(con, frozenset({RateKey("EUR", date(2024, 12, 31))}), "CAD")
    expected = 1.10 * (1.0 / 0.78)
    assert rates[RateKey("EUR", date(2024, 12, 31))] == pytest.approx(expected)


def test_resolve_rates_prefers_direct_pair_over_triangulation(con):
    # Plant a direct CAD->USD-via-something-else scenario: a direct CAD->EUR pair with a
    # DIFFERENT value than what triangulation via USD would produce.
    con.execute("INSERT INTO dashboard_fx VALUES ('USD', 'EUR', '2024-12-31', 0.90)")
    con.execute("INSERT INTO dashboard_fx VALUES ('CAD', 'EUR', '2024-12-31', 0.55)")
    rates = resolve_rates(con, frozenset({RateKey("CAD", date(2024, 12, 31))}), "EUR")
    assert rates[RateKey("CAD", date(2024, 12, 31))] == pytest.approx(0.55)  # direct, not 0.78*0.90


def test_resolve_rates_omits_key_when_nothing_resolvable(con):
    rates = resolve_rates(con, frozenset({RateKey("JPY", date(2024, 12, 31))}), "USD")
    assert rates == {}


def test_resolve_rates_date_anchors_independently_per_key(con):
    keys = frozenset({RateKey("CAD", date(2023, 6, 30)), RateKey("CAD", date(2024, 12, 31))})
    rates = resolve_rates(con, keys, "USD")
    assert rates[RateKey("CAD", date(2023, 6, 30))] == pytest.approx(0.72)
    assert rates[RateKey("CAD", date(2024, 12, 31))] == pytest.approx(0.78)


def test_resolve_rates_usd_to_usd_is_trivial_noop_leg(con):
    rates = resolve_rates(con, frozenset({RateKey("USD", date(2024, 12, 31))}), "USD")
    assert rates[RateKey("USD", date(2024, 12, 31))] == pytest.approx(1.0)


def test_resolve_rates_empty_keys_short_circuits(con):
    assert resolve_rates(con, frozenset(), "USD") == {}


# ── available_target_currencies ───────────────────────────────────────────────────────────

def test_available_target_currencies_grows_with_new_fx_data(repo, con):
    assert repo.available_target_currencies() == ("CAD", "USD")
    con.execute("INSERT INTO dashboard_fx VALUES ('EUR', 'USD', '2024-12-31', 1.10)")
    con.execute("INSERT INTO dashboard_fx VALUES ('USD', 'EUR', '2024-12-31', 0.91)")
    assert repo.available_target_currencies() == ("CAD", "EUR", "USD")


def test_available_target_currencies_degrades_when_fx_view_missing(monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    empty_con = duckdb.connect(":memory:")
    empty_con.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " period_type VARCHAR, period_end DATE)"
    )
    repo = CompanyListingRepository(connection=empty_con)
    assert repo.available_target_currencies() == ()


# ── _relabel_native_currency (via screen_table, target_currency=None) ────────────────────────

def test_default_call_relabels_but_does_not_convert(repo):
    result = repo.screen_table(columns=["Market Cap", "Free Cash Flow"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "CADCO")
    # Value unconverted (still the raw native number)...
    assert row.values["Free Cash Flow"] == pytest.approx(100.0)
    # ...but the unit is corrected from the mislabeled "usd" to the real native "cad".
    assert row.units["Free Cash Flow"] == "cad"


def test_relabel_leaves_usd_reporter_untouched(repo):
    result = repo.screen_table(columns=["Free Cash Flow"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "USDCO")
    assert row.values["Free Cash Flow"] == pytest.approx(200.0)
    assert row.units["Free Cash Flow"] == "usd"


def test_relabel_leaves_market_cap_alone(repo):
    result = repo.screen_table(columns=["Market Cap"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "CADCO")
    assert row.units["Market Cap"] == "cad"  # already correct pre-feature, untouched


def test_relabel_never_guesses_missing_reporting_currency(repo):
    result = repo.screen_table(columns=["Free Cash Flow"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "NOCCY")
    assert row.units["Free Cash Flow"] == "usd"  # left exactly as-is, no reporting_currency known


def test_ratio_metric_is_never_relabeled_or_touched(repo):
    result = repo.screen_table(columns=["P/E"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "CADCO")
    assert row.units["P/E"] == "ratio"
    assert row.values["P/E"] == pytest.approx(15.0)


# ── _apply_currency_lens (via screen_table, target_currency set) ─────────────────────────────

def test_converts_every_currency_column_not_just_market_cap(repo):
    result = repo.screen_table(
        columns=["Market Cap", "Free Cash Flow", "P/E"], target_currency="USD", page_size=10,
    )
    row = next(r for r in result.rows if r.ticker == "CADCO")
    assert row.values["Market Cap"] == pytest.approx(500.0 * 0.78)
    assert row.units["Market Cap"] == "usd"
    assert row.values["Free Cash Flow"] == pytest.approx(100.0 * 0.78)
    assert row.units["Free Cash Flow"] == "usd"
    # Ratio column untouched.
    assert row.values["P/E"] == pytest.approx(15.0)
    assert row.units["P/E"] == "ratio"


def test_uses_each_cells_own_period_end_not_a_shared_one(repo):
    result = repo.screen_table(
        columns=["Free Cash Flow", "EV"], target_currency="USD", page_size=10,
    )
    row = next(r for r in result.rows if r.ticker == "CADCO")
    # FCF is dated 2024-12-31 (rate 0.78); EV is dated 2023-06-30 (rate 0.72) -- different
    # dates must resolve to different rates, not one shared per-ticker date.
    assert row.values["Free Cash Flow"] == pytest.approx(100.0 * 0.78)
    assert row.values["EV"] == pytest.approx(900.0 * 0.72)


def test_usd_reporter_is_a_pure_noop_under_usd_target(repo):
    result = repo.screen_table(
        columns=["Market Cap", "Free Cash Flow"], target_currency="USD", page_size=10,
    )
    row = next(r for r in result.rows if r.ticker == "USDCO")
    assert row.values["Market Cap"] == pytest.approx(1000.0)
    assert row.values["Free Cash Flow"] == pytest.approx(200.0)


def test_missing_reporting_currency_ticker_stays_native_under_conversion(repo):
    result = repo.screen_table(columns=["Free Cash Flow"], target_currency="USD", page_size=10)
    row = next(r for r in result.rows if r.ticker == "NOCCY")
    # No known reporting_currency -> relabel never fired -> unit stays "usd", so the lens
    # treats it as already-USD (native==target) and leaves the value untouched too.
    assert row.units["Free Cash Flow"] == "usd"
    assert row.values["Free Cash Flow"] == pytest.approx(50.0)


def test_degrades_gracefully_when_fx_view_missing(monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " period_type VARCHAR, period_end DATE)"
    )
    con.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?,?)", _METRIC_ROWS)
    # No dashboard_fx table registered at all.
    repo = CompanyListingRepository(connection=con)
    result = repo.screen_table(
        columns=["Market Cap", "Free Cash Flow"], target_currency="USD", page_size=10,
    )
    row = next(r for r in result.rows if r.ticker == "CADCO")
    # Relabel (pure Python, no dashboard_fx dependency) still applies...
    assert row.units["Free Cash Flow"] == "cad"
    # ...but no actual conversion happens: values stay native.
    assert row.values["Free Cash Flow"] == pytest.approx(100.0)
    assert row.values["Market Cap"] == pytest.approx(500.0)


def test_target_currency_none_never_calls_resolve_rates(repo, con):
    # A sanity check that the default (None) path is a true no-conversion no-op: drop the
    # dashboard_fx table entirely and confirm the call still succeeds (would raise if
    # _apply_currency_lens were invoked when it shouldn't be).
    con.execute("DROP TABLE dashboard_fx")
    result = repo.screen_table(columns=["Market Cap", "Free Cash Flow"], page_size=10)
    row = next(r for r in result.rows if r.ticker == "CADCO")
    assert row.values["Market Cap"] == pytest.approx(500.0)
    assert row.units["Free Cash Flow"] == "cad"  # relabel is independent of dashboard_fx
