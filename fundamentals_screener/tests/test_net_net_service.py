"""Tests for services.get_net_net_screen — sorting, hero stats, and the "hide value traps"
filter. Exercises only the pure post-repository logic directly (no DuckDB, no Django
settings): _price_to_ncav_ratio is a plain function over NetNetRow instances, and
get_net_net_screen's sort/stats/filter logic is tested via a monkeypatched
CompanyListingRepository so no real repository call happens.
"""

from __future__ import annotations

import pytest
from fundamentals_screener.dtos import NetNetRow
from fundamentals_screener.services import _price_to_ncav_ratio, get_net_net_screen

from fundamentals_screener import services as services_module


def _row(ticker: str, *, price=10.0, relaxed=None, moderate=None, strict=None, f_score=None) -> NetNetRow:
    return NetNetRow(
        ticker=ticker, name=f"{ticker} Corp", sector="Industrials", industry="Machinery",
        country="United States", market="US", price=price, market_cap=None,
        ncav_per_share_relaxed=relaxed, ncav_per_share_moderate=moderate,
        ncav_per_share_strict=strict, f_score=f_score, z_score=None, z_score_zone=None,
    )


def test_price_to_ncav_ratio_picks_the_right_level_field():
    row = _row("A", price=5.0, relaxed=10.0, moderate=8.0, strict=6.0)
    assert _price_to_ncav_ratio(row, "relaxed") == 0.5
    assert _price_to_ncav_ratio(row, "moderate") == 0.625
    assert _price_to_ncav_ratio(row, "strict") == pytest.approx(5.0 / 6.0)


@pytest.mark.parametrize("kwargs", [
    {"price": None, "relaxed": 10.0},
    {"price": 5.0, "relaxed": None},
    {"price": 5.0, "relaxed": 0.0},
    {"price": 5.0, "relaxed": -3.0},
])
def test_price_to_ncav_ratio_none_when_either_side_unusable(kwargs):
    row = _row("A", **kwargs)
    assert _price_to_ncav_ratio(row, "relaxed") is None


class _FakeRepo:
    """Stand-in for CompanyListingRepository — records the exact kwargs it was called with and
    returns a fixed row set, so get_net_net_screen's own sort/filter/stats logic is exercised
    without any DuckDB/meta dependency."""

    def __init__(self, rows, universe_size):
        self._rows = rows
        self._universe_size = universe_size
        self.screen_calls = []
        self.scope_calls = []

    def scope_size(self, **kwargs):
        self.scope_calls.append(kwargs)
        return self._universe_size

    def net_net_screen(self, **kwargs):
        self.screen_calls.append(kwargs)
        return self._rows


@pytest.fixture
def patch_repo(monkeypatch):
    def _patch(rows, universe_size=100):
        fake = _FakeRepo(rows, universe_size)
        monkeypatch.setattr(services_module, "CompanyListingRepository", lambda: fake)
        return fake

    return _patch


def test_sorts_cheapest_first_descending_discount(patch_repo):
    rows = (
        _row("EXPENSIVE", price=9.0, relaxed=10.0),   # ratio 0.90
        _row("CHEAP", price=3.0, relaxed=10.0),        # ratio 0.30
        _row("MID", price=6.0, relaxed=10.0),          # ratio 0.60
    )
    patch_repo(rows)
    result = get_net_net_screen(level="relaxed")
    assert [r.ticker for r in result.rows] == ["CHEAP", "MID", "EXPENSIVE"]


def test_rows_with_no_valid_ratio_sort_last(patch_repo):
    rows = (
        _row("NORATIO", price=5.0, relaxed=None),
        _row("HASRATIO", price=5.0, relaxed=10.0),
    )
    patch_repo(rows)
    result = get_net_net_screen(level="relaxed")
    assert [r.ticker for r in result.rows] == ["HASRATIO", "NORATIO"]


def test_hero_stats_below_value_and_classic_net_net_thresholds(patch_repo):
    rows = (
        _row("ABOVE", price=12.0, relaxed=10.0),    # ratio 1.20 — not below value
        _row("BELOW", price=9.0, relaxed=10.0),     # ratio 0.90 — below value, not classic
        _row("CLASSIC", price=6.0, relaxed=10.0),   # ratio 0.60 — below value AND classic (<=0.67)
        _row("EXACT67", price=6.7, relaxed=10.0),   # ratio 0.67 — exactly the classic boundary
    )
    patch_repo(rows, universe_size=250)
    result = get_net_net_screen(level="relaxed")
    assert result.stats.universe_size == 250
    assert result.stats.eligible_count == 4
    assert result.stats.below_value_count == 3          # BELOW, CLASSIC, EXACT67
    assert result.stats.classic_net_net_count == 2       # CLASSIC, EXACT67


def test_hide_value_traps_excludes_low_f_score_but_keeps_unknown(patch_repo):
    rows = (
        _row("WEAK", price=5.0, relaxed=10.0, f_score=2.0),
        _row("STRONG", price=5.0, relaxed=10.0, f_score=7.0),
        _row("UNKNOWN", price=5.0, relaxed=10.0, f_score=None),
    )
    patch_repo(rows)
    result = get_net_net_screen(level="relaxed", hide_value_traps=True)
    tickers = {r.ticker for r in result.rows}
    assert tickers == {"STRONG", "UNKNOWN"}
    assert result.stats.eligible_count == 2


def test_hero_stats_reflect_hide_value_traps_not_the_unfiltered_set(patch_repo):
    rows = (
        _row("WEAK", price=5.0, relaxed=10.0, f_score=1.0),   # would count as below-value if kept
        _row("STRONG", price=5.0, relaxed=10.0, f_score=8.0),
    )
    patch_repo(rows)
    result = get_net_net_screen(level="relaxed", hide_value_traps=True)
    assert result.stats.eligible_count == 1
    assert result.stats.below_value_count == 1


def test_unrecognized_level_falls_back_to_relaxed(patch_repo):
    fake = patch_repo((_row("A", price=5.0, relaxed=10.0),))
    get_net_net_screen(level="not-a-real-level")
    assert fake.screen_calls[0]["level"] == "relaxed"


def test_descriptive_filters_pass_through_to_both_repo_calls(patch_repo):
    fake = patch_repo((_row("A", price=5.0, relaxed=10.0),))
    get_net_net_screen(level="relaxed", sector="Industrials", country="United States", market="US")
    assert fake.scope_calls[0] == {"sector": "Industrials", "country": "United States", "market": "US"}
    assert fake.screen_calls[0]["sector"] == "Industrials"
    assert fake.screen_calls[0]["country"] == "United States"
    assert fake.screen_calls[0]["market"] == "US"
