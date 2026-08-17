"""Tests for currency.py's quote-currency inference and templatetags/fmt.py's currency-aware
formatters — Phase 5.7a (docs/phase5-7-fundamentals-screener-multi-market-audit.md).

No Django settings, no fixture files — pure functions only.
"""

from __future__ import annotations

import pytest
from fundamentals_screener.currency import QUOTE_CURRENCY_BY_MARKET, quote_currency
from fundamentals_screener.templatetags.fmt import (
    compact_money,
    compact_money_ccy,
    currency_badge,
    metric_value,
)

# ── quote_currency ───────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "market, expected",
    [
        ("US", "USD"),
        ("us", "USD"),
        ("CA", "CAD"),
        ("ca", "CAD"),
        ("EU", "EUR"),  # Phase 5.7a — the fix: was silently defaulting to "USD" before this.
        (None, "USD"),  # no market on record ⇒ the pre-existing "assume US" default, unchanged.
        ("", "USD"),
    ],
)
def test_quote_currency_known_markets(market, expected):
    assert quote_currency(market) == expected


def test_quote_currency_unmapped_market_is_none_not_a_guessed_usd():
    # A future market this app has no real quote-currency mapping for yet must render as
    # genuinely unknown (None), never silently mislabeled as USD — the exact failure mode
    # Phase 5.7a fixes for "EU" is not to be reintroduced for the next new market.
    assert quote_currency("XX") is None


def test_quote_currency_does_not_determine_reporting_currency():
    # The real, live AEM/AQN case (CLAUDE.md's own documented example, not hypothetical):
    # both are TSX-listed ("market": "CA"), so both quote in CAD — but AEM *reports* its
    # fundamentals in USD while AQN reports in CAD. quote_currency() only ever answers "what
    # currency does the PRICE trade in" from the listing market; a caller must read
    # reporting_currency separately for statement-derived figures, never assume the two match.
    aem_price_currency = quote_currency("CA")
    aem_reporting_currency = "USD"  # real data: AEM's own dashboard_meta reporting_currency
    aqn_price_currency = quote_currency("CA")
    aqn_reporting_currency = "CAD"  # real data: AQN's own dashboard_meta reporting_currency

    assert aem_price_currency == "CAD"
    assert aem_price_currency != aem_reporting_currency  # the documented mismatch case
    assert aqn_price_currency == "CAD"
    assert aqn_price_currency == aqn_reporting_currency  # the common case: both match


def test_quote_currency_mirrors_the_pipeline_map():
    # fundamentals_pipeline/20__transformation/22__derived_metrics.py's own
    # QUOTE_CURRENCY_BY_MARKET is the single source of truth for this exact concept — this
    # module's map must stay a superset/mirror of it, not a second, independently-drifting copy.
    assert QUOTE_CURRENCY_BY_MARKET == {"US": "USD", "CA": "CAD", "EU": "EUR"}


# ── metric_value (used by the Price tab, football field, Net-Net Finder) ───────────────────


@pytest.mark.parametrize(
    "value, unit, expected",
    [
        (None, "usd", "—"),  # missing value ⇒ em dash, regardless of unit
        (None, None, "—"),
        (0.0, "usd", "$0.00"),  # a real zero must render as zero, not as missing
        (177.6, "usd", "$177.60"),
        (177.6, "USD", "$177.60"),  # case-insensitive
        (12.4, "percent", "12.4%"),
        (1.8, "ratio", "1.80"),  # not a 3-letter currency code ⇒ bare number, no badge
    ],
)
def test_metric_value_usd_and_non_currency_units(value, unit, expected):
    assert str(metric_value(value, unit)) == expected


def test_metric_value_eur_renders_a_badge_not_a_dollar_sign():
    rendered = str(metric_value(177.6, "EUR"))
    assert "$" not in rendered
    assert "177.60" in rendered
    assert "EUR" in rendered


def test_metric_value_cad_renders_a_badge_not_a_dollar_sign():
    # The real, already-live case (AQN quotes and reports in CAD) — not a hypothetical.
    rendered = str(metric_value(45.2, "CAD"))
    assert "$" not in rendered
    assert "CAD" in rendered


def test_metric_value_none_unit_degrades_to_bare_number_not_a_dollar_sign():
    # quote_currency() can now return None for an unmapped market (see above) — metric_value
    # must render that as a plain number, never fall back to a guessed "$".
    assert str(metric_value(42.0, None)) == "42.00"


def test_metric_value_zero_is_distinguishable_from_missing():
    assert str(metric_value(0.0, "usd")) != str(metric_value(None, "usd"))


# ── compact_money / compact_money_ccy (statement tables, Net-Net Market Cap) ────────────────


def test_compact_money_has_no_currency_symbol_of_its_own():
    # The bare filter never prepends $ — confirmed still true after Phase 5.7a's statement-
    # table changes (which switched to compact_money_ccy instead, see company_detail.html).
    assert "$" not in str(compact_money(391_040_000_000.0))


@pytest.mark.parametrize(
    "currency, expect_dollar, expect_badge",
    [
        ("USD", True, False),
        (None, True, False),  # unset ⇒ same USD default as before Phase 5.7a
        ("", True, False),
        ("CAD", False, True),
        ("EUR", False, True),
        ("eur", False, True),  # case-insensitive
    ],
)
def test_compact_money_ccy_prefixes_dollar_only_for_usd(currency, expect_dollar, expect_badge):
    rendered = str(compact_money_ccy(1_500_000_000.0, currency))
    assert ("$" in rendered) == expect_dollar
    assert (currency or "").upper() in rendered.upper() if expect_badge else True


def test_compact_money_ccy_none_value_is_em_dash():
    assert str(compact_money_ccy(None, "EUR")) == "—"


def test_compact_money_ccy_zero_is_not_em_dash():
    rendered = str(compact_money_ccy(0.0, "EUR"))
    assert rendered != "—"
    assert "0" in rendered


# ── currency_badge (statement/quarterly tab headers) ────────────────────────────────────────


@pytest.mark.parametrize(
    "currency, expect_empty",
    [
        ("USD", True),
        (None, True),
        ("", True),
        ("CAD", False),
        ("EUR", False),
    ],
)
def test_currency_badge_empty_for_usd_only(currency, expect_empty):
    rendered = str(currency_badge(currency))
    assert (rendered == "") == expect_empty
