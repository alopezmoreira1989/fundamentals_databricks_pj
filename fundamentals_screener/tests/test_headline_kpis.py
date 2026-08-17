"""Tests for services.headline_kpis's currency stamping — Phase 5.7a
(docs/phase5-7-fundamentals-screener-multi-market-audit.md).

Pure DTO-in/DTO-out, no repository/DuckDB/Django dependency.
"""

from __future__ import annotations

from fundamentals_screener.dtos import CompanyStatements, Statement, StatementLine
from fundamentals_screener.services import headline_kpis

_STATEMENTS = CompanyStatements(statements=(
    Statement(
        name="Income Statement",
        years=(2024, 2023),
        lines=(
            StatementLine(display_name="Revenue", section="Revenue", values=(400.0, 380.0)),
            StatementLine(display_name="Net Income", section="Net Income", values=(90.0, 85.0)),
        ),
    ),
    Statement(
        name="Balance Sheet",
        years=(2024, 2023),
        lines=(
            StatementLine(display_name="Total Assets", section="Assets", values=(1_000.0, 950.0)),
        ),
    ),
    Statement(
        name="Cash Flow",
        years=(2024, 2023),
        lines=(
            StatementLine(display_name="Operating CF", section="Operating", values=(120.0, 110.0)),
        ),
    ),
))


def test_headline_kpis_stamps_the_given_currency_on_every_row():
    kpis = headline_kpis(_STATEMENTS, currency="EUR")
    assert kpis  # sanity: the fixture actually produced rows
    assert all(k.currency == "EUR" for k in kpis)


def test_headline_kpis_defaults_to_none_not_a_guessed_usd():
    # None here still degrades to compact_money_ccy's own USD default at render time — but the
    # DTO itself must not fabricate a currency it was never told.
    kpis = headline_kpis(_STATEMENTS)
    assert all(k.currency is None for k in kpis)


def test_headline_kpis_usd_ticker_gets_usd_stamped_explicitly():
    kpis = headline_kpis(_STATEMENTS, currency="USD")
    assert all(k.currency == "USD" for k in kpis)


def test_headline_kpis_values_unaffected_by_currency():
    eur = {k.label: k.value for k in headline_kpis(_STATEMENTS, currency="EUR")}
    usd = {k.label: k.value for k in headline_kpis(_STATEMENTS, currency="USD")}
    # Phase 5.7a only labels the currency — it must never convert/rescale a value.
    assert eur == usd
    assert eur["Revenue"] == 400.0
