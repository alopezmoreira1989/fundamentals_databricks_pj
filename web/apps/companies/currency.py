"""Quote-currency inference from a ticker's listing market — presentation-only.

Mirrors the Streamlit app's ``lib/currency.py::quote_currency()``. No FX/unit conversion
happens here (see CLAUDE.md's currency-alignment convention) — this only labels a raw price
with the currency it's already denominated in.
"""

from __future__ import annotations

# Quote currency by listing market — mirrors 22__derived_metrics.py's QUOTE_CURRENCY_BY_MARKET.
QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}


def quote_currency(market: str | None) -> str:
    """Currency a ticker's PRICE trades in, from its listing ``market`` ("US"/"CA")."""
    return QUOTE_CURRENCY_BY_MARKET.get((market or "US").upper(), "USD")
