"""Quote-currency inference from a ticker's listing market — presentation-only.

No FX/unit conversion happens here — this only labels a raw price with the currency it's
already denominated in.
"""

from __future__ import annotations

# Quote currency by listing market.
QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}


def quote_currency(market: str | None) -> str:
    """Currency a ticker's PRICE trades in, from its listing ``market`` ("US"/"CA")."""
    return QUOTE_CURRENCY_BY_MARKET.get((market or "US").upper(), "USD")
