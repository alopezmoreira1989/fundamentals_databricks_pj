"""Quote-currency inference from a ticker's listing market — presentation-only.

No FX/unit conversion happens here — this only labels a raw price with the currency it's
already denominated in. ``dashboard_prices`` carries no currency column of its own (see
``fundamentals_pipeline/artifacts.py``'s ``_PRICES_SPEC``) — market-keyed inference is the only
signal available for a raw quote, same as upstream.

Mirrors ``QUOTE_CURRENCY_BY_MARKET`` in ``fundamentals_pipeline/20__transformation/
22__derived_metrics.py`` / ``23__intrinsic_value.py`` — the pipeline's own single source of
truth for this exact concept (Phase 5.6 already added the ``"EU": "EUR"`` entry there for the
currency-alignment fix). Keep this map in sync with that one rather than inventing a second,
divergent definition.
"""

from __future__ import annotations

# Quote currency by listing market — kept in sync with the pipeline's own
# QUOTE_CURRENCY_BY_MARKET (see module docstring).
QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD", "EU": "EUR"}


def quote_currency(market: str | None) -> str | None:
    """Currency a ticker's PRICE trades in, from its listing ``market`` ("US"/"CA"/"EU").

    ``None`` for a market this app doesn't have a real quote-currency mapping for — never
    silently guessed as USD (a real gap should render as unlabeled, not mislabeled)."""
    return QUOTE_CURRENCY_BY_MARKET.get((market or "US").upper())
