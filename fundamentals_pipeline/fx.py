"""Pure FX conversion helper — currency alignment for market-cap-derived figures.

**The bug this exists to fix:** some filers report fundamentals in a different currency
than their primary listing quotes in (e.g. a Canadian gold miner reporting in USD while its
TSX listing quotes in CAD). `market_cap = price × shares` is only arithmetically correct when
both operands share a currency — a same-ticker unit mismatch, independent of any cross-market
comparability question. This is NOT about converting everything to one display currency (the
repo owner's explicit decision is native-currency storage, no dual columns, no blanket USD
conversion) — it's a correctness fix: when a ticker's quote currency and its
`reporting_currency` differ, the price must be converted into the reporting currency *before*
it's combined with fundamentals, so the stored `market_cap` is in the same currency as
`net_income`, `equity`, etc.

**Non-negotiable date-anchoring rule:** every conversion uses the FX rate dated at the
figure's own observation date — a fiscal `period_end` for FY figures, a price's own trade
date for TTM/live figures — never the SEC `filed` timestamp and never "today's" or
"run-time" spot rate. This is the currency-domain analogue of the bug already fixed once for
prices (see CLAUDE.md: `market_data` mispriced non-December filers by pairing a fiscal figure
with a calendar-year-end price; `market_cap_asof` prices each FY at its own real fiscal close
instead). Converting a 2022 figure with today's FX rate would reintroduce that exact class of
bug in a new dimension.

`convert_price()` deliberately takes no date parameter — date selection (which FX rate to
look up) is entirely the CALLER's responsibility (the Spark as-of join in
`22__derived_metrics.py` / `23__intrinsic_value.py`, keyed on `period_end`), so it can never
silently drift inside this pure function. See `tests/test_fx.py` for the contract test that
locks this down.

Import-safe: no Spark/`dbutils`/network dependency, unit-tested like `valuation.py`/
`periods.py`.
"""

from __future__ import annotations


class MissingFxRateError(Exception):
    """Raised when a currency conversion is required but no FX rate is available.

    Never silently skipped and never approximated with a stale or "latest available" rate —
    a required conversion either succeeds with the correctly-dated rate or fails loudly.
    """


def convert_price(
    price: float,
    from_currency: str,
    to_currency: str,
    rate: float | None,
) -> float:
    """Convert `price` from `from_currency` to `to_currency`.

    `rate` must be the FX rate for the pair (base=`from_currency`, quote=`to_currency`) dated
    at the price's own observation date — "quote-per-1-base", so
    ``price_in_to_currency = price * rate`` (mirrors the yfinance `"{base}{quote}=X"` ticker
    convention, e.g. `rate("CADUSD=X")` is USD per 1 CAD).

    No-op when `from_currency == to_currency` — `rate` is ignored (may legitimately be
    `None`) since no conversion is needed. Otherwise raises `MissingFxRateError` if `rate` is
    `None` — a required conversion must never silently pass the price through unconverted.
    """
    if from_currency == to_currency:
        return price
    if rate is None:
        raise MissingFxRateError(
            f"No FX rate available to convert {from_currency!r} -> {to_currency!r}"
        )
    return price * rate
