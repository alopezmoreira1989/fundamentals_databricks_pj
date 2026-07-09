"""Currency badge + USD-lens conversion for Canadian (and future non-USD) tickers.

Two ground-truth currencies per ticker, which can differ (see CLAUDE.md's
currency-alignment convention):
  * ``reporting_currency`` — the currency of Market Cap and all raw financials.
  * quote currency — the currency the PRICE trades in (never converted pipeline-side),
    derived from listing ``market`` via QUOTE_CURRENCY_BY_MARKET below.

Conversion mirrors ``fundamentals_pipeline.fx.convert_price()``'s scalar semantics and
``22__derived_metrics.py``'s Spark asof-join: for each row needing conversion, find the
most recent FX rate on or before that row's own observation date (never spot/today's
rate), then multiply. A row with no matching rate is left in its NATIVE currency and
must keep showing its ``currency_badge()`` — never silently guessed.
"""

from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from fundamentals_pipeline.fx import convert_price

from .format import is_missing

# Quote currency by listing market — mirrors 22__derived_metrics.py's
# QUOTE_CURRENCY_BY_MARKET (the pipeline-side ground truth this reads from `market`).
QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}


def quote_currency(market: str | None) -> str:
    """Currency a ticker's PRICE trades in, from its listing `market` ("US"/"CA")."""
    return QUOTE_CURRENCY_BY_MARKET.get((market or "US").upper(), "USD")


def currency_badge(currency: str | None) -> str:
    """Small inline label for a non-USD currency; "" for USD/empty (no badge shown)."""
    if not currency or currency.upper() == "USD":
        return ""
    return f'<span class="ccy-badge">{html.escape(currency.upper())}</span>'


def usd_lens_toggle(key: str = "usd_lens") -> bool:
    """Shared 'view in USD' toggle — same session_state key wherever it's rendered, so
    the choice persists as the user moves between the Screener and Company pages."""
    return st.toggle("View in USD", key=key)


def usd_lens_convert(value: float, currency: str, as_of, fx: pd.DataFrame | None) -> tuple[float, str]:
    """Convert one scalar value to USD if possible; else return it unchanged with its badge.

    Shared by every "View in USD" card (top KPI strip, Overview tab) so they always agree —
    `currency` is assumed already uppercased/non-empty-checked by the caller.
    """
    if fx is None or fx.empty or is_missing(as_of):
        return value, currency_badge(currency)
    converted, ok = convert_to_usd(pd.Series([value]), pd.Series([currency]), pd.Series([as_of]), fx)
    if bool(ok.iloc[0]):
        return float(converted.iloc[0]), ""
    return value, currency_badge(currency)


def convert_to_usd(
    values: pd.Series, currencies: pd.Series, as_of_dates: pd.Series, fx: pd.DataFrame
) -> tuple[pd.Series, pd.Series]:
    """Convert `values` to USD where `currencies` != USD, date-anchored per row to
    `as_of_dates` (a fiscal period_end for Market Cap, a trade date for price).

    Returns `(out, converted)`: `out` holds the converted value (or the original value,
    unchanged, for any row that didn't need or couldn't get a conversion); `converted` is
    a same-index boolean Series — True only where a real USD value was produced. Rows
    already in USD are NOT flagged `converted` (nothing to badge either way). Rows needing
    conversion with no FX rate dated on or before their own `as_of_date` are left
    UNCONVERTED (native currency, `converted=False`) — callers must keep badging those so
    a native value is never shown as if it were USD.
    """
    out = pd.to_numeric(values, errors="coerce").astype(float).copy()
    converted = pd.Series(False, index=out.index)
    # astype(object) first: currencies may be category dtype (from _optimize_dtypes),
    # whose .fillna() rejects a fill value ("") that isn't already one of its categories.
    ccy = currencies.astype(object).fillna("").astype(str).str.upper()
    dates = pd.to_datetime(as_of_dates, errors="coerce")

    if fx is None or fx.empty:
        return out, converted

    needs_mask = (ccy != "") & (ccy != "USD") & dates.notna()
    if not needs_mask.any():
        return out, converted

    frame = pd.DataFrame({"value": out, "ccy": ccy, "as_of": dates})[needs_mask]

    for code, grp in frame.groupby("ccy"):
        pair_fx = (
            fx[(fx["base"].astype(str).str.upper() == code) & (fx["quote"].astype(str).str.upper() == "USD")]
            [["date", "rate"]]
            .dropna()
            .sort_values("date")
        )
        if pair_fx.empty:
            continue
        grp_sorted = grp.sort_values("as_of")
        merged = pd.merge_asof(
            grp_sorted, pair_fx, left_on="as_of", right_on="date", direction="backward"
        )
        for idx, val, rate in zip(
            grp_sorted.index, merged["value"].to_numpy(), merged["rate"].to_numpy(), strict=True
        ):
            if pd.notna(rate):
                out.loc[idx] = convert_price(float(val), code, "USD", float(rate))
                converted.loc[idx] = True
            # else: no rate on/before this date — leave native (out already holds it).

    return out, converted
