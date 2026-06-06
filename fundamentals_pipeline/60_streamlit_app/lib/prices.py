"""Price-series helpers for the company Price tab: per-ticker slice, resampling, Altair chart."""
from __future__ import annotations

import altair as alt
import pandas as pd

from lib.colors import BLUE, CREAM

ACCENT = BLUE   # accent blue #185FA5 (reuse the shared token)

# pandas>=2.2: 'ME' = month-end (not deprecated 'M'); 'W' = weekly. None = daily (no resample).
RESAMPLE = {"Daily": None, "Weekly": "W", "Monthly": "ME"}


def prices_for(prices: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Sorted, NaN-dropped daily rows for one ticker (date, close, adj_close)."""
    if prices is None or prices.empty:
        return prices.iloc[:0] if prices is not None else pd.DataFrame(
            columns=["date", "close", "adj_close"])
    out = prices[prices["ticker"].astype(str) == ticker][["date", "close", "adj_close"]]
    return out.dropna(subset=["adj_close"]).sort_values("date").reset_index(drop=True)


def resample_prices(df: pd.DataFrame, freq_label: str) -> pd.DataFrame:
    """Daily → period-close at the chosen frequency. 'Daily' is a pass-through."""
    freq = RESAMPLE.get(freq_label)
    if not freq or df.empty:
        return df
    return (df.set_index("date")
              .resample(freq)
              .agg({"close": "last", "adj_close": "last"})
              .dropna(subset=["adj_close"])
              .reset_index())


def price_chart(df: pd.DataFrame, ticker: str):
    """Interactive Altair line of adjusted close. Returns None if there's nothing to plot."""
    if df is None or df.empty:
        return None
    return (
        alt.Chart(df)
        .mark_line(color=ACCENT, strokeWidth=1.6)
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("adj_close:Q", title="Adjusted close (USD)",
                    scale=alt.Scale(zero=False, nice=True)),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("adj_close:Q", title="Adj close", format="$,.2f"),
                alt.Tooltip("close:Q", title="Close", format="$,.2f"),
            ],
        )
        .properties(height=360)
        .configure_view(fill=CREAM, stroke=None)
        .configure_axis(grid=True, gridColor="#E7E2D8", domainColor="#CBBFA8",
                        labelFont="Inter", titleFont="Inter")
        .interactive()  # scroll-zoom + pan
    )
