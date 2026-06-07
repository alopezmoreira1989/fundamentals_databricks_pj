"""Price-series helpers for the company Price tab: per-ticker slice, resampling, KPI, Altair chart."""
from __future__ import annotations

import altair as alt
import pandas as pd

from .colors import BLUE, CREAM
from .format import fmt_delta  # arrow + up/down class, same styling as the masthead KPI strip

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


def render_price_kpi(pdf: pd.DataFrame) -> str:
    """Single KPI card: most recent share price + day-over-day change.

    Reuses the masthead `.kpi` markup. Headline = raw `close` (actual traded price).
    Always computed from the DAILY series, independent of the freq toggle. Returns '' if empty.
    """
    if pdf is None or pdf.empty:
        return ""
    last = pdf.iloc[-1]
    price = last["close"] if pd.notna(last["close"]) else last["adj_close"]
    if pd.isna(price):
        return ""
    as_of = pd.to_datetime(last["date"]).strftime("%b %d, %Y")

    chg_pct = None
    if len(pdf) >= 2:
        prev = pdf.iloc[-2]["close"]
        if pd.notna(prev) and prev != 0:
            chg_pct = (price - prev) / abs(prev) * 100

    if chg_pct is None:
        delta_label, delta_cls, suffix = "—", "flat", ""
    else:
        delta_label, delta_cls = fmt_delta(chg_pct)
        suffix = " (1d)"

    return (
        '<div class="kpi-strip" style="grid-template-columns:minmax(220px,300px);margin-bottom:20px;">'
        '<div class="kpi">'
        '<div class="label">LATEST PRICE</div>'
        f'<div class="value">${price:,.2f}</div>'
        f'<div class="delta {delta_cls}">{delta_label}{suffix}  ·  {as_of}</div>'
        '</div></div>'
    )


def price_chart(df: pd.DataFrame, ticker: str, freq: str = "Daily"):
    """Interactive Altair line of adjusted close with a hierarchical (month / year) time axis.

    Vega-Lite renders a tick label as two stacked lines when labelExpr returns an array: month
    (plus day-of-month for Daily/Weekly, so zooming reveals individual weeks) on top, year on a
    subordinate line below, shown only at the first tick of each year. Returns None if empty.
    """
    if df is None or df.empty:
        return None

    if freq == "Monthly":
        label_expr = (
            "[timeFormat(datum.value, '%b'), "
            "timeFormat(datum.value, '%m') == '01' ? timeFormat(datum.value, '%Y') : '']"
        )
    else:  # Daily / Weekly — include the day so zoomed-in ticks distinguish weeks
        label_expr = (
            "[timeFormat(datum.value, '%b %d'), "
            "(timeFormat(datum.value, '%m') == '01' && toNumber(timeFormat(datum.value, '%d')) <= 7)"
            " ? timeFormat(datum.value, '%Y') : '']"
        )

    return (
        alt.Chart(df)
        .mark_line(color=ACCENT, strokeWidth=1.6)
        .encode(
            x=alt.X("date:T", title=None,
                    axis=alt.Axis(labelExpr=label_expr, labelAngle=0, labelOverlap=True)),
            y=alt.Y("adj_close:Q", title="Adjusted close (USD)",
                    scale=alt.Scale(zero=False, nice=True)),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
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
