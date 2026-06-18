"""Price-series helpers for the company Price tab: slice + SMAs, resampling, KPIs, Altair chart."""
from __future__ import annotations

import altair as alt
import pandas as pd

from .colors import BLUE, CORAL, CREAM, GREEN
from .format import fmt_cagr  # CAGR label + css class (fmt_delta is NOT reused — it bakes in "YoY")

ACCENT = BLUE   # accent blue #185FA5 (reuse the shared token)

# pandas>=2.2: 'ME' = month-end (not deprecated 'M'); 'W' = weekly. None = daily (no resample).
RESAMPLE = {"Daily": None, "Weekly": "W", "Monthly": "ME"}

# Quick-range windows. Labels are calendar-invariant (the user thinks in calendar time, not grain
# units), so the option set narrows with the grain — no "1M" on a monthly view. "Max" = full history.
WINDOW_SETS = {
    "Daily":   ["1M", "3M", "6M", "1Y", "3Y", "5Y", "Max"],
    "Weekly":  ["3M", "6M", "1Y", "3Y", "5Y", "Max"],
    "Monthly": ["1Y", "3Y", "5Y", "10Y", "Max"],
}
WINDOW_DEFAULTS = {"Daily": "1Y", "Weekly": "1Y", "Monthly": "5Y"}
# Trailing offset per label, applied to the series' max date. "Max" maps to None (no truncation).
WINDOW_OFFSETS = {
    "1M":  pd.DateOffset(months=1),
    "3M":  pd.DateOffset(months=3),
    "6M":  pd.DateOffset(months=6),
    "1Y":  pd.DateOffset(years=1),
    "3Y":  pd.DateOffset(years=3),
    "5Y":  pd.DateOffset(years=5),
    "10Y": pd.DateOffset(years=10),
    "Max": None,
}

# SMA windows in TRADING DAYS (computed on the daily series, then sampled at the chart frequency)
# — so "SMA 200" always means 200 days, never 200 weeks/months.
SMA_WINDOWS = {"sma20": 20, "sma50": 50, "sma200": 200}

# Price + SMA palette on cream. Price = strong accent; SMAs fast→slow, warm→earth. GREEN/CORAL
# reuse the shared colors.py tokens; SMA 20's amber (#C8881F) has no exact token, kept literal.
_SERIES_COLORS = {"Price": ACCENT, "SMA 20": "#C8881F", "SMA 50": GREEN, "SMA 200": CORAL}
_SERIES_ORDER = list(_SERIES_COLORS)


# ── data shaping ──────────────────────────────────────────────────────────────

def prices_for(prices: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Sorted daily rows for one ticker (date, close, adj_close) + SMA 20/50/200 on adj_close."""
    if prices is None or prices.empty:
        cols = ["date", "close", "adj_close", *SMA_WINDOWS]
        return prices.iloc[:0] if prices is not None else pd.DataFrame(columns=cols)
    out = (prices[prices["ticker"].astype(str) == ticker][["date", "close", "adj_close"]]
           .dropna(subset=["adj_close"]).sort_values("date").reset_index(drop=True))
    for col, win in SMA_WINDOWS.items():
        out[col] = out["adj_close"].rolling(win).mean()   # NaN until `win` days exist (correct)
    return out


def slice_window(df: pd.DataFrame, window: str) -> pd.DataFrame:
    """Return df filtered to the trailing window. 'Max' (or an unknown label) is a pass-through.

    The anchor is the series' most recent date, so the last row of the slice is always the
    latest price — render_price_kpis()'s LATEST PRICE card stays a point-in-time fact regardless
    of the window.
    """
    if df is None or df.empty:
        return df
    offset = WINDOW_OFFSETS.get(window)
    if offset is None:   # "Max" or unrecognised → full history
        return df
    dates = pd.to_datetime(df["date"])
    return df[dates >= dates.max() - offset].reset_index(drop=True)


def price_window_css(active: str) -> str:
    """Compact styling for the quick-range button row; fills the active button with the accent.

    Targets Streamlit's per-key wrapper class (``st-key-pxwin-<label>``); the button keys in the
    view must match that ``pxwin-<label>`` scheme.
    """
    return (
        "<style>"
        'div[class*="st-key-pxwin-"] button{padding:0.1rem 0.5rem;min-height:0;line-height:1.3;'
        "border-radius:6px;font-size:0.78rem;font-weight:600;}"
        f'.st-key-pxwin-{active} button{{background:{ACCENT};border-color:{ACCENT};}}'
        f'.st-key-pxwin-{active} button p{{color:#fff;}}'
        "</style>"
    )


def resample_prices(df: pd.DataFrame, freq_label: str) -> pd.DataFrame:
    """Daily → period-close at the chosen frequency. 'Daily' is a pass-through.

    SMAs are carried as the period's last DAILY value, so they stay 'N-day' averages
    regardless of the weekly/monthly view.
    """
    freq = RESAMPLE.get(freq_label)
    if not freq or df.empty:
        return df
    agg = {"close": "last", "adj_close": "last", **{c: "last" for c in SMA_WINDOWS}}
    agg = {c: a for c, a in agg.items() if c in df.columns}   # defensive
    return df.set_index("date").resample(freq).agg(agg).dropna(subset=["adj_close"]).reset_index()


# ── KPI strip ─────────────────────────────────────────────────────────────────

def _price_delta(pct: float | None, suffix: str = "") -> tuple[str, str]:
    """Arrow + class for a price delta — like fmt_delta but WITHOUT the 'YoY' suffix."""
    if pct is None or pd.isna(pct):
        return ("—", "flat")
    if pct > 0.05:
        return (f"▲ {pct:.1f}%{suffix}", "up")
    if pct < -0.05:
        return (f"▼ {abs(pct):.1f}%{suffix}", "down")
    return (f"≈ {pct:+.1f}%{suffix}", "flat")


def _signed_pct(pct: float | None) -> tuple[str, str]:
    """('+28.4%', 'up'|'down'|'flat') for a percent shown as a headline value."""
    if pct is None or pd.isna(pct):
        return ("—", "flat")
    cls = "up" if pct > 0.05 else ("down" if pct < -0.05 else "flat")
    return (f"{pct:+.1f}%", cls)


def _px(v: float) -> str:
    """Share-price formatting: 2 decimals under $100, whole dollars above (keeps cards short)."""
    return f"${v:,.2f}" if abs(v) < 100 else f"${v:,.0f}"


def _val_color(cls: str) -> str:
    return {"up": "var(--positive)", "down": "var(--negative)"}.get(cls, "var(--ink)")


def _kpi_card(label: str, value: str, delta: str, delta_cls: str, value_cls: str = "") -> str:
    color = f"color:{_val_color(value_cls)};" if value_cls else ""
    return (
        '<div class="kpi">'
        f'<div class="label">{label}</div>'
        f'<div class="value" style="{color}">{value}</div>'
        f'<div class="delta {delta_cls}">{delta}</div>'
        '</div>'
    )


def render_price_kpis(pdf: pd.DataFrame) -> str:
    """4-card strip: latest price, 1Y return, 52-week range, window CAGR.

    Derived stats use adj_close (consistent with the plotted line, split-safe); the LATEST
    PRICE headline uses raw close. All computed from the DAILY series, independent of the
    freq toggle. Returns '' when empty.
    """
    if pdf is None or pdf.empty:
        return ""
    s = pdf.dropna(subset=["adj_close"]).sort_values("date").reset_index(drop=True)
    if s.empty:
        return ""

    dates = pd.to_datetime(s["date"])
    last_date = dates.iloc[-1]
    last_adj = float(s.iloc[-1]["adj_close"])
    price = float(s.iloc[-1]["close"]) if pd.notna(s.iloc[-1]["close"]) else last_adj

    # 1) Latest price + 1d change (raw close)
    chg_1d = None
    if len(s) >= 2 and pd.notna(s.iloc[-2]["close"]) and s.iloc[-2]["close"] != 0:
        chg_1d = (price - float(s.iloc[-2]["close"])) / abs(float(s.iloc[-2]["close"])) * 100
    d1_label, d1_cls = _price_delta(chg_1d, " (1d)")
    card1 = _kpi_card("LATEST PRICE", _px(price),
                      f"{d1_label}  ·  {last_date.strftime('%b %d, %Y')}", d1_cls)

    # 2) 1-year return (adj_close, as-of ~1y ago)
    prior = s[dates <= last_date - pd.DateOffset(years=1)]
    if prior.empty:
        card2 = _kpi_card("1Y RETURN", "—", "· < 1 year of data", "flat")
    else:
        base = float(prior.iloc[-1]["adj_close"])
        ret = (last_adj / base - 1) * 100 if base else None
        rv, rcls = _signed_pct(ret)
        base_dt = pd.to_datetime(prior.iloc[-1]["date"]).strftime("%b %Y")
        card2 = _kpi_card("1Y RETURN", rv, f"from {_px(base)}  ·  {base_dt}", "flat", value_cls=rcls)

    # 3) 52-week range + today's position within it
    win = s[dates >= last_date - pd.DateOffset(weeks=52)]
    hi, lo = float(win["adj_close"].max()), float(win["adj_close"].min())
    pos = (last_adj - lo) / (hi - lo) * 100 if hi > lo else 100.0
    card3 = _kpi_card("52W RANGE", f"{_px(lo)} – {_px(hi)}", f"today at {pos:.0f}% of range", "flat")

    # 4) Annualized return over the available window (adj_close)
    first = float(s.iloc[0]["adj_close"])
    years = max(1, round((last_date - dates.iloc[0]).days / 365.25))
    cagr_label, cagr_cls = fmt_cagr(first, last_adj, years)
    card4 = _kpi_card("PRICE CAGR", cagr_label, f"annualized  ·  {years}y", cagr_cls)

    return f'<div class="kpi-strip">{card1}{card2}{card3}{card4}</div>'


# ── chart ─────────────────────────────────────────────────────────────────────

def price_chart(df: pd.DataFrame, ticker: str, freq: str = "Daily"):
    """Interactive Altair chart: adjusted close + SMA 20/50/200, hierarchical month/year axis.

    SMAs are 'N-DAY' averages (computed on the daily series upstream), shown at the chart's
    frequency. Vega-Lite stacks the tick label into two lines (month/day on top, year below at
    each January) via labelExpr. Clicking a legend entry toggles a series. Returns None if empty.
    """
    if df is None or df.empty:
        return None

    plot = df.rename(columns={"adj_close": "Price", "sma20": "SMA 20",
                              "sma50": "SMA 50", "sma200": "SMA 200"})
    value_cols = [c for c in _SERIES_ORDER if c in plot.columns]

    if freq == "Monthly":
        label_expr = ("[timeFormat(datum.value, '%b'), "
                      "timeFormat(datum.value, '%m') == '01' ? timeFormat(datum.value, '%Y') : '']")
    else:  # Daily / Weekly — include the day so zoomed-in ticks distinguish weeks
        label_expr = ("[timeFormat(datum.value, '%b %d'), "
                      "(timeFormat(datum.value, '%m') == '01' && toNumber(timeFormat(datum.value, '%d')) <= 7)"
                      " ? timeFormat(datum.value, '%Y') : '']")

    sel = alt.selection_point(fields=["series"], bind="legend")  # click legend to toggle a line

    return (
        alt.Chart(plot)
        .transform_fold(value_cols, as_=["series", "value"])
        .mark_line(strokeWidth=1.4)
        .encode(
            x=alt.X("date:T", title=None,
                    axis=alt.Axis(labelExpr=label_expr, labelAngle=0, labelOverlap=True)),
            y=alt.Y("value:Q", title="Adjusted close (USD)",
                    scale=alt.Scale(zero=False, nice=True)),
            color=alt.Color("series:N", title=None, sort=_SERIES_ORDER,
                            scale=alt.Scale(domain=_SERIES_ORDER,
                                            range=[_SERIES_COLORS[s] for s in _SERIES_ORDER]),
                            legend=alt.Legend(orient="top-left", symbolStrokeWidth=2)),
            opacity=alt.condition(sel, alt.value(1.0), alt.value(0.12)),
            tooltip=[alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                     alt.Tooltip("series:N", title="Series"),
                     alt.Tooltip("value:Q", title="Value", format="$,.2f")],
        )
        .properties(height=360)
        .add_params(sel)
        .configure_view(fill=CREAM, stroke=None)
        .configure_axis(grid=True, gridColor="#E7E2D8", domainColor="#CBBFA8",
                        labelFont="Inter", titleFont="Inter")
        .interactive()  # scroll-zoom + pan
    )
