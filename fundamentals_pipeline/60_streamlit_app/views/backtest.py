"""Backtest — investment-philosophy archetypes vs SPY, equity curve + metrics.

Reads the published `dashboard_backtest.parquet` (one row per archetype × fiscal_year).
Degrades gracefully when the artifact is absent (backtester not run / not published yet):
shows a notice and the rest of the app keeps working — `load_backtest()` never st.stop()s.

The headline metrics (CAGR / max drawdown / volatility / Sharpe) are derived client-side
from the published series via `fundamentals_pipeline._core.backtest` — the SAME pure helpers
the pipeline notebook uses, so the numbers reconcile by construction.
"""

import sys
from pathlib import Path

import altair as alt
import streamlit as st
from lib.colors import BLUE, CREAM, GRAY
from lib.data import load_backtest

# Reuse the same pure backtest helpers as the pipeline. Put the repo root on sys.path (as
# lib.data does) so _core is importable on Streamlit Cloud; keeps the app Databricks-free.
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from fundamentals_pipeline._core import backtest as bt  # noqa: E402

# Mirrors backtest_archetypes.json `config.risk_free_rate` (the app stays self-contained —
# it does not read the pipeline config). Update both if you change the assumption.
RISK_FREE = 0.04


def _pretty(slug: str) -> str:
    return slug.replace("_", " ").title()


def _kpi_card(label: str, value: str, sub: str = "", value_cls: str = "") -> str:
    color = {"up": "var(--positive)", "down": "var(--negative)"}.get(value_cls, "var(--ink)")
    return (
        '<div class="kpi">'
        f'<div class="label">{label}</div>'
        f'<div class="value" style="color:{color};">{value}</div>'
        f'<div class="delta flat">{sub}</div>'
        '</div>'
    )


def _pct(v) -> str:
    return "—" if v is None else f"{v * 100:+.1f}%"


df = load_backtest()

st.markdown(
    '<div class="masthead"><div class="masthead-left">'
    '<div class="eyebrow">US Equities · Strategy Backtest</div><h1>Investment archetypes</h1>'
    '<div class="ticker-row"><span class="ticker-chip">Annual rebalance · equal weight</span></div>'
    '</div><div class="masthead-right"><div>Forward returns vs SPY</div>'
    '<div style="margin-top:6px;">No look-ahead (filing-date driven)</div></div></div>',
    unsafe_allow_html=True,
)

# Survivorship caveat — always visible (results are biased upward).
st.warning(
    "**Survivorship bias:** the universe is tickers that exist *today*. Companies that "
    "delisted (bankruptcies, buyouts) never enter, so historical returns are biased "
    "**upward**. Treat these as a relative comparison between archetypes, not an absolute "
    "forecast.",
    icon="⚠️",
)

if df.empty:
    st.info(
        "Backtest data has not been published yet. Once `70_backtest/71__run_backtest` runs "
        "in the pipeline and `51`/`52` publish the artifacts, the equity curves appear here."
    )
else:
    archetypes = sorted(df["archetype"].astype(str).unique())
    pick = st.selectbox("Archetype", archetypes, format_func=_pretty)

    sub = df[df["archetype"].astype(str) == pick].sort_values("fiscal_year").reset_index(drop=True)

    # ── headline metrics (derived from the series via _core.backtest) ──────────────
    equity = [100.0, *sub["portfolio_value"].tolist()]
    returns = sub["portfolio_return"].tolist()
    n_years = len(sub)
    last_pv = float(sub["portfolio_value"].iloc[-1]) if n_years else None

    port_cagr = bt.cagr(100.0, last_pv, n_years) if last_pv else None
    mdd = bt.max_drawdown(equity)
    vol = bt.annualized_vol(returns)
    shp = bt.sharpe(returns, RISK_FREE)

    has_bench = sub["benchmark_value"].notna().any()
    excess = None
    bench_sub = ""
    if has_bench:
        bench_returns = sub["benchmark_return"].dropna().tolist()
        last_bv = float(sub["benchmark_value"].dropna().iloc[-1])
        bench_cagr = bt.cagr(100.0, last_bv, len(bench_returns)) if bench_returns else None
        if port_cagr is not None and bench_cagr is not None:
            excess = port_cagr - bench_cagr
            bench_sub = f"SPY {_pct(bench_cagr)}"

    cards = [
        _kpi_card("CAGR", _pct(port_cagr), f"{n_years}y · annualized",
                  "up" if (port_cagr or 0) > 0 else "down"),
        _kpi_card("VS SPY", _pct(excess), bench_sub or "benchmark n/a",
                  "up" if (excess or 0) > 0 else ("down" if excess is not None else "")),
        _kpi_card("MAX DRAWDOWN", _pct(mdd), "peak → trough", "down"),
        _kpi_card("SHARPE", "—" if shp is None else f"{shp:.2f}",
                  f"σ {_pct(vol)} · rf {RISK_FREE * 100:.0f}%"),
    ]
    st.markdown(f'<div class="kpi-strip">{"".join(cards)}</div>', unsafe_allow_html=True)

    # ── equity curve (growth of $100) ──────────────────────────────────────────────
    plot = sub.rename(columns={"portfolio_value": "Portfolio", "benchmark_value": "Benchmark (SPY)"})
    series = ["Portfolio"] + (["Benchmark (SPY)"] if has_bench else [])
    colors = {"Portfolio": BLUE, "Benchmark (SPY)": GRAY}

    chart = (
        alt.Chart(plot)
        .transform_fold(series, as_=["series", "value"])
        .mark_line(strokeWidth=2, point=True)
        .encode(
            x=alt.X("fiscal_year:O", title="Fiscal year"),
            y=alt.Y("value:Q", title="Growth of $100", scale=alt.Scale(zero=False, nice=True)),
            color=alt.Color("series:N", title=None, sort=series,
                            scale=alt.Scale(domain=series, range=[colors[s] for s in series]),
                            legend=alt.Legend(orient="top-left")),
            tooltip=[alt.Tooltip("fiscal_year:O", title="FY"),
                     alt.Tooltip("series:N", title="Series"),
                     alt.Tooltip("value:Q", title="Value", format="$,.0f")],
        )
        .properties(height=360)
        .configure_view(fill=CREAM, stroke=None)
        .configure_axis(grid=True, gridColor="#E7E2D8", domainColor="#CBBFA8",
                        labelFont="Inter", titleFont="Inter")
    )
    st.altair_chart(chart, use_container_width=True)

    st.caption(
        f"Holdings per year: {int(sub['n_holdings'].min())}–{int(sub['n_holdings'].max())}. "
        "Each name enters at the close on its 10-K filing date and is held to the next "
        "year's filing; cohorts are equal-weighted and chained into the curve above."
    )
