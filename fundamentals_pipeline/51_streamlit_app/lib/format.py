"""Number formatting helpers shared across renderers."""

from __future__ import annotations

import math
from typing import Optional

import pandas as pd

EM_DASH = "—"


def is_missing(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v)


def fmt_num(v) -> str:
    """Table number cells: thousands-separated integer, accounting-style negatives."""
    if is_missing(v):
        return EM_DASH
    if v < 0:
        return f"({abs(v):,.0f})"
    return f"{v:,.0f}"


def fmt_eps(v) -> str:
    """Per-share rows: 2 decimals, accounting-style negatives."""
    if is_missing(v):
        return EM_DASH
    if v < 0:
        return f"({abs(v):,.2f})"
    return f"{v:,.2f}"


def fmt_kpi(v) -> str:
    """KPI strip: $391.0B / $93.7M (scale-aware, one decimal)."""
    if is_missing(v):
        return EM_DASH
    sign = "-" if v < 0 else ""
    a = abs(v)
    # The export keeps raw USD. We display in $B if ≥ $1B else $M.
    # Source values are already in raw USD (not millions); financials are large numbers.
    if a >= 1_000_000_000:
        return f"{sign}${a / 1_000_000_000:.1f}B"
    if a >= 1_000_000:
        return f"{sign}${a / 1_000_000:.1f}M"
    return f"{sign}${a:,.0f}"


def fmt_metric(v, unit: Optional[str], signed: bool = False) -> str:
    """Derived metrics: format depending on unit (percent / ratio / usd).

    `signed=True` forces a leading "+" on positive percents — only for metrics
    that represent *direction* (growth YoY, margin of safety), not level
    (margins, returns, yields). Negatives always show "−" either way.
    """
    if is_missing(v):
        return EM_DASH
    if unit == "percent":
        if signed:
            return f"{v:+.1f}%"
        return f"{v:.1f}%"
    if unit == "ratio":
        return f"{v:,.2f}x"
    if unit == "usd":
        return fmt_kpi(v)
    return f"{v:,.2f}"


def fmt_delta(yoy_pct: Optional[float]) -> tuple[str, str]:
    """Returns (label_html, css_class) for a YoY delta in the KPI strip."""
    if is_missing(yoy_pct):
        return ("· no prior year", "flat")
    if yoy_pct > 0.05:
        return (f"▲ {yoy_pct:.1f}% YoY", "up")
    if yoy_pct < -0.05:
        return (f"▼ {abs(yoy_pct):.1f}% YoY", "down")
    return (f"≈ {yoy_pct:+.1f}% YoY", "flat")


def fmt_cagr(start, end, years: int) -> tuple[str, str]:
    """Compute CAGR and return (label, css_class). 'n/a' for sign changes or missing endpoints."""
    if is_missing(start) or is_missing(end) or years < 1:
        return (EM_DASH, "")
    if start == 0:
        return ("n/a", "")
    # CAGR is undefined when the series crosses zero (sign change).
    if (start < 0) != (end < 0):
        return ("n/a", "")
    if start < 0 and end < 0:
        # Both negative: compute on absolute values, flip the interpretation.
        ratio = abs(end) / abs(start)
        cagr = (ratio ** (1 / years) - 1) * 100
        cagr = -cagr  # "less negative" = positive trend
    else:
        ratio = end / start
        cagr = (ratio ** (1 / years) - 1) * 100
    label = f"{cagr:+.1f}%"
    cls = "up" if cagr > 0.05 else ("down" if cagr < -0.05 else "")
    return (label, cls)


def short_year(fiscal_year: int) -> str:
    """2024 → '24."""
    return f"'{str(fiscal_year)[-2:]}"


def short_quarter(period_type: str, fiscal_year: int) -> str:
    """('Q4', 2024) → '24-Q4."""
    return f"'{str(fiscal_year)[-2:]}-{period_type}"