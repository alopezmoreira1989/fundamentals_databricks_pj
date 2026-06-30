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


def fmt_num_scaled(v, divisor: int = 1, decimals: int | None = None) -> str:
    """Statement-table cells with an optional unit scale.

    divisor: 1 / 1e3 / 1e6 / 1e9. When divisor == 1, behaves exactly like
    fmt_num (thousands-separated integer, accounting-style negatives) so the
    'Units' view is byte-identical to today. When scaled, defaults to 1 decimal
    so small magnitudes don't collapse to 0 / round away signal (e.g. $391.0B
    rather than $391B; a $40M line under 'Billions' shows 0.0 rather than 0).
    """
    if is_missing(v):
        return EM_DASH
    if divisor == 1:
        return fmt_num(v)                       # unchanged Units path
    scaled = v / divisor
    dp = 1 if decimals is None else decimals
    if scaled < 0:
        return f"({abs(scaled):,.{dp}f})"
    return f"{scaled:,.{dp}f}"


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


def fmt_mos(v) -> str:
    """Margin-of-safety %: clamp the DISPLAY to ±100% so an extreme value (e.g. a method
    whose book value is distorted) doesn't render as a broken-looking huge number. The
    underlying signal still uses the true (unclamped) value, so the color is unaffected.
    """
    if is_missing(v):
        return EM_DASH
    if v >= 100:
        return ">+100%"
    if v <= -100:
        return "<−100%"          # real minus sign (U+2212)
    return f"{v:+.1f}%"


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


def trend_growth(values) -> tuple[Optional[float], Optional[float]]:
    """Robust annual growth over the whole series via log-linear regression.
    Fits OLS to ln(value) vs. year offset, annualizes the slope:
        growth% = (exp(slope) - 1) * 100
    Every observation pulls on the fit, so a single anomalous endpoint can't
    swing the read. Returns (growth_pct, r_squared):
      * strictly positive series -> log-linear fit; r_squared is in LOG space
        (how steadily exponential the trend is -- itself a quality signal);
      * all-negative series -> fit on |value|, sign flipped, mirroring fmt_cagr;
      * sign-crossing, or < 3 usable points -> (None, None).
    """
    pts = [(i, v) for i, v in enumerate(values) if not is_missing(v)]
    if len(pts) < 3:
        return (None, None)
    vals = [v for _, v in pts]
    all_pos = all(v > 0 for v in vals)
    all_neg = all(v < 0 for v in vals)
    if not (all_pos or all_neg):
        return (None, None)
    xs = [float(i) for i, _ in pts]
    ys = [math.log(abs(v)) for _, v in pts]
    n = len(xs)
    x_bar, y_bar = sum(xs) / n, sum(ys) / n
    sxx = sum((x - x_bar) ** 2 for x in xs)
    if sxx == 0:
        return (None, None)
    slope = sum((x - x_bar) * (y - y_bar) for x, y in zip(xs, ys)) / sxx
    growth = (math.exp(slope) - 1) * 100.0
    if all_neg:
        growth = -growth
    intercept = y_bar - slope * x_bar
    ss_tot = sum((y - y_bar) ** 2 for y in ys)
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    r2 = (1.0 - ss_res / ss_tot) if ss_tot > 0 else None
    return (growth, r2)


def fmt_trend_cagr(values) -> tuple[str, str, str]:
    """Robust trend growth for display. Returns (label, css_class, r2_badge)."""
    growth, r2 = trend_growth(values)
    if growth is None:
        return ("n/a", "", "")
    label = f"{growth:+.1f}%"
    cls = "up" if growth > 0.05 else ("down" if growth < -0.05 else "")
    badge = f"R²&nbsp;{r2:.2f}" if r2 is not None else ""
    return (label, cls, badge)


def short_year(fiscal_year: int) -> str:
    """2024 → '24."""
    return f"'{str(fiscal_year)[-2:]}"


def short_quarter(period_type: str, fiscal_year: int) -> str:
    """('Q4', 2024) → '24-Q4."""
    return f"'{str(fiscal_year)[-2:]}-{period_type}"