"""Pure backtest helpers — as-of eligibility, predicate evaluation, portfolio stats.

CONTRACT: reused by ``70_backtest/71__run_backtest.py`` (pipeline) AND by the Streamlit
``views/backtest.py`` (the equity-curve metrics are derived client-side from the published
series, so the same code runs in both places). No Spark/pandas/streamlit dependency — plain
Python on lists/scalars/``datetime.date`` so it is unit-testable in isolation.

No-look-ahead is enforced by :func:`as_of_eligible`: a name's fundamentals for fiscal year Y
are only usable from their *as-of date* — the SEC 10-K filing date, or ``period_end + lag``
when the filing date is unavailable.
"""

from __future__ import annotations

import math
import operator
from datetime import date, timedelta

from .valuation import safe_div  # noqa: F401 — re-exported for the notebook's convenience

Number = float | int | None

# Comparison operators allowed in archetype predicates.
_OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}


def _is_missing(v: Number) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# ── as-of (no look-ahead) ────────────────────────────────────────────────────────
def as_of_date(filed: date | None, period_end: date | None, lag_days: int = 90) -> date | None:
    """The date a fiscal year's fundamentals become public-knowledge.

    Prefer the actual SEC filing date (``filed``); else fall back to
    ``period_end + lag_days`` (the filing-lag proxy). ``None`` if neither is available.
    """
    if filed is not None:
        return filed
    if period_end is None:
        return None
    return period_end + timedelta(days=lag_days)


def as_of_eligible(as_of: date | None, rebalance_date: date | None) -> bool:
    """True iff fundamentals dated ``as_of`` were knowable by ``rebalance_date`` (``as_of <=
    rebalance_date``). A missing date is never eligible — we refuse to assume knowledge.
    """
    if as_of is None or rebalance_date is None:
        return False
    return as_of <= rebalance_date


# ── predicate evaluation ──────────────────────────────────────────────────────────
def passes_predicates(metrics: dict[str, Number], predicates: list) -> bool:
    """True iff every ``[metric, op, threshold]`` predicate holds for ``metrics``.

    A missing/NaN metric **fails** its predicate (conservative — a screen can't pass a
    name on a value it doesn't have). Raises ``ValueError`` on an unknown operator.
    """
    for metric, op, threshold in predicates:
        v = metrics.get(metric)
        if _is_missing(v):
            return False
        fn = _OPS.get(op)
        if fn is None:
            raise ValueError(f"unknown operator {op!r}; expected one of {sorted(_OPS)}")
        if not fn(v, threshold):
            return False
    return True


# ── portfolio statistics (operate on a series of PERIODIC returns / an equity curve) ──
def cagr(start_value: Number, end_value: Number, years: Number) -> float | None:
    """Compound annual growth rate from an equity-curve endpoint pair.

    ``None`` if either value is missing/non-positive or ``years <= 0`` (CAGR is undefined
    across a wipeout or a zero horizon).
    """
    if _is_missing(start_value) or _is_missing(end_value) or _is_missing(years):
        return None
    if start_value <= 0 or end_value <= 0 or years <= 0:
        return None
    return (float(end_value) / float(start_value)) ** (1.0 / float(years)) - 1.0


def max_drawdown(equity_curve: list[float]) -> float | None:
    """Largest peak-to-trough decline of an equity curve, as a non-positive fraction.

    ``-0.30`` means a 30% drawdown. ``None`` for an empty curve; ``0.0`` for a curve that
    only ever rises.
    """
    pts = [float(v) for v in equity_curve if not _is_missing(v)]
    if not pts:
        return None
    peak = pts[0]
    mdd = 0.0
    for v in pts:
        if v > peak:
            peak = v
        if peak > 0:
            dd = v / peak - 1.0
            if dd < mdd:
                mdd = dd
    return mdd


def annualized_vol(returns: list[float]) -> float | None:
    """Sample standard deviation of a series of periodic (here: annual) returns.

    Inputs are already annual, so this *is* the annualized volatility. ``None`` for fewer
    than two observations.
    """
    pts = [float(r) for r in returns if not _is_missing(r)]
    n = len(pts)
    if n < 2:
        return None
    mean = sum(pts) / n
    var = sum((r - mean) ** 2 for r in pts) / (n - 1)
    return math.sqrt(var)


def sharpe(returns: list[float], risk_free: float = 0.0) -> float | None:
    """Sharpe ratio from annual returns: ``(mean_return - risk_free) / annualized_vol``.

    ``None`` for fewer than two observations or zero volatility.
    """
    pts = [float(r) for r in returns if not _is_missing(r)]
    n = len(pts)
    if n < 2:
        return None
    mean = sum(pts) / n
    var = sum((r - mean) ** 2 for r in pts) / (n - 1)
    vol = math.sqrt(var)
    if vol < 1e-12:   # ~constant returns → zero vol (FP-safe) → undefined Sharpe
        return None
    return (mean - risk_free) / vol
