"""Scalar reference implementations of the intrinsic-value formulas.

CONTRACT: the PySpark/numpy column algebra in
``20__transformation/23__intrinsic_value.py`` (and ``safe_div`` in
``22__derived_metrics.py``) must mirror these scalar definitions. If you change a
formula in ``23``, change it here too and update ``tests/test_valuation.py``.

Conventions, matching the Spark code:
  * A "missing" input is ``None`` or ``float('nan')``.
  * Division by zero / by a missing denominator yields ``None`` (mirrors the
    ``F.when(den != 0, ...).otherwise(lit(None))`` guard), never raises.
  * Growth / yield / discount rates are decimal fractions (0.08 = 8%), exactly as
    stored in ``00__config/valuation_assumptions.json``.
"""

from __future__ import annotations

import math
from typing import Union

# `from __future__ import annotations` only defers *annotation* evaluation — this is a plain
# assignment, so the RHS runs at import time. PEP 604 `float | int | None` needs `type.__or__`,
# which doesn't exist before Python 3.10; `typing.Union` is the 3.9-safe equivalent.
Number = Union[float, int, None]


def _is_missing(v: Number) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def _z(v: Number) -> float:
    """Missing → 0.0 (mirrors the ``z = np.where(isnan, 0, x)`` helper in ``23``)."""
    return 0.0 if _is_missing(v) else float(v)


def safe_div(num: Number, den: Number) -> float | None:
    """``num / den``; ``None`` if either operand is missing or ``den == 0``.

    Mirrors ``safe_div`` / ``safe_div_col`` in ``22__derived_metrics.py``.
    """
    if _is_missing(num) or _is_missing(den) or float(den) == 0.0:
        return None
    return float(num) / float(den)


def pe_ratio(market_cap: Number, net_income: Number) -> float | None:
    """P/E = market_cap / net_income.

    Returns ``None`` when net_income is missing, zero, or negative — a loss-making
    company has no meaningful P/E multiple. Mirrors the ``F.when(net_income > 0, ...)``
    guard in ``22__derived_metrics.py`` (which the general ``safe_div`` does NOT apply,
    since other multiples like P/B accept negative denominators).
    """
    if _is_missing(net_income) or float(net_income) <= 0:
        return None
    if _is_missing(market_cap):
        return None
    return float(market_cap) / float(net_income)


def earnings_yield(net_income: Number, market_cap: Number) -> float | None:
    """Earnings Yield % = net_income / market_cap × 100.

    Returns ``None`` when net_income is missing, zero, or negative.
    Mirrors the same guard as ``pe_ratio`` (they are inverses of each other).
    """
    if _is_missing(net_income) or float(net_income) <= 0:
        return None
    if _is_missing(market_cap) or float(market_cap) == 0.0:
        return None
    return float(net_income) / float(market_cap) * 100.0


def graham_number(eps: Number, bvps: Number, magic: float = 22.5) -> float | None:
    """Graham's napkin rule: ``sqrt(magic * eps * bvps)``.

    ``None`` unless both ``eps > 0`` and ``bvps > 0`` (a loss or negative book value
    makes the square root meaningless). ``magic`` defaults to 22.5 (15× max P/E ×
    1.5× max P/B), per ``valuation_assumptions.json``. The book-distortion guards
    (retained earnings < 0, P/B > 10) live in ``23`` as a separate skip condition,
    not here — this function only owns the core formula.
    """
    if _is_missing(eps) or _is_missing(bvps) or eps <= 0 or bvps <= 0:
        return None
    return math.sqrt(magic * float(eps) * float(bvps))


def graham_revised(
    eps: Number,
    growth: Number,
    aaa_yield: Number,
    base_pe: float = 8.5,
    growth_multiplier: float = 2.0,
    aaa_yield_norm: float = 4.4,
    growth_cap: float = 0.15,
) -> float | None:
    """Graham's revised formula with growth:

        eps * (base_pe + growth_multiplier * g*100) * aaa_yield_norm / (aaa_yield*100)

    ``growth`` is a decimal fraction; it is floored at 0 (Graham's ``base_pe`` IS the
    no-growth P/E — a shrinking firm must not push the multiple below it) and capped
    at ``growth_cap`` (15%), matching the ``np.clip(min(g, cap), 0, None)`` in ``23``.
    ``None`` if ``eps`` is missing or ``eps <= 0``, or if ``aaa_yield`` is missing/zero.
    Defaults from ``valuation_assumptions.json``: base_pe=8.5, growth_multiplier=2.0,
    aaa_yield_norm=4.4, growth_cap=0.15.
    """
    if _is_missing(eps) or eps <= 0 or _is_missing(aaa_yield) or float(aaa_yield) == 0.0:
        return None
    g = _z(growth)
    g_eff = max(0.0, min(g, growth_cap))
    return float(eps) * (base_pe + growth_multiplier * g_eff * 100.0) * aaa_yield_norm / (float(aaa_yield) * 100.0)


def dcf_value(
    fcf: Number,
    growth: Number,
    discount_rate: Number,
    terminal_growth: Number,
    years: int,
) -> float | None:
    """Two-stage DCF — present value of the cash-flow stream.

    Stage 1: ``fcf`` grows at ``growth`` for ``years`` periods, each discounted at
    ``discount_rate`` (explicit per-year loop, to match ``23`` bit-for-bit rather than
    a closed-form sum). Stage 2: a Gordon terminal value
    ``cf_terminal / (discount_rate - terminal_growth)`` discounted back ``years``.

    Returns ``pv_stage1 + pv_terminal`` — the PV of the FCF stream. The net-cash bridge
    (``- debt + cash``) and the per-share division that ``23`` applies afterwards are
    deliberately NOT part of this scalar (they need balance-sheet inputs the caller
    supplies). ``None`` if ``fcf`` is missing/``<= 0``, ``years < 1``, or
    ``discount_rate <= terminal_growth`` (terminal value would be undefined/negative).
    """
    if _is_missing(fcf) or fcf <= 0 or years < 1:
        return None
    if _is_missing(discount_rate) or _is_missing(terminal_growth) or float(discount_rate) <= float(terminal_growth):
        return None
    g, dr, tg = _z(growth), float(discount_rate), float(terminal_growth)
    cf = float(fcf)
    pv_stage1 = 0.0
    for t in range(1, int(years) + 1):
        cf = cf * (1 + g)
        pv_stage1 += cf / ((1 + dr) ** t)
    cf_terminal = cf * (1 + tg)
    terminal_value = cf_terminal / (dr - tg)
    pv_terminal = terminal_value / ((1 + dr) ** int(years))
    return pv_stage1 + pv_terminal


def owner_earnings(
    net_income: Number,
    dna: Number,
    sbc: Number,
    capex: Number,
    delta_wc: Number,
) -> float:
    """Buffett's Owner Earnings (1986): ``NI + D&A + SBC - CapEx - ΔWC``.

    Each missing component is treated as 0 (mirrors the ``z()`` coalesce in ``23``,
    so the figure is never ``None``). This returns the dollar figure; the valuation
    step (``× multiple`` or ``/ discount_rate``, then ``/ shares``) is applied by the
    caller, matching how ``23`` keeps ``oe_dollars`` separate from ``oev``.
    """
    return _z(net_income) + _z(dna) + _z(sbc) - _z(capex) - _z(delta_wc)


def eps_cagr(eps_start: Number, eps_end: Number, n_years: int) -> float | None:
    """Trailing EPS CAGR: ``(eps_end / eps_start) ** (1/n_years) - 1``.

    ``None`` unless both endpoints are present and ``> 0`` (a CAGR across a sign change
    or from a non-positive base is meaningless) and ``n_years >= 1``. Mirrors the
    point-in-time EPS-CAGR window in ``23`` (which also requires both EPS > 0).
    """
    if _is_missing(eps_start) or _is_missing(eps_end) or eps_start <= 0 or eps_end <= 0:
        return None
    if n_years < 1:
        return None
    return (float(eps_end) / float(eps_start)) ** (1.0 / int(n_years)) - 1.0
