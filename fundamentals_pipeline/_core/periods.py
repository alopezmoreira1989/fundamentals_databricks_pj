"""Scalar reference for the quarterly period arithmetic.

CONTRACT: mirrors the ``Q4`` derivation in
``20_transformation/21b__derive_quarterly.py``. ``Q4 = FY − YTD_Q3`` is intentional
(it captures year-end audit adjustments) — see CLAUDE.md. This module owns ONLY the
fallback arithmetic; the preferred path in ``21b`` is to use a reported Q4 standalone
(the ~90d ``fp="FY"`` row whose ``period_end == fy_end``) when the issuer publishes one,
and only fall back to ``FY − YTD_Q3``. That standalone-vs-derived choice is a
data-shape decision made in Spark and is out of scope for this scalar.
"""

from __future__ import annotations

import math

Number = float | int | None

VALID_KINDS = ("flow_additive", "flow_nonadditive", "stock")


def _is_missing(v: Number) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def q4_from_fy_ytd(fy_value: Number, ytd_q3_value: Number, kind: str) -> float | None:
    """Derive a Q4 value from the annual FY figure and the YTD-through-Q3 figure.

    Semantics by concept ``kind`` (matching ``21b``):

    * ``flow_additive`` (Income Statement, Cash Flow — summable): ``fy_value − ytd_q3_value``.
      ``None`` if either input is missing (you cannot plug a Q4 from a half-known year).
    * ``flow_nonadditive``: ``None`` — a non-additive flow cannot be derived by
      subtraction; ``21b`` keeps it only when a Q4 standalone was actually reported.
    * ``stock`` (Balance Sheet snapshots): the Q4 (fiscal-year-end) snapshot equals the
      FY snapshot — both are taken at the same ``period_end`` — so this returns
      ``fy_value`` unchanged. Subtraction is never applied to a stock concept.

    Raises ``ValueError`` for an unrecognised ``kind``.
    """
    if kind == "flow_additive":
        if _is_missing(fy_value) or _is_missing(ytd_q3_value):
            return None
        return float(fy_value) - float(ytd_q3_value)
    if kind == "flow_nonadditive":
        return None
    if kind == "stock":
        return None if _is_missing(fy_value) else float(fy_value)
    raise ValueError(f"unknown kind {kind!r}; expected one of {VALID_KINDS}")
