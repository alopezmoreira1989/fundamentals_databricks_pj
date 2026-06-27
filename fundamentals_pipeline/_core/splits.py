"""Scalar reference for cumulative stock-split adjustment factors.

CONTRACT: mirrors the per-period split factor computed in Spark by
``20_transformation/22__derived_metrics.py`` (Net Buyback Yield % / Piotroski no-dilution)
and ``20_transformation/23__intrinsic_value.py`` (the trailing EPS-CAGR growth input). Those
two notebooks must produce the SAME factor as this function for a given ``(period_end, splits)``.

A per-share figure (EPS, weighted-average share count) reported for a fiscal period ending at
``period_end`` is on the split basis IN EFFECT AT THAT TIME. To compare such figures ACROSS
periods (a YoY share change, a multi-year EPS CAGR) they must be rescaled to a single CURRENT
basis. The cumulative factor is the product of every split ratio whose ex-date falls AFTER
``period_end``::

    factor = ∏ ratio  for each split with ex_date > period_end

Then ``EPS_adj = EPS / factor`` and ``Shares_adj = Shares × factor``; totals such as Net Income
are never rescaled. A period after all splits has an empty product → factor ``1.0``, so the most
recent period is left unchanged and already on the live-price basis. Reverse splits (ratio < 1,
e.g. a 1-for-10 → ``0.1``) fall out of the same product.

The ex-date comparison is STRICT (``>``): a split dated exactly on ``period_end`` already affected
that period's reported per-share figure, so it must NOT be applied again.

This module deliberately does NOT touch the historical market cap, which uses the raw close ×
raw share count both as-of ``period_end`` (period-consistent) — see ``22__derived_metrics``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

Split = tuple[date, float]


def cumulative_split_factor(period_end: date, splits: Iterable[Split]) -> float:
    """Cumulative split factor that rescales a period's per-share figure to the current basis.

    ``splits`` is any iterable of ``(ex_date, ratio)`` pairs for the ticker (order irrelevant).
    Only splits with ``ex_date > period_end`` contribute; a non-positive or falsy ratio is
    skipped as bad data. Empty (or all-prior) → ``1.0``.
    """
    factor = 1.0
    for ex_date, ratio in splits:
        if ratio and ratio > 0 and ex_date > period_end:
            factor *= float(ratio)
    return factor
