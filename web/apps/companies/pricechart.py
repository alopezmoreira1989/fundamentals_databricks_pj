"""Presentation helper: headline price stats (last/min/max close) for the Price tab's KPI row.

Pure layout math (no I/O, no financial logic). The visual chart itself (line + SMAs + axis +
legend) is :func:`apps.companies.charts.price_line_chart` — this module now only derives the
scalar figures shown above/below it. Kept out of the view/service so it stays unit-testable —
same split as :mod:`apps.valuation.football`.
"""

from __future__ import annotations

from dataclasses import dataclass

from repositories.dtos import PricePoint


@dataclass(frozen=True, slots=True)
class PriceChart:
    last_close: float
    min_close: float
    max_close: float
    first_date: str
    last_date: str
    up: bool  # last close >= first close → the range closed higher


def build_price_chart(series: tuple[PricePoint, ...]) -> PriceChart | None:
    """Headline stats for ``series`` (raw close, not split-adjusted — matches the "latest
    quotable price" the KPI row shows), or ``None`` for fewer than two points."""
    if len(series) < 2:
        return None
    closes = [p.close for p in series]
    return PriceChart(
        last_close=series[-1].close,
        min_close=min(closes),
        max_close=max(closes),
        first_date=series[0].date,
        last_date=series[-1].date,
        up=series[-1].close >= series[0].close,
    )
