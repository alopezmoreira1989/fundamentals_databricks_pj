"""Valuation application service — coordinates the valuation repository.

The MoS values are precomputed by ``fundamentals_pipeline`` upstream, so this use case is a
straight read; when a future use case needs live valuation math, it calls
``fundamentals_pipeline`` here — never the repository.
"""

from __future__ import annotations

from repositories.dtos import MetricPoint
from repositories.valuation import ValuationRepository


def get_margin_of_safety(ticker: str) -> tuple[MetricPoint, ...]:
    """Latest Margin-of-Safety metrics for the ticker (empty if none/unknown)."""
    return ValuationRepository().margin_of_safety(ticker)
