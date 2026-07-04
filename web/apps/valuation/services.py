"""Valuation application service — coordinates the valuation repository.

The MoS values are precomputed by ``fundamentals_pipeline`` upstream, so this use case is a
straight read; when a future use case needs live valuation math, it calls
``fundamentals_pipeline`` here — never the repository.
"""

from __future__ import annotations

from repositories.dtos import FootballField, MetricPoint, MosScenario
from repositories.valuation import ValuationRepository


def get_margin_of_safety(ticker: str) -> tuple[MetricPoint, ...]:
    """Latest Margin-of-Safety metrics for the ticker (empty if none/unknown)."""
    return ValuationRepository().margin_of_safety(ticker)


def get_margin_of_safety_scenarios(ticker: str) -> tuple[MosScenario, ...]:
    """MoS organized per (method, basis) with Bear / Mid / Bull columns (empty if none)."""
    return ValuationRepository().margin_of_safety_scenarios(ticker)


def get_intrinsic_value_field(ticker: str) -> FootballField:
    """Per-method TTM intrinsic-value ranges + market price (the football field)."""
    return ValuationRepository().intrinsic_value_field(ticker)
