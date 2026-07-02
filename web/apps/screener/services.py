"""Screener application service — coordinates the screener repository."""

from __future__ import annotations

from repositories.dtos import ScreenRow
from repositories.screener import ScreenerRepository


def run_screen(
    *,
    metric: str,
    min_value: float | None = None,
    max_value: float | None = None,
    limit: int = 50,
) -> tuple[ScreenRow, ...]:
    """Latest-FY screen of ``metric`` within optional inclusive ``[min_value, max_value]``."""
    return ScreenerRepository().screen(
        metric=metric, min_value=min_value, max_value=max_value, limit=limit
    )
