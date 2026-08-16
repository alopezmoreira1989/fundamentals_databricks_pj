"""Write-safety guards — pure decision logic for destructive Delta write operations.

Extracted after the 2026-08-16 incident (see
docs/phase5-6-european-dashboard-data-integration.md §10): `10__ingestion/
12__fetch_market_data.py` silently overwrote the whole `market_prices_daily` table with a
caller's narrow ticker scope, and `20__transformation/22__derived_metrics.py` /
`23__intrinsic_value.py` each had a MERGE's orphan-cleanup ``DELETE`` fire against nearly the
entire table when an upstream dependency came back anomalously empty. Both notebooks are
Spark-only and not directly unit-testable (this repo's `tests/` suite is deliberately
Spark-free — see CLAUDE.md), so this module holds the pure comparison/decision core of each
guard: the notebooks fetch the real counts/sets from Spark and delegate the actual safety
judgment here.
"""

from __future__ import annotations

from collections.abc import Iterable

DEFAULT_MAX_ORPHAN_DELETE_FRACTION = 0.10


class UnsafeFullOverwriteError(Exception):
    """Raised when a caller's active ticker scope does not cover the full production universe
    but the write path is about to do a whole-table overwrite anyway.

    A whole-table overwrite is only correct when the batch being written genuinely represents
    everything the table should contain (the normal, unscoped production run). Any narrower
    scope — an explicit ticker override, or a caller pre-seeding its own subset — must use a
    scoped write (e.g. a keyed `MERGE`) instead, never a full overwrite.
    """


def assert_full_overwrite_safe(
    active_tickers: Iterable[str],
    full_universe: Iterable[str],
    benchmark_tickers: Iterable[str] = (),
) -> None:
    """Raise `UnsafeFullOverwriteError` unless every ticker in `full_universe` is covered by
    `active_tickers` (after excluding `benchmark_tickers`, which are additive market data with
    no fundamentals and never part of the production ticker universe).

    A no-op when `full_universe` is empty (nothing to protect against, e.g. a fresh/test
    environment with no `config.tickers` rows yet).
    """
    full = set(full_universe)
    active = set(active_tickers) - set(benchmark_tickers)
    if full and not full.issubset(active):
        missing = len(full - active)
        raise UnsafeFullOverwriteError(
            f"active ticker scope ({len(active)} ticker(s)) does not cover the full "
            f"production universe ({len(full)} ticker(s)) — {missing} missing from the "
            f"active scope. A full-table overwrite is unsafe for a bounded ticker scope."
        )


class UnsafeOrphanDeleteError(Exception):
    """Raised when a MERGE's orphan-cleanup DELETE would remove an implausibly large fraction
    of existing rows — a signal that an upstream dependency came back anomalously empty or
    narrow this run, not that this many rows genuinely became stale at once."""


def assert_orphan_delete_safe(
    would_delete: int,
    existing: int,
    max_fraction: float = DEFAULT_MAX_ORPHAN_DELETE_FRACTION,
) -> None:
    """Raise `UnsafeOrphanDeleteError` if `would_delete / existing` exceeds `max_fraction`.

    A no-op when `existing` is 0 (nothing to protect) — a healthy run's orphan cleanup is
    normally a small drift (occasional stale-value retirement); a bulk deletion is the anomaly
    this guard exists to catch, not the common case.
    """
    if existing > 0 and would_delete / existing > max_fraction:
        raise UnsafeOrphanDeleteError(
            f"would delete {would_delete:,} of {existing:,} existing rows "
            f"({would_delete / existing:.0%}) — exceeds the {max_fraction:.0%} safety "
            f"threshold for a single run's orphan cleanup."
        )
