"""Write-safety guards — pure decision logic for destructive Delta write operations.

Extracted after the 2026-08-16 incident (see
docs/phase5-6-european-dashboard-data-integration.md §10): `10__ingestion/
12__fetch_market_data.py` silently overwrote the whole `market_prices_daily` table with a
caller's narrow ticker scope, and `20__transformation/22__derived_metrics.py` /
`23__intrinsic_value.py` each had a MERGE's orphan-cleanup ``DELETE`` (and, in `23`'s case, a
plain ``UPDATE``) fire against nearly the entire table when an upstream dependency
(`market_prices_daily`) came back anomalously empty — cascading into `market_cap_asof` and
`market_cap_live` (both zeroed by an unconditional full overwrite) too. All three notebooks
are Spark-only and not directly unit-testable (this repo's `tests/` suite is deliberately
Spark-free — see CLAUDE.md), so this module holds the pure comparison/decision core of each
guard: the notebooks fetch the real counts/sets from Spark and delegate the actual safety
judgment here.

**Primary vs. secondary defense** (per the 2026-08-17 hardening pass): `is_full_universe_run`
is the PRIMARY guard — a semantic check answering "does this run's upstream input actually
represent the full universe," evaluated BEFORE any destructive write is attempted, so a
degraded input never reaches a `DELETE`/`UPDATE`/overwrite in the first place.
`assert_orphan_delete_safe`'s percentage threshold is a SECONDARY, defense-in-depth check on
the delete-based MERGEs only — useful for a failure mode `is_full_universe_run` doesn't cover
(a genuine full-universe run whose OWN logic produces an implausibly large orphan set for some
unrelated reason), but deliberately not the primary mechanism: a percentage threshold set too
loose still permits a real catastrophe (90,000-of-100,000 rows destroyed is still a
catastrophe at just under a 10% threshold), where a semantic "was this run even looking at the
full universe" check does not have that failure mode.
"""

from __future__ import annotations

from collections.abc import Iterable

DEFAULT_MAX_ORPHAN_DELETE_FRACTION = 0.10
DEFAULT_MIN_UNIVERSE_COVERAGE = 0.90


def is_full_universe_run(source_ticker_count: int, reference_ticker_count: int,
                          min_coverage: float = DEFAULT_MIN_UNIVERSE_COVERAGE) -> bool:
    """True if `source_ticker_count` covers at least `min_coverage` of `reference_ticker_count`
    — the semantic "is this a full-universe run" signal, computed from real ticker coverage
    rather than assumed from a flag a caller could forget to set correctly.

    This is deliberately NOT a caller-supplied boolean (a `FULL_UNIVERSE_RUN = True/False`
    flag a notebook sets by hand would have exactly the same "silently wrong" failure mode the
    2026-08-16 incident already demonstrated with `force_full_refresh` — a flag whose meaning
    the caller misunderstood). Instead the caller measures its own actual input (e.g. how many
    distinct tickers `market_prices_daily` covers) and the reference universe it should cover
    (e.g. how many distinct tickers `financials` has), and this function turns that comparison
    into the yes/no signal every guard in this module keys off of.

    A no-op — always `True` — when `reference_ticker_count` is 0 (nothing to compare against,
    e.g. a fresh/test environment).
    """
    if reference_ticker_count <= 0:
        return True
    return source_ticker_count / reference_ticker_count >= min_coverage


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


def scoped_orphan_keys(orphan_keys: Iterable[tuple], owned_metrics: Iterable[str]) -> set[tuple]:
    """Filter `orphan_keys` — `(ticker, fiscal_year, metric)` tuples present in a target table
    but absent from a fresh run's own source — down to the subset this caller actually OWNS
    (metric, the 3rd tuple element, is in `owned_metrics`).

    2026-08-17 (docs/phase5-6-european-dashboard-data-integration.md §14): `financials_metrics`
    is jointly populated by `22__derived_metrics.py` (its own base/valuation metrics) and
    `23__intrinsic_value.py` (its own intrinsic-value labels), each via an independent MERGE
    into the SAME table. A metric absent from `22`'s own freshly-computed source is only a
    genuine orphan if `22` is the one who owns it — a `23`-owned label being absent from `22`'s
    source is not evidence of anything; `22` never produces those rows in the first place. This
    is what a real dry-run (2026-08-17) found: of 474,698 raw "orphans," 100% belonged to `23`'s
    own metric vocabulary, none to `22`'s.

    Deliberately an ALLOW-list (only delete what I positively know I own), not a deny-list
    (delete everything except what I know belongs to some other specific owner) — this caller
    only needs to know its OWN vocabulary, not enumerate every other current or future owner of
    rows in the same table.
    """
    owned = set(owned_metrics)
    return {k for k in orphan_keys if k[2] in owned}


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
