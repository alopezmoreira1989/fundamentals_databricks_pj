"""Batched, date-anchored, triangulated FX-rate resolution shared by the General Screener and
Company Detail currency-lens features.

Non-negotiable date-anchoring rule (unchanged from fundamentals_pipeline/fx.py's own docstring):
every rate is looked up AS OF the figure's own observation date, never today's spot rate.
resolve_rates takes (native_currency, as_of) pairs as data, never infers a date.

The Company Detail page used to have its own bespoke per-ticker `usd_fx_rate`/`_FX_RATE_SQL`
machinery (Market Cap KPI's sole caller); both were retired once Market Cap was absorbed into
this shared engine like every other currency-denominated figure on that page — see
`CompanyRepository.resolve_currency_rates`/`services.apply_currency_lens`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass(frozen=True)
class RateKey:
    """One (native currency, observation date) pair needing a rate to some target currency."""

    native: str  # ISO-ish currency code, upper-cased
    as_of: Any  # datetime.date, from the underlying period_end column


def resolve_rates(
    con: duckdb.DuckDBPyConnection,
    keys: frozenset[RateKey],
    target_currency: str,
) -> dict[RateKey, float]:
    """The date-anchored native -> target_currency rate for every key in `keys`. A key is
    OMITTED (never fabricated) when no rate resolves at all. Direct dashboard_fx pair if one
    exists; otherwise triangulate through USD: rate(native->USD, as_of) * rate(USD->target,
    as_of), each leg independently date-anchored to the SAME as_of. native=="USD" or
    target=="USD" skips that leg (trivially 1.0) rather than probing a nonexistent USD->USD row.

    ONE batched round trip for the whole `keys` set (TEMP TABLE + executemany + 3 LEFT JOINs,
    each QUALIFY-ordered) -- never N per-key queries. Works identically whether called for a
    screener page's ~50 tickers or one company-detail page's ~15-20 distinct dates.
    """
    if not keys:
        return {}
    target = target_currency.upper()
    con.execute("DROP TABLE IF EXISTS needed_rates")
    con.execute("CREATE TEMP TABLE needed_rates (native VARCHAR, as_of DATE)")
    con.executemany(
        "INSERT INTO needed_rates VALUES (?, ?)",
        [(k.native.upper(), k.as_of) for k in keys],
    )
    sql = """
        WITH direct AS (
            SELECT n.native, n.as_of, fx.rate
            FROM needed_rates n
            LEFT JOIN dashboard_fx fx
              ON fx.base = n.native AND fx.quote = ? AND fx.date <= n.as_of
            QUALIFY row_number() OVER (PARTITION BY n.native, n.as_of ORDER BY fx.date DESC) = 1
        ),
        to_usd AS (
            SELECT n.native, n.as_of, fx.rate
            FROM needed_rates n
            LEFT JOIN dashboard_fx fx
              ON fx.base = n.native AND fx.quote = 'USD' AND fx.date <= n.as_of
            QUALIFY row_number() OVER (PARTITION BY n.native, n.as_of ORDER BY fx.date DESC) = 1
        ),
        usd_to_target AS (
            SELECT DISTINCT n.as_of, fx.rate
            FROM needed_rates n
            LEFT JOIN dashboard_fx fx
              ON fx.base = 'USD' AND fx.quote = ? AND fx.date <= n.as_of
            QUALIFY row_number() OVER (PARTITION BY n.as_of ORDER BY fx.date DESC) = 1
        )
        SELECT d.native, d.as_of, d.rate AS direct_rate,
               tu.rate AS to_usd_rate, ut.rate AS usd_to_target_rate
        FROM direct d
        LEFT JOIN to_usd tu ON tu.native = d.native AND tu.as_of = d.as_of
        LEFT JOIN usd_to_target ut ON ut.as_of = d.as_of
    """
    rows = con.execute(sql, [target, target]).fetchall()
    resolved: dict[RateKey, float] = {}
    for native, as_of, direct_rate, to_usd_rate, usd_to_target_rate in rows:
        key = RateKey(native=native, as_of=as_of)
        if direct_rate is not None:
            resolved[key] = float(direct_rate)
            continue
        leg1 = 1.0 if native == "USD" else to_usd_rate
        leg2 = 1.0 if target == "USD" else usd_to_target_rate
        if leg1 is not None and leg2 is not None:
            resolved[key] = float(leg1) * float(leg2)
        # else: no rate resolvable at all (direct or triangulated) -- key stays absent
    return resolved
