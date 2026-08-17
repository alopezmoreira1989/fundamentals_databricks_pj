"""Phase 6.5c: `Total Equity (incl NCI)` dual-row + fallback (docs/phase6-5-total-equity-nci-
normalization-fix.md). The real fix lives in `21__clean_and_merge.py`'s §2/§2b, expressed as
PySpark DataFrame operations that require a live Spark session -- this repo has no local
pyspark install and no Spark CI (CLAUDE.md's own documented convention: "notebooks are still
validated ad-hoc / via `30__analysis` checks"), and `21__clean_and_merge.py` itself executes
`spark.sql(...)`/`spark.table(...)` at MODULE level (confirmed: it cannot be exec'd outside a
Databricks session at all, unlike `16__fetch_eu_xbrl.py`'s conditionally-gated design), so it
cannot be imported and driven directly the way `test_eu_adapter_stmt_kind_priority.py` does for
the EU adapter.

This file instead unit-tests a pure-Python REFERENCE implementation (`_apply_fallback` below)
that mirrors the real Spark logic's algorithm exactly:

    _direct_equity_keys = clean_fy.filter(concept == "Total Stockholders Equity")
                                   .select(ticker, stmt, fiscal_year).distinct()
    _equity_fallback_rows = clean_fy.filter(concept == "Total Equity (incl NCI)")
                                     .join(_direct_equity_keys, on=[ticker, stmt, fiscal_year],
                                           how="left_anti")
                                     .withColumn(concept, "Total Stockholders Equity")
                                     .withColumn(is_derived, True)
    clean_fy = clean_fy.unionByName(_equity_fallback_rows)

proving the ALGORITHM's keying/precedence/idempotency semantics are correct in isolation. It is
not a substitute for real-data validation against the actual Spark code (see
docs/phase6-5-total-equity-nci-normalization-fix.md §12 for that evidence) -- it is a spec test
for the logic this file's own Spark code must match.
"""

from __future__ import annotations

import copy


def _apply_fallback(rows: list[dict]) -> list[dict]:
    """Pure-Python mirror of 21__clean_and_merge.py's §2b. `rows` is `clean_fy`-shaped: each a
    dict with at least ticker/stmt/concept/fiscal_year/value/is_derived. Returns the union of the
    input rows plus any synthesized "Total Stockholders Equity" fallback rows -- never mutates
    or removes an existing row.
    """
    direct_keys = {
        (r["ticker"], r["stmt"], r["fiscal_year"])
        for r in rows
        if r["concept"] == "Total Stockholders Equity"
    }
    fallback_rows = []
    for r in rows:
        if r["concept"] != "Total Equity (incl NCI)":
            continue
        key = (r["ticker"], r["stmt"], r["fiscal_year"])
        if key in direct_keys:
            continue
        fb = copy.deepcopy(r)
        fb["concept"] = "Total Stockholders Equity"
        fb["is_derived"] = True
        fallback_rows.append(fb)
    return rows + fallback_rows


def _row(ticker, stmt, concept, fy, value, is_derived=False):
    return {
        "ticker": ticker, "stmt": stmt, "concept": concept, "fiscal_year": fy,
        "value": value, "is_derived": is_derived,
    }


def test_both_concepts_present_both_survive_direct_wins_its_slot():
    rows = [
        _row("CAT", "Balance Sheet", "Total Stockholders Equity", 2024, 100.0),
        _row("CAT", "Balance Sheet", "Total Equity (incl NCI)", 2024, 110.0),
    ]
    out = _apply_fallback(rows)

    direct = [r for r in out if r["concept"] == "Total Stockholders Equity"]
    incl_nci = [r for r in out if r["concept"] == "Total Equity (incl NCI)"]
    assert len(direct) == 1 and direct[0]["value"] == 100.0 and direct[0]["is_derived"] is False
    assert len(incl_nci) == 1 and incl_nci[0]["value"] == 110.0
    assert len(out) == 2  # no synthesized fallback row -- direct already exists


def test_only_incl_nci_produces_both_rows_fallback_matches_legacy_value():
    """The CAT/T/VZ/PG/ADM shape: real production data has ALWAYS shown the incl-NCI value
    under "Total Stockholders Equity" for these tickers (docs/phase6-5..., §B2) -- the fallback
    row's value must equal that pre-existing behavior exactly, not a new/different number."""
    rows = [_row("CAT", "Balance Sheet", "Total Equity (incl NCI)", 2025, 21318000000.0)]
    out = _apply_fallback(rows)

    assert len(out) == 2
    incl_nci = next(r for r in out if r["concept"] == "Total Equity (incl NCI)")
    fallback = next(r for r in out if r["concept"] == "Total Stockholders Equity")
    assert incl_nci["value"] == 21318000000.0
    assert fallback["value"] == 21318000000.0  # legacy value preserved exactly
    assert fallback["is_derived"] is True       # marked as a substitution, not a real fact


def test_only_direct_produces_only_direct_row():
    rows = [_row("AAPL", "Balance Sheet", "Total Stockholders Equity", 2024, 500.0)]
    out = _apply_fallback(rows)

    assert out == rows  # unchanged -- no synthetic "Total Equity (incl NCI)" is ever invented


def test_neither_produces_no_row():
    assert _apply_fallback([]) == []


def test_direct_and_broad_differ_both_values_preserved_distinctly():
    rows = [
        _row("VNOM", "Balance Sheet", "Total Stockholders Equity", 2024, 359245.0),
        _row("VNOM", "Balance Sheet", "Total Equity (incl NCI)", 2024, 603646.0),
    ]
    out = _apply_fallback(rows)

    direct = next(r for r in out if r["concept"] == "Total Stockholders Equity")
    incl_nci = next(r for r in out if r["concept"] == "Total Equity (incl NCI)")
    assert direct["value"] == 359245.0
    assert incl_nci["value"] == 603646.0
    assert direct["value"] != incl_nci["value"]


def test_fallback_is_evaluated_per_ticker_and_fiscal_year():
    """Same ticker, one fy has a direct fact, the other doesn't -- the fallback must fire ONLY
    for the fy genuinely missing it, not for both, and not for neither."""
    rows = [
        _row("FCC", "Balance Sheet", "Total Stockholders Equity", 2024, 200.0),
        _row("FCC", "Balance Sheet", "Total Equity (incl NCI)", 2024, 250.0),
        _row("FCC", "Balance Sheet", "Total Equity (incl NCI)", 2020, 90.0),
        # 2020 has NO direct Total Stockholders Equity fact.
    ]
    out = _apply_fallback(rows)

    fy2024_direct = [r for r in out if r["fiscal_year"] == 2024 and r["concept"] == "Total Stockholders Equity"]
    fy2020_direct = [r for r in out if r["fiscal_year"] == 2020 and r["concept"] == "Total Stockholders Equity"]
    assert len(fy2024_direct) == 1 and fy2024_direct[0]["value"] == 200.0 and fy2024_direct[0]["is_derived"] is False
    assert len(fy2020_direct) == 1 and fy2020_direct[0]["value"] == 90.0 and fy2020_direct[0]["is_derived"] is True


def test_no_accidental_cross_statement_fallback():
    """A direct fact under one stmt must never suppress the fallback for the SAME ticker/fy
    under a DIFFERENT stmt (the key is (ticker, stmt, fiscal_year), not (ticker, fiscal_year))."""
    rows = [
        _row("XYZ", "Balance Sheet", "Total Equity (incl NCI)", 2024, 42.0),
        _row("XYZ", "Cash Flow", "Total Stockholders Equity", 2024, 999.0),  # different stmt
    ]
    out = _apply_fallback(rows)

    bs_direct = [r for r in out if r["stmt"] == "Balance Sheet" and r["concept"] == "Total Stockholders Equity"]
    assert len(bs_direct) == 1 and bs_direct[0]["value"] == 42.0 and bs_direct[0]["is_derived"] is True


def test_idempotent_running_twice_produces_the_same_result():
    rows = [
        _row("CAT", "Balance Sheet", "Total Equity (incl NCI)", 2025, 21318000000.0),
        _row("FCC", "Balance Sheet", "Total Stockholders Equity", 2024, 2732716000.0),
        _row("FCC", "Balance Sheet", "Total Equity (incl NCI)", 2024, 3736019000.0),
    ]
    once = _apply_fallback(rows)
    # Re-running on a FRESH `clean_fy`-shaped input (as the real notebook does every run --
    # `clean_fy` is always recomputed from `raw`, never from the previous run's own output) must
    # reproduce the identical set of rows, not double the fallback rows or oscillate labels.
    twice = _apply_fallback(rows)

    def key(r):
        return (r["ticker"], r["stmt"], r["concept"], r["fiscal_year"])

    assert sorted(once, key=key) == sorted(twice, key=key)
    # No duplicate canonical keys within a single run's output.
    keys = [key(r) for r in once]
    assert len(keys) == len(set(keys))


def test_never_overwrites_a_direct_fact_even_when_broad_value_is_larger():
    """Guards against the exact failure mode this phase exists to prevent: the broader concept
    must never win the "Total Stockholders Equity" slot merely because dedup/tiebreak logic
    might otherwise prefer a larger value -- the fallback path here is only ever reached via
    left-anti join (key absent), never a value-based tiebreak."""
    rows = [
        _row("MS", "Balance Sheet", "Total Stockholders Equity", 2024, 50.0),
        _row("MS", "Balance Sheet", "Total Equity (incl NCI)", 2024, 5000.0),  # much larger
    ]
    out = _apply_fallback(rows)

    direct = next(r for r in out if r["concept"] == "Total Stockholders Equity")
    assert direct["value"] == 50.0
    assert direct["is_derived"] is False
