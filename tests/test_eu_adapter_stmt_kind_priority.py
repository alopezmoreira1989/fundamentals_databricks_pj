"""Proves the Phase 6.3 fix to 16__fetch_eu_xbrl.py's `_LABEL_TO_STMT_KIND` construction
(docs/phase6-3-net-income-statement-classification.md) -- a label appearing in more than one
STATEMENTS concept map must resolve deterministically to a fixed priority
(Income Statement > Balance Sheet > Cash Flow), independent of STATEMENTS' own dict/insertion
order, and NOT because that priority happens to be convenient for the 3 real Net Income
collisions specifically.

Real bug being regression-tested: the old code built `_LABEL_TO_STMT_KIND` via a dict
comprehension iterating `STATEMENTS.items()`, so the LAST statement processed for a given label
silently won. Because Python's `STATEMENTS = {"Income Statement": ..., "Balance Sheet": ...,
"Cash Flow": ...}` insertion order put Cash Flow last, every collision (all 3 in production
data are in the Net Income family) resolved to "Cash Flow" -- wrong, since Cash Flow is
structurally a reconciliation statement that reuses figures computed elsewhere, never their
primary source.

Every test here loads a FRESH copy of 16__fetch_eu_xbrl.py (via
test_eu_adapter_protocol_conformance._load_eu_adapter_module, RUN_EU_PILOT=False) with a
pre-seeded, synthetic STATEMENTS dict -- so these tests exercise the real production code path
of the fix, not a reimplementation of it.
"""

from __future__ import annotations

from test_eu_adapter_protocol_conformance import _load_eu_adapter_module

# The 3 real collisions in production STATEMENTS (fundamentals_pipeline/00__config/01__tickers.py
# lines 87-89 / 181-183) -- defined identically (same XBRL concept, same `kind`) in both
# INCOME_STATEMENT and CASH_FLOW. Hardcoded here (not imported -- 01__tickers.py is a
# Databricks-notebook-only config file with %run/dbutils module-level dependencies that can't be
# imported outside a notebook session) so this test is anchored to real production data, not an
# invented example.
_REAL_NET_INCOME_COLLISIONS = {
    "Net Income": ("NetIncomeLoss", "flow_additive"),
    "Net Income (to common)": ("NetIncomeLossAvailableToCommonStockholdersBasic", "flow_additive"),
    "Net Income (incl NCI)": ("ProfitLoss", "flow_additive"),
}


def _statements_fixture(*, reversed_outer=False, reversed_inner=False):
    """Builds a synthetic STATEMENTS dict exercising every priority pairing: a 3-way collision
    (Net Income family, Income Statement+Balance Sheet+Cash Flow), a 2-way Income-Statement-vs-
    Cash-Flow collision unrelated to Net Income (Interest Expense), a 2-way Balance-Sheet-vs-
    Cash-Flow collision (Cash and Equivalents -- a Balance Sheet stock concept that also appears
    as the Cash Flow statement's ending-balance line), plus one label unique to each statement
    (Revenue / Total Assets / Operating Cash Flow) to prove non-colliding labels are unaffected.

    ``reversed_outer``/``reversed_inner`` rebuild the same fixture with the outer STATEMENTS
    dict's key order reversed, and/or each inner concept-map's key order reversed -- used to
    prove the result is independent of insertion order at both levels.
    """
    income_statement = {
        "Net Income": _REAL_NET_INCOME_COLLISIONS["Net Income"],
        "Net Income (to common)": _REAL_NET_INCOME_COLLISIONS["Net Income (to common)"],
        "Net Income (incl NCI)": _REAL_NET_INCOME_COLLISIONS["Net Income (incl NCI)"],
        "Revenue": ("Revenues", "flow_additive"),
        "Interest Expense": ("InterestExpense", "flow_additive"),
    }
    balance_sheet = {
        "Net Income": _REAL_NET_INCOME_COLLISIONS["Net Income"],  # 3-way: also on the Balance Sheet
        "Total Assets": ("Assets", "stock"),
        "Cash and Equivalents": ("CashAndCashEquivalentsAtCarryingValue", "stock"),
    }
    cash_flow = {
        "Net Income": _REAL_NET_INCOME_COLLISIONS["Net Income"],
        "Net Income (to common)": _REAL_NET_INCOME_COLLISIONS["Net Income (to common)"],
        "Net Income (incl NCI)": _REAL_NET_INCOME_COLLISIONS["Net Income (incl NCI)"],
        "Interest Expense": ("InterestPaid", "flow_additive"),
        "Cash and Equivalents": ("CashAndCashEquivalentsPeriodIncreaseDecrease", "flow_additive"),
        "Operating Cash Flow": ("NetCashProvidedByUsedInOperatingActivities", "flow_additive"),
    }

    if reversed_inner:
        income_statement = dict(reversed(income_statement.items()))
        balance_sheet = dict(reversed(balance_sheet.items()))
        cash_flow = dict(reversed(cash_flow.items()))

    statements = {
        "Income Statement": income_statement,
        "Balance Sheet": balance_sheet,
        "Cash Flow": cash_flow,
    }
    if reversed_outer:
        statements = dict(reversed(statements.items()))
    return statements


def test_real_net_income_collisions_resolve_to_income_statement():
    """The 3 real production collisions must all resolve to Income Statement, not Cash Flow --
    the exact bug (Owner Earnings silently zeroed, wrong consolidated-vs-attributable Net Income
    value winning) documented in docs/phase6-3-net-income-statement-classification.md."""
    module = _load_eu_adapter_module(statements=_statements_fixture())

    for label, (_xbrl_concept, kind) in _REAL_NET_INCOME_COLLISIONS.items():
        assert module._LABEL_TO_STMT_KIND[label] == ("Income Statement", kind), label


def test_non_colliding_labels_keep_their_only_statement():
    """A label that appears in exactly one concept map must resolve to that statement,
    regardless of the collision-priority machinery -- proves the fix doesn't over-apply."""
    module = _load_eu_adapter_module(statements=_statements_fixture())

    assert module._LABEL_TO_STMT_KIND["Revenue"] == ("Income Statement", "flow_additive")
    assert module._LABEL_TO_STMT_KIND["Total Assets"] == ("Balance Sheet", "stock")
    assert module._LABEL_TO_STMT_KIND["Operating Cash Flow"] == (
        "Cash Flow",
        "flow_additive",
    )


def test_priority_generalizes_beyond_income_statement_vs_cash_flow():
    """The Net Income family only ever exercises Income Statement > Cash Flow. This proves the
    OTHER two pairings the priority rule claims to cover also hold with a real synthetic
    collision each -- i.e. the rule is a genuine 3-tier ordering, not a special case that happens
    to fix Net Income and was merely asserted (not demonstrated) to generalize:

    - Income Statement > Balance Sheet: "Net Income" (3-way collision) still resolves to Income
      Statement even though it's also defined on the Balance Sheet in this fixture (never true in
      real production data -- deliberately synthetic, to isolate this specific pairing).
    - Balance Sheet > Cash Flow: "Cash and Equivalents" is a genuine real-world ambiguity (a
      Balance Sheet stock balance that also legitimately appears as the Cash Flow statement's
      ending-balance reconciliation line) and must resolve to Balance Sheet, its true primary
      source, not Cash Flow.
    - Income Statement > Cash Flow directly, for a non-Net-Income label: "Interest Expense" (an
      Income Statement P&L line that Cash Flow also reconciles) must resolve to Income Statement.
    """
    module = _load_eu_adapter_module(statements=_statements_fixture())

    assert module._LABEL_TO_STMT_KIND["Net Income"][0] == "Income Statement"
    assert module._LABEL_TO_STMT_KIND["Cash and Equivalents"] == ("Balance Sheet", "stock")
    assert module._LABEL_TO_STMT_KIND["Interest Expense"] == ("Income Statement", "flow_additive")


def test_result_is_independent_of_statements_dict_insertion_order():
    """Direct proof of the fix's core claim: the old code's bug was that the result depended on
    STATEMENTS' dict/insertion order (whichever statement was processed LAST for a given label
    won). Rebuilding the identical fixture with the outer STATEMENTS dict reversed, the inner
    concept-map dicts reversed, and both reversed together must produce the exact same
    _LABEL_TO_STMT_KIND -- if the fix regressed to insertion-order-dependence, at least one of
    these three variants would silently disagree with the baseline.
    """
    baseline = _load_eu_adapter_module(statements=_statements_fixture())._LABEL_TO_STMT_KIND

    outer_reversed = _load_eu_adapter_module(
        statements=_statements_fixture(reversed_outer=True)
    )._LABEL_TO_STMT_KIND
    inner_reversed = _load_eu_adapter_module(
        statements=_statements_fixture(reversed_inner=True)
    )._LABEL_TO_STMT_KIND
    both_reversed = _load_eu_adapter_module(
        statements=_statements_fixture(reversed_outer=True, reversed_inner=True)
    )._LABEL_TO_STMT_KIND

    assert outer_reversed == baseline
    assert inner_reversed == baseline
    assert both_reversed == baseline


def test_missing_statements_global_yields_empty_map_not_a_crash():
    """Outside Databricks with no STATEMENTS pre-seeded at all (the existing conformance test's
    default _load_eu_adapter_module() call, no `statements=` kwarg), the defensive
    globals().get("STATEMENTS", {}) fallback must still produce an empty, harmless map -- this
    fix must not introduce a hard dependency on STATEMENTS being present."""
    module = _load_eu_adapter_module()  # no statements= kwarg -- mirrors the real notebook default

    assert module._LABEL_TO_STMT_KIND == {}
