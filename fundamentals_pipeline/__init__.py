"""fundamentals_pipeline — the project's installable Python package and public API.

This package is the single source of truth for the project's financial logic and the
pipeline's data contract. Every environment consumes it identically — the Django web app,
Databricks notebooks, scripts, and the test suite — via a standard editable install
(``pip install -e .``); no consumer manipulates ``sys.path``.

Importable public modules (pure Python, ``import math`` only — no Spark / Databricks /
Streamlit / Django dependency, so they unit-test in plain CPython):

  * ``schemas``   — the export ↔ app artifact contract
  * ``valuation`` — scalar Graham / Graham-revised / DCF / Owner-Earnings / EPS-CAGR refs
  * ``periods``   — quarterly period arithmetic (Q4 = FY − YTD_Q3)
  * ``backtest``  — as-of eligibility, predicate eval, CAGR / drawdown / vol / Sharpe
  * ``splits``    — cumulative split-factor helpers

The ``NN__`` directories that live alongside these modules (``00__config`` …
``90__pipelines``, ``60__frontends``) are Databricks notebook sources, **not** importable
sub-packages — their digit-prefixed names are deliberately not valid Python module names.
The reference scalars here mirror the PySpark column algebra in those notebooks
(``20__transformation/23__intrinsic_value.py``, ``22__derived_metrics.py``) bit-for-bit, so
the formulas stay testable in isolation; the web app imports them as a public API and never
reimplements them.
"""

from .backtest import (
    annualized_vol,
    as_of_date,
    as_of_eligible,
    cagr,
    max_drawdown,
    passes_predicates,
    sharpe,
)
from .periods import q4_from_fy_ytd
from .valuation import (
    dcf_value,
    eps_cagr,
    graham_number,
    graham_revised,
    owner_earnings,
    safe_div,
)

__all__ = [
    "safe_div",
    "graham_number",
    "graham_revised",
    "dcf_value",
    "owner_earnings",
    "eps_cagr",
    "q4_from_fy_ytd",
    "as_of_date",
    "as_of_eligible",
    "passes_predicates",
    "cagr",
    "max_drawdown",
    "annualized_vol",
    "sharpe",
]
