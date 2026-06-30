"""Pure-Python reference implementations of the pipeline's financial logic.

This is a standalone **shared library** living at the repo root (a sibling of
``fundamentals_pipeline/`` and ``web/``), not a pipeline step — so it is intentionally
exempt from the ``NN__name.py`` stage-order filename convention used inside
``fundamentals_pipeline/``. It carries no Spark / pandas / Databricks / Django dependency
(``import math`` only) so it can be imported and unit-tested in plain CPython, and reused
by both the Databricks pipeline and the Django web app without either depending on the other.

The functions here are the *reference* implementations: the PySpark/numpy column
algebra in ``20__transformation/23__intrinsic_value.py`` and ``22__derived_metrics.py``
must mirror these scalar definitions bit-for-bit. The scalar versions exist so the
formulas are testable in isolation and so the PART 5 backtester can reuse them.
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
