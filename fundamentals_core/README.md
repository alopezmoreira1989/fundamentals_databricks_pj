# fundamentals_core

Pure-Python **shared domain library** for the project. It lives at the repo root as a
sibling of `fundamentals_pipeline/` (the Databricks processing layer) and `web/` (the
Django application layer), and is depended on by both — neither of those depends on the
other.

It carries **no** Spark / Databricks / Streamlit / Django dependency (`import math` only),
so it can be imported and unit-tested in plain CPython, on Streamlit Cloud, and inside a
Databricks notebook alike.

## Modules

| Module | Responsibility |
|---|---|
| `schemas.py` | The export ↔ app **artifact contract** (column sets + dtype families of the published parquet, required meta keys). Asserted on the export side and validated on every consumer. |
| `valuation.py` | Scalar **reference** implementations of Graham / Graham-revised / DCF / Owner-Earnings / EPS-CAGR. The PySpark column algebra in the pipeline must mirror these bit-for-bit. |
| `periods.py` | Quarterly **period arithmetic** (`Q4 = FY − YTD_Q3`). |
| `backtest.py` | As-of / no-look-ahead eligibility, predicate evaluation, CAGR / max-drawdown / volatility / Sharpe. |
| `splits.py` | Cumulative split-factor helpers for cross-year, split-adjusted metrics. |

## Why it is a standalone package

This is the project's **single source of truth** for financial logic. Extracting it to a
top-level installable package means the financial formulas, the schema contract, and the
screening bands exist in exactly one place; the pipeline and the web app both *import*
them and can never drift. No business logic or financial calculation is reimplemented in
the web layer — it only renders what this library and the published artifacts provide.

## Install (editable, for local dev / CI)

```
pip install -e .
```

(Databricks notebooks and the Streamlit app also resolve it via a repo-root `sys.path`
fallback, so an explicit install is optional in those runtimes.)
