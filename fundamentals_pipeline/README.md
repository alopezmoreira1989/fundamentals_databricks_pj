# fundamentals_pipeline

The project's **installable Python package** and the single source of truth for its
financial logic and data contract. Every environment consumes it identically — the Django
web app, Databricks notebooks, scripts, and the test suite — via a standard editable
install (`pip install -e .` from the repo root); **no consumer manipulates `sys.path`.**

## Two things live under this package

1. **Importable public modules** (top level of the package) — pure Python, `import math`
   only, no Spark / Databricks / Streamlit / Django dependency, so they unit-test in plain
   CPython:

   | Module | Responsibility |
   |---|---|
   | `schemas.py` | The export ↔ app **artifact contract** (column sets + dtype families of the published parquet, required meta keys). Asserted on the export side, validated on every consumer. |
   | `valuation.py` | Scalar **reference** Graham / Graham-revised / DCF / Owner-Earnings / EPS-CAGR. The PySpark column algebra in the pipeline mirrors these bit-for-bit. |
   | `periods.py` | Quarterly **period arithmetic** (`Q4 = FY − YTD_Q3`). |
   | `backtest.py` | As-of / no-look-ahead eligibility, predicate evaluation, CAGR / max-drawdown / volatility / Sharpe. |
   | `splits.py` | Cumulative split-factor helpers for cross-year, split-adjusted metrics. |

2. **Databricks notebook stages** — the `NN__` subdirectories (`00__config/` …
   `90__pipelines/`, `60__frontends/`). These are Databricks notebook *sources*, **not**
   importable sub-packages: their digit-prefixed names are deliberately not valid Python
   module names, so `import fundamentals_pipeline.00__config` is impossible by design.

## Install (every environment, the same way)

```
pip install -e .
```

- **Local dev / CI / tests** — `requirements-dev.txt` runs `-e .`.
- **Streamlit Cloud** — `requirements.txt` runs `-e .`.
- **Databricks** — installed once in `90__pipelines/91__full_pipeline` session-dependencies
  `%pip` cell; the `%run`-included notebooks (`51`, `71`) then import it as a normal package.
- **Django (Phase 1+)** — declares `fundamentals_pipeline` as a dependency and imports the
  public modules above. The web layer renders what these modules return and **never
  reimplements** any financial logic.

## Why one package

This is the project's **single source of truth** for financial logic. Keeping the formulas,
the schema contract, and (later) the screening bands in exactly one installable place means
the pipeline and the web app both *import* them and can never drift.
