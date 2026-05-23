# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Databricks analytical pipeline that ingests SEC EDGAR XBRL filings (10-K/10-Q) for ~3,000 US tickers, joins Yahoo Finance year-end prices, derives financial metrics, and serves them via Delta tables to a Databricks dashboard. Entry point: `90_pipelines/91__full_pipeline.ipynb` (run as a Databricks Job). Code is bilingual — most prose, metric labels, and JSON hierarchies are in Spanish.

## Conventions that must be preserved

- **File naming `NN__name.py` is mandatory.** Files use `<stage><order>__<purpose>` so the pipeline order is visible from filenames alone (e.g., `21__clean_and_merge.py`, `21b__derive_quarterly.py`). New files must follow the pattern.
- **Spanish naming is intentional.** Concepts, metric labels, variable names, and comments in `00_config/*.json` (`concept_hierarchy.json`, `metrics_hierarchy.json`, `favorites.json`) and most module docs are in Spanish. Keep new code in the same style — do not translate existing identifiers.
- **`Q4 = FY − YTD_Q3` is intentional, not a bug.** `20_transformation/21b__derive_quarterly.py` derives Q4 by subtracting the YTD-Q3 figure from the annual FY to capture year-end audit adjustments. Do not "fix" this to `Q1+Q2+Q3+Q4`. Q1–Q3 use standalone SEC reports or YTD deltas based on each concept's `kind` (flow_additive / flow_nonadditive / stock).
- **Balance Sheet dedup**: SEC re-reports prior snapshots in later 10-Qs. Dedup by `(ticker, concept, period_end)` keeping the latest `filed` — preserve this when touching ingestion/merge.
- **`market_data.fiscal_year` is calendar year**, not fiscal — yfinance returns calendar year-end. Tickers like AAPL/Sep, MSFT/Jun, WMT/Jan have a 0–11 month offset.

## Operational gotchas

- **SEC User-Agent must be set before running ingestion.** `00_config/01__tickers.py` ships with placeholder `"MyCompany myemail@example.com"`. SEC blocks requests without a real org/email. Flag this if you see it unchanged when working near ingestion.
- **Unity Catalog schemas are pre-provisioned.** Code reads/writes `main.financials` and `main.config`; it does NOT create catalog or schema. Don't add `CREATE CATALOG` / `CREATE SCHEMA` statements — assume they exist.
- **`%run` and `dbutils` only work inside Databricks.** Notebooks pull config via `%run "/Workspace/.../01__tickers"`. Flag any change that introduces these in a `.py` that's expected to run locally via Databricks Connect.
- **No catalog/schema CREATE, no test suite, no lint config currently.** Validation today is ad-hoc via `30_analysis/31__company_analysis.py`.

## Workflow

- **Plan before editing notebooks or pipeline `.py` files.** They are stateful and side-effecting (writes to Delta tables, calls SEC/yfinance APIs). Outline the change first, then implement.
- **Flag Databricks-only assumptions** explicitly when proposing changes (uses `dbutils`, `%run`, `spark`, Unity Catalog three-part names, etc.) so the user knows what will break locally.
- **Run from `91__full_pipeline.ipynb`** as a Databricks Job; it accepts `tickers_override`, `run_optimization`, `rebuild_config`. Local smoke test for Databricks Connect credentials is `test_connection.py` (gitignored).

## Layout

- `00_config/` — tickers list, XBRL concept map, metric hierarchies, master-table builders
- `10_ingestion/` — parallel SEC (8-worker, rate-limited) and yfinance fetch
- `20_transformation/` — annual merge, quarterly derivation, pruning, derived metrics
- `30_analysis/` — ad-hoc validation queries
- `40_dashboards/` — dashboard SQL and `.lvdash.json`
- `90_pipelines/91__full_pipeline.ipynb` — orchestration entry point