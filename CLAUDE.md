# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Databricks analytical pipeline that ingests SEC EDGAR XBRL filings (10-K/10-Q) for ~3,000 US tickers, joins Yahoo Finance year-end prices, derives financial metrics, and serves them via Delta tables to a Databricks dashboard. Entry point: `90_pipelines/91__full_pipeline.py` (Databricks notebook source format, run as a Databricks Job). **All new work is produced in English** (docs, code, identifiers, comments, commit messages); some legacy content (prose, metric labels, JSON hierarchy values) is still in Spanish and is treated as data — see Conventions.

## Conventions that must be preserved

- **File naming `NN__name.py` is mandatory.** Files use `<stage><order>__<purpose>` so the pipeline order is visible from filenames alone (e.g., `21__clean_and_merge.py`, `21b__derive_quarterly.py`). New files must follow the pattern.
- **English for all new work; don't bulk-translate existing Spanish data labels.** All new documentation, code, identifiers, variable names, comments, and commit messages must be in English. (The owner may phrase requests in Spanish — that is never a cue to switch the output language.) **Caveat:** the existing Spanish concept/metric labels in `00_config/*.json` (`concept_hierarchy.json`, `metrics_hierarchy.json`) are used as Delta join keys and dashboard display values — renaming them is a breaking data change. Leave existing identifiers as-is unless explicitly asked to migrate them, and only with the data/dashboard impact handled.
- **`Q4 = FY − YTD_Q3` is intentional, not a bug.** `20_transformation/21b__derive_quarterly.py` derives Q4 by subtracting the YTD-Q3 figure from the annual FY to capture year-end audit adjustments. Do not "fix" this to `Q1+Q2+Q3+Q4`. Q1–Q3 use standalone SEC reports or YTD deltas based on each concept's `kind` (flow_additive / flow_nonadditive / stock).
- **Balance Sheet dedup**: SEC re-reports prior snapshots in later 10-Qs. Dedup by `(ticker, concept, period_end)` keeping the latest `filed` — preserve this when touching ingestion/merge.
- **Pricing is `period_end`-aligned, sourced from `market_prices_daily`.** `10_ingestion/12__fetch_market_data.py` writes a **daily** price store `main.financials.market_prices_daily` (one row per `(ticker, date)`; raw `close` for market cap, `adj_close` for returns). `22__derived_metrics.py` computes each FY's `market_cap` as the raw `close` on the latest trading day ≤ that FY's `period_end` × `Shares Diluted` reported as-of `period_end` — so non-December filers (AAPL/Sep, MSFT/Jun, WMT/Jan) are priced at their real fiscal close, not Dec 31. Use raw `close` (not `adj_close`) for market cap — `adj_close` would fold future splits into the historical cap.
- **`market_data` is legacy/deprecated**, kept for backward compatibility only. `12` rebuilds it (last raw `close` per **calendar** year × FY `Shares Diluted`) **from** `market_prices_daily`; its `fiscal_year` is still calendar year (the 0–11 month offset remains there by design). Don't add new dependencies on `market_data` — price off `market_prices_daily` + `period_end` instead.

## Operational gotchas

- **SEC User-Agent must be set before running ingestion.** `00_config/01__tickers.py` ships with placeholder `"MyCompany myemail@example.com"`. SEC blocks requests without a real org/email. Flag this if you see it unchanged when working near ingestion.
- **Unity Catalog schemas are pre-provisioned.** Code reads/writes `main.financials` and `main.config`; it does NOT create catalog or schema. Don't add `CREATE CATALOG` / `CREATE SCHEMA` statements — assume they exist.
- **`%run` and `dbutils` only work inside Databricks.** Notebooks pull config via `%run "/Workspace/.../01__tickers"`. Flag any change that introduces these in a `.py` that's expected to run locally via Databricks Connect.
- **No catalog/schema CREATE, no test suite, no lint config currently.** Validation today is ad-hoc via `30_analysis/31__company_analysis.py`.

## Workflow

- **Plan before editing notebooks or pipeline `.py` files.** They are stateful and side-effecting (writes to Delta tables, calls SEC/yfinance APIs). Outline the change first, then implement.
- **Flag Databricks-only assumptions** explicitly when proposing changes (uses `dbutils`, `%run`, `spark`, Unity Catalog three-part names, etc.) so the user knows what will break locally.
- **Run from `91__full_pipeline.py`** as a Databricks Job; it accepts `tickers_override`, `run_optimization`, `rebuild_config`. Local smoke test for Databricks Connect credentials is `test_connection.py` (gitignored).
- **Branch discipline: `main` is the single source of truth.** GitHub `main` is the production source and feeds the read-only Databricks Repo mirror (see *Sync GitHub → Databricks Repo* below). Do feature work on `dev_alm`, validate, then merge to `main` via the normal PR flow. **Never force-push `main`** — it triggers the sync and is the production source.

## Layout

- `00_config/` — tickers list, XBRL concept map, metric hierarchies, master-table builders
- `10_ingestion/` — parallel SEC (8-worker, rate-limited) and yfinance fetch
- `20_transformation/` — annual merge, quarterly derivation, pruning, derived metrics
- `30_analysis/` — ad-hoc validation queries
- `40_dashboards/` — dashboard SQL and `.lvdash.json`
- `90_pipelines/91__full_pipeline.py` — orchestration entry point (Databricks notebook source format)

## Sync GitHub → Databricks Repo

GitHub `main` is the single source of truth. The Databricks Repo (synced by
`.github/workflows/sync-databricks.yml`) is a **read-only mirror**, located at
`/Workspace/Shared/fundamentals_databricks_pj` (Actions variable
`DATABRICKS_REPO_PATH`).

- **The Repo lives under `/Workspace/Shared/`, not under `/Repos/<your-email>`.** The sync
  is done by the service principal `gh-actions-repo-sync`, which cannot recreate a Repo in a
  human's `/Repos/<email>` namespace (that is owner-managed). That's why the
  auto-repair (delete+recreate) requires a folder the SP actually controls, such as
  `/Workspace/Shared`. The `REPO_ID` is resolved from the git URL, not from a fixed id.
- **Don't edit or run the notebooks directly from the synced Repo.**
  Opening a `.py` notebook in the workspace editor rewrites it (cell metadata/reformatting)
  and creates local changes that break the pull with `GIT_CONFLICT`.
- To iterate interactively, clone the repo into a separate Databricks Repo under
  your user folder and work there; push the changes via GitHub.
- If you need to run a notebook from the synced Repo, launch it as a **Job** against
  its path (don't open it in the editor): running does not rewrite the source, editing does.
- The workflow is self-repairing: on `GIT_CONFLICT` it deletes and recreates the Repo from
  `main`, discarding the local state. If you had unsaved work in the synced
  Repo, it will be lost — that's why you must not work there.