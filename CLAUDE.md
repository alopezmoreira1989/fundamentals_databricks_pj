# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Databricks analytical pipeline that ingests SEC EDGAR XBRL filings (10-K/10-Q) for ~3,000 US tickers, joins Yahoo Finance year-end prices, derives financial metrics, and serves them via Delta tables to a Databricks dashboard. Entry point: `90_pipelines/91__full_pipeline.py` (Databricks notebook source format, run as a Databricks Job). Code is bilingual — most prose, metric labels, and JSON hierarchies are in Spanish.

## Conventions that must be preserved

- **File naming `NN__name.py` is mandatory.** Files use `<stage><order>__<purpose>` so the pipeline order is visible from filenames alone (e.g., `21__clean_and_merge.py`, `21b__derive_quarterly.py`). New files must follow the pattern.
- **Spanish naming is intentional.** Concepts, metric labels, variable names, and comments in `00_config/*.json` (`concept_hierarchy.json`, `metrics_hierarchy.json`, `favorites.json`) and most module docs are in Spanish. Keep new code in the same style — do not translate existing identifiers.
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

## Layout

- `00_config/` — tickers list, XBRL concept map, metric hierarchies, master-table builders
- `10_ingestion/` — parallel SEC (8-worker, rate-limited) and yfinance fetch
- `20_transformation/` — annual merge, quarterly derivation, pruning, derived metrics
- `30_analysis/` — ad-hoc validation queries
- `40_dashboards/` — dashboard SQL and `.lvdash.json`
- `90_pipelines/91__full_pipeline.py` — orchestration entry point (Databricks notebook source format)

## Sync GitHub → Databricks Repo

GitHub `main` es la única fuente de verdad. El Databricks Repo (sincronizado por
`.github/workflows/sync-databricks.yml`) es un **espejo de solo lectura**, ubicado en
`/Workspace/Shared/fundamentals_databricks_pj` (variable de Actions
`DATABRICKS_REPO_PATH`).

- **El Repo vive bajo `/Workspace/Shared/`, no bajo `/Repos/<tu-email>`.** El sync lo
  hace el service principal `gh-actions-repo-sync`, que no puede recrear un Repo en el
  namespace `/Repos/<email>` de un humano (es gestionado por su dueño). Por eso el
  auto-repair (delete+recreate) exige una carpeta que el SP sí controle, como
  `/Workspace/Shared`. El `REPO_ID` se resuelve por la URL del git, no por un id fijo.
- **No edites ni ejecutes los notebooks directamente desde el Repo sincronizado.**
  Abrir un notebook `.py` en el editor del workspace lo reescribe (metadatos de
  celdas/reformateo) y crea cambios locales que rompen el pull con `GIT_CONFLICT`.
- Para iterar de forma interactiva, clona el repo en un Databricks Repo aparte bajo
  tu carpeta de usuario y trabaja allí; envía los cambios vía GitHub.
- Si necesitas correr un notebook del Repo sincronizado, lánzalo como **Job** sobre
  su ruta (no lo abras en el editor): ejecutar no reescribe el fuente, editar sí.
- El workflow es auto-reparable: ante `GIT_CONFLICT` borra y recrea el Repo desde
  `main`, descartando el estado local. Si tenías trabajo sin guardar en el Repo
  sincronizado, se perderá — por eso no se debe trabajar ahí.