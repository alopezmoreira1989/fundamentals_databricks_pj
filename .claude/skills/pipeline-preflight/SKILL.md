---
name: pipeline-preflight
description: Pre-flight checks before submitting 90__pipelines/91__full_pipeline.py as a Databricks Job. Verifies SEC User-Agent is no longer the placeholder, Databricks Connect credentials are usable (if running locally), valuation_assumptions.json is well-formed, and main.financials / main.config schemas exist in Unity Catalog. Use whenever the user is about to kick off a full pipeline run, after cloning the repo for the first time, or when the pipeline fails and the cause is not obvious. Runs BEFORE a run; for post-run data checks use financials-invariants or validate-quarters.
metadata:
  author: Alejandro López Moreira
  version: 1.1.0
---

# pipeline-preflight

## Purpose

The full pipeline (`fundamentals_pipeline/90__pipelines/91__full_pipeline.py`, Databricks notebook source format) hits SEC EDGAR, yfinance, and Unity Catalog, and runs for many minutes per scrape. A few cheap upfront checks catch the failure modes that waste the most time. CLAUDE.md spells out three of them explicitly — this skill turns them into a script.

## When to run

- Before the first pipeline run on a freshly cloned repo.
- Before any pipeline run that includes new tickers or new concepts (so a misconfig doesn't get noticed three statements in).
- When the user says "the pipeline's failing" and the symptom isn't obvious — run this first before deeper debugging.

## Checks

Run all four; report each as PASS / FAIL / SKIP with a one-line reason. Don't fail-fast — surface everything in one pass so the user can fix all gaps at once.

### 1. SEC User-Agent is not the placeholder

Read `fundamentals_pipeline/00__config/01__tickers.py`. Find the line:

```python
SEC_USER_AGENT = "..."
```

- **FAIL** if the value equals `"MyCompany myemail@example.com"` (the placeholder shipped in the repo) or is empty. SEC rejects requests without a real org + contact email; `10__ingestion/11__fetch_sec_xbrl.py` will return 403s for every ticker. Prompt the user to set their real org name and email.
- **PASS** otherwise. Don't validate the email format — SEC is lenient as long as it's not the literal placeholder.

### 2. Databricks Connect credentials (local-run only)

Only relevant if the user is about to run from their laptop via Databricks Connect — skip if they're submitting straight to a Databricks Job. Probe:

- Existence of `test_connection.py` at the project root (gitignored per `.gitignore` — its presence indicates they've set up a local smoke test).
- Existence of `~/.databrickscfg` OR `DATABRICKS_HOST` + `DATABRICKS_TOKEN` env vars.

If the user wants to actually test the connection, suggest they run `test_connection.py` themselves with `! python test_connection.py` — don't run it inside a subprocess here because it may prompt or take >10s to fail.

- **PASS** if at least one credential source is detected.
- **SKIP** with a one-line note if running on Databricks (presence of `dbutils` / `spark` globals implies a Databricks context — but you can't check that from a static skill).
- **FAIL** if neither source exists AND the user said they're running locally.

### 3. valuation_assumptions.json is well-formed

Read `fundamentals_pipeline/00__config/valuation_assumptions.json` and verify:

- Valid JSON (parses without error).
- Top-level keys include `defaults` and `overrides`.
- `defaults` has subsections: `graham`, `graham_revised`, `dcf`, `owner_earnings`, `margin_of_safety`.
- `defaults.dcf` has numeric values for `wacc`, `growth_stage1`, `growth_terminal`, `horizon_years`.
- For each ticker in `overrides`, every nested numeric field is a number (not a string).
- Optional sanity ranges (warn, don't fail):
  - `dcf.wacc` between 0.03 and 0.20
  - `dcf.growth_terminal` strictly less than `dcf.wacc` (Gordon model requires this — otherwise terminal value blows up)
  - `dcf.horizon_years` between 1 and 30

- **PASS** if all hard checks pass. Warnings get listed but don't change the status.
- **FAIL** with the parse error or the failing key if any hard check fails. `23__intrinsic_value.py` will crash on these.

### 4. Unity Catalog schemas are reachable

Only run if a Databricks SQL warehouse / Spark session is available. Skip otherwise with a one-line note.

```sql
SHOW SCHEMAS IN main;
```

Verify `financials` AND `config` appear. Then:

```sql
SHOW TABLES IN main.config;
SHOW TABLES IN main.financials;
```

Verify `main.config.tickers` exists. The first run of the pipeline writes `main.financials.financials_raw` and `main.financials.financials`, so they may legitimately not exist yet — note their absence as INFO, not FAIL.

- **PASS** if `main.financials` and `main.config` schemas exist and `main.config.tickers` has at least one row.
- **FAIL** if either schema is missing — per CLAUDE.md, "Code reads/writes `main.financials` and `main.config`; it does NOT create catalog or schema." An admin needs to provision them.
- **SKIP** if no SQL access — say so explicitly.

## Reporting

Print a compact summary at the end:

```
SEC User-Agent           : PASS
Databricks Connect creds : PASS
valuation_assumptions    : PASS  (1 warning: TSLA dcf.growth_stage1=0.15 > soft cap 0.12)
Unity Catalog reachable  : PASS

Pre-flight: GREEN — safe to run 91__full_pipeline.
```

If any FAIL, end with `Pre-flight: RED — fix the above before running the pipeline.` and DO NOT offer to run the pipeline anyway.

## What NOT to do

- Don't modify `01__tickers.py`, `valuation_assumptions.json`, or any other config file unless the user asks. This skill reports state; it doesn't fix it.
- Don't add `CREATE CATALOG` / `CREATE SCHEMA` statements as a "fix" for check #4. Per CLAUDE.md, those must be provisioned manually by an admin.
- Don't run any actual ingestion call (no SEC requests, no yfinance fetches) — preflight should be cheap and idempotent.
- Don't auto-run `test_connection.py` — let the user run it themselves so they see the credential prompts directly.

## Related

- [[validate-concept-hierarchy]] — run right after pre-flight if new concepts were added since the last successful run.
- [[validate-quarters]] / [[validate-balance-sheet-dedup]] — run *after* the pipeline finishes, not before.
