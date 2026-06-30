---
name: databricks-notebook
description: Author a new pipeline step as a Databricks notebook-source .py file with the conventions this repo enforces — the NN__name.py stage-order filename, the Databricks notebook source header, COMMAND cell separators, MAGIC-prefixed markdown and run directives, and never an .ipynb. Includes the Unity Catalog gotcha checklist (no CREATE CATALOG or SCHEMA in notebooks, the CREATE TABLE IF NOT EXISTS vs schema-evolution-breaks-MERGE trap, serverless session resets) and how to wire the step into 91__full_pipeline.py. Use when asked to create a new notebook, add a pipeline step, write a new NN__ script, or scaffold a Databricks job notebook.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# databricks-notebook

## CRITICAL

- **Filename `NN__name.py` is mandatory** — `<stage><order>__<purpose>` so pipeline order is visible from the name alone (e.g. `21__clean_and_merge.py`, `21b__derive_quarterly.py`, `21c__prune_quarterly.py`). New files must follow it.
- **Notebook source format, never `.ipynb`.** First line must be `# Databricks notebook source`. Cells are separated by `# COMMAND ----------`. Markdown and run directives use the `# MAGIC` prefix (`# MAGIC %md`, `# MAGIC %run "..."`). This keeps diffs clean in Git and is what the sync workflow expects.
- **`%run` and `dbutils` only work inside Databricks.** If the file is meant to be prototyped locally via Databricks Connect, flag every `%run` / `dbutils` / notebook-magic dependency — they will fail locally.
- **No `CREATE CATALOG` / `CREATE SCHEMA`.** `main.financials` and `main.config` are pre-provisioned; code reads/writes them but never creates catalog or schema.
- Pipeline notebooks are stateful and side-effecting (Delta writes, SEC/yfinance calls). **Plan the change first**, then implement.

## Skeleton

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 24__my_step
# MAGIC One-line purpose. Reads X, writes Y.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"   # constants come from the %run above

# COMMAND ----------

# MAGIC %md ## 1. Do the work

# COMMAND ----------

# ... transformation ...

# COMMAND ----------

# MAGIC %md ## 2. Write / MERGE

# COMMAND ----------

# ... write ...
```

- Constants (`CATALOG`, `SCHEMA`, `TABLE`, `QUARTERLY_WINDOW`, concept maps) come from
  `%run "../00__config/01__tickers"` — don't redefine them.
- Keep prose, markdown headers, and comments in the repo's bilingual style (Spanish is intentional in
  config and most module docs).

## Unity Catalog gotcha checklist

- **No `CREATE CATALOG` / `CREATE SCHEMA IF NOT EXISTS`** — assume `main.financials` / `main.config` exist; an admin provisions them.
- **`CREATE TABLE IF NOT EXISTS` does NOT evolve schema.** If you add a column to an existing table, `IF NOT EXISTS` silently keeps the old schema and a later `MERGE` with the new column fails. When the schema changed: DROP and recreate the table (or `ALTER TABLE ... ADD COLUMNS`), don't rely on `IF NOT EXISTS`.
- **Serverless has no `.cache()` / `.persist()`** (`[NOT_SUPPORTED_WITH_SERVERLESS]`). To materialize a reused frame use `df.localCheckpoint(eager=True)`.
- **Serverless zombie session** — if a notebook hangs or behaves oddly after a long idle, detach and pick "New session" rather than re-running in the stale one.
- **Three-part names everywhere** (`main.financials.financials`); never assume a default catalog/schema.

## Wiring into the pipeline

`90__pipelines/91__full_pipeline.py` runs the steps in sequence and records per-step wall-clock
timing. To add a step: insert it in the correct stage order, pass through the job parameters it
needs (`tickers_override`, `run_optimization`, `force_full_refresh`), and keep it idempotent
(re-runnable on the latest `scraped_at` without duplicating rows). After adding a step that creates
or renames a concept, run [[validate-concept-hierarchy]]; after one that touches merge/derivation,
run [[financials-invariants]] / [[validate-quarters]].

## What NOT to do

- Don't save the file as `.ipynb` or omit the `# Databricks notebook source` header.
- Don't break the `NN__` ordering or invent a non-conforming name.
- Don't add catalog/schema creation as a "convenience".
- Don't introduce `%run` / `dbutils` into a file the user expects to run locally without flagging it.
- Don't open/edit the synced read-only Repo under `/Workspace/Shared/...`; edit in Git, let the sync mirror it (CLAUDE.md).

## Related

- [[pipeline-preflight]] — run before kicking off the job the new step belongs to.
- [[financials-invariants]] / [[validate-quarters]] / [[validate-concept-hierarchy]] — post-change validation.
