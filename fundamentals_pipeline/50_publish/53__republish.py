# Databricks notebook source
# MAGIC %md
# MAGIC # 50_publish / 53__republish
# MAGIC
# MAGIC **Targeted re-publish** — re-exports the dashboard slice and uploads it to the
# MAGIC `latest` GitHub Release **without re-running the full pipeline**. Use when the
# MAGIC Delta tables are already correct but the public Release is stale (e.g. a prior
# MAGIC `91__full_pipeline` run failed at the publish step).
# MAGIC
# MAGIC Runs `51__export_dashboard_data` then `52__publish_to_github` **inline via `%run`**
# MAGIC (same Spark/driver context, shared `/tmp`). Unlike `91`, which invokes them through
# MAGIC `dbutils.notebook.run` with a `timeout_seconds` cap, `%run` has **no per-notebook
# MAGIC timeout** — so this avoids the serverless child-notebook startup/queue that made
# MAGIC `51` hit the 600s `TIMEDOUT` even though its actual work is only ~3–4 min.
# MAGIC
# MAGIC Idempotent: re-exports from the current Delta state and overwrites the Release assets.
# MAGIC Requires the `github/github_pat` Databricks secret (same as `52`).

# COMMAND ----------

# MAGIC %run "./51__export_dashboard_data"

# COMMAND ----------

# MAGIC %run "./52__publish_to_github"

# COMMAND ----------

print("✓ Re-publish complete — 51 export + 52 GitHub upload ran inline.")
