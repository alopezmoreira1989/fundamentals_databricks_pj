# 50_publish — dashboard data publish step

Final stage of the pipeline. Takes a slice of `main.financials.*` and pushes it
to a GitHub Release so the public Streamlit app at
[../51_streamlit_app](../51_streamlit_app) (live at https://alm-fundamentals.streamlit.app/)
can fetch it without authenticating to Databricks.

```
51__export_dashboard_data.py
    └─ /tmp/dashboard_data.parquet
    └─ /tmp/dashboard_metrics.parquet
    └─ /tmp/dashboard_meta.json

52__publish_to_github.py
    └─ uploads /tmp/* as release assets to
       github.com/alopezmoreira1989/fundamentals_databricks_pj
       under tags `data-YYYY-MM-DD` and `latest` (floating)
```

Both notebooks are wired into `90_pipelines/91__full_pipeline.py` as
STEPS 10/11 — every full pipeline run publishes fresh data.

## One-time setup

### 1. Create a fine-grained GitHub PAT

1. Go to GitHub → Settings → Developer settings → Personal access tokens →
   Fine-grained tokens → **Generate new token**.
2. Repository access: **Only select repositories** → choose
   `fundamentals_databricks_pj`.
3. Repository permissions: **Contents: Read and write** (everything else can
   stay at *No access*).
4. Generate, copy the `github_pat_...` value.

### 2. Store the PAT in Databricks Secrets

From your local terminal with the Databricks CLI authenticated:

```bash
databricks secrets create-scope github
databricks secrets put-secret github github_pat
# paste the token when prompted
```

The `52__` notebook reads it via `dbutils.secrets.get(scope="github", key="github_pat")`.

## Running standalone

Both notebooks are safe to run by themselves (e.g. after a manual data fix):

```python
# In a Databricks notebook
%run "/Workspace/.../fundamentals_pipeline/50_publish/51__export_dashboard_data"
%run "/Workspace/.../fundamentals_pipeline/50_publish/52__publish_to_github"
```

`52__` will replace today's `data-YYYY-MM-DD` release if it already exists, and
always moves the `latest` tag. No accumulation, no manual cleanup needed.

## Pulling a local fixture for Streamlit dev

The repo ships with committed synthetic fixtures in `51_streamlit_app/fixtures/`
(~2,500 tickers). For local Streamlit dev, these work out of the box.

To replace synthetic data with real pipeline data, two options:

**Option A — copy via a Unity Catalog Volume.** Edit `51__export_dashboard_data.py`
and set `COPY_TO_VOLUME = True` (point `VOLUME_PATH` at a Volume you can read).
After the notebook runs:

```bash
databricks fs cp -r dbfs:/Volumes/main/financials/_publish "fundamentals_pipeline/51_streamlit_app/fixtures/"
```

**Option B — download directly from the GitHub `latest` release** once the
publish step has run at least once:

```bash
mkdir -p "fundamentals_pipeline/51_streamlit_app/fixtures"
cd       "fundamentals_pipeline/51_streamlit_app/fixtures"
curl -sLO https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest/dashboard_data.parquet
curl -sLO https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest/dashboard_metrics.parquet
curl -sLO https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest/dashboard_meta.json
```

The Streamlit app's `lib/data.py` looks for `fixtures/*` first and falls back to
the GitHub URL only if the local files are missing.

## What's in each file

### `dashboard_data.parquet`

Long-format financials joined with the concept hierarchy.

| column        | type      | notes                                                  |
|---------------|-----------|--------------------------------------------------------|
| ticker        | string    |                                                        |
| period_type   | string    | `'FY'`, `'Q1'..'Q4'`                                   |
| period_end    | date      |                                                        |
| fiscal_year   | int       |                                                        |
| stmt          | string    | `'Income Statement'`, `'Balance Sheet'`, `'Cash Flow'` |
| section       | string    | from `concept_hierarchy.json` (e.g. `'Assets'`)        |
| group         | string    | inner group (e.g. `'Current Assets'`)                  |
| concept       | string    | canonical name (e.g. `'Cash & Equivalents'`)           |
| display_name  | string    | what the table renders                                 |
| sort_order    | int       | from the hierarchy build                               |
| value         | double    | in USD (or shares for Shares Diluted, $/share for EPS) |

Retention: last 10 FY rows and last 12 quarterly rows per ticker × concept.

### `dashboard_metrics.parquet`

Long-format derived metrics joined with the metrics hierarchy.

| column      | type    | notes                                              |
|-------------|---------|----------------------------------------------------|
| ticker      | string  |                                                    |
| period_type | string  |                                                    |
| period_end  | date    |                                                    |
| fiscal_year | int     |                                                    |
| category    | string  | e.g. `'Profitability'`                             |
| subcategory | string  | e.g. `'Margins'`                                   |
| metric      | string  | e.g. `'Gross Margin %'`                            |
| unit        | string  | `'percent'`, `'usd'`, `'ratio'`                    |
| sort_order  | int     |                                                    |
| value       | double  |                                                    |

### `dashboard_meta.json`

```json
{
  "schema_version":   2,
  "build_timestamp":  "2026-05-24T20:13:04+00:00",
  "tickers":          [{"ticker": "AAPL", "company": "Apple Inc."}, ...],
  "fy_ranges":        [{"ticker": "AAPL", "fy_min": 2015, "fy_max": 2024}, ...],
  "row_counts":       {"financials": 1621944, "metrics": 1176410},
  "retention":        {"fy_years": 10, "quarterly_periods": 12}
}
```

Bumping `SCHEMA_VERSION` in `51__export_dashboard_data.py` is a signal to the
Streamlit app that the columns changed — keep the two in sync.
