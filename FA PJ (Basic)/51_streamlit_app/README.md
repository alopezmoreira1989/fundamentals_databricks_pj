# 51_streamlit_app — public financials dashboard

A public, editorial-style financial statements dashboard powered by Streamlit
Community Cloud. Fetches nightly-published parquet files from a GitHub Release
(`latest` tag) so it requires **no Databricks credentials** and is free to host.

Live URL (once deployed):
`https://al-lopez-moreira-fundamentals-databricks-pj-app.streamlit.app`

---

## Architecture

```
Databricks pipeline (nightly)
    └── 50_publish/51__ → writes 3 artifacts to /tmp/
    └── 50_publish/52__ → uploads to GitHub Release (tag `latest`)
                              │
                              ▼
       Streamlit Community Cloud (this app)
       Fetches from: github.com/al-lopez-moreira/fundamentals_databricks_pj
                     /releases/download/latest/{dashboard_data,metrics,meta}.*
                              │
                              ▼
           Anonymous viewer: editorial-newspaper dashboard
           Ticker dropdown • 5 tabs • no login
```

---

## Local development

### Prerequisites

- Python 3.10+
- `pip install -r requirements.txt`

### Get fixture data (one-time)

You need the three published files locally so the app can render without hitting
the GitHub Release. See `../50_publish/README.md` for options. Quickest after
the pipeline has published at least once:

```bash
cd fixtures/
curl -sLO https://github.com/al-lopez-moreira/fundamentals_databricks_pj/releases/download/latest/dashboard_data.parquet
curl -sLO https://github.com/al-lopez-moreira/fundamentals_databricks_pj/releases/download/latest/dashboard_metrics.parquet
curl -sLO https://github.com/al-lopez-moreira/fundamentals_databricks_pj/releases/download/latest/dashboard_meta.json
```

### Run locally

```bash
streamlit run app.py
```

Open `http://localhost:8501`. The app detects `fixtures/` and reads from there
instead of GitHub. Fixture files are `.gitignore`d.

---

## Streamlit Cloud deployment

1. Push the repo to GitHub (must be **public** for both free Streamlit hosting
   and public GitHub Release asset access).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Configure:
   - **Repository:** `al-lopez-moreira/fundamentals_databricks_pj`
   - **Branch:** `main`
   - **Main file path:** `FA PJ (Basic)/51_streamlit_app/app.py`
4. Deploy. Streamlit Cloud auto-installs from `requirements.txt`.
5. (Optional) Set a custom subdomain in app settings.

**No secrets are needed in Streamlit Cloud** — the parquet files are public
GitHub Release assets.

---

## Refresh story

- Databricks pipeline runs nightly (or on-demand via Job).
- Final pipeline steps (10/11 in `91__full_pipeline.py`) export + publish to GitHub.
- Streamlit app re-fetches on cache TTL (1 hour) or when a user reloads after
  cache expiry. Max data staleness: ~25 hours.
- To force-refresh: Streamlit Cloud → app menu → **Clear cache** → reload.

---

## File structure

```
app.py                      entry point
styles.css                  CSS spec (editorial-newspaper theme + Streamlit overrides)
notes.json                  ticker-specific footnotes
requirements.txt            dependencies
.streamlit/config.toml      Streamlit theme
.gitignore                  excludes fixtures/*.parquet/*.json
fixtures/                   local-dev data (gitignored)
lib/
├── __init__.py
├── data.py                 fetch + cache parquet from GitHub (or fixtures/)
├── format.py               number formatting (accounting negatives, KPI $B, CAGR)
├── colors.py               sparkline stroke color + CSS row-class rules
├── sparkline.py            inline SVG sparkline generator
├── tables.py               long→wide pivot for each statement
├── kpis.py                 KPI strip renderer
├── quarterly.py            combo SVG chart (bars + YoY line, dynamic axes)
└── render.py               masthead, table HTML, waterfall, metrics grid, footnotes
```

---

## Visual reference

The pixel-level target is `FA PJ (Basic)/aapl_dashboard.html` — a
self-contained 1268-line static HTML file with the same CSS as `styles.css`.
When iterating on rendering, open both in parallel and diff visually.

Expected differences from the reference:
- The app renders **all** concepts in `concept_hierarchy.json`, not just the
  curated AAPL subset in the reference. Extra rows render as regular rows with
  `—` for missing years.
- 6 metric cards (including Intrinsic Value) vs. 5 in the reference.
- Quarterly chart axes auto-fit to each ticker's revenue scale.
