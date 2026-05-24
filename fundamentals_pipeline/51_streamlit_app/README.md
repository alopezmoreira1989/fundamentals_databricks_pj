# 51_streamlit_app — public financials dashboard

**Live app: https://alm-fa-dashboard.streamlit.app/**

A public, editorial-style financial statements dashboard powered by Streamlit
Community Cloud. Serves ~2,500 tickers (S&P 500 + Russell 2000 proxy) with
synthetic data for preview. When the Databricks pipeline publishes real data to
a GitHub Release (`latest` tag), the app picks it up automatically — **no
Databricks credentials** needed at runtime.

---

## Architecture

```
Databricks pipeline (nightly)
    └── 50_publish/51__ → writes 3 artifacts to /tmp/
    └── 50_publish/52__ → uploads to GitHub Release (tag `latest`)
                              │
                              ▼
       Streamlit Community Cloud (this app)
       Reads from: committed fixtures/ (synthetic preview data)
       Falls back to: github.com/alopezmoreira1989/fundamentals_databricks_pj
                      /releases/download/latest/{dashboard_data,metrics,meta}.*
                              │
                              ▼
           Anonymous viewer: editorial-newspaper dashboard
           Ticker search (2,500+) • 5 tabs • no login
```

---

## Local development

### Prerequisites

- Python 3.10+
- `pip install -r requirements.txt`

### Fixture data

The repo ships with **committed synthetic fixtures** in `fixtures/` (~2,500
tickers with AAPL-scaled data). These are used for both local dev and the
Streamlit Cloud preview deployment.

To regenerate or expand the synthetic fixtures:

```bash
python generate_russell2000_fixtures.py
```

To replace with real data from Databricks (requires Databricks Connect):

```bash
python fetch_fixtures.py
```

### Run locally

```bash
streamlit run app.py
```

Open `http://localhost:8501`. The app detects `fixtures/` and reads from there
instead of GitHub.

---

## Streamlit Cloud deployment

1. Push the repo to GitHub (must be **public** for free Streamlit hosting).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Configure:
   - **Repository:** `alopezmoreira1989/fundamentals_databricks_pj`
   - **Branch:** `main`
   - **Main file path:** `fundamentals_pipeline/51_streamlit_app/app.py`
4. Deploy. Streamlit Cloud auto-installs from `requirements.txt`.

**No secrets are needed in Streamlit Cloud** — synthetic fixtures are committed
to the repo.

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
app.py                                  entry point
styles.css                              CSS spec (editorial-newspaper theme + Streamlit overrides)
notes.json                              ticker-specific footnotes
.streamlit/config.toml                  Streamlit theme
fixtures/                               synthetic data (~2,500 tickers, committed)
├── dashboard_data.parquet              financials (long-format)
├── dashboard_metrics.parquet           derived metrics
└── dashboard_meta.json                 tickers, FY ranges, row counts
generate_russell2000_fixtures.py        regenerate synthetic fixtures (S&P 500 + Russell 2000 proxy)
fetch_fixtures.py                       pull real data from Databricks (requires Connect)
lib/
├── __init__.py
├── data.py                 fetch + cache parquet from fixtures/ (or GitHub Release fallback)
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

The pixel-level target is `fundamentals_pipeline/aapl_dashboard.html` — a
self-contained 1268-line static HTML file with the same CSS as `styles.css`.
When iterating on rendering, open both in parallel and diff visually.

Expected differences from the reference:
- The app renders **all** concepts in `concept_hierarchy.json`, not just the
  curated AAPL subset in the reference. Extra rows render as regular rows with
  `—` for missing years.
- 6 metric cards (including Intrinsic Value) vs. 5 in the reference.
- Quarterly chart axes auto-fit to each ticker's revenue scale.
