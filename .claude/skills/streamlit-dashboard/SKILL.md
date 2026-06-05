---
name: streamlit-dashboard
description: Conventions for editing the public Streamlit dashboard under fundamentals_pipeline/60_streamlit_app — the editorial design tokens (cream background, Fraunces / Inter / JetBrains Mono fonts, accent blue), English-only UI strings, the st.pills logical-bucket screener pattern, and the regression guards already in the code (import BLUE before use in render.py, never downcast the value column to float32 in data.py, html.escape company names rendered under unsafe_allow_html). Use when asked to add a Streamlit component, edit the screener UI, change dashboard styling, render a metric card, or work on lib/render.py, lib/data.py, or views. The app must stay free of any Databricks dependency.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# streamlit-dashboard

## CRITICAL

- **No Databricks dependency at runtime.** The app reads precomputed parquet from `fixtures/` (or the GitHub Release fallback) — never `spark`, `dbutils`, or Databricks Connect. Keep it that way so it runs free on Streamlit Community Cloud.
- **English-only UI strings.** Visible labels are English (Spanish stays in config/comments). Don't introduce Spanish into rendered text.
- **Preserve the existing regression guards** (these are already correct in the code — do not reintroduce the bugs they prevent):
  - `BLUE` (and other color constants) must be imported from `lib/colors.py` before use in `lib/render.py` — it is imported at the top; keep new color refs imported, not bare.
  - In `lib/data.py` `_optimize_dtypes`, **never downcast the `value` column to float32** — it holds raw USD figures with ~7+ significant digits (e.g. 274,515,000,000) that float32 corrupts. Category/int downcasts are fine; `value` stays float64.
  - Company names (and any free text) interpolated into HTML under `unsafe_allow_html=True` must go through `html.escape(...)` — tickers like AT&T and S&P contain HTML-significant characters.
- The pixel-level visual target is `fundamentals_pipeline/aapl_dashboard.html`; diff against it when changing rendering, but render **all** concepts from the hierarchy, not a curated subset.

## Design tokens (summary)

Defined in `60_streamlit_app/styles.css` (CSS vars), `.streamlit/config.toml` (Streamlit theme),
and mirrored as Python constants in `lib/colors.py`. Full table in `references/design-tokens.md`.

- Background: cream `#FAF8F4`.
- Fonts: `Fraunces` (display/serif), `Inter` (body sans), `JetBrains Mono` (mono).
- Accent blue: `#185FA5` (= `BLUE` in `lib/colors.py`, `--accent` in CSS).

## File map (60_streamlit_app)

- `app.py` — entry point.
- `styles.css` + `.streamlit/config.toml` — theme.
- `lib/data.py` — load + cache parquet (fixtures or GitHub fallback); `_optimize_dtypes`.
- `lib/colors.py` — color constants + row-class rules.
- `lib/render.py` — masthead, table HTML, waterfall, metrics grid, valuation football field, footnotes.
- `lib/signals.py` — metric threshold banding (good/warn/bad), threshold tooltip text.
- `lib/format.py`, `lib/tables.py`, `lib/kpis.py`, `lib/quarterly.py`, `lib/sparkline.py`, `lib/screener.py` — helpers.
- `views/company.py`, `views/screener.py` — the two pages.

## Patterns

### Screener range filters use `st.pills` logical buckets
`views/screener.py` renders `st.pills(metric, [bucket labels], selection_mode="multi", ...)` over
metric-specific buckets from `buckets_for(metric, unit)` in `lib/screener.py`. Buckets are aligned to
meaningful thresholds (e.g. Graham-style P/E bands). To add a filterable metric, define its buckets in
`lib/screener.py` and let the view render the pills — don't hand-roll sliders.

### Render a metric card / row
Go through `lib/render.py` helpers and `lib/signals.py` for the good/warn/bad color. New colors come
from `lib/colors.py` (imported), new thresholds from `lib/signals.py` (`_LOWER_IS_BETTER` /
`_HIGHER_IS_BETTER` / a special case like MoS). Escape any interpolated text.

### Add a metric to the grid
The grid is data-driven from `metrics_hierarchy.json` (category to subcategory to metric). Add the
metric there and to the pipeline (`22__derived_metrics.py` or `23__intrinsic_value.py`); the app
renders it without per-ticker curation.

## What NOT to do

- Don't add `spark` / `dbutils` / Databricks Connect imports.
- Don't downcast `value` to float32 in `_optimize_dtypes`.
- Don't interpolate unescaped text into `unsafe_allow_html=True` markup.
- Don't hardcode a hex color inline — use the `lib/colors.py` constant / CSS var.
- Don't curate per-ticker content; render from the hierarchy JSONs and the data.
- Don't introduce Spanish into visible UI strings.

## Related

- [[valuation-methods]] — the football field and MoS banding read precomputed intrinsic values; formulas live in the pipeline, not here.
- [[claude-code-prompt-packaging]] — package frontend changes as a separate prompt from pipeline changes.
