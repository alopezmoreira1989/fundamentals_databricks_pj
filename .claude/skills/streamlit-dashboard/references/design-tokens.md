# Design tokens (reference)

Ground truth from `fundamentals_pipeline/60__frontends/61__streamlit/`. Re-read the files before relying on
these; captured at skill-authoring time.

## Colors

| Token | Value | Defined in |
|---|---|---|
| Background (cream) | `#FAF8F4` | `styles.css` `--bg`; `.streamlit/config.toml` `backgroundColor` |
| Accent blue | `#185FA5` | `styles.css` `--accent`; `lib/colors.py` `BLUE` |

Use the `lib/colors.py` constant (`BLUE`, etc.) in Python and the CSS var (`var(--accent)`) in
`styles.css`. `BLUE` is imported at the top of `lib/render.py` — keep new color refs imported, never
bare (a bare `BLUE` would NameError).

Signal colors (good/warn/bad) come from `lib/signals.py` + `lib/colors.py` row-class rules; don't
hardcode status hexes inline.

## Fonts

Declared as CSS vars in `styles.css` and loaded via a Google Fonts import in `lib/render.py`:

| Role | Family | CSS var |
|---|---|---|
| Display / headings | `Fraunces` (fallback Times New Roman, serif) | `--display` |
| Body | `Inter` (fallback system-ui, sans-serif) | `--sans` |
| Mono / figures | `JetBrains Mono` (fallback SF Mono, Menlo) | `--mono` |

## Data-handling guards (lib/data.py)

`_optimize_dtypes(df)` reduces memory by category-encoding strings and downcasting some numerics —
but the `value` column is deliberately **kept float64**. Comment in the code:

> do NOT downcast `value` to float32 — it has ~7 significant digits, which corrupts large raw-USD
> figures (e.g. 274,515,000,000) that the IS/BS/CF tables render at full resolution via fmt_num.

`fiscal_year` to int and `sort_order` to float are fine; `value` is not.

## HTML safety (lib/render.py)

Company/free text rendered under `unsafe_allow_html=True` (e.g. the masthead `h1`) is wrapped in
`html.escape(...)`. Tickers/companies like AT&T and S&P contain `&` which would otherwise break the
markup. Always escape interpolated text.

## Screener buckets (lib/screener.py + views/screener.py)

Range filters are `st.pills(..., selection_mode="multi")` over metric-specific buckets from
`buckets_for(metric, unit)`. Buckets align to meaningful thresholds (Graham-style bands). Add new
filter metrics by defining their buckets in `lib/screener.py`.

## UI language

Visible strings are English-only. Spanish appears only in code comments and config JSON, not in
rendered UI.
