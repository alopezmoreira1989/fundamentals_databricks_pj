# Fundamentals Analytics — Databricks Pipeline

End-to-end Databricks pipeline that ingests **annual (10-K) and quarterly (10-Q)** XBRL financial filings from SEC EDGAR, fetches market data from Yahoo Finance, and serves Income Statement, Balance Sheet, Cash Flow, and derived financial metrics via direct queries on Delta tables. A public Streamlit dashboard provides read-only access without Databricks credentials.

```
favorites.json / concept_hierarchy.json / metrics_hierarchy.json   ← edit here
valuation_assumptions.json                                         ← edit here
        ↓
02__tickers_master              build main.config.tickers
        ↓
03__concept_hierarchy_master    build main.config.concept_hierarchy
        ↓
04__metrics_hierarchy_master    build main.config.metrics_hierarchy
        ↓
11__fetch_sec_xbrl              SEC API → financials_raw  (10-K + 10-Q)
        ↓
12__fetch_market_data           Yahoo Finance → market_data
        ↓
21__clean_and_merge             FY rows   → MERGE into financials
21b__derive_quarterly           Q1..Q4    → MERGE into financials
21c__prune_quarterly            keep last 12 quarters per ticker
        ↓
22__derived_metrics             margins, FCF, YoY growth, leverage, valuation ratios
        ↓
23__intrinsic_value             Graham, Graham Revised, DCF, Owner Earnings (FY + TTM)
        ↓
31__company_analysis            validation queries
        ↓
32__coverage_check              verify favorites reached financials + metrics
        ↓
51__export_dashboard_data       slice + write parquet artifacts to /tmp/
        ↓
52__publish_to_github           upload artifacts as GitHub Release (latest tag)
```

---

## Project structure

```
fundamentals_databricks_pj/
│
├── 00_config/
│   ├── 01__tickers.py                       ← constantes, nombres de tabla y mapas XBRL (con kind por concept)
│   ├── 02__tickers_master.py                ← construye main.config.tickers (S&P 500 + Russell 3000 + favoritos)
│   ├── 03__concept_hierarchy_master.py      ← construye main.config.concept_hierarchy desde JSON
│   ├── 04__metrics_hierarchy_master.py      ← construye main.config.metrics_hierarchy desde JSON
│   ├── favorites.json                       ← lista de tickers favoritos (con overrides opcionales: cik, aliases)
│   ├── concept_hierarchy.json               ← jerarquía de conceptos contables
│   ├── metrics_hierarchy.json               ← jerarquía de derived metrics
│   └── valuation_assumptions.json           ← supuestos de valoración (WACC, growth, overrides por ticker)
│
├── 10_ingestion/
│   ├── 11__fetch_sec_xbrl.py         ← SEC EDGAR XBRL API → financials_raw (parallel + Arrow + batched)
│   └── 12__fetch_market_data.py      ← Yahoo Finance (yfinance) → market_data
│
├── 20_transformation/
│   ├── 21__clean_and_merge.py        ← MERGE FY rows into financials
│   ├── 21b__derive_quarterly.py      ← derive standalone Q1..Q4 (with Q4 = FY − YTD_Q3) → MERGE into financials
│   ├── 21c__prune_quarterly.py       ← enforce rolling window of QUARTERLY_WINDOW (=12) quarters per ticker
│   ├── 22__derived_metrics.py        ← FCF, margins, YoY, leverage, valuation ratios (FY only)
│   └── 23__intrinsic_value.py        ← Graham, Graham Revised, DCF, Owner Earnings (FY + TTM)
│
├── 30_analysis/
│   ├── 31__company_analysis.py       ← ad-hoc validation queries
│   └── 32__coverage_check.py         ← post-pipeline check: favorites coverage + ingestion failures
│
├── 40_dashboards/
│   ├── 41__dashboard_queries.py      ← SQL feeding the Databricks dashboard
│   └── Main Dashboard.lvdash.json    ← dashboard definition (annual pages + quarterly page)
│
├── 50_publish/
│   ├── 51__export_dashboard_data.py  ← slice financials + metrics → /tmp/ parquet artifacts
│   └── 52__publish_to_github.py      ← upload to GitHub Release (latest tag)
│
├── 51_streamlit_app/                 ← public Streamlit Cloud dashboard (see its own README)
│
└── 90_pipelines/
    └── 91__full_pipeline.py          ← entry point del Job — ejecuta los 11 steps en secuencia
```

---

## Pipeline flow

```
favorites.json                editar tickers favoritos (sin tocar Databricks)
concept_hierarchy.json        editar jerarquía de conceptos contables
metrics_hierarchy.json        editar jerarquía de derived metrics
valuation_assumptions.json    editar supuestos de valoración (WACC, growth, etc.)
      ↓
02__tickers_master              construye main.config.tickers              (manual)
      ↓
03__concept_hierarchy_master    construye main.config.concept_hierarchy    (auto)
      ↓
04__metrics_hierarchy_master    construye main.config.metrics_hierarchy    (auto)
      ↓
11__fetch_sec_xbrl              SEC API → financials_raw                   (10-K + 10-Q)
      ↓
12__fetch_market_data           Yahoo Finance → market_data
      ↓
21__clean_and_merge             FY rows   → MERGE into financials
21b__derive_quarterly           Q1..Q4    → MERGE into financials
21c__prune_quarterly            keep last QUARTERLY_WINDOW Qs per ticker
      ↓
22__derived_metrics             márgenes, FCF, YoY, leverage, ratios de valoración (FY only)
      ↓
23__intrinsic_value             Graham, Graham Revised, DCF, Owner Earnings (FY + TTM)
      ↓
31__company_analysis            queries de validación
      ↓
32__coverage_check              verificar que favoritos llegaron a financials + metrics
      ↓
51__export_dashboard_data       slice + write parquet artifacts to /tmp/
      ↓
52__publish_to_github           upload to GitHub Release (latest tag)
```

`main.config.tickers` se reconstruye manualmente con `02__tickers_master` cuando edites `favorites.json`. Las jerarquías se reconstruyen automáticamente en cada run desde sus JSONs.

---

## Favoritos (`favorites.json`)

Los tickers favoritos se gestionan editando `00_config/favorites.json` directamente en el repositorio Git. Se incluyen siempre en la ingesta y en la exportación al dashboard público.

```json
[
  {"ticker": "TSM",  "company": "Taiwan Semiconductor", "note": ""},
  {"ticker": "VNOM", "company": "Viper Energy Inc",     "cik": "0001602065", "note": "MLP→C-corp 2024"},
  {"ticker": "FOO",  "company": "Foo Corp",             "aliases": ["FOO-OLD"], "note": "ticker change 2025"}
]
```

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `ticker` | string | sí | Símbolo del ticker |
| `company` | string | sí | Nombre de la empresa |
| `note` | string | no | Nota libre (no usada por código) |
| `cik` | string | no | CIK de 10 dígitos con padding (ej. `"0001602065"`). Fuerza este CIK en la ingesta SEC, ignorando el lookup estándar. Útil tras conversiones MLP→C-corp, spin-offs, o cuando SEC tarda en actualizar su índice |
| `aliases` | list[str] | no | Tickers históricos que apuntan a la misma empresa. Si el pipeline intenta resolver un alias, usa el CIK del ticker canónico |

---

## Jerarquías (`concept_hierarchy.json` y `metrics_hierarchy.json`)

Ambas jerarquías son archivos JSON en `00_config/` editables desde el repo. El pipeline las aplana a tablas Delta en cada ejecución.

**`concept_hierarchy.json`** — árbol contable (Income Statement, Balance Sheet, Cash Flow): qué conceptos van bajo qué grupo y en qué orden aparecen en el dashboard.

**`metrics_hierarchy.json`** — organización de las derived metrics en 2 niveles: `category → subcategory → metric`. Seis categorías: Profitability, Cash Flow, Growth, Financial Health, Valuation, Intrinsic Value.

Para modificarlas: edita el JSON, commit + push, y el siguiente run del pipeline reconstruye la tabla automáticamente.

---

## Tables

| Table | Description |
|---|---|
| `{CATALOG}.config.tickers` | Universo de tickers activos (S&P 500 + Russell 3000 + favoritos) |
| `{CATALOG}.config.concept_hierarchy` | Jerarquía de conceptos contables |
| `{CATALOG}.config.metrics_hierarchy` | Jerarquía de derived metrics |
| `{CATALOG}.{SCHEMA}.financials_raw` | Append-only audit log of all SEC scrapes (10-K + 10-Q) |
| `{CATALOG}.{SCHEMA}.financials` | Long-format fact table — one row per ticker / fiscal_year / period_type / concept |
| `{CATALOG}.{SCHEMA}.market_data` | Year-end closing prices and market cap per ticker / fiscal_year |
| `{CATALOG}.{SCHEMA}.financials_metrics` | Derived metrics — margins, FCF, YoY, leverage, valuation ratios |
| `{CATALOG}.{SCHEMA}.financials_intrinsic_value` | Intrinsic value models — Graham, DCF, Owner Earnings (FY + TTM) |
| `{CATALOG}.{SCHEMA}.ingestion_failures` | Append-only log of ingestion errors (SEC + yfinance) per run |

---

## `ingestion_failures` — error tracking

Append-only log of tickers that failed during ingestion (SEC or yfinance). Written at the end of `11__fetch_sec_xbrl` and `12__fetch_market_data` on each run.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Ticker that failed |
| `error_type` | STRING | Category: `cik_not_found`, `http_404`, `http_5xx`, `timeout`, `json_decode`, `non_json_response`, `empty_facts`, `other` |
| `error_message` | STRING | First 500 chars of the exception message |
| `step` | STRING | Pipeline step: `fetch_cik`, `fetch_facts`, `extract`, `market_data` |
| `scraped_at` | TIMESTAMP | Timestamp of the run |

Query failures from the latest run:
```sql
SELECT * FROM main.financials.ingestion_failures
WHERE scraped_at = (SELECT MAX(scraped_at) FROM main.financials.ingestion_failures)
ORDER BY error_type, ticker;
```

---

## Coverage check — `32__coverage_check`

Post-pipeline notebook (`30_analysis/32__coverage_check.py`) that verifies all favorite tickers made it through the full pipeline. Checks:

1. Favorites present in `config.tickers` but missing from `financials`
2. Favorites present in `financials` but missing from `financials_metrics`
3. Tickers with ingestion failures in the latest run

**Threshold:** raises `RuntimeError` (hard fail) if >5% of favorites are missing from `financials`. Otherwise warnings only. Runs as step 10/12 in `91__full_pipeline`.

---

## `financials_raw` — append-only audit log

Stores every fact returned by SEC EDGAR's XBRL API, across both annual (10-K) and quarterly (10-Q) filings, plus their amendments. Period metadata is preserved so downstream notebooks can derive standalone quarters and handle restatements deterministically.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `company` | STRING | Company name |
| `stmt` | STRING | `Income Statement` / `Balance Sheet` / `Cash Flow` |
| `concept` | STRING | Display label (from XBRL concept map) |
| `kind` | STRING | `flow_additive` / `flow_nonadditive` / `stock` — drives quarterly derivation logic |
| `fy` | INT | Fiscal year per SEC |
| `fp` | STRING | Fiscal period per SEC: `FY` / `Q1` / `Q2` / `Q3` |
| `form` | STRING | `10-K` / `10-Q` / `10-K/A` / `10-Q/A` |
| `period_start` | DATE | Start of period (NULL for stock concepts) |
| `period_end` | DATE | End of period |
| `period_shape` | STRING | `Q_standalone` (~90d) / `YTD_6M` / `YTD_9M` / `FY_or_TTM` / `snapshot` / `other_Xd` |
| `value` | DOUBLE | Raw value in USD |
| `filed` | DATE | Filing date (used for restatement dedupe — latest `filed` wins) |
| `scraped_at` | TIMESTAMP | Fetch timestamp |

---

## `financials` — clean fact table

Long-format fact table with one row per `ticker / fiscal_year / period_type / concept`. Annual (FY) rows are kept for full history; quarterly rows (Q1..Q4) are limited to the most recent `QUARTERLY_WINDOW` (= 12) per ticker.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `company` | STRING | Company name |
| `stmt` | STRING | `Income Statement` / `Balance Sheet` / `Cash Flow` |
| `concept` | STRING | Financial line item name |
| `fiscal_year` | INT | Fiscal year (from SEC `fy`, not calendar year) |
| `period_type` | STRING | `FY` / `Q1` / `Q2` / `Q3` / `Q4` |
| `period_end` | DATE | End of period — useful for cross-ticker chronological ordering |
| `value` | DOUBLE | Raw value in USD (or native unit for EPS/shares) |
| `is_derived` | BOOLEAN | `true` if computed (Q4 = FY − YTD_Q3, or Q1..Q3 derived from YTD differences); `false` if reported directly |
| `scraped_at` | TIMESTAMP | Source scrape timestamp |

---

## Quarterly data — how it works

The pipeline ingests every fact SEC returns, but the `financials` fact table only exposes **standalone quarter values** (not the YTD accumulated ones). This is achieved in `21b__derive_quarterly` according to the concept's `kind`:

| Kind | Examples | Q1, Q2, Q3 | Q4 |
|---|---|---|---|
| `flow_additive` | Revenue, Net Income, OCF, CapEx | Standalone (~90d) if SEC reported it; otherwise `YTD_n − YTD_(n-1)` | **Always** `FY (10-K) − YTD_Q3` |
| `flow_nonadditive` | EPS, Shares Diluted | Standalone (~90d) only; NULL otherwise | NULL (cannot be derived sensibly) |
| `stock` | Assets, Cash, Equity | Snapshot at `period_end`; deduped by `(ticker, concept, period_end)` keeping latest `filed` | Snapshot at FY end (from `21__clean_and_merge`) |

**Why Q4 is always derived from `FY − YTD_Q3`:** SEC reports the full FY in the 10-K, but never a standalone Q4 fact. Computing Q4 from the 10-K total (rather than summing Q1+Q2+Q3+Q4) ensures any year-end audit adjustments are correctly captured.

**Rolling window of 12 quarters:** `21c__prune_quarterly` enforces a window of the 12 most recent quarters per ticker. FY rows are kept for full history.

**Balance Sheet duplicates:** SEC re-reports the prior FY snapshot in each subsequent 10-Q as a comparative. `21b` dedupes by `(ticker, concept, period_end)` keeping the latest `filed`, so each `period_end` shows exactly one value.

### Verifying quarterly correctness

```sql
-- Should return zero or very few rows (small rounding diffs)
WITH q AS (
    SELECT ticker, concept, fiscal_year,
           SUM(CASE WHEN period_type IN ('Q1','Q2','Q3','Q4') THEN value END) AS qsum,
           MAX(CASE WHEN period_type = 'FY' THEN value END)                    AS fy
    FROM main.financials.financials
    WHERE stmt IN ('Income Statement', 'Cash Flow')
    GROUP BY ticker, concept, fiscal_year
    HAVING COUNT(DISTINCT period_type) = 5
)
SELECT * FROM q
WHERE ABS((qsum - fy) / NULLIF(fy, 0)) > 0.001
ORDER BY ticker, fiscal_year DESC;
```

---

## `market_data` — year-end prices & market cap

Year-end adjusted closing prices fetched from Yahoo Finance via `yfinance`, joined with `Shares Diluted` from `financials` to compute an annual market cap.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `fiscal_year` | INT | Calendar year (yfinance has no notion of fiscal years) |
| `price_close` | DOUBLE | Last adjusted closing price of the year |
| `shares_diluted` | DOUBLE | Diluted share count sourced from `financials` (FY only) |
| `market_cap` | DOUBLE | `price_close × shares_diluted` |
| `fetched_at` | TIMESTAMP | Fetch timestamp |

> **Note on `fiscal_year`:** the column name matches `financials` for join convenience, but the value here is **calendar year**. For companies with non-December fiscal year-ends (AAPL/Sep, MSFT/Jun, WMT/Jan), this introduces a known 0–11 month offset between fundamentals (fiscal) and price (calendar). Acceptable for trend analysis; precise valuation requires `period_end`-based pricing.

---

## Metrics hierarchy — `main.config.metrics_hierarchy`

Lookup table organising derived metrics into categories. Rebuilt every run from `00_config/metrics_hierarchy.json`. Join with `financials_metrics` by `metric` to add `category` / `subcategory` filters to the dashboard and get stable row ordering via `sort_order`.

| Column | Type | Description |
|---|---|---|
| `category` | STRING | Profitability, Cash Flow, Growth, Financial Health, Valuation, Intrinsic Value |
| `subcategory` | STRING | Margins, YoY, Leverage, Liquidity, Price Multiples, Enterprise Value, Absolute |
| `metric` | STRING | Nombre exacto tal como aparece en `financials_metrics.metric` |
| `unit` | STRING | `percent` / `usd` / `ratio` |
| `requires_market_data` | BOOLEAN | `true` para las métricas que dependen de `market_data` |
| `sort_order` | INT | Orden global (10, 20, 30, ...) |

---

## Derived metrics — `financials_metrics`

Long-format table: one row per `ticker / fiscal_year / metric`. Computed by `22__derived_metrics` from `financials` (FY rows only) and `market_data`.

```
ticker | company | fiscal_year | metric          | value
-------|---------|-------------|-----------------|----------
AAPL   | Apple   | 2023        | Net Margin %    |     25.31
AAPL   | Apple   | 2023        | Free Cash Flow  | 99584000000
AAPL   | Apple   | 2023        | P/E             |     28.74
```

Las métricas están organizadas en 6 categorías. La jerarquía completa vive en
`00_config/metrics_hierarchy.json` y se materializa en `main.config.metrics_hierarchy`.

### Profitability — Margins

| Metric | Formula |
|---|---|
| `Gross Margin %` | `Gross Profit / Revenue × 100` |
| `Operating Margin %` | `Operating Income / Revenue × 100` |
| `Net Margin %` | `Net Income / Revenue × 100` |
| `FCF Margin %` | `Free Cash Flow / Revenue × 100` |

### Profitability — Returns

| Metric | Formula |
|---|---|
| `ROA %` | `Net Income / Total Assets × 100` |
| `ROE %` | `Net Income / Total Stockholders Equity × 100` |
| `ROIC %` | `Operating Income / Invested Capital × 100` |
| `ROCE %` | `Operating Income / Capital Employed × 100` |
| `CROIC %` | `Free Cash Flow / Invested Capital × 100` |

### Cash Flow — Absolute

| Metric | Formula |
|---|---|
| `Free Cash Flow` | `Operating Cash Flow − CapEx` |

### Growth — YoY

| Metric | Formula |
|---|---|
| `Revenue YoY %` | YoY % change in Revenue |
| `Net Income YoY %` | YoY % change in Net Income |
| `Operating Cash Flow YoY %` | YoY % change in Operating CF |
| `Free Cash Flow YoY %` | YoY % change in FCF |

### Financial Health — Leverage

| Metric | Formula |
|---|---|
| `Debt / Equity` | `(LT Debt + ST Debt) / Total Stockholders Equity` |
| `Debt / Assets` | `(LT Debt + ST Debt) / Total Assets` |

### Financial Health — Liquidity

| Metric | Formula |
|---|---|
| `Current Ratio` | `Total Current Assets / Total Current Liabilities` |

### Valuation — Price Multiples *(requires `market_data`)*

| Metric | Formula |
|---|---|
| `P/E` | `Market Cap / Net Income` |
| `P/S` | `Market Cap / Revenue` |
| `P/FCF` | `Market Cap / Free Cash Flow` |
| `P/B` | `Market Cap / Total Stockholders Equity` |

### Valuation — Enterprise Value *(requires `market_data`)*

| Metric | Formula | Notes |
|---|---|---|
| `EV` | `Market Cap + Total Debt − (Cash & Equivalents + ST Investments)` | Enterprise value in USD |
| `EV/EBITDA` | `EV / (Operating Income + D&A)` | Outliers beyond ±500× filtered out |

### Valuation — Yields *(requires `market_data`)*

| Metric | Formula |
|---|---|
| `Earnings Yield %` | `Net Income / Market Cap × 100` |
| `Sales Yield %` | `Revenue / Market Cap × 100` |
| `FCF Yield %` | `Free Cash Flow / Market Cap × 100` |
| `Op Cash Flow Yield %` | `Operating Cash Flow / Market Cap × 100` |
| `Book Yield %` | `Total Stockholders Equity / Market Cap × 100` |
| `EBITDA Yield %` | `EBITDA / Market Cap × 100` |

### Intrinsic Value *(requires `market_data`)*

| Metric | Formula |
|---|---|
| `Graham Number (FY/TTM)` | `sqrt(22.5 × EPS × Book Value per Share)` |
| `Graham Revised Value (FY/TTM)` | `(EPS × (8.5 + 2g) × 4.4) / Y` |
| `DCF Value per Share (FY/TTM)` | Discounted 10-year FCF projection + terminal value |
| `Owner Earnings (FY/TTM)` | `Net Income + D&A − CapEx − ΔWC` |
| `Owner Earnings Value/Share (FY/TTM)` | Capitalized Owner Earnings at required return |
| `MoS %` | Margin of Safety: `(Intrinsic Value − Price) / Intrinsic Value × 100` |

> Las métricas de valoración solo se rellenan para combinaciones `ticker / fiscal_year` donde `market_data` tiene un `market_cap` válido. Los supuestos de valoración (WACC, growth rate, etc.) se configuran en `00_config/valuation_assumptions.json`.

---

## Public Streamlit Dashboard

**Live app: https://alm-fundamentals.streamlit.app/**

A read-only dashboard at Streamlit Community Cloud renders the same data without Databricks credentials. Currently serves ~2,500 tickers (S&P 500 + Russell 2000 proxy) with synthetic data for preview; production data is published via GitHub Release. See [`51_streamlit_app/README.md`](fundamentals_pipeline/51_streamlit_app/README.md) for details.

---

## Dashboard — `Main Dashboard.lvdash.json`

The dashboard has the following pages:

- **Scorecard** — high-level KPIs (annual)
- **Balance Sheet** — annual BS pivot
- **Income Statement** — annual IS pivot
- **Cash Flow** — annual CF pivot
- **Derived Metrics** — all calculated metrics (annual)
- **Quarterly** — last 12 quarters per ticker:
  - Quarterly Revenue with YoY growth (same-Q comparison)
  - Quarterly Income Statement pivot
  - Quarterly Balance Sheet snapshot with YoY change

All annual pages filter `period_type = 'FY'` in their queries. The quarterly page filters `period_type IN ('Q1','Q2','Q3','Q4')`.

---

## Pipeline parameters

The full pipeline (`91__full_pipeline`) accepts Databricks Job parameters at runtime:

| Parameter | Default | Description |
|---|---|---|
| `tickers_override` | *(empty)* | Comma-separated list of tickers — bypasses `main.config.tickers` |
| `run_optimization` | `false` | Run `OPTIMIZE + VACUUM` on Delta tables at the end of the pipeline |
| `rebuild_config` | `false` | Reserved — ticker rebuild must still be run manually via `02__tickers_master` |

Example:
```json
{"tickers_override": "AAPL,TSLA,MSFT", "run_optimization": "false"}
```

---

## Column reference

All monetary values are displayed in **millions or billions USD** in the dashboard widgets (raw values in tables are in USD). Per-share figures (EPS) and share counts are kept in their native units.

### Income Statement

| Concept | Display label | Unit |
|---|---|---|
| `Revenue` / `Revenue (contract)` | Revenue | $ |
| `Cost of Revenue` | Cost of Revenue | $ |
| `Gross Profit` | Gross Profit | $ |
| `R&D Expense` | R&D | $ |
| `SG&A Expense` | SG&A | $ |
| `Operating Expenses` | Operating Expenses | $ |
| `Operating Income` | Operating Income | $ |
| `Interest Expense` | Interest Expense | $ |
| `Income Before Tax` | Income Before Tax | $ |
| `Income Tax` | Income Tax | $ |
| `Net Income` | Net Income | $ |
| `EPS Basic` | EPS Basic | USD |
| `EPS Diluted` | EPS Diluted | USD |
| `Shares Diluted` | Shares Diluted | shares |

> Revenue is coalesced from two XBRL tags (`Revenues` and `RevenueFromContractWithCustomerExcludingAssessedTax`) since companies report under different tags.

---

## Architectural notes

**Favorites in JSON, not Delta.** An earlier iteration used a Delta table (`main.config.favorites`) managed via a notebook. Simplified to `favorites.json` so the favorites list can be edited from the editor or GitHub without opening Databricks.

**Hierarchies in JSON, not code.** Both `concept_hierarchy.json` and `metrics_hierarchy.json` live as declarative JSON in `00_config/`. Each has a master notebook that flattens it into a Delta lookup table. This decouples structure (order, grouping, categories) from transformation logic.

**Why `fiscal_year` everywhere.** Migrated from `year` to `fiscal_year` to be semantically correct: SEC reports against fiscal years (which differ from calendar years for ~30% of US issuers). The column name is consistent across `financials`, `financials_metrics`, and `market_data` for join convenience, even though `market_data` stores calendar-year-end prices.

**SEC fetch parallelism.** `11__fetch_sec_xbrl` uses `ThreadPoolExecutor` with 8 workers and a global rate limiter (`MIN_REQUEST_GAP = 0.12s` enforced via Lock + monotonic clock). Writes happen incrementally in batches of 250 tickers via `flush_batch()` — avoids building a multi-million-row pandas DataFrame in driver memory before a single `createDataFrame` call.
