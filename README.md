# Fundamentals Analytics — Financial Statements

Produces the three core financial statement outputs (Income Statement, Balance Sheet, Cash Flow) by querying the long-format `financials` fact table directly. No intermediate wide tables or views are materialised.

```
ticker | company | year | concept           | value
-------|---------|------|-------------------|----------
AAPL   | Apple   | 2020 | Revenue           | 274520000000
AAPL   | Apple   | 2020 | Gross Profit      | 104956000000
AAPL   | Apple   | 2020 | Net Income        | 57411000000
```

Dashboard queries pivot and format this data on the fly, filtering by `statement` and `concept` as needed.

---

## Estructura del proyecto

```
fundamentals_databricks_pj/
│
├── 00_config/
│   ├── 01__tickers.py                       ← constantes, nombres de tabla y mapas XBRL
│   ├── 02__tickers_master.ipynb             ← construye main.config.tickers (S&P 500 + Russell 3000 + favoritos)
│   ├── 03__concept_hierarchy_master.ipynb   ← construye main.config.concept_hierarchy desde concept_hierarchy.json
│   ├── 04__metrics_hierarchy_master.ipynb   ← construye main.config.metrics_hierarchy desde metrics_hierarchy.json
│   ├── favorites.json                       ← lista de tickers favoritos — editar aquí directamente
│   ├── concept_hierarchy.json               ← jerarquía de conceptos contables — editar aquí directamente
│   └── metrics_hierarchy.json               ← jerarquía de derived metrics — editar aquí directamente
│
├── 10_ingestion/
│   ├── 11__fetch_sec_xbrl.py     ← SEC EDGAR → financials_raw (Delta, append-only)
│   └── 12__fetch_market_data.py  ← Yahoo Finance → market_data (precios + market cap)
│
├── 20_transformation/
│   ├── 21__clean_and_merge.py    ← MERGE into financials (clean fact table)
│   └── 22__derived_metrics.ipynb ← FCF, márgenes, ratios, YoY growth, valoración
│
├── 30_analysis/
│   └── 31__company_analysis.py   ← queries ad-hoc y validación
│
├── 40_dashboards/
│   ├── 41__dashboard_queries.py  ← SQL que alimenta el dashboard
│   └── Main Dashboard.lvdash.json
│
└── 90_pipelines/
    └── 91__full_pipeline.ipynb   ← entry point del Job — ejecuta todo en secuencia
```

### Flujo del pipeline

```
favorites.json                editar tickers favoritos (sin tocar Databricks)
concept_hierarchy.json        editar jerarquía de conceptos contables
metrics_hierarchy.json        editar jerarquía de derived metrics
      ↓
02__tickers_master              construye main.config.tickers              (manual)
      ↓
03__concept_hierarchy_master    construye main.config.concept_hierarchy    (auto)
      ↓
04__metrics_hierarchy_master    construye main.config.metrics_hierarchy    (auto)
      ↓
11__fetch_sec_xbrl              SEC API → financials_raw
      ↓
12__fetch_market_data           Yahoo Finance → market_data
      ↓
21__clean_and_merge             deduplica → MERGE into financials
      ↓
22__derived_metrics             márgenes, FCF, YoY, leverage, ratios de valoración
      ↓
31__company_analysis            queries de validación
```

`main.config.tickers` se reconstruye manualmente con `02__tickers_master` cuando edites
`favorites.json` (las fuentes de S&P 500 / Russell 3000 cambian poco). Las dos
jerarquías sí se reconstruyen automáticamente en cada run del pipeline desde sus
respectivos JSONs.

---

## Favoritos (`favorites.json`)

Los tickers favoritos se gestionan editando `00_config/favorites.json` directamente en el repositorio Git. Se incluyen siempre en la ingesta, independientemente de si están en el S&P 500 o el Russell 3000.

```json
[
  {"ticker": "TSM",  "company": "Taiwan Semiconductor", "note": ""},
  {"ticker": "ASML", "company": "ASML Holding",         "note": ""}
]
```

**Cómo añadir o quitar un ticker favorito:**
1. Edita `00_config/favorites.json` en el repo
2. Haz commit y push
3. Ejecuta `02__tickers_master` (o lanza el Job completo)

No es necesario abrir ni ejecutar ningún notebook de Databricks para gestionar favoritos.

---

## Jerarquías (`concept_hierarchy.json` y `metrics_hierarchy.json`)

Ambas jerarquías son archivos JSON en `00_config/` editables directamente desde el repo. El pipeline las re-aplana a tablas Delta en cada ejecución, así que cualquier cambio se refleja sin pasos manuales adicionales.

**`concept_hierarchy.json`** define el árbol contable (Income Statement, Balance Sheet, Cash Flow) — qué conceptos van bajo qué grupo y en qué orden aparecen en el dashboard. Soporta anidamiento variable: `section → group → concept`, con subtotales como conceptos al final de su grupo.

**`metrics_hierarchy.json`** define la organización de las derived metrics en 2 niveles fijos: `category → subcategory → metric`. Cinco categorías: Profitability, Cash Flow, Growth, Financial Health, Valuation.

**Cómo modificar una jerarquía:**
1. Edita el JSON correspondiente en `00_config/`
2. Commit + push
3. El siguiente run del pipeline reconstruirá la tabla automáticamente (o ejecuta el notebook master por separado)

---

## Tables

| Table | Description |
|---|---|
| `{CATALOG}.config.tickers` | Universo de tickers activos (S&P 500 + Russell 3000 + favoritos) |
| `{CATALOG}.config.concept_hierarchy` | Jerarquía de conceptos contables (stmt → section → group → concept) |
| `{CATALOG}.config.metrics_hierarchy` | Jerarquía de derived metrics (category → subcategory → metric) |
| `{CATALOG}.{SCHEMA}.financials` | Long-format fact table — one row per ticker / year / concept |
| `{CATALOG}.{SCHEMA}.financials_raw` | Append-only audit log of all SEC scrapes |
| `{CATALOG}.{SCHEMA}.market_data` | Year-end closing prices and market cap per ticker / year |
| `{CATALOG}.{SCHEMA}.financials_metrics` | Derived metrics — margins, FCF, YoY growth, leverage, valuation ratios |

---

## Source table — `financials`

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol (e.g. `AAPL`) |
| `company` | STRING | Company name |
| `year` | INT | Fiscal year |
| `stmt` | STRING | One of `Income Statement`, `Balance Sheet`, `Cash Flow` |
| `concept` | STRING | Financial line item name (XBRL-derived) |
| `value` | DOUBLE | Raw value in USD |

---

## Market data — `market_data`

Year-end closing prices fetched from Yahoo Finance (via `yfinance`), joined with `Shares Diluted` from `financials` to derive an annual market cap. Required by all valuation metrics.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `year` | INT | Calendar year |
| `price_close` | DOUBLE | Last closing price of the year (adjusted) |
| `shares_diluted` | DOUBLE | Diluted share count sourced from `financials` |
| `market_cap` | DOUBLE | `price_close × shares_diluted` |
| `fetched_at` | TIMESTAMP | Fetch timestamp |

---

## Metrics hierarchy — `main.config.metrics_hierarchy`

Tabla de lookup que organiza las derived metrics en categorías. Se reconstruye en cada run desde `00_config/metrics_hierarchy.json`. Joinear con `financials_metrics` por `metric` para añadir filtros de `category` / `subcategory` al dashboard y obtener orden de filas estable vía `sort_order`.

| Column | Type | Description |
|---|---|---|
| `category` | STRING | Profitability, Cash Flow, Growth, Financial Health, Valuation |
| `subcategory` | STRING | Margins, YoY, Leverage, Liquidity, Price Multiples, Enterprise Value, Absolute |
| `metric` | STRING | Nombre exacto tal como aparece en `financials_metrics.metric` |
| `unit` | STRING | `percent` / `usd` / `ratio` |
| `requires_market_data` | BOOLEAN | `true` para las métricas que dependen de `market_data` |
| `sort_order` | INT | Orden global (10, 20, 30, ...) — usar para `ORDER BY` en el dashboard |

---

## Derived metrics — `financials_metrics`

Long-format table: one row per `ticker / year / metric`. Computed by `22__derived_metrics` from `financials` and `market_data`.

```
ticker | company | year | metric          | value
-------|---------|------|-----------------|-------
AAPL   | Apple   | 2023 | Net Margin %    | 25.31
AAPL   | Apple   | 2023 | Free Cash Flow  | 99584000000
AAPL   | Apple   | 2023 | P/E             | 28.74
```

Las 17 métricas están organizadas en 5 categorías. La jerarquía completa vive en
`00_config/metrics_hierarchy.json` y se materializa en `main.config.metrics_hierarchy`
(ver sección anterior).

### Profitability — Margins

| Metric | Formula |
|---|---|
| `Gross Margin %` | `Gross Profit / Revenue × 100` |
| `Operating Margin %` | `Operating Income / Revenue × 100` |
| `Net Margin %` | `Net Income / Revenue × 100` |
| `FCF Margin %` | `Free Cash Flow / Revenue × 100` |

### Cash Flow — Absolute

| Metric | Formula |
|---|---|
| `Free Cash Flow` | `Operating Cash Flow − CapEx` |

### Growth — YoY

| Metric | Formula |
|---|---|
| `Revenue YoY %` | Year-over-year % change in Revenue |
| `Net Income YoY %` | Year-over-year % change in Net Income |
| `Operating Cash Flow YoY %` | Year-over-year % change in Operating CF |
| `Free Cash Flow YoY %` | Year-over-year % change in FCF |

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

### Valuation — Enterprise Value *(requires `market_data`)*

| Metric | Formula | Notes |
|---|---|---|
| `EV` | `Market Cap + Total Debt − (Cash & Equivalents + ST Investments)` | Enterprise value in USD |
| `EV/EBITDA` | `EV / (Operating Income + D&A)` | Outliers beyond ±500× are filtered out |

> Las métricas de valoración solo se rellenan para combinaciones ticker/año donde `market_data` tiene un `market_cap` válido. Si `12__fetch_market_data` no se ha ejecutado, estas métricas se omiten silenciosamente.

---

## Statement filters

Each dashboard statement filters on the `stmt` column and selects the relevant `concept` values:

| Statement | `stmt` filter |
|---|---|
| Income Statement | `'Income Statement'` |
| Balance Sheet | `'Balance Sheet'` |
| Cash Flow | `'Cash Flow'` |

---

## Column reference

All monetary values are in **billions USD** (divided by 1e9, rounded to 2 decimal places) for display. Per-share figures (EPS) and share counts are kept in their native units.

### Income Statement

| Concept | Display label | Unit |
|---|---|---|
| `Revenue` / `Revenue (contract)` | Revenue | $bn |
| `Cost of Revenue` | Cost of Revenue | $bn |
| `Gross Profit` | Gross Profit | $bn |
| `R&D Expense` | R&D | $bn |
| `SG&A Expense` | SG&A | $bn |
| `Operating Expenses` | Operating Expenses | $bn |
| `Operating Income` | Operating Income | $bn |
| `Interest Expense` | Interest Expense | $bn |
| `Income Before Tax` | Income Before Tax | $bn |
| `Income Tax` | Income Tax | $bn |
| `Net Income` | Net Income | $bn |
| `EPS Basic` | EPS Basic | USD |
| `EPS Diluted` | EPS Diluted | USD |
| `Shares Diluted` | Shares Diluted | bn shares |

> Revenue is coalesced from two XBRL tags (`Revenue` and `Revenue (contract)`) since companies report under different tags.

### Balance Sheet

| Concept | Display label | Unit |
|---|---|---|
| `Cash & Equivalents` | Cash & Equivalents | $bn |
| `Short-term Investments` | ST Investments | $bn |
| `Accounts Receivable` | Accounts Receivable | $bn |
| `Inventory` | Inventory | $bn |
| `Total Current Assets` | Current Assets | $bn |
| `PP&E Net` | PP&E Net | $bn |
| `Goodwill` | Goodwill | $bn |
| `Intangible Assets` | Intangibles | $bn |
| `Total Assets` | Total Assets | $bn |
| `Accounts Payable` | Accounts Payable | $bn |
| `Short-term Debt` | ST Debt | $bn |
| `Total Current Liabilities` | Current Liabilities | $bn |
| `Long-term Debt` | LT Debt | $bn |
| `Total Liabilities` | Total Liabilities | $bn |
| `Additional Paid-in Capital` | Paid-in Capital | $bn |
| `Retained Earnings` | Retained Earnings | $bn |
| `Total Stockholders Equity` | Total Equity | $bn |
| `Total Liabilities & Equity` | Total Liabilities & Equity | $bn |

### Cash Flow

| Concept | Display label | Unit | Notes |
|---|---|---|---|
| `Net Income` | Net Income | $bn | Reconciliation starting point |
| `Depreciation & Amortization` | D&A | $bn | |
| `Stock-based Compensation` | SBC | $bn | |
| `Changes in Working Capital` | Working Capital Change | $bn | |
| `Operating Cash Flow` | Operating CF | $bn | |
| `CapEx` | CapEx | $bn | |
| `Acquisitions` | Acquisitions | $bn | |
| `Purchases of Investments` | Purchases of Investments | $bn | |
| `Sales of Investments` | Sales of Investments | $bn | |
| `Investing Cash Flow` | Investing CF | $bn | |
| `Debt Issuance` | Debt Issuance | $bn | |
| `Debt Repayment` | Debt Repayment | $bn | |
| `Dividends Paid` | Dividends | $bn | |
| `Share Repurchases` | Buybacks | $bn | |
| `Financing Cash Flow` | Financing CF | $bn | |
| `Net Change in Cash` | Net Change in Cash | $bn | |

> Free Cash Flow is derived at query time as `Operating CF − CapEx` and is not stored as a concept in `financials`.

---

## Design decisions

**No wide tables or views.** An earlier iteration of this project materialised the three statements as Delta wide tables (one column per concept), and later as Delta views. Both approaches were discarded in favour of querying `financials` directly, which keeps the pipeline simpler and avoids any schema drift between the fact table and its derivatives.

**Favorites gestionados en Git, no en Delta.** Una iteración anterior usaba una tabla Delta (`main.config.favorites`) gestionada desde un notebook (`02__favorites`). Se simplificó a un archivo `favorites.json` en el repositorio, lo que permite editar la lista de favoritos directamente desde el editor o GitHub sin necesidad de abrir Databricks.

**Jerarquías en JSON, no en código.** Tanto la jerarquía contable (`concept_hierarchy.json`) como la de derived metrics (`metrics_hierarchy.json`) viven en archivos JSON declarativos en `00_config/`. Cada uno tiene su notebook master correspondiente que las aplana a una tabla Delta de lookup. Esto desacopla la estructura (orden, agrupación, categorías) del código de transformación, así que un cambio de categoría o de orden no requiere tocar Python.
