# Fundamentals Analytics — Databricks Pipeline

End-to-end Databricks pipeline that ingests XBRL financial filings from SEC EDGAR, fetches market data from Yahoo Finance, and serves Income Statement, Balance Sheet, Cash Flow, and derived financial metrics via direct queries on Delta tables.

```
favorites.json / concept_hierarchy.json   ← edit tickers & concept order here
        ↓
02__tickers_master          build main.config.tickers
        ↓
03__concept_hierarchy_master  build main.config.concept_hierarchy
        ↓
11__fetch_sec_xbrl          SEC API → financials_raw
        ↓
12__fetch_market_data       Yahoo Finance → market_data
        ↓
21__clean_and_merge         deduplicate → MERGE into financials
        ↓
22__derived_metrics         margins, FCF, YoY growth, leverage, valuation ratios
        ↓
31__company_analysis        validation queries
```

---

## Project structure

```
fundamentals_databricks_pj/
│
├── 00_config/
│   ├── 01__tickers.py                ← catalog/schema constants + XBRL concept maps
│   ├── 02__tickers_master.ipynb      ← builds main.config.tickers (S&P 500 + Russell 3000 + favorites)
│   ├── 03__concept_hierarchy_master  ← builds main.config.concept_hierarchy
│   ├── favorites.json                ← extra tickers to always include — edit here
│   └── concept_hierarchy.json        ← display order and grouping for the dashboard
│
├── 10_ingestion/
│   ├── 11__fetch_sec_xbrl.py         ← SEC EDGAR XBRL API → financials_raw (append-only)
│   └── 12__fetch_market_data.py      ← Yahoo Finance (yfinance) → market_data
│
├── 20_transformation/
│   ├── 21__clean_and_merge.py        ← MERGE into financials (idempotent)
│   └── 22__derived_metrics.ipynb     ← FCF, margins, YoY growth, leverage, valuation ratios
│
├── 30_analysis/
│   └── 31__company_analysis.py       ← ad-hoc queries and validation
│
├── 40_dashboards/
│   ├── 41__dashboard_queries.py      ← SQL feeding the Databricks dashboard
│   └── Main Dashboard.lvdash.json    ← dashboard definition
│
└── 90_pipelines/
    └── 91__full_pipeline.ipynb       ← Job entry point — runs all steps in sequence
```

---

## Delta tables

| Table | Description |
|---|---|
| `main.config.tickers` | Master ticker universe (S&P 500 + Russell 3000 + favorites) |
| `main.config.concept_hierarchy` | Display order and grouping for dashboard concepts |
| `main.financials.financials_raw` | Append-only audit log of all SEC XBRL scrapes |
| `main.financials.financials` | Clean long-format fact table — one row per ticker / year / concept |
| `main.financials.market_data` | Year-end closing prices and market cap per ticker / year |
| `main.financials.financials_metrics` | Derived metrics — margins, FCF, YoY growth, leverage, valuation ratios |

---

## `financials` — source fact table

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol (e.g. `AAPL`) |
| `company` | STRING | Company name |
| `year` | INT | Fiscal year |
| `stmt` | STRING | One of `Income Statement`, `Balance Sheet`, `Cash Flow` |
| `concept` | STRING | Financial line item name (XBRL-derived) |
| `value` | DOUBLE | Raw value in USD |

---

## `market_data` — year-end prices & market cap

Year-end adjusted closing prices fetched from Yahoo Finance via `yfinance`, joined with `Shares Diluted` from `financials` to compute an annual market cap. Required by all valuation metrics.

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol |
| `year` | INT | Calendar year |
| `price_close` | DOUBLE | Last adjusted closing price of the year |
| `shares_diluted` | DOUBLE | Diluted share count sourced from `financials` |
| `market_cap` | DOUBLE | `price_close × shares_diluted` |
| `fetched_at` | TIMESTAMP | Fetch timestamp |

---

## `financials_metrics` — derived metrics

Long-format table: one row per `ticker / year / metric`. Computed by `22__derived_metrics` from `financials` and `market_data`.

```
ticker | company | year | metric          | value
-------|---------|------|-----------------|----------
AAPL   | Apple   | 2023 | Net Margin %    |     25.31
AAPL   | Apple   | 2023 | Free Cash Flow  | 99584000000
AAPL   | Apple   | 2023 | P/E             |     28.74
```

### Margins

| Metric | Formula |
|---|---|
| `Gross Margin %` | `Gross Profit / Revenue × 100` |
| `Operating Margin %` | `Operating Income / Revenue × 100` |
| `Net Margin %` | `Net Income / Revenue × 100` |
| `FCF Margin %` | `Free Cash Flow / Revenue × 100` |

### Cash flow

| Metric | Formula |
|---|---|
| `Free Cash Flow` | `Operating Cash Flow − CapEx` |

### YoY growth

| Metric | Formula |
|---|---|
| `Revenue YoY %` | Year-over-year % change in Revenue |
| `Net Income YoY %` | Year-over-year % change in Net Income |
| `Operating Cash Flow YoY %` | Year-over-year % change in Operating CF |
| `Free Cash Flow YoY %` | Year-over-year % change in FCF |

### Leverage & liquidity

| Metric | Formula |
|---|---|
| `Debt / Equity` | `(LT Debt + ST Debt) / Total Stockholders Equity` |
| `Debt / Assets` | `(LT Debt + ST Debt) / Total Assets` |
| `Current Ratio` | `Total Current Assets / Total Current Liabilities` |

### Valuation ratios *(requires `market_data`)*

| Metric | Formula | Notes |
|---|---|---|
| `P/E` | `Market Cap / Net Income` | |
| `P/S` | `Market Cap / Revenue` | |
| `P/FCF` | `Market Cap / Free Cash Flow` | |
| `EV` | `Market Cap + Total Debt − (Cash & Equivalents + ST Investments)` | Enterprise value in USD |
| `EV/EBITDA` | `EV / (Operating Income + D&A)` | Outliers beyond ±500× filtered out |

> Valuation ratios are only populated for ticker/year combinations where `market_data` has a valid `market_cap`. If `12__fetch_market_data` has not run, these metrics are skipped gracefully.

---

## Favorites (`favorites.json`)

Tickers in `00_config/favorites.json` are always included in the ingestion, regardless of whether they appear in the S&P 500 or Russell 3000 universe.

```json
[
  {"ticker": "TSM",  "company": "Taiwan Semiconductor", "note": ""},
  {"ticker": "ASML", "company": "ASML Holding",         "note": ""}
]
```

To add or remove a ticker: edit the file, commit and push, then run `02__tickers_master` (or trigger the full Job). No Databricks notebooks need to be opened manually.

---

## Pipeline parameters

The full pipeline (`91__full_pipeline`) accepts Databricks Job parameters at runtime:

| Parameter | Default | Description |
|---|---|---|
| `tickers_override` | *(empty)* | Comma-separated list of tickers — bypasses `main.config.tickers` |
| `run_optimization` | `false` | Run `OPTIMIZE + VACUUM` on Delta tables at the end of the pipeline |
| `rebuild_config` | `false` | Flag for future use — ticker rebuild must still be run manually via `02__tickers_master` |

Example:
```json
{"tickers_override": "AAPL,TSLA,MSFT", "run_optimization": "false"}
```

---

## Column reference

All monetary values are displayed in **billions USD** (divided by 1e9, rounded to 2 decimal places). Per-share figures (EPS) and share counts are kept in their native units.

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

> Revenue is coalesced from two XBRL tags (`Revenues` and `RevenueFromContractWithCustomerExcludingAssessedTax`) since companies report under different tags.

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

> Free Cash Flow is derived at compute time as `Operating CF − CapEx` and is not stored as a concept in `financials`.

---

## Design decisions

**No wide tables or views.** An earlier iteration materialised the three statements as Delta wide tables (one column per concept), and later as Delta views. Both approaches were discarded in favour of querying `financials` directly, which keeps the pipeline simpler and avoids schema drift between the fact table and its derivatives.

**Append-only raw layer.** `financials_raw` is never updated or deleted — every SEC scrape is appended with a `fetched_at` timestamp. The clean `financials` table is maintained via an idempotent `MERGE` in `21__clean_and_merge`, which handles SEC restatements (UPDATE) and new data (INSERT) without touching unchanged rows.
