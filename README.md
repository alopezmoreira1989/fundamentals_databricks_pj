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

## Tables

| Table | Description |
|---|---|
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

## Derived metrics — `financials_metrics`

Long-format table: one row per `ticker / year / metric`. Computed by `22__derived_metrics` from `financials` and `market_data`.

```
ticker | company | year | metric          | value
-------|---------|------|-----------------|-------
AAPL   | Apple   | 2023 | Net Margin %    | 25.31
AAPL   | Apple   | 2023 | Free Cash Flow  | 99584000000
AAPL   | Apple   | 2023 | P/E             | 28.74
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
| `EV/EBITDA` | `EV / (Operating Income + D&A)` | Outliers beyond ±500× are filtered out |

> Valuation ratios are only populated for ticker/year combinations where `market_data` has a valid `market_cap`. If `12__fetch_market_data` has not been run, these metrics are skipped gracefully.

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