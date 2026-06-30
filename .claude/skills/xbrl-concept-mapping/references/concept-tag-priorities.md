# Concept tag priorities & synonyms (reference)

Ground-truth snapshot of the coalescing config in
`fundamentals_pipeline/00__config/01__tickers.py`. Re-read the file before relying on these — it
evolves. Values below were captured against the file at skill-authoring time.

## `kind` taxonomy

| kind | statements | quarterly derivation |
|---|---|---|
| `flow_additive` | Income Statement, Cash Flow | Q1–Q3 standalone or YTD deltas; Q4 = FY − YTD_Q3 |
| `flow_nonadditive` | Income Statement (EPS, shares) | standalone only; Q4 NULL (cannot sum) |
| `stock` | Balance Sheet | snapshot at period_end; dedup by (ticker, concept, period_end) |

## Mechanism 1 — tag-fallback lists (`extract_series_multi`)

Resolved at **ingestion** in `11__fetch_sec_xbrl.py`. One display name, a priority-ordered list of
candidate tags; per period the lowest-index tag present wins.

```python
"Short-term Debt": (["DebtCurrent", "LongTermDebtCurrent", "ShortTermBorrowings"], "stock")
"Long-term Debt":  (["LongTermDebtNoncurrent", "LongTermDebt",
                     "LongTermDebtAndCapitalLeaseObligations"], "stock")
```

Single-tag concepts take the fast path (`extract_series_multi` returns immediately when given a
`str`), so ~99% of concepts incur no overhead.

## Mechanism 2 — synonym collapse + priority tie-break

Resolved at **merge** in `21__clean_and_merge.py`. Separate single-tag display names collapse to a
canonical name, with `CONCEPT_PRIORITY` breaking ties when variants coexist in the same fiscal year.

### `CONCEPT_PRIORITY` (lower = preferred)

```
Net Income                     0      Revenue                       0
Net Income (to common)         1      Revenue (contract)            1
Net Income (incl NCI)          2      Revenue (contract incl tax)   2
Total Stockholders Equity      0      Revenue (sales net)           3
Total Equity (incl NCI)        1      Revenue (sales goods)         4
Operating Cash Flow            0      Revenue (sales services)      5
Operating Cash Flow (cont ops) 1      Revenue (oil & gas)           6
Interest Expense               0      Revenue (bank)                7
Interest Expense (nonoperating) 1
Interest Expense (incl debt)    2
```

### `CONCEPT_SYNONYMS` (variant → canonical)

```
Revenue (contract)            -> Revenue
Revenue (contract incl tax)   -> Revenue
Revenue (sales net)           -> Revenue
Revenue (sales goods)         -> Revenue
Revenue (sales services)      -> Revenue
Revenue (bank)                -> Revenue
Revenue (oil & gas)           -> Revenue
Net Income (to common)        -> Net Income
Net Income (incl NCI)         -> Net Income
Total Equity (incl NCI)       -> Total Stockholders Equity
Operating Cash Flow (cont ops)-> Operating Cash Flow
Interest Expense (nonoperating)-> Interest Expense
Interest Expense (incl debt)  -> Interest Expense
```

> The live `main.financials.financials` table stores the **canonical** name (post-collapse), so
> compare validators and dashboards against canonical concepts, not the variant display names.

## Concepts deliberately NOT in CONCEPT_PRIORITY

Operating Income, Gross Profit, and Income Tax are single-tag concepts — `CONCEPT_PRIORITY` is a
no-op for them. Their residual Q-sum divergence is genuine data lumpiness, not a mapping gap
(settled; see the project's quarter-divergence note). Don't add them to `CONCEPT_PRIORITY`
expecting a fix.

## Other constants

- `QUARTERLY_WINDOW = 12` — rolling window of quarters kept per ticker (`21c__prune_quarterly.py`).
- `SEC_USER_AGENT` — must be a real org + email or SEC returns 403.
