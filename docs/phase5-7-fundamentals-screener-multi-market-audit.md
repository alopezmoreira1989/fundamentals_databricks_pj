# Phase 5.7 — `fundamentals_screener` Multi-Market Audit

**Audit only. No production code was changed to produce this document.** Scope: determine
exactly what `fundamentals_screener` needs before it can correctly display the 8 European
companies Phase 5.6 published (FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP), without breaking
existing US/CA behavior, the public URL contract, or the external consumer
(`alopezmoreira1989/alopezm_my_website`).

Every finding below is grounded in the actual current code — file and line cited — not in
assumption. Where a finding could not be verified against real synced data (no local
`FUNDAMENTALS_DATA_PATH` cache with a post-Phase-5.6 export exists in this environment), the
finding is instead traced analytically through the real pipeline SQL and the real screener code
path, end to end, and flagged as such.

---

## 1. Executive summary

`fundamentals_screener` was built for a US/Canada universe where a bare ticker is a safe global
identity and every monetary figure is implicitly USD (or, since the Canada work, one of a
`{US: USD, CA: CAD}` pair). Phase 5.6 published 8 European companies into the same
`dashboard_*` artifacts the app already reads, with `market='EU'`, real ISO country names, MIC
exchange codes, and `reporting_currency='EUR'`. The good news first: the **descriptive** layer
(General Screener filters, sorting, pagination, the DuckDB connection/artifact-loading layer)
is already fully data-driven and needs **zero** code changes — the 8 EU companies will appear
in the General Screener, filterable by their real country/market, the moment the data syncs.

The bad news is concentrated in one place: **currency display**. The app has exactly one
currency-aware code path (Market Cap, built specifically for the Canada work) and one
currency-*unaware* default (`unit == "usd"` → hardcoded `$`) that nearly every other monetary
figure — Revenue, Net Income, EPS-derived valuation estimates, the whole Income Statement/
Balance Sheet/Cash Flow, the football field, Net-Net Finder's Price/NCAV/Market Cap columns —
falls through to. For a EUR-reporting company this is not a cosmetic nit: it labels real euro
figures as dollars, on nearly every tab. This was **already latent for CAD-native Canadian
reporters** (a pre-existing bug Phase 5.6 makes far more visible, not something Phase 5.6
introduced) — one hardcoded `${{ ... }}` in the Net-Net Finder template is the single most
concrete example, proven wrong for Canada today, not just a EU risk.

A second, structural finding: the app has **no defenses of its own** against a same-ticker
cross-market collision — every lookup, cache key, and in-memory dict is keyed on bare `ticker`
alone. Today this is safe only because the pipeline's export-time
`check_no_export_ticker_collision()` guard rejects a real collision before publish, and because
none of the 8 real EU tickers happen to collide with a US/CA ticker. This is a known, accepted,
and explicitly-scoped architectural decision (ADR-0012: `ticker` stays the app's routing/display
key, `MIC:ISIN` becomes the pipeline's internal identity, the app's URL contract is explicitly
declared out of scope for that ADR) — but ADR-0012 itself leaves the collision risk as an
**open question**, "not proven absent in general." Phase 5.7 does not need to (and, per
ADR-0012, should not) redesign the URL contract; it should decide whether the upstream guard is
sufficient defense-in-depth on its own, or whether `fundamentals_screener` needs its own
internal safety net.

Third: the Quarterly and Filings tabs are more US-centric in *labeling and data source* than in
*query logic*. The Quarterly tab's SQL is already generic (any non-`'FY'` `period_type` renders
correctly) but is moot today: ESEF ingestion writes `period_type='FY'` only — no interim EU
period exists in the data yet, so the tab silently doesn't render for any of the 8 companies
(degrades safely, but the "Interim" generalization the audit prompt anticipated is currently a
pipeline gap, not a screener gap). The Filings tab is hard-wired to SEC only, at both the data
layer (`dashboard_filings` has zero rows for any non-SEC-filed ticker, by construction — no
ESEF/FIRDS writer exists) and the copy layer ("SEC filings · 10-K & 10-Q", filter pills
`10-K`/`10-Q` only). It degrades to an empty tab for the 8 EU companies today, not a crash — but
a real, separate data-integrity risk was found in the same code path (§7).

Fourth, and the one piece of unambiguously good news beyond the General Screener: the null
semantics discipline is already sound. `Shares Diluted` and every other legitimately-missing
ESEF figure renders as an em dash, not a fabricated zero — confirmed at the filter layer too
(`value IS NOT NULL` gates, never a silent pass), and the Investor Presets code already has a
working precedent (Buffett's MoS NULL-passthrough for sector-inapplicable metrics) for
distinguishing "structurally not applicable" from "failed the bar."

None of the 8 companies are unable to render at all. Every gap found is either an honest empty
state (Filings, Quarterly) or a mislabeled-but-numerically-correct figure (currency). Nothing
found requires touching the public URL contract, the DTO shapes, or the external consumer.

---

## 2. Current architecture / data flow

```
FIRDS (ESMA) → eu_admission_candidates (admitted only)
                        │
SEC/EDGAR (US/CA)  config.tickers          ESEF filings → 16__fetch_eu_xbrl.py
        │                │                              │
        └── financials ──┴──────────────────────────────┘   (both branches write the
                 │                                            SAME financials table,
                 ▼                                            keyed on bare `ticker`)
        22__derived_metrics.py / 23__intrinsic_value.py
                 │
                 ▼
        51__export_dashboard_data.py
          UNION ALL:
            config.tickers (US/CA)  ──┐
            eu_admission_candidates ──┴──► tickers_df → dashboard_meta.json["tickers"]
          financials/financials_metrics/market_prices_daily → dashboard_data /
          dashboard_metrics / dashboard_prices  (bare `ticker` column, NO market/country/
          exchange/currency column in these three — only in dashboard_meta.json)
                 │
                 ▼  GitHub Release "latest" (parquet + meta JSON)
                 │
        fundamentals_screener/data_source.py (cron sync, no live-request network)
                 │
        repository.py (DuckDB, one view per artifact, generic)
                 │
        repositories/*.py → dtos.py (frozen dataclasses, no market/currency-of-value field
                                       except HeadlineKpi.currency / ScreenTableRow.units)
                 │
        services.py (compose repositories, no financial logic of its own)
                 │
        views.py (HTTP only) → templates (Bootstrap + Chart.js/Lightweight Charts)
```

Only `dashboard_meta.json`'s per-ticker record carries `market`/`country`/`exchange`/
`reporting_currency`/`accounting_standard`. The four bulk artifacts
(`dashboard_data`/`dashboard_metrics`/`dashboard_prices`/`dashboard_forecast`, plus
backtest/fx/filings) key everything by bare `ticker` only — confirmed directly in
`fundamentals_pipeline/50__publish/51__export_dashboard_data.py:105-134` (the meta UNION ALL)
vs. `:228-258` (financials slice, ticker-only) and `:270-289` (metrics slice, ticker-only).

---

## 3. Public contracts

Per `fundamentals_screener/urls.py:16-23`:

```python
path("", views.screen, name="screen"),
path("data/", views.screen_data, name="screen_data"),
path("about/", views.about, name="about"),
path("<str:ticker>/", views.company_detail, name="company_detail"),
path("<str:ticker>/data/", views.company_data, name="company_data"),
path("<str:ticker>/news/", views.company_news, name="company_news"),
```

The package's own README (`fundamentals_screener/README.md:197-202`) states this explicitly:
"`urls.py`'s route names, the template filenames under `templates/fundamentals_screener/`, and
the shape of `dtos.py` are what any consuming project's own templates/overrides couple against.
Changing any of them is a breaking change."

**Classification of every contract surface touched by this audit's findings:**

| Contract surface | Classification | Why |
|---|---|---|
| `/<str:ticker>/` route shape | **SAFE — do not touch** | ADR-0012 explicitly rules this in scope-out: "`fundamentals_screener`'s URL routes and the Streamlit app's ticker-based lookups are equally untouched... a listing's `ticker` attribute is still available for that purpose." No finding in this audit requires changing the route. |
| `dtos.py` field additions (e.g. a `currency` field on `Statement`/`NetNetRow`) | **INTERNAL CHANGE**, not breaking, *if additive* | README's own contract is about the *shape* consumers couple to; adding an optional field to a dataclass that templates already read by attribute is additive. Removing/renaming an existing field (e.g. `FilingRow.form`) would be breaking — no finding here proposes that. |
| Template filenames | **SAFE** | No finding proposes renaming a template file. |
| `company_data`/`screen_data` JSON shape | **INTERNAL CHANGE if additive, BREAKING if a key changes meaning** | `company_data` (`views.py:901-920`) already omits `market`/`currency` from its JSON — adding them is additive. |
| `currency.py`'s `QUOTE_CURRENCY_BY_MARKET` map | **SAFE to extend** | Internal helper, not part of the versioned contract; adding `"EU"` (or better, generalizing it) is a pure bugfix, not a contract change. |

No finding in this audit requires a BREAKING change to any public contract. This significantly
narrows Phase 5.7's implementation risk: everything found can be fixed as additive DTO fields,
internal helper generalizations, or template copy, without a version bump beyond a normal minor
release.

---

## 4. Identity / ticker audit

**Full findings** (background investigation, cross-checked against ADR-0012 and
`51__export_dashboard_data.py` directly):

`fundamentals_screener` treats a bare `ticker` string as the sole identity key everywhere — in
every SQL `WHERE ticker = ?` / `GROUP BY` / `PARTITION BY ticker`, every Python dict keyed by
ticker, the URL contract, and the external Yahoo-news cache key. Nothing in the app carries
`market` (or the pipeline's `MIC:ISIN`) as part of any lookup or dedup key.

The 8 EU companies do **not** reach the dashboard as `MIC:ISIN` — confirmed directly in
`51__export_dashboard_data.py:120-131`: the EU branch's `SELECT` publishes `e.ticker` (the
short resolved mnemonic, e.g. `FCC`, set by `apply_ticker_enrichment` in
`fundamentals_pipeline/sources/eu_admission.py`) into the exact same `ticker` column the US/CA
branch uses. ISIN/`mic`/`listing_id` exist internally in `eu_admission_candidates` but the
export's `SELECT` never pulls them — they never reach any published artifact.

Today this is safe **only because upstream enforces it**, not because the app is market-aware:
`fundamentals_pipeline/identity.py`'s `check_no_export_ticker_collision()`
(`identity.py:301-318`) hard-fails the export if any ticker string appears in both branches, and
its own comment in `51__export_dashboard_data.py:139-150` names this app explicitly:
"corrupting every downstream `{ticker: record}` dict this export (and fundamentals_screener's
own repositories, per the Phase 5.5 audit) builds. Not observed today (FCC/FCT are absent from
config.tickers, confirmed live)." ADR-0012 (`docs/adr/0012-listing-identity-key.md`, "Open
questions") independently and explicitly leaves this as unresolved: "Whether a same-ticker-root
share-class collision risk exists anywhere in the real European universe FIRDS would surface...
not proven absent in general."

**Every ticker-keyed construct in the app is IDENTITY-risky if that upstream guard is ever
bypassed, or the EU universe grows past 8 hand-picked names into overlap territory:**

- Every DuckDB view (`dashboard_metrics`, `dashboard_data`, `dashboard_prices`,
  `dashboard_forecast`, `dashboard_filings`) is filtered/partitioned by `ticker` alone, no
  `market` predicate anywhere in `repositories/*.py`. A collision would silently interleave two
  companies' statements/prices/metrics in the same result set — a data-merge corruption, not a
  crash.
- `CompanyRepository.get_summary()` (`repositories/companies.py:350-372`) linear-scans meta
  records and returns the **first** ticker match — the identity resolver behind
  `company_detail`, `company_data`, `company_news`, `net_net_snapshot`, `get_forecast_chart`.
  A second same-ticker company would simply be invisible.
- In-memory dicts that would clobber on collision: `company_listing.py:436`
  (`by_ticker = {rec.get("ticker",""): rec for rec in scope}`, feeds the metric-page and
  net-net rows), `company_listing.py:629` (same pattern in `net_net_screen`),
  `companies.py:304-309` `_peer_roster()`, `services.py:336-369`/`404-409` (Net-Net Finder's
  sort/ratio dicts).
- `news.py:80`'s Yahoo-headline cache key, `f"fundamentals_screener:news:{ticker}:{limit}"`, is
  ticker-only — two companies sharing a ticker would read/serve each other's cached news
  (Django's cache framework, up to the configured TTL — worse under the README-recommended
  persistent `FileBasedCache` than the CGI default, since the CGI default has no shared cache
  process either).
- Yahoo RSS (`news.py:35`) and Logo.dev (`templatetags/logos.py:19-28`) both key external
  lookups on the bare ticker — low-stakes (wrong logo/no news degrade gracefully), but also
  **already broken for non-US symbols regardless of collision**: neither appends a market
  suffix, so a Canadian ticker's Yahoo RSS feed is already wrong today (needs `RY.TO`, gets
  `RY`) — the same gap will apply identically to the 8 EU tickers (needs `ALO.PA`, gets `ALO`).

**Templates**: every `{% url 'fundamentals_screener:company_detail' ... %}` call site checked
(`_screen_results.html`, `_netnet_content.html`, `_presets_content.html`,
`company_detail.html:385`) consistently reverses from `row.ticker`/`s.ticker` — no divergent
field used anywhere, so there is no template-level identity bug independent of the
already-cited backend one.

**Classification**: the URL contract itself is SAFE (per ADR-0012, not to be touched). The
in-app collision exposure is a real, if currently dormant, **P1/MEDIUM** finding — not P0,
because the upstream guard is a real, tested, currently-effective defense, and the 8 real
tickers are confirmed non-colliding. It becomes urgent only if/when the EU admission universe
scales meaningfully past a hand-picked pilot set.

---

## 5. Currency audit

This is the audit's largest and most concrete finding set. Three independent investigations
(direct reading + two background agents) converged on the same root cause from different
entry points, which is itself a useful cross-check.

### 5.1 The root cause

`fundamentals_screener/currency.py:10,15`:

```python
QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}

def quote_currency(market: str | None) -> str:
    return QUOTE_CURRENCY_BY_MARKET.get((market or "US").upper(), "USD")
```

European rows carry `market = 'EU'` (`51__export_dashboard_data.py:126`, `'EU' AS market`).
`quote_currency("EU")` falls through the `.get(..., "USD")` default and returns `"USD"`. This
value flows directly into the Price tab: `views.py:782`
(`price_currency = quote_currency(summary.market).lower()`) →
`company_detail.html:398` (`{{ price_chart.last_close|metric_value:price_currency }}`) →
`templatetags/fmt.py:51-52` (`if u == "usd": return f"${value:,.2f}"`). **A EUR close price
renders with a `$` prefix.**

### 5.2 The systemic amplifier: `unit == "usd"` as a generic label, not a currency

The deeper issue is upstream of the screener entirely. `fundamentals_pipeline/00__config/
metrics_hierarchy.json` sets `"unit": "usd"` as a **generic "monetary, in the company's own
reporting currency" label** on nearly every dollar-denominated metric — Revenue, Net Income,
Free Cash Flow, NCAV, Graham Number, DCF Value per Share, Owner Earnings, Tangible Book Value,
EV — identically for US, CA, and EU rows (confirmed: ~40+ occurrences of literal `"unit": "usd"`
across the file). Only `market_cap_asof`/`market_cap_live`/the Live-NCAV metrics use the real
per-row currency code (`LOWER(md.currency)`, `51__export_dashboard_data.py:306,341`) as their
unit — a deliberate fix built specifically for the Canada currency-alignment work, never
extended to any other metric.

`templatetags/fmt.py:51-52`'s `metric_value` filter trusts this label literally: `unit == "usd"`
→ hardcoded `$`. Since nearly every metric in `dashboard_metrics` carries that literal string
regardless of the company's real `reporting_currency`, **every monetary figure except Market
Cap renders with a hardcoded `$`, for every company** — this was already wrong for CAD-native
Canadian statement figures (Revenue, Net Income, etc., not just EU), Phase 5.6 just makes it
land on 100% of a new market's figures instead of a subset of one existing market's.

### 5.3 Confirmed concrete instances (file:line, with quote)

| # | File:line | Quote | Company-visible effect |
|---|---|---|---|
| 1 | `currency.py:10,15` | `QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}` / `.get(..., "USD")` | Price tab close price mislabeled `$` for all 8 EU tickers |
| 2 | `views.py:782` | `price_currency = quote_currency(summary.market).lower()` | propagates #1 into the template |
| 3 | `templatetags/fmt.py:51-52` | `if u == "usd": return f"${value:,.2f}"` | the actual `$`-hardcoding mechanism, fed bad data by #2 and by the metrics_hierarchy `unit` label |
| 4 | `services.py:213-225` `headline_kpis()` | builds `HeadlineKpi(label=label, value=value, fiscal_year=year)` — **no `currency=` argument** | Revenue/Net Income/Total Assets/Operating CF headline KPI cards (Overview tab) always render with `$` via `compact_money_ccy`'s `currency or "USD"` default (`fmt.py:84`) |
| 5 | `company_detail.html:531` | `{{ b.bear\|metric_value:"usd" }} – {{ b.mid\|metric_value:"usd" }} – {{ b.bull\|metric_value:"usd" }}` | the valuation football field (Graham/DCF/Owner Earnings bear-mid-bull) hardcodes `"usd"` as a literal filter argument, not even reading a unit from data — always `$`, for every company |
| 6 | `_netnet_content.html:102-103` | `{{ item.row.price\|metric_value:"usd" }}` / `{{ item.ncav_per_share\|metric_value:"usd" }}` | Net-Net Finder's Price and NCAV/Share columns always `$` |
| 7 | `_netnet_content.html:132` | `${{ item.row.market_cap\|compact_money }}` | **a literal `$` character in the template**, bypassing the `compact_money_ccy` filter that exists specifically to prevent this — the single most direct piece of evidence in this audit, and already provably wrong today for any CAD-native Canadian net-net candidate, not just a EU risk |
| 8 | `views.py:273` | `show_usd_toggle = "CA" in markets` | General Screener's "View Market Cap in USD" toggle is gated on Canada's literal presence, not on "is any non-USD currency present" — cosmetically stale now that `services.available_markets()` (`company_listing.py:318-326`) can return `"EU"` too (functionally harmless today only because CA already exists in the universe) |
| 9 | `company_listing.py:579-580` | `if usd_lens and "Market Cap" in display_metrics and rows: rows = self._apply_usd_lens(con, rows)` | runs **after** filter/sort/count (`:541,551-563`) — the USD-lens toggle is cosmetic-only; a metric filter/sort bound in the General Screener still compares raw, un-converted values across currencies |
| 10 | `services.py:336-353` `_net_net_sort_value` | `if key == "price": return row.price` (also `ncav_per_share`, `market_cap`) | user-selectable Net-Net Finder column sorts compare raw absolute values across currencies; only the default `discount` sort (a ratio) is currency-safe |
| 11 | `static/.../forecasting.js:64-67`, `balance_sheet_chart.js:39-41`, `statement_charts.js:85` | `return "$" + (v/1e9).toFixed(1) + "B"` / `": $" + compact(...)` | three Chart.js chart scripts hardcode `$` client-side with zero currency data passed from the server at all |

### 5.4 What already works correctly (don't re-fix this)

- `services.get_market_cap_kpi()` (`services.py:228-246`) is fully currency-correct: reads the
  real per-row `unit` (native currency), and — only when the USD-lens toggle is on **and** a
  same-date FX rate exists — converts via `fundamentals_pipeline.fx.convert_price` using
  `repo.usd_fx_rate()`, which itself degrades to `None` (never a guessed rate) on a missing
  pair. This is the one metric Phase 5.6/the Canada work already made fully multi-currency.
- `ScreenTableRow.units` (General Screener, Investor Presets) is architecturally correct — a
  per-row, per-column unit map, not a column-wide assumption — it just inherits bad *data*
  (the generic `"usd"` label) for every metric except Market Cap. Fixing the metrics_hierarchy
  unit labels upstream would make this path correct with **zero screener code changes**.
- Investor Presets' filter thresholds (`company_listing.py:149-228`, Graham/Buffett/Lynch) are
  entirely ratio/percentage-based (P/E, P/B, Debt/Equity, margins, ROE, PEG) — no absolute-
  dollar threshold anywhere, so Presets' eligibility logic itself is currency-safe by
  construction, independent of any display fix.
- `company_detail.html:352`'s headline-KPI-adjacent derived-metrics rows already use
  `compact_money_ccy:k.currency` — the safe filter exists and is correctly wired in at least
  one place; it just isn't used everywhere it should be (§5.3 items 4, 6, 7).

**Priority**: items 1-7 are **P0 — incorrect financial interpretation on the company's own
detail page**, the tab a user is most likely to actually read numbers from. Items 8-11 are
**P1-P2** (toggle staleness / silently-cross-currency sort — misleading, not mislabeling).

---

## 6. Interim-period audit

The Quarterly tab's SQL is more generic than its UI copy:

- `CompanyRepository.get_quarterly()` / `_QUARTERLY_SQL` (`repositories/companies.py:311-320,
  574-616`) filters only `period_type <> 'FY'` — no `IN ('Q1','Q2','Q3','Q4')` enum, no
  assumption of exactly 4 periods/year. Column labels are built directly from the data,
  `f"{period_type} {int(fiscal_year)}"` (`companies.py:587`) — an `"H1 2026"` column would
  render correctly today if the data ever produced one.
- **But it won't, yet**: `fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py:363-380`
  writes ESEF records with `"fp": "FY"` hardcoded — no semi-annual/interim value is produced
  anywhere in the EU ingestion path today. `dashboard_data`'s `period_type` is a free string in
  the schema (`fundamentals_pipeline/artifacts.py:24-36` — `{"string"}`, no enum constraint),
  but empirically the pipeline only ever writes `Q1`/`Q2`/`Q3`/`Q4`/`FY` (US/CA) and now `FY`
  only (EU). Result: `_QUARTERLY_SQL`'s `period_type <> 'FY'` filter returns **zero rows** for
  all 8 EU tickers today, and `company_detail.html:482`'s `{% if quarterly.lines %}` guard
  means the tab simply doesn't render — a clean, honest empty state, not a bug.
- The UI copy hardcodes the word "Quarterly" regardless: tab label
  (`company_detail.html:309`, `>Quarterly<`) and eyebrow text
  (`company_detail.html:491`, `{{ quarterly.name }} · by fiscal quarter`). Harmless while the
  tab never renders for EU tickers, but would read misleadingly the moment interim EU data
  exists (an "H1 2026" grid under an eyebrow that says "by fiscal quarter").
- **Fiscal-year-end display**: no code path hardcodes December 31 — `period_end`-driven value
  computation is correct for Alstom's real March 31 fiscal year (confirmed: no "Dec 31"/
  "month == 12" match anywhere in `services.py`/`companies.py`). But `period_end` itself is
  **never rendered anywhere in the template** — confirmed zero matches for `period_end` in
  `company_detail.html`. Every annual column header is a bare `FY {{ y }}`
  (`company_detail.html:457`, also 353/530/581/728/741). A user has no way to see that
  Alstom's "FY 2026" column actually spans April 2025–March 2026 rather than the calendar year
  — an existing ambiguity (also true for AAPL/MSFT today) that becomes materially worse once a
  non-calendar-FY European filer sits in the same comparison view (peer benchmark, Compare-to
  picker, screener table) as calendar-FY companies.

**Conclusion**: the "Interim" generalization the audit prompt anticipated is, today, **blocked
upstream, not in the screener** — the pipeline doesn't yet tag or ingest any EU semi-annual/
interim fact at all. The screener-side work (relabeling "Quarterly"→"Interim", surfacing
`period_end` next to `FY {{ y }}`) is real but low-urgency until the pipeline produces H1/9M
data to display. **Priority: P2** (misleading only in a state that doesn't exist yet) for the
relabeling itself; **P1** for surfacing `period_end` (a real, present-day ambiguity for Alstom
even on FY-only data).

---

## 7. Reports / filings audit

Hard-wired to SEC at both the data layer and the copy layer:

- `_FILINGS_SQL` (`repositories/companies.py:190-227`) reads `dashboard_filings` only, and its
  `CASE` logic recognizes exactly `'10-K'`/`'10-Q'` — anything else falls to
  `period_type = NULL`. `FilingRow`'s own docstring (`dtos.py:41-49`) and `get_filings`'s
  (`companies.py:374-383`) both describe it as "real SEC 10-K/10-Q filings," not generically.
- Template copy is equally hard-wired: `company_detail.html:710`
  (`SEC filings · 10-K &amp; 10-Q`), filter pills `All`/`10-K`/`10-Q`
  (`company_detail.html:713-715`), client-side JS exact-match filter (`r.dataset.form ===
  filter`).
- **`dashboard_filings` has zero rows for any of the 8 EU tickers, by construction**:
  `fundamentals_pipeline/10__ingestion/15__fetch_sec_filings.py:92-104` resolves each ticker
  via `get_cik()` against a `TICKER_MAP` built from SEC's `company_tickers.json`; a ticker not
  in SEC's own index raises `ValueError`, caught and logged as `error_type="cik_not_found"`
  (`:100-135`) — no rows produced. `16__fetch_eu_xbrl.py` and `17__firds_admission.py` never
  write to `sec_filings`, and `51__export_dashboard_data.py:516-542`'s filings export reads
  `sec_filings` only — no EU/ESEF union exists at that boundary (unlike the meta/financials/
  metrics slices, which do union the two branches). The Filings tab degrades to an honest empty
  list for all 8 companies — not a crash, not a fabricated row.
- **A real, separate data-integrity risk, not just a coverage gap**: `get_cik()` matches purely
  on bare ticker string with no exchange/country check. A European ticker that happens to
  collide with an unrelated US-listed symbol (plausible for short symbols — the pilot set
  includes `ISP`, `NAI`, `RAND`, none confirmed clear of SEC's ~10,000-company ticker map in
  this audit) would silently pull that **wrong US company's** 10-K/10-Q filings into the
  European company's Filings tab, rather than erroring or staying empty. This is the same class
  of risk `check_no_export_ticker_collision()` guards against for the meta/financials export,
  but **`15__fetch_sec_filings.py` has no equivalent guard of its own** — worth flagging to the
  pipeline owner as a Phase 5.6-adjacent gap, not strictly a `fundamentals_screener` fix.

**Priority**: the empty-tab coverage gap is **P2** (honest, non-misleading, but incomplete —
European regulatory disclosures do exist at national OAMs, just not ingested). The
ticker-collision risk in `15__fetch_sec_filings.py` is **P1, MEDIUM risk** — it would silently
show *wrong* data, not just missing data, and sits outside `fundamentals_screener`'s own
codebase (a pipeline-side fix, flagged here because the audit's scope is the whole data path a
`fundamentals_screener` user sees).

---

## 8. Market / exchange audit

- `available_countries()`/`available_markets()`/`available_sectors()`/`available_industries()`
  (`company_listing.py:304-337`) all derive **dynamically** from `load_meta()["tickers"]` — no
  hardcoded list anywhere. Spain/France/Italy (whichever real countries the 8 companies
  resolve to) and `"EU"` will appear in filter dropdowns automatically, zero code changes.
- `_scope()` (`company_listing.py:339-363`) and every SQL builder in `company_listing.py`
  filter/sort generically on `country`/`market` columns — no `CASE WHEN market='US'` branching
  found anywhere.
- **yfinance/Yahoo symbol translation is not replicated in the app, and this is not new**: the
  pipeline has real per-market Yahoo suffix maps (`.TO` for Canada in
  `12__fetch_market_data.py`, MIC-keyed `.MC`/`.PA`/`.AS`/`.MI` in `18__fetch_eu_market_data.py`
  — both pipeline-internal only). `fundamentals_screener/news.py:35,85`'s
  `fetch_yahoo_news()` builds its RSS URL from the raw published `ticker` with **no** suffix
  logic — already wrong for Canada (needs `RY.TO`, gets `RY`) before Europe existed; the same
  gap applies identically and for the same reason to the 8 EU tickers.
- **Badges/logos**: `templatetags/logos.py` has no market/country logic at all — a bare
  ticker-keyed Logo.dev hotlink + monogram fallback. `company_detail.html:374` renders country
  as plain text (`{{ s.country|default:"—" }}`) — no flag/icon system exists to extend; EU
  countries render as plain text with zero changes.
- **Sorting**: no `ORDER BY market` or market-specific `CASE` in `company_listing.py` — generic
  throughout.

**Priority**: the Yahoo-symbol gap is **P2** (degrades to "no live news," already true for
Canada — not a regression, but worth fixing once, for both markets, rather than twice). Every
other item in this section is **SAFE / no change needed**.

---

## 9. Dashboard-data contract

`51__export_dashboard_data.py`'s published shape, as it actually exists after Phase 5.6:

| Artifact | Ticker-keyed only? | Carries market/country/currency? |
|---|---|---|
| `dashboard_meta.json["tickers"]` | — | **Yes** — `market`, `country`, `exchange` (=MIC for EU), `reporting_currency`, `accounting_standard`, all three index-membership flags (`false` for EU) |
| `dashboard_data` (financials, long) | Yes, bare `ticker` | No |
| `dashboard_metrics` | Yes, bare `ticker` | No (`unit` sometimes doubles as currency — see §5) |
| `dashboard_prices` | Yes, bare `ticker` | No |
| `dashboard_forecast` | Yes, bare `ticker` | No |
| `dashboard_filings` | Yes, bare `ticker` | No — and EU has zero rows (§7) |
| `dashboard_fx` | `base`/`quote`/`pair`/`date` | N/A (this IS the currency data) |
| `dashboard_backtest` | Yes, bare `ticker` | No |

`ISIN`/`MIC`/`listing_id` never reach any published artifact — confirmed directly in
`51__export_dashboard_data.py:120-131`'s `SELECT`, which pulls `e.mic AS exchange` (repurposing
the existing `exchange` field, not a new column) and drops `isin` entirely. This means
`fundamentals_screener` **cannot** currently distinguish "this EU row's real underlying
identity" from its bare ticker even if it wanted to — the data to do so was deliberately not
published (consistent with ADR-0012's explicit non-goal: "Does not change
`fundamentals_screener`'s URL routes, DTOs, or template contract").

`fundamentals_screener`'s own data-loading layer (`data_source.py`, `repository.py`) is fully
generic — iterates `ARTIFACT_NAMES`, registers each as a DuckDB view unconditionally, no market
filtering anywhere. **No change needed here.**

---

## 10. General Screener

**Would all 8 EU companies naturally appear with no filters applied? Yes, with zero code
changes**, confirmed by tracing the actual query path:

- No WHERE clause, hardcoded list, or index-membership requirement excludes a `market='EU'`
  row with `in_sp500=in_r3000=in_tsx_composite=false` — `_scope()`
  (`company_listing.py:339-363`) filters only on the explicit, optional
  `sector`/`index`/`country`/`market`/`industry`/`search` params the user actually supplies;
  none is defaulted to a non-empty value.
- The "Index" filter (`_screen_main.html:57-63`) is a fixed 3-option dropdown (S&P 500 /
  Russell 3000 / S&P/TSX Composite) with `"Any"` as the default/unselected state
  (`_index_flag()`, `company_listing.py:292-294`, returns `None` for unset). This is
  **correctly NOT applicable** to EU rows, not a gap — FIRDS admission is deliberately not an
  index-membership concept (CLAUDE.md's Option B), so there is nothing for a 4th "FIRDS"
  option to mean here; a EU company simply has no index and the default "Any" state shows it
  regardless.
- `Sector`/`Industry` are `NULL` for all 8 EU companies (Phase 5.6 leaves them unpopulated, no
  European sector taxonomy source yet). `available_sectors()`/`available_industries()`
  (`company_listing.py:304-309,328-337`) both explicitly filter `if (s := rec.get("sector"))`
  — a falsy/`None` sector is excluded from the picker's own option list (correct — no
  "Unknown" ghost option), and a company with `sector=None` still passes through `_scope()`
  when no sector filter is applied. Verified: nothing coerces a `None` sector into a string
  that could accidentally exclude the row from an unrelated filter.
- The one real gap: `views.py:273`'s `show_usd_toggle = "CA" in markets` (already flagged in
  §5.3 item 8) means the USD-lens toggle's *visibility* is keyed to Canada specifically, not to
  "is any non-USD currency present" — currently harmless (CA is always in the universe today)
  but conceptually wrong and worth fixing alongside the rest of §5.
- A minor, non-blocking UX nuance confirmed by a dedicated null-semantics pass: since
  `available_sectors()`/`available_industries()` build their dropdown options from
  `{s for rec in ... if (s := rec.get("sector"))}` (`company_listing.py:304-309,328-337`), a
  company with `sector=None` (all 8 EU companies, today) never gets a menu entry — but this
  does **not** exclude it from the default "All sectors" view (`_scope()`'s own sector check
  only applies `if sector and ...`), and it remains fully findable via the free-text `search`
  box regardless. Degrades to "unselectable by dropdown specifically," never to silent
  exclusion or a fabricated sector label — not worth a fix on its own, noted for completeness.

**Classification: READY**, with the one currency-toggle-gating nit from §5 carried over, not a
new finding.

---

## 11. Company Detail

Tab-by-tab, tested conceptually against FCC / ALO / IBE / SGO (traced through the real code
path, not assumed from AAPL parity):

| Tab | Classification | Why |
|---|---|---|
| Overview | **REQUIRES BACKEND CHANGE** | Headline KPIs (Revenue/Net Income/Total Assets/Operating CF) hardcode `$` via `headline_kpis()`'s missing `currency=` arg (§5.3 #4) — numerically correct, visibly wrong |
| Price | **REQUIRES BACKEND CHANGE** | `quote_currency("EU")` → `"USD"` fallback (§5.1) — visibly wrong |
| Income Statement / Balance Sheet / Cash Flow | **READY WITH SMALL CHANGE** | Values are correct and currency-silent (no `$` shown at all, per the currency-agent's finding — `compact_money`, not `compact_money_ccy`, is used here) — not mislabeled, just undisclosed. A small addition (wire `compact_money_ccy` + the ticker's `reporting_currency` into the statement table) would make these fully correct rather than merely non-wrong |
| Quarterly | **NOT APPLICABLE / LEGITIMATE NULL** today | Tab doesn't render (§6) — an honest empty state given no interim EU data exists yet, not a screener bug |
| Valuation (football field + MoS) | **REQUIRES BACKEND CHANGE** | `metric_value:"usd"` hardcoded literally in the template (§5.3 #5) — the most direct fix in the whole audit (delete the literal `"usd"`, pass the ticker's real currency) |
| Derived metrics | **READY WITH SMALL CHANGE** | `MetricSeries`/`ScreenTableRow`-style per-row units already exist architecturally; correctness depends on the upstream `metrics_hierarchy.json` unit-label fix (§5.2), which is a pipeline change, not a screener change |
| Forecasting | **REQUIRES BACKEND CHANGE (small)** | `forecasting.js:64-67` hardcodes `$` on the Y-axis with no currency passed from the server at all |
| Filings | **NOT APPLICABLE / LEGITIMATE NULL** today | Empty tab, honest (§7) — not a screener bug, blocked on a pipeline-side ESEF filings source that doesn't exist yet |
| Net-Net card (Valuation page's snapshot, not the Finder) | **REQUIRES BACKEND CHANGE** | Reuses the same `metric_value:"usd"`-style rendering as the Finder (§13) |

No tab crashes or 404s for a EU ticker — `company_detail()`'s only `Http404` path
(`views.py:776-777,813-814`) is an unknown-ticker guard, unaffected by any of these findings.

---

## 12. Valuation / null semantics

Investigated specifically for the confirmed real gap: `Shares Diluted` was unavailable in the
pilot ESEF filings for some of the 8 companies, and must render as legitimately unknown, never
a fabricated `0`/`false`/`"USD"`/empty string.

- **Display layer is sound.** Every numeric-rendering filter in `templatetags/fmt.py`
  (`metric_value`, `compact_money`, `compact_money_ccy`, `sign_class`, `delta_chip`) returns the
  em-dash `_EMPTY = "—"` (`fmt.py:19`) for `None` — no `{{ value|default:"0" }}`-style
  workaround found anywhere in the templates checked, and no filter coerces `None` into a
  falsy-but-rendered `0`.
- **Filter/eligibility layer correctly excludes, never silently passes, a NULL.** Net-Net
  Finder's eligibility gate is `WHERE metric = ? AND period_type = 'FY' AND value IS NOT NULL`
  (`company_listing.py:638-640`) — a company with no computable NCAV for a level is excluded
  from that level's results, not shown with a fabricated zero-discount row. The General
  Screener's `MetricFilter` bounds (`_filter_clause`) behave the same way under normal SQL NULL
  semantics — a NULL pivot value fails a `>=`/`<=` bound rather than passing it.
  `preset_screen()`'s own docstring (`company_listing.py:970-975`) explicitly documents the one
  deliberate exception to this default — Buffett's Margin-of-Safety criterion treats a
  structurally-NULL value (Energy/Financials/Real Estate sectors, where MoS is inapplicable by
  design, not missing by data gap) as a pass-through on that one criterion, evaluating the
  company on its other criteria instead of failing it outright. This is a real, working
  precedent for exactly the kind of "structurally absent, not broken" judgment a missing
  EU `Shares Diluted` would need if a future preset criterion depended on an EPS-derived metric
  for a company that can't produce one.
- A EU company missing `Shares Diluted` therefore does not crash Lynch's PEG/EPS-CAGR criteria
  — those metrics themselves would be `NULL` in `dashboard_metrics` (computed upstream, in the
  pipeline, not in the screener), and the screener's existing NULL-exclusion behavior would
  correctly drop that company from a PEG-filtered result rather than showing a misleading
  `0%`/`0` PEG.

**Classification: SAFE — no change needed.** This section of the audit found no null-handling
defect. It's included in full because the audit prompt specifically asked for verification, not
because a fix is warranted.

---

## 13. JavaScript / visualization audit

Consolidated from §5.3's evidence plus a dedicated pass:

| File:line | Finding |
|---|---|
| `forecasting.js:64-67` | Hardcoded `"$" + (v/1e9).toFixed(1) + "B"` (and `/1e6`, `/1e3`, bare) on the forecast fan-chart Y-axis — no currency passed from the server |
| `balance_sheet_chart.js:39-41`, `:117` | Same hardcoded `$` pattern in the Balance Sheet composition chart's axis and tooltip |
| `statement_charts.js:85` | Tooltip label `": $" + compact(...)` — hardcoded `$`; the shared `compact()` helper itself (`:33-37`) is currency-agnostic, the `$` is bolted on only at this one call site |
| `screener.js`, `price_chart.js` | Clean — no `$`/`USD`/currency hardcoding found in either file |

None of these are crashes — every EU company's charts render, just with a `$` prefix on EUR
figures. Fixing this requires the same information the backend fixes in §5.3 need (the
ticker's real `reporting_currency`) threaded into the existing `json_script` chart payloads
(`views.py`'s `_tab_chart_json`/`_forecast_chart_json`/`_balance_sheet_json` helpers already
exist as the natural place to add one more field).

**Priority: P1** (visible on every chart a EU company has, but purely cosmetic/mislabeling,
same tier as the template-level currency findings in §5).

---

## 14. Net-Net / football field / presets

- **Net-Net Finder**: the audit's single worst concrete instance (§5.3 #6, #7) —
  `_netnet_content.html:102-103,132` hardcodes `"usd"`/literal `$` for Price, NCAV/Share, and
  Market Cap, with **no per-row currency field in the `NetNetRow` DTO at all**
  (`dtos.py:312-341` — confirmed no `currency` attribute exists), unlike `ScreenTableRow` which
  already carries `units: Mapping[str, str|None]`. This is the one place in the app where even
  the *data model* would need to change (add a `currency`/`unit` field to `NetNetRow`), not
  just the template — every other currency finding in this audit is fixable by wiring already-
  existing DTO fields (`HeadlineKpi.currency`, `ScreenTableRow.units`) through more
  consistently.
- **Football field**: §5.3 #5 — the single most mechanical fix in the audit (a hardcoded
  string literal `"usd"` in one template line, `company_detail.html:531`).
- **Investor Presets**: currency-safe by construction (§5.4) — every threshold is a ratio or
  percentage, never an absolute-dollar bound. `EPS CAGR (5Y) %`/`PEG` (Lynch's own criteria)
  degrade correctly for a company missing the underlying `Shares Diluted` input (§12) — no
  currency or null-handling fix needed for Presets' eligibility logic itself. Presets' own
  results *table* still uses `ScreenTableRow.units`, so it inherits the same "correct
  mechanism, generic-unit upstream data" situation as the General Screener (§5.4).

**Priority**: Net-Net Finder is **P0** (both the worst instance and the only one requiring a
DTO change, not just a template/services fix). Football field is **P0** but trivial (LOW risk,
one-line fix). Presets: **SAFE**.

---

## 15. External consumer

`fundamentals_screener/README.md:197-202` states the versioning contract directly (quoted in
§3). Grep for `alopezm_my_website`/`alopezm.xyz` across the package found no direct references
inside `fundamentals_screener` itself — the coupling is entirely through the public contract
surfaces enumerated in §3 (route names, template filenames, DTO shapes), which is the correct,
loosely-coupled design the README describes, not a hidden dependency.

**None of this audit's proposed changes (§17) touch a route name, remove/rename a DTO field, or
rename a template file.** Every proposed fix is either: (a) an additive DTO field (e.g. a
`currency` field on `NetNetRow`, an optional `currency=` argument already accepted by
`HeadlineKpi`/`compact_money_ccy` but not always populated), (b) an internal helper
generalization (`currency.py`'s market→currency map), or (c) template/JS copy changes that
don't alter a template's *filename* or a route's *shape*. Per the README's own contract
language, none of these require a version bump beyond an ordinary minor release, and none
require the external consumer to change anything on its side.

---

## 16. Real European acceptance cases

Traced analytically end-to-end for the 8 real tickers (FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP)
through the actual current code, since no local `FUNDAMENTALS_DATA_PATH` cache with a
post-Phase-5.6 export was available in this environment to load live. No mock data was
substituted — every claim below is a direct trace through real pipeline SQL
(`51__export_dashboard_data.py`) and real screener code (cited throughout §4-§14), not an
assumption.

For any of the 8 tickers, today, once the data syncs:

1. **General Screener**: appears with no filters, filterable by real `country`/`market`,
   `sector`/`industry` blank (§10). **Works.**
2. **`/<ticker>/` company detail page**: loads (no 404 path is affected by any finding here).
   **Works.**
3. **Overview tab**: loads; headline KPIs show real Revenue/Net Income/Total Assets/Operating
   CF figures — correct numbers, `$`-prefixed instead of `€` (§5.3 #4, §11). **Works, mislabeled.**
4. **Price tab**: loads; real close price — correct number, `$`-prefixed instead of `€`
   (§5.1, §11). **Works, mislabeled.**
5. **Income Statement / Balance Sheet / Cash Flow tabs**: load; correct numbers, no currency
   symbol shown at all (§11). **Works, undisclosed currency.**
6. **Quarterly tab**: does not render (`{% if quarterly.lines %}` false — zero rows, §6).
   **Absent, not broken.**
7. **Valuation tab (football field + MoS)**: loads if the company has TTM/FY intrinsic-value
   metrics computed upstream; bear/mid/bull figures `$`-prefixed via the hardcoded template
   literal (§5.3 #5, §11). **Works, mislabeled.**
8. **Derived metrics tab**: loads; sparkline + peer-benchmark figures render, currency label
   correctness depends on the upstream `metrics_hierarchy.json` fix (§5.2, §11). **Works,
   mostly mislabeled (all except Market Cap).**
9. **Forecasting tab**: loads if `financials_forecast` has rows for the ticker; chart Y-axis
   `$`-prefixed client-side (§13). **Works, mislabeled.**
10. **Filings tab**: renders an empty list — `dashboard_filings` has zero rows for any of the 8
    (§7). **Absent, not broken, honest.**
11. **Net-Net Finder (`?mode=netnet`)**: a company with positive NCAV at some level appears;
    Price/NCAV/Discount/Market Cap columns `$`-prefixed, the worst instance in the audit
    (§5.3 #6-#7, §14). **Works, most severely mislabeled.**
12. **Investor Presets (`?mode=presets`)**: a qualifying company appears under whichever
    school's ratio-based criteria it passes; a missing `Shares Diluted` correctly excludes it
    from PEG-dependent Lynch results rather than showing a fabricated value (§12). **Works
    correctly.**

**No case in this list requires a URL, DTO-shape, or route change to reach a working (if
sometimes mislabeled) state.** The single ticker most likely to visibly demonstrate the
non-calendar-fiscal-year ambiguity (§6) once tested against real data is **Alstom (ALO)**,
given its confirmed March 31 fiscal year-end.

---

## 17. Proposed changes

Each entry: current behavior → problem → evidence → proposed behavior → affected files →
breaking-change risk → dependencies. Ordered by priority (§18 has the full matrix).

### 17.1 Generalize `currency.py`'s quote-currency map (P0, LOW risk)
- **Current**: `QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}`, defaults unmapped
  markets to `"USD"` (`currency.py:10,15`).
- **Problem**: mislabels EUR prices as USD for `market="EU"`.
- **Proposed**: either add `"EU": "EUR"` (Generation-1-accurate, since Phase 5.6 is a single
  EU-wide `market` value today) or — more robust against a future per-country EU split — derive
  price-quote currency from the ticker's own `reporting_currency` when `market` isn't a
  recognized quote-currency key, rather than defaulting to USD.
- **Affected**: `currency.py` only.
- **Breaking-change risk**: SAFE (internal helper, not a public contract).
- **Dependencies**: none.

### 17.2 Stop hardcoding `"usd"` in templates (P0, LOW risk)
- **Current**: `company_detail.html:531` (football field), `_netnet_content.html:102-103,132`
  (Net-Net Finder).
- **Problem**: renders `$` regardless of the company's real currency.
- **Proposed**: pass the ticker's real `reporting_currency`/`unit` into these render contexts
  (already available as `summary.reporting_currency`/`price_currency` in `company_detail.html`;
  needs adding to `NetNetRow` for the Finder — see 17.4) and use `compact_money_ccy`/
  `metric_value:currency` instead of a literal `"usd"`.
- **Affected**: `company_detail.html`, `_netnet_content.html`.
- **Breaking-change risk**: SAFE.
- **Dependencies**: 17.4 for the Net-Net Finder half.

### 17.3 Populate `HeadlineKpi.currency` for statement-derived KPIs (P0, LOW risk)
- **Current**: `services.py:213-225` `headline_kpis()` never sets `currency=`.
- **Problem**: Overview tab's Revenue/Net Income/Total Assets/Operating CF cards default to
  `$` via `compact_money_ccy`'s own `currency or "USD"` fallback.
- **Proposed**: pass the ticker's `reporting_currency` (already fetched as part of `summary` in
  the same view) into each `HeadlineKpi`.
- **Affected**: `services.py`, `views.py` (thread `summary.reporting_currency` into the call).
- **Breaking-change risk**: SAFE (additive use of an existing optional DTO field).
- **Dependencies**: none.

### 17.4 Add a `currency` field to `NetNetRow` (P0, LOW-MEDIUM risk)
- **Current**: `dtos.py:312-341` — no currency field.
- **Problem**: the Finder has no data available to badge Price/NCAV/Market Cap correctly even
  if the template were fixed.
- **Proposed**: add an optional `currency: str | None = None` field, populate from
  `dashboard_metrics`'s per-row unit (same source `market_cap_asof`/`market_cap_live` already
  use correctly elsewhere) in `CompanyListingRepository.net_net_screen()`.
- **Affected**: `dtos.py`, `repositories/company_listing.py`, `_netnet_content.html`,
  `services.py` (`_net_net_sort_value` unaffected — sorting concern, see 17.6).
- **Breaking-change risk**: SAFE — additive dataclass field with a default, per §3's
  classification table.
- **Dependencies**: none.

### 17.5 Wire `compact_money_ccy` into financial-statement tables (P1, LOW risk)
- **Current**: `company_detail.html:470,505` use bare `compact_money` — correct numbers,
  undisclosed currency, for every company (not just EU).
- **Proposed**: pass `summary.reporting_currency` through and switch to `compact_money_ccy`.
- **Affected**: `company_detail.html`.
- **Breaking-change risk**: SAFE.
- **Dependencies**: none.

### 17.6 Fix `charts.py`/JS hardcoded `$` (P1, LOW risk)
- **Current**: `forecasting.js:64-67`, `balance_sheet_chart.js:39-41,117`,
  `statement_charts.js:85`.
- **Proposed**: thread the ticker's `reporting_currency` into the existing `json_script`
  payloads (`views.py`'s `_tab_chart_json`/`_forecast_chart_json`/`_balance_sheet_json`) and
  read it client-side instead of a literal `$`.
- **Affected**: `views.py`, three JS files.
- **Breaking-change risk**: SAFE (JSON payload gains a key; nothing removed).
- **Dependencies**: none.

### 17.7 Generalize the USD-lens toggle gate (P2, LOW risk)
- **Current**: `views.py:273` — `show_usd_toggle = "CA" in markets`.
- **Proposed**: `show_usd_toggle = any(m != "US" for m in markets)` or equivalent — matches
  `company_detail`'s own already-generic gate (`views.py:784`).
- **Affected**: `views.py`.
- **Breaking-change risk**: SAFE.
- **Dependencies**: none.

### 17.8 (Upstream, pipeline-side, not `fundamentals_screener`) Real per-metric currency units
- **Current**: `metrics_hierarchy.json` sets `"unit": "usd"` as a generic label for nearly
  every monetary metric.
- **Problem**: the root cause of §5.2's systemic amplification — fixing 17.1-17.7 only patches
  the specific render sites this audit found; a metric added later would inherit the same bug
  by default.
- **Proposed**: flag to the pipeline owner — this is out of `fundamentals_screener`'s own
  codebase and outside this audit's non-goals boundary (§18 of the user's prompt: "modify
  Databricks" is explicitly off-limits for THIS phase), but is the actual root cause and should
  be the first thing Phase 5.8 (or whatever implementation phase follows) considers, since
  fixing it upstream would make 17.5/17.6's screener-side work correct-by-default for every
  metric, not just the ones enumerated here.
- **Breaking-change risk**: N/A (pipeline, not this app) — flagged for awareness only.

### 17.9 Add a `currency` field to statement DTOs directly (P3, LOW risk, optional alternative to 17.5)
- Instead of threading `reporting_currency` through every call site, consider adding it once to
  `CompanyStatements`/`QuarterGrid` at the DTO level so every consumer (template, future JSON
  API, future PDF export) gets it for free. Lower urgency than 17.5 — a "nice generalization,"
  not a distinct bug.

### 17.10 Internal cross-market ticker-collision defense (P1, MEDIUM risk, LOW urgency)
- **Current**: no defense inside `fundamentals_screener` itself (§4); the upstream export
  guard is the only protection.
- **Proposed**: not a URL/DTO change (ADR-0012 rules that out) — instead, consider a
  **fail-loud check inside the app's own data-sync boundary** (`sync_fundamentals_data`, or
  `data_source.validate()`) that independently re-verifies ticker uniqueness in the synced
  `dashboard_meta.json`, so a collision that somehow reached a published release (upstream
  guard bypassed, or removed in a future refactor) is caught at sync time rather than silently
  corrupting `get_summary()`/`by_ticker` dicts at request time. This is defense-in-depth, not a
  fix for an active bug — explicitly **not urgent** given the upstream guard's current,
  verified effectiveness and the small, curated pilot set.
- **Affected**: `data_source.py` or the `sync_fundamentals_data` management command.
- **Breaking-change risk**: SAFE (an additive validation step).
- **Dependencies**: none, but low priority relative to §17.1-17.7.

### 17.11 Filings/Quarterly tab copy (P2/P3, LOW risk, low urgency)
- Relabel "Quarterly"→something period-agnostic and surface `period_end` next to `FY {{ y }}`
  headers (§6); soften "SEC filings · 10-K & 10-Q" copy if/when a non-SEC filings source is
  ever ingested (§7). Both are cosmetic and currently affect a tab state (empty) that isn't
  misleading on its own — **lowest urgency in this list**, sequence after the currency fixes.

---

## 18. Breaking-change analysis

| Change | Contract surface touched | Classification |
|---|---|---|
| 17.1 `currency.py` map | none (internal helper) | SAFE |
| 17.2 template currency wiring | none (template internals, not filenames) | SAFE |
| 17.3 `HeadlineKpi.currency` population | none (existing optional field, just populated) | SAFE |
| 17.4 `NetNetRow.currency` field | `dtos.py` shape — but additive, default `None` | SAFE, additive |
| 17.5 statement table currency | none | SAFE |
| 17.6 JS/chart payload currency | `_tab_chart_json` JSON shape — additive key | SAFE, additive |
| 17.7 USD-lens toggle gate | none | SAFE |
| 17.8 pipeline `unit` fix | outside this app entirely | N/A to this contract |
| 17.9 statement DTO currency field | `dtos.py` shape — additive | SAFE, additive |
| 17.10 sync-time collision check | none (internal validation) | SAFE |
| 17.11 tab copy/labels | none (template content, not filenames) | SAFE |

**No proposed change in this audit is a BREAKING change to the public contract described in
§3.** This is the single most decision-relevant conclusion for whoever scopes the
implementation phase: none of this requires bumping `fundamentals_screener`'s major version,
coordinating a synchronized deploy with `alopezm_my_website`, or touching the `/<str:ticker>/`
route ADR-0012 already ruled out of scope.

---

## 19. Implementation order

Grouped into three roadmap phases (§20 gives them names/complexity), sequenced by
dependency and by how much of the currency-mislabeling surface each closes:

1. **First** — 17.1 (`currency.py` map) and 17.3 (`HeadlineKpi.currency`): fix the Overview and
   Price tabs, the two tabs a user reads first. No dependencies on anything else.
2. **Second** — 17.2 (football field literal) and 17.5 (statement tables): closes the
   remaining Company Detail currency gaps. Independent of each other, can land together.
3. **Third** — 17.4 (`NetNetRow.currency`) then 17.2's Net-Net Finder half (depends on 17.4
   landing first, since the template needs the field to exist before it can read it).
4. **Fourth** — 17.6 (JS/chart currency) and 17.7 (toggle gate): lowest-visible-impact of the
   P0/P1 tier, no dependencies.
5. **Fifth, and can run in parallel with 1-4 since it's a different codebase** — 17.8, flagged
   to the pipeline owner as a separate Databricks-side change; makes 17.5/17.6 correct-by-
   default for every future metric once it lands, but 17.5/17.6 do not need to wait for it —
   they can hardcode a per-tab `reporting_currency` pass-through today and inherit correctness
   automatically once 17.8 fixes the upstream `unit` labels.
6. **Sixth, low urgency** — 17.10 (sync-time collision defense) and 17.11 (tab copy/labels):
   neither blocks anything, neither is urgent given today's verified-safe pilot state.

---

## 20. Explicit non-goals

Per the audit's own scope (and confirmed against ADR-0012 where relevant):

- **Not proposed**: any change to `/<str:ticker>/` or any other route in `urls.py`.
- **Not proposed**: renaming any template file.
- **Not proposed**: removing or renaming any existing `dtos.py` field.
- **Not proposed**: a new public JSON API endpoint.
- **Not proposed**: implementing the full "Reporting currency (native) / Display currency
  (Native / USD / EUR / CAD)" architecture the audit prompt sketched as a likely target — this
  audit only determines which backend data already supports it (Market Cap: yes, via
  `usd_fx_rate`/`convert_price`; everything else: no, blocked on 17.8) and which does not.
  Building a 3-or-4-way display-currency selector is real, separate implementation work for a
  later phase, not scoped here.
- **Not proposed**: renaming the Quarterly tab to "Interim" or the Filings tab to "Reports" —
  both are flagged as directionally correct (§6, §7) but low-urgency, since the underlying data
  gap (no EU interim periods, no EU filings source) makes the current labels merely
  premature-looking rather than actively wrong today.
- **Not proposed**: any Databricks pipeline change. 17.8 is flagged for the pipeline owner's
  awareness only, not scoped or implemented here.
- **Not proposed**: any change to `main.config.tickers`, `eu_admission_candidates`, or any
  Delta table.
- **Not proposed**: any change to the Streamlit app (`60__frontends/61__streamlit/`) — out of
  this audit's stated scope (`fundamentals_screener` only), though it likely has an
  analogous currency-mislabeling issue worth a separate, future look.
- **No mocks were created** to make any part of this audit "pass" — every acceptance-case trace
  in §16 is a real code-path trace against the real pipeline SQL and real screener code, and
  every gap found (Quarterly/Filings empty state) is reported as a real gap, not worked around.

---

## Priority / risk summary

| # | Finding | Priority | Risk |
|---|---|---|---|
| §5.3 #1-#3 | `quote_currency("EU")` → USD fallback | P0 | LOW |
| §5.3 #4 | Headline KPIs missing currency | P0 | LOW |
| §5.3 #5 | Football field hardcoded `"usd"` | P0 | LOW |
| §5.3 #6-#7 | Net-Net Finder hardcoded `$`/`"usd"`, no DTO field | P0 | LOW-MEDIUM |
| §13 | JS chart hardcoded `$` | P1 | LOW |
| §11 | Statement tables currency-silent | P1 | LOW |
| §5.3 #8 | USD-lens toggle gate stale (`"CA" in markets`) | P2 | LOW |
| §5.3 #9-#10 | USD-lens cosmetic-only; cross-currency sort | P1 | LOW |
| §7 | Filings tab empty for EU (data gap) | P2 | N/A (pipeline) |
| §7 | SEC filings ticker-collision risk | P1 | MEDIUM (pipeline) |
| §6 | Quarterly tab empty for EU (data gap) | P2 | N/A (pipeline) |
| §6 | `period_end` never shown (Alstom ambiguity) | P1 | LOW |
| §4 | No in-app cross-market ticker-collision defense | P1 | MEDIUM |
| §5.2 | `metrics_hierarchy.json` generic `"usd"` unit label | P0 (root cause) | N/A (pipeline) |
| §12 | Null semantics | — | SAFE, no finding |
| §10 | General Screener EU inclusion | — | SAFE, no finding |
| §3 | Public contract stability | — | SAFE, confirmed by design (ADR-0012) |

---

## Final decision — implementation roadmap

**Phase 5.7a — Currency correctness on Company Detail** *(small)*
17.1, 17.2 (football field half), 17.3. Closes the Overview/Price/Valuation-tab mislabeling —
the three tabs a user reads numbers from first. No dependencies, no DTO changes, no template
filename changes. This alone converts the worst, most-visible P0 findings into non-issues.

**Phase 5.7b — Net-Net Finder currency + statement-table disclosure** *(medium)*
17.4 (the one real DTO addition in this audit) → 17.2 (Net-Net Finder half), plus 17.5
(statement tables) and 17.6 (JS charts) done together since they share the "thread
`reporting_currency` through an existing payload" shape. Medium only because 17.4 requires
touching a repository query, not because it's risky — still SAFE/additive per §18.

**Phase 5.7c — Toggle/copy cleanup + hardening** *(small)*
17.7 (toggle gate), 17.11 (Quarterly/Filings copy), 17.10 (sync-time collision check). Lowest
urgency, can slip behind other work indefinitely without user-visible harm.

**Flagged, not scheduled, for the pipeline owner**: 17.8 (the real root cause — generic `"usd"`
unit labels in `metrics_hierarchy.json`) and the `15__fetch_sec_filings.py` ticker-collision gap
(§7). Both are outside `fundamentals_screener`'s codebase and this audit's non-goals boundary,
but both are load-bearing for Phase 5.7a-c's fixes to actually stay correct as new metrics are
added, and are the two findings most worth a dedicated follow-up phase of their own.

**Answer to "what is the minimum safe set of changes required to make `fundamentals_screener`
genuinely multi-market?"**: Phase 5.7a alone would already make the three most-visible tabs
correct for all 8 EU companies. Phase 5.7a+b closes every currency-mislabeling finding in this
audit. Nothing in any of the three phases touches the public contract, so none of it needs to
be coordinated with the external consumer's own deploy — each can ship as an ordinary
`fundamentals_screener` patch/minor release, independently, in the order above.
