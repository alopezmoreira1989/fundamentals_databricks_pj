# Phase 5.5 — `fundamentals_screener` European Compatibility Audit

**Audit / design phase only — no code, templates, JavaScript, URLs, database schemas, Databricks
pipeline code, or Streamlit touched.** Determines exactly what must change (and what already
works) before the 8 real European companies now in `main.financials.financials` (Phase 5.3/5.4,
merged) can be exposed through `fundamentals_screener`.

Every finding is classified by **compatibility severity**:

- **A — SAFE INTERNAL CHANGE**: implementation detail, no external contract affected.
- **B — COMPATIBLE CHANGE**: extends behavior without touching existing US/CA URLs/responses.
- **C — VERSIONED BREAKING CHANGE**: changes the documented public contract (route names,
  template filenames, DTO shapes — per `fundamentals_screener/README.md`'s own "Versioning"
  section) — requires a version bump before a consumer updates its pin.
- **D — MUST NOT CHANGE IN PHASE 5.5**: out of this phase's scope regardless of severity.

## Executive summary

The most important finding of this audit is **not about Django at all**: European data cannot
reach `fundamentals_screener` (or the Streamlit app) yet, **regardless of any code compatibility
question**, because `51__export_dashboard_data.py`'s ticker-universe query INNER JOINs against
`main.config.tickers` — and the 8 admitted European tickers were deliberately never written
there (Phase 5.3/5.4's own explicit scope boundary). This is a real, structural, upstream
blocker — see §2 and Finding #1 in the table below — not a Django-app defect.

Separately, and more encouragingly: `fundamentals_screener`'s own architecture is **already more
multi-market-ready than a from-scratch audit might expect**, because the earlier Canadian (TSX)
rollout already generalized several of the exact mechanisms Europe now needs — a real,
already-working `reporting_currency`-driven USD-toggle, a fully data-driven (not hardcoded)
`market` filter, and `country`/`market` fields already present in DTOs and templates. The
remaining gaps are narrower than "the app assumes US only."

**Recommendation** (§25 of the driving brief): **YES, WITH CONDITIONS** — see §21 for the exact
blockers, none of which require breaking the existing `/AAPL/`-style URL contract.

## 1. Current architecture

`views.py → services.py → repositories/ → repository.py (DuckDB) → data_source.py (cache)`
(per the package's own README). No financial logic lives in this package — every value is
already computed upstream and published as Parquet; this app only reads, formats, and renders.
Data arrives exclusively via `manage.py sync_fundamentals_data`, which downloads
`dashboard_*.parquet` + `dashboard_meta.json` from this repo's GitHub Release `latest` — **the
web views never touch Databricks directly**. This means the true gate for "is Europe visible in
the app" is entirely upstream of Django: whatever reaches the published artifacts is what the
app can possibly show, and nothing more.

## 2. The real blocker: European data does not reach the published artifacts yet

**VERIFIED FACT**, direct code read of `fundamentals_pipeline/50__publish/51__export_dashboard_data.py`:

```sql
SELECT t.ticker, t.company, t.sector, ..., t.market, ...
FROM {CATALOG}.config.tickers t
JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
  ON f.ticker = t.ticker
ORDER BY t.ticker
```

This is the **entire** ticker universe the export (and therefore the app) will ever see — an
`INNER JOIN` between `config.tickers` and `financials`. **Live-verified this session**: none of
the 8 EU tickers (FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP) exist in `main.config.tickers` —
confirmed by direct query, 0 rows. They therefore would not appear in `tickers_df` even if
`51__export_dashboard_data.py` were run today, despite having 108 real rows in `financials`
itself. This is a real, structural blocker Phase 5.3/5.4 knowingly left open (both phases
explicitly excluded `config.tickers` writes from their scope), not a bug — but it means **the
Django-layer question this audit was asked to answer ("can the app display these 8 companies")
has a prerequisite answer of "not yet, independent of any Django change"**.

Also confirmed absent for all 8 EU tickers (same live query session): `market_prices_daily`,
`market_cap_asof`, `financials_metrics` (all derived/screener-facing metrics — P/E, ROE, margins,
etc.), `financials_intrinsic_value`. **Only raw statement line items exist today** (Revenue, Net
Income, Total Assets, Cash & Equivalents — the 5 canonical concepts Phase 5.1's mapping covers).
This has a real, direct consequence for scope: even once the export-universe gap is closed, a
European company's detail page would have **no derived-metrics tab content, no price chart, no
market cap, no valuation football field, no NCAV, no forecast** — only the raw Financial
Statements tab would have real content, because none of those other features' upstream data
exists yet. This is `Finding #1` in the table below, and it materially shapes §21's "conditions."

## 3. Identity assumptions

**VERIFIED FACT**, `views.py:771-777`:

```python
def company_detail(request: HttpRequest, ticker: str) -> HttpResponse:
    ticker = ticker.upper()
    summary = services.get_company_summary(ticker)
    if summary is None:
        raise Http404(f"unknown ticker {ticker!r}")
```

This is the core assumption: **a bare ticker string is the sole company identity** for the
detail page and everything downstream of it. There is no `market`/`MIC` parameter anywhere in
the URL, the view signature, or the lookup. Every repository query below confirms the same
pattern at the SQL level — but the risk is **more diffuse and more silent than a single lookup
function**, re-derived independently at four separate layers, each with its own collision
failure mode:

- **URL layer**: only a `<str:ticker>` path segment exists — no market/MIC slot to extend.
- **View-parameter layer**: every company-scoped view passes the bare ticker straight through,
  unchanged, to `services`.
- **SQL-partition layer**: `QUALIFY row_number() OVER (PARTITION BY ticker[, metric] ORDER BY
  fiscal_year DESC) = 1` (`companies.py:42,56,70`; `company_listing.py:40,61,97`;
  `screener.py:17`) picks "the" latest row for a bare ticker — two companies sharing a ticker
  in `dashboard_metrics`/`dashboard_prices` would have their rows interleaved into one partition,
  silently returning the wrong one.
- **Python-dict/linear-scan layer** — the most concrete, most silent risk found:
  `CompanyRepository.get_summary()` (`companies.py:350-372`) does a **linear scan that returns
  the first match and stops** (`for rec in records: if rec.get("ticker") == ticker: return
  CompanySummary(...)`) — a second same-ticker record would be silently unreachable, not an
  error. `company_listing.py`'s `_metric_page()` (436) and `net_net_screen()` (629, 666, 668)
  build **`by_ticker = {rec.get("ticker", ""): rec for rec in scope}`-style dicts three separate
  times** — a dict construction that *silently overwrites* one company's row with another's on a
  collision, with no dedup logic, no warning, no error anywhere. This is the exact class of bug
  `fundamentals_pipeline/identity.py`'s `check_no_cross_market_collision()` guard already exists
  to prevent for `config.tickers` — `fundamentals_screener` has no equivalent guard at all.

None of these are hit by live data today (§5: EU tickers aren't in the meta list yet), so the
risk is **latent, not triggered** — but it would activate immediately and silently the moment
any EU ticker symbol coincided with an existing US/CA one, with no error to signal it happened.

## 4. URL / API contract

**VERIFIED FACT**, `urls.py` (full file, 6 routes):

```python
path("", views.screen, name="screen")
path("data/", views.screen_data, name="screen_data")
path("about/", views.about, name="about")
path("<str:ticker>/", views.company_detail, name="company_detail")
path("<str:ticker>/data/", views.company_data, name="company_data")
path("<str:ticker>/news/", views.company_news, name="company_news")
```

Per the package's own README ("Versioning — this is a public API contract"): the contract is
specifically **route names** (`company_detail`, etc.), **template filenames**, and **DTO
shapes** — not necessarily the literal URL path string. This is a narrower, more precise
definition than "never touch the URL" — it means a path could in principle gain an *optional*
segment/query param without breaking the contract, as long as the route name and the default
(no-param) behavior are preserved, and reversal (`{% url 'fundamentals_screener:company_detail'
ticker %}`) keeps working unchanged for existing callers.

**Is `/FCC/` currently ambiguous?** No — confirmed by direct query, `FCC`/`FCT` do not exist in
`main.config.tickers` today (0 rows for either), so there is no *live* collision. But this is a
property of today's data, not a structural guarantee: the project's own prior research (Phase
5.2) found `FCC` also matches an unrelated Vietnamese company and `FCT` matches ≥5 unrelated
global companies — a bare ticker is provably not globally unique in principle, only currently
non-colliding in this specific dataset. **`/FCC/` resolving to Spanish FCC the moment it's
exported is safe only because no US/CA ticker happens to collide today** — a fragile, not
structural, safety property.

## 5. Real data validation

**VERIFIED FACT**, direct query against `main.financials.financials` (Phase 5.4's own validated
output) and every downstream table the app reads:

| Table | EU rows (8 tickers) |
|---|---|
| `financials` | 108 (real) |
| `financials_raw` | 757 (real, cumulative across validation runs) |
| `config.tickers` | **0** |
| `market_prices_daily` | **0** |
| `market_cap_asof` | **0** |
| `stock_splits` | **0** |
| `financials_metrics` | **0** |
| `financials_intrinsic_value` | **0** |

This table alone answers most of §5-§10 of the driving brief: the application currently has
**nothing to do** with these 8 tickers, because none of them are in its actual data source (the
published artifacts) — this was verified against the real upstream Databricks tables directly,
not by running the Django app (which cannot see anything not yet exported).

## 6. Database query assumptions

**VERIFIED FACT**, `repositories/companies.py` (representative sample, not exhaustive — every
query in this file follows the same shape):

```sql
-- latest_metrics / metric_history (fundamentals-level, derived)
WHERE ticker = ? AND period_type = 'FY' AND category IS NOT NULL

-- statements (fundamentals-level, raw)
WHERE ticker = ? AND period_type = 'FY' AND stmt IS NOT NULL AND value IS NOT NULL

-- price chart / SMA (market-data-level)
WHERE ticker = ? AND close IS NOT NULL AND adj_close IS NOT NULL

-- filings bracketing CTE (fundamentals-adjacent, SEC-specific)
WHEN f.form = '10-K' AND a.fy_end = CAST(f.report_date AS DATE) THEN a.fiscal_year
WHEN f.form = '10-Q' AND ... THEN a.fiscal_year + 1
```

Every query is a flat, single-parameter `WHERE ticker = ?` — structurally simple, not layered
with joins that assume anything beyond ticker uniqueness. This is a **positive** finding for
compatibility: extending the identity model later (e.g. to `(ticker, market)`) would mean adding
one bind parameter per query, not untangling a complex join graph. Classified per the driving
brief's required distinction:

| Query class | Level | Ticker-uniqueness assumption |
|---|---|---|
| `latest_metrics`/`metric_history`/statements/quarterly | fundamentals-level | Yes — would need disambiguation if two markets' tickers collide |
| price chart/SMA/`market_cap`/NCAV-live | market-data-level | Yes, same risk, separate table |
| filings bracketing | fundamentals-adjacent, **SEC-form-literal-specific** | Yes, and additionally hardcodes `10-K`/`10-Q` — silently produces zero rows for a non-SEC form like `ESEF`, not an error |
| news cache key | display-level | Yes (see §9) |

**The `10-K`/`10-Q` hardcoding in the filings bracketing CTE is a real, distinct finding** from
the general ticker-uniqueness question — even if a European ticker reached this query, the tab
would silently show nothing (not break), since `dashboard_filings` itself is SEC-only and no
`ESEF` form literal is recognized here. Consistent with §2's finding that Filings-tab data
doesn't exist for EU tickers at all yet.

## 7. Fundamentals vs. market data

**VERIFIED FACT.** The application already keeps these conceptually separate at the query level
(§6) — fundamentals queries read `dashboard_data`/`dashboard_metrics`, market-data queries read
`dashboard_prices`. This already mirrors the target `issuer → fundamentals` / `issuer → listing →
ticker → market data` split reasonably well **at the read layer** — the gap is not architectural
confusion, it's that the *market-data* half of that split has no EU inputs to read yet (§2),
since the EU vertical slice (Phase 5.3/5.4) never touched `market_prices_daily`/`config.tickers`.

## 8. Currency assumptions

**VERIFIED FACT — a genuinely mixed picture, more precise than "already generic."** Two real,
working, currency-aware mechanisms exist — but a real, concrete list of hardcoded-USD spots
exists alongside them, not instead of them.

**What is genuinely currency-aware today:**

- `templatetags/fmt.py`'s `metric_value()`/`compact_money_ccy()` correctly render any 3-letter
  currency code (not just USD) with a badge, and the general Screener table
  (`_screen_results.html:52`) and company-detail KPI/price rows (`company_detail.html:352,398`)
  correctly use this per-row/per-KPI currency rather than a hardcoded prefix.
- `company_detail`'s own currency variables (`views.py:782-785`):
  ```python
  price_currency = quote_currency(summary.market).lower()
  reporting_currency = (summary.reporting_currency or "USD").upper()
  show_usd_toggle = price_currency != "usd" or reporting_currency != "USD"
  ```
  generically keyed off `reporting_currency`/`market`, not `if market == "CA"`.
- `dashboard_fx`/`convert_price` (`companies.py:518-526`, `company_listing.py:1073`,
  `services.py:244`) is genuinely generic (any `base` currency) and correctly date-anchored on
  `period_end`, never a run-time spot rate — matching the pipeline's own established discipline.
  A **second**, separate `show_usd_toggle` (`views.py:273`, gated on `"CA" in markets`) drives
  the bulk Screener's own "View Market Cap in USD" toggle — distinct from `company_detail`'s
  per-page toggle above, both real, both currently only exercised for CAD.

**What is hardcoded to USD, found by direct grep — a real, concrete list, not a vague risk:**

| Location | What's hardcoded |
|---|---|
| `currency.py:10` | `QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}` — a 2-entry dict; `quote_currency(market)` **silently defaults any unrecognized market to `"USD"`**, not an error |
| `_netnet_content.html:102-103` | `{{ item.row.price|metric_value:"usd" }}`, `{{ item.ncav_per_share|metric_value:"usd" }}` — Net-Net Finder table ignores the row's actual native currency |
| `_netnet_content.html:132` | `` `${{ item.row.market_cap|compact_money }}` `` — literal `$`, no currency badge at all |
| `_netnet_card.html:39` | `{{ lv.ncav_per_share|metric_value:"usd" }}` — same hardcode on the company-detail Net-Net card |
| `company_detail.html:531` | Valuation football-field bars: `{{ b.bear|metric_value:"usd" }}` etc. — matches `FootballBar`'s own docstring ("per-share, USD") |
| `views.py:453-458` (`_NET_NET_HEADER_COLUMNS`) | Column metadata itself hardcodes `"usd"` for price/NCAV-per-share/market-cap |
| `static/.../js/forecasting.js:60-67` | `fmt()` prefixes `"$"` unconditionally in the forecast fan-chart tooltips/axis — no currency passed from Python at all |
| `static/.../js/statement_charts.js:85` | Tooltip label `": $" + compact(...)` |
| `static/.../js/balance_sheet_chart.js:39-41` | `"$" + …` for the balance-sheet composition chart |

**Net picture**: the general Screener and company-detail KPI/price rows would very likely render
EUR correctly with no code change (the machinery is genuinely generic, just never exercised for
EUR specifically — untested, not unbuilt). The **Net-Net Finder** (both the standalone screener
and the company-detail card), the **valuation football field**, and **every client-side chart**
would silently mislabel EUR values as `$` today. `currency.py`'s silent USD fallback for an
unrecognized `market` value is the single sharpest, most concrete finding in this section — a
real, wrong-but-silent behavior waiting for the first non-US/CA `market` value to reach it.

## 9. Caching assumptions

**VERIFIED FACT.** The only cache usage in the entire package is `news.py:80`:

```python
cache_key = f"fundamentals_screener:news:{ticker}:{limit}"
```

Keyed on bare ticker only, no market component. **A real, concrete, low-severity collision
risk**: if a US/CA ticker and a European ticker ever shared a symbol AND both had the news
widget active, one's cached Yahoo Finance headlines could serve under the other's page — narrow
blast radius (a display-only news widget, not financial data), but a real, fixable gap.

## 10. Test coverage

**VERIFIED FACT.** Only `tests/test_net_net_service.py` references market/country-aware fixtures
at all among the test suite's 9 files (all `"market": "US"`/`"country": "United States"`) — the
rest implicitly test against US tickers/USD/calendar fiscal years without exercising the market
dimension. No test uses `market: "CA"`, `CAD`, or any non-US ticker anywhere in the suite.
`test_forecasting_service.py:38` additionally hardcodes `unit="usd"` in its fixture, reinforcing
the same USD-only assumption at the test level. No test constructs two meta records sharing one
`ticker` value, so §3's `by_ticker`/linear-scan collision risk is entirely untested today.

## 11. External consumer dependency

**VERIFIED FACT**, `fundamentals_screener/README.md`, "Versioning" section (quoted in full at
§4 above). The consumer (`alopezm_my_website`, external, not in this workspace) is documented to
couple against **route names**, **template filenames**, and **`dtos.py` shapes** specifically —
not the pipeline's internal identity model, not `config.tickers`, not Databricks. This means
Phase 5.3/5.4's own identity changes (`issuer_id`/`listing_id = MIC:ISIN`) are **already fully
insulated** from the external contract by construction — nothing about admitting new European
companies requires touching anything the consumer actually depends on, provided route
names/template filenames/DTO shapes are preserved (additive DTO fields are safe; removing or
renaming existing ones is not).

## 12. Required changes — findings table

| # | Location | Assumption | EU impact | Severity | Contract impact |
|---|---|---|---|---|---|
| 1 | `51__export_dashboard_data.py:80-93` | Ticker universe = `INNER JOIN config.tickers` | EU tickers silently excluded from every published artifact | **BLOCKING** | NONE (pipeline-internal) |
| 2 | `companies.py:350-372` (`get_summary`) | Linear scan returns first ticker match, stops | A second same-ticker record is silently unreachable | HIGH | NONE (internal) |
| 3 | `company_listing.py:436,629,666,668` (`by_ticker`/`price_by_ticker`/`market_cap_by_ticker` dicts) | Dict keyed by bare ticker | A colliding row **silently overwrites** the other — no error, no dedup, no warning | HIGH | NONE (internal) |
| 4 | `views.py:771-777` (`company_detail`) + every SQL `PARTITION BY ticker` idiom | Bare `ticker` is sufficient, unique identity | `/FCC/` cannot disambiguate markets if a future collision occurs | HIGH | Depends on fix shape (see §13) |
| 5 | `currency.py:10` (`QUOTE_CURRENCY_BY_MARKET`) | 2-entry dict (`US`/`CA`), unknown market silently → `USD` | A EU `market` value would be silently mislabeled `USD`, not an error | HIGH (silent + wrong, not just missing) | NONE (internal) |
| 6 | Net-Net Finder (`_netnet_content.html`, `_netnet_card.html`, `views.py:453-458`) + valuation football field (`company_detail.html:531`) + all chart JS (`forecasting.js`, `statement_charts.js`, `balance_sheet_chart.js`) | Hardcoded `"usd"`/`"$"`, ignoring the row's real currency | EUR values would display mislabeled as `$` in these specific surfaces | MEDIUM (a real, itemized list — see §8 — not a vague risk) | NONE |
| 7 | `repositories/companies.py` (filings CTE) | `form IN ('10-K','10-Q')` hardcoded | ESEF filings silently produce empty Filings tab | LOW (silent-empty, not broken) | NONE |
| 8 | `news.py:80` | Cache key = bare ticker | Cross-market ticker collision could leak cached news for up to 30 min (`_TTL=1800`) | MEDIUM | NONE (internal cache key format) |
| 9 | `repositories/company_listing.py:318-326` (`available_markets`) | Market filter list is data-driven | **None — already generic**, positive finding | N/A (already compatible) | NONE |
| 10 | General Screener + company-detail KPI/price rows (`_screen_results.html:52`, `company_detail.html:352,398`) + `dashboard_fx` FX conversion | Genuinely currency-generic, date-anchored correctly | **Likely already correct for EUR, but untested** — no EUR record has ever reached this code (§2) | N/A (already compatible, unverified) | NONE |
| 11 | `dtos.py:27-30,250-252,294-296,329-331` | `country`/`market` already DTO fields | **None — already present**, just never populated for EU | N/A (already compatible) | NONE |
| 12 | Derived metrics, valuation, forecast, price-chart tabs | Assume `financials_metrics`/`market_prices_daily`/`financials_intrinsic_value`/`dashboard_forecast` data exists | All empty for EU tickers today (§2) — tabs would render empty, not error (needs confirming, not assumed — see §18 Open Questions) | MEDIUM | NONE |
| 13 | Test suite | No non-US/EUR/non-calendar-FY fixture exists; no same-ticker-collision fixture exists either | Real EU behavior AND the collision-risk code paths (findings #2/#3) are both entirely unverified by any existing test | MEDIUM | NONE |

## 13. Required migration table

| Component | Current assumption | Required EU change | Breaking? |
|---|---|---|---|
| `51__export_dashboard_data.py` ticker universe | `INNER JOIN config.tickers` | Either admit EU tickers into `config.tickers` (a real identity-model question ADR-0012 already flags as open — MIC:ISIN vs. MIC:TICKER), or extend the export query to also include admitted-but-not-`config.tickers` issuers | No (pipeline-internal, no Django/API surface) |
| `get_summary`/`by_ticker` dicts (`companies.py`, `company_listing.py`) | First-match-wins / silent-overwrite on ticker collision | Add an explicit collision guard (fail loud or dedup deterministically) once a collision becomes possible — a real gap today, not yet exercised | No (internal), but currently a silent-wrong-data risk if left unfixed |
| `currency.py`'s `QUOTE_CURRENCY_BY_MARKET` | 2-entry dict, silent USD fallback | Add the real EU currency mapping (or fail loud on an unrecognized market) before any EU `market` value reaches it | No (internal) |
| `company_detail` URL/view | `<str:ticker>/` | **Recommendation: none, initially.** Since no live collision exists (§4), ship EU support with the identical URL shape first; revisit only if/when a real collision is admitted | No, if done this way |
| Net-Net Finder / football field / chart JS hardcoded `$`/`usd` | Ignore row currency | Thread the real per-row currency through (mirrors what `_screen_results.html`/`metric_value` already do correctly elsewhere) | No (additive, template/JS-only) |
| Filings tab query | `10-K`/`10-Q` literal | Extend to recognize `ESEF` (mirrors the exact `21__clean_and_merge.py` precedent from Phase 5.1) — but only once `dashboard_filings` actually carries ESEF rows, which it does not yet | No (additive literal) |
| News cache key | `ticker` only | Add `market` (or listing_id) to the cache key | No (internal format) |
| DTOs | `country`/`market`/`reporting_currency` already present | None — populate, don't add | No |
| Tests | No EU fixture, no collision fixture | Add both (FCC/Alstom/Iberdrola per §17's minimal matrix, plus a synthetic same-ticker-two-markets case) once data is exportable | No |

## 14. Important — do not over-engineer (per the driving brief's own §23)

The single most consequential finding of this audit is that **most of the "hard" architectural
work the driving brief anticipated (currency toggle, generic market filter, country/market DTO
fields) is already done** — built generically during the earlier Canadian rollout, not
US-specific despite having only ever been exercised for US/CA data. The real remaining gap is
narrower and mostly upstream of Django entirely (§2). This audit does **not** recommend a
redesign of the identity model, the URL contract, or the query layer — see §21.

## 15. Important — identity distinctness preserved

This audit confirms `issuer_id`/`listing_id`/`ISIN`/`MIC`/`ticker` are kept conceptually
distinct throughout — `fundamentals_screener` never sees any of Phase 5.0/5.3's identity
primitives at all today (it only reads the published Parquet, which is entirely bare-ticker-keyed
per the export query in §2); `listing_id = MIC:ISIN` remains a pipeline-internal concept that
has no bearing on this app's own (much simpler) ticker-keyed read model unless a future decision
deliberately threads it through the export.

## 16. Real European data validation — what actually exists to test against

Per §5's table: only `financials` has real EU rows today (108 rows, 8 tickers, 4 concepts —
Revenue/Net Income/Total Assets/Cash & Equivalents — see Phase 5.4's own doc for full detail).
Alstom's non-calendar (March) fiscal year is present and correctly unnormalized in the source
data (`period_end` values `2023-03-31` .. `2026-03-31`) — any future EU-aware test/UI work should
use Alstom specifically to exercise this.

## 17. Minimal future compatibility test matrix (not built in this phase)

| Ticker | Exercises |
|---|---|
| FCC | Baseline EU case, EUR, calendar FY |
| Alstom | Non-calendar (March) fiscal year |
| Iberdrola | Second Spanish issuer (XMAD dedup sanity) |
| Fincantieri | `MTAA` (segment, not operating `XMIL`) MIC, ESEF amendment history |

## 18. Open questions (not resolved by this audit)

- Whether admitting EU tickers into `config.tickers` is itself the right fix for the export gap,
  or whether `51__export_dashboard_data.py`'s join should instead be loosened — this is a real
  design decision (touches ADR-0012's still-open `listing_id` question) deliberately left to a
  future phase, not this audit.
- Whether derived-metrics/valuation/price-chart/forecast tabs would render a clean "no data yet"
  empty state for a EU ticker or actually error — **not verified**, since no EU ticker has ever
  reached the app's actual code path (§2 blocks it structurally). This should be checked with a
  real (even if manually seeded) EU record before committing to the "additive, no breaking
  change" recommendation below as final.
- Whether `available_markets()`'s already-generic filter would need a real market VALUE for
  Europe (e.g. `"EU"` vs. per-country `"ES"`/`"FR"`/`"IT"`/`"NL"`) — not decided; the pipeline's
  own `market` column today only has `"US"`/`"CA"` values, and this choice belongs to whatever
  future phase actually populates `config.tickers` for EU issuers.

## 19. Recommended implementation sequence (proposed, not implemented)

- **Phase A — required backend changes** (Databricks, not Django): resolve the `config.tickers`/
  export-universe gap (§2/§18) — the actual prerequisite everything else depends on.
- **Phase B — API compatibility**: none required beyond A — DTOs already carry the needed fields
  (§7).
- **Phase C — UI/template changes**: verify (don't assume) that a EU ticker with sparse upstream
  data renders clean empty states rather than errors, across every tab; extend the filings CTE to
  recognize `ESEF` once `dashboard_filings` actually has EU rows; fix the news cache key (§9).
- **Phase D — external website compatibility**: none anticipated — the documented contract
  (route names/template filenames/DTO shapes) is untouched by any change identified here.
- **Phase E — testing**: add the 4-ticker matrix from §17 once Phase A lands.

## 20. Repository README as architectural evidence

Confirmed the package's own stated scope (`README.md` "What v1 covers") already documents
`country`/`market` as existing screener filter dimensions — this audit's code-level findings
(§8/§12) are consistent with, not a discovery contradicting, that documentation.

## 21. Required recommendation

**Can `fundamentals_screener` support the current 8 European companies with additive changes
while preserving all existing US/CA URL/API contracts?**

## YES, WITH CONDITIONS

Exact blockers, in dependency order:

1. **(BLOCKING, pipeline-side, not Django)** European tickers must reach
   `main.config.tickers` — or `51__export_dashboard_data.py`'s universe query must be extended —
   before any EU data can reach the published artifacts at all. Nothing in `fundamentals_screener`
   itself can be meaningfully tested or shipped before this.
2. **(Small, compatible, but should land before #1 makes it live)** Fix `currency.py`'s silent
   USD fallback for an unrecognized `market` (§8/§12 finding #5) — otherwise the very first EU
   record to reach this code renders with a confidently wrong currency label, silently.
3. **(Small, compatible)** Add an explicit collision guard where `get_summary()`'s linear scan
   and `company_listing.py`'s `by_ticker`-style dicts currently silently pick/overwrite one of
   two same-ticker records (§3/§12 findings #2-#3) — not triggered by today's 8 tickers, but a
   real, silent-wrong-data gap this project's own `identity.py` precedent says should never ship
   unguarded.
4. **(Verification, not a code change)** Confirm every tab renders a clean empty state (not an
   error) for a ticker with only raw-statement data and nothing else (§18) — genuinely unverified,
   since no EU ticker has ever reached this app's real code path.
5. **(Small, compatible)** Fix the news cache key to include `market` (§9/§13).
6. **(Small, compatible, template/JS-only)** Thread real per-row currency through the Net-Net
   Finder, the valuation football field, and the client-side chart JS (§8/§12 finding #6) —
   mirrors what the general Screener and company KPIs already do correctly.
7. **(Small, compatible, deferred until `dashboard_filings` has EU rows)** Extend the filings
   bracketing CTE to recognize `ESEF`.

None of these require changing `<str:ticker>/`, any route name, any template filename, or any
existing DTO field — the existing URL/API contract survives unchanged under this plan.

## 22. Not done in this phase (restated)

No Django code, templates, JavaScript, URLs, database schemas, Databricks pipeline code, or
Streamlit file was modified. No implementation was performed. This document is the only
deliverable.
