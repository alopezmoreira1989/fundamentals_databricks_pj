# Phase 6.2 — Multi-Market Financial Periods & Reports UI

Presentation-layer phase in `fundamentals_screener`. Makes the "Quarterly" and "Filings" tabs
genuinely multi-market-neutral for the real US/Canada/Europe universe now live in production
(AAPL, AQN, and the 8 admitted European issuers — FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP).
Explicitly **not** about expanding data coverage — `financials_metrics` stays empty for Europe,
Tier 2 concepts are untouched, and `22`/`23` were never run. This phase is about representing
the periods and reports that already exist correctly, not generating new ones.

Labels: **VERIFIED** (checked against real data or real rendered output this phase) /
**IMPLEMENTED** (a real code/template change made this phase) / **DEFERRED** (identified,
deliberately not done).

## Executive summary

**Quarterly → Periods**: a pure copy/label change. The underlying SQL and DTO were already
period-shape-neutral (`period_type <> 'FY'`, free-text column labels) — only the visible tab
label and eyebrow text hardcoded "quarter." **Filings**: kept its name — real data confirms
every filing this tab has ever shown is genuinely SEC-sourced (zero non-SEC rows exist in the
published `dashboard_filings` artifact for any of the 9 non-US tickers checked), so renaming to
"Reports" would have been cosmetic, not evidence-based. What *did* need a fix: the eyebrow text
unconditionally announced "SEC filings · 10-K & 10-Q" even for a company with none — now
conditional on real content. No URL, route name, template filename, or DTO field was changed.

## 1. Current US assumptions (audit, before implementation)

**VERIFIED**, via a full code trace (`urls.py` → `views.py` → `services.py` →
`repositories/companies.py` → `dtos.py` → `company_detail.html` → JS):

- **Quarterly tab**: `_QUARTERLY_SQL` (`repositories/companies.py`) filters
  `period_type <> 'FY'` — not an enum of `Q1`–`Q4`. Column labels are built directly from data
  (`f"{period_type} {fiscal_year}"`), so a non-quarter `period_type` string would already render
  correctly with zero further code change. The US-specific part was entirely presentation:
  the tab label read `"Quarterly"` and the eyebrow read `"... · by fiscal quarter ..."`
  regardless of what the columns actually contained.
- **Filings tab**: different in kind. `_FILINGS_SQL`'s bracketing `CASE` pattern-matches the
  literal strings `'10-K'`/`'10-Q'` to derive `fiscal_year`/`period_type` — a real **logic**
  dependency on SEC vocabulary, not just a label. The template hardcoded the eyebrow
  `"SEC filings · 10-K & 10-Q"` unconditionally, plus `All`/`10-K`/`10-Q` filter pills. No
  `source`/`source_id` field exists on `FilingRow` to ever distinguish a future non-SEC filing
  from an SEC one.
- **Neither tab has a dedicated URL route** — both live inside `company_detail` (`<str:ticker>/`,
  `urls.py:20`) and the single `templates/fundamentals_screener/company_detail.html` file. No
  DTO field name (`QuarterGrid`, `FilingRow`) is itself US-specific.
- **Test coverage before this phase**: zero dedicated tests for `get_quarterly()` (no
  `test_quarterly.py` existed). `test_filings.py` existed (11 tests) but only against
  AAPL/MSFT fixtures — no non-US ticker case.

## 2. Current Canada behavior

**VERIFIED**, against the real published `dashboard_data.parquet`/`dashboard_filings.parquet`
(downloaded from the GitHub Release `latest` tag this phase, not assumed): `AQN` has **only**
`period_type = 'FY'` rows in `dashboard_data` (293 rows, zero `Q1`–`Q4`) and **zero** rows in
`dashboard_filings`. This was not previously confirmed anywhere in this project's own
documentation as a *Canada* fact specifically (prior audits focused on Europe) — Canada's
MJDS/40-F annual-only filing status (already documented in CLAUDE.md's `21__clean_and_merge.py`
note) turns out to produce the exact same "no interim data, no filings metadata" shape as
Europe's ESEF path. This matters: Phase 6.2's fix is a genuine multi-market fix, not a
Europe-only patch that happens to also help Canada.

## 3. Current Europe behavior

**VERIFIED**, same real artifact, all 8 admitted tickers: every one has `period_type = 'FY'`
only in `dashboard_data` (row counts: FCC 81, ALO 70, NAI 38, FCT 34, IBE 85, SGO 70, RAND 80,
ISP 16) and **zero** rows in `dashboard_filings`. `dashboard_meta.json` confirms real, correct
per-ticker metadata already live: `market='EU'`, `country` (ISO 2-letter, e.g. `ES`/`FR`),
`reporting_currency='EUR'`, `accounting_standard='ifrs-full'`, `exchange` = the primary-listing
MIC (e.g. `XMAD`, `XPAR`).

## 4. Naming decision — "Periods"

**IMPLEMENTED.** Tab label changed from `Quarterly` to `Periods`
(`company_detail.html`, nav button + `<title>` context unaffected). `Interim` was considered and
rejected on the same grounds the repo owner raised before implementation started: FY is not
interim, and the 8 live European companies' data is *entirely* FY today — labeling their absent
tab "Interim" would have been backwards even by omission-implication. `Periods` makes no claim
about cadence and needs no future redesign if an `H1`/`9M` period type is ever published — the
underlying query already tolerates it.

The eyebrow text changed from `"{{ quarterly.name }} · by fiscal quarter ..."` to
`"{{ quarterly.name }} · by reporting period ..."` — same reasoning, applied to the one other
place "quarter" was hardcoded into copy.

**Not changed**: the `QuarterGrid` DTO class name, its fields, the `get_quarterly()` method
name, the `pane-quarterly`/`tab-quarterly` internal element IDs, or any JS variable/selector
(`quarterly-chart-data`, `data-quarterly-chart`). None of these are part of the documented
public contract (route names / template filenames / DTO shapes per `README.md`'s own
"Versioning" section), and renaming them would have added diff surface with no benefit to any
consumer — internal identifiers, left alone per "smallest compatible change."

## 5. "Periods" design

**VERIFIED + IMPLEMENTED.** The tab's own visibility guard (`{% if quarterly.lines %}`,
pre-existing, unchanged) already means the tab simply doesn't appear for a ticker with no
non-FY periods — confirmed live this phase (see §10) that AQN/FCC/ALO all correctly omit the
tab entirely, rather than showing an empty "Periods" pane. This is the graceful-empty-state
behavior Part E of the driving brief asked for, and it already existed; this phase's job was to
verify it, not build it.

Fiscal year handling: unchanged and already correct. `get_quarterly()`/`_QUARTERLY_SQL` never
infer a period from a calendar date — they consume `period_type`/`period_end`/`fiscal_year`
exactly as published. Alstom's real March 31 fiscal year end (visible on its FY rows in
`dashboard_data`) is unaffected by anything in this phase, since Alstom (like every European
issuer) has no interim rows to begin with.

## 6. "Reports"/"Filings" design

**IMPLEMENTED, but the name itself was NOT changed.** Per the driving brief's own instruction
("the problem is not necessarily the word 'Filings'"), the real evidence (§2/§3 above — zero
non-SEC rows exist in `dashboard_filings` for any live non-US ticker) confirms "Filings" is
still an accurate description of the one real thing this tab has ever shown. Renaming to
"Reports" would have been a cosmetic change with no data-model justification — exactly the
outcome the repo owner flagged as plausible before implementation.

What changed instead:

- The eyebrow is now conditional: `{% if filings %}SEC filings · 10-K & 10-Q{% else %}Filings{% endif %}`
  — the SEC-specific announcement only appears when there is real SEC content to describe;
  otherwise a neutral "Filings" eyebrow pairs with the pre-existing (unchanged) empty-state
  message `"No filings found for this ticker."`. Before this fix, a European or Canadian
  ticker's page announced "SEC filings · 10-K & 10-Q" immediately above a message saying none
  exist — not broken, but misleading in tone (implying the company was expected to have SEC
  filings specifically).
- The `period_type` column header inside the Filings table, previously literally `"Quarter"`,
  is now `"Period"` — for the same reason as §4: it renders a generic `period_type` value
  (`"Annual"` for `FY`, or a real quarter string), and "Quarter" overclaimed what the column
  could ever contain.

**Not changed**: the `10-K`/`10-Q` filter pills, the `_FILINGS_SQL` bracketing logic, the
`FilingRow` DTO (no `source` field added), or the SEC-vocabulary `form` values themselves. These
were all deliberately left alone — see §12 (Deferred).

## 7. API-contract impact

**VERIFIED.** Nothing in this phase touches:

- `urls.py` — no route added, removed, or renamed.
- Template filenames — only the contents of the existing `company_detail.html` changed.
- `dtos.py` — no field added, removed, renamed, or retyped on `QuarterGrid` or `FilingRow`.

Per `README.md`'s own definition of the public contract ("route names, template filenames, and
the shape of `dtos.py`"), this phase makes **zero** breaking changes. No version bump is
required for this change alone (though a routine bump may still happen for other reasons at
release time).

## 8. Empty-state behavior

**VERIFIED** (Periods tab, pre-existing, confirmed correct) **+ IMPLEMENTED** (Filings eyebrow,
this phase):

| Ticker | Periods tab | Filings eyebrow | Filings body |
|---|---|---|---|
| AAPL (real Q1-Q4 data) | Shown, real quarter columns | `SEC filings · 10-K & 10-Q` | Real filing rows |
| AQN (Canada, FY-only) | Omitted entirely | `Filings` (neutral) | `No filings found for this ticker.` |
| FCC/ALO/IBE/SGO (Europe, FY-only) | Omitted entirely | `Filings` (neutral) | `No filings found for this ticker.` |

No `0`, no fabricated quarter, no fabricated report row anywhere — confirmed by live rendering
(§10), not just code inspection.

## 9. Implementation changes (files touched)

```
fundamentals_screener/fundamentals_screener/templates/fundamentals_screener/company_detail.html
fundamentals_screener/tests/test_quarterly.py                (new)
fundamentals_screener/tests/test_filings.py                  (+1 test)
docs/phase6-2-multi-market-periods-reports.md                 (this file)
```

No `views.py`, `services.py`, `repositories/`, or `dtos.py` change. No Databricks pipeline file
touched. No `urls.py` change.

## 10. Real-data validation

**VERIFIED**, against the real published GitHub Release `latest` artifacts (all 7
`dashboard_*.parquet` files + `dashboard_meta.json`, downloaded fresh this phase — not a stale
local copy) — no Databricks production run, no Databricks table modified, per the driving
brief's explicit constraint.

A minimal local Django host project was assembled (`INSTALLED_APPS = ["django.contrib.
staticfiles", "django.contrib.humanize", "fundamentals_screener"]`, `FUNDAMENTALS_DATA_PATH`
pointed at the downloaded artifacts — the exact wiring the package's own README documents for a
real consumer) and `fundamentals_screener` was reinstalled in editable mode
(`pip install -e .`) so the running server reflected this phase's actual template edits, not a
stale installed copy (the environment's previously-installed copy was a stale `0.8.3`, well
behind the real `0.11.0` in this repo — a real, worth-noting environment gotcha, not a code
issue). A real `runserver` was started and each page fetched over real HTTP:

```
GET /apps/screener/AAPL/  -> 200
GET /apps/screener/AQN/   -> 200
GET /apps/screener/FCC/   -> 200
GET /apps/screener/ALO/   -> 200
```

Confirmed in the real rendered HTML:

- AAPL: `>Periods<` tab present, `by reporting period` eyebrow present, real quarter columns
  `Q2 2025`–`Q3 2026` (chronological, newest-first source data reversed correctly for display).
  Filings eyebrow reads `SEC filings · 10-K & 10-Q` (real filings exist).
- AQN, FCC, ALO: **zero** occurrences of `>Periods<`, `>Quarterly<`, or `by reporting period` —
  the tab is correctly omitted, not rendered empty. Filings eyebrow reads the neutral `Filings`;
  body shows `No filings found for this ticker.`.
- FCC specifically: page title `FCC — ACCIONES FOMENTO DE CONSTRUCCIONES Y CONTRATAS, S.A.`;
  98 `EUR` occurrences on the page (currency badges rendering throughout); real Financial
  Statements content confirmed present with the exact Revenue values already validated in
  Phase 5.4 (`9,071,416,000` FY2024, `9,026,016,000` FY2023, etc.) — confirming this phase's
  template edits didn't disturb anything else on the page. Zero traceback/error markers found
  in any of the 4 fetched pages.
- No `>Quarter</th>` (the old Filings column header) found on any of the 4 pages;
  `>Period</th>` confirmed present instead.

## 11. Tests

**VERIFIED.** Full `fundamentals_screener` suite: **185 passed** (180 pre-existing + 5 new),
`ruff check` clean (both the package's own `ruff` invocation and the repo-root one CI actually
runs). New coverage:

- `tests/test_quarterly.py` (new, 4 tests) — closes a real, pre-existing gap (zero prior
  coverage for `get_quarterly()`): a real US-shaped fixture returns real quarter columns in the
  correct newest-first order; a real EU/CA-shaped (FY-only) fixture returns a genuinely empty
  `QuarterGrid` (`columns == ()`, `lines == ()`), not a fabricated one; an unknown ticker
  degrades the same way; a shared-table US+EU fixture proves no cross-ticker leakage.
- `tests/test_filings.py` (+1 test) — a real, known issuer (real `dashboard_data` FY anchors)
  with zero `dashboard_filings` rows returns `()`, distinct from the pre-existing "ticker absent
  from both tables entirely" test.

Both new/updated test files were checked against this project's own documented Python 3.9-floor
pitfalls for this specific package (`zip(..., strict=)`, module-level PEP 604 unions) — neither
pattern appears.

## 12. Known limitations / deferred (out of scope, per the driving brief)

- **`FilingRow` has no `source`/`source_id` field.** If a future phase ever publishes ESEF
  filing-level metadata (currently: no such pipeline exists — `dashboard_filings` is written
  exclusively by `15__fetch_sec_filings.py`, SEC-only), the `10-K`/`10-Q` `CASE`-matching logic
  in `_FILINGS_SQL` and the filter pills would need real generalization, not just a label
  change. Deferred — no ESEF filing-metadata source exists to generalize against yet, and the
  driving brief explicitly prohibits inventing artificial European filing types.
- **The `10-K`/`10-Q` filter pills remain hardcoded.** Since 100% of real `dashboard_filings`
  rows are SEC-sourced today, this is not currently a correctness problem — deferred alongside
  the point above, for the same reason.
- **Docstrings in `dtos.py`/`repositories/companies.py`/`services.py` still say "SEC filing"
  in a few places** (e.g. `FilingRow`'s own docstring). Not updated this phase — internal,
  non-contract, cosmetic; left for a future pass rather than expanding this phase's diff.
- **`financials_metrics` is still empty for all 8 European tickers** — unrelated to this phase,
  explicitly out of scope, unchanged.
- Tier 2 concept expansion, `22`/`23` recompute, valuation/ratio work: all explicitly out of
  scope, none started.

## 13. Recommended next step

Not decided by this phase. The repo owner's own two named options (Tier 2 pipeline expansion vs.
further multi-market UI polish) remain open; this phase closes out the specific "Quarterly"/
"Filings" naming question both were waiting on.
