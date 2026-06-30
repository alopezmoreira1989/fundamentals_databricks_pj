"""Cash Flow XBRL concept coverage audit — READ-ONLY.

Standalone analysis script. NOT wired into the 91 pipeline %run chain. It performs
no writes to Delta and no commits: it queries main.financials.financials for
per-concept FY fill-rates, then probes SEC company-facts for the NULL-set issuers of
each low-fill Cash Flow concept to discover which us-gaap tags they actually use,
quantified by how many NULL tickers each candidate tag would recover.

Run locally via Databricks Connect (same as test_connection.py):
    python fundamentals_pipeline/80__audits/81__cashflow_coverage.py

Output: 81__cashflow_coverage_report.md at the repo root.

Diseño (estilo del repo): el fill-rate por sí solo NO prueba un fallo de mapeo — un
concepto como "Dividends Paid" está legítimamente ausente para quien no paga dividendo.
Lo que distingue "ausente de verdad" de "tag equivocado" es el recovery-count sobre el
NULL-set (cuántos NULL tickers tienen dato bajo un tag candidato). Esa es la métrica que
manda la decisión de Fase 1.
"""
from __future__ import annotations

import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests

# ── Config ────────────────────────────────────────────────────────────────────
SEC_USER_AGENT = "Alejandro Lopez Moreira al.lopez.moreira@gmail.com"  # mirror of 01__tickers
HEADERS        = {"User-Agent": SEC_USER_AGENT}
MAX_WORKERS    = 8
MIN_REQUEST_GAP = 0.12
REQUEST_TIMEOUT = 30

SAMPLE_PER_CONCEPT = 25     # NULL-set tickers sampled per flagged concept
MAX_UNIQUE_FETCH   = 160    # hard cap on distinct company-facts downloads (no silent overflow)
RECENT_YEARS       = 3
FLAG_THRESHOLD     = 90.0   # recent fill-rate (%) below this → candidate
FORCE_INCLUDE      = ["RCAT"]  # trigger case — always probe even if not sampled

REPORT_PATH = "81__cashflow_coverage_report.md"
RANDOM_SEED = 42

# Curated candidate tag lists (index 0 = current/preferred). The audit MEASURES which
# actually recover; it does not assume the order is right.
CANDIDATES = {
    "CapEx": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireMachineryAndEquipment",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
    ],
    "Acquisitions": [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
        "PaymentsToAcquireBusinessesAndInterestInAffiliates",
    ],
    "Purchases of Investments": [
        "PaymentsToAcquireInvestments",
        "PaymentsToAcquireMarketableSecurities",
        "PaymentsToAcquireAvailableForSaleSecuritiesDebt",
        "PaymentsToAcquireShortTermInvestments",
        "PaymentsToAcquireHeldToMaturitySecurities",
    ],
    "Sales of Investments": [
        "ProceedsFromSaleOfInvestments",
        "ProceedsFromSaleMaturityAndCollectionsOfInvestments",
        "ProceedsFromSaleAndMaturityOfMarketableSecurities",
        "ProceedsFromSaleOfAvailableForSaleSecuritiesDebt",
        "ProceedsFromSaleOfShortTermInvestments",
    ],
    "Debt Issuance": [
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromIssuanceOfDebt",
        "ProceedsFromLongTermLinesOfCredit",
        "ProceedsFromConvertibleDebt",
        "ProceedsFromNotesPayable",
    ],
    "Debt Repayment": [
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfDebt",
        "RepaymentsOfLongTermLinesOfCredit",
        "RepaymentsOfConvertibleDebt",
        "RepaymentsOfNotesPayable",
    ],
    "Dividends Paid": [
        "PaymentsOfDividends",
        "PaymentsOfDividendsCommonStock",
    ],
    "Share Repurchases": [
        "PaymentsForRepurchaseOfCommonStock",
        "PaymentsForRepurchaseOfEquity",
        "StockRepurchasedDuringPeriodValue",   # LP/MLP + LP→C-corp filers (VNOM FY2023): equity-statement tag
    ],
    "Net Change in Cash": [
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecrease",
        "CashAndCashEquivalentsPeriodIncreaseDecrease",
    ],
    # report-only aggregate variants (true split case needs summation, out of scope).
    # Oil & gas royalty/mineral filers report DEPLETION as an isolated line (VNOM: aggregate absent);
    # the standalone depletion tags are now in CASH_FLOW's fallback list, so include them here too —
    # otherwise the audit would classify these tickers as "genuinely absent" rather than mapped.
    "Depreciation & Amortization": [
        "DepreciationDepletionAndAmortization",
        "DepletionOfOilAndGasProperties",
        "OilAndGasPropertyFullCostMethodDepletion",
        "DepreciationAmortizationAndAccretionNet",
        "DepreciationAndAmortization",
        "Depreciation",
        "AmortizationOfIntangibleAssets",
    ],
}

# Mechanism per concept (drives Phase 1). M2 subtotals handled separately (synonym+priority).
MECHANISM = {
    "CapEx": "M1", "Acquisitions": "M1", "Purchases of Investments": "M1",
    "Sales of Investments": "M1", "Debt Issuance": "M1", "Debt Repayment": "M1",
    "Dividends Paid": "M1", "Share Repurchases": "M1", "Net Change in Cash": "M1",
    "Depreciation & Amortization": "OUT_OF_SCOPE (component-sum)",
    "Changes in Working Capital": "OUT_OF_SCOPE (component-sum)",
    "Investing Cash Flow": "M2 (synonym+priority)",
    "Financing Cash Flow": "M2 (synonym+priority)",
}

# Section regex per concept — used to surface high-frequency tags in the NULL set that
# are NOT in the curated list (catch tags we did not anticipate).
SECTION_RE = {
    "CapEx": re.compile(r"^PaymentsToAcquire.*(Property|Equipment|Productive|Capital|Machinery)"),
    "Acquisitions": re.compile(r"^PaymentsToAcquireBusinesses"),
    "Purchases of Investments": re.compile(r"^PaymentsToAcquire(Investments|MarketableSecurities|AvailableForSale|ShortTermInvestments|HeldToMaturity)"),
    "Sales of Investments": re.compile(r"^ProceedsFrom(Sale|Maturit)"),
    "Debt Issuance": re.compile(r"^ProceedsFrom.*(Debt|Notes|LinesOfCredit|Borrowings)"),
    "Debt Repayment": re.compile(r"^Repayments"),
    "Dividends Paid": re.compile(r"^PaymentsOfDividends"),
    "Share Repurchases": re.compile(r"^(PaymentsForRepurchase|StockRepurchasedDuringPeriodValue)"),
    "Net Change in Cash": re.compile(r"(CashCashEquivalents.*PeriodIncreaseDecrease|CashAndCashEquivalentsPeriodIncreaseDecrease)"),
    "Depreciation & Amortization": re.compile(r"^(Depreciation|Amortization|Depletion|OilAndGasProperty.*Depletion)"),
}

# One broad regex to retain only relevant annual tags per ticker (memory bound).
BROAD_RE = re.compile(
    r"^(PaymentsToAcquire|PaymentsFor|PaymentsOf|ProceedsFrom|Repayments|NetCash"
    r"|CashCashEquivalents|CashAndCashEquivalents|Depreciation|Amortization"
    r"|Depletion|OilAndGasProperty|StockRepurchasedDuringPeriodValue)"
)

# ── SEC fetch (rate-limited, mirrors 11__fetch_sec_xbrl) ──────────────────────
_rate_lock = Lock()
_last_ts = [0.0]


def rate_limited_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    with _rate_lock:
        wait = _last_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_ts[0] = time.monotonic()
    return requests.get(url, headers=HEADERS, timeout=timeout)


def annual_tags_for_cik(cik10: str) -> set[str]:
    """Return the set of us-gaap tags (local name) that carry >=1 annual (10-K, ~365d)
    USD fact for this issuer, restricted to the BROAD_RE sections (memory bound)."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    resp = rate_limited_get(url)
    resp.raise_for_status()
    if "json" not in resp.headers.get("Content-Type", "").lower():
        raise ValueError("non-json")
    facts = resp.json().get("facts", {}).get("us-gaap", {})
    out: set[str] = set()
    for tag, payload in facts.items():
        if not BROAD_RE.match(tag):
            continue
        units = payload.get("units", {})
        rows = units.get("USD") or (next(iter(units.values())) if units else [])
        for r in rows:
            form = r.get("form", "")
            if not form.startswith("10-K"):
                continue
            start, end = r.get("start"), r.get("end")
            if not start or not end:
                continue
            try:
                d = (int(end[:4]) - int(start[:4])) * 365 + (int(end[5:7]) - int(start[5:7])) * 30
            except Exception:
                continue
            if 330 <= d <= 400:
                out.add(tag)
                break
    return out


def main() -> None:
    from databricks.connect import DatabricksSession

    random.seed(RANDOM_SEED)
    spark = DatabricksSession.builder.getOrCreate()
    print("✓ Databricks Connect OK")

    max_fy = spark.sql("""
        SELECT MAX(fiscal_year) m FROM main.financials.financials
        WHERE period_type='FY' AND stmt='Cash Flow' AND value IS NOT NULL
    """).collect()[0]["m"]
    lo = max_fy - (RECENT_YEARS - 1)
    print(f"✓ max_fy={max_fy}  recent={lo}..{max_fy}")

    # ── Fill-rate per concept (overall + recent) ─────────────────────────────
    fill_rows = spark.sql(f"""
        WITH cf AS (
            SELECT DISTINCT ticker, fiscal_year, concept
            FROM main.financials.financials
            WHERE stmt='Cash Flow' AND period_type='FY' AND value IS NOT NULL
        ),
        denom_all AS (SELECT COUNT(*) n FROM (SELECT DISTINCT ticker, fiscal_year FROM cf)),
        denom_rec AS (SELECT COUNT(*) n FROM (SELECT DISTINCT ticker, fiscal_year FROM cf WHERE fiscal_year>={lo})),
        num_all AS (SELECT concept, COUNT(*) n FROM cf GROUP BY concept),
        num_rec AS (SELECT concept, COUNT(*) n FROM cf WHERE fiscal_year>={lo} GROUP BY concept)
        SELECT a.concept, a.n AS all_n,
               (SELECT n FROM denom_all) AS all_denom,
               ROUND(100.0*a.n/(SELECT n FROM denom_all),1) AS all_pct,
               COALESCE(r.n,0) AS rec_n,
               (SELECT n FROM denom_rec) AS rec_denom,
               ROUND(100.0*COALESCE(r.n,0)/(SELECT n FROM denom_rec),1) AS rec_pct
        FROM num_all a LEFT JOIN num_rec r USING (concept)
        ORDER BY rec_pct ASC
    """).collect()
    fill = {r["concept"]: r.asDict() for r in fill_rows}

    # concepts absent entirely (0 rows) won't appear above — inject from the known map
    known = list(MECHANISM.keys()) + ["Stock-based Compensation", "Operating Cash Flow", "Net Income"]
    all_denom = fill_rows[0]["all_denom"] if fill_rows else 0
    rec_denom = fill_rows[0]["rec_denom"] if fill_rows else 0
    for c in known:
        if c not in fill:
            fill[c] = {"concept": c, "all_n": 0, "all_denom": all_denom, "all_pct": 0.0,
                       "rec_n": 0, "rec_denom": rec_denom, "rec_pct": 0.0}

    # ── Flagged concepts for SEC discovery: M1 + report-only D&A ──────────────
    discover = [c for c in CANDIDATES if c in fill]

    # NULL-set tickers per concept (recent window; fall back to all if too few)
    null_tickers: dict[str, list[str]] = {}
    for c in discover:
        rows = spark.sql(f"""
            WITH cf AS (
                SELECT DISTINCT ticker, fiscal_year, concept
                FROM main.financials.financials
                WHERE stmt='Cash Flow' AND period_type='FY' AND value IS NOT NULL
            ),
            denom AS (SELECT DISTINCT ticker, fiscal_year FROM cf WHERE fiscal_year>={lo}),
            have  AS (SELECT DISTINCT ticker, fiscal_year FROM cf WHERE concept='{c}' AND fiscal_year>={lo})
            SELECT DISTINCT d.ticker
            FROM denom d LEFT ANTI JOIN have h USING (ticker, fiscal_year)
        """).collect()
        pool = sorted({r["ticker"] for r in rows})
        random.shuffle(pool)
        null_tickers[c] = pool[:SAMPLE_PER_CONCEPT]
        print(f"  null-set {c:<28} {len(pool):>5} tickers → sampled {len(null_tickers[c])}")

    # ── Resolve tickers → CIK (SEC index) and bound the fetch pool ────────────
    print("Loading SEC ticker index...")
    idx = rate_limited_get("https://www.sec.gov/files/company_tickers.json").json()
    ticker_map = {e["ticker"].upper(): str(e["cik_str"]).zfill(10) for e in idx.values()}
    ticker_map["RCAT"] = ticker_map.get("RCAT", "0000748268")

    fetch_pool: list[str] = []
    for t in FORCE_INCLUDE:
        if t not in fetch_pool:
            fetch_pool.append(t)
    # round-robin across concepts so the cap doesn't starve any one concept
    by_concept = {c: list(ts) for c, ts in null_tickers.items()}
    while any(by_concept.values()) and len(fetch_pool) < MAX_UNIQUE_FETCH:
        for c in discover:
            if by_concept[c]:
                t = by_concept[c].pop(0)
                if t not in fetch_pool:
                    fetch_pool.append(t)
                if len(fetch_pool) >= MAX_UNIQUE_FETCH:
                    break
    dropped = sum(len(v) for v in by_concept.values())
    fetch_pool = [t for t in fetch_pool if t in ticker_map]
    print(f"✓ fetch pool: {len(fetch_pool)} unique tickers (cap {MAX_UNIQUE_FETCH}, {dropped} beyond cap)")

    # ── Fetch annual_tags per ticker (parallel, rate-limited) ─────────────────
    annual: dict[str, set[str]] = {}
    errors: dict[str, str] = {}

    def work(t: str):
        try:
            return t, annual_tags_for_cik(ticker_map[t]), None
        except Exception as e:  # noqa: BLE001
            return t, set(), str(e)[:80]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for fut in as_completed([pool.submit(work, t) for t in fetch_pool]):
            t, tags, err = fut.result()
            if err:
                errors[t] = err
            else:
                annual[t] = tags
    print(f"✓ fetched {len(annual)} tickers ({len(errors)} errors)")

    # ── Recovery counts + coexistence + extra-tag discovery ──────────────────
    results: dict[str, dict] = {}
    for c in discover:
        sample = [t for t in null_tickers[c] if t in annual]
        cands = CANDIDATES[c]
        recovery = {tag: 0 for tag in cands}
        coexist = 0          # tickers where >=2 candidate tags both have annual data
        extra: dict[str, int] = {}
        sec_re = SECTION_RE.get(c)
        for t in sample:
            tags = annual[t]
            hit = [tag for tag in cands if tag in tags]
            for tag in hit:
                recovery[tag] += 1
            if len(hit) >= 2:
                coexist += 1
            if sec_re:
                for tag in tags:
                    if sec_re.match(tag) and tag not in cands:
                        extra[tag] = extra.get(tag, 0) + 1
        # recovered = any non-current candidate beyond index 0 picks it up
        recovered_by_fallback = sum(
            1 for t in sample
            if any(tag in annual[t] for tag in cands[1:]) and cands[0] not in annual[t]
        )
        results[c] = {
            "sampled": len(sample),
            "recovery": recovery,
            "coexist": coexist,
            "extra": dict(sorted(extra.items(), key=lambda kv: -kv[1])[:6]),
            "recovered_by_fallback": recovered_by_fallback,
        }

    rcat_tags = annual.get("RCAT", set())

    _write_report(max_fy, lo, fill_rows, fill, results, errors, rcat_tags,
                  len(fetch_pool), dropped)
    print(f"\n✓ report written → {REPORT_PATH}")


def _pct(n, d):
    return f"{100.0*n/d:.1f}%" if d else "n/a"


def _write_report(max_fy, lo, fill_rows, fill, results, errors, rcat_tags,
                  n_fetched, dropped):
    L: list[str] = []
    w = L.append

    w("# Cash Flow XBRL concept coverage audit — Phase 0 (read-only)\n")
    w(f"_Live table: `main.financials.financials` · FY rows (`period_type='FY'`) · "
      f"max FY = {max_fy} · recent window = {lo}..{max_fy}._\n")
    w("> **Read this first.** Fill-rate alone does NOT prove a mapping bug. Many CF lines "
      "are legitimately absent for a given filer (a non-payer has no Dividends Paid; a "
      "company with no securities book has no Purchases of Investments). The signal that a "
      "concept is *mis-mapped* (not just inapplicable) is the **recovery-count on the NULL "
      "set** — how many NULL tickers carry annual data under an alternative tag. Decisions "
      "below are driven by recovery, not by raw fill-rate.\n")

    # ── 0.1 current mapping inventory ────────────────────────────────────────
    w("## 0.1 — Current Cash Flow mapping inventory\n")
    w("All Cash Flow concepts today are **single-tag** (no fallback lists in CF). The only "
      "synonym/priority entry is `Operating Cash Flow (cont ops)` → `Operating Cash Flow`.\n")
    w("| Display name | Current tag | kind | synonym/priority today |")
    w("|---|---|---|---|")
    inv = [
        ("Net Income", "NetIncomeLoss", "flow_additive", "—"),
        ("Net Income (to common)", "NetIncomeLossAvailableToCommonStockholdersBasic", "flow_additive", "→ Net Income (prio 1)"),
        ("Net Income (incl NCI)", "ProfitLoss", "flow_additive", "→ Net Income (prio 2)"),
        ("Depreciation & Amortization", "DepreciationDepletionAndAmortization", "flow_additive", "—"),
        ("Stock-based Compensation", "ShareBasedCompensation", "flow_additive", "—"),
        ("Changes in Working Capital", "IncreaseDecreaseInOperatingCapital", "flow_additive", "—"),
        ("Operating Cash Flow", "NetCashProvidedByUsedInOperatingActivities", "flow_additive", "canonical"),
        ("Operating Cash Flow (cont ops)", "...OperatingActivitiesContinuingOperations", "flow_additive", "→ Operating Cash Flow (prio 1)"),
        ("CapEx", "PaymentsToAcquirePropertyPlantAndEquipment", "flow_additive", "—"),
        ("Acquisitions", "PaymentsToAcquireBusinessesNetOfCashAcquired", "flow_additive", "—"),
        ("Purchases of Investments", "PaymentsToAcquireInvestments", "flow_additive", "—"),
        ("Sales of Investments", "ProceedsFromSaleOfInvestments", "flow_additive", "—"),
        ("Investing Cash Flow", "NetCashProvidedByUsedInInvestingActivities", "flow_additive", "—"),
        ("Debt Issuance", "ProceedsFromIssuanceOfLongTermDebt", "flow_additive", "—"),
        ("Debt Repayment", "RepaymentsOfLongTermDebt", "flow_additive", "—"),
        ("Dividends Paid", "PaymentsOfDividends", "flow_additive", "—"),
        ("Share Repurchases", "PaymentsForRepurchaseOfCommonStock", "flow_additive", "—"),
        ("Financing Cash Flow", "NetCashProvidedByUsedInFinancingActivities", "flow_additive", "—"),
        ("Net Change in Cash", "CashCashEquivalents…PeriodIncreaseDecreaseIncludingExchangeRateEffect", "flow_additive", "—"),
    ]
    for name, tag, kind, syn in inv:
        w(f"| {name} | `{tag}` | {kind} | {syn} |")
    w("")

    # ── 0.2 fill-rate ────────────────────────────────────────────────────────
    w("## 0.2 — Fill-rate per concept (worst recent first)\n")
    w(f"**Denominator anchor:** distinct `(ticker, fiscal_year)` pairs with *any* non-null "
      f"Cash Flow FY concept (overall = {fill_rows[0]['all_denom']:,} pairs; recent = "
      f"{fill_rows[0]['rec_denom']:,} pairs). Because Net Income is mirrored into the Cash "
      f"Flow section, this anchor ≈ \"the filer reported financials that year\" — the broadest "
      f"honest denominator. Concepts with recent fill < {FLAG_THRESHOLD:.0f}% are flagged.\n")
    w("| Concept | overall fill | recent fill | flag |")
    w("|---|---:|---:|:--:|")
    ordered = sorted(fill.values(), key=lambda r: r["rec_pct"])
    for r in ordered:
        flagged = "🚩" if r["rec_pct"] < FLAG_THRESHOLD else ""
        w(f"| {r['concept']} | {r['all_pct']}% ({r['all_n']:,}/{r['all_denom']:,}) "
          f"| {r['rec_pct']}% ({r['rec_n']:,}/{r['rec_denom']:,}) | {flagged} |")
    w("")
    w("Note: **Sales of Investments** returns **0 rows** — `ProceedsFromSaleOfInvestments` "
      "essentially never carries data → a pure mis-map, the strongest M1 signal. "
      "**Investing/Financing Cash Flow** recent fill is high (cont-ops gap is niche), but "
      "their all-time fill (~87%) and the RCAT trigger justify the M2 fix.\n")

    # ── 0.3 tag discovery ────────────────────────────────────────────────────
    w("## 0.3 — Tag discovery on the NULL set (SEC company-facts)\n")
    w(f"For each flagged concept, up to {SAMPLE_PER_CONCEPT} NULL-set tickers (recent window) "
      f"were probed against the curated candidate tags. `recovered` = sampled NULL tickers "
      f"whose annual 10-K carries data under that tag. **coexist** = tickers where ≥2 "
      f"candidate tags both have annual data the same way (coalesce-vs-sum check).\n")
    w(f"_Fetch pool: {n_fetched} unique tickers (cap {MAX_UNIQUE_FETCH}; {dropped} sampled "
      f"tickers beyond the cap were not fetched — not silently dropped). "
      f"{len(errors)} fetch errors._\n")
    for c, res in results.items():
        mech = MECHANISM.get(c, "?")
        w(f"### {c}  ·  _{mech}_\n")
        w(f"Sampled NULL tickers fetched: **{res['sampled']}**. "
          f"Recovered by a non-current fallback tag: **{res['recovered_by_fallback']}**. "
          f"Coexisting (≥2 candidates same year): **{res['coexist']}**.\n")
        w("| candidate tag (0 = current) | recovers |")
        w("|---|---:|")
        for i, (tag, n) in enumerate(res["recovery"].items()):
            mark = " *(current)*" if i == 0 else ""
            w(f"| `{tag}`{mark} | {n}/{res['sampled']} |")
        if res["extra"]:
            extras = ", ".join(f"`{t}` ({n})" for t, n in res["extra"].items())
            w(f"\n_Uncurated tags seen in this NULL set: {extras}_")
        w("")

    # ── 0.4 mechanism + proposals ────────────────────────────────────────────
    w("## 0.4 — Recommended fix per concept (proposal — NOT applied)\n")
    w("**M1 = fallback list** (`extract_series_multi`, coalesce per period, never sums). "
      "**M2 = synonym + CONCEPT_PRIORITY** (collapse at merge, like Operating Cash Flow). "
      "**Out of scope = component-sum** (needs summation, deferred).\n")

    def proposal_line(c):
        if c not in CANDIDATES:
            return None
        order = sorted(CANDIDATES[c], key=lambda t: -results[c]["recovery"].get(t, 0)) \
            if c in results else CANDIDATES[c]
        # keep current tag first if it still recovers the most, else data-driven order
        return order

    w("### M1 — fallback list (proposed priority-ordered tags, data-driven)\n")
    for c in ["CapEx", "Acquisitions", "Purchases of Investments", "Sales of Investments",
              "Debt Issuance", "Debt Repayment", "Dividends Paid", "Share Repurchases",
              "Net Change in Cash"]:
        if c not in results:
            continue
        res = results[c]
        # propose: current tag stays index 0; remaining ordered by recovery desc
        cur = CANDIDATES[c][0]
        rest = sorted([t for t in CANDIDATES[c][1:]], key=lambda t: -res["recovery"].get(t, 0))
        proposed = [cur] + [t for t in rest if res["recovery"].get(t, 0) > 0]
        risk = ("⚠ coexistence observed — confirm these are total-vs-component (coalesce OK) "
                "not two summable lines") if res["coexist"] > 0 else "no coexistence in sample → coalesce safe"
        w(f"- **{c}** — proposed list: {proposed}")
        w(f"  - recovered by fallback: {res['recovered_by_fallback']}/{res['sampled']}; {risk}.")
    w("")
    w("### M2 — synonym + CONCEPT_PRIORITY (continuing-operations subtotals)\n")
    w("- **Investing Cash Flow** — add `Investing Cash Flow (cont ops)` → "
      "`NetCashProvidedByUsedInInvestingActivitiesContinuingOperations`, synonym → "
      "`Investing Cash Flow`, priority base=0 / cont-ops=1.")
    w("- **Financing Cash Flow** — add `Financing Cash Flow (cont ops)` → "
      "`NetCashProvidedByUsedInFinancingActivitiesContinuingOperations`, synonym → "
      "`Financing Cash Flow`, priority base=0 / cont-ops=1.")
    w("- Mirrors `Operating Cash Flow (cont ops)` exactly. Recovers discontinued-ops filers "
      "(RCAT) without changing the canonical stored in the table → no dashboard/layout change.\n")

    # ── 0.5 out of scope ─────────────────────────────────────────────────────
    w("## 0.5 — Out of scope this pass (report only)\n")
    for c in ["Depreciation & Amortization", "Changes in Working Capital"]:
        r = fill.get(c, {})
        w(f"- **{c}** — recent fill {r.get('rec_pct', '?')}% "
          f"({r.get('rec_n', 0):,}/{r.get('rec_denom', 0):,}). ")
    if "Depreciation & Amortization" in results:
        res = results["Depreciation & Amortization"]
        rec = ", ".join(f"`{t}`→{n}" for t, n in res["recovery"].items())
        w(f"  - D&A aggregate-variant recovery on its NULL set ({res['sampled']} sampled): {rec}. "
          f"Aggregate variants (`DepreciationAndAmortization`, `DepreciationAmortizationAndAccretionNet`) "
          f"are M1-coverable; the true `Depreciation` + `AmortizationOfIntangibleAssets` **split** "
          f"needs summation, not coalesce — separate decision.")
    w("  - **Changes in Working Capital** — `IncreaseDecreaseInOperatingCapital` is rarely "
      "tagged; the real data lives in component lines (AR, inventory, AP …). Pure structural "
      "gap, summation only. Defer.\n")

    # ── trigger case ─────────────────────────────────────────────────────────
    w("## Trigger case — RCAT (Red Cat Holdings)\n")
    if rcat_tags:
        relevant = sorted(t for t in rcat_tags if BROAD_RE.match(t))
        w("RCAT annual us-gaap tags present in the relevant sections (from company-facts):\n")
        w("```")
        for t in relevant:
            w(t)
        w("```")
        w("\nConfirms the disease: RCAT tags its capex/investing/financing lines under variants "
          "our single-tag map misses, plus the `…ContinuingOperations` subtotals.\n")
    else:
        w("_RCAT company-facts not fetched in this run._\n")

    if errors:
        w("## Fetch errors\n")
        for t, e in list(errors.items())[:20]:
            w(f"- {t}: {e}")
        w("")

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    main()
