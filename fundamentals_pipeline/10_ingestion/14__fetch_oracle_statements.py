# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 14__fetch_oracle_statements  ⚠️ EXPERIMENTAL — Tier-A linkbase oracle
# MAGIC
# MAGIC Builds an **independent statement-line → tag → value oracle** from each filer's own
# MAGIC XBRL **presentation linkbase** (not the SEC-rendered statements, so a later discrepancy
# MAGIC can never be blamed on a renderer). Written to `main.config.reconciliation_oracle`;
# MAGIC consumed by `30_analysis/35__reconcile_filings` (Tier A — coverage / wrong-tag).
# MAGIC
# MAGIC **Why the linkbase:** to know the app picked the *wrong* (or no) tag for a statement
# MAGIC line, we must know **which us-gaap tag the filer actually used for that line**. Only the
# MAGIC presentation linkbase carries the statement-line → tag mapping. The SEC `companyfacts`
# MAGIC API (what `11` ingests) does not.
# MAGIC
# MAGIC **Per filing, four artifacts (discovered via the filing `index.json`, robust to naming):**
# MAGIC - `FilingSummary.xml` — classifies each report by `MenuCategory`; `Role` links the report
# MAGIC   to a presentation role URI. We keep `MenuCategory="Statements"`, drop *Parenthetical*,
# MAGIC   and classify each role's `ShortName` → Income Statement / Balance Sheet / Cash Flow.
# MAGIC   (This is SEC metadata, not rendered values — independence holds. The `.xsd` `roleType`
# MAGIC   definition text `" - Statement - "` is an available secondary signal, not needed here.)
# MAGIC - `*_pre.xml` — for each kept role, walk `presentationArc`s to get the line items (tags)
# MAGIC   on that statement, each with its `preferredLabel`.
# MAGIC - `*_lab.xml` — resolve the human-readable `oracle_label` / `preferred_label` text, and
# MAGIC   detect `negatedLabel` (statement *displays* a flipped sign; the fact stores natural sign).
# MAGIC - instance (`*_htm.xml`) — the value, from the **consolidated** context only.
# MAGIC
# MAGIC **Three subtleties encoded (each would otherwise generate garbage findings):**
# MAGIC - **`negatedLabel`** → store the **natural-sign** fact in `oracle_value`; record `negated`
# MAGIC   so the reconciler can *explain* an apparent sign-flip instead of triggering on it.
# MAGIC - **Raw, unscaled values** → XBRL stores the full number ("in thousands" is presentation
# MAGIC   only), so `oracle_value` is directly comparable to the app and any 10ⁿ factor is the
# MAGIC   *app's* bug.
# MAGIC - **Custom-namespace tags** → captured with `tag_namespace='custom'` (covers the filer's
# MAGIC   own namespace **and** `dei`). A coverage gap on a custom tag is **not** fixable by adding
# MAGIC   a us-gaap tag, so the reconciler keeps it separate (never proposes an impossible fix).
# MAGIC
# MAGIC **Scope (v1):** the golden set in `00_config/reconciliation_golden_set.json` (last 10-K +
# MAGIC last 10-Q per ticker), plus `COMBINED_FILERS` from `01__tickers.py` unioned dynamically.
# MAGIC
# MAGIC **Databricks-only:** uses `spark`, `%run`, `lxml`, writes Delta. `lxml` is **not**
# MAGIC preinstalled on serverless → the `%pip install lxml` cell below is required for a
# MAGIC standalone run (mirrors how `91__full_pipeline` front-loads it for `13`).
# MAGIC
# MAGIC > Read-only against `main.financials.*` (it reads `financials_raw` only to sync
# MAGIC > `scraped_at` consistency — none of `main.financials` is written). Writes only
# MAGIC > `main.config.reconciliation_oracle`. Respects the SEC rate limit + real `SEC_USER_AGENT`.

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Lock

import pandas as pd
import requests
from lxml import etree
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

HEADERS        = {"User-Agent": SEC_USER_AGENT}
ORACLE_TABLE   = f"{CATALOG}.config.reconciliation_oracle"
GOLDEN_JSON    = "../00_config/reconciliation_golden_set.json"

# SEC rate limiter — same shape as 11__fetch_sec_xbrl (Lock + monotonic, global min gap).
# 14 runs standalone (not %run-ed from 11), so the limiter is redefined here. Single-threaded
# (the golden set is small), but the Lock keeps it correct if it is ever parallelized.
MIN_REQUEST_GAP = 0.12
REQUEST_TIMEOUT = 60
_rate_lock       = Lock()
_last_request_ts = [0.0]


def rate_limited_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """Thread-safe HTTP GET enforcing a global MIN_REQUEST_GAP between request starts."""
    with _rate_lock:
        wait = _last_request_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()
    return requests.get(url, headers=HEADERS, timeout=timeout)


if SEC_USER_AGENT.strip().lower().startswith("mycompany"):
    print("⚠️  SEC_USER_AGENT looks like the placeholder — SEC will block requests. "
          "Set a real org/email in 00_config/01__tickers.py before running.")

# COMMAND ----------

# MAGIC %md ## 1. Scope — golden set (JSON) ∪ COMBINED_FILERS (dynamic)

# COMMAND ----------

def _load_golden(path: str) -> list[dict]:
    """Load the golden-set JSON. Tolerant of // comment lines and discardable _xxx keys."""
    raw   = Path(path).read_text(encoding="utf-8")
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
    data  = json.loads("\n".join(lines))
    rows  = data["tickers"] if isinstance(data, dict) else data
    return [r for r in rows if r.get("ticker")]


_golden = _load_golden(GOLDEN_JSON)
_scope = {
    r["ticker"].upper(): {"n_10k": int(r.get("n_10k", 1)), "n_10q": int(r.get("n_10q", 1)), "cik": None}
    for r in _golden
}
# Union COMBINED_FILERS (pulled dynamically — never hardcoded). Their consolidated facts sit
# under a single dei:LegalEntityAxis member, so the instance reader must accept that member
# in addition to no-dimension facts (see _is_consolidated below). `cik` override carried through.
for t, cfg in (COMBINED_FILERS or {}).items():
    tu = t.upper()
    _scope.setdefault(tu, {"n_10k": 1, "n_10q": 1, "cik": None})
    _scope[tu]["cik"]    = cfg.get("cik") or _scope[tu]["cik"]
    _scope[tu]["member"] = cfg.get("member")

_combined_members = {t.upper(): cfg.get("member") for t, cfg in (COMBINED_FILERS or {}).items()}
print(f"✓ Golden set: {len(_golden)} ticker(s) from JSON")
print(f"✓ Combined-filers unioned: {list(_combined_members) or '(none)'}")
print(f"✓ Total Tier-A scope: {len(_scope)} ticker(s)")

# COMMAND ----------

# MAGIC %md ## 2. Concept lookup (us-gaap tag → (stmt, label)) — for evidence only
# MAGIC
# MAGIC The oracle stores the **raw** `oracle_tag`; the reconciler (`35`) does the
# MAGIC tag → app-concept mapping. We keep a `(tag, stmt)` lookup here purely to attach a
# MAGIC readable `oracle_label` fallback. Keyed on `(tag, stmt)` — **not** first-hit-wins —
# MAGIC because `NetIncomeLoss` legitimately appears on both the Income Statement and Cash Flow.

# COMMAND ----------

# (tag, stmt) -> label  (all stmt occurrences, unlike 13's first-hit-wins CONCEPT_LOOKUP)
TAG_STMT_LABEL: dict[tuple, str] = {}
for _stmt, _cmap in STATEMENTS.items():
    for _label, (_xbrl, _kind) in _cmap.items():
        for _tag in ([_xbrl] if isinstance(_xbrl, str) else _xbrl):
            TAG_STMT_LABEL.setdefault((_tag, _stmt), _label)

# COMMAND ----------

# MAGIC %md ## 3. SEC helpers — CIK, recent 10-K/10-Q, filing-dir artifacts
# MAGIC
# MAGIC Generalises `13__fetch_dimensional_10k`: handles **both 10-K and 10-Q**, and returns the
# MAGIC filing **directory** so the linkbase siblings can be resolved. Artifacts are discovered by
# MAGIC enumerating the filing `index.json` (robust to the `_htm`-vs-bare stem mismatch that pure
# MAGIC name-construction trips on).

# COMMAND ----------

_XLINK  = "http://www.w3.org/1999/xlink"
_LINK   = "http://www.xbrl.org/2003/linkbase"
_XBRLI  = "http://www.xbrl.org/2003/instance"
_STD_LABEL   = "http://www.xbrl.org/2003/role/label"
_TERSE_LABEL = "http://www.xbrl.org/2003/role/terseLabel"


def _get_cik(ticker: str, override: str | None) -> str | None:
    if override:
        return str(override).zfill(10)
    try:
        idx = rate_limited_get("https://www.sec.gov/files/company_tickers.json").json()
        for e in idx.values():
            if e["ticker"].upper() == ticker.upper():
                return str(e["cik_str"]).zfill(10)
    except Exception as exc:
        print(f"    ⚠ {ticker}: could not resolve CIK ({exc})")
    return None


def _recent_filings(cik: str, n_10k: int, n_10q: int) -> list[dict]:
    """Most recent n_10k 10-K(s) and n_10q 10-Q(s) (incl. /A). One dict per filing."""
    cik_int = int(cik)
    subs = rate_limited_get(f"https://data.sec.gov/submissions/CIK{cik}.json").json()
    rec  = subs["filings"]["recent"]
    out, c10k, c10q = [], 0, 0
    for form, acc, doc, fdate in zip(
        rec["form"], rec["accessionNumber"], rec["primaryDocument"], rec["filingDate"], strict=False):
        if form in ("10-K", "10-K/A") and c10k < n_10k:
            c10k += 1
        elif form in ("10-Q", "10-Q/A") and c10q < n_10q:
            c10q += 1
        else:
            continue
        acc_nodash = acc.replace("-", "")
        out.append({
            "form":        form,
            "accession":   acc,
            "filing_date": fdate,
            "primary_doc": doc,
            "dir_url":     f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}",
        })
        if c10k >= n_10k and c10q >= n_10q:
            break
    return out


def _discover_artifacts(dir_url: str, primary_doc: str) -> dict | None:
    """Resolve instance / _pre / _lab / FilingSummary from the filing index.json."""
    try:
        items = rate_limited_get(f"{dir_url}/index.json").json()["directory"]["item"]
    except Exception as exc:
        print(f"      ⚠ index.json failed: {exc}")
        return None
    names = [it["name"] for it in items]
    low   = {n.lower(): n for n in names}
    stem  = re.sub(r"\.htm$", "", primary_doc, flags=re.I).lower()

    def _first(pred):
        for lc, real in low.items():
            if pred(lc):
                return real
        return None

    instance = low.get(f"{stem}_htm.xml") or _first(lambda n: n.endswith("_htm.xml"))
    pre      = _first(lambda n: n.endswith("_pre.xml"))
    lab      = _first(lambda n: n.endswith("_lab.xml"))
    fsummary = low.get("filingsummary.xml")
    if not (instance and pre and lab and fsummary):
        print(f"      ⚠ missing artifact(s): inst={bool(instance)} pre={bool(pre)} "
              f"lab={bool(lab)} summary={bool(fsummary)}")
        return None
    return {"instance": f"{dir_url}/{instance}", "pre": f"{dir_url}/{pre}",
            "lab": f"{dir_url}/{lab}", "summary": f"{dir_url}/{fsummary}"}

# COMMAND ----------

# MAGIC %md ## 4. Linkbase parsers (FilingSummary, presentation, label) + instance reader

# COMMAND ----------

def _classify_stmt(short_name: str) -> str:
    """Map a FilingSummary ShortName → IS / BS / CF, else 'Other'. Order matters:
    Comprehensive Income and the Equity statement are excluded (they are not one of the
    three statements the app ingests)."""
    s = (short_name or "").upper()
    if "PARENTHETICAL" in s:
        return "Other"
    if "COMPREHENSIVE" in s:
        return "Other"
    if "CASH FLOW" in s:
        return "Cash Flow"
    if "BALANCE SHEET" in s or "FINANCIAL POSITION" in s or "FINANCIAL CONDITION" in s:
        return "Balance Sheet"
    if "STOCKHOLDERS" in s or "SHAREHOLDERS" in s or "EQUITY" in s:
        return "Other"
    if "OPERATIONS" in s or "INCOME" in s or "EARNINGS" in s or "LOSS" in s:
        return "Income Statement"
    return "Other"


def _statement_roles(summary_url: str) -> dict[str, str]:
    """role_uri -> stmt, for FilingSummary reports with MenuCategory='Statements'
    classifiable as IS/BS/CF (Parenthetical / Comprehensive / Equity dropped)."""
    root = etree.fromstring(rate_limited_get(summary_url).content)
    roles = {}
    for rep in root.iter("Report"):
        if (rep.findtext("MenuCategory") or "") != "Statements":
            continue
        role = (rep.findtext("Role") or "").strip()
        if not role:
            continue
        stmt = _classify_stmt(rep.findtext("ShortName") or "")
        if stmt != "Other":
            roles[role] = stmt
    return roles


def _split_frag(frag: str) -> tuple[str, str]:
    """schema-id fragment ('us-gaap_NetIncomeLoss', 'aapl_Foo', 'dei_Bar') → (namespace, localname).
    namespace is 'us-gaap' only for the us-gaap taxonomy; dei + filer-specific → 'custom'."""
    if frag.startswith("us-gaap_"):
        return "us-gaap", frag[len("us-gaap_"):]
    if "_" in frag:
        return "custom", frag.split("_", 1)[1]
    return "custom", frag


def _parse_presentation(pre_url: str, keep_roles: dict) -> list[dict]:
    """For each kept role, the presented line items: (role, stmt, frag, preferred_label_role)."""
    root = etree.fromstring(rate_limited_get(pre_url).content)
    lines = []
    for plink in root.iter(f"{{{_LINK}}}presentationLink"):
        role = plink.get(f"{{{_XLINK}}}role")
        stmt = keep_roles.get(role)
        if stmt is None:
            continue
        loc_href = {loc.get(f"{{{_XLINK}}}label"): loc.get(f"{{{_XLINK}}}href")
                    for loc in plink.findall(f"{{{_LINK}}}loc")}
        seen = set()
        for arc in plink.findall(f"{{{_LINK}}}presentationArc"):
            href = loc_href.get(arc.get(f"{{{_XLINK}}}to"))
            if not href:
                continue
            frag = href.split("#")[-1]
            if frag in seen:
                continue
            seen.add(frag)
            lines.append({"role": role, "stmt": stmt, "frag": frag,
                          "pref_role": arc.get("preferredLabel")})
    return lines


def _parse_labels(lab_url: str) -> dict[str, dict]:
    """frag -> {label_role: text}. Resolves loc → labelArc → label resource."""
    root = etree.fromstring(rate_limited_get(lab_url).content)
    loc  = {lo.get(f"{{{_XLINK}}}label"): lo.get(f"{{{_XLINK}}}href").split("#")[-1]
            for lo in root.iter(f"{{{_LINK}}}loc")}
    res  = {}   # resource label -> [(role, text)]
    for lab in root.iter(f"{{{_LINK}}}label"):
        res.setdefault(lab.get(f"{{{_XLINK}}}label"), []).append(
            (lab.get(f"{{{_XLINK}}}role") or "", lab.text or ""))
    out = {}
    for arc in root.iter(f"{{{_LINK}}}labelArc"):
        frag = loc.get(arc.get(f"{{{_XLINK}}}from"))
        if not frag:
            continue
        for role, text in res.get(arc.get(f"{{{_XLINK}}}to"), []):
            out.setdefault(frag, {})[role] = text
    return out


def _label_text(labels: dict, frag: str, role: str | None) -> str | None:
    d = labels.get(frag, {})
    if role and role in d:
        return d[role]
    return d.get(_STD_LABEL) or d.get(_TERSE_LABEL) or next(iter(d.values()), None)


def _parse_contexts(root) -> dict:
    """Context id -> {start, end, instant, dims}. (Same shape as 13's parser.)"""
    ctx = {}
    for c in root.findall(f"{{{_XBRLI}}}context"):
        per = c.find(f"{{{_XBRLI}}}period")
        if per is None:
            continue
        start = per.find(f"{{{_XBRLI}}}startDate")
        end   = per.find(f"{{{_XBRLI}}}endDate")
        inst  = per.find(f"{{{_XBRLI}}}instant")
        dims  = []
        seg = c.find(f".//{{{_XBRLI}}}segment")
        if seg is not None:
            for m in seg:
                axis   = (m.get("dimension") or "").split(":")[-1]
                member = (m.text or "").strip().split(":")[-1]
                dims.append((axis, member))
        ctx[c.get("id")] = {
            "start":   start.text if start is not None else None,
            "end":     end.text   if end   is not None else None,
            "instant": inst.text  if inst  is not None else None,
            "dims":    dims,
        }
    return ctx


def _is_consolidated(c: dict, member: str | None) -> bool:
    """Consolidated context = no dimension, OR (combined-filer) exactly one
    LegalEntityAxis = the configured parent member. Generalises 13's _parent_fact, which
    required the single-member case; normal filers report consolidated facts with NO dimension."""
    if not c["dims"]:
        return True
    if member and len(c["dims"]) == 1 and c["dims"][0] == ("LegalEntityAxis", member):
        return True
    return False


def _index_instance(instance_url: str, member: str | None) -> dict[tuple, list]:
    """(namespace, localname) -> [{start, end, value}] for consolidated-context facts."""
    resp = rate_limited_get(instance_url)
    if resp.status_code != 200 or b"<xbrl" not in resp.content[:5000].lower():
        return {}
    root = etree.fromstring(resp.content)
    ctx  = _parse_contexts(root)
    facts: dict[tuple, list] = {}
    for el in root.iter():
        if el.text is None:
            continue
        q = etree.QName(el)
        if not q.namespace:
            continue
        c = ctx.get(el.get("contextRef"))
        if c is None or not _is_consolidated(c, member):
            continue
        try:
            value = float(el.text)
        except (TypeError, ValueError):
            continue
        ns_kind = "us-gaap" if "us-gaap" in q.namespace else "custom"
        end = c["end"] or c["instant"]
        if end is None:
            continue
        facts.setdefault((ns_kind, q.localname), []).append(
            {"start": c["start"], "end": end, "value": value})
    return facts

# COMMAND ----------

# MAGIC %md ## 5. Build oracle rows for one filing
# MAGIC
# MAGIC For each presented line we keep, per period, the consolidated fact of the right shape:
# MAGIC Balance Sheet → `snapshot`; Income Statement / Cash Flow → annual (`FY_or_TTM`) for a
# MAGIC 10-K, sub-annual (`Q_standalone` / `YTD_*`) for a 10-Q. Period anchoring is by
# MAGIC `period_end` (a DATE) so the reconciler join is immune to the fiscal-vs-calendar
# MAGIC `fiscal_year` labelling difference for non-December filers.

# COMMAND ----------

def _build_rows(ticker: str, cik: str, filing: dict, member: str | None,
                scraped_at: datetime) -> list[dict]:
    arts = _discover_artifacts(filing["dir_url"], filing["primary_doc"])
    if arts is None:
        return []
    keep_roles = _statement_roles(arts["summary"])
    if not keep_roles:
        print("      ⚠ no IS/BS/CF statement roles in FilingSummary")
        return []
    lines  = _parse_presentation(arts["pre"], keep_roles)
    labels = _parse_labels(arts["lab"])
    facts  = _index_instance(arts["instance"], member)
    if not facts:
        print("      ⚠ instance had no consolidated facts")
        return []

    is_10k = filing["form"].startswith("10-K")
    rows, seen = [], set()
    for ln in lines:
        ns, localname = _split_frag(ln["frag"])
        for f in facts.get((ns, localname), []):
            start, end = f["start"], f["end"]
            shape = classify_period_shape(
                pd.to_datetime(start) if start else pd.NaT, pd.to_datetime(end))
            if ln["stmt"] == "Balance Sheet":
                if shape != "snapshot":
                    continue
                period_type = "FY" if is_10k else "Q"
            else:
                if is_10k:
                    if shape != "FY_or_TTM":
                        continue
                    period_type = "FY"
                else:
                    if shape not in ("Q_standalone", "YTD_6M", "YTD_9M"):
                        continue
                    period_type = "Q"
            try:
                end_dt = date.fromisoformat(end)
            except ValueError:
                continue
            # Dedup per (stmt, tag, period_end): one presented value per line per period.
            key = (ln["stmt"], localname, end_dt)
            if key in seen:
                continue
            seen.add(key)
            pref_role = ln["pref_role"]
            rows.append({
                "cik":            cik,
                "ticker":         ticker,
                "accession":      filing["accession"],
                "form":           filing["form"],
                "fiscal_year":    end_dt.year,
                "period_end":     end_dt,
                "period_type":    period_type,
                "stmt":           ln["stmt"],
                "oracle_tag":     localname,
                "tag_namespace":  ns,
                "oracle_label":   _label_text(labels, ln["frag"], None)
                                  or TAG_STMT_LABEL.get((localname, ln["stmt"])),
                "preferred_label": _label_text(labels, ln["frag"], pref_role),
                "negated":        bool(pref_role and "negated" in pref_role.lower()),
                "oracle_value":   f["value"],   # natural sign, raw (unscaled)
                "role_uri":       ln["role"],
                "scraped_at":     scraped_at,
            })
    return rows

# COMMAND ----------

# MAGIC %md ## 6. Collect across the Tier-A scope

# COMMAND ----------

scraped_at = datetime.now(timezone.utc)
records, n_filings = [], 0
for ticker, cfg in _scope.items():
    cik = _get_cik(ticker, cfg.get("cik"))
    if not cik:
        print(f"  ✗ {ticker}: no CIK")
        continue
    member = _combined_members.get(ticker)
    try:
        filings = _recent_filings(cik, cfg["n_10k"], cfg["n_10q"])
    except Exception as exc:
        print(f"  ✗ {ticker}: submissions failed ({exc})")
        continue
    got_t = 0
    for filing in filings:
        try:
            rows = _build_rows(ticker, cik, filing, member, scraped_at)
        except Exception as exc:
            print(f"    ⚠ {ticker} {filing['form']} {filing['accession']}: parse failed ({exc})")
            continue
        records.extend(rows)
        got_t += len(rows)
        n_filings += 1
    print(f"  ✓ {ticker} [{cik}]: {got_t} oracle rows from {len(filings)} filing(s)")

print(f"\nTotal oracle rows: {len(records):,} from {n_filings} filing(s)")

# COMMAND ----------

# MAGIC %md ## 7. Write `main.config.reconciliation_oracle` (idempotent MERGE)
# MAGIC
# MAGIC `CREATE TABLE IF NOT EXISTS` does **not** evolve schema — if the column set drifts we
# MAGIC DROP+recreate (this is a rebuildable derived audit table in `config`). MERGE key:
# MAGIC `(ticker, accession, stmt, oracle_tag, period_end)` — re-running a filing upserts in place.

# COMMAND ----------

ORACLE_SCHEMA = StructType([
    StructField("cik",             StringType(),    True),
    StructField("ticker",          StringType(),    False),
    StructField("accession",       StringType(),    False),
    StructField("form",            StringType(),    True),
    StructField("fiscal_year",     IntegerType(),   True),
    StructField("period_end",      DateType(),      False),
    StructField("period_type",     StringType(),    True),
    StructField("stmt",            StringType(),    False),
    StructField("oracle_tag",      StringType(),    False),
    StructField("tag_namespace",   StringType(),    True),
    StructField("oracle_label",    StringType(),    True),
    StructField("preferred_label", StringType(),    True),
    StructField("negated",         BooleanType(),   True),
    StructField("oracle_value",    DoubleType(),    True),
    StructField("role_uri",        StringType(),    True),
    StructField("scraped_at",      TimestampType(), True),
])
_DESIRED_COLS = [f.name for f in ORACLE_SCHEMA.fields]


def _table_exists(fqn: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False


# Schema-drift guard: drop+recreate if the existing column set differs.
if _table_exists(ORACLE_TABLE):
    _existing = [f.name for f in spark.table(ORACLE_TABLE).schema.fields]
    if _existing != _DESIRED_COLS:
        print(f"⚠ schema drift on {ORACLE_TABLE} — dropping and recreating.")
        spark.sql(f"DROP TABLE {ORACLE_TABLE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ORACLE_TABLE} (
        cik             STRING,
        ticker          STRING  NOT NULL,
        accession       STRING  NOT NULL,
        form            STRING,
        fiscal_year     INT,
        period_end      DATE    NOT NULL,
        period_type     STRING,
        stmt            STRING  NOT NULL,
        oracle_tag      STRING  NOT NULL,
        tag_namespace   STRING,
        oracle_label    STRING,
        preferred_label STRING,
        negated         BOOLEAN,
        oracle_value    DOUBLE,
        role_uri        STRING,
        scraped_at      TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if records:
    pdf = pd.DataFrame(records).drop_duplicates(
        subset=["ticker", "accession", "stmt", "oracle_tag", "period_end"], keep="first")
    pdf["fiscal_year"] = pdf["fiscal_year"].astype("Int64")
    pdf = pdf[_DESIRED_COLS]
    sdf = spark.createDataFrame(pdf, schema=ORACLE_SCHEMA)
    sdf.createOrReplaceTempView("incoming_oracle")
    spark.sql(f"""
        MERGE INTO {ORACLE_TABLE} AS t
        USING incoming_oracle AS s
        ON  t.ticker     = s.ticker
        AND t.accession  = s.accession
        AND t.stmt       = s.stmt
        AND t.oracle_tag = s.oracle_tag
        AND t.period_end = s.period_end
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ Merged {len(pdf):,} oracle rows → {ORACLE_TABLE}")
else:
    print("⊘ Nothing to write.")

# COMMAND ----------

# MAGIC %md ## 8. Verify — row counts + sample

# COMMAND ----------

spark.sql(f"""
    SELECT tag_namespace, stmt, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers
    FROM {ORACLE_TABLE}
    GROUP BY tag_namespace, stmt
    ORDER BY tag_namespace, stmt
""").display()

spark.sql(f"""
    SELECT ticker, form, stmt, oracle_tag, tag_namespace, period_end, negated,
           ROUND(oracle_value, 2) AS oracle_value, oracle_label
    FROM {ORACLE_TABLE}
    WHERE ticker IN ('AAPL', 'T')
    ORDER BY ticker, stmt, period_end DESC, oracle_tag
    LIMIT 60
""").display()
