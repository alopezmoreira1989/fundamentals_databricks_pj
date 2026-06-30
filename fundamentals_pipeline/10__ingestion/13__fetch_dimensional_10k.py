# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 13__fetch_dimensional_10k  ⚠️ EXPERIMENTAL
# MAGIC
# MAGIC Fetches the primary annual concepts for **combined-filers** (a 10-K that covers
# MAGIC two registrants, e.g. a REIT + its Operating Partnership: Tanger Inc. / SKT).
# MAGIC
# MAGIC **Why this is needed:** in a combined 10-K, each statement line carries a
# MAGIC `dei:LegalEntityAxis` member. The SEC `companyfacts` API **drops dimensional facts**,
# MAGIC so `11__fetch_sec_xbrl` sees no annual totals for revenue / net income / equity
# MAGIC for those tickers → `21` finds no FY → ROE/Net Margin/P-E are null. This stage
# MAGIC downloads the **XBRL instance of the 10-K** (`…_htm.xml`), resolves dimensions,
# MAGIC and extracts the fact for the **parent-entity member** (the one matching the ticker),
# MAGIC emitting it WITHOUT a dimension to `financials_raw` so the rest of the pipeline
# MAGIC (21/21b/22/23) works unchanged.
# MAGIC
# MAGIC **Gate (no penalty on normal runs):** only processes tickers in the `COMBINED_FILERS`
# MAGIC dict in `00__config/01__tickers.py` (`ticker → {"member": <local-name of parent member>, "cik": opt}`),
# MAGIC e.g. `"SKT": {"member": "TangerIncMember"}`. Config kept separate from `favorites.json`
# MAGIC intentionally (being there does NOT mark the ticker as a favourite). Empty dict → **no-op**.
# MAGIC
# MAGIC **Status:** validated locally against SKT's FY2024 10-K (Revenue 526.06M, Net
# MAGIC Income attributable 98.595M). Selection heuristic documented below. Treat as
# MAGIC experimental: review output before trusting new tickers.
# MAGIC
# MAGIC Databricks-only: uses `spark`, `%run`, writes to `financials_raw`.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import re
import requests
from datetime import date

import pandas as pd
from lxml import etree
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType,
)

HEADERS  = {"User-Agent": SEC_USER_AGENT}
raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
N_FILINGS_BACK = 3   # how many recent 10-Ks to parse (each carries ~3 comparative years)

# COMMAND ----------

# MAGIC %md ## 1. Allowlist from `COMBINED_FILERS` (00__config/01__tickers.py)

# COMMAND ----------

# `COMBINED_FILERS` arrives via %run from 01__tickers: ticker → {"member": ..., "cik": opt}.
# Config kept separate from favorites.json intentionally (being here does NOT mark the ticker as a favourite).
_allowlist = {
    t.upper(): {"member": cfg["member"], "cik": (cfg.get("cik") or None)}
    for t, cfg in (COMBINED_FILERS or {}).items()
}
print(f"Combined-filers en allowlist: {list(_allowlist) or '(ninguno — no-op)'}")

# Map xbrl_concept → (stmt, label, kind) for the concepts we care about.
# The tag slot may be a str or a LIST of fallback tags in priority order
# (see extract_series_multi in 11__fetch_sec_xbrl.py). We expand each tag to the same
# (stmt, label, kind); with setdefault, the FIRST label that claims a tag wins —
# consistent with the first-hit-wins logic in ingestion.
CONCEPT_LOOKUP: dict[str, tuple] = {}
for stmt, cmap in STATEMENTS.items():
    for label, (xbrl, kind) in cmap.items():
        for tag in ([xbrl] if isinstance(xbrl, str) else xbrl):
            CONCEPT_LOOKUP.setdefault(tag, (stmt, label, kind))

# COMMAND ----------

# MAGIC %md ## 2. SEC helpers: resolve CIK, latest N 10-Ks, XBRL instance

# COMMAND ----------

def _get_cik(ticker: str, override: str | None) -> str | None:
    if override:
        return override
    try:
        idx = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS, timeout=30).json()
        for e in idx.values():
            if e["ticker"].upper() == ticker.upper():
                return str(e["cik_str"]).zfill(10)
    except Exception as exc:
        print(f"    ⚠ {ticker}: no se pudo resolver CIK ({exc})")
    return None


def _recent_10k_instances(cik: str, n: int) -> list[str]:
    """Returns URLs for the `…_htm.xml` instances of the last n 10-Ks."""
    cik_int = int(cik)
    subs = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=HEADERS, timeout=60).json()
    rec = subs["filings"]["recent"]
    urls = []
    for form, acc, doc in zip(rec["form"], rec["accessionNumber"], rec["primaryDocument"]):
        if form not in ("10-K", "10-K/A"):
            continue
        acc_nodash = acc.replace("-", "")
        stem = re.sub(r"\.htm$", "", doc, flags=re.I)
        urls.append(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{stem}_htm.xml")
        if len(urls) >= n:
            break
    return urls

# COMMAND ----------

# MAGIC %md ## 3. XBRL instance parser
# MAGIC
# MAGIC **Parent-fact selection:** among the annual (~365d) or snapshot facts for a concept,
# MAGIC we keep those with **exactly one** `LegalEntityAxis` dimension whose member
# MAGIC (local-name) matches the `member` configured in `COMBINED_FILERS`. We discard facts
# MAGIC with any OTHER dimension (segments, equity components, partner type…) to avoid
# MAGIC picking up breakdowns.

# COMMAND ----------

_XBRLI = "http://www.xbrl.org/2003/instance"
_LEGAL_ENTITY_AXIS = "LegalEntityAxis"   # local-name de dei:LegalEntityAxis


def _parse_contexts(root) -> dict:
    ctx = {}
    for c in root.findall(f"{{{_XBRLI}}}context"):
        per = c.find(f"{{{_XBRLI}}}period")
        if per is None:
            continue
        start = per.find(f"{{{_XBRLI}}}startDate")
        end   = per.find(f"{{{_XBRLI}}}endDate")
        inst  = per.find(f"{{{_XBRLI}}}instant")
        dims = []
        seg = c.find(f".//{{{_XBRLI}}}segment")
        if seg is not None:
            for m in seg:
                axis = (m.get("dimension") or "").split(":")[-1]
                member = (m.text or "").strip().split(":")[-1]
                dims.append((axis, member))
        ctx[c.get("id")] = {
            "start":   start.text if start is not None else None,
            "end":     end.text   if end   is not None else None,
            "instant": inst.text  if inst  is not None else None,
            "dims":    dims,
        }
    return ctx


def _parent_fact(c: dict, member: str) -> bool:
    """True if the context has EXACTLY LegalEntityAxis=member and nothing else."""
    return len(c["dims"]) == 1 and c["dims"][0] == (_LEGAL_ENTITY_AXIS, member)


def _extract(instance_url: str, member: str) -> list[dict]:
    """Returns rows (financials_raw style, without dimension) for the target concepts."""
    resp = requests.get(instance_url, headers=HEADERS, timeout=60)
    if resp.status_code != 200 or b"<xbrl" not in resp.content[:5000].lower():
        return []
    root = etree.fromstring(resp.content)
    ctx = _parse_contexts(root)

    rows = []
    for el in root.iter():
        q = etree.QName(el)
        if not q.namespace or "us-gaap" not in q.namespace:
            continue
        meta = CONCEPT_LOOKUP.get(q.localname)
        if meta is None or el.text is None:
            continue
        c = ctx.get(el.get("contextRef"))
        if c is None or not _parent_fact(c, member):
            continue

        stmt, label, kind = meta
        start = c["start"]
        end   = c["end"] or c["instant"]
        if end is None:
            continue
        shape = classify_period_shape(
            pd.to_datetime(start) if start else pd.NaT, pd.to_datetime(end)
        )
        # stock → snapshot at close only; flow → annual only (~365d)
        if kind == "stock":
            if shape != "snapshot":
                continue
        else:
            if shape != "FY_or_TTM":
                continue
        try:
            value = float(el.text)
        except (TypeError, ValueError):
            continue

        end_dt = date.fromisoformat(end)
        rows.append({
            "stmt":         stmt,
            "concept":      label,
            "kind":         kind,
            "fy":           end_dt.year,           # FYE de diciembre (combined-filer REITs)
            "fp":           "FY",
            "form":         "10-K",
            "period_start": date.fromisoformat(start) if start else None,
            "period_end":   end_dt,
            "period_shape": shape,
            "value":        value,
        })
    return rows

# COMMAND ----------

# MAGIC %md ## 4. Collect for all tickers in the allowlist

# COMMAND ----------

records = []
if _allowlist:
    # Sync scraped_at with the last scrape from 11 so that 21/21b include these rows
    # (21 filters by MAX(scraped_at)). If financials_raw is empty, use now.
    latest_scrape = spark.sql(f"SELECT MAX(scraped_at) AS ts FROM {raw_full}").collect()[0]["ts"]
    if latest_scrape is None:
        from datetime import datetime, timezone
        latest_scrape = datetime.now(timezone.utc)
    print(f"scraped_at sincronizado: {latest_scrape}")

    for ticker, cfg in _allowlist.items():
        try:
            cik = _get_cik(ticker, cfg["cik"])
        except Exception as exc:
            print(f"  ✗ {ticker}: resolución de CIK falló ({exc})")
            continue
        if not cik:
            continue
        try:
            instances = _recent_10k_instances(cik, N_FILINGS_BACK)
        except Exception as exc:
            print(f"  ✗ {ticker}: submissions falló ({exc})")
            continue
        n_before = len(records)
        for url in instances:
            try:
                for r in _extract(url, cfg["member"]):
                    r.update({"ticker": ticker.upper(), "company": ticker.upper(), "scraped_at": latest_scrape})
                    records.append(r)
            except Exception as exc:
                print(f"    ⚠ {ticker}: parse failed on {url.split('/')[-1]} ({exc})")
        got = len(records) - n_before
        print(f"  ✓ {ticker} [{cfg['member']}]: {got} rows (concept×year, parent member)")
else:
    print("⊘ Empty allowlist — no-op.")

print(f"\nTotal dimensional rows extracted: {len(records):,}")

# COMMAND ----------

# MAGIC %md ## 5. Append to financials_raw (same schema as 11)

# COMMAND ----------

if records:
    # Dedup by (ticker, stmt, concept, fy, period_end): multiple 10-Ks report the same
    # comparative years → keep one (values match; 21 deduplicates again anyway).
    df = pd.DataFrame(records).drop_duplicates(
        subset=["ticker", "stmt", "concept", "fy", "period_end"], keep="first"
    )
    df["fy"] = df["fy"].astype("Int64")

    SCHEMA_DEF = StructType([
        StructField("ticker",        StringType(),    False),
        StructField("company",       StringType(),    True),
        StructField("stmt",          StringType(),    False),
        StructField("concept",       StringType(),    False),
        StructField("kind",          StringType(),    False),
        StructField("fy",            IntegerType(),   True),
        StructField("fp",            StringType(),    True),
        StructField("form",          StringType(),    True),
        StructField("period_start",  DateType(),      True),
        StructField("period_end",    DateType(),      False),
        StructField("period_shape",  StringType(),    True),
        StructField("value",         DoubleType(),    True),
        StructField("filed",         DateType(),      True),
        StructField("scraped_at",    TimestampType(), True),
    ])
    df["filed"] = None
    df = df[[f.name for f in SCHEMA_DEF.fields]]

    # Experimental stage: a write failure must NOT bring down the pipeline (91 runs it via %run).
    try:
        sdf = spark.createDataFrame(df, schema=SCHEMA_DEF)
        sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(raw_full)
        print(f"✓ {len(df):,} dimensional rows → {raw_full}")
        print("Sample:")
        sdf.select("ticker", "stmt", "concept", "fy", "value").orderBy("ticker", "concept", "fy").show(40, truncate=False)
    except Exception as exc:
        print(f"⚠ Dimensional append failed (non-fatal): {exc}")
else:
    print("⊘ Nothing to write.")
