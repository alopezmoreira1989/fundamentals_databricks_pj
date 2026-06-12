# Databricks notebook source
# MAGIC %md
# MAGIC # POC — 10-Q ingestion logic
# MAGIC
# MAGIC **Objective:** validate the derived-Q4 math and map how heterogeneous
# MAGIC the filing landscape is across companies, BEFORE touching the pipeline.
# MAGIC
# MAGIC **Writes to no table.** Only prints results for visual inspection.
# MAGIC
# MAGIC **What it validates:**
# MAGIC 1. Do companies report `Revenues` as standalone (~90d), cumulative YTD, or both?
# MAGIC 2. Does Q1 + Q2 + Q3 + Q4_derived = FY?
# MAGIC 3. Does `Assets` (Balance Sheet) come as a point-in-time snapshot?
# MAGIC 4. Are SEC's `fp` and `fy` fields reliable even for non-calendar fiscal years?
# MAGIC
# MAGIC **Ticker sample** chosen to cover distinct profiles:
# MAGIC - `AAPL` — fiscal year ends in September (the "hard" case)
# MAGIC - `MSFT` — fiscal year ends in June
# MAGIC - `WMT`  — fiscal year ends in January
# MAGIC - `JPM`  — financial (may have different concepts)
# MAGIC - `O`    — REIT (different structure)
# MAGIC - `KO`   — calendar-year FY, classic large cap

# COMMAND ----------

import requests
import pandas as pd
import time
from collections import defaultdict

HEADERS = {"User-Agent": "POC quarterly research"}  # ← change this to your real User-Agent

SAMPLE_TICKERS = ["AAPL", "MSFT", "WMT", "JPM", "O", "KO"]

# Representative concepts: 1 IS flow, 1 CF flow, 1 BS stock
PROBE_CONCEPTS = {
    "Revenues":                                    ("Income Statement", "flow"),
    "RevenueFromContractWithCustomerExcludingAssessedTax": ("Income Statement", "flow"),
    "NetCashProvidedByUsedInOperatingActivities":  ("Cash Flow",        "flow"),
    "Assets":                                      ("Balance Sheet",    "stock"),
}

# COMMAND ----------

# MAGIC %md ## 1. Helpers — fetch SEC + classify duration

# COMMAND ----------

def get_cik(ticker: str) -> tuple:
    url  = "https://www.sec.gov/files/company_tickers.json"
    data = requests.get(url, headers=HEADERS, timeout=10).json()
    for entry in data.values():
        if entry["ticker"].upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10), entry["title"]
    raise ValueError(f"Ticker '{ticker}' not found")


def get_facts(cik: str) -> dict:
    url  = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(0.15)
    return resp.json()


def classify_duration(start, end):
    """Returns the period 'shape' based on end-start. None if it is a stock concept."""
    if pd.isna(start):
        return "snapshot"   # stock concept — no tiene start
    days = (pd.to_datetime(end) - pd.to_datetime(start)).days
    if   70  <= days <=  100: return "Q_standalone"   # ~90d
    elif 160 <= days <=  200: return "YTD_6M"         # ~180d
    elif 250 <= days <=  290: return "YTD_9M"         # ~270d
    elif 350 <= days <=  380: return "FY_or_TTM"      # ~365d
    else:                     return f"other_{days}d"


def extract_concept_raw(facts: dict, concept: str, namespace: str = "us-gaap") -> pd.DataFrame:
    """
    Returns ALL raw rows for the concept, without any filtering.
    Adds derived columns: 'duration_days', 'period_shape'.
    """
    try:
        units    = facts["facts"][namespace][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["end"] = pd.to_datetime(df["end"])
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
        df["duration_days"] = (df["end"] - df["start"]).dt.days
    else:
        df["start"] = pd.NaT
        df["duration_days"] = None

    df["period_shape"] = df.apply(
        lambda r: classify_duration(r["start"], r["end"]), axis=1
    )

    # Keep only 10-K/10-Q/10-K/A/10-Q/A
    df = df[df["form"].isin(["10-K", "10-Q", "10-K/A", "10-Q/A"])].copy()

    return df

# COMMAND ----------

# MAGIC %md ## 2. Shape inventory by ticker × concept
# MAGIC
# MAGIC For each (ticker, concept) counts how many rows there are for each `period_shape`.
# MAGIC This answers the key question: does each company report the standalone Q, the YTD, or both?

# COMMAND ----------

inventory = []

for ticker in SAMPLE_TICKERS:
    print(f"\n── {ticker} ──")
    try:
        cik, company = get_cik(ticker)
        facts = get_facts(cik)
        print(f"  CIK: {cik}  Company: {company}")

        for concept, (stmt, kind) in PROBE_CONCEPTS.items():
            df = extract_concept_raw(facts, concept)
            if df.empty:
                print(f"  {concept:55s} → not reported")
                continue

            shape_counts = df["period_shape"].value_counts().to_dict()
            fp_set       = sorted(df["fp"].unique().tolist())
            form_set     = sorted(df["form"].unique().tolist())

            # Q4 standalone: ~90d rows that appear inside a 10-K (fp="FY").
            # Indicator of whether the company publishes Q4 as standalone in its 10-K.
            q4_std_rows = df[
                (df["fp"] == "FY")
                & (df["form"].isin(["10-K", "10-K/A"]))
                & (df["period_shape"] == "Q_standalone")
            ]
            n_q4_standalone = len(q4_std_rows)

            inventory.append({
                "ticker":          ticker,
                "concept":         concept,
                "kind":            kind,
                "n_rows":          len(df),
                "shapes":          shape_counts,
                "fp":              fp_set,
                "forms":           form_set,
                "n_q4_standalone": n_q4_standalone,
            })
            q4_flag = f"  [Q4_std×{n_q4_standalone}]" if n_q4_standalone else ""
            print(f"  {concept[:55]:55s} → {dict(shape_counts)}{q4_flag}")

    except Exception as e:
        print(f"  ERROR: {e}")

inv_df = pd.DataFrame(inventory)
print("\n\n=== INVENTORY SUMMARY ===")
print(inv_df.to_string())

# COMMAND ----------

# MAGIC %md ## 3. Specific test: Q1+Q2+Q3+Q4 = FY reconciliation (for AAPL, latest complete FY)
# MAGIC
# MAGIC Takes the most recent FY with complete data and applies both strategies in parallel:
# MAGIC - **Strategy A:** direct standalone if available, otherwise derived from YTD
# MAGIC - **Strategy B:** everything derived from YTD (ignore standalones)
# MAGIC
# MAGIC We validate which one reconciles better with the 10-K FY.

# COMMAND ----------

def latest_fy_from_10k(df_concept: pd.DataFrame):
    """Returns the most recent (fy, value, end) reported in a 10-K."""
    fy_rows = df_concept[
        (df_concept["form"].isin(["10-K", "10-K/A"]))
        & (df_concept["fp"] == "FY")
        & (df_concept["period_shape"] == "FY_or_TTM")
    ].copy()
    if fy_rows.empty:
        return None
    # keep the latest filed per fy (restatements)
    fy_rows = fy_rows.sort_values("filed").drop_duplicates(subset=["fy"], keep="last")
    fy_rows = fy_rows.sort_values("fy", ascending=False)
    top = fy_rows.iloc[0]
    return int(top["fy"]), float(top["val"]), top["end"]


def quarter_values_for_fy(df_concept: pd.DataFrame, fy: int):
    """
    For a given fiscal year, returns a dict with all available options:
    - q1_standalone, q2_standalone, q3_standalone, q4_standalone
        ~90d in Q1/Q2/Q3 (from 10-Q) and ~90d in FY (from 10-K, if the company reports it)
    - ytd_q1, ytd_q2, ytd_q3   (~90/180/270d in Q1/Q2/Q3)
    Always keeping the latest 'filed' for each combination.

    Note on Q4 standalone:
    There is no official fp="Q4" in XBRL. When a company publishes Q4 standalone
    in its 10-K, it appears as a ~90d duration with fp="FY" and end == fy_end.
    We detect it by shape (Q_standalone) within 10-K filings, not by fp.
    """
    out = {}

    # ── Standalones + YTD for Q1/Q2/Q3 (come from 10-Q with fp="Qn") ──
    for fp, key_std, key_ytd in [
        ("Q1", "q1_standalone", "ytd_q1"),
        ("Q2", "q2_standalone", "ytd_q2"),
        ("Q3", "q3_standalone", "ytd_q3"),
    ]:
        chunk = df_concept[(df_concept["fy"] == fy) & (df_concept["fp"] == fp)]
        if chunk.empty:
            continue

        # standalone (~90d)
        std = chunk[chunk["period_shape"] == "Q_standalone"].sort_values("filed")
        if not std.empty:
            out[key_std] = float(std.iloc[-1]["val"])

        # YTD (depends on fp)
        ytd_shape = {"Q1": "Q_standalone", "Q2": "YTD_6M", "Q3": "YTD_9M"}[fp]
        ytd = chunk[chunk["period_shape"] == ytd_shape].sort_values("filed")
        if not ytd.empty:
            out[key_ytd] = float(ytd.iloc[-1]["val"])

    # ── Q4 standalone: ~90d inside the 10-K (fp="FY") ──
    # Look for rows with period_shape="Q_standalone" in 10-K filings for the current FY.
    q4_chunk = df_concept[
        (df_concept["fy"] == fy)
        & (df_concept["fp"] == "FY")
        & (df_concept["form"].isin(["10-K", "10-K/A"]))
        & (df_concept["period_shape"] == "Q_standalone")
    ].sort_values("filed")
    if not q4_chunk.empty:
        out["q4_standalone"] = float(q4_chunk.iloc[-1]["val"])

    return out


def reconcile(ticker: str, concept: str):
    cik, company = get_cik(ticker)
    facts = get_facts(cik)
    df = extract_concept_raw(facts, concept)
    if df.empty:
        print(f"{ticker} / {concept}: not reported")
        return

    fy_info = latest_fy_from_10k(df)
    if fy_info is None:
        print(f"{ticker} / {concept}: no FY row found")
        return

    fy, fy_val, fy_end = fy_info
    qvals = quarter_values_for_fy(df, fy)

    # ── Q1/Q2/Q3: standalone if available, derived otherwise ──
    def derive_qstandalone(q, qvals):
        std_key = f"q{q}_standalone"
        if std_key in qvals:
            return qvals[std_key], "standalone"
        ytd_now = qvals.get(f"ytd_q{q}")
        ytd_prev = qvals.get(f"ytd_q{q-1}") if q > 1 else 0
        if ytd_now is None or ytd_prev is None:
            return None, "missing"
        return ytd_now - ytd_prev, "derived_from_ytd"

    q1_a, src_q1 = derive_qstandalone(1, qvals)
    q2_a, src_q2 = derive_qstandalone(2, qvals)
    q3_a, src_q3 = derive_qstandalone(3, qvals)

    # ── Q4 via TWO paths so they can be cross-checked ──
    ytd_q3 = qvals.get("ytd_q3")
    q4_via_fy_minus_ytd = (fy_val - ytd_q3) if ytd_q3 is not None else None
    q4_standalone       = qvals.get("q4_standalone")  # None if the company does not report it

    # We choose the "official" one following the ingestion strategy:
    # FY − YTD_Q3 is the primary method (always available when YTD_Q3 exists);
    # standalone is the cross-check when available.
    if q4_via_fy_minus_ytd is not None:
        q4_a, src_q4 = q4_via_fy_minus_ytd, "FY − YTD_Q3"
    elif q4_standalone is not None:
        q4_a, src_q4 = q4_standalone, "Q4 standalone (10-K)"
    else:
        q4_a, src_q4 = None, "missing"

    print(f"\n{'─'*70}")
    print(f"  {ticker}  ({company})  /  {concept}")
    print(f"  FY {fy} (10-K, ends {fy_end.date()})  =  {fy_val:>20,.0f}")
    print(f"{'─'*70}")
    for q, val, src in [(1,q1_a,src_q1),(2,q2_a,src_q2),(3,q3_a,src_q3),(4,q4_a,src_q4)]:
        if val is None:
            print(f"  Q{q}  =  {'<missing>':>20}   ({src})")
        else:
            print(f"  Q{q}  =  {val:>20,.0f}   ({src})")

    # ── Cross-check of the two Q4 paths (if both are available) ──
    if q4_via_fy_minus_ytd is not None and q4_standalone is not None:
        diff_q4 = q4_standalone - q4_via_fy_minus_ytd
        base    = q4_via_fy_minus_ytd if q4_via_fy_minus_ytd else 1
        pct_q4  = (diff_q4 / base * 100) if base else 0
        print(f"  {'─'*64}")
        print(f"  Q4 cross-check:")
        print(f"    via FY−YTD_Q3        =  {q4_via_fy_minus_ytd:>20,.0f}")
        print(f"    via Q4 standalone    =  {q4_standalone:>20,.0f}")
        print(f"    Diff:                   {diff_q4:>+20,.0f}  ({pct_q4:+.4f}%)")
        if abs(pct_q4) < 0.01:
            print(f"    ✓ Both paths agree")
        else:
            print(f"    ✗ Discrepancy between paths — investigate")
    elif q4_standalone is None and q4_via_fy_minus_ytd is not None:
        print(f"  (Q4 standalone not reported — only available via FY−YTD_Q3)")

    # ── Σ Q1..Q4 == FY reconciliation ──
    if None not in (q1_a, q2_a, q3_a, q4_a):
        total = q1_a + q2_a + q3_a + q4_a
        diff  = total - fy_val
        pct   = (diff / fy_val * 100) if fy_val else 0
        print(f"  {'─'*64}")
        print(f"  Σ Q1..Q4 =  {total:>20,.0f}")
        print(f"  Diff vs FY: {diff:>+20,.0f}  ({pct:+.4f}%)")
        if abs(pct) < 0.01:
            print(f"  ✓ CUADRA")
        else:
            print(f"  ✗ NO CUADRA — investigar")

# COMMAND ----------

# MAGIC %md ## 4. Run reconcile over the sample
# MAGIC
# MAGIC For each ticker, we try the most likely Revenue concept.
# MAGIC Some companies use `Revenues`, others use `RevenueFromContractWithCustomerExcludingAssessedTax`.

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    for concept in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"]:
        try:
            reconcile(ticker, concept)
        except Exception as e:
            print(f"{ticker} / {concept}: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 5. Reconcile over Operating Cash Flow (sanity check with a different concept)

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    try:
        reconcile(ticker, "NetCashProvidedByUsedInOperatingActivities")
    except Exception as e:
        print(f"{ticker} / OCF: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 6. Balance Sheet — confirm it is a pure snapshot
# MAGIC
# MAGIC For `Assets` we expect:
# MAGIC - `start` always NaT (no start, only end)
# MAGIC - `period_shape` = "snapshot"
# MAGIC - Multiple values per year (one per filing where it appears)

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    try:
        cik, _ = get_cik(ticker)
        facts = get_facts(cik)
        df = extract_concept_raw(facts, "Assets")
        if df.empty:
            print(f"{ticker} / Assets: not reported"); continue
        df = df.sort_values("end", ascending=False).head(5)
        print(f"\n── {ticker} / Assets — last 5 reports ──")
        print(df[["end", "fy", "fp", "form", "period_shape", "val", "filed"]].to_string(index=False))
    except Exception as e:
        print(f"{ticker} / Assets: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 7. Conclusions to verify manually
# MAGIC
# MAGIC After running this, answer:
# MAGIC
# MAGIC 1. **Do all companies report YTD?** (expected: yes, SEC rule)
# MAGIC 2. **Do some ALSO report standalone Q?** (expected: most do, at ~90d duration)
# MAGIC 3. **Are there companies that report ONLY standalone without YTD?** (expected: rare, but check)
# MAGIC 4. **Does the "standalone if available, derive otherwise" strategy reconcile to the cent with FY?**
# MAGIC 5. **Does any ticker have a significant `period_shape = other_Xd`?** (would be a rare case to investigate)
# MAGIC 6. **Are `fp` and `fy` consistent even for AAPL/MSFT/WMT (non-calendar FY)?**
# MAGIC 7. **Which companies publish Q4 standalone in their 10-K?** (check `n_q4_standalone` in the inventory
# MAGIC    and the "Q4 cross-check" section of reconcile)
# MAGIC 8. **When both Q4 paths exist (FY−YTD_Q3 vs Q4 standalone), do they match?**
# MAGIC    If not → the company restated YTD_Q3 when filing the 10-K, or uses a different concept in Q4.
# MAGIC
# MAGIC If everything reconciles, the ingestion strategy is:
# MAGIC ```
# MAGIC Q1, Q2, Q3:  prefer standalone (~90d); fallback to YTD_n − YTD_(n-1)
# MAGIC Q4:          always derived = FY_10K − YTD_Q3
# MAGIC BS concepts: direct snapshot from the 'end' of each filing
# MAGIC ```
