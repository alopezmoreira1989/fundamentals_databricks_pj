# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 33__history_audit
# MAGIC
# MAGIC Audita la cobertura histórica de cada ticker activo contra la SEC companyfacts API
# MAGIC y flagea los que probablemente tienen histórico cortado por una de estas tres causas:
# MAGIC
# MAGIC 1. **CIK predecesor faltante** (`flag_short_history`) — tras una fusión o
# MAGIC    conversión MLP→C-corp, el ticker apunta a un CIK nuevo cuyo `companyfacts`
# MAGIC    solo contiene filings recientes; el histórico vive bajo el CIK antiguo.
# MAGIC 2. **Concept renaming** (`flag_concept_gap`) — el tag XBRL canónico que
# MAGIC    ingestamos (p.ej. `Revenues`) tiene menos años que un sinónimo (p.ej.
# MAGIC    `RevenueFromContractWithCustomerExcludingAssessedTax`); falta añadir el
# MAGIC    sinónimo a `CONCEPT_SYNONYMS` en `01__tickers.py`.
# MAGIC 3. **Año stub/transición** (`flag_stub_years`) — hay rows con `fp='FY'` en
# MAGIC    10-K con duración fuera de 350–380d (p.ej. cambio de fiscal year-end);
# MAGIC    el filtro estricto antiguo los descartaba.
# MAGIC
# MAGIC **Escribe a**: `main.financials.history_audit` (Delta, overwrite cada run —
# MAGIC es un snapshot del estado SEC en el momento de la corrida).
# MAGIC
# MAGIC **NO escribe** a `financials_raw`, `financials`, ni a otras tablas del pipeline.
# MAGIC
# MAGIC **Cómo correr**:
# MAGIC - Por defecto: audita todos los `ACTIVE_TICKERS` (~3000 tickers, ~13 min con 8 workers).
# MAGIC - Override ad-hoc: setear `TICKERS_OVERRIDE = ["VNOM", "AAPL"]` antes de ejecutar.

# COMMAND ----------

# MAGIC %md ## 0. Load config

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/01__tickers"

# COMMAND ----------

# ── Override manual: lista de tickers; None = todos los activos ──────────────
TICKERS_OVERRIDE: "list[str] | None" = None

# ── Heurísticas ──────────────────────────────────────────────────────────────
MIN_EXPECTED_YEARS = 5     # menos años que esto → flag_short_history
MIN_REVENUE_GAP    = 2     # años de diferencia entre sinónimos → flag_concept_gap

# ── Parallelism & rate limit (mismo patrón que 11__fetch_sec_xbrl.py) ────────
MAX_WORKERS     = 8
MIN_REQUEST_GAP = 0.12
REQUEST_TIMEOUT = 30

# ── Output ───────────────────────────────────────────────────────────────────
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.history_audit"

# COMMAND ----------

import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Windows console (cp1252) revienta con → ✓ ✗ ⚠ — fuerza UTF-8 si el stream lo soporta.
# No-op en Databricks.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HEADERS = {"User-Agent": SEC_USER_AGENT}

# COMMAND ----------

# MAGIC %md ## 1. Lista de tickers a auditar

# COMMAND ----------

if TICKERS_OVERRIDE:
    AUDIT_TICKERS = [t.upper().strip() for t in TICKERS_OVERRIDE]
    print(f"✓ Override manual — auditando {len(AUDIT_TICKERS)} ticker(s): {AUDIT_TICKERS[:20]}{'…' if len(AUDIT_TICKERS) > 20 else ''}")
else:
    tickers_df = spark.table(f"{CATALOG}.config.tickers").select("ticker").collect()
    AUDIT_TICKERS = [row.ticker.upper() for row in tickers_df]
    print(f"✓ Universo completo — auditando {len(AUDIT_TICKERS):,} tickers desde {CATALOG}.config.tickers")

# COMMAND ----------

# MAGIC %md ## 2. Conceptos a sondear
# MAGIC
# MAGIC Familias amplias para detectar renaming. Si el canónico tiene menos cobertura
# MAGIC que cualquier alternativo del mismo grupo, lo flageamos.

# COMMAND ----------

REVENUE_FAMILY = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "OilAndGasRevenue",
    "InterestAndDividendIncomeOperating",
]

NET_INCOME_FAMILY = [
    "NetIncomeLoss",
    "ProfitLoss",
    "NetIncomeLossAttributableToParent",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
]

ASSETS_FAMILY = [
    "Assets",
]

ALL_PROBES = REVENUE_FAMILY + NET_INCOME_FAMILY + ASSETS_FAMILY

# ── Tags XBRL que el pipeline YA ingesta y colapsa a "Revenue" ───────────────
# Derivado de INCOME_STATEMENT + CONCEPT_SYNONYMS (heredados del %run de 01__tickers).
# Si un tag de REVENUE_FAMILY no está aquí, significa que el pipeline lo desconoce
# y dispararemos flag_concept_gap. Mantener este set en sync con el pipeline es
# automático — al añadir un sinónimo a 01__tickers.py, este set se actualiza solo.
INGESTED_REVENUE_TAGS = {
    tag
    for label, (tag, _kind) in INCOME_STATEMENT.items()
    if label == "Revenue" or CONCEPT_SYNONYMS.get(label) == "Revenue"
}
print(f"  Pipeline reconoce {len(INGESTED_REVENUE_TAGS)} tag(s) como Revenue: {sorted(INGESTED_REVENUE_TAGS)}")

# COMMAND ----------

# MAGIC %md ## 3. Rate limiter + ticker index (cached)

# COMMAND ----------

_rate_lock = Lock()
_last_request_ts = [0.0]

def rate_limited_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    with _rate_lock:
        wait = _last_request_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()
    return requests.get(url, headers=HEADERS, timeout=timeout)


print("Loading SEC ticker index...")
_idx_resp = rate_limited_get("https://www.sec.gov/files/company_tickers.json")
_idx_resp.raise_for_status()
_idx = _idx_resp.json()

TICKER_MAP = {
    entry["ticker"].upper(): (str(entry["cik_str"]).zfill(10), entry["title"])
    for entry in _idx.values()
}
print(f"✓ Ticker index loaded — {len(TICKER_MAP):,} tickers known to SEC")

# COMMAND ----------

# MAGIC %md ## 4. Helpers — SEC fetch + análisis de un ticker

# COMMAND ----------

def classify_duration(start, end):
    if pd.isna(start):
        return "snapshot"
    days = (pd.to_datetime(end) - pd.to_datetime(start)).days
    if   70  <= days <=  100: return "Q_standalone"
    elif 160 <= days <=  200: return "YTD_6M"
    elif 250 <= days <=  290: return "YTD_9M"
    elif 350 <= days <=  380: return "FY_or_TTM"
    else:                     return f"other_{days}d"


def concept_fy_coverage(facts: dict, concept: str) -> "tuple[set[int], int]":
    """
    Devuelve (set de fy distintos con fp='FY' en 10-K family, n rows con period_shape='other_*').
    Para conceptos de balance (snapshot), devuelve los años a partir de period_end.
    """
    try:
        units    = facts["facts"]["us-gaap"][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return set(), 0

    if not rows:
        return set(), 0

    df = pd.DataFrame(rows)
    df = df[df["form"].isin(["10-K", "10-K/A"])]
    if df.empty:
        return set(), 0

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
    else:
        df["start"] = pd.NaT

    df["period_shape"] = df.apply(lambda r: classify_duration(r["start"], r["end"]), axis=1)

    # Stocks: snapshots, año = end.year. Flows: fp='FY' y period_shape!='snapshot'
    is_stock = df["period_shape"].eq("snapshot").all()
    if is_stock:
        years = set(df["end"].dt.year.dropna().astype(int).tolist())
        n_stub = 0
    else:
        df_fy = df[(df["fp"] == "FY") & (df["period_shape"] != "snapshot")].copy()
        if df_fy.empty:
            return set(), 0
        years = set(df_fy["fy"].dropna().astype(int).tolist())
        n_stub = int(df_fy["period_shape"].astype(str).str.startswith("other_").sum())

    return years, n_stub


def audit_ticker(ticker: str) -> dict:
    """
    Audita un ticker. Devuelve dict con todos los campos requeridos por el schema
    de la tabla destino. En caso de error, devuelve un dict con `error` poblado y
    los campos numéricos a NULL.
    """
    base = {
        "ticker":               ticker,
        "current_cik":          None,
        "former_names":         [],
        "first_10k_year":       None,
        "last_10k_year":        None,
        "n_10k":                0,
        "years_revenue":        [],
        "years_net_income":     [],
        "years_assets":         [],
        "flag_short_history":   False,
        "flag_concept_gap":     False,
        "flag_stub_years":      False,
        "n_flags":              0,
        "concept_max_coverage": None,
        "action_recommended":   None,
        "error":                None,
    }

    # Resolver CIK
    cik_info = TICKER_MAP.get(ticker.upper())
    if cik_info is None:
        base["error"] = "ticker_not_in_sec_index"
        base["action_recommended"] = "Ticker no resuelve en SEC ticker index — añadir override 'cik' en favorites.json"
        return base
    cik, _ = cik_info
    base["current_cik"] = cik

    # Submissions: formerNames + 10-K dates
    try:
        sub_resp = rate_limited_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
        sub_resp.raise_for_status()
        subs = sub_resp.json()
        base["former_names"] = [f.get("name", "") for f in subs.get("formerNames", []) if f.get("name")]

        recent = subs.get("filings", {}).get("recent", {})
        forms  = recent.get("form", [])
        fdates = recent.get("filingDate", [])
        ten_k_dates = [fdates[i] for i, f in enumerate(forms) if f in ("10-K", "10-K/A")]
        if ten_k_dates:
            years_10k = [int(d[:4]) for d in ten_k_dates]
            base["first_10k_year"] = min(years_10k)
            base["last_10k_year"]  = max(years_10k)
            base["n_10k"]          = len(ten_k_dates)
    except Exception as e:
        base["error"] = f"submissions_fetch_failed: {str(e)[:100]}"
        return base

    # Companyfacts
    try:
        facts_resp = rate_limited_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
        facts_resp.raise_for_status()
        if "json" not in facts_resp.headers.get("Content-Type", "").lower():
            base["error"] = "non_json_facts"
            return base
        facts = facts_resp.json()
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", 0)
        if code == 404:
            base["error"] = "no_companyfacts"
            base["action_recommended"] = "Emisor sin companyfacts (puede ser muy pequeño o nuevo)"
        else:
            base["error"] = f"facts_http_{code}"
        return base
    except Exception as e:
        base["error"] = f"facts_fetch_failed: {str(e)[:100]}"
        return base

    # Coverage por familia: nos quedamos con el sinónimo de mejor cobertura por familia
    family_results = {}  # family_name → {best_concept, best_years, all_concept_years}
    family_stubs = 0

    for family_name, family_concepts in [
        ("revenue", REVENUE_FAMILY),
        ("net_income", NET_INCOME_FAMILY),
        ("assets", ASSETS_FAMILY),
    ]:
        per_concept = {}
        for concept in family_concepts:
            years, n_stub = concept_fy_coverage(facts, concept)
            if years:
                per_concept[concept] = years
            family_stubs += n_stub

        if per_concept:
            best_concept = max(per_concept.items(), key=lambda kv: len(kv[1]))[0]
            best_years   = per_concept[best_concept]
        else:
            best_concept, best_years = None, set()

        family_results[family_name] = {
            "best_concept": best_concept,
            "best_years":   best_years,
            "per_concept":  per_concept,
        }

    base["years_revenue"]    = sorted(family_results["revenue"]["best_years"])
    base["years_net_income"] = sorted(family_results["net_income"]["best_years"])
    base["years_assets"]     = sorted(family_results["assets"]["best_years"])
    base["concept_max_coverage"] = family_results["revenue"]["best_concept"]

    # ── Flag 1: histórico corto ─────────────────────────────────────────────
    all_years = (
        family_results["revenue"]["best_years"]
        | family_results["net_income"]["best_years"]
        | family_results["assets"]["best_years"]
    )
    if all_years and (max(all_years) - min(all_years) + 1) < MIN_EXPECTED_YEARS:
        base["flag_short_history"] = True

    # ── Flag 2: concept gap (lo que el pipeline ingesta vs el mejor sinónimo) ─
    # Si el mejor sinónimo de la familia Revenue cubre MIN_REVENUE_GAP años más que
    # los tags que el pipeline reconoce (INGESTED_REVENUE_TAGS, derivado de
    # CONCEPT_SYNONYMS) → flag. Tras añadir un sinónimo a 01__tickers.py el flag
    # desaparece automáticamente.
    revenue_per_concept = family_results["revenue"]["per_concept"]
    if revenue_per_concept:
        canonical_years = set()
        for tag in INGESTED_REVENUE_TAGS:
            canonical_years |= revenue_per_concept.get(tag, set())
        best_years = family_results["revenue"]["best_years"]
        gap = len(best_years - canonical_years)
        if gap >= MIN_REVENUE_GAP:
            base["flag_concept_gap"] = True

    # ── Flag 3: stub years ─────────────────────────────────────────────────
    if family_stubs > 0:
        base["flag_stub_years"] = True

    base["n_flags"] = int(base["flag_short_history"]) + int(base["flag_concept_gap"]) + int(base["flag_stub_years"])

    # ── Acción recomendada ─────────────────────────────────────────────────
    actions = []
    if base["flag_short_history"]:
        fn_hint = base["former_names"] or "sin formerNames; mirar Previous CIKs en EDGAR"
        actions.append(
            "CIK predecesor probable — buscar en EDGAR full-text por formerNames "
            f"({fn_hint}) y anadir a cik_aliases en favorites.json"
        )
    if base["flag_concept_gap"]:
        best_tag = base["concept_max_coverage"]
        actions.append(
            f"Sinonimo a anadir: el tag {best_tag} cubre mas anos que el canonico — "
            "anadirlo a INCOME_STATEMENT en 01__tickers.py y al dict CONCEPT_SYNONYMS apuntando a Revenue"
        )
    if base["flag_stub_years"]:
        actions.append(
            "Hay anos con period_shape=other_* (stubs/transiciones). La capa c de 21__clean_and_merge.py "
            "(max-duration window) ya los captura — verificar que ese ticker tiene FY tras re-ingestar."
        )
    base["action_recommended"] = " | ".join(actions) if actions else None
    return base

# COMMAND ----------

# MAGIC %md ## 5. Run audit en paralelo

# COMMAND ----------

started_at = time.monotonic()
results    = []
state_lock = Lock()
completed  = [0]
total      = len(AUDIT_TICKERS)

def _worker(ticker: str):
    rec = audit_ticker(ticker)
    with state_lock:
        completed[0] += 1
        n = completed[0]
        results.append(rec)
        if n % 100 == 0 or n == total:
            elapsed = time.monotonic() - started_at
            rate    = n / elapsed if elapsed else 0
            eta_s   = (total - n) / rate if rate else 0
            print(f"  [{n:>5}/{total}]  ({rate:.1f} t/s, ETA {eta_s/60:.1f} min)")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(_worker, t) for t in AUDIT_TICKERS]
    for _ in as_completed(futures):
        pass

elapsed = time.monotonic() - started_at
print(f"\n✓ Auditoría completa en {elapsed/60:.1f} min ({total/elapsed:.1f} t/s)")

# COMMAND ----------

# MAGIC %md ## 6. Build pandas DF + escribir Delta

# COMMAND ----------

audit_df = pd.DataFrame(results)
audit_df["audited_at"] = datetime.utcnow()

# Coerce nullable int columns para que createDataFrame no se queje
for col in ("first_10k_year", "last_10k_year", "n_10k", "n_flags"):
    audit_df[col] = audit_df[col].astype("Int64")

# Schema explícito (orden y nullability para que coincida exactamente con la tabla)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, ArrayType,
)

schema = StructType([
    StructField("ticker",               StringType(),               False),
    StructField("current_cik",          StringType(),               True),
    StructField("former_names",         ArrayType(StringType()),    True),
    StructField("first_10k_year",       IntegerType(),              True),
    StructField("last_10k_year",        IntegerType(),              True),
    StructField("n_10k",                IntegerType(),              True),
    StructField("years_revenue",        ArrayType(IntegerType()),   True),
    StructField("years_net_income",     ArrayType(IntegerType()),   True),
    StructField("years_assets",         ArrayType(IntegerType()),   True),
    StructField("flag_short_history",   BooleanType(),              True),
    StructField("flag_concept_gap",     BooleanType(),              True),
    StructField("flag_stub_years",      BooleanType(),              True),
    StructField("n_flags",              IntegerType(),              True),
    StructField("concept_max_coverage", StringType(),               True),
    StructField("action_recommended",   StringType(),               True),
    StructField("error",                StringType(),               True),
    StructField("audited_at",           TimestampType(),            False),
])

# Reordenar columnas al orden del schema
audit_df = audit_df[[f.name for f in schema.fields]]

sdf = spark.createDataFrame(audit_df, schema=schema)

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AUDIT_TABLE)
)
print(f"✓ {sdf.count():,} filas escritas → {AUDIT_TABLE}")

# COMMAND ----------

# MAGIC %md ## 7. Resumen — top sospechosos

# COMMAND ----------

print("\n" + "="*78)
print(f"  RESUMEN AUDITORÍA  —  {AUDIT_TABLE}")
print("="*78)

n_total      = len(audit_df)
n_errors     = audit_df["error"].notna().sum()
n_flagged    = (audit_df["n_flags"] > 0).sum()
n_3flags     = (audit_df["n_flags"] == 3).sum()
n_2flags     = (audit_df["n_flags"] == 2).sum()
n_1flag      = (audit_df["n_flags"] == 1).sum()
n_short      = audit_df["flag_short_history"].sum()
n_gap        = audit_df["flag_concept_gap"].sum()
n_stub       = audit_df["flag_stub_years"].sum()

print(f"  Tickers auditados        : {n_total:,}")
print(f"  Con errores fetch        : {n_errors:,}")
print(f"  Con al menos 1 flag      : {n_flagged:,}")
print(f"    - 3 flags (críticos)   : {n_3flags:,}")
print(f"    - 2 flags              : {n_2flags:,}")
print(f"    - 1 flag               : {n_1flag:,}")
print(f"  Breakdown por flag:")
print(f"    flag_short_history     : {n_short:,}")
print(f"    flag_concept_gap       : {n_gap:,}")
print(f"    flag_stub_years        : {n_stub:,}")

# COMMAND ----------

top_suspects = (
    audit_df[audit_df["n_flags"] > 0]
    .sort_values(["n_flags", "n_10k"], ascending=[False, True])
    .head(20)
)

if not top_suspects.empty:
    print("\n── TOP-20 TICKERS MÁS SOSPECHOSOS ──")
    show_cols = ["ticker", "current_cik", "n_flags", "flag_short_history",
                 "flag_concept_gap", "flag_stub_years", "n_10k",
                 "first_10k_year", "last_10k_year", "concept_max_coverage"]
    print(top_suspects[show_cols].to_string(index=False))

    print("\n── ACCIÓN RECOMENDADA (top 10) ──")
    for _, row in top_suspects.head(10).iterrows():
        print(f"\n  {row['ticker']}  (CIK {row['current_cik']}, flags={row['n_flags']})")
        if row["former_names"]:
            print(f"    formerNames: {row['former_names']}")
        print(f"    → {row['action_recommended']}")
else:
    print("\n✓ Ningún ticker flageado — todo el histórico parece completo")

# COMMAND ----------

# MAGIC %md ## 8. Quick SQL para uso posterior
# MAGIC
# MAGIC ```sql
# MAGIC -- Tickers críticos (3 flags)
# MAGIC SELECT * FROM main.financials.history_audit
# MAGIC WHERE n_flags = 3 ORDER BY n_10k ASC;
# MAGIC
# MAGIC -- Candidatos a añadir un sinónimo (flag_concept_gap)
# MAGIC SELECT ticker, current_cik, concept_max_coverage, years_revenue
# MAGIC FROM main.financials.history_audit
# MAGIC WHERE flag_concept_gap = TRUE;
# MAGIC
# MAGIC -- Candidatos a tener CIK predecesor (flag_short_history)
# MAGIC SELECT ticker, current_cik, former_names, first_10k_year, last_10k_year
# MAGIC FROM main.financials.history_audit
# MAGIC WHERE flag_short_history = TRUE ORDER BY n_10k ASC;
# MAGIC ```
