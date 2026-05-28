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
# MAGIC - Por defecto: audita todos los `ACTIVE_TICKERS` (~3000 tickers, ~20–25 min con 8 workers).
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
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from threading import Lock

# Windows console (cp1252) revienta con → ✓ ✗ ⚠ — fuerza UTF-8 si el stream lo soporta.
# No-op en Databricks.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HEADERS = {"User-Agent": SEC_USER_AGENT}

# ── HTTP session compartida ──────────────────────────────────────────────────
# Reusar la misma Session a través de todos los workers ahorra el handshake
# TCP+TLS (~50–150ms por request) en cada llamada después de la primera. El
# pool tiene que ser ≥ MAX_WORKERS para que no se convierta en un punto de
# serialización oculto.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.mount("https://", HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2))

# Buffer de latencias para el resumen p50/p95/p99 al final. Capped para que
# no crezca sin límite en runs largos.
_LATENCIES: "deque[float]" = deque(maxlen=20000)

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
    _t = time.monotonic()
    resp = SESSION.get(url, timeout=timeout)
    _LATENCIES.append(time.monotonic() - _t)
    return resp


print("Loading SEC ticker index...")
_idx_resp = rate_limited_get("https://www.sec.gov/files/company_tickers.json")
_idx_resp.raise_for_status()
_idx = _idx_resp.json()

TICKER_MAP = {
    entry["ticker"].upper(): (str(entry["cik_str"]).zfill(10), entry["title"])
    for entry in _idx.values()
}
print(f"✓ Ticker index loaded — {len(TICKER_MAP):,} tickers known to SEC")

# ── CIK aliases desde favorites.json ─────────────────────────────────────────
# Permite que la auditoría vea el histórico combinado de CIKs predecesores
# (fusiones MLP→C-corp, spinoffs). Sin esto, un ticker con cik_aliases
# configurado en el pipeline aparecería falsamente con flag_short_history porque
# el CIK actual de SEC solo tiene filings recientes (caso VNOM).
_FAV_CIK_ALIASES = {}
try:
    with open(FAVORITES_JSON_PATH, "r", encoding="utf-8") as f:
        _fav_raw = f.read()
    _fav_lines = [l for l in _fav_raw.splitlines() if not l.strip().startswith("/")]
    for _entry in json.loads("\n".join(_fav_lines)):
        _t = _entry["ticker"].upper().strip()
        _aliases = [str(c).zfill(10) for c in _entry.get("cik_aliases", []) if c]
        if _aliases:
            _FAV_CIK_ALIASES[_t] = _aliases
except Exception as _e:
    print(f"  ⚠ Could not load cik_aliases from favorites.json: {_e}")
if _FAV_CIK_ALIASES:
    print(f"  ✓ CIK aliases activos para auditoría: {_FAV_CIK_ALIASES}")

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


def merge_facts(*facts_dicts: dict) -> dict:
    """
    Concatena los arrays `facts[ns][concept]["units"][unit]` a través de múltiples
    JSONs de companyfacts. Misma lógica que 11__fetch_sec_xbrl.merge_facts —
    duplicada aquí para evitar acoplar este script (que es read-only) con el
    notebook de ingesta (que tiene side effects al ejecutarse vía %run).
    """
    if not facts_dicts:
        return {}
    if len(facts_dicts) == 1:
        return facts_dicts[0]

    merged = {"facts": {}}
    for k, v in facts_dicts[0].items():
        if k != "facts":
            merged[k] = v

    for fd in facts_dicts:
        for ns, concepts in fd.get("facts", {}).items():
            ns_bucket = merged["facts"].setdefault(ns, {})
            for concept, payload in concepts.items():
                if concept not in ns_bucket:
                    ns_bucket[concept] = {
                        "label":       payload.get("label"),
                        "description": payload.get("description"),
                        "units":       {u: list(rows) for u, rows in payload.get("units", {}).items()},
                    }
                else:
                    existing_units = ns_bucket[concept]["units"]
                    for unit_key, rows in payload.get("units", {}).items():
                        existing_units.setdefault(unit_key, []).extend(rows)
    return merged


def derive_10k_stats(facts: dict) -> "tuple[int|None, int|None, int]":
    """
    Deriva (first_10k_year, last_10k_year, n_10k) de los rows de companyfacts
    sin llamar /submissions. Recorre todos los conceptos sondeados y junta los
    distintos (fy, accn) en form='10-K'/'10-K/A'.

    Tradeoff respecto al método antiguo (contar filings desde /submissions):
    si un 10-K no reporta NINGUNO de los conceptos en ALL_PROBES, no se cuenta.
    Para emisores normales eso no pasa (todos reportan Revenue/NetIncome/Assets).
    n_10k es ahora "cantidad de accessions 10-K observadas en conceptos sondeados".
    """
    fys: "set[int]" = set()
    accns: "set[str]" = set()
    ns_bucket = facts.get("facts", {}).get("us-gaap", {})
    for concept in ALL_PROBES:
        units = ns_bucket.get(concept, {}).get("units", {})
        for unit_rows in units.values():
            for row in unit_rows:
                if row.get("form") not in ("10-K", "10-K/A"):
                    continue
                fy = row.get("fy")
                if fy is not None:
                    fys.add(int(fy))
                accn = row.get("accn")
                if accn:
                    accns.add(accn)
    return (min(fys) if fys else None, max(fys) if fys else None, len(accns))


def _build_action_recommended(rec: dict) -> "str | None":
    actions = []
    if rec["flag_short_history"]:
        fn_hint = rec["former_names"] or "sin formerNames; mirar Previous CIKs en EDGAR"
        actions.append(
            "CIK predecesor probable — buscar en EDGAR full-text por formerNames "
            f"({fn_hint}) y anadir a cik_aliases en favorites.json"
        )
    if rec["flag_concept_gap"]:
        best_tag = rec["concept_max_coverage"]
        actions.append(
            f"Sinonimo a anadir: el tag {best_tag} cubre mas anos que el canonico — "
            "anadirlo a INCOME_STATEMENT en 01__tickers.py y al dict CONCEPT_SYNONYMS apuntando a Revenue"
        )
    if rec["flag_stub_years"]:
        actions.append(
            "Hay anos con period_shape=other_* (stubs/transiciones). La capa c de 21__clean_and_merge.py "
            "(max-duration window) ya los captura — verificar que ese ticker tiene FY tras re-ingestar."
        )
    return " | ".join(actions) if actions else None


def fetch_former_names(current_cik: str, aliased_ciks: "list[str]") -> "list[str]":
    """
    Llamada lazy a /submissions para popular formerNames. Se invoca SOLO para
    tickers con flag_short_history en el segundo pase (post-paralelo). Errores
    en alias son silenciosos; un error en el CIK primario devuelve [].
    """
    names: "set[str]" = set()
    for c in [current_cik] + list(aliased_ciks or []):
        if not c:
            continue
        try:
            resp = rate_limited_get(f"https://data.sec.gov/submissions/CIK{c}.json")
            resp.raise_for_status()
            for fn in resp.json().get("formerNames", []):
                if fn.get("name"):
                    names.add(fn["name"])
        except Exception:
            continue
    return sorted(names)


def audit_ticker(ticker: str) -> dict:
    """
    Audita un ticker. Devuelve dict con todos los campos requeridos por el schema
    de la tabla destino. En caso de error, devuelve un dict con `error` poblado y
    los campos numéricos a NULL.

    NOTA: este pase NO llama a /submissions. former_names queda vacío y se rellena
    en un segundo pase lazy solo para tickers con flag_short_history (es el único
    flag cuya action_recommended depende de formerNames).
    """
    base = {
        "ticker":               ticker,
        "current_cik":          None,
        "aliased_ciks":         [],
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

    # Resolver CIK primario
    cik_info = TICKER_MAP.get(ticker.upper())
    if cik_info is None:
        base["error"] = "ticker_not_in_sec_index"
        base["action_recommended"] = "Ticker no resuelve en SEC ticker index — añadir override 'cik' en favorites.json"
        return base
    cik, _ = cik_info
    base["current_cik"] = cik

    # CIKs a auditar: primario + aliases configurados en favorites.json.
    # El primario es obligatorio; los aliases son best-effort (un alias roto no
    # tumba la auditoría del ticker — solo se pierde su contribución).
    aliases = _FAV_CIK_ALIASES.get(ticker.upper(), [])
    base["aliased_ciks"] = aliases
    cik_list = [cik] + [a for a in aliases if a != cik]

    # ── Companyfacts: fetch en cada CIK y merge ────────────────────────────
    facts_list = []
    for c in cik_list:
        try:
            facts_resp = rate_limited_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json")
            facts_resp.raise_for_status()
            if "json" not in facts_resp.headers.get("Content-Type", "").lower():
                if c == cik:
                    base["error"] = "non_json_facts"
                    return base
                continue
            facts_list.append(facts_resp.json())
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", 0)
            if c == cik:
                if code == 404:
                    base["error"] = "no_companyfacts"
                    base["action_recommended"] = "Emisor sin companyfacts (puede ser muy pequeño o nuevo)"
                else:
                    base["error"] = f"facts_http_{code}"
                return base
            # alias roto — silencioso, seguimos
        except Exception as e:
            if c == cik:
                base["error"] = f"facts_fetch_failed: {str(e)[:100]}"
                return base

    facts = merge_facts(*facts_list) if facts_list else {}
    if not facts.get("facts"):
        base["error"] = "no_facts_after_merge"
        return base

    # ── 10-K stats derivados de companyfacts (evita el fetch a /submissions) ─
    first_y, last_y, n_10k = derive_10k_stats(facts)
    base["first_10k_year"] = first_y
    base["last_10k_year"]  = last_y
    base["n_10k"]          = n_10k

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

    # action_recommended se calcula aquí; si flag_short_history dispara, el pase
    # lazy de /submissions lo va a regenerar después con formerNames poblado.
    base["action_recommended"] = _build_action_recommended(base)
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
print(f"\n✓ Pase principal completo en {elapsed/60:.1f} min ({total/elapsed:.1f} t/s)")

# ── Pase lazy: /submissions solo para flag_short_history ────────────────────
# formerNames únicamente influye en action_recommended cuando flag_short_history
# dispara. Saltarlo en el pase principal nos ahorra ~3000 requests (uno por
# ticker). Aquí los recuperamos solo para los tickers flageados (típicamente
# <100) y regeneramos action_recommended para esos.
flagged_idxs = [i for i, r in enumerate(results) if r.get("flag_short_history")]
if flagged_idxs:
    print(f"\n  Pase lazy /submissions para {len(flagged_idxs)} ticker(s) flageados…")
    lazy_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=4) as pool:
        fut_to_i = {
            pool.submit(fetch_former_names, results[i]["current_cik"], results[i].get("aliased_ciks") or []): i
            for i in flagged_idxs
        }
        for fut in as_completed(fut_to_i):
            i = fut_to_i[fut]
            try:
                results[i]["former_names"] = fut.result()
            except Exception as e:
                results[i]["former_names"] = []
                print(f"    ⚠ {results[i]['ticker']}: {str(e)[:80]}")
            results[i]["action_recommended"] = _build_action_recommended(results[i])
    print(f"  ✓ Pase lazy completo en {(time.monotonic()-lazy_started):.1f}s")

# Resumen de latencias HTTP (cap: últimas 20k requests del run)
if _LATENCIES:
    lats_ms = sorted(l * 1000 for l in _LATENCIES)
    n_lat   = len(lats_ms)
    p50     = lats_ms[n_lat // 2]
    p95     = lats_ms[min(n_lat - 1, int(n_lat * 0.95))]
    p99     = lats_ms[min(n_lat - 1, int(n_lat * 0.99))]
    print(f"  HTTP latency  p50: {p50:.0f}ms  p95: {p95:.0f}ms  p99: {p99:.0f}ms  (n={n_lat})")

total_elapsed = time.monotonic() - started_at
print(f"✓ Auditoría completa en {total_elapsed/60:.1f} min (total)")

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
    StructField("aliased_ciks",         ArrayType(StringType()),    True),
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

# Guard: cuando TICKERS_OVERRIDE está activo (modo test), NO sobreescribir la
# tabla — preservamos el snapshot del último run completo y solo imprimimos.
if TICKERS_OVERRIDE:
    print(f"⚠ TICKERS_OVERRIDE activo — saltando write a {AUDIT_TABLE} para no clobberar el snapshot completo.")
    print(f"\n── Resultado del run ({len(audit_df)} ticker(s)) ──")
    show_cols = ["ticker", "current_cik", "aliased_ciks", "n_flags",
                 "flag_short_history", "flag_concept_gap", "flag_stub_years",
                 "n_10k", "first_10k_year", "last_10k_year",
                 "concept_max_coverage", "error"]
    print(audit_df[show_cols].to_string(index=False))
else:
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
