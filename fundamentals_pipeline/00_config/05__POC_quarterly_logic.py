# Databricks notebook source
# MAGIC %md
# MAGIC # POC — Lógica de ingestión 10-Q
# MAGIC
# MAGIC **Objetivo:** validar la matemática del Q4 derivado y mapear cuán heterogéneo
# MAGIC es el panorama de filings entre empresas, ANTES de tocar el pipeline.
# MAGIC
# MAGIC **No escribe en ninguna tabla.** Solo imprime resultados para inspección visual.
# MAGIC
# MAGIC **Lo que valida:**
# MAGIC 1. ¿Las empresas reportan `Revenues` como standalone (~90d), YTD acumulado, o ambos?
# MAGIC 2. ¿Cuadra Q1 + Q2 + Q3 + Q4_derivado = FY?
# MAGIC 3. ¿`Assets` (Balance Sheet) viene como snapshot puntual?
# MAGIC 4. ¿Los campos `fp` y `fy` de SEC son fiables incluso para fiscal years no-calendario?
# MAGIC
# MAGIC **Muestra de tickers** elegida para cubrir perfiles distintos:
# MAGIC - `AAPL` — fiscal year acaba en septiembre (caso "duro")
# MAGIC - `MSFT` — fiscal year acaba en junio
# MAGIC - `WMT`  — fiscal year acaba en enero
# MAGIC - `JPM`  — financial (puede tener concepts distintos)
# MAGIC - `O`    — REIT (estructura distinta)
# MAGIC - `KO`   — calendar year FY, gran cap clásica

# COMMAND ----------

import requests
import pandas as pd
import time
from collections import defaultdict

HEADERS = {"User-Agent": "POC quarterly research"}  # ← cámbialo por tu User-Agent real

SAMPLE_TICKERS = ["AAPL", "MSFT", "WMT", "JPM", "O", "KO"]

# Concepts representativos: 1 flow del IS, 1 flow del CF, 1 stock del BS
PROBE_CONCEPTS = {
    "Revenues":                                    ("Income Statement", "flow"),
    "RevenueFromContractWithCustomerExcludingAssessedTax": ("Income Statement", "flow"),
    "NetCashProvidedByUsedInOperatingActivities":  ("Cash Flow",        "flow"),
    "Assets":                                      ("Balance Sheet",    "stock"),
}

# COMMAND ----------

# MAGIC %md ## 1. Helpers — fetch SEC + clasificar duración

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
    """Devuelve la 'forma' del periodo según end-start. None si es stock."""
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
    Devuelve TODOS los rows raw del concept, sin filtrar nada.
    Añade columnas derivadas: 'duration_days', 'period_shape'.
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

    # Solo quedarnos con 10-K/10-Q/10-K/A/10-Q/A
    df = df[df["form"].isin(["10-K", "10-Q", "10-K/A", "10-Q/A"])].copy()

    return df

# COMMAND ----------

# MAGIC %md ## 2. Inventario de "formas" por ticker × concept
# MAGIC
# MAGIC Para cada (ticker, concept) cuenta cuántas filas hay de cada `period_shape`.
# MAGIC Esto contesta la pregunta clave: ¿cada empresa reporta el standalone Q, el YTD, o ambos?

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

            # Q4 standalone: ~90d que aparecen dentro de 10-K (fp="FY").
            # Indicador de si la empresa publica el Q4 como standalone en su 10-K.
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

# MAGIC %md ## 3. Test específico: cuadre Q1+Q2+Q3+Q4 = FY (para AAPL, último FY completo)
# MAGIC
# MAGIC Coge el FY más reciente con datos completos y aplica las dos estrategias en paralelo:
# MAGIC - **Estrategia A:** standalone directo si existe, si no derivado de YTD
# MAGIC - **Estrategia B:** todo derivado de YTD (ignorar standalones)
# MAGIC
# MAGIC Validamos cuál cuadra mejor con el FY del 10-K.

# COMMAND ----------

def latest_fy_from_10k(df_concept: pd.DataFrame):
    """Devuelve el (fy, value, end) más reciente reportado en un 10-K."""
    fy_rows = df_concept[
        (df_concept["form"].isin(["10-K", "10-K/A"]))
        & (df_concept["fp"] == "FY")
        & (df_concept["period_shape"] == "FY_or_TTM")
    ].copy()
    if fy_rows.empty:
        return None
    # quedarnos con el último filed por fy (restatements)
    fy_rows = fy_rows.sort_values("filed").drop_duplicates(subset=["fy"], keep="last")
    fy_rows = fy_rows.sort_values("fy", ascending=False)
    top = fy_rows.iloc[0]
    return int(top["fy"]), float(top["val"]), top["end"]


def quarter_values_for_fy(df_concept: pd.DataFrame, fy: int):
    """
    Para un fiscal year concreto, devuelve dict con todas las opciones disponibles:
    - q1_standalone, q2_standalone, q3_standalone, q4_standalone
        ~90d en Q1/Q2/Q3 (de 10-Q) y ~90d en FY (de 10-K, si la empresa lo reporta)
    - ytd_q1, ytd_q2, ytd_q3   (~90/180/270d en Q1/Q2/Q3)
    Quedándonos siempre con el último 'filed' para cada combinación.

    Nota sobre Q4 standalone:
    En XBRL no existe un fp="Q4" oficial. Cuando una empresa publica el Q4 standalone
    en su 10-K, aparece como una duration de ~90d con fp="FY" y end == fy_end.
    Lo detectamos por forma (Q_standalone) dentro de filings 10-K, no por fp.
    """
    out = {}

    # ── Standalones + YTD para Q1/Q2/Q3 (vienen de 10-Q con fp="Qn") ──
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

        # YTD (depende del fp)
        ytd_shape = {"Q1": "Q_standalone", "Q2": "YTD_6M", "Q3": "YTD_9M"}[fp]
        ytd = chunk[chunk["period_shape"] == ytd_shape].sort_values("filed")
        if not ytd.empty:
            out[key_ytd] = float(ytd.iloc[-1]["val"])

    # ── Q4 standalone: ~90d dentro del 10-K (fp="FY") ──
    # Buscamos rows con period_shape="Q_standalone" en filings 10-K del FY actual.
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

    # ── Q1/Q2/Q3: standalone si existe, derivado si no ──
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

    # ── Q4 por DOS vías para poder cruzarlas ──
    ytd_q3 = qvals.get("ytd_q3")
    q4_via_fy_minus_ytd = (fy_val - ytd_q3) if ytd_q3 is not None else None
    q4_standalone       = qvals.get("q4_standalone")  # None si la empresa no lo reporta

    # Elegimos el "oficial" siguiendo la estrategia de ingestión:
    # FY − YTD_Q3 es el método primario (siempre disponible si hay YTD_Q3);
    # standalone es el cross-check cuando existe.
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

    # ── Cruce de las dos vías de Q4 (si disponemos de ambas) ──
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
            print(f"    ✓ Ambas vías coinciden")
        else:
            print(f"    ✗ Discrepancia entre vías — investigar")
    elif q4_standalone is None and q4_via_fy_minus_ytd is not None:
        print(f"  (Q4 standalone no reportado — solo disponible vía FY−YTD_Q3)")

    # ── Cuadre Σ Q1..Q4 == FY ──
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

# MAGIC %md ## 4. Run reconcile sobre la muestra
# MAGIC
# MAGIC Para cada ticker, probamos el concept de Revenue más probable.
# MAGIC Algunas empresas usan `Revenues`, otras `RevenueFromContractWithCustomerExcludingAssessedTax`.

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    for concept in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"]:
        try:
            reconcile(ticker, concept)
        except Exception as e:
            print(f"{ticker} / {concept}: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 5. Reconcile sobre Operating Cash Flow (sanity check con un concept distinto)

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    try:
        reconcile(ticker, "NetCashProvidedByUsedInOperatingActivities")
    except Exception as e:
        print(f"{ticker} / OCF: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 6. Balance Sheet — confirmar que es snapshot puro
# MAGIC
# MAGIC Para `Assets` esperamos:
# MAGIC - `start` siempre NaT (sin start, solo end)
# MAGIC - `period_shape` = "snapshot"
# MAGIC - Múltiples valores por año (uno por cada filing donde aparezca)

# COMMAND ----------

for ticker in SAMPLE_TICKERS:
    try:
        cik, _ = get_cik(ticker)
        facts = get_facts(cik)
        df = extract_concept_raw(facts, "Assets")
        if df.empty:
            print(f"{ticker} / Assets: not reported"); continue
        df = df.sort_values("end", ascending=False).head(5)
        print(f"\n── {ticker} / Assets — últimos 5 reportes ──")
        print(df[["end", "fy", "fp", "form", "period_shape", "val", "filed"]].to_string(index=False))
    except Exception as e:
        print(f"{ticker} / Assets: ERROR {e}")

# COMMAND ----------

# MAGIC %md ## 7. Conclusiones a verificar a mano
# MAGIC
# MAGIC Después de correr esto, responder:
# MAGIC
# MAGIC 1. **¿Todas las empresas reportan YTD?** (esperado: sí, es la regla SEC)
# MAGIC 2. **¿Algunas reportan TAMBIÉN standalone Q?** (esperado: la mayoría sí, en duración ~90d)
# MAGIC 3. **¿Hay empresas que reportan SOLO standalone sin YTD?** (esperado: raro, pero comprobar)
# MAGIC 4. **¿La estrategia "standalone si existe, derivar si no" cuadra al céntimo con FY?**
# MAGIC 5. **¿Algún ticker tiene `period_shape = other_Xd` significativo?** (sería un caso raro a investigar)
# MAGIC 6. **¿`fp` y `fy` son consistentes incluso para AAPL/MSFT/WMT (FY no-calendario)?**
# MAGIC 7. **¿Qué empresas publican Q4 standalone en su 10-K?** (mirar `n_q4_standalone` en el inventario
# MAGIC    y la sección "Q4 cross-check" del reconcile)
# MAGIC 8. **Cuando existen las dos vías de Q4 (FY−YTD_Q3 vs Q4 standalone), ¿coinciden?**
# MAGIC    Si no coinciden → la empresa restated YTD_Q3 al publicar el 10-K, o usa un concept distinto en Q4.
# MAGIC
# MAGIC Si todo cuadra, la estrategia de ingestión es:
# MAGIC ```
# MAGIC Q1, Q2, Q3:  preferir standalone (~90d); fallback a YTD_n − YTD_(n-1)
# MAGIC Q4:          siempre derivado = FY_10K − YTD_Q3
# MAGIC BS concepts: snapshot directo del 'end' de cada filing
# MAGIC ```
