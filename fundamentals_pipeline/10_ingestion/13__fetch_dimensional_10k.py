# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 13__fetch_dimensional_10k  ⚠️ EXPERIMENTAL
# MAGIC
# MAGIC Recupera los conceptos primarios anuales de **combined-filers** (un 10-K que cubre
# MAGIC dos registrantes, p.ej. una REIT + su Operating Partnership: Tanger Inc. / SKT).
# MAGIC
# MAGIC **Por qué hace falta:** en un 10-K combinado, cada línea de estado lleva un miembro
# MAGIC `dei:LegalEntityAxis`. La API `companyfacts` de SEC **descarta los facts dimensionales**,
# MAGIC así que `11__fetch_sec_xbrl` no ve ningún total anual de ingresos / net income / equity
# MAGIC para esos tickers → `21` no encuentra FY → ROE/Net Margin/P-E nulos. Esta etapa baja la
# MAGIC **instancia XBRL del 10-K** (`…_htm.xml`), resuelve dimensiones, y extrae el fact del
# MAGIC **miembro de la entidad padre** (el que corresponde al ticker), emitiéndolo SIN dimensión
# MAGIC a `financials_raw` para que el resto del pipeline (21/21b/22/23) funcione sin cambios.
# MAGIC
# MAGIC **Gate (no penaliza el run normal):** solo procesa los tickers del dict `COMBINED_FILERS`
# MAGIC en `00_config/01__tickers.py` (`ticker → {"member": <local-name del miembro padre>, "cik": opt}`),
# MAGIC p.ej. `"SKT": {"member": "TangerIncMember"}`. Config separada de `favorites.json` a
# MAGIC propósito (estar ahí NO marca el ticker como favorito). Dict vacío → **no-op**.
# MAGIC
# MAGIC **Estado:** validado localmente contra el 10-K FY2024 de SKT (Revenue 526.06M, Net
# MAGIC Income attribuible 98.595M). Heurística de selección documentada abajo. Tratar como
# MAGIC experimental: revisar la salida antes de confiar en nuevos tickers.
# MAGIC
# MAGIC Databricks-only: usa `spark`, `%run`, escribe a `financials_raw`.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

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
N_FILINGS_BACK = 3   # cuántos 10-K recientes parsear (cada uno trae ~3 años comparativos)

# COMMAND ----------

# MAGIC %md ## 1. Allowlist desde `COMBINED_FILERS` (00_config/01__tickers.py)

# COMMAND ----------

# `COMBINED_FILERS` llega vía %run de 01__tickers: ticker → {"member": ..., "cik": opt}.
# Config separada de favorites.json a propósito (estar aquí NO marca al ticker favorito).
_allowlist = {
    t.upper(): {"member": cfg["member"], "cik": (cfg.get("cik") or None)}
    for t, cfg in (COMBINED_FILERS or {}).items()
}
print(f"Combined-filers en allowlist: {list(_allowlist) or '(ninguno — no-op)'}")

# Mapa xbrl_concept → (stmt, label, kind) para los conceptos que nos interesan.
CONCEPT_LOOKUP = {
    xbrl: (stmt, label, kind)
    for stmt, cmap in STATEMENTS.items()
    for label, (xbrl, kind) in cmap.items()
}

# COMMAND ----------

# MAGIC %md ## 2. Helpers SEC: resolver CIK, último N 10-K, instancia XBRL

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
    """Devuelve URLs de las instancias `…_htm.xml` de los últimos n 10-K."""
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

# MAGIC %md ## 3. Parser de la instancia XBRL
# MAGIC
# MAGIC **Selección del fact padre:** entre los facts anuales (~365d) o snapshot del concepto,
# MAGIC nos quedamos con los que tienen **exactamente una** dimensión `LegalEntityAxis` cuyo
# MAGIC miembro (local-name) == el `member` configurado en `COMBINED_FILERS`. Descartamos facts
# MAGIC con cualquier OTRA dimensión (segmentos, equity components, partner type…) para no coger
# MAGIC desgloses.

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
    """True si el contexto tiene EXACTAMENTE LegalEntityAxis=member y nada más."""
    return len(c["dims"]) == 1 and c["dims"][0] == (_LEGAL_ENTITY_AXIS, member)


def _extract(instance_url: str, member: str) -> list[dict]:
    """Devuelve filas (estilo financials_raw, sin dimensión) para los conceptos objetivo."""
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
        # stock → solo snapshot al cierre; flow → solo anual (~365d)
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

# MAGIC %md ## 4. Recolectar para todos los tickers de la allowlist

# COMMAND ----------

records = []
if _allowlist:
    # Sincroniza scraped_at con el último scrape de 11 para que 21/21b incluyan estas filas
    # (21 filtra por MAX(scraped_at)). Si financials_raw está vacío, usa now.
    latest_scrape = spark.sql(f"SELECT MAX(scraped_at) AS ts FROM {raw_full}").collect()[0]["ts"]
    if latest_scrape is None:
        from datetime import datetime, timezone
        latest_scrape = datetime.now(timezone.utc)
    print(f"scraped_at sincronizado: {latest_scrape}")

    for ticker, cfg in _allowlist.items():
        cik = _get_cik(ticker, cfg["cik"])
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
                print(f"    ⚠ {ticker}: parse falló en {url.split('/')[-1]} ({exc})")
        got = len(records) - n_before
        print(f"  ✓ {ticker} [{cfg['member']}]: {got} filas (concepto×año, miembro padre)")
else:
    print("⊘ Allowlist vacía — no-op.")

print(f"\nTotal filas dimensionales extraídas: {len(records):,}")

# COMMAND ----------

# MAGIC %md ## 5. Append a financials_raw (mismo schema que 11)

# COMMAND ----------

if records:
    # Dedup por (ticker, stmt, concept, fy, period_end): varios 10-K reportan los mismos
    # años comparativos → quédate con uno (los valores coinciden; 21 re-deduplica igualmente).
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

    sdf = spark.createDataFrame(df, schema=SCHEMA_DEF)
    sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(raw_full)
    print(f"✓ {len(df):,} filas dimensionales → {raw_full}")
    print("Muestra:")
    sdf.select("ticker", "stmt", "concept", "fy", "value").orderBy("ticker", "concept", "fy").show(40, truncate=False)
else:
    print("⊘ Nada que escribir.")
