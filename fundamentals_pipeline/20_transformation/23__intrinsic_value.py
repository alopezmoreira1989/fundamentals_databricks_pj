# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 23__intrinsic_value
# MAGIC
# MAGIC Calcula el **valor intrínseco** de cada empresa bajo cuatro prismas distintos,
# MAGIC para **cada fiscal year histórico** y para **TTM** (rolling 4 quarters).
# MAGIC
# MAGIC | Method | Idea | Output |
# MAGIC |---|---|---|
# MAGIC | `graham_number` | √(22.5 × EPS × BVPS) — la regla de servilleta de Graham | $/acción |
# MAGIC | `graham_revised` | EPS × (8.5 + 2g) × 4.4 / Y_AAA — fórmula con growth | $/acción |
# MAGIC | `dcf` | DCF 2-stage sobre FCF (u Owner Earnings) | $/acción |
# MAGIC | `owner_earnings` | Owner Earnings × multiple (Buffett) o / discount_rate | $/acción |
# MAGIC
# MAGIC **Output principal:** `{catalog}.{schema}.financials_intrinsic_value` (nueva)
# MAGIC con columnas `period_type ∈ {'FY','TTM'}`, `fiscal_year`, `period_end`.
# MAGIC
# MAGIC Adicionalmente, expone las métricas clave (IV per share y MoS por método y por
# MAGIC período) en `financials_metrics` con sufijos `(FY)` y `(TTM)` para que el
# MAGIC dashboard pueda filtrarlas como cualquier otra métrica.
# MAGIC
# MAGIC **Lee de:**
# MAGIC - `financials` (long-format con `period_type`, `period_end`, `fiscal_year`)
# MAGIC - `market_data` (price_close, market_cap por ticker × fiscal_year)
# MAGIC - `00_config/valuation_assumptions.json` (defaults + overrides por ticker)
# MAGIC
# MAGIC > **Advertencia importante:** las valoraciones son tan buenas como sus supuestos.
# MAGIC > Especialmente el DCF es **muy sensible** a `WACC` y `growth_stage1`. Cambia estos
# MAGIC > parámetros en `valuation_assumptions.json` antes de tomar decisiones reales.

# COMMAND ----------

# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths son **relativos a la ubicación de este notebook** en el workspace.
# MAGIC Si falla con `NameError`, ajusta la ruta para que apunte a `00_config/01__tickers`.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

import json
import math
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# ── Paths & table names ──────────────────────────────────────────────────────
ASSUMPTIONS_JSON_PATH = "../00_config/valuation_assumptions.json"

full_table  = f"{CATALOG}.{SCHEMA}.{TABLE}"
market_tbl  = f"{CATALOG}.{SCHEMA}.market_data"
iv_tbl      = f"{CATALOG}.{SCHEMA}.financials_intrinsic_value"
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"

print(f"Source        : {full_table}")
print(f"Market data   : {market_tbl}")
print(f"Target IV     : {iv_tbl}")
print(f"Metrics table : {metrics_tbl}")

# COMMAND ----------

# MAGIC %md ## 1. Cargar supuestos desde JSON

# COMMAND ----------

def _load_assumptions(path: str) -> dict:
    """JSON con tolerancia a líneas-comentario // y claves _xxx descartables."""
    raw   = Path(path).read_text(encoding="utf-8")
    lines = [l for l in raw.splitlines() if not l.strip().startswith("//")]
    data  = json.loads("\n".join(lines))

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if not k.startswith("_")}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj

    return _clean(data)


ASSUMPTIONS = _load_assumptions(ASSUMPTIONS_JSON_PATH)
DEFAULTS    = ASSUMPTIONS["defaults"]
OVERRIDES   = ASSUMPTIONS.get("overrides", {})

print(f"✓ Loaded assumptions — {len(OVERRIDES)} ticker override(s)")
print(f"  DCF defaults  : WACC={DEFAULTS['dcf']['wacc']}, "
      f"g1={DEFAULTS['dcf']['growth_stage1']}, "
      f"g_terminal={DEFAULTS['dcf']['growth_terminal']}, "
      f"horizon={DEFAULTS['dcf']['horizon_years']}y")


def _merge_dicts(base: dict, over: dict) -> dict:
    out = {k: dict(v) if isinstance(v, dict) else v for k, v in base.items()}
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = {**out[k], **v}
        else:
            out[k] = v
    return out


def assumptions_for(ticker: str) -> dict:
    return _merge_dicts(DEFAULTS, OVERRIDES.get(ticker, {}))

# COMMAND ----------

# MAGIC %md ## 2. Definir los conceptos que necesitamos
# MAGIC
# MAGIC IMPORTANTE: incluimos `stmt` en el join — `Net Income` aparece tanto en
# MAGIC `Income Statement` como en `Cash Flow` (reconciliación), y sin filtrar
# MAGIC duplicaríamos filas al pivotar.

# COMMAND ----------

# (stmt, concept, alias) — orden controlado, sin colisiones de nombre al pivotar
NEEDED = [
    ("Income Statement", "Net Income",                  "net_income"),
    ("Income Statement", "Revenue",                     "revenue"),
    ("Income Statement", "Operating Income",            "op_income"),
    ("Income Statement", "EPS Diluted",                 "eps"),
    ("Income Statement", "Shares Diluted",              "shares"),

    ("Balance Sheet",    "Total Stockholders Equity",   "equity"),
    ("Balance Sheet",    "Total Assets",                "assets"),
    ("Balance Sheet",    "Long-term Debt",              "lt_debt"),
    ("Balance Sheet",    "Short-term Debt",             "st_debt"),
    ("Balance Sheet",    "Cash & Equivalents",          "cash"),
    ("Balance Sheet",    "Short-term Investments",      "st_inv"),
    ("Balance Sheet",    "Retained Earnings",           "retained_earnings"),  # for Graham applicability guard

    ("Cash Flow",        "Operating Cash Flow",         "ocf"),
    ("Cash Flow",        "CapEx",                       "capex"),
    ("Cash Flow",        "Depreciation & Amortization", "dna"),
    ("Cash Flow",        "Stock-based Compensation",    "sbc"),
    ("Cash Flow",        "Changes in Working Capital",  "delta_wc"),
]

# Conceptos de tipo "flow" (sumables a TTM). Los Balance Sheet son "stock" (snapshot).
STOCK_ALIASES = {"equity", "assets", "lt_debt", "st_debt", "cash", "st_inv", "retained_earnings"}
# Shares también es "stock" en sentido TTM: usamos las del quarter más reciente,
# no las sumamos (el alias 'shares' es weighted-average diluted del periodo).
STOCK_ALIASES.add("shares")

ALIAS_OF = {(s, c): a for s, c, a in NEEDED}
ALL_ALIASES = [a for _, _, a in NEEDED]

# Construir el filtro como OR de (stmt, concept) específicos
filter_cond = F.lit(False)
for stmt, concept, _ in NEEDED:
    filter_cond = filter_cond | ((F.col("stmt") == stmt) & (F.col("concept") == concept))

# Subset relevante con alias en columna nueva
fin_subset = (
    spark.table(full_table)
    .filter(filter_cond)
    .filter(F.col("value").isNotNull())
)

# Aplicar mapping stmt+concept → alias usando CASE WHEN
alias_col = F.lit(None).cast("string")
for stmt, concept, alias in NEEDED:
    alias_col = F.when(
        (F.col("stmt") == stmt) & (F.col("concept") == concept),
        F.lit(alias),
    ).otherwise(alias_col)
fin_subset = fin_subset.withColumn("alias", alias_col).filter(F.col("alias").isNotNull())

# Deduplicar por si una scrape introdujo duplicados exactos
fin_subset = fin_subset.dropDuplicates(
    ["ticker", "stmt", "concept", "fiscal_year", "period_type", "period_end"]
)

# Defensa contra company inconsistente por ticker — misma razón que en 22__derived_metrics:
# el MERGE de 21__clean_and_merge solo actualiza company cuando cambia value, así que
# algunas filas viejas conservan un company stale (entityNames SEC con escapes, restructures,
# overrides en favorites.json). Sin esto, el groupBy(ticker, company, fiscal_year) de abajo
# produce filas duplicadas y el MERGE final peta con DELTA_MULTIPLE_SOURCE_ROW_MATCHING.
_company_w = Window.partitionBy("ticker")
fin_subset = fin_subset.withColumn(
    "company", F.first("company", ignorenulls=True).over(_company_w)
)

# Materializa fin_subset (el scan de financials filtrado a ~16 conceptos): lo consumen fy_wide,
# quarters y varios count() → sin esto se re-escanea ~7×. localCheckpoint(eager) lo materializa
# una vez y trunca el linaje. .cache()/.persist() NO van en serverless ([NOT_SUPPORTED_WITH_SERVERLESS]).
fin_subset = fin_subset.localCheckpoint(eager=True)

print(f"✓ Concept subset prepared — {fin_subset.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3. Pivot FY — una fila por (ticker, fiscal_year)

# COMMAND ----------

fy_wide = (
    fin_subset
    .filter(F.col("period_type") == "FY")
    .groupBy("ticker", "company", "fiscal_year")
    .pivot("alias", ALL_ALIASES)
    .agg(F.first("value"))
    .withColumnRenamed("fiscal_year", "year")
)

print(f"✓ FY wide: {fy_wide.count():,} (ticker, year) rows")

# COMMAND ----------

# MAGIC %md ## 4. Pivot TTM — una fila por ticker (rolling 4 quarters)
# MAGIC
# MAGIC Para cada (ticker, alias) ordenamos los quarters por `period_end DESC` y
# MAGIC nos quedamos con los 4 más recientes. Luego:
# MAGIC - **Flow** (IS, CF) → SUMA de los 4 valores
# MAGIC - **Stock** (BS, Shares) → primer valor (el más reciente)

# COMMAND ----------

quarters = (
    fin_subset
    .filter(F.col("period_type").isin("Q1", "Q2", "Q3", "Q4"))
)

w_recent = Window.partitionBy("ticker", "alias").orderBy(F.col("period_end").desc())
quarters_ranked = quarters.withColumn("rn", F.row_number().over(w_recent)) \
                          .filter(F.col("rn") <= 4)

# Materializa quarters_ranked (window de los 4 quarters más recientes): alimenta ttm_flow Y
# ttm_stock → dos lecturas del mismo window. localCheckpoint una vez (cache no va en serverless).
quarters_ranked = quarters_ranked.localCheckpoint(eager=True)

# Aggregate: SUM si flow, FIRST (rn=1) si stock
ttm_flow = (
    quarters_ranked
    .filter(~F.col("alias").isin(list(STOCK_ALIASES)))
    .groupBy("ticker", "alias")
    .agg(
        F.count(F.lit(1)).alias("_n_quarters"),
        F.sum("value").alias("value"),
        F.max("period_end").alias("period_end"),
        F.first("company", ignorenulls=True).alias("company"),
        F.first("fiscal_year").alias("fiscal_year"),  # del más reciente
    )
    # Sólo conservar TTM si tenemos 4 quarters completos
    .filter(F.col("_n_quarters") == 4)
    .drop("_n_quarters")
)

ttm_stock = (
    quarters_ranked
    .filter(F.col("alias").isin(list(STOCK_ALIASES)))
    .filter(F.col("rn") == 1)
    .select(
        "ticker", "alias", "value",
        F.col("period_end"),
        "company",
        "fiscal_year",
    )
)

ttm_long = ttm_flow.unionByName(ttm_stock)

# Pivot a wide. Para period_end y fiscal_year usamos el MAX entre todos los
# aliases del mismo ticker (el más reciente disponible).
ttm_meta = ttm_long.groupBy("ticker").agg(
    F.max("period_end").alias("period_end"),
    F.first("company", ignorenulls=True).alias("company"),
    F.max("fiscal_year").alias("year"),
)

ttm_pivot = (
    ttm_long
    .groupBy("ticker")
    .pivot("alias", ALL_ALIASES)
    .agg(F.first("value"))
)

ttm_wide = ttm_meta.join(ttm_pivot, on="ticker", how="inner")

print(f"✓ TTM wide: {ttm_wide.count():,} ticker rows")

# COMMAND ----------

# MAGIC %md ## 5. Join con market_data y bajar a Pandas
# MAGIC
# MAGIC Para **FY**: precio del mismo `fiscal_year`.
# MAGIC Para **TTM**: precio del `fiscal_year` más reciente disponible en market_data.

# COMMAND ----------

try:
    mkt = (
        spark.table(market_tbl)
        .select(
            "ticker",
            F.col("fiscal_year").alias("year"),
            "price_close",
            "market_cap",
        )
    )
    has_market_data = True
except Exception:
    print("⚠ market_data not available — Margin of Safety will be NULL.")
    mkt = None
    has_market_data = False

# FY: join directo por (ticker, year)
if has_market_data:
    fy_with_price = fy_wide.join(mkt, on=["ticker", "year"], how="left")
else:
    fy_with_price = (
        fy_wide
        .withColumn("price_close", F.lit(None).cast("double"))
        .withColumn("market_cap",  F.lit(None).cast("double"))
    )

# TTM: usar el price_close del año más reciente disponible en market_data para
# cada ticker (puede ser el año actual sin shares todavía, o el del año pasado).
if has_market_data:
    w_latest_price = Window.partitionBy("ticker").orderBy(F.col("year").desc())
    latest_price = (
        mkt
        .filter(F.col("price_close").isNotNull())
        .withColumn("rn", F.row_number().over(w_latest_price))
        .filter(F.col("rn") == 1)
        .select("ticker", F.col("price_close"), F.col("year").alias("price_year"))
    )
    ttm_with_price = ttm_wide.join(latest_price, on="ticker", how="left")
    # market_cap actual (para futuras métricas — no estrictamente necesario aquí)
    ttm_with_price = ttm_with_price.withColumn("market_cap", F.lit(None).cast("double"))
else:
    ttm_with_price = (
        ttm_wide
        .withColumn("price_close", F.lit(None).cast("double"))
        .withColumn("market_cap",  F.lit(None).cast("double"))
        .withColumn("price_year",  F.lit(None).cast("int"))
    )

print(f"✓ FY rows : {fy_with_price.count():,}")
print(f"✓ TTM rows: {ttm_with_price.count():,}")

# COMMAND ----------

# Bajar a pandas — volumen razonable (~30k FY + ~3k TTM)
fy_pdf  = fy_with_price.toPandas()
ttm_pdf = ttm_with_price.toPandas()

# (Sin unpersist: tampoco soportado en serverless. Los localCheckpoint se liberan al cerrar
# la sesión; a partir de aquí todo es pandas/numpy en el driver + escrituras Delta.)

# Asegurar columnas auxiliares
if "period_end" not in fy_pdf.columns:
    fy_pdf["period_end"] = pd.NaT
fy_pdf["period_type"] = "FY"

ttm_pdf["period_type"] = "TTM"
# Renombrar para que ambos pdf compartan schema
# (fy ya tiene 'year'; ttm también)

# Derivados comunes
for pdf in (fy_pdf, ttm_pdf):
    pdf["fcf"]  = pdf["ocf"].astype(float) - pdf["capex"].fillna(0).astype(float)
    pdf["bvps"] = pdf["equity"] / pdf["shares"]
    # Owner Earnings $ (Buffett 1986: NI + D&A + SBC − CapEx − ΔWC). Vectorizado con
    # fillna(0) = el _safe(.., 0) de la versión por-fila. Lo consumen compute_all (vía DCF
    # use_owner_earnings + método owner_earnings) y el paso 9 de exposición de OE absoluto.
    pdf["oe_dollars"] = (
        pdf["net_income"].fillna(0) + pdf["dna"].fillna(0) + pdf["sbc"].fillna(0)
        - pdf["capex"].fillna(0) - pdf["delta_wc"].fillna(0)
    )

# Universe of tickers EVALUATED this run. The exposure/iv MERGEs below only upsert, so a
# method that is now SKIPPED for a ticker (e.g. Graham Number suppressed for a distorted
# book value) would keep its previously-published rows forever. We use this view to scope
# the orphan-deletes (steps 8b / 9b) so we only clean stale rows for tickers we recomputed.
spark.createDataFrame(
    pd.DataFrame({"ticker": pd.Series(
        sorted(set(fy_pdf["ticker"]).union(set(ttm_pdf["ticker"]))), dtype="string")})
).createOrReplaceTempView("iv_processed_tickers")

print(f"✓ FY pandas : {len(fy_pdf):,} rows")
print(f"✓ TTM pandas: {len(ttm_pdf):,} rows")

# COMMAND ----------

# MAGIC %md ## 6. Las cuatro fórmulas

# COMMAND ----------

# Las cuatro fórmulas, VECTORIZADAS sobre todo el dataframe con numpy. La versión anterior
# iteraba fila-a-fila (pdf.iterrows × 4 métodos × un merge de dicts por fila) — el sink de
# wall-clock del paso una vez eliminados los re-scans. Esta versión es equivalente fila-a-fila
# a la anterior (validado a diff < 1e-9 sobre datos sintéticos cubriendo todas las skip-conditions
# y overrides). Cada skip-condition se vuelve una máscara NaN; una fila se incluye sólo si su iv
# no es NaN y > 0, idéntico al `continue` original. El DCF mantiene el LOOP por año (vectorizado
# entre filas, NO closed-form) para reproducir bit-a-bit la suma flotante del loop original.


def _params_for(ticker: str) -> dict:
    """Aplana assumptions_for(ticker) a columnas escalares. Se llama UNA vez por ticker
    ÚNICO (no por fila), reutilizando la misma lógica de merge defaults+overrides."""
    a = assumptions_for(ticker)
    g, gr, d, oe = a["graham"], a["graham_revised"], a["dcf"], a["owner_earnings"]
    return {
        "ticker":         ticker,
        "magic":          float(g["magic_number"]),
        "gr_base_pe":     float(gr["base_pe"]),
        "gr_growth_mult": float(gr["growth_multiplier"]),
        "gr_aaa_norm":    float(gr["aaa_yield_norm"]),
        "gr_aaa_yield":   float(gr["graham_aaa_yield"]),
        "gr_growth_cap":  float(gr["growth_cap"]),
        "dcf_skip":       bool(d.get("skip", False)),
        "dcf_wacc":       float(d["wacc"]),
        "dcf_g1":         float(d["growth_stage1"]),
        "dcf_gt":         float(d["growth_terminal"]),
        "dcf_horizon":    int(d["horizon_years"]),
        "dcf_use_oe":     bool(d.get("use_owner_earnings", False)),
        "oe_skip":        bool(oe.get("skip", False)),
        "oe_method":      oe.get("method", "multiple"),
        "oe_multiple":    float(oe["multiple"]),
        "oe_dr":          float(oe["discount_rate"]),
    }

# COMMAND ----------

# MAGIC %md ## 7. Computar — para cada fila (FY o TTM) × cada método (vectorizado)

# COMMAND ----------

def compute_all(pdf, period_type, computed_at):
    """Calcula los 4 métodos para TODO el dataframe (numpy, sin iterrows) y arma las filas."""
    if len(pdf) == 0:
        return []

    # Parámetros por ticker único → columnas; merge en vez de un dict-merge por fila.
    params = pd.DataFrame([_params_for(t) for t in pdf["ticker"].unique()])
    m = pdf.merge(params, on="ticker", how="left")

    def col(name):
        return m[name].to_numpy(dtype="float64")

    eps, bvps, shares = col("eps"), col("bvps"), col("shares")
    price, retained   = col("price_close"), col("retained_earnings")
    ni, dna, sbc      = col("net_income"), col("dna"), col("sbc")
    capex, dwc        = col("capex"), col("delta_wc")
    fcf               = col("fcf")
    lt_debt, st_debt  = col("lt_debt"), col("st_debt")
    cash, st_inv      = col("cash"), col("st_inv")

    magic          = col("magic")
    gr_base_pe     = col("gr_base_pe")
    gr_growth_mult = col("gr_growth_mult")
    gr_aaa_norm    = col("gr_aaa_norm")
    gr_aaa_yield   = col("gr_aaa_yield")
    gr_growth_cap  = col("gr_growth_cap")
    dcf_skip       = m["dcf_skip"].to_numpy(dtype=bool)
    dcf_wacc, dcf_g1, dcf_gt = col("dcf_wacc"), col("dcf_g1"), col("dcf_gt")
    dcf_horizon    = m["dcf_horizon"].to_numpy(dtype="int64")
    dcf_use_oe     = m["dcf_use_oe"].to_numpy(dtype=bool)
    oe_skip        = m["oe_skip"].to_numpy(dtype=bool)
    oe_method      = m["oe_method"].to_numpy(dtype=object)
    oe_multiple, oe_dr = col("oe_multiple"), col("oe_dr")

    nan = np.nan
    z = lambda arr: np.where(np.isnan(arr), 0.0, arr)   # = _safe(.., 0)
    oe_dollars = z(ni) + z(dna) + z(sbc) - z(capex) - z(dwc)

    with np.errstate(invalid="ignore", divide="ignore"):
        # ── graham_number ──  sqrt(magic·EPS·BVPS); skip si EPS/BVPS no-positivos o book
        # distorsionado (retained < 0, o P/B > 10). bvps>0 garantizado en la rama válida.
        gn_valid = ~np.isnan(eps) & ~np.isnan(bvps) & (eps > 0) & (bvps > 0)
        pb = np.where(bvps != 0, price / bvps, np.inf)
        distort = (~np.isnan(retained) & (retained < 0)) | (~np.isnan(price) & (pb > 10))
        gn = np.where(gn_valid & ~distort, np.sqrt(magic * eps * bvps), nan)

        # ── graham_revised ──  g = min(dcf.growth_stage1, growth_cap) (decisión documentada).
        g_eff = np.minimum(dcf_g1, gr_growth_cap)
        grv_valid = ~np.isnan(eps) & (eps > 0)
        grv = np.where(
            grv_valid,
            eps * (gr_base_pe + gr_growth_mult * g_eff * 100) * gr_aaa_norm / (gr_aaa_yield * 100),
            nan,
        )

        # ── dcf ──  loop por año (vectorizado entre filas) = bit-identical al loop original.
        starting_cf = np.where(dcf_use_oe, oe_dollars, fcf)
        dcf_valid = (
            ~dcf_skip & ~np.isnan(shares) & (shares > 0)
            & ~np.isnan(starting_cf) & (starting_cf > 0) & (dcf_wacc > dcf_gt)
        )
        cf  = np.where(np.isnan(starting_cf), 0.0, starting_cf).astype("float64")
        pv1 = np.zeros_like(cf)
        for t in range(1, int(dcf_horizon.max()) + 1):
            active = t <= dcf_horizon                       # filas cuyo horizonte aún cubre t
            cf  = np.where(active, cf * (1 + dcf_g1), cf)    # deja de crecer pasado el horizonte
            pv1 = np.where(active, pv1 + cf / ((1 + dcf_wacc) ** t), pv1)
        cf_term = cf * (1 + dcf_gt)
        tv      = cf_term / (dcf_wacc - dcf_gt)
        pv_term = tv / ((1 + dcf_wacc) ** dcf_horizon)
        debt    = z(lt_debt) + z(st_debt)
        cash_t  = z(cash) + z(st_inv)
        dcf_ips = (pv1 + pv_term - debt + cash_t) / shares
        dcf = np.where(dcf_valid, dcf_ips, nan)

        # ── owner_earnings ──  OE × múltiplo  ó  OE / discount_rate (perpetuidad Gordon).
        oe_valid = ~oe_skip & (oe_dollars > 0) & ~np.isnan(shares) & (shares > 0)
        total = np.where(
            oe_method == "multiple", oe_dollars * oe_multiple,
            np.where(oe_method == "perpetuity", oe_dollars / oe_dr, nan),
        )
        oev = np.where(oe_valid & ~np.isnan(total), total / shares, nan)

    company    = m["company"].to_numpy(dtype=object)
    ticker_arr = m["ticker"].to_numpy(dtype=object)
    year       = m["year"].to_numpy()
    pend_ts    = pd.to_datetime(m["period_end"], errors="coerce")
    pend       = [p.date() if pd.notna(p) else None for p in pend_ts]

    # Meta diagnóstica (columna `assumptions`) — NO la consume el dashboard ni el export;
    # se conserva por inspección ad-hoc de la tabla. Construida sólo sobre filas supervivientes.
    def _meta_gn(i):
        return {"magic": float(magic[i]), "eps": float(eps[i]), "bvps": float(bvps[i])}

    def _meta_grv(i):
        return {"eps": float(eps[i]), "g": float(g_eff[i]), "base_pe": float(gr_base_pe[i]),
                "growth_mult": float(gr_growth_mult[i]), "aaa_norm": float(gr_aaa_norm[i]),
                "aaa_current_pct": float(gr_aaa_yield[i] * 100)}

    def _meta_dcf(i):
        return {"cf_basis": "owner_earnings" if dcf_use_oe[i] else "fcf",
                "starting_cf": float(starting_cf[i]), "wacc": float(dcf_wacc[i]),
                "g1": float(dcf_g1[i]), "g_terminal": float(dcf_gt[i]), "horizon": int(dcf_horizon[i]),
                "pv_stage1": round(float(pv1[i]), 0), "pv_terminal": round(float(pv_term[i]), 0),
                "debt": float(debt[i]), "cash": float(cash_t[i])}

    def _meta_oe(i):
        meta = {"method": oe_method[i], "oe": float(oe_dollars[i]),
                "oe_per_share": float(oe_dollars[i] / shares[i])}
        if oe_method[i] == "multiple":
            meta["multiple"] = float(oe_multiple[i])
        elif oe_method[i] == "perpetuity":
            meta["discount_rate"] = float(oe_dr[i])
        return meta

    rows = []
    methods = (
        ("graham_number",  gn,  _meta_gn),
        ("graham_revised", grv, _meta_grv),
        ("dcf",            dcf, _meta_dcf),
        ("owner_earnings", oev, _meta_oe),
    )
    for method_name, iv, meta_fn in methods:
        keep = ~np.isnan(iv) & (iv > 0)
        for i in np.nonzero(keep)[0]:
            i   = int(i)
            ivv = float(iv[i])
            sh  = shares[i]
            pr  = price[i]
            rows.append({
                "ticker":                    ticker_arr[i],
                "company":                   company[i],
                "period_type":               period_type,
                "fiscal_year":               int(year[i]) if pd.notna(year[i]) else None,
                "period_end":                pend[i],
                "method":                    method_name,
                "intrinsic_value_per_share": ivv,
                "intrinsic_value_total":     float(ivv * sh) if not np.isnan(sh) else None,
                "price_close":               float(pr) if not np.isnan(pr) else None,
                "margin_of_safety_pct":      float((ivv - pr) / ivv * 100) if not np.isnan(pr) else None,
                "assumptions":               json.dumps(meta_fn(i), default=str),
                "computed_at":               computed_at,
            })
    return rows


computed_at = datetime.utcnow()

fy_rows  = compute_all(fy_pdf,  "FY",  computed_at)
ttm_rows = compute_all(ttm_pdf, "TTM", computed_at)
all_rows = fy_rows + ttm_rows

iv_pdf = pd.DataFrame(all_rows)
print(f"✓ Computed {len(iv_pdf):,} valuations ({len(fy_rows):,} FY + {len(ttm_rows):,} TTM)")
if len(iv_pdf):
    print(iv_pdf.groupby(["period_type", "method"]).size().unstack(fill_value=0))

# COMMAND ----------

# MAGIC %md ## 8. Escribir a Delta (MERGE — idempotente)

# COMMAND ----------

schema = T.StructType([
    T.StructField("ticker",                    T.StringType(),    False),
    T.StructField("company",                   T.StringType(),    True),
    T.StructField("period_type",               T.StringType(),    False),
    T.StructField("fiscal_year",               T.IntegerType(),   True),
    T.StructField("period_end",                T.DateType(),      True),
    T.StructField("method",                    T.StringType(),    False),
    T.StructField("intrinsic_value_per_share", T.DoubleType(),    True),
    T.StructField("intrinsic_value_total",     T.DoubleType(),    True),
    T.StructField("price_close",               T.DoubleType(),    True),
    T.StructField("margin_of_safety_pct",      T.DoubleType(),    True),
    T.StructField("assumptions",               T.StringType(),    True),
    T.StructField("computed_at",               T.TimestampType(), True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {iv_tbl} (
        ticker                    STRING    NOT NULL,
        company                   STRING,
        period_type               STRING    NOT NULL,
        fiscal_year               INT,
        period_end                DATE,
        method                    STRING    NOT NULL,
        intrinsic_value_per_share DOUBLE,
        intrinsic_value_total     DOUBLE,
        price_close               DOUBLE,
        margin_of_safety_pct      DOUBLE,
        assumptions               STRING,
        computed_at               TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if len(iv_pdf):
    iv_sdf = spark.createDataFrame(iv_pdf, schema=schema)
    iv_sdf.createOrReplaceTempView("incoming_iv")

    # MERGE: la clave única es (ticker, period_type, fiscal_year, method).
    # Para TTM, fiscal_year es el del quarter más reciente, así que cada run
    # de TTM upserts la misma fila (si el quarter no ha cambiado) o inserta
    # una nueva (si hay quarter más reciente).
    spark.sql(f"""
        MERGE INTO {iv_tbl} AS target
        USING incoming_iv AS source
        ON  target.ticker      = source.ticker
        AND target.period_type = source.period_type
        AND COALESCE(target.fiscal_year, -1) = COALESCE(source.fiscal_year, -1)
        AND target.method      = source.method

        WHEN MATCHED THEN UPDATE SET
            target.intrinsic_value_per_share = source.intrinsic_value_per_share,
            target.intrinsic_value_total     = source.intrinsic_value_total,
            target.price_close               = source.price_close,
            target.margin_of_safety_pct      = source.margin_of_safety_pct,
            target.assumptions               = source.assumptions,
            target.computed_at               = source.computed_at,
            target.period_end                = source.period_end,
            target.company                   = source.company

        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✓ Merged into {iv_tbl}")

    # ── 8b. Orphan cleanup ──────────────────────────────────────────────────────
    # The MERGE above only upserts. For tickers EVALUATED this run, delete (method,
    # period, year) combinations absent from the freshly-computed `incoming_iv` — i.e.
    # methods that became inapplicable (e.g. Graham Number skipped on a distorted book
    # value) or turned non-positive. Mirrors 21's step-4 orphan DELETE. Scoped to
    # iv_processed_tickers so it never touches tickers not recomputed this run.
    n_orphan_iv = spark.sql(f"""
        SELECT t.ticker FROM {iv_tbl} t
        JOIN iv_processed_tickers p ON t.ticker = p.ticker
        WHERE NOT EXISTS (
            SELECT 1 FROM incoming_iv s
            WHERE s.ticker = t.ticker AND s.period_type = t.period_type
              AND COALESCE(s.fiscal_year, -1) = COALESCE(t.fiscal_year, -1)
              AND s.method = t.method)
    """).count()
    spark.sql(f"""
        MERGE INTO {iv_tbl} AS t
        USING (
            SELECT t.ticker, t.period_type, t.fiscal_year, t.method
            FROM {iv_tbl} t
            JOIN iv_processed_tickers p ON t.ticker = p.ticker
            WHERE NOT EXISTS (
                SELECT 1 FROM incoming_iv s
                WHERE s.ticker = t.ticker AND s.period_type = t.period_type
                  AND COALESCE(s.fiscal_year, -1) = COALESCE(t.fiscal_year, -1)
                  AND s.method = t.method)
        ) AS s
        ON  t.ticker = s.ticker AND t.period_type = s.period_type
        AND COALESCE(t.fiscal_year, -1) = COALESCE(s.fiscal_year, -1)
        AND t.method = s.method
        WHEN MATCHED THEN DELETE
    """)
    print(f"✓ Orphan cleanup on {iv_tbl}: {n_orphan_iv:,} stale method-rows deleted")
else:
    print(f"⊘ No valuations computed — {iv_tbl} unchanged.")

# COMMAND ----------

# MAGIC %md ## 9. Exponer las métricas en `financials_metrics`
# MAGIC
# MAGIC Dos variantes por método: `(FY)` y `(TTM)`. Las filas FY se asocian a su
# MAGIC `fiscal_year` correspondiente; las TTM se escriben con el `fiscal_year` del
# MAGIC quarter más reciente del ticker.

# COMMAND ----------

# Mapeo: (method, output_field, period_type) → metric_name
EXPOSED = [
    # ── FY ──
    ("graham_number",  "FY",  "intrinsic_value_per_share", "Graham Number (FY)"),
    ("graham_number",  "FY",  "margin_of_safety_pct",      "MoS % (Graham Number, FY)"),
    ("graham_revised", "FY",  "intrinsic_value_per_share", "Graham Revised Value (FY)"),
    ("graham_revised", "FY",  "margin_of_safety_pct",      "MoS % (Graham Revised, FY)"),
    ("dcf",            "FY",  "intrinsic_value_per_share", "DCF Value per Share (FY)"),
    ("dcf",            "FY",  "margin_of_safety_pct",      "MoS % (DCF, FY)"),
    ("owner_earnings", "FY",  "intrinsic_value_per_share", "Owner Earnings Value/Share (FY)"),
    ("owner_earnings", "FY",  "margin_of_safety_pct",      "MoS % (Owner Earnings, FY)"),
    # ── TTM ──
    ("graham_number",  "TTM", "intrinsic_value_per_share", "Graham Number (TTM)"),
    ("graham_number",  "TTM", "margin_of_safety_pct",      "MoS % (Graham Number, TTM)"),
    ("graham_revised", "TTM", "intrinsic_value_per_share", "Graham Revised Value (TTM)"),
    ("graham_revised", "TTM", "margin_of_safety_pct",      "MoS % (Graham Revised, TTM)"),
    ("dcf",            "TTM", "intrinsic_value_per_share", "DCF Value per Share (TTM)"),
    ("dcf",            "TTM", "margin_of_safety_pct",      "MoS % (DCF, TTM)"),
    ("owner_earnings", "TTM", "intrinsic_value_per_share", "Owner Earnings Value/Share (TTM)"),
    ("owner_earnings", "TTM", "margin_of_safety_pct",      "MoS % (Owner Earnings, TTM)"),
]

# Detectar el schema real de financials_metrics (puede tener fiscal_year o year)
try:
    metrics_cols = {f.name for f in spark.table(metrics_tbl).schema.fields}
except Exception:
    metrics_cols = set()

year_col = "fiscal_year" if "fiscal_year" in metrics_cols else "year"
print(f"  financials_metrics year column: '{year_col}'")

exposed_frames = []
if len(iv_pdf):
    for method, ptype, field, metric_label in EXPOSED:
        subset = iv_pdf[
            (iv_pdf["method"] == method) & (iv_pdf["period_type"] == ptype)
        ][["ticker", "company", "fiscal_year", field]].copy()

        subset = subset.rename(columns={field: "value", "fiscal_year": year_col})
        subset["metric"] = metric_label
        subset = subset[["ticker", "company", year_col, "metric", "value"]]
        exposed_frames.append(subset)

# Owner Earnings absoluto (FY y TTM) — métrica útil aparte. `oe_dollars` ya está como columna
# (vectorizado, = _owner_earnings_dollars con _safe→0), nunca NaN → toda fila produce su métrica,
# igual que la versión por-fila anterior. El dropna(value) de abajo es no-op aquí (nunca NaN).
oe_frames = []
for _pdf, ptype in ((fy_pdf, "FY"), (ttm_pdf, "TTM")):
    if not len(_pdf):
        continue
    _yr = [int(y) if pd.notna(y) else None for y in _pdf["year"].to_numpy()]
    oe_frames.append(pd.DataFrame({
        "ticker":  _pdf["ticker"].to_numpy(),
        "company": _pdf["company"].to_numpy(),
        year_col:  _yr,
        "metric":  f"Owner Earnings ({ptype})",
        "value":   _pdf["oe_dollars"].to_numpy(dtype=float),
    }))

exposed_frames.extend(oe_frames)

if exposed_frames:
    exposed_pdf = pd.concat(exposed_frames, ignore_index=True).dropna(subset=["value"])

    if metrics_cols:
        exposed_sdf = spark.createDataFrame(exposed_pdf)
        exposed_sdf.createOrReplaceTempView("incoming_iv_metrics")

        spark.sql(f"""
            MERGE INTO {metrics_tbl} AS target
            USING incoming_iv_metrics AS source
            ON  target.ticker = source.ticker
            AND target.{year_col} = source.{year_col}
            AND target.metric = source.metric

            WHEN MATCHED AND target.value != source.value THEN
                UPDATE SET target.value = source.value, target.company = source.company

            WHEN NOT MATCHED THEN
                INSERT (ticker, company, {year_col}, metric, value)
                VALUES (source.ticker, source.company, source.{year_col}, source.metric, source.value)
        """)

        print(f"✓ Exposed {len(exposed_pdf):,} rows in {metrics_tbl}")

        # ── 9b. Orphan cleanup ──────────────────────────────────────────────────────
        # Same rationale as 8b: the exposure MERGE only upserts, so a skipped method's
        # label rows (e.g. "Graham Number (FY)" / "MoS % (Graham Number, FY)") would
        # linger in financials_metrics. Delete IV-label rows absent from the fresh set,
        # for evaluated tickers only. The IN-list confines the delete to the intrinsic
        # labels this notebook owns — it never touches metrics produced by 22.
        _iv_labels = [lbl for *_, lbl in EXPOSED] + ["Owner Earnings (FY)", "Owner Earnings (TTM)"]
        _iv_labels_sql = ", ".join("'" + lbl.replace("'", "''") + "'" for lbl in _iv_labels)
        spark.sql(f"""
            MERGE INTO {metrics_tbl} AS t
            USING (
                SELECT t.ticker, t.{year_col} AS yr, t.metric
                FROM {metrics_tbl} t
                JOIN iv_processed_tickers p ON t.ticker = p.ticker
                WHERE t.metric IN ({_iv_labels_sql})
                  AND NOT EXISTS (
                    SELECT 1 FROM incoming_iv_metrics s
                    WHERE s.ticker = t.ticker AND s.{year_col} = t.{year_col}
                      AND s.metric = t.metric)
            ) AS s
            ON  t.ticker = s.ticker AND t.{year_col} = s.yr AND t.metric = s.metric
            WHEN MATCHED THEN DELETE
        """)
        print(f"✓ Orphan IV-metric cleanup on {metrics_tbl} complete")
    else:
        print(f"⊘ {metrics_tbl} not found — skipping exposure step (run 22__derived_metrics first).")
else:
    print("⊘ No metrics to expose.")

# COMMAND ----------

# MAGIC %md ## 10. Preview

# COMMAND ----------

spark.sql(f"""
    SELECT period_type, fiscal_year, method,
           ROUND(intrinsic_value_per_share, 2) AS iv_per_share,
           ROUND(price_close,               2) AS price,
           ROUND(margin_of_safety_pct,      1) AS mos_pct,
           period_end
    FROM {iv_tbl}
    WHERE ticker = 'AAPL'
    ORDER BY period_type DESC, fiscal_year DESC, method
    LIMIT 40
""").display()
