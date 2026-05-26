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

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/01__tickers"

# COMMAND ----------

import json
import math
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# ── Paths & table names ──────────────────────────────────────────────────────
ASSUMPTIONS_JSON_PATH = "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/valuation_assumptions.json"

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

    ("Cash Flow",        "Operating Cash Flow",         "ocf"),
    ("Cash Flow",        "CapEx",                       "capex"),
    ("Cash Flow",        "Depreciation & Amortization", "dna"),
    ("Cash Flow",        "Stock-based Compensation",    "sbc"),
    ("Cash Flow",        "Changes in Working Capital",  "delta_wc"),
]

# Conceptos de tipo "flow" (sumables a TTM). Los Balance Sheet son "stock" (snapshot).
STOCK_ALIASES = {"equity", "assets", "lt_debt", "st_debt", "cash", "st_inv"}
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

print(f"✓ FY pandas : {len(fy_pdf):,} rows")
print(f"✓ TTM pandas: {len(ttm_pdf):,} rows")

# COMMAND ----------

# MAGIC %md ## 6. Las cuatro fórmulas

# COMMAND ----------

def _safe(v, default=0):
    return default if v is None or pd.isna(v) else v


def graham_number(row, a):
    eps   = row.get("eps")
    bvps  = row.get("bvps")
    magic = a["graham"]["magic_number"]

    if pd.isna(eps) or pd.isna(bvps) or eps <= 0 or bvps <= 0:
        return None, {"skipped": "eps or bvps non-positive", "magic": magic}

    iv = math.sqrt(magic * eps * bvps)
    return iv, {"magic": magic, "eps": eps, "bvps": bvps}


def graham_revised(row, a):
    cfg = a["graham_revised"]
    eps = row.get("eps")

    if pd.isna(eps) or eps <= 0:
        return None, {"skipped": "eps non-positive"}

    g = min(a["dcf"]["growth_stage1"], cfg["growth_cap"])
    base_pe     = cfg["base_pe"]
    growth_mult = cfg["growth_multiplier"]
    aaa_norm    = cfg["aaa_yield_norm"]
    aaa_current = cfg["graham_aaa_yield"] * 100   # decimal → %

    iv = eps * (base_pe + growth_mult * g * 100) * aaa_norm / aaa_current
    return iv, {
        "eps": eps, "g": g, "base_pe": base_pe,
        "growth_mult": growth_mult, "aaa_norm": aaa_norm,
        "aaa_current_pct": aaa_current,
    }


def _owner_earnings_dollars(row):
    """Buffett 1986: NI + D&A + SBC − CapEx − ΔWC.
    Limitación: usamos CapEx total (no podemos separar maintenance vs growth)."""
    ni  = _safe(row.get("net_income"))
    dna = _safe(row.get("dna"))
    sbc = _safe(row.get("sbc"))
    cx  = _safe(row.get("capex"))
    dwc = _safe(row.get("delta_wc"))
    return ni + dna + sbc - cx - dwc


def dcf_value(row, a):
    cfg = a["dcf"]
    if cfg.get("skip"):
        return None, {"skipped": "ticker opted out (skip=true)"}

    shares = row.get("shares")
    if pd.isna(shares) or shares <= 0:
        return None, {"skipped": "no shares"}

    if cfg.get("use_owner_earnings"):
        starting_cf = _owner_earnings_dollars(row)
        cf_label = "owner_earnings"
    else:
        starting_cf = row.get("fcf")
        cf_label = "fcf"

    if pd.isna(starting_cf) or starting_cf <= 0:
        return None, {"skipped": f"{cf_label} non-positive"}

    wacc    = cfg["wacc"]
    g1      = cfg["growth_stage1"]
    gt      = cfg["growth_terminal"]
    horizon = int(cfg["horizon_years"])

    if wacc <= gt:
        return None, {"skipped": "wacc <= terminal growth (Gordon divergente)"}

    pv_stage1 = 0.0
    cf = starting_cf
    for t in range(1, horizon + 1):
        cf       = cf * (1 + g1)
        pv_stage1 += cf / ((1 + wacc) ** t)

    cf_terminal = cf * (1 + gt)
    tv          = cf_terminal / (wacc - gt)
    pv_terminal = tv / ((1 + wacc) ** horizon)
    enterprise_value = pv_stage1 + pv_terminal

    debt = _safe(row.get("lt_debt")) + _safe(row.get("st_debt"))
    cash = _safe(row.get("cash"))    + _safe(row.get("st_inv"))
    equity_value = enterprise_value - debt + cash

    iv_per_share = equity_value / shares

    return iv_per_share, {
        "cf_basis": cf_label, "starting_cf": starting_cf,
        "wacc": wacc, "g1": g1, "g_terminal": gt, "horizon": horizon,
        "pv_stage1": round(pv_stage1, 0),
        "pv_terminal": round(pv_terminal, 0),
        "debt": debt, "cash": cash,
    }


def owner_earnings_value(row, a):
    cfg = a["owner_earnings"]
    if cfg.get("skip"):
        return None, {"skipped": "ticker opted out"}

    oe = _owner_earnings_dollars(row)
    if oe is None or pd.isna(oe) or oe <= 0:
        return None, {"skipped": "owner_earnings non-positive", "oe": oe}

    shares = row.get("shares")
    if pd.isna(shares) or shares <= 0:
        return None, {"skipped": "no shares"}

    method = cfg.get("method", "multiple")
    if method == "multiple":
        mult  = cfg["multiple"]
        total = oe * mult
        meta  = {"method": "multiple", "oe": oe, "multiple": mult}
    elif method == "perpetuity":
        dr    = cfg["discount_rate"]
        total = oe / dr
        meta  = {"method": "perpetuity", "oe": oe, "discount_rate": dr}
    else:
        return None, {"skipped": f"unknown method '{method}'"}

    return total / shares, {**meta, "oe_per_share": oe / shares}


METHODS = {
    "graham_number":  graham_number,
    "graham_revised": graham_revised,
    "dcf":            dcf_value,
    "owner_earnings": owner_earnings_value,
}

# COMMAND ----------

# MAGIC %md ## 7. Computar — para cada fila (FY o TTM) × cada método

# COMMAND ----------

def compute_all(pdf, period_type, computed_at):
    """Itera el dataframe y produce filas de resultado."""
    rows = []
    for _, row in pdf.iterrows():
        ticker = row["ticker"]
        a      = assumptions_for(ticker)

        for method_name, fn in METHODS.items():
            iv, meta = fn(row, a)
            if iv is None or pd.isna(iv) or iv <= 0:
                continue

            shares      = row.get("shares")
            price       = row.get("price_close")
            total_value = iv * shares if pd.notna(shares) else None
            mos_pct     = ((iv - price) / iv) * 100 if (pd.notna(price) and iv > 0) else None

            # period_end (cuándo termina el período cubierto)
            pend = row.get("period_end")
            if pend is not None and not pd.isna(pend):
                pend = pd.to_datetime(pend).date()
            else:
                pend = None

            rows.append({
                "ticker":                    ticker,
                "company":                   row.get("company"),
                "period_type":               period_type,
                "fiscal_year":               int(row["year"]) if pd.notna(row.get("year")) else None,
                "period_end":                pend,
                "method":                    method_name,
                "intrinsic_value_per_share": float(iv),
                "intrinsic_value_total":     float(total_value) if total_value is not None else None,
                "price_close":               float(price) if pd.notna(price) else None,
                "margin_of_safety_pct":      float(mos_pct) if mos_pct is not None else None,
                "assumptions":               json.dumps(meta, default=str),
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

# Owner Earnings absoluto (FY y TTM) — métrica útil aparte
oe_frames = []
for pdf, ptype in ((fy_pdf, "FY"), (ttm_pdf, "TTM")):
    rows = []
    for _, row in pdf.iterrows():
        oe = _owner_earnings_dollars(row)
        if oe is not None and pd.notna(oe):
            rows.append({
                "ticker":  row["ticker"],
                "company": row.get("company"),
                year_col:  int(row["year"]) if pd.notna(row.get("year")) else None,
                "metric":  f"Owner Earnings ({ptype})",
                "value":   float(oe),
            })
    if rows:
        oe_frames.append(pd.DataFrame(rows))

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
