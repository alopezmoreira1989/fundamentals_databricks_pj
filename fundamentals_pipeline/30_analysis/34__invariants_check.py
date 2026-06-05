# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 34__invariants_check
# MAGIC
# MAGIC Post-pipeline **structural-invariant gate** sobre `main.financials.financials`.
# MAGIC Comprueba las invariantes documentadas en CLAUDE.md (y en el skill
# MAGIC `.claude/skills/financials-invariants`). Es el gemelo "pipeline" del validador
# MAGIC read-only `scripts/check_invariants.py` de ese skill: misma lógica de queries,
# MAGIC pero en formato notebook y con `raise` en fallos duros para que el Job lo marque.
# MAGIC
# MAGIC **Comprobaciones:**
# MAGIC - INV1  Balance Sheet único por `(ticker, concept, period_end)`            [duro]
# MAGIC - INV2  Q1+Q2+Q3+Q4 ≈ FY — gate por *tasa* (no por fila)                   [duro si supera el techo]
# MAGIC - INV3  conceptos de dos estados (Net Income / D&A / SBC) presentes 2×      [info]
# MAGIC - INV4  orden TTM por `period_end`                                          [code-level, no observable aquí → SKIP]
# MAGIC - INV5  `market_data.fiscal_year` es año natural (no futuro)               [duro]
# MAGIC
# MAGIC **Behavior:** imprime PASS/FAIL/INFO/SKIP por invariante; `raise RuntimeError`
# MAGIC si alguna dura falla. En `91__full_pipeline` el step va envuelto en try/except
# MAGIC (no-fatal): un fallo se registra como ⚠ y el pipeline continúa.
# MAGIC
# MAGIC > La tasa sana de divergencia Q-sum es ~3–4% (es el suelo irreducible, no un
# MAGIC > bug). Para el desglose por concepto/año usar el skill `validate-quarters`.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

FIN = f"{CATALOG}.{SCHEMA}.financials"
MKT = f"{CATALOG}.{SCHEMA}.market_data"

# Q-sum: tolerancia por fila y techo de tasa. El baseline ~3–4% es esperado; el techo
# solo dispara ante una regresión. Mantener en sync con scripts/check_invariants.py del skill.
QSUM_TOLERANCE = 0.001     # 0.1% por (ticker, stmt, concept, fy)
QSUM_RATE_CEILING = 0.06   # 6% de las tuplas comprobadas

results = []   # (status, label, detail) — status in PASS/FAIL/INFO/SKIP
HARD_FAIL = {"FAIL"}


def _scalar(sql):
    return spark.sql(sql).collect()[0][0]


def _table_exists(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md ## INV1 — Balance Sheet único por (ticker, concept, period_end)

# COMMAND ----------

dup_groups = _scalar(f"""
    SELECT COUNT(*) FROM (
        SELECT ticker, concept, period_end
        FROM {FIN}
        WHERE stmt = 'Balance Sheet'
        GROUP BY ticker, concept, period_end
        HAVING COUNT(*) > 1
    )
""")
if dup_groups == 0:
    results.append(("PASS", "INV1 BS dedup uniqueness",
                    "Balance Sheet único por (ticker, concept, period_end)."))
else:
    results.append(("FAIL", "INV1 BS dedup uniqueness",
                    f"{dup_groups:,} grupos (ticker, concept, period_end) con >1 fila BS. "
                    f"El fix es aguas arriba en 21__clean_and_merge / 21b__derive_quarterly."))

# COMMAND ----------

# MAGIC %md ## INV2 — Q1+Q2+Q3+Q4 ≈ FY (gate por tasa)

# COMMAND ----------

_row = spark.sql(f"""
    WITH q AS (
        SELECT ticker, stmt, concept, fiscal_year,
               SUM(CASE WHEN period_type IN ('Q1','Q2','Q3','Q4') THEN value END) AS qsum,
               MAX(CASE WHEN period_type = 'FY' THEN value END)                    AS fy
        FROM {FIN}
        WHERE stmt IN ('Income Statement', 'Cash Flow')
          AND concept NOT IN ('EPS Basic', 'EPS Diluted', 'Shares Diluted')
        GROUP BY ticker, stmt, concept, fiscal_year
        HAVING COUNT(DISTINCT period_type) = 5
    )
    SELECT COUNT(*) AS checked,
           SUM(CASE WHEN ABS((qsum - fy) / NULLIF(fy, 0)) > {QSUM_TOLERANCE}
                    THEN 1 ELSE 0 END) AS divergent
    FROM q
""").collect()[0]
_checked, _divergent = int(_row["checked"] or 0), int(_row["divergent"] or 0)
if _checked == 0:
    results.append(("SKIP", "INV2 Q-sum rate gate",
                    "Aún no hay tuplas con FY y los 4 trimestres."))
else:
    _rate = _divergent / _checked
    _msg = f"{_divergent:,}/{_checked:,} divergentes a {QSUM_TOLERANCE:.1%} ({_rate:.2%})."
    if _rate > QSUM_RATE_CEILING:
        results.append(("FAIL", "INV2 Q-sum rate gate",
                        f"{_msg} Supera el techo {QSUM_RATE_CEILING:.0%} — usar validate-quarters "
                        f"para el desglose por concepto/año."))
    else:
        results.append(("PASS", "INV2 Q-sum rate gate",
                        f"{_msg} Dentro del techo ({QSUM_RATE_CEILING:.0%})."))

# COMMAND ----------

# MAGIC %md ## INV3 — conceptos de dos estados (info)

# COMMAND ----------

_two = spark.sql(f"""
    SELECT concept, COUNT(DISTINCT stmt) AS n_stmts
    FROM {FIN}
    GROUP BY concept
    HAVING COUNT(DISTINCT stmt) > 1
    ORDER BY concept
""").collect()
if not _two:
    results.append(("INFO", "INV3 two-statement concepts",
                    "0 conceptos en dos estados — se esperaba Net Income (y a menudo D&A, SBC); "
                    "investigar si es 0."))
else:
    _names = ", ".join(r["concept"] for r in _two[:8])
    results.append(("INFO", "INV3 two-statement concepts",
                    f"{len(_two)} concepto(s) en dos estados: {_names}. Todo join por concepto "
                    f"DEBE incluir `stmt` (si no, doble conteo / divergencia ~100% falsa)."))

# COMMAND ----------

# MAGIC %md ## INV4 — orden TTM por period_end (code-level)

# COMMAND ----------

# El orden TTM (4 trimestres más recientes por `period_end`, no por `fiscal_year`) es una
# invariante del CÓDIGO en 23__intrinsic_value.py — no es observable desde las tablas
# publicadas (no se guarda qué trimestres alimentan cada fila TTM). Verificarlo por revisión
# de código, no por datos. Lo dejamos como SKIP explícito para no dar un PASS engañoso.
results.append(("SKIP", "INV4 TTM ordering",
                "Invariante de código (23__intrinsic_value.py ordena por period_end); "
                "no observable desde las tablas — verificar por revisión de código."))

# COMMAND ----------

# MAGIC %md ## INV5 — market_data.fiscal_year es año natural (no futuro)

# COMMAND ----------

if not _table_exists(MKT):
    results.append(("SKIP", "INV5 market_data calendar-year", "market_data no disponible."))
else:
    _max_fy = _scalar(f"SELECT MAX(fiscal_year) FROM {MKT}")
    _cur = _scalar("SELECT YEAR(CURRENT_DATE())")
    if _max_fy is None:
        results.append(("SKIP", "INV5 market_data calendar-year", "market_data sin filas."))
    elif _max_fy > _cur:
        results.append(("FAIL", "INV5 market_data calendar-year",
                        f"max fiscal_year = {_max_fy} > año natural actual {_cur} — "
                        f"mislabel calendario/fiscal o join erróneo."))
    else:
        results.append(("PASS", "INV5 market_data calendar-year",
                        f"max fiscal_year = {_max_fy} (plausible; actual {_cur})."))

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

print("=" * 72)
print("financials-invariants — structural check")
print("=" * 72)
for status, label, detail in results:
    print(f"  {status:4}  {label:32}  {detail}")
print()

_hard = [r for r in results if r[0] in HARD_FAIL]
if _hard:
    _summary = "; ".join(f"{lbl}: {dt}" for _, lbl, dt in _hard)
    raise RuntimeError(f"HARD FAIL — {len(_hard)} invariante(s) dura(s): {_summary}")
print("✓ Todas las invariantes duras se cumplen.")
