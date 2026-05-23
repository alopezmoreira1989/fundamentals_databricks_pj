# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 03__concept_hierarchy_master
# MAGIC
# MAGIC Lee el árbol de jerarquía desde `00_config/concept_hierarchy.json`, lo aplana,
# MAGIC y escribe la tabla `main.config.concept_hierarchy`.
# MAGIC
# MAGIC **Dos tipos de nodos en el JSON:**
# MAGIC - **`group`** — solo agrupa visualmente, NO existe en `financials`. Su nombre aparece como `parent_concept` para sus hijos. Ejemplos: `Assets`, `Current Assets`, `Liabilities`, `Operating Activities`.
# MAGIC - **`concept`** — existe en `financials` (mapeado en `01__tickers.py`). Emite una fila en la tabla final. Los subtotales como `Total Assets` o `Total Current Assets` son conceptos colocados al final de su grupo.
# MAGIC
# MAGIC **Resultado:** un subtotal aparece dentro del grupo al que pertenece, en última
# MAGIC posición — orden contable tradicional.
# MAGIC
# MAGIC **Salida (ejemplo Balance Sheet):**
# MAGIC ```
# MAGIC stmt          | concept              | parent_concept   | level | sort_order
# MAGIC Balance Sheet | Cash & Equivalents   | Current Assets   | 1     | 10
# MAGIC Balance Sheet | ST Investments       | Current Assets   | 1     | 20
# MAGIC Balance Sheet | Accounts Receivable  | Current Assets   | 1     | 30
# MAGIC Balance Sheet | Inventory            | Current Assets   | 1     | 40
# MAGIC Balance Sheet | Total Current Assets | Current Assets   | 1     | 50  ← subtotal
# MAGIC Balance Sheet | PP&E Net             | Assets           | 1     | 60
# MAGIC Balance Sheet | Goodwill             | Assets           | 1     | 70
# MAGIC Balance Sheet | Intangibles          | Assets           | 1     | 80
# MAGIC Balance Sheet | Total Assets         | Assets           | 1     | 90  ← total
# MAGIC ```
# MAGIC
# MAGIC ### ✏️ Cómo modificar la jerarquía
# MAGIC 1. Edita `00_config/concept_hierarchy.json` en el repo
# MAGIC 2. Commit + push
# MAGIC 3. Re-ejecuta este notebook (o el pipeline completo)

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

HIERARCHY_JSON_PATH = "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/concept_hierarchy.json"
TARGET_TABLE        = f"{CATALOG}.config.concept_hierarchy"

print(f"Source : {HIERARCHY_JSON_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar y aplanar el JSON

# COMMAND ----------

with open(HIERARCHY_JSON_PATH, "r", encoding="utf-8") as f:
    tree = json.load(f)

# Las claves que empiezan por '_' son metadata/comentarios — descartarlas
tree = {k: v for k, v in tree.items() if not k.startswith("_")}

print(f"Statements encontrados: {list(tree.keys())}")

# COMMAND ----------

def flatten(node, stmt, parent, group_stack, level, rows, counter):
    """
    Recorre el árbol y emite una fila por cada nodo de tipo 'concept'.
    Los nodos de tipo 'group' NO emiten fila — solo sirven como contenedor
    visual y como `parent_concept` para sus hijos.

    `group_stack` es una lista que rastrea los grupos atravesados hasta llegar
    al concept actual. De ese stack derivamos las columnas:
      - `section` = group_stack[0]  (Assets, Liabilities & Equity, Operating Activities, ...)
      - `group`   = group_stack[-1] (el grupo más cercano al concept; Current Assets,
                                     Current Liabilities, Stockholders Equity, ...)
                    o NULL si solo hay 1 grupo en el stack (el concept está directo
                    bajo la section, ej. 'Total Assets')

    - node        : dict (un nodo) o list (hermanos)
    - parent      : nombre del padre directo (grupo o concept), None si es raíz
    - group_stack : lista de nombres de grupos atravesados, en orden
    - level       : profundidad (1 = nivel superior)
    - counter     : contador mutable [int] para asignar sort_order globalmente
                    dentro de cada statement (10, 20, 30, ...)
    """
    if isinstance(node, list):
        for child in node:
            flatten(child, stmt, parent, group_stack, level, rows, counter)
        return

    # ── Nodo 'group': no emite fila, empuja su nombre al stack y recursa ─
    if "group" in node:
        group_name = node["group"]
        new_stack = group_stack + [group_name]
        for child in node.get("children") or []:
            flatten(child, stmt, group_name, new_stack, level, rows, counter)
        return

    # ── Nodo 'concept': emite fila ────────────────────────────────────────
    concept  = node["concept"]
    display  = node.get("display_name", concept)
    children = node.get("children") or []

    # Si tiene hijos (caso del Income Statement, post-orden), recursamos primero
    if children:
        for child in children:
            flatten(child, stmt, concept, group_stack, level + 1, rows, counter)

    # Derivar section y group del stack
    section = group_stack[0]  if len(group_stack) >= 1 else None
    # group = el grupo más cercano al concept (último del stack), pero solo si
    # es distinto de la section (si solo hay 1 grupo, group queda NULL)
    group   = group_stack[-1] if len(group_stack) >= 2 else None
    if group == concept:  # defensa: subtotal con mismo nombre que el grupo
        group = None

    counter[0] += 10
    rows.append({
        "stmt":           stmt,
        "concept":        concept,
        "display_name":   display,
        "parent_concept": parent,
        "section":        section,
        "group":          group,
        "level":          level,
        "sort_order":     counter[0],
    })


rows = []
for stmt, root in tree.items():
    counter = [0]  # sort_order reinicia en cada statement
    flatten(root, stmt, None, [], 1, rows, counter)

print(f"Total filas: {len(rows)}")
for stmt in tree.keys():
    n = sum(1 for r in rows if r["stmt"] == stmt)
    print(f"  {stmt}: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación — concepts del JSON deben existir en `01__tickers.py`

# COMMAND ----------

valid_by_stmt = {stmt: set(concepts.keys()) for stmt, concepts in STATEMENTS.items()}

errors = []
warnings = []

for r in rows:
    stmt = r["stmt"]
    if stmt not in valid_by_stmt:
        errors.append(f"  Statement desconocido en JSON: '{stmt}'")
        continue
    if r["concept"] not in valid_by_stmt[stmt]:
        errors.append(f"  [{stmt}] concept '{r['concept']}' no existe en 01__tickers.py")

for stmt, valid in valid_by_stmt.items():
    in_hierarchy = {r["concept"] for r in rows if r["stmt"] == stmt}
    # 'Revenue (contract)' se coalesce a 'Revenue' en 22__derived_metrics —
    # se permite que esté ausente de la jerarquía.
    missing = (valid - in_hierarchy) - {"Revenue (contract)"}
    if missing:
        warnings.append(f"  [{stmt}] presente en 01__tickers.py pero NO en JSON: {sorted(missing)}")

if errors:
    for e in errors: print("✗", e)
    raise ValueError("Validación fallida — corrige concept_hierarchy.json")

for w in warnings: print("⚠", w)
print("\n✅ Validación OK — todos los concepts del JSON existen en 01__tickers.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escribir a Delta (overwrite — la jerarquía es siempre fuente única)

# COMMAND ----------

schema = StructType([
    StructField("stmt",           StringType(),  False),
    StructField("concept",        StringType(),  False),
    StructField("display_name",   StringType(),  False),
    StructField("parent_concept", StringType(),  True),
    StructField("section",        StringType(),  True),
    StructField("group",          StringType(),  True),
    StructField("level",          IntegerType(), False),
    StructField("sort_order",     IntegerType(), False),
])

sdf = spark.createDataFrame(rows, schema=schema)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ {sdf.count():,} filas escritas → {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview

# COMMAND ----------

spark.sql(f"""
    SELECT
        stmt,
        section,
        `group`,
        display_name AS concept,
        parent_concept,
        level,
        sort_order
    FROM {TARGET_TABLE}
    ORDER BY
        CASE stmt
            WHEN 'Income Statement' THEN 1
            WHEN 'Balance Sheet'    THEN 2
            WHEN 'Cash Flow'        THEN 3
        END,
        sort_order
""").display()