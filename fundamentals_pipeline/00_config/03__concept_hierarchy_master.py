# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 03__concept_hierarchy_master
# MAGIC
# MAGIC Reads the hierarchy tree from `00_config/concept_hierarchy.json`, flattens it,
# MAGIC and writes the `main.config.concept_hierarchy` table.
# MAGIC
# MAGIC **Two node types in the JSON:**
# MAGIC - **`group`** — visual grouping only, does NOT exist in `financials`. Its name appears as `parent_concept` for its children. Examples: `Assets`, `Current Assets`, `Liabilities`, `Operating Activities`.
# MAGIC - **`concept`** — exists in `financials` (mapped in `01__tickers.py`). Emits one row in the final table. Subtotals such as `Total Assets` or `Total Current Assets` are concepts placed at the end of their group.
# MAGIC
# MAGIC **Result:** a subtotal appears within the group it belongs to, in the last
# MAGIC position — traditional accounting order.
# MAGIC
# MAGIC **Output (Balance Sheet example):**
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
# MAGIC ### ✏️ How to modify the hierarchy
# MAGIC 1. Edit `00_config/concept_hierarchy.json` in the repo
# MAGIC 2. Commit + push
# MAGIC 3. Re-run this notebook (or the full pipeline)

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

HIERARCHY_JSON_PATH = "../00_config/concept_hierarchy.json"
TARGET_TABLE        = f"{CATALOG}.config.concept_hierarchy"

print(f"Source : {HIERARCHY_JSON_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and flatten the JSON

# COMMAND ----------

with open(HIERARCHY_JSON_PATH, "r", encoding="utf-8") as f:
    tree = json.load(f)

# Keys starting with '_' are metadata/comments — discard them
tree = {k: v for k, v in tree.items() if not k.startswith("_")}

print(f"Statements encontrados: {list(tree.keys())}")

# COMMAND ----------

def flatten(node, stmt, parent, group_stack, level, rows, counter):
    """
    Traverses the tree and emits one row per 'concept' node.
    'group' nodes do NOT emit a row — they serve only as visual containers
    and as `parent_concept` for their children.

    `group_stack` is a list tracking the groups traversed to reach the
    current concept. From that stack we derive the columns:
      - `section` = group_stack[0]  (Assets, Liabilities & Equity, Operating Activities, ...)
      - `group`   = group_stack[-1] (the group closest to the concept; Current Assets,
                                     Current Liabilities, Stockholders Equity, ...)
                    or NULL if there is only 1 group in the stack (the concept sits
                    directly under the section, e.g. 'Total Assets')

    - node        : dict (one node) or list (siblings)
    - parent      : name of the direct parent (group or concept), None if root
    - group_stack : list of group names traversed, in order
    - level       : depth (1 = top level)
    - counter     : mutable counter [int] for assigning sort_order globally
                    within each statement (10, 20, 30, ...)
    """
    if isinstance(node, list):
        for child in node:
            flatten(child, stmt, parent, group_stack, level, rows, counter)
        return

    # ── 'group' node: does not emit a row, pushes its name onto the stack and recurses ─
    if "group" in node:
        group_name = node["group"]
        new_stack = group_stack + [group_name]
        for child in node.get("children") or []:
            flatten(child, stmt, group_name, new_stack, level, rows, counter)
        return

    # ── 'concept' node: emits row ────────────────────────────────────────
    concept  = node["concept"]
    display  = node.get("display_name", concept)
    children = node.get("children") or []

    # If it has children (Income Statement case, post-order), recurse first
    if children:
        for child in children:
            flatten(child, stmt, concept, group_stack, level + 1, rows, counter)

    # Derive section and group from the stack
    section = group_stack[0]  if len(group_stack) >= 1 else None
    # group = the group closest to the concept (last in stack), but only if
    # different from section (if only 1 group, group remains NULL)
    group   = group_stack[-1] if len(group_stack) >= 2 else None
    if group == concept:  # guard: subtotal with same name as the group
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
    counter = [0]  # sort_order resets for each statement
    flatten(root, stmt, None, [], 1, rows, counter)

print(f"Total rows: {len(rows)}")
for stmt in tree.keys():
    n = sum(1 for r in rows if r["stmt"] == stmt)
    print(f"  {stmt}: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validation — concepts in the JSON must exist in `01__tickers.py`

# COMMAND ----------

valid_by_stmt = {stmt: set(concepts.keys()) for stmt, concepts in STATEMENTS.items()}

errors = []
warnings = []

for r in rows:
    stmt = r["stmt"]
    if stmt not in valid_by_stmt:
        errors.append(f"  Unknown statement in JSON: '{stmt}'")
        continue
    if r["concept"] not in valid_by_stmt[stmt]:
        errors.append(f"  [{stmt}] concept '{r['concept']}' does not exist in 01__tickers.py")

for stmt, valid in valid_by_stmt.items():
    in_hierarchy = {r["concept"] for r in rows if r["stmt"] == stmt}
    # 'Revenue (contract)' is coalesced to 'Revenue' in 22__derived_metrics —
    # allowed to be absent from the hierarchy.
    missing = (valid - in_hierarchy) - {"Revenue (contract)"}
    if missing:
        warnings.append(f"  [{stmt}] present in 01__tickers.py but NOT in JSON: {sorted(missing)}")

if errors:
    for e in errors: print("✗", e)
    raise ValueError("Validation failed — fix concept_hierarchy.json")

for w in warnings: print("⚠", w)
print("\n✅ Validation OK — all JSON concepts exist in 01__tickers.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta (overwrite — the hierarchy is always the single source)

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

print(f"✓ {sdf.count():,} rows written → {TARGET_TABLE}")

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