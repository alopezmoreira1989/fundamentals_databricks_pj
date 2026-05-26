# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 02__tickers_master
# MAGIC
# MAGIC Builds the unified ticker universe by merging:
# MAGIC - **S&P 500** — 500 large-cap US stocks (Wikipedia)
# MAGIC - **Russell 3000** — broad US market ~3000 stocks (iShares IWV ETF)
# MAGIC - **Favorites** — from `00_config/favorites.json` in the repo root
# MAGIC
# MAGIC **Output table:** `main.config.tickers`
# MAGIC
# MAGIC ```
# MAGIC ticker | company    | in_sp500 | in_r3000 | is_favorite
# MAGIC AAPL   | Apple      | true     | true     | false
# MAGIC TSM    | TSMC       | false    | false    | true
# MAGIC ```
# MAGIC
# MAGIC ### ✏️ Cómo añadir/quitar favoritos
# MAGIC Edita `00_config/favorites.json` en el repositorio Git y vuelve a ejecutar este notebook.
# MAGIC No es necesario tocar ningún notebook de Databricks.

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
import requests
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

INGEST_SP500  = True
INGEST_R3000  = True

# Path al JSON de favoritos — relativo a la raíz del repo en el workspace
FAVORITES_JSON_PATH = "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/favorites.json"

TARGET_TABLE = f"{CATALOG}.config.tickers"

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pull index constituents + favorites

# COMMAND ----------

def fetch_sp500() -> pd.DataFrame:
    url  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    html = requests.get(url, headers=_HEADERS).text
    df   = pd.read_html(html, flavor="lxml")[0][["Symbol", "Security"]].copy()
    df.columns = ["ticker", "company"]
    df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)
    return df.dropna(subset=["ticker"])


def fetch_russell3000() -> pd.DataFrame:
    url = (
        "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf"
        "/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    )
    resp = requests.get(url, headers=_HEADERS)
    df   = pd.read_csv(StringIO(resp.text), skiprows=9, header=0)
    df   = df[["Ticker", "Name"]].dropna(subset=["Ticker"])
    df.columns = ["ticker", "company"]
    df   = df[df["ticker"].str.match(r"^[A-Z\-]+$")]
    return df.reset_index(drop=True)


def fetch_favorites() -> pd.DataFrame:
    """
    Lee los favoritos desde 00_config/favorites.json en el repositorio.
    El JSON es un array de objetos: [{"ticker": "TSM", "company": "...", "note": "..."}]
    Los comentarios con // son ignorados (se limpian antes de parsear).
    """
    try:
        with open(FAVORITES_JSON_PATH, "r", encoding="utf-8") as f:
            raw = f.read()

        # Eliminar líneas de comentario (// ...) para permitir JSON con anotaciones
        lines   = [l for l in raw.splitlines() if not l.strip().startswith("/")]
        cleaned = "\n".join(lines)
        data    = json.loads(cleaned)

        if not data:
            print("  ℹ favorites.json está vacío — sin favoritos")
            return pd.DataFrame(columns=["ticker", "company"])

        df = pd.DataFrame(data)[["ticker", "company"]].copy()
        df["ticker"] = df["ticker"].str.upper().str.strip()
        print(f"  ✓ {len(df)} favorito(s) cargados desde favorites.json")
        return df

    except FileNotFoundError:
        print(f"  ⚠ No se encontró {FAVORITES_JSON_PATH} — sin favoritos")
        return pd.DataFrame(columns=["ticker", "company"])
    except json.JSONDecodeError as e:
        print(f"  ✗ Error al parsear favorites.json: {e}")
        return pd.DataFrame(columns=["ticker", "company"])

# COMMAND ----------

raw_sources: dict[str, pd.DataFrame] = {}

if INGEST_SP500:
    print("Fetching S&P 500...")
    raw_sources["sp500"] = fetch_sp500()
    print(f"  ✓ {len(raw_sources['sp500'])} tickers")

if INGEST_R3000:
    print("Fetching Russell 3000...")
    raw_sources["r3000"] = fetch_russell3000()
    print(f"  ✓ {len(raw_sources['r3000'])} tickers")

print("Cargando favoritos desde favorites.json...")
favorites_df = fetch_favorites()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Merge into unified ticker universe

# COMMAND ----------

master = favorites_df.copy()
master["is_favorite"] = True

for source_name in ["sp500", "r3000"]:
    flag_col = f"in_{source_name}"
    if source_name in raw_sources:
        idx_df = raw_sources[source_name][["ticker", "company"]].copy()
        idx_df[flag_col] = True
        master = master.merge(idx_df, on="ticker", how="outer", suffixes=("", f"_{source_name}"))
        if f"company_{source_name}" in master.columns:
            master["company"] = master["company"].fillna(master[f"company_{source_name}"])
            master.drop(columns=[f"company_{source_name}"], inplace=True)
    else:
        master[flag_col] = False

bool_cols = ["is_favorite", "in_sp500", "in_r3000"]
for col in bool_cols:
    if col not in master.columns:
        master[col] = False
    master[col] = master[col].fillna(False)

master = master[["ticker", "company"] + bool_cols].drop_duplicates("ticker").sort_values("ticker")

print(f"\nUniverso unificado: {len(master):,} tickers únicos")
print(master[bool_cols].sum().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta

# COMMAND ----------

schema = StructType([
    StructField("ticker",      StringType(),  False),
    StructField("company",     StringType(),  True),
    StructField("is_favorite", BooleanType(), False),
    StructField("in_sp500",    BooleanType(), False),
    StructField("in_r3000",    BooleanType(), False),
])

sdf = spark.createDataFrame(master, schema=schema)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ticker")
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ {sdf.count():,} tickers escritos → {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)                                                AS total_tickers,
        SUM(CAST(is_favorite AS INT))                           AS favorites,
        SUM(CAST(in_sp500    AS INT))                           AS in_sp500,
        SUM(CAST(in_r3000    AS INT))                           AS in_r3000,
        SUM(CASE WHEN in_sp500 AND in_r3000 THEN 1 ELSE 0 END) AS in_both,
        SUM(CASE WHEN is_favorite AND NOT in_sp500
                  AND NOT in_r3000 THEN 1 ELSE 0 END)           AS favorites_only
    FROM {TARGET_TABLE}
""").show()

fav_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_favorite").collect()[0][0]
if fav_count > 0:
    print("\nFavoritos activos:")
    spark.sql(f"""
        SELECT ticker, company, in_sp500, in_r3000
        FROM {TARGET_TABLE} WHERE is_favorite ORDER BY ticker
    """).show()
else:
    print("\nℹ Sin favoritos — edita 00_config/favorites.json para añadir")