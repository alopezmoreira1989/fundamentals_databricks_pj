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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

INGEST_SP500  = True
INGEST_R3000  = True

# FAVORITES_JSON_PATH heredado de 01__tickers vía %run

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
    """
    Fetch IWV (iShares Russell 3000 ETF) holdings via the BlackRock fundDownload API.
    Returns a DataFrame with columns [ticker, company].

    The response is XML Excel (SpreadsheetML). The header row is detected
    dynamically by scanning for a row containing a recognised ticker column
    name, so the parser is resilient to metadata rows being added or removed.
    """
    import re

    url = (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/"
        "product-data/api/v1/get-fund-document"
        "?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares"
        "&locale=en_US&portfolioId=239714&component=fundDownload&userType=individual"
    )
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    resp.raise_for_status()

    if "<ss:Worksheet" not in resp.text[:2000]:
        raise ValueError(
            f"IWV response is not SpreadsheetML (first 200 chars: {resp.text[:200]})"
        )

    # Locate the Holdings worksheet
    ws_start = resp.text.find('ss:Name="Holdings"')
    if ws_start == -1:
        raise ValueError("Holdings worksheet not found in IWV SpreadsheetML response")
    ws_end = resp.text.find("</ss:Worksheet>", ws_start)
    ws_xml = resp.text[ws_start:ws_end]

    rows = re.findall(r"<ss:Row[^>]*>(.*?)</ss:Row>", ws_xml, re.DOTALL)

    # Dynamic header detection: find row containing a recognized ticker column
    TICKER_COLS  = {"Ticker", "Symbol", "Holding Ticker"}
    COMPANY_COLS = {"Name", "Issuer Name", "Security Name", "Description"}

    header_idx = None
    header_cells = []
    for i, row in enumerate(rows):
        cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
        if TICKER_COLS & set(cells):
            header_idx = i
            header_cells = cells
            break

    if header_idx is None:
        all_row_samples = []
        for i, row in enumerate(rows[:15]):
            cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
            all_row_samples.append(f"  Row {i}: {cells}")
        raise ValueError(
            f"Could not find header row with any of {TICKER_COLS}. "
            f"First 15 rows:\n" + "\n".join(all_row_samples)
        )

    # Resolve column indices with flexible mapping
    ticker_col = next((j for j, c in enumerate(header_cells) if c in TICKER_COLS), None)
    company_col = next((j for j, c in enumerate(header_cells) if c in COMPANY_COLS), None)

    if ticker_col is None:
        raise ValueError(f"Ticker column not found. Header: {header_cells}")
    if company_col is None:
        raise ValueError(f"Company column not found. Header: {header_cells}")

    used_ticker_name = header_cells[ticker_col]
    used_company_name = header_cells[company_col]

    # Parse data rows
    records = []
    for row in rows[header_idx + 1:]:
        cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
        if len(cells) <= max(ticker_col, company_col):
            continue
        t = cells[ticker_col].strip()
        c = cells[company_col].strip()
        if t and re.match(r"^[A-Z][A-Z0-9.\-]{0,6}$", t):
            records.append({"ticker": t, "company": c})

    df = pd.DataFrame(records)

    # Validation
    if len(df) < 1500:
        raise ValueError(
            f"IWV holdings too few: {len(df)} (expected ~2500+). "
            f"Possible format change or partial response."
        )

    nan_pct = df["ticker"].isna().sum() / len(df) if len(df) > 0 else 0
    if nan_pct > 0.05:
        raise ValueError(f"IWV holdings: {nan_pct:.1%} of tickers are NaN (>5% threshold)")

    print(f"  ✓ Parsed {len(df)} holdings from IWV fundDownload API")
    print(f"    Header at row {header_idx}, columns: {used_ticker_name}/{used_company_name}")
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
    try:
        raw_sources["r3000"] = fetch_russell3000()
        print(f"  ✓ {len(raw_sources['r3000'])} tickers")
    except Exception as _r3k_err:
        print(f"  ✗ Russell 3000 fetch failed: {_r3k_err}")
        print(f"  ⚠ Continuing with S&P 500 + favorites only — R3000 will be missing")

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