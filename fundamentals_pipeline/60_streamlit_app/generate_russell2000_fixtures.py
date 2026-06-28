"""
Expand Streamlit fixtures by adding ~2000 small-cap tickers (synthetic data).

Reads the existing S&P 500 fixtures, fetches additional US-listed tickers from
SEC EDGAR, generates AAPL-scaled synthetic data for ~2000 new tickers, and
writes merged fixtures.
"""

import json
import random
import zlib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

FIXTURE_DIR = Path(__file__).parent / "fixtures"
DATA_FILE = "dashboard_data.parquet"
METRIC_FILE = "dashboard_metrics.parquet"
META_FILE = "dashboard_meta.json"

TARGET_NEW_TICKERS = 2000

# Synthetic universe flags (schema v6). The base fixtures are the S&P 500; the
# added tickers are a Russell 2000 proxy → all are in the (broader) Russell 3000.
FAVORITES = {"AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "JPM", "V", "JNJ", "WMT"}

# The 11 canonical GICS sectors. Synthetic fixtures have no real sector, so each
# ticker gets a deterministic (CRC32-of-ticker) placeholder — stable across runs —
# purely so the screener's sector filter is demonstrable in the preview deployment.
GICS_SECTORS = [
    "Communication Services", "Consumer Discretionary", "Consumer Staples",
    "Energy", "Financials", "Health Care", "Industrials",
    "Information Technology", "Materials", "Real Estate", "Utilities",
]

random.seed(42)
np.random.seed(42)


def _assign_sector(ticker: str) -> str:
    """Deterministic placeholder GICS sector for a synthetic preview ticker."""
    return GICS_SECTORS[zlib.crc32(ticker.encode("utf-8")) % len(GICS_SECTORS)]


# Placeholder sub-sector labels — synthetic fixtures carry no real Yahoo industry, so each
# ticker gets a deterministic placeholder purely so the industry grouping is demonstrable in
# the preview deployment (real industries arrive in the published artifacts at schema v8).
_PLACEHOLDER_INDUSTRIES = [
    "Software—Application", "Semiconductors", "Banks—Diversified", "Oil & Gas E&P",
    "Drug Manufacturers", "Specialty Retail", "Aerospace & Defense", "Utilities—Regulated",
    "REIT—Industrial", "Packaged Foods", "Telecom Services", "Auto Manufacturers",
]


def _assign_industry(ticker: str) -> str:
    """Deterministic placeholder sub-sector for a synthetic preview ticker."""
    return _PLACEHOLDER_INDUSTRIES[zlib.crc32(("ind:" + ticker).encode("utf-8")) % len(_PLACEHOLDER_INDUSTRIES)]


def build_market_cap_rows(data: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    """Synthetic `Market Cap` metric rows = P/S × Revenue per (ticker, FY).

    Mirrors the real export (50_publish/51): category/subcategory NULL so the
    detail page ignores them, unit 'usd', period_type 'FY'.
    """
    rev = data[
        (data["stmt"] == "Income Statement")
        & (data["concept"] == "Revenue")
        & (data["period_type"] == "FY")
    ][["ticker", "fiscal_year", "period_end", "value"]].rename(columns={"value": "rev"})
    ps = metrics[(metrics["metric"] == "P/S") & (metrics["period_type"] == "FY")][
        ["ticker", "fiscal_year", "value"]
    ].rename(columns={"value": "ps"})

    mc = rev.merge(ps, on=["ticker", "fiscal_year"], how="inner")
    if mc.empty:
        return pd.DataFrame(columns=metrics.columns)
    mc["value"] = (mc["rev"] * mc["ps"]).round(1)
    mc["period_type"] = "FY"
    mc["category"] = pd.NA
    mc["subcategory"] = pd.NA
    mc["metric"] = "Market Cap"
    mc["unit"] = "usd"
    mc["sort_order"] = pd.NA
    return mc[metrics.columns]


def fetch_smallcap_tickers(existing: set[str], n: int = TARGET_NEW_TICKERS) -> pd.DataFrame:
    """Fetch ~n non-S&P-500 US tickers from SEC EDGAR as Russell 2000 proxy."""
    headers = {"User-Agent": "fundamentals-dashboard al.lopez.moreira@gmail.com"}
    resp = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    seen = set()
    tickers = []
    for v in data.values():
        t = v["ticker"]
        if t in existing or t in seen:
            continue
        if not (1 <= len(t) <= 5 and t.isalpha() and t == t.upper()):
            continue
        seen.add(t)
        tickers.append({"ticker": t, "company": v["title"].title()})

    tickers.sort(key=lambda x: x["ticker"])
    return pd.DataFrame(tickers[:n])


def generate_synthetic_data(
    template_df: pd.DataFrame,
    new_tickers: list[dict],
    scale_range: tuple[float, float] = (0.005, 0.10),
) -> pd.DataFrame:
    """Generate synthetic financials by scaling AAPL template data."""
    frames = []
    for info in new_tickers:
        ticker = info["ticker"]
        scale = random.uniform(*scale_range)
        chunk = template_df.copy()
        chunk["ticker"] = ticker
        chunk["value"] = chunk["value"] * scale
        noise = np.random.normal(1.0, 0.03, size=len(chunk))
        chunk["value"] = chunk["value"] * noise
        chunk["value"] = chunk["value"].round(1)
        frames.append(chunk)
    if not frames:
        return pd.DataFrame(columns=template_df.columns)
    return pd.concat(frames, ignore_index=True)


def generate_synthetic_metrics(
    template_df: pd.DataFrame,
    new_tickers: list[dict],
    scale_range: tuple[float, float] = (0.005, 0.10),
) -> pd.DataFrame:
    """Generate synthetic metrics by scaling/perturbing AAPL template."""
    pct_metrics = template_df[template_df["unit"] == "percent"]
    abs_metrics = template_df[template_df["unit"] != "percent"]

    frames = []
    for info in new_tickers:
        ticker = info["ticker"]
        scale = random.uniform(*scale_range)

        # Percentage metrics: perturb slightly
        pct_chunk = pct_metrics.copy()
        pct_chunk["ticker"] = ticker
        noise = np.random.normal(1.0, 0.10, size=len(pct_chunk))
        pct_chunk["value"] = (pct_chunk["value"] * noise).round(2)

        # Absolute metrics: scale proportionally
        abs_chunk = abs_metrics.copy()
        abs_chunk["ticker"] = ticker
        abs_chunk["value"] = abs_chunk["value"] * scale
        noise = np.random.normal(1.0, 0.03, size=len(abs_chunk))
        abs_chunk["value"] = (abs_chunk["value"] * noise).round(2)

        frames.append(pd.concat([pct_chunk, abs_chunk], ignore_index=True))

    if not frames:
        return pd.DataFrame(columns=template_df.columns)
    return pd.concat(frames, ignore_index=True)


def main():
    print("=== Russell 2000 Fixture Expansion ===\n")

    # 1. Load existing fixtures
    print("1. Loading existing fixtures...")
    existing_data = pd.read_parquet(FIXTURE_DIR / DATA_FILE)
    existing_metrics = pd.read_parquet(FIXTURE_DIR / METRIC_FILE)
    with open(FIXTURE_DIR / META_FILE) as f:
        existing_meta = json.load(f)

    existing_tickers = set(existing_data["ticker"].unique())
    print(f"   Existing: {len(existing_tickers)} tickers")

    # 2. Fetch small-cap tickers from SEC EDGAR (Russell 2000 proxy)
    print("\n2. Fetching small-cap tickers from SEC EDGAR...")
    r2000 = fetch_smallcap_tickers(existing_tickers)
    print(f"   New tickers to add: {len(r2000)}")

    new_ticker_list = r2000.to_dict("records")

    # 4. Extract AAPL template
    print("\n3. Generating synthetic data...")
    aapl_data = existing_data[existing_data["ticker"] == "AAPL"].copy()
    aapl_metrics = existing_metrics[existing_metrics["ticker"] == "AAPL"].copy()

    # 5. Generate synthetic data (small-cap scale: 0.5% to 10% of AAPL)
    new_data = generate_synthetic_data(aapl_data, new_ticker_list)
    new_metrics = generate_synthetic_metrics(aapl_metrics, new_ticker_list)
    print(f"   Generated {len(new_data):,} data rows, {len(new_metrics):,} metric rows")

    # 6. Merge
    print("\n4. Merging...")
    merged_data = pd.concat([existing_data, new_data], ignore_index=True)
    merged_metrics = pd.concat([existing_metrics, new_metrics], ignore_index=True)

    # 6b. Inject synthetic Market Cap rows (schema v3).
    market_cap = build_market_cap_rows(merged_data, merged_metrics)
    merged_metrics = pd.concat([merged_metrics, market_cap], ignore_index=True)
    print(f"   + Market Cap rows: {len(market_cap):,}")

    # 7. Build meta — with universe flags + placeholder GICS sector + placeholder industry.
    def _flags(t: str) -> dict:
        return {
            "is_favorite": t in FAVORITES,
            "in_sp500":    t in existing_tickers,   # base fixtures = S&P 500
            "in_r3000":    True,                     # proxy: all in Russell 3000
            "sector":      _assign_sector(t),        # deterministic placeholder
            "industry":    _assign_industry(t),      # deterministic placeholder
        }

    base_info = [{"ticker": e["ticker"], "company": e["company"]} for e in existing_meta["tickers"]]
    new_info = [{"ticker": r["ticker"], "company": r["company"]} for r in new_ticker_list]
    all_tickers_info = [{**info, **_flags(info["ticker"])} for info in base_info + new_info]
    all_tickers_info.sort(key=lambda x: x["ticker"])

    all_ticker_syms = sorted(merged_data["ticker"].unique())
    fy_ranges = []
    for t in all_ticker_syms:
        t_fy = merged_data[
            (merged_data["ticker"] == t) & (merged_data["period_type"] == "FY")
        ]
        if len(t_fy) > 0:
            fy_ranges.append(
                {
                    "ticker": t,
                    "fy_min": int(t_fy["fiscal_year"].min()),
                    "fy_max": int(t_fy["fiscal_year"].max()),
                }
            )

    meta = {
        "schema_version": 7,
        "build_timestamp": datetime.now(timezone.utc).isoformat(),
        "tickers": all_tickers_info,
        "fy_ranges": fy_ranges,
        "row_counts": {
            "financials": len(merged_data),
            "metrics": len(merged_metrics),
        },
        "retention": {"fy_years": 10, "quarterly_periods": 12},
    }

    # 8. Write
    print("\n5. Writing fixtures...")
    merged_data.to_parquet(FIXTURE_DIR / DATA_FILE, index=False, engine="pyarrow")
    merged_metrics.to_parquet(FIXTURE_DIR / METRIC_FILE, index=False, engine="pyarrow")
    with open(FIXTURE_DIR / META_FILE, "w") as f:
        json.dump(meta, f, indent=2)

    data_mb = (FIXTURE_DIR / DATA_FILE).stat().st_size / 1024 / 1024
    metric_mb = (FIXTURE_DIR / METRIC_FILE).stat().st_size / 1024 / 1024
    meta_kb = (FIXTURE_DIR / META_FILE).stat().st_size / 1024

    print(f"   {DATA_FILE}: {data_mb:.1f} MB")
    print(f"   {METRIC_FILE}: {metric_mb:.1f} MB")
    print(f"   {META_FILE}: {meta_kb:.0f} KB")
    print(f"\n=== Done: {len(all_ticker_syms)} total tickers ===")


if __name__ == "__main__":
    main()
