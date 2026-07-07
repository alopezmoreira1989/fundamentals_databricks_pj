"""Tests for the XIC (S&P/TSX Composite proxy) holdings CSV parser
(fundamentals_pipeline/tickers_universe.py).

The fixture below is a small, hand-built excerpt in the REAL export shape (confirmed live
2026-07): a banner row, an NBSP row, the real header, a couple of real equity holdings, one
cash-collateral row and one futures row (index-mechanics noise that must be filtered out), and
a trailing NBSP footer row.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fundamentals_pipeline.tickers_universe import parse_tsx_composite_csv

_SAMPLE_CSV = (
    'Fund Holdings as of,"Jul 6, 2026"\n'
    "\xa0\n"
    "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Shares,Price,"
    "Location,Exchange,Currency,FX Rate,Market Currency\n"
    '"RY","ROYAL BANK OF CANADA","Financials","Equity","2,530,799,185.95","8.19",'
    '"2,530,799,185.95","8,570,555.00","295.29","Canada","Toronto Stock Exchange",'
    '"CAD","1.00","CAD"\n'
    '"SHOP","SHOPIFY SUBORDINATE VOTING INC CLA","Information Technology","Equity",'
    '"1,276,832,756.70","4.13","1,276,832,756.70","7,479,981.00","170.70","Canada",'
    '"Toronto Stock Exchange","CAD","1.00","CAD"\n'
    '"XYZ","MYSTERY SECTOR CO","Not A Real GICS Sector","Equity","1.00","0.00",'
    '"1.00","1.00","1.00","Canada","Toronto Stock Exchange","CAD","1.00","CAD"\n'
    '"MLPFT","CASH COLLATERAL CAD MLPFT","Cash and/or Derivatives",'
    '"Cash Collateral and Margins","4,246,000.00","0.01","4,246,000.00","4,246,000.00",'
    '"100.00","Canada","-","CAD","1.00","CAD"\n'
    '"PTU6","S&P/TSE 60 INDEX SEP 26","Cash and/or Derivatives","Futures","0.00","0.00",'
    '"78,690,400.00","190.00","2,070.80","-","The Montreal Exchange / Bourse De Montreal",'
    '"CAD","1.00","CAD"\n'
    "\xa0\n"
)


def test_parses_equity_rows_only():
    df = parse_tsx_composite_csv(_SAMPLE_CSV)
    assert set(df["ticker"]) == {"RY", "SHOP", "XYZ"}
    assert "MLPFT" not in set(df["ticker"])
    assert "PTU6" not in set(df["ticker"])


def test_columns_and_values():
    df = parse_tsx_composite_csv(_SAMPLE_CSV)
    assert list(df.columns) == ["ticker", "company", "sector"]
    ry = df[df["ticker"] == "RY"].iloc[0]
    assert ry["company"] == "ROYAL BANK OF CANADA"
    assert ry["sector"] == "Financials"


def test_unmapped_sector_becomes_none():
    df = parse_tsx_composite_csv(_SAMPLE_CSV)
    xyz = df[df["ticker"] == "XYZ"].iloc[0]
    assert xyz["sector"] is None


def test_missing_header_raises():
    with pytest.raises(ValueError, match="header row"):
        parse_tsx_composite_csv("not,a,real,csv\n1,2,3,4\n")


def test_returns_dataframe():
    df = parse_tsx_composite_csv(_SAMPLE_CSV)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
