"""Unit tests for the pure Streamlit-app helpers in 60__frontends/61__streamlit/lib/.

``format`` and ``signals`` depend only on pandas, so they import unconditionally.
``screener`` transitively imports ``streamlit`` (via ``lib.data``); we ``importorskip``
it so the suite still runs in an environment without streamlit installed.
"""

from __future__ import annotations

import pandas as pd
import pytest
from lib import format as fmt
from lib import signals


# ── format ────────────────────────────────────────────────────────────────────
def test_is_missing():
    assert fmt.is_missing(None)
    assert fmt.is_missing(float("nan"))
    assert not fmt.is_missing(0)
    assert not fmt.is_missing(1.5)


def test_fmt_num_accounting_negatives_and_separators():
    assert fmt.fmt_num(1500) == "1,500"
    assert fmt.fmt_num(-1500) == "(1,500)"
    assert fmt.fmt_num(None) == fmt.EM_DASH


def test_fmt_num_scaled_units_path_identical():
    assert fmt.fmt_num_scaled(1500, divisor=1) == "1,500"
    assert fmt.fmt_num_scaled(2_000_000_000, divisor=1_000_000_000) == "2.0"


def test_fmt_metric_by_unit():
    assert fmt.fmt_metric(12.345, "percent") == "12.3%"
    assert fmt.fmt_metric(2.5, "ratio") == "2.50x"
    assert fmt.fmt_metric(None, "percent") == fmt.EM_DASH


def test_fmt_metric_signed_percent():
    assert fmt.fmt_metric(5.0, "percent", signed=True) == "+5.0%"


def test_fmt_kpi_scale_aware():
    assert fmt.fmt_kpi(391_000_000_000) == "$391.0B"
    assert fmt.fmt_kpi(93_700_000) == "$93.7M"
    assert fmt.fmt_kpi(-1_000_000_000) == "-$1.0B"


def test_fmt_cagr_basic_and_sign_change():
    label, cls = fmt.fmt_cagr(100, 121, 2)
    assert label == "+10.0%"
    assert cls == "up"
    # sign change -> n/a
    assert fmt.fmt_cagr(-10, 10, 3)[0] == "n/a"


def test_short_year_and_quarter():
    assert fmt.short_year(2024) == "'24"
    assert fmt.short_quarter("Q4", 2024) == "'24-Q4"


def test_trend_growth_too_few_points():
    assert fmt.trend_growth([1.0, 2.0]) == (None, None)


# ── signals ───────────────────────────────────────────────────────────────────
def test_signal_absolute_lower_is_better():
    assert signals.signal_absolute("P/E", 12) == "good"
    assert signals.signal_absolute("P/E", 20) == "warn"
    assert signals.signal_absolute("P/E", 30) == "bad"


def test_signal_absolute_higher_is_better():
    assert signals.signal_absolute("ROE %", 20) == "good"
    assert signals.signal_absolute("ROE %", 10) == "warn"
    assert signals.signal_absolute("ROE %", 5) == "bad"


def test_signal_absolute_tolerates_fy_ttm_suffix():
    assert signals.signal_absolute("P/E (FY)", 12) == "good"


def test_signal_absolute_mos_band():
    assert signals.signal_absolute("MoS % (DCF, FY)", 40) == "good"
    assert signals.signal_absolute("MoS % (DCF, FY)", 10) == "warn"
    assert signals.signal_absolute("MoS % (DCF, FY)", -5) == "bad"


def test_signal_absolute_missing_and_unknown():
    assert signals.signal_absolute("P/E", None) is None
    assert signals.signal_absolute("Totally Unknown Metric", 1) is None


# ── screener masks (streamlit-gated) ──────────────────────────────────────────
screener = pytest.importorskip("lib.screener")


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "XOM"],
            "company": ["Apple Inc", "Microsoft Corp", "Exxon Mobil"],
            "sector": ["Information Technology", "Information Technology", "Energy"],
            "is_favorite": [True, False, False],
            "in_sp500": [True, True, True],
            "in_r3000": [True, True, True],
            "P/E": [28.0, 12.0, float("nan")],
        }
    )


def test_universe_mask_all_is_noop(frame):
    assert screener.universe_mask(frame, "All").all()


def test_universe_mask_favorites(frame):
    mask = screener.universe_mask(frame, "Favorites")
    assert list(mask) == [True, False, False]


def test_sector_mask_default_and_specific(frame):
    assert screener.sector_mask(frame, "All sectors").all()
    assert list(screener.sector_mask(frame, "Energy")) == [False, False, True]


def test_search_mask(frame):
    # matches ticker prefix
    assert list(screener.search_mask(frame, "aap")) == [True, False, False]
    # matches company substring
    assert list(screener.search_mask(frame, "micro")) == [False, True, False]
    # empty query -> no filter
    assert screener.search_mask(frame, "").all()


def test_buckets_for_resolution():
    assert screener.buckets_for("P/E", None) is screener._VALUATION_MULTIPLE_BAND
    assert screener.buckets_for("P/E (FY)", None) is screener._VALUATION_MULTIPLE_BAND
    assert screener.buckets_for("MoS % (DCF, FY)", None) is screener._MOS_BAND
    # unit fallback
    assert screener.buckets_for("Some Ratio Metric", "ratio") is screener._UNIT_BUCKETS["ratio"]
    # nothing matches -> empty
    assert screener.buckets_for("Unknown", None) == []


def test_bucket_mask_or_and_nan_excluded(frame):
    buckets = screener.buckets_for("P/E", None)
    # 28 -> Rich, 12 -> Fair, NaN -> excluded. None falls in "Cheap" [0, 10).
    mask_cheap = screener.bucket_mask(frame["P/E"], ["Cheap"], buckets)
    assert list(mask_cheap) == [False, False, False]
    # 12 falls in the "Fair" band [10, 15); NaN stays excluded.
    mask_fair = screener.bucket_mask(frame["P/E"], ["Fair"], buckets)
    assert list(mask_fair) == [False, True, False]


def test_bucket_mask_empty_selection_is_noop(frame):
    buckets = screener.buckets_for("P/E", None)
    assert screener.bucket_mask(frame["P/E"], [], buckets).all()


# ── industry plumbing in build_screener_frame (schema v8) ─────────────────────
def _fake_load(meta: dict):
    """Stand-in for lib.data.load_latest_data → (data, metrics, meta)."""
    metrics = pd.DataFrame(
        {
            "ticker": ["AAPL", "XOM"],
            "metric": ["P/E", "P/E"],
            "period_type": ["FY", "FY"],
            "fiscal_year": [2024, 2024],
            "unit": ["ratio", "ratio"],
            "sort_order": [1.0, 1.0],
            "value": [28.0, 11.0],
        }
    )
    return pd.DataFrame(), metrics, meta


def test_build_screener_frame_industry_roundtrips_and_defaults(monkeypatch):
    # AAPL carries industry; XOM's record omits it → NaN → "Unknown" bucket.
    meta = {"tickers": [
        {"ticker": "AAPL", "company": "Apple Inc", "sector": "Information Technology",
         "industry": "Consumer Electronics"},
        {"ticker": "XOM", "company": "Exxon Mobil", "sector": "Energy"},
    ]}
    monkeypatch.setattr(screener, "load_latest_data", lambda: _fake_load(meta))
    screener.build_screener_frame.clear()
    wide, _unit_map, _order = screener.build_screener_frame()
    by_ticker = wide.set_index("ticker")["industry"]
    assert by_ticker["AAPL"] == "Consumer Electronics"
    assert by_ticker["XOM"] == screener.UNKNOWN_INDUSTRY


def test_build_screener_frame_industry_column_absent_defaults(monkeypatch):
    # Pre-v8 artifact: no record has an industry key → column created, all "Unknown".
    meta = {"tickers": [
        {"ticker": "AAPL", "company": "Apple Inc", "sector": "Information Technology"},
        {"ticker": "XOM", "company": "Exxon Mobil", "sector": "Energy"},
    ]}
    monkeypatch.setattr(screener, "load_latest_data", lambda: _fake_load(meta))
    screener.build_screener_frame.clear()
    wide, _unit_map, _order = screener.build_screener_frame()
    assert "industry" in wide.columns
    assert (wide["industry"] == screener.UNKNOWN_INDUSTRY).all()


# ── industry filter + comparison aggregation (Phase 2) ─────────────────────────
def test_industry_mask_default_and_specific():
    df = pd.DataFrame(
        {"ticker": ["A", "B", "C"], "industry": ["Semis", "Banks", "Semis"]}
    )
    assert screener.industry_mask(df, screener.ALL_INDUSTRIES).all()
    assert list(screener.industry_mask(df, "Semis")) == [True, False, True]
    # missing column → no-op
    assert screener.industry_mask(pd.DataFrame({"ticker": ["A"]}), "Semis").all()


def test_industry_options_excludes_unknown_and_sorts():
    df = pd.DataFrame({"industry": ["Semis", "Banks", screener.UNKNOWN_INDUSTRY, "Semis", None]})
    assert screener.industry_options(df) == [screener.ALL_INDUSTRIES, "Banks", "Semis"]
    # no column → just the no-op default
    assert screener.industry_options(pd.DataFrame({"ticker": ["A"]})) == [screener.ALL_INDUSTRIES]


def test_industry_summary_medians_filters_and_info():
    wide = pd.DataFrame({
        "ticker": ["A", "B", "C", "D", "E", "F"],
        "industry": ["Semis", "Semis", "Semis", "Banks", "Banks", screener.UNKNOWN_INDUSTRY],
        "sector": ["Information Technology", "Information Technology", "Energy",
                   "Financials", "Financials", "Energy"],
        "P/E": [10.0, 20.0, -5.0, 8.0, 12.0, 99.0],   # Semis ex-neg median of [10, 20] = 15
        "ROE %": [30.0, 10.0, 20.0, 5.0, 15.0, 1.0],  # Semis plain median [30, 10, 20] = 20
    })
    summary, info = screener.industry_summary(wide, min_count=3)
    # Banks (n=2) hidden; Unknown excluded → only Semis qualifies.
    assert list(summary["industry"]) == ["Semis"]
    row = summary.set_index("industry").loc["Semis"]
    assert row["n"] == 3
    assert row["sector"] == "Information Technology"   # modal GICS sector in the group
    assert row["P/E"] == 15.0                          # ex-negatives median
    assert row["ROE %"] == 20.0
    assert info == {"hidden_small": 1, "min_count": 3, "unknown": 1, "n_industries": 1}


def test_industry_summary_no_industry_column():
    summary, info = screener.industry_summary(pd.DataFrame({"ticker": ["A"], "P/E": [10.0]}))
    assert summary.empty
    assert info["n_industries"] == 0 and info["unknown"] == 0
