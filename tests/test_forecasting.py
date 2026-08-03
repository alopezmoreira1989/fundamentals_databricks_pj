"""Unit tests for the Forecasting training-panel-assembly helpers (fundamentals_pipeline/
forecasting.py)."""

from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd
import pytest

from fundamentals_pipeline import forecasting as fc


# ── log_growth ────────────────────────────────────────────────────────────────────
def test_log_growth_positive_to_positive():
    assert fc.log_growth(100.0, 110.0) == math.log(1.1)


def test_log_growth_none_when_either_missing():
    assert fc.log_growth(None, 110.0) is None
    assert fc.log_growth(100.0, None) is None


def test_log_growth_none_when_start_non_positive():
    assert fc.log_growth(0.0, 110.0) is None
    assert fc.log_growth(-50.0, 110.0) is None


def test_log_growth_none_when_end_non_positive():
    # The hurdle-model rationale: a positive-to-negative transition has no defined log_growth
    # (structurally can't represent a loss via a multiplicative ratio) — is_loss carries this.
    assert fc.log_growth(100.0, -20.0) is None
    assert fc.log_growth(100.0, 0.0) is None


# ── is_loss ───────────────────────────────────────────────────────────────────────
def test_is_loss_negative_and_zero():
    assert fc.is_loss(-20.0) == 1
    assert fc.is_loss(0.0) == 1


def test_is_loss_positive():
    assert fc.is_loss(20.0) == 0


def test_is_loss_none_when_missing():
    assert fc.is_loss(None) is None


def test_is_loss_independent_of_starting_sign():
    # A company recovering from a loss into a profit: log_growth is undefined (can't ratio a
    # negative start), but is_loss on the endpoint is well-defined and says "not a loss."
    assert fc.log_growth(-30.0, 50.0) is None
    assert fc.is_loss(50.0) == 0
    # And the reverse: profit collapsing into a loss.
    assert fc.log_growth(50.0, -30.0) is None
    assert fc.is_loss(-30.0) == 1


# ── reinvestment_rate ─────────────────────────────────────────────────────────────
def test_reinvestment_rate_basic():
    assert fc.reinvestment_rate(capex=40.0, d_and_a=10.0, net_income=100.0) == 0.3


def test_reinvestment_rate_none_when_missing_or_zero_denominator():
    assert fc.reinvestment_rate(None, 10.0, 100.0) is None
    assert fc.reinvestment_rate(40.0, 10.0, 0.0) is None


# ── fcf_conversion ────────────────────────────────────────────────────────────────
def test_fcf_conversion_basic():
    assert fc.fcf_conversion(free_cash_flow=80.0, net_income=100.0) == 0.8


def test_fcf_conversion_none_when_missing_or_zero_denominator():
    assert fc.fcf_conversion(None, 100.0) is None
    assert fc.fcf_conversion(80.0, 0.0) is None


# ── size_decile (per-fiscal-year, never across years) ──────────────────────────────
def test_size_decile_ranks_within_year_only():
    # Year 2020: 10 tickers, evenly spaced market caps -> deciles 1..10.
    # Year 2021: only 3 tickers -> too few for deciles, all NaN.
    market_caps = pd.Series([float(i) for i in range(1, 11)] + [1.0, 2.0, 3.0])
    fiscal_years = pd.Series([2020] * 10 + [2021] * 3)
    result = fc.size_decile(market_caps, fiscal_years)
    assert sorted(result[:10].tolist()) == list(range(1, 11))
    assert result[10:].isna().all()


def test_size_decile_length_mismatch_raises():
    try:
        fc.size_decile(pd.Series([1.0, 2.0]), pd.Series([2020]))
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


# ── growth_trend ──────────────────────────────────────────────────────────────────
def test_growth_trend_uses_available_window():
    # 4 consecutive years -> 3 YoY transitions, exactly at min_years=3.
    values = {2020: 100.0, 2021: 110.0, 2022: 121.0, 2023: 133.1}
    trend = fc.growth_trend(values)
    expected = sum(math.log(1.1) for _ in range(3)) / 3
    assert trend == pytest.approx(expected)


def test_growth_trend_none_when_insufficient_history():
    values = {2022: 100.0, 2023: 110.0}  # only 1 transition, need >= 3
    assert fc.growth_trend(values) is None


def test_growth_trend_gap_year_breaks_transition():
    # 2021 is missing entirely -> the 2020->2022 "transition" is skipped, not bridged.
    values = {2019: 100.0, 2020: 110.0, 2022: 130.0, 2023: 140.0}
    trend = fc.growth_trend(values, min_years=1)
    # Only 2019->2020 and 2022->2023 are valid consecutive transitions.
    expected = (math.log(110.0 / 100.0) + math.log(140.0 / 130.0)) / 2
    assert trend == pytest.approx(expected)


# ── expanding_window_split (no-look-ahead) ──────────────────────────────────────────
def test_expanding_window_split_excludes_not_yet_filed():
    panel = pd.DataFrame({
        "ticker": ["A", "B", "C"],
        "as_of": [date(2023, 3, 1), date(2023, 6, 1), None],
    })
    # A fiscal-year-based cutoff would treat "filed mid-year" rows as all-or-nothing; here B's
    # as_of (2023-06-01) is after the cutoff even though it's the "same year" as A's.
    train, test = fc.expanding_window_split(panel, "as_of", date(2023, 4, 1))
    assert train["ticker"].tolist() == ["A"]
    assert set(test["ticker"]) == {"B", "C"}  # None as_of is never eligible


# ── build_training_panel (end-to-end shape check) ───────────────────────────────────
def _tiny_inputs():
    financials = pd.DataFrame([
        {"ticker": "AAA", "fiscal_year": 2021, "concept": "Revenue", "value": 100.0},
        {"ticker": "AAA", "fiscal_year": 2021, "concept": "Net Income", "value": 10.0},
        {"ticker": "AAA", "fiscal_year": 2022, "concept": "Revenue", "value": 110.0},
        {"ticker": "AAA", "fiscal_year": 2022, "concept": "Net Income", "value": 12.0},
    ])
    metrics = pd.DataFrame([
        {"ticker": "AAA", "fiscal_year": 2021, "metric": "Free Cash Flow", "value": 8.0},
        {"ticker": "AAA", "fiscal_year": 2021, "metric": "ROE %", "value": 15.0},
        {"ticker": "AAA", "fiscal_year": 2021, "metric": "CapEx", "value": 5.0},
        {"ticker": "AAA", "fiscal_year": 2021, "metric": "Depreciation & Amortization", "value": 3.0},
        {"ticker": "AAA", "fiscal_year": 2022, "metric": "Free Cash Flow", "value": 9.0},
        {"ticker": "AAA", "fiscal_year": 2022, "metric": "ROE %", "value": 16.0},
        {"ticker": "AAA", "fiscal_year": 2022, "metric": "CapEx", "value": 6.0},
        {"ticker": "AAA", "fiscal_year": 2022, "metric": "Depreciation & Amortization", "value": 3.5},
    ])
    tickers = pd.DataFrame([{"ticker": "AAA", "sector": "Technology", "industry": "Software"}])
    market_cap = pd.DataFrame([
        {"ticker": "AAA", "fiscal_year": 2021, "market_cap": 1000.0},
        {"ticker": "AAA", "fiscal_year": 2022, "market_cap": 1100.0},
    ])
    filed_dates = pd.DataFrame([
        {"ticker": "AAA", "fiscal_year": 2021, "filed": date(2022, 2, 1), "period_end": date(2021, 12, 31)},
        {"ticker": "AAA", "fiscal_year": 2022, "filed": date(2023, 2, 1), "period_end": date(2022, 12, 31)},
    ])
    return financials, metrics, tickers, market_cap, filed_dates


def test_build_training_panel_shape_and_columns():
    financials, metrics, tickers, market_cap, filed_dates = _tiny_inputs()
    panel = fc.build_training_panel(
        financials, metrics, tickers, market_cap, filed_dates, horizons=(1, 2)
    )
    # 2 fiscal years x 2 horizons = 4 rows.
    assert len(panel) == 4
    for col in ("sector", "industry", "size_decile", "as_of_date",
                "log_growth_revenue", "is_loss_revenue",
                "log_growth_net_income", "is_loss_net_income",
                "log_growth_free_cash_flow", "is_loss_free_cash_flow"):
        assert col in panel.columns


def test_build_training_panel_known_target_value():
    financials, metrics, tickers, market_cap, filed_dates = _tiny_inputs()
    panel = fc.build_training_panel(
        financials, metrics, tickers, market_cap, filed_dates, horizons=(1,)
    )
    row_2021 = panel[panel["fiscal_year"] == 2021].iloc[0]
    assert row_2021["log_growth_revenue"] == pytest.approx(math.log(110.0 / 100.0))
    assert row_2021["is_loss_revenue"] == 0
    # 2022 + horizon 1 = 2023, which has no data -> target is missing (row still present).
    # Assigning a Python None into a numeric DataFrame column coerces it to NaN, not a literal
    # None, so pd.isna() is the right check here (not `is None`).
    row_2022 = panel[panel["fiscal_year"] == 2022].iloc[0]
    assert pd.isna(row_2022["log_growth_revenue"])


# ── rearrange_quantiles (Chernozhukov et al. quantile-crossing fix) ────────────────
def test_rearrange_quantiles_leaves_already_monotonic_rows_unchanged():
    predictions = {0.10: [1.0, -2.0], 0.50: [2.0, 0.0], 0.90: [3.0, 5.0]}
    rearranged = fc.rearrange_quantiles(predictions)
    assert rearranged[0.10].tolist() == [1.0, -2.0]
    assert rearranged[0.50].tolist() == [2.0, 0.0]
    assert rearranged[0.90].tolist() == [3.0, 5.0]


def test_rearrange_quantiles_sorts_crossed_quantiles_per_row():
    # Row 0: p10=5 > p50=2 > p90=1 -- fully inverted, must come out sorted ascending.
    # Row 1: already monotonic, untouched.
    predictions = {0.10: [5.0, 1.0], 0.50: [2.0, 2.0], 0.90: [1.0, 3.0]}
    rearranged = fc.rearrange_quantiles(predictions)
    assert rearranged[0.10].tolist() == [1.0, 1.0]
    assert rearranged[0.50].tolist() == [2.0, 2.0]
    assert rearranged[0.90].tolist() == [5.0, 3.0]


def test_rearrange_quantiles_reassigns_to_the_same_level_keys():
    predictions = {0.25: [10.0], 0.75: [10.0]}
    rearranged = fc.rearrange_quantiles(predictions)
    assert set(rearranged) == {0.25, 0.75}


# ── reconstruct_forecast_value ──────────────────────────────────────────────────────
def test_reconstruct_forecast_value_normal_case():
    value = fc.reconstruct_forecast_value(100.0, math.log(1.1), 0.50, p_loss=0.0)
    assert value == pytest.approx(110.0)


def test_reconstruct_forecast_value_none_when_value_from_missing():
    assert fc.reconstruct_forecast_value(None, math.log(1.1), 0.50, p_loss=0.0) is None


def test_reconstruct_forecast_value_none_when_log_growth_missing():
    assert fc.reconstruct_forecast_value(100.0, None, 0.50, p_loss=0.0) is None


def test_reconstruct_forecast_value_floors_to_zero_when_already_non_positive():
    """A ticker already at/below zero today: the multiplicative reconstruction is undefined for
    a non-positive base (mirrors log_growth's own training-time exclusion) -- floors to 0.0
    rather than negative_base * positive_exp_growth still coming out negative (the real bug
    caught during manual smoke-testing, 2026-08-01)."""
    assert fc.reconstruct_forecast_value(0.0, math.log(1.5), 0.50, p_loss=0.0) == 0.0
    assert fc.reconstruct_forecast_value(-50.0, math.log(1.5), 0.50, p_loss=0.0) == 0.0


def test_reconstruct_forecast_value_floors_to_zero_when_p_loss_exceeds_quantile_level():
    # P(loss)=0.60 means the p10/p25/p50 quantiles are all in loss territory (0.60 >= each
    # level), but p75/p90 are not.
    assert fc.reconstruct_forecast_value(100.0, math.log(1.1), 0.50, p_loss=0.60) == 0.0
    assert fc.reconstruct_forecast_value(100.0, math.log(1.1), 0.75, p_loss=0.60) == pytest.approx(110.0)


def test_reconstruct_forecast_value_p_loss_exactly_equal_to_quantile_level_floors():
    # "q is the value below which a fraction q of the distribution lies" -- P(loss) == q means
    # that quantile itself is right at the loss boundary, treated as loss territory (>=, not >).
    assert fc.reconstruct_forecast_value(100.0, math.log(1.1), 0.50, p_loss=0.50) == 0.0


def test_reconstruct_forecast_value_missing_p_loss_degrades_to_not_a_loss():
    """No classifier available for this (metric, horizon) -- see train_loss_classifiers'
    "up to 15" skip -- degrades to the normal multiplicative reconstruction, never raises."""
    value = fc.reconstruct_forecast_value(100.0, math.log(1.1), 0.50, p_loss=None)
    assert value == pytest.approx(110.0)


# ── LightGBM model training + prediction (end-to-end) ───────────────────────────────
def _synthetic_training_panel(n: int = 300, seed: int = 0) -> pd.DataFrame:
    """A cross-sectional panel shaped like build_training_panel's real output, big enough for
    LightGBM to actually fit on (unlike _tiny_inputs' 4-row panel, which is for shape-checking
    build_training_panel itself, not for training a real model)."""
    rng = np.random.default_rng(seed)
    sectors = rng.choice(["Technology", "Health Care"], n)
    industries = rng.choice(["Software", "Biotechnology", "Hardware"], n)
    panel = pd.DataFrame({
        "ticker": [f"T{i}" for i in range(n)],
        "fiscal_year": rng.integers(2015, 2023, n),
        "horizon": rng.integers(1, 6, n),
        "sector": sectors,
        "industry": industries,
        "size_decile": rng.integers(1, 11, n).astype(float),
        "Gross Margin %": rng.normal(40, 10, n),
        "Operating Margin %": rng.normal(15, 8, n),
        "Net Margin %": rng.normal(10, 6, n),
        "ROE %": rng.normal(15, 10, n),
        "Debt / Equity": rng.uniform(0, 2, n),
        "Debt / Assets": rng.uniform(0, 1, n),
        "Net Debt / EBITDA": rng.normal(2, 1, n),
        "reinvestment_rate": rng.normal(0.3, 0.1, n),
        "fcf_conversion": rng.normal(0.8, 0.2, n),
        "growth_trend_revenue": rng.normal(0.05, 0.05, n),
        "growth_trend_net_income": rng.normal(0.05, 0.08, n),
        "growth_trend_free_cash_flow": rng.normal(0.05, 0.08, n),
        "Revenue": rng.uniform(100, 1000, n),
        "Net Income": rng.uniform(10, 200, n),
        "Free Cash Flow": rng.uniform(5, 150, n),
    })
    for suffix in fc.TARGET_METRICS:
        panel[f"log_growth_{suffix}"] = rng.normal(0.05, 0.2, n)
        panel[f"is_loss_{suffix}"] = rng.integers(0, 2, n)
    # Revenue essentially never posts a loss -- the real-world degenerate single-class case
    # train_loss_classifiers must skip rather than crash on.
    panel["is_loss_revenue"] = 0
    return panel


_LGBM_PARAMS = {"min_child_samples": 1, "min_data_in_leaf": 1}


def test_train_quantile_models_returns_one_model_per_metric_times_quantile_level():
    panel = _synthetic_training_panel()
    models = fc.train_quantile_models(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    assert set(models) == {
        (suffix, level) for suffix in fc.TARGET_METRICS for level in fc.QUANTILE_LEVELS
    }


def test_train_loss_classifiers_skips_single_class_metric_horizon_combos():
    """Revenue's is_loss column is a constant 0 in the fixture (mirrors real data -- Revenue
    essentially never goes negative) -- every (revenue, horizon) combo must be skipped, not
    fabricated as a constant-probability model."""
    panel = _synthetic_training_panel()
    models = fc.train_loss_classifiers(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    assert not any(suffix == "revenue" for suffix, _horizon in models)
    assert any(suffix == "net_income" for suffix, _horizon in models)


def test_predict_forecast_is_never_negative():
    """Both zero-floor rules (already-non-positive value_from, and p_loss >= quantile_level)
    compose correctly end-to-end through real trained models -- no reconstructed forecast value
    is ever negative."""
    panel = _synthetic_training_panel()
    quantile_models = fc.train_quantile_models(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    classifier_models = fc.train_loss_classifiers(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    forecasts = fc.predict_forecast(panel, quantile_models, classifier_models)
    assert len(forecasts) == len(panel) * len(fc.TARGET_METRICS) * len(fc.QUANTILE_LEVELS)
    assert (forecasts["forecast_value"] >= 0).all()


def test_predict_forecast_quantiles_are_monotonic_per_row():
    panel = _synthetic_training_panel()
    quantile_models = fc.train_quantile_models(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    classifier_models = fc.train_loss_classifiers(panel, num_boost_round=5, lightgbm_params=_LGBM_PARAMS)
    forecasts = fc.predict_forecast(panel, quantile_models, classifier_models)
    pivot = forecasts.pivot_table(
        index=["ticker", "fiscal_year", "horizon", "metric"],
        columns="quantile_level", values="forecast_value",
    )
    # Every row's values, read left-to-right (ascending quantile level), must be non-decreasing.
    assert (pivot.diff(axis=1).iloc[:, 1:] >= -1e-9).all(axis=None)


def test_predict_forecast_empty_when_no_models_trained():
    panel = _synthetic_training_panel(n=20)
    forecasts = fc.predict_forecast(panel, quantile_models={}, classifier_models={})
    assert forecasts.empty
    assert list(forecasts.columns) == [
        "ticker", "fiscal_year", "horizon", "metric", "quantile_level", "forecast_value",
    ]
