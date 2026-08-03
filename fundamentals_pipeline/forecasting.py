"""Cross-sectional forecasting-panel assembly — feature/target construction for the
LightGBM quantile-regression Forecasting models (see ``docs/mockups/forecasting_tab.html``).

CONTRACT: reused by the pipeline stage ``90__pipelines/24__forecasting.py`` (not yet built —
issue #332), which does all the Spark I/O (reading ``main.financials.financials``,
``financials_metrics``, ``financials_raw``, ``config.tickers``, ``market_cap_asof``) and hands
this module plain pandas DataFrames; this module does zero I/O of its own. No Spark/``dbutils``
dependency, so it's unit-testable like ``identity.py``/``schemas.py``.

Two-part (hurdle) target design: ``log(value_{t+h}/value_t)`` is a pure multiplicative growth
rate — a forecast reconstructed from it (``value_t * exp(predicted_growth)``) can never be
negative, so it structurally cannot represent a company going into losses. :func:`log_growth`
stays the literal formula for the quantile regressors (``None`` whenever either endpoint is
``<= 0``); :func:`is_loss` is a separate binary target (independent of the starting value's
sign) for a companion classifier — see issue #331's scope update.

No-look-ahead is enforced via :mod:`fundamentals_pipeline.backtest`'s ``as_of_eligible``
(reused, not reimplemented) — a fiscal year's features are only usable from their as-of date
(SEC filing date, or ``period_end + lag`` fallback), the same discipline the backtester uses.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Union

import numpy as np
import pandas as pd

from .backtest import as_of_date as _as_of_date
from .backtest import as_of_eligible
from .valuation import eps_cagr

# `from __future__ import annotations` only defers *annotation* evaluation — this is a plain
# assignment, so the RHS runs at import time. PEP 604 `float | int | None` needs `type.__or__`,
# which doesn't exist before Python 3.10; `typing.Union` is the 3.9-safe equivalent.
Number = Union[float, int, None]

# The three forecast targets, keyed by the column suffix used throughout the panel (`log_growth_
# revenue`, `is_loss_net_income`, ...) → the concept/metric name it's sourced from. `Revenue` and
# `Net Income` are raw `financials` concepts; `Free Cash Flow` only exists in `financials_metrics`
# (see issue #329's Phase-0 audit) — callers merge both into one `target_values` table (see
# `build_training_panel`) so this module can treat all three uniformly.
TARGET_METRICS: Mapping[str, str] = {
    "revenue": "Revenue",
    "net_income": "Net Income",
    "free_cash_flow": "Free Cash Flow",
}


def _is_missing(v: Number) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


# ── target formulas (per metric × horizon) ──────────────────────────────────────────────────
def log_growth(value_from: Number, value_to: Number) -> float | None:
    """``log(value_to / value_from)`` — the continuous quantile-regression target.

    ``None`` whenever either value is missing or non-positive: the ratio (and its log) is
    undefined for a zero/negative denominator, and a sign-flipping ratio isn't a meaningful
    growth rate. Loss transitions are instead captured by :func:`is_loss`, not by this function
    returning a substitute value — never silently guessed.
    """
    if _is_missing(value_from) or _is_missing(value_to):
        return None
    if value_from <= 0 or value_to <= 0:
        return None
    return math.log(float(value_to) / float(value_from))


def is_loss(value_to: Number) -> int | None:
    """``1`` if ``value_to <= 0`` (a loss/negative value at the forecast horizon), else ``0``.

    Defined independent of the starting value's sign — this is the binary companion target
    that lets the two-part model represent a forecast going into losses, something
    :func:`log_growth` structurally cannot (see module docstring). ``None`` only when
    ``value_to`` itself is missing.
    """
    if _is_missing(value_to):
        return None
    return 1 if value_to <= 0 else 0


def reinvestment_rate(capex: Number, d_and_a: Number, net_income: Number) -> float | None:
    """``(CapEx - D&A) / Net Income`` — how much of earnings is being reinvested net of the
    depreciation runoff. ``None`` if any input is missing or ``net_income`` is zero (undefined
    denominator) — never divides by a zero/near-zero base.
    """
    if _is_missing(capex) or _is_missing(d_and_a) or _is_missing(net_income):
        return None
    if net_income == 0:
        return None
    return (float(capex) - float(d_and_a)) / float(net_income)


def fcf_conversion(free_cash_flow: Number, net_income: Number) -> float | None:
    """``Free Cash Flow / Net Income`` — how much of reported earnings actually converts to
    cash. ``None`` if either input is missing or ``net_income`` is zero.
    """
    if _is_missing(free_cash_flow) or _is_missing(net_income):
        return None
    if net_income == 0:
        return None
    return float(free_cash_flow) / float(net_income)


# ── cross-sectional bucketing ────────────────────────────────────────────────────────────────
def size_decile(market_caps: pd.Series, fiscal_years: pd.Series) -> pd.Series:
    """Market-cap decile (1-10, 10 = largest), computed independently *within* each fiscal
    year — never across years, which would let a later year's larger/smaller universe change
    an earlier year's ranking (a look-ahead leak in disguise).

    ``NaN`` for a missing market cap, or for a fiscal year with fewer than 10 tickers carrying
    a market cap (too few to form deciles meaningfully).
    """
    if len(market_caps) != len(fiscal_years):
        raise ValueError(
            f"market_caps/fiscal_years length mismatch: {len(market_caps)} != {len(fiscal_years)}"
        )
    frame = pd.DataFrame({
        "market_cap": pd.Series(market_caps).to_numpy(),
        "fiscal_year": pd.Series(fiscal_years).to_numpy(),
    })

    def _decile(group: pd.Series) -> pd.Series:
        if group.dropna().shape[0] < 10:
            return pd.Series(float("nan"), index=group.index)
        return pd.qcut(group, 10, labels=False, duplicates="drop") + 1

    result = frame.groupby("fiscal_year")["market_cap"].transform(_decile)
    result.index = pd.Series(market_caps).index
    return result


# ── multi-year trend feature ─────────────────────────────────────────────────────────────────
def growth_trend(
    values_by_year: Mapping[int, Number], *, min_years: int = 3, max_years: int = 5
) -> float | None:
    """Mean year-over-year :func:`log_growth` over the trailing ``min_years``-to-``max_years``
    fiscal years of ``values_by_year`` (keyed by fiscal year).

    Uses whichever window is actually available in ``[min_years, max_years]`` trailing YoY
    transitions — "3-5y", not a fixed 5y. A gap year (a missing intervening fiscal year) breaks
    the consecutive-year requirement for that one transition rather than silently bridging it.
    ``None`` if fewer than ``min_years`` valid transitions remain.
    """
    years = sorted(values_by_year)
    recent_years = years[-(max_years + 1):]
    transitions = []
    for prev_year, cur_year in zip(recent_years, recent_years[1:]):
        if cur_year != prev_year + 1:
            continue
        g = log_growth(values_by_year[prev_year], values_by_year[cur_year])
        if g is not None:
            transitions.append(g)
    if len(transitions) < min_years:
        return None
    return sum(transitions) / len(transitions)


# ── no-look-ahead walk-forward split ─────────────────────────────────────────────────────────
def expanding_window_split(
    panel: pd.DataFrame, as_of_col: str, cutoff_date: date
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits ``panel`` into ``(train, test)`` by no-look-ahead eligibility, not by fiscal
    year: a row belongs to ``train`` iff its ``as_of_col`` date was actually knowable by
    ``cutoff_date`` (via :func:`fundamentals_pipeline.backtest.as_of_eligible`) — never a bare
    ``fiscal_year <= cutoff_year`` comparison, which would leak rows filed late in an otherwise-
    eligible-looking year.

    ``test`` is every row that fails eligibility. This function doesn't define a forward test
    horizon by itself; the caller chains successive cutoffs into an expanding-window
    walk-forward loop.
    """
    eligible = panel[as_of_col].apply(lambda d: as_of_eligible(d, cutoff_date))
    return panel[eligible].copy(), panel[~eligible].copy()


# ── panel assembly ───────────────────────────────────────────────────────────────────────────
def build_training_panel(
    financials: pd.DataFrame,
    metrics: pd.DataFrame,
    tickers: pd.DataFrame,
    market_cap: pd.DataFrame,
    filed_dates: pd.DataFrame,
    *,
    horizons: tuple[int, ...] = (1, 2, 3, 4, 5),
) -> pd.DataFrame:
    """Assemble the ticker-year-horizon training panel from already-fetched inputs (no I/O
    here — the caller, eventually ``24__forecasting.py``, does all the Spark reads).

    Expected inputs (all FY-only, long format unless noted):

    - ``financials``: columns ``ticker, fiscal_year, concept, value`` — pre-filtered to
      ``concept in {"Revenue", "Net Income"}``.
    - ``metrics``: columns ``ticker, fiscal_year, metric, value`` — must include
      ``"Free Cash Flow"``, ``"Gross Margin %"``, ``"Operating Margin %"``, ``"Net Margin %"``,
      ``"ROE %"``, ``"Debt / Equity"``, ``"Debt / Assets"``, ``"Net Debt / EBITDA"``,
      ``"CapEx"``, ``"Depreciation & Amortization"``; any others are ignored.
    - ``tickers``: columns ``ticker, sector, industry`` (wide, one row per ticker).
    - ``market_cap``: columns ``ticker, fiscal_year, market_cap``.
    - ``filed_dates``: columns ``ticker, fiscal_year, filed, period_end`` (``filed`` may be
      ``None`` — :func:`fundamentals_pipeline.backtest.as_of_date` falls back to
      ``period_end + lag`` per row).

    Returns one row per ``(ticker, fiscal_year, horizon)`` for ``horizon in horizons``, with
    ``sector``, ``industry``, ``size_decile``, the margin/ROE/leverage features,
    ``reinvestment_rate``, ``fcf_conversion``, a ``growth_trend_<metric>`` per target metric,
    ``as_of_date``, and per target metric both ``log_growth_<metric>`` and ``is_loss_<metric>``
    columns. A row with no data at ``fiscal_year + horizon`` yet (the common case for large
    horizons on the most recent fiscal years) still appears, with ``None`` targets — those rows
    are excluded from training by construction (no target to regress on) but stay in the panel
    for inference.
    """
    fin_wide = financials.pivot_table(
        index=["ticker", "fiscal_year"], columns="concept", values="value", aggfunc="first"
    )
    met_wide = metrics.pivot_table(
        index=["ticker", "fiscal_year"], columns="metric", values="value", aggfunc="first"
    )

    # One wide table covering all 3 target concepts, uniformly — Revenue/Net Income from
    # `financials`, Free Cash Flow from `financials_metrics` (it has no raw-concept form).
    target_cols = list(TARGET_METRICS.values())
    target_values = fin_wide.reindex(columns=["Revenue", "Net Income"]).join(
        met_wide.reindex(columns=["Free Cash Flow"]), how="outer"
    ).reindex(columns=target_cols)

    base = target_values.join(met_wide, how="outer", rsuffix="_metric").reset_index()
    base = base.merge(tickers[["ticker", "sector", "industry"]], on="ticker", how="left")
    base = base.merge(market_cap[["ticker", "fiscal_year", "market_cap"]],
                       on=["ticker", "fiscal_year"], how="left")
    base = base.merge(filed_dates[["ticker", "fiscal_year", "filed", "period_end"]],
                       on=["ticker", "fiscal_year"], how="left")

    base["size_decile"] = size_decile(base["market_cap"], base["fiscal_year"])
    base["as_of_date"] = [
        _as_of_date(f, p) for f, p in zip(base["filed"], base["period_end"])
    ]
    base["reinvestment_rate"] = [
        reinvestment_rate(capex, da, ni)
        for capex, da, ni in zip(
            base.get("CapEx"), base.get("Depreciation & Amortization"), base["Net Income"]
        )
    ]
    base["fcf_conversion"] = [
        fcf_conversion(fcf, ni) for fcf, ni in zip(base["Free Cash Flow"], base["Net Income"])
    ]

    # Per-ticker trailing history for the growth-trend feature: only years <= this row's own
    # fiscal_year are ever visible to that row (no look-ahead into a ticker's own future years).
    history = {
        ticker: grp.set_index("fiscal_year")[target_cols].to_dict("index")
        for ticker, grp in base[["ticker", "fiscal_year", *target_cols]].groupby("ticker")
    }

    def _trend_for(row: pd.Series, target_col: str) -> float | None:
        hist = history[row["ticker"]]
        trailing = {
            fy: vals[target_col] for fy, vals in hist.items() if fy <= row["fiscal_year"]
        }
        return growth_trend(trailing)

    for suffix, target_col in TARGET_METRICS.items():
        base[f"growth_trend_{suffix}"] = base.apply(_trend_for, axis=1, target_col=target_col)

    # Expand to one row per (ticker, fiscal_year, horizon), then attach the h-year-ahead target.
    panel = base.merge(pd.DataFrame({"horizon": list(horizons)}), how="cross")

    def _future_value(row: pd.Series, target_col: str) -> Number:
        hist = history[row["ticker"]]
        future_year = row["fiscal_year"] + row["horizon"]
        future = hist.get(future_year)
        return future[target_col] if future is not None else None

    for suffix, target_col in TARGET_METRICS.items():
        future_values = panel.apply(_future_value, axis=1, target_col=target_col)
        panel[f"log_growth_{suffix}"] = [
            log_growth(v_from, v_to) for v_from, v_to in zip(panel[target_col], future_values)
        ]
        panel[f"is_loss_{suffix}"] = [is_loss(v) for v in future_values]

    feature_cols = [
        "ticker", "fiscal_year", "horizon", "sector", "industry", "size_decile",
        "Gross Margin %", "Operating Margin %", "Net Margin %", "ROE %",
        "Debt / Equity", "Debt / Assets", "Net Debt / EBITDA",
        "reinvestment_rate", "fcf_conversion",
        *[f"growth_trend_{suffix}" for suffix in TARGET_METRICS],
        "as_of_date",
        *[f"log_growth_{suffix}" for suffix in TARGET_METRICS],
        *[f"is_loss_{suffix}" for suffix in TARGET_METRICS],
    ]
    return panel.reindex(columns=feature_cols).reset_index(drop=True)


# ── LightGBM quantile-regression model training (issue #331) ───────────────────────────────
#
# CONTRACT: `lightgbm` is imported lazily, inside the two train_* functions below, never at
# module level — this module is imported by 24__forecasting.py (issue #332, Databricks) and by
# this repo's own test suite (`requirements-dev.txt`, which does pin lightgbm), but nothing else
# in this codebase needs it, and the panel-assembly/pure-math functions above must stay
# importable in an environment that never installed it (mirrors this package's own
# artifacts.py/schemas.py split — see that module's docstring for the identical rationale).
#
# Model count: 15 quantile regressors (3 target metrics x 5 quantile levels), `horizon` is a
# numeric FEATURE within each one (never a separate model per horizon) per issue #331's own
# instruction. Up to 15 binary loss classifiers (3 target metrics x 5 horizons) -- "up to",
# because a (metric, horizon) whose training rows are single-class (e.g. Revenue essentially
# never goes negative) can't fit a binary classifier and is skipped, not fabricated (see
# train_loss_classifiers). horizon is NOT a feature there since each classifier is already
# scoped to one horizon.

#: The 5 quantile levels this feature trains regressors for (p10/p25/p50/p75/p90 in the
#: mockup's Bear/Low Bear/Crab/Low Bull/Bull legend — the scenario NAME mapping is a display
#: concern for a later issue; this module only ever deals in the numeric quantile level).
QUANTILE_LEVELS: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)

# Cross-sectional predictor columns build_training_panel produces — every TARGET_METRICS/
# log_growth_*/is_loss_* column is a label, not a feature, and is deliberately excluded here
# (a model must never see its own target, or another horizon's/metric's target, as an input).
FEATURE_COLUMNS: tuple[str, ...] = (
    "sector", "industry", "size_decile", "horizon",
    "Gross Margin %", "Operating Margin %", "Net Margin %", "ROE %",
    "Debt / Equity", "Debt / Assets", "Net Debt / EBITDA",
    "reinvestment_rate", "fcf_conversion",
    "growth_trend_revenue", "growth_trend_net_income", "growth_trend_free_cash_flow",
)

# `sector`/`industry` are raw, un-one-hot categoricals per issue #330's own feature spec —
# LightGBM's native categorical splitting needs pandas 'category' dtype (see _prepare_features).
CATEGORICAL_FEATURES: tuple[str, ...] = ("sector", "industry")


def _prepare_features(panel: pd.DataFrame) -> pd.DataFrame:
    """`panel` (build_training_panel's output, or any frame carrying the same FEATURE_COLUMNS)
    reduced to the model-input columns, with sector/industry cast to pandas 'category' dtype so
    LightGBM treats them as native categoricals instead of raw strings."""
    features = panel.reindex(columns=list(FEATURE_COLUMNS)).copy()
    for col in CATEGORICAL_FEATURES:
        features[col] = features[col].astype("category")
    return features


# Moderate regularization defaults for both train_* functions below — folded in ahead of
# `**(lightgbm_params or {})` so a caller's own params still win. Addresses a real overfitting
# concern raised in review: LightGBM (any GBM) can overfit with unlimited leaves/no minimum leaf
# size, same as any other boosting library — this isn't a reason to switch away from LightGBM
# (see train_quantile_models' own scikit-learn-avoidance docstring for why a from-scratch
# scikit-learn-based alternative was rejected), just a reason to tune it properly.
_REGULARIZATION_DEFAULTS: dict[str, Any] = {
    "num_leaves": 31, "min_data_in_leaf": 20, "lambda_l1": 0.1, "lambda_l2": 0.1,
}


def _train_one_booster(
    lgb: Any,
    features: pd.DataFrame,
    target: pd.Series,
    val_features: pd.DataFrame | None,
    val_target: pd.Series | None,
    params: Mapping[str, Any],
    num_boost_round: int,
    early_stopping_rounds: int,
) -> Any:
    """Shared train_set/valid_set/early-stopping wiring for both train_quantile_models and
    train_loss_classifiers, so the early-stopping logic lives in exactly one place.

    ``val_features``/``val_target`` are ``None`` (no early stopping — trains for the fixed
    ``num_boost_round``) unless the caller supplied a non-empty ``validation_panel`` with rows
    for this same target column — see both callers' own docstrings for why the *split itself*
    is the caller's responsibility, not this function's (it must come from
    :func:`expanding_window_split`, never a random split).
    """
    train_set = lgb.Dataset(
        features, label=target, categorical_feature=list(CATEGORICAL_FEATURES), free_raw_data=False,
    )
    if val_features is None or val_target is None or val_features.empty:
        return lgb.train(dict(params), train_set, num_boost_round=num_boost_round)
    val_set = lgb.Dataset(
        val_features, label=val_target, categorical_feature=list(CATEGORICAL_FEATURES),
        reference=train_set, free_raw_data=False,
    )
    return lgb.train(
        dict(params), train_set, num_boost_round=num_boost_round, valid_sets=[val_set],
        callbacks=[lgb.early_stopping(early_stopping_rounds, verbose=False)],
    )


def train_quantile_models(
    panel: pd.DataFrame,
    *,
    validation_panel: pd.DataFrame | None = None,
    quantile_levels: Sequence[float] = QUANTILE_LEVELS,
    num_boost_round: int = 100,
    early_stopping_rounds: int = 20,
    lightgbm_params: Mapping[str, Any] | None = None,
) -> dict[tuple[str, float], Any]:
    """One LightGBM quantile regressor per (target-metric suffix, quantile level) — trained on
    that metric's own rows where ``log_growth_<metric>`` is non-null (a row with no known future
    value, the common case for large horizons on recent fiscal years, is excluded from training
    by construction, never imputed). Returns ``{(suffix, quantile_level): fitted_booster}``.

    Uses ``lightgbm``'s native ``Dataset``/``train`` API, not the ``lightgbm.sklearn`` wrapper
    — the latter hard-requires ``scikit-learn`` to even be imported, which issue #329's Phase 0
    audit never verified installs on Databricks Free Edition serverless (only ``lightgbm``/
    ``catboost`` were checked); the native API needs nothing beyond ``lightgbm`` itself. This is
    also why a Random-Forest-based alternative was rejected on review: plain
    ``RandomForestRegressor`` has no native quantile-regression support, and a quantile-capable
    RF variant would be a second unaudited dependency, not a lighter one.

    ``validation_panel``, if given, enables early stopping (``early_stopping_rounds``) against
    that metric's own non-null rows in it — build it via :func:`expanding_window_split` on a
    cutoff BEFORE calling this function (never a random split; see that function's own
    no-look-ahead rationale). ``None`` (the default) trains for the fixed ``num_boost_round``
    with no early stopping. Moderate regularization defaults (see ``_REGULARIZATION_DEFAULTS``)
    are applied unless overridden via ``lightgbm_params``.
    """
    import lightgbm as lgb

    models: dict[tuple[str, float], Any] = {}
    for suffix in TARGET_METRICS:
        target_col = f"log_growth_{suffix}"
        rows = panel[panel[target_col].notna()]
        if rows.empty:
            continue
        features = _prepare_features(rows)
        target = rows[target_col]

        val_features: pd.DataFrame | None = None
        val_target: pd.Series | None = None
        if validation_panel is not None:
            val_rows = validation_panel[validation_panel[target_col].notna()]
            if not val_rows.empty:
                val_features = _prepare_features(val_rows)
                val_target = val_rows[target_col]

        for level in quantile_levels:
            params: dict[str, Any] = {
                "objective": "quantile", "alpha": level, "verbosity": -1,
                **_REGULARIZATION_DEFAULTS, **(lightgbm_params or {}),
            }
            models[(suffix, level)] = _train_one_booster(
                lgb, features, target, val_features, val_target, params,
                num_boost_round, early_stopping_rounds,
            )
    return models


def train_loss_classifiers(
    panel: pd.DataFrame,
    *,
    validation_panel: pd.DataFrame | None = None,
    horizons: Sequence[int] = (1, 2, 3, 4, 5),
    num_boost_round: int = 100,
    early_stopping_rounds: int = 20,
    lightgbm_params: Mapping[str, Any] | None = None,
) -> dict[tuple[str, int], Any]:
    """One binary LightGBM classifier per (target-metric suffix, horizon) predicting
    ``P(is_loss_<metric>)`` — scoped to a single horizon's own rows (unlike the quantile
    regressors above, horizon is NOT a feature here, since each model already IS one horizon).

    A (metric, horizon) combination whose ``is_loss_<metric>`` column is single-class in the
    training rows (e.g. Revenue essentially never goes negative, or too little history exists
    yet at a large horizon) is skipped — a binary classifier cannot be fit on one class, and
    fabricating a constant-probability model would be worse than callers degrading to "assume
    not a loss" (see :func:`predict_forecast`). Returns ``{(suffix, horizon): fitted_booster}``,
    which may have fewer than ``len(TARGET_METRICS) * len(horizons)`` entries.

    Same native ``Dataset``/``train`` API, same ``validation_panel``-gated early stopping, and
    same regularization defaults as :func:`train_quantile_models` — see that function's own
    docstring for the full rationale on all three. Real classifier quality (not just "did it
    overfit the training set") is best checked with :func:`cross_validate_loss_classifier`.
    """
    import lightgbm as lgb

    models: dict[tuple[str, int], Any] = {}
    for suffix in TARGET_METRICS:
        target_col = f"is_loss_{suffix}"
        for horizon in horizons:
            rows = panel[(panel["horizon"] == horizon) & panel[target_col].notna()]
            if rows[target_col].nunique() < 2:
                continue
            features = _prepare_features(rows)
            target = rows[target_col]

            val_features: pd.DataFrame | None = None
            val_target: pd.Series | None = None
            if validation_panel is not None:
                val_rows = validation_panel[
                    (validation_panel["horizon"] == horizon) & validation_panel[target_col].notna()
                ]
                if not val_rows.empty and val_rows[target_col].nunique() >= 2:
                    val_features = _prepare_features(val_rows)
                    val_target = val_rows[target_col]

            params: dict[str, Any] = {
                "objective": "binary", "verbosity": -1,
                **_REGULARIZATION_DEFAULTS, **(lightgbm_params or {}),
            }
            models[(suffix, horizon)] = _train_one_booster(
                lgb, features, target, val_features, val_target, params,
                num_boost_round, early_stopping_rounds,
            )
    return models


# ── classifier evaluation: ROC-AUC via walk-forward cross-validation ────────────────────────
def roc_auc_score(y_true: Sequence[int], y_score: Sequence[float]) -> float | None:
    """Area under the ROC curve, via the Mann-Whitney U / rank-sum equivalence — a small,
    dependency-free implementation deliberately NOT ``sklearn.metrics.roc_auc_score``: issue
    #329's Phase 0 audit never verified scikit-learn installs on Databricks Free Edition
    serverless (only ``lightgbm``/``catboost`` were checked, and this module already hit that
    exact gap once — see :func:`train_quantile_models`'s own docstring on why it uses
    ``lightgbm``'s native API instead of the ``sklearn`` wrapper).

    ``None`` when ``y_true`` has only one class (AUC is undefined with no negative or no
    positive example to separate), never a fabricated 0.5.
    """
    y_true_arr = np.asarray(y_true)
    y_score_arr = np.asarray(y_score, dtype=float)
    n_pos = int((y_true_arr == 1).sum())
    n_neg = int((y_true_arr == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return None
    order = np.argsort(y_score_arr, kind="mergesort")
    sorted_scores = y_score_arr[order]
    ranks = np.arange(1, len(y_score_arr) + 1, dtype=float)
    # Average ranks across ties (equal predicted scores) so tied predictions don't arbitrarily
    # favor whichever happened to sort first.
    i = 0
    while i < len(sorted_scores):
        j = i
        while j + 1 < len(sorted_scores) and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        if j > i:
            ranks[i:j + 1] = ranks[i:j + 1].mean()
        i = j + 1
    rank_by_original_position = np.empty(len(y_score_arr))
    rank_by_original_position[order] = ranks
    sum_ranks_pos = rank_by_original_position[y_true_arr == 1].sum()
    return float((sum_ranks_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def cross_validate_loss_classifier(
    panel: pd.DataFrame,
    suffix: str,
    horizon: int,
    *,
    n_folds: int = 5,
    num_boost_round: int = 100,
    lightgbm_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Walk-forward (expanding-window) cross-validated ROC-AUC for one (target metric, horizon)
    loss classifier — answers "how good is this classifier really", never a random k-fold
    split: ``n_folds`` cutoff dates, evenly spaced across this (metric, horizon)'s own
    ``as_of_date`` range, each reusing :func:`expanding_window_split` (never a random
    row-level split — same no-look-ahead discipline as everywhere else in this module). Each
    fold trains fresh on every row eligible as of that fold's cutoff and scores
    :func:`roc_auc_score` on the rows that become eligible before the NEXT fold's cutoff (or,
    for the last fold, everything remaining).

    Returns ``{"fold_scores": [...], "mean_auc": float | None}`` — a fold whose training rows
    or validation slice is single-class contributes no score (skipped, not fabricated);
    ``mean_auc`` is ``None`` if every fold was unscoreable (e.g. too few distinct ``as_of_date``
    values for ``n_folds``, or the metric/horizon combo is rare enough that every fold ends up
    single-class — mirrors :func:`train_loss_classifiers`' own single-class skip).
    """
    import lightgbm as lgb

    target_col = f"is_loss_{suffix}"
    rows = panel[
        (panel["horizon"] == horizon) & panel[target_col].notna() & panel["as_of_date"].notna()
    ]
    dates = np.sort(rows["as_of_date"].unique())
    if len(dates) < n_folds + 1:
        return {"fold_scores": [], "mean_auc": None}

    boundaries = [dates[int(len(dates) * i / (n_folds + 1))] for i in range(1, n_folds + 1)]
    fold_scores: list[float] = []
    for i, cutoff in enumerate(boundaries):
        train_rows, remainder = expanding_window_split(rows, "as_of_date", cutoff)
        next_cutoff = boundaries[i + 1] if i + 1 < len(boundaries) else None
        fold_rows = (
            expanding_window_split(remainder, "as_of_date", next_cutoff)[0]
            if next_cutoff is not None else remainder
        )
        if train_rows[target_col].nunique() < 2 or fold_rows.empty or fold_rows[target_col].nunique() < 2:
            continue
        params: dict[str, Any] = {
            "objective": "binary", "verbosity": -1,
            **_REGULARIZATION_DEFAULTS, **(lightgbm_params or {}),
        }
        train_set = lgb.Dataset(
            _prepare_features(train_rows), label=train_rows[target_col],
            categorical_feature=list(CATEGORICAL_FEATURES), free_raw_data=False,
        )
        model = lgb.train(params, train_set, num_boost_round=num_boost_round)
        predictions = model.predict(_prepare_features(fold_rows))
        score = roc_auc_score(fold_rows[target_col].to_numpy(), predictions)
        if score is not None:
            fold_scores.append(score)
    mean_auc = float(np.mean(fold_scores)) if fold_scores else None
    return {"fold_scores": fold_scores, "mean_auc": mean_auc}


# ── combining quantile regressors + loss classifier into one forecast path ─────────────────
def rearrange_quantiles(predictions: Mapping[float, Sequence[float]]) -> dict[float, np.ndarray]:
    """Chernozhukov, Fernández-Val & Galichon (2010) rearrangement: independently-trained
    quantile regressors have no constraint that they agree with each other, so a lower quantile
    level can (and in practice sometimes does) predict a higher raw value than a higher quantile
    level for the same row — "quantile crossing". This is a required correction, not an edge
    case to skip: sorts each row's predicted values into non-decreasing order across quantile
    levels, then reassigns the sorted values back to the SAME quantile-level keys (the smallest
    value always lands on the lowest quantile level, regardless of which model produced it).

    ``predictions`` maps quantile level -> that level's predicted values (one entry per row,
    same length/order across all levels). Returns the same shape, monotonic per row.
    """
    levels = sorted(predictions)
    stacked = np.column_stack([np.asarray(predictions[level], dtype=float) for level in levels])
    rearranged = np.sort(stacked, axis=1)
    return {level: rearranged[:, i] for i, level in enumerate(levels)}


def reconstruct_forecast_value(
    value_from: Number, log_growth_quantile: Number, quantile_level: float, p_loss: Number
) -> float | None:
    """Combines one quantile regressor's ``log_growth`` prediction with the loss classifier's
    ``P(loss)`` into the single absolute-value forecast the Forecasting tab plots (issue #331's
    "how do these combine" design question — see ``CLAUDE.md``'s Forecasting entry for the full
    write-up):

    - A quantile level ``q`` is, by definition, "the value below which a fraction ``q`` of the
      distribution lies" — so if ``P(loss) >= q``, at least that much of the distribution's
      lower tail is a loss, meaning the ``q``-th quantile itself falls in loss territory. This
      is a property of quantiles, not a tuned threshold (e.g. ``P(loss)=0.85`` means the
      p10/p25/p50/p75 quantiles are all losses, only p90 isn't).
    - The quantile regressor never learned a loss MAGNITUDE (:func:`log_growth` is only defined,
      and only ever trained on, positive-to-positive transitions) and this feature deliberately
      caps model count at ~15 regressors + ~15 classifiers — no third "loss magnitude" model.
      A quantile in loss territory is floored to ``0.0``: honest about "no magnitude estimate"
      rather than fabricating a specific negative number.
    - ``value_from`` itself already ``<= 0`` (the ticker is ALREADY in loss/breakeven territory
      at the forecast's starting point) hits the same ``0.0`` floor, for the same reason: the
      multiplicative reconstruction is undefined for a non-positive base (mirrors
      :func:`log_growth`'s own training-time exclusion of non-positive endpoints), so a model
      trained only on positive-``value_from`` rows would be extrapolating out of its domain,
      not predicting. Confirmed as a real bug during manual smoke-testing (2026-08-01): without
      this check, a negative ``value_from`` times a positive ``exp(growth)`` still comes out
      negative, silently bypassing the "loss floors to 0" rule entirely for tickers that are
      already unprofitable today.
    - Otherwise (not in loss territory, or ``p_loss`` is missing/unavailable — e.g. no
      classifier could be trained for this metric/horizon, see :func:`train_loss_classifiers`
      — which degrades to "assume not a loss", never raises), reconstructs the usual
      multiplicative way: ``value_from * exp(log_growth_quantile)``.

    ``None`` only when ``value_from``/``log_growth_quantile`` themselves are missing (nothing to
    reconstruct from).
    """
    if _is_missing(value_from) or _is_missing(log_growth_quantile):
        return None
    if value_from <= 0:
        return 0.0
    if not _is_missing(p_loss) and p_loss >= quantile_level:
        return 0.0
    return float(value_from) * math.exp(float(log_growth_quantile))


def predict_forecast(
    panel: pd.DataFrame,
    quantile_models: Mapping[tuple[str, float], Any],
    classifier_models: Mapping[tuple[str, int], Any],
    *,
    quantile_levels: Sequence[float] = QUANTILE_LEVELS,
) -> pd.DataFrame:
    """Applies trained models (:func:`train_quantile_models`/:func:`train_loss_classifiers`) to
    ``panel`` (the same shape :func:`build_training_panel` returns — used here for INFERENCE, so
    its target columns may be absent or all-``None``) and reconstructs one absolute-value
    forecast per ``(row, target metric, quantile level)`` via :func:`rearrange_quantiles` +
    :func:`reconstruct_forecast_value`.

    Returns a long-format frame: ``ticker, fiscal_year, horizon, metric`` (the
    :data:`TARGET_METRICS` suffix), ``quantile_level, forecast_value``. A ``(metric, quantile)``
    or ``(metric, horizon)`` pair with no trained model (see the two train_* functions' own
    "up to 15" / empty-rows notes) is simply absent from the output for that combination, rather
    than raising.
    """
    features = _prepare_features(panel)
    frames: list[pd.DataFrame] = []
    for suffix, target_col in TARGET_METRICS.items():
        value_from = panel[target_col].to_numpy(dtype=float)
        raw_predictions = {
            level: quantile_models[(suffix, level)].predict(features)
            for level in quantile_levels
            if (suffix, level) in quantile_models
        }
        if not raw_predictions:
            continue
        rearranged = rearrange_quantiles(raw_predictions)

        p_loss = pd.Series(0.0, index=panel.index)
        for horizon, group_index in panel.groupby("horizon").groups.items():
            model = classifier_models.get((suffix, int(horizon)))
            if model is None:
                continue  # no classifier for this (metric, horizon) -- stays "assume not a loss"
            # Native Booster.predict() for objective="binary" returns P(class=1) directly (a
            # 1-D array), unlike sklearn's predict_proba()[:, 1] — see train_loss_classifiers'
            # own docstring for why this module uses the native API at all.
            p_loss.loc[group_index] = model.predict(features.loc[group_index])

        for level, log_growth_values in rearranged.items():
            forecast_values = [
                reconstruct_forecast_value(vf, lg, level, pl)
                for vf, lg, pl in zip(value_from, log_growth_values, p_loss.to_numpy())
            ]
            frames.append(pd.DataFrame({
                "ticker": panel["ticker"].to_numpy(),
                "fiscal_year": panel["fiscal_year"].to_numpy(),
                "horizon": panel["horizon"].to_numpy(),
                "metric": suffix,
                "quantile_level": level,
                "forecast_value": forecast_values,
            }))
    if not frames:
        return pd.DataFrame(
            columns=["ticker", "fiscal_year", "horizon", "metric", "quantile_level",
                     "forecast_value"]
        )
    return pd.concat(frames, ignore_index=True)


# ── terminal-year blend (years 6-10), issue #332 ────────────────────────────────────────────
#
# Years 1-5 come straight from predict_forecast (the explicit ML forecast); years 6-10 instead
# converge each scenario's own growth rate toward the DCF model's existing terminal-growth
# assumption (valuation_assumptions.json's bull/mid/bear "growth_terminal") — the "same
# two-stage spirit as the existing DCF" issue #332 asks for, mirroring valuation.py's
# dcf_value() (explicit stage, then a terminal regime), but producing a full per-year VALUE
# PATH rather than one lump Gordon terminal value, since the Forecasting fan chart needs a
# point at every FY, not a single terminal number.
def terminal_growth_for_quantile(
    quantile_level: float, bear_terminal: float, mid_terminal: float, bull_terminal: float,
) -> float:
    """Maps a quantile level to a terminal growth rate, anchored at ``q=0.10 -> bear_terminal``,
    ``q=0.50 -> mid_terminal``, ``q=0.90 -> bull_terminal`` (the DCF model's own 3-scenario
    terminal-growth assumptions) via linear interpolation across two segments:
    ``[0.10, 0.50]`` (bear -> mid) and ``[0.50, 0.90]`` (mid -> bull).

    Resolves a genuinely new design question issue #332 raised: the Forecasting feature has 5
    quantile scenarios (Bear/Low Bear/Crab/Low Bull/Bull), but the existing DCF model only has
    3 terminal-growth profiles. At ``q=0.10/0.50/0.90`` this returns EXACTLY bear/mid/bull; at
    the mockup's Low Bear (``q=0.25``) and Low Bull (``q=0.75``) it's a principled
    interpolation tied to the actual quantile level (e.g. 0.25 is 37.5% of the way from 0.10 to
    0.50), not an arbitrary halfway split.

    A ``quantile_level`` outside ``[0.10, 0.90]`` extrapolates linearly along whichever segment
    it's closest to — this module's own :data:`QUANTILE_LEVELS` never go outside that range, so
    this is just "don't silently clamp", not a case this feature actually exercises.
    """
    if quantile_level <= 0.50:
        weight = (quantile_level - 0.10) / (0.50 - 0.10)
        return bear_terminal + weight * (mid_terminal - bear_terminal)
    weight = (quantile_level - 0.50) / (0.90 - 0.50)
    return mid_terminal + weight * (bull_terminal - mid_terminal)


def blend_terminal_years(
    value_at_horizon5: Number,
    exit_growth_rate: Number,
    terminal_growth: Number,
    *,
    terminal_years: int = 5,
) -> list[float] | None:
    """Years 6-10's (or however many ``terminal_years``) absolute-value forecast path: linearly
    blends the ANNUAL growth rate from ``exit_growth_rate`` (year 5's own realized CAGR — see
    :func:`fundamentals_pipeline.valuation.eps_cagr`, reused directly since it's already generic
    CAGR math despite the EPS-specific name) down/up to ``terminal_growth`` (see
    :func:`terminal_growth_for_quantile`) by the final terminal year, compounding year over
    year. Returns the list of ``terminal_years`` absolute values (years 6, 7, ..., 6+terminal_years-1).

    ``value_at_horizon5 <= 0`` floors EVERY terminal year to ``0.0`` — same zero-floor
    composition as :func:`reconstruct_forecast_value`: a scenario already in loss territory at
    year 5 has no real growth-rate signal left to blend from, so extending it further would be
    fabricating, not forecasting. ``None`` only when an input is itself missing (nothing to
    blend from).
    """
    if (
        _is_missing(value_at_horizon5)
        or _is_missing(exit_growth_rate)
        or _is_missing(terminal_growth)
    ):
        return None
    if value_at_horizon5 <= 0:
        return [0.0] * terminal_years
    values: list[float] = []
    current = float(value_at_horizon5)
    for step in range(1, terminal_years + 1):
        rate = exit_growth_rate + (terminal_growth - exit_growth_rate) * (step / terminal_years)
        current = current * (1 + rate)
        values.append(current)
    return values


def blend_terminal_years_from_values(
    value_from: Number,
    value_at_horizon5: Number,
    terminal_growth: Number,
    *,
    terminal_years: int = 5,
    explicit_years: int = 5,
) -> list[float] | None:
    """Convenience wrapper around :func:`blend_terminal_years`: computes the year-0-to-year-5
    exit CAGR from ``value_from``/``value_at_horizon5`` via
    :func:`fundamentals_pipeline.valuation.eps_cagr` (no separate CAGR formula), then delegates
    to :func:`blend_terminal_years` — the call shape ``24__forecasting.py`` actually uses (it
    has ``value_from`` and ``predict_forecast``'s own reconstructed year-5 value on hand, not a
    pre-computed exit rate).

    Order of checks matters here: ``value_at_horizon5 <= 0`` must floor to
    ``[0.0] * terminal_years`` (matching :func:`blend_terminal_years`'s own zero-floor rule)
    even though ``eps_cagr`` would also return ``None`` for that input — checked BEFORE calling
    ``eps_cagr``, so that floor is never misread as "missing, return None". Only a non-positive
    ``value_from`` (with a genuinely positive ``value_at_horizon5``) makes ``eps_cagr`` return
    ``None`` here, in which case this returns ``None`` too — the same "no meaningful growth
    rate from a non-positive base" convention :func:`log_growth` already uses.
    """
    if _is_missing(value_at_horizon5) or _is_missing(terminal_growth):
        return None
    if value_at_horizon5 <= 0:
        return [0.0] * terminal_years
    exit_growth_rate = eps_cagr(value_from, value_at_horizon5, explicit_years)
    if exit_growth_rate is None:
        return None
    return blend_terminal_years(
        value_at_horizon5, exit_growth_rate, terminal_growth, terminal_years=terminal_years,
    )
