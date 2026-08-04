"""Artifact-name and meta-JSON contract constants — the pandas-FREE half of the export↔app
schema contract, split out of :mod:`schemas` so a caller that only needs these (e.g.
``fundamentals_screener``'s request-path DuckDB connection setup, which just needs to know
which parquet files exist, never the dtype-level validation) doesn't pay pandas's ~400ms
cold-import cost on every request under a no-persistent-process (CGI) deployment.

:mod:`schemas` re-exports everything here for full backward compatibility — existing
``from fundamentals_pipeline import schemas; schemas.ARTIFACT_NAMES`` (or
``schemas.validate_meta``/``schemas.SchemaError``/etc.) call sites are unaffected by this
split. Dtype-level validation (``dtype_family``/``validate_artifact``/``assert_artifact``),
which genuinely needs ``pandas.DataFrame``/``Series``, stays in :mod:`schemas`.
"""

from __future__ import annotations


class SchemaError(Exception):
    """Raised when an artifact (parquet frame or meta dict) violates the contract."""


# ── per-artifact specs: column → set of acceptable dtype families ────────────────
# `period_end` / `date` accept {datetime, string}: date32 reads as object at export time
# and becomes datetime64 only after lib/data.py normalizes it.
_DATA_SPEC: dict[str, set[str]] = {
    "ticker": {"string"},
    "period_type": {"string"},
    "period_end": {"datetime", "string"},
    "fiscal_year": {"numeric"},
    "stmt": {"string"},
    "section": {"string"},
    "group": {"string"},
    "concept": {"string"},
    "display_name": {"string"},
    "sort_order": {"numeric"},
    "value": {"numeric"},
}

_METRICS_SPEC: dict[str, set[str]] = {
    "ticker": {"string"},
    "period_type": {"string"},
    "period_end": {"datetime", "string"},
    "fiscal_year": {"numeric"},
    "category": {"string"},
    "subcategory": {"string"},
    "metric": {"string"},
    "unit": {"string"},
    "sort_order": {"numeric"},
    "value": {"numeric"},
}

_PRICES_SPEC: dict[str, set[str]] = {
    "ticker": {"string"},
    "date": {"datetime", "string"},
    "close": {"numeric"},
    "adj_close": {"numeric"},
}

# Full daily FX-rate history (not just the latest value) — so a future frontend "view in USD"
# toggle can convert a HISTORICAL figure using the rate from that figure's own period_end,
# never today's spot rate (see fundamentals_pipeline/fx.py's date-anchoring rule). `pair` is
# the yfinance ticker (e.g. "CADUSD=X"); `base`/`quote` are the same pair decomposed for a
# direct lookup without re-parsing the string.
_FX_SPEC: dict[str, set[str]] = {
    "base": {"string"},
    "quote": {"string"},
    "pair": {"string"},
    "date": {"datetime", "string"},
    "rate": {"numeric"},
}

# Backtest equity-curve series (one row per archetype × fiscal_year). benchmark_* are NULL
# (all-NaN float → still 'numeric') when the benchmark ticker is absent from the price store.
_BACKTEST_SPEC: dict[str, set[str]] = {
    "archetype": {"string"},
    "fiscal_year": {"numeric"},
    "portfolio_return": {"numeric"},
    "benchmark_return": {"numeric"},
    "portfolio_value": {"numeric"},
    "benchmark_value": {"numeric"},
    "n_holdings": {"numeric"},
}

# Real SEC filing list (10-K/10-Q only — see 15__fetch_sec_filings.py), one row per filing.
# `filing_date`/`report_date` accept {datetime, string} like every other date-like column
# here — SEC's submissions API returns plain ISO date strings, and report_date is sometimes
# blank for certain forms (kept nullable, not coalesced). Both frontends' company page
# Filings tab reads this directly — neither needs its own SEC credentials or live fetch
# anymore (see the pipeline stage's own docstring for the incident that motivated this).
_FILINGS_SPEC: dict[str, set[str]] = {
    "ticker": {"string"},
    "form": {"string"},
    "filing_date": {"datetime", "string"},
    "report_date": {"datetime", "string"},
    "description": {"string"},
    "url": {"string"},
}

# 10-year cross-sectional ML scenario forecasts (issue #332's 24__forecasting.py): one row
# per (ticker, base fiscal_year, horizon 1-10, target metric, quantile level). `horizon` is
# years ahead of `fiscal_year` (the ticker's own latest reported FY at the time the pipeline
# ran); years 1-5 come from the LightGBM quantile regressors, years 6-10 from the
# terminal-growth blend (see forecasting.py's own module docstring). `forecast_value` is an
# absolute dollar value, never a growth rate — already reconstructed/floored (see
# reconstruct_forecast_value) so consumers never need to re-derive it from a raw prediction.
_FORECAST_SPEC: dict[str, set[str]] = {
    "ticker": {"string"},
    "fiscal_year": {"numeric"},
    "horizon": {"numeric"},
    "metric": {"string"},
    "quantile_level": {"numeric"},
    "forecast_value": {"numeric"},
}

ARTIFACTS: dict[str, dict[str, set[str]]] = {
    "dashboard_data": _DATA_SPEC,
    "dashboard_metrics": _METRICS_SPEC,
    "dashboard_prices": _PRICES_SPEC,
    "dashboard_backtest": _BACKTEST_SPEC,
    "dashboard_fx": _FX_SPEC,
    "dashboard_filings": _FILINGS_SPEC,
    "dashboard_forecast": _FORECAST_SPEC,
}
ARTIFACT_NAMES = tuple(ARTIFACTS)

# Meta JSON: top-level keys required; per-ticker records must carry at least these.
# Sub-keys of `retention` / `row_counts` are intentionally NOT pinned (they have evolved
# across schema versions — e.g. `quarters` vs the older `quarterly_periods`).
META_REQUIRED_KEYS = ("schema_version", "build_timestamp", "tickers", "fy_ranges", "row_counts", "retention")
TICKER_REQUIRED_KEYS = ("ticker", "company")


def required_columns(name: str) -> tuple[str, ...]:
    """Column names the named artifact must carry."""
    if name not in ARTIFACTS:
        raise ValueError(f"unknown artifact {name!r}; expected one of {ARTIFACT_NAMES}")
    return tuple(ARTIFACTS[name])


def validate_meta(meta: dict) -> list[str]:
    """Return a list of contract violations for the meta JSON dict (empty ⇒ valid)."""
    violations: list[str] = []
    if not isinstance(meta, dict):
        return [f"dashboard_meta: expected a dict, got {type(meta).__name__}"]
    for key in META_REQUIRED_KEYS:
        if key not in meta:
            violations.append(f"dashboard_meta: missing required key '{key}'")
    tickers = meta.get("tickers")
    if "tickers" in meta and not isinstance(tickers, list):
        violations.append(f"dashboard_meta: 'tickers' must be a list, got {type(tickers).__name__}")
    elif isinstance(tickers, list):
        for i, rec in enumerate(tickers):
            if not isinstance(rec, dict):
                violations.append(f"dashboard_meta: tickers[{i}] must be a dict, got {type(rec).__name__}")
                break
            missing = [k for k in TICKER_REQUIRED_KEYS if k not in rec]
            if missing:
                violations.append(f"dashboard_meta: tickers[{i}] missing key(s) {missing}")
                break
    return violations


def assert_meta(meta: dict) -> None:
    """Raise ``SchemaError`` if the meta dict violates the contract (else return None)."""
    violations = validate_meta(meta)
    if violations:
        raise SchemaError("dashboard_meta failed schema validation:\n  - " + "\n  - ".join(violations))
