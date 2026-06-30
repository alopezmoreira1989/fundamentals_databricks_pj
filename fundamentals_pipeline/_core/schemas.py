"""Schema contract on the export ↔ Streamlit boundary — the single source of truth.

The publish layer (``50__publish/51__export_dashboard_data.py``) writes three parquet
artifacts plus a meta JSON; the public Streamlit app (``60__frontends/61__streamlit/lib/data.py``)
reads them. This module pins the column set + dtype *family* of each artifact and the
required keys of the meta JSON, so a pipeline change to the export can never silently
ship a shape the app can't read:

  * the export side asserts BEFORE writing parquet → a bad run fails loudly;
  * the load side validates AFTER reading → an incompatible artifact yields a readable
    error (hard for the core data/metrics; soft for prices, which must never block the app).

Import-safe: depends only on ``pandas`` (no Spark, no streamlit). Dtype checks are by
*family* (``numeric`` / ``datetime`` / ``string`` / ``bool``), not exact dtype, because the
same column legitimately reads as ``object`` (raw date32) at export time and as
``datetime64`` / ``category`` after the app normalizes it.
"""

from __future__ import annotations

import pandas as pd
from pandas.api import types as ptypes


class SchemaError(Exception):
    """Raised when an artifact (parquet frame or meta dict) violates the contract."""


# ── dtype families ──────────────────────────────────────────────────────────────
def dtype_family(series: pd.Series) -> str:
    """Map a pandas dtype to a coarse family: bool / numeric / datetime / string / other.

    ``bool`` is checked before ``numeric`` (pandas treats bool as a numeric subtype).
    Categorical and object/string dtypes all collapse to ``string``.
    """
    dt = series.dtype
    if ptypes.is_bool_dtype(dt):
        return "bool"
    if ptypes.is_numeric_dtype(dt):
        return "numeric"
    if ptypes.is_datetime64_any_dtype(dt):
        return "datetime"
    if isinstance(dt, pd.CategoricalDtype) or ptypes.is_object_dtype(dt) or ptypes.is_string_dtype(dt):
        return "string"
    return "other"


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

ARTIFACTS: dict[str, dict[str, set[str]]] = {
    "dashboard_data": _DATA_SPEC,
    "dashboard_metrics": _METRICS_SPEC,
    "dashboard_prices": _PRICES_SPEC,
    "dashboard_backtest": _BACKTEST_SPEC,
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


def validate_artifact(name: str, df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable contract violations for one parquet artifact.

    Empty list ⇒ the frame satisfies the contract. Columns beyond the required set are
    allowed (additive changes don't break the app). Raises ``ValueError`` for an
    unknown artifact name.
    """
    if name not in ARTIFACTS:
        raise ValueError(f"unknown artifact {name!r}; expected one of {ARTIFACT_NAMES}")
    spec = ARTIFACTS[name]
    violations: list[str] = []
    for col, families in spec.items():
        if col not in df.columns:
            violations.append(f"{name}: missing required column '{col}'")
            continue
        fam = dtype_family(df[col])
        if fam not in families:
            violations.append(
                f"{name}: column '{col}' has dtype family '{fam}' (dtype {df[col].dtype}); "
                f"expected one of {sorted(families)}"
            )
    return violations


def assert_artifact(name: str, df: pd.DataFrame) -> None:
    """Raise ``SchemaError`` if the frame violates the contract (else return None)."""
    violations = validate_artifact(name, df)
    if violations:
        raise SchemaError(f"{name} failed schema validation:\n  - " + "\n  - ".join(violations))


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
