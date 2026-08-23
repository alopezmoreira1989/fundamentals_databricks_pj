"""Tests for the units-scale selector's core formatter: `fmt.fmt_value` (a context-aware
`simple_tag` — see its own docstring for why it isn't a second filter argument) plus its private
`_scaled_body` helper.

Pure functions, no Django template engine needed: `fmt_value` takes `context` as its first
positional argument and only ever calls `context.get("value_scale", "auto")` on it, so a plain
dict stands in for a real render Context here.
"""

from __future__ import annotations

from fundamentals_screener.templatetags import fmt

# ── _scaled_body ──────────────────────────────────────────────────────────────────────────────


def test_scaled_body_normal_is_full_comma_grouped_number():
    assert fmt._scaled_body(1_234_567_890.5, "normal") == "1,234,567,890.50"


def test_scaled_body_forced_billions():
    assert fmt._scaled_body(1_234_567_890.0, "B") == "1.23B"


def test_scaled_body_forced_millions():
    assert fmt._scaled_body(45_300_000.0, "M") == "45.3M"


def test_scaled_body_forced_thousands():
    assert fmt._scaled_body(6_100.0, "K") == "6.1K"


def test_scaled_body_forced_scale_applies_even_to_a_small_value():
    # Forcing Billions on a $500 figure is a deliberate, self-selected choice -- not a bug.
    assert fmt._scaled_body(500.0, "B") == "0.00B"


def test_scaled_body_negative_value_keeps_sign_under_forced_scale():
    assert fmt._scaled_body(-45_300_000.0, "M") == "-45.3M"


def test_scaled_body_auto_matches_compact_money_exactly():
    for v in (1_234_567_890.0, 45_300_000.0, 6_100.0, 12.5, -391_040_000_000.0):
        assert fmt._scaled_body(v, "auto") == fmt.compact_money(v)


def test_scaled_body_unrecognized_scale_falls_back_to_auto():
    assert fmt._scaled_body(1_234_567_890.0, "bogus") == fmt.compact_money(1_234_567_890.0)


# ── fmt_value ─────────────────────────────────────────────────────────────────────────────────


def test_fmt_value_none_is_em_dash_regardless_of_scale():
    assert fmt.fmt_value({"value_scale": "B"}, None, "usd") == "—"


def test_fmt_value_percent_is_never_touched_by_scale():
    for scale in ("auto", "normal", "B", "M", "K"):
        assert fmt.fmt_value({"value_scale": scale}, -43.789, "percent") == "-43.8%"


def test_fmt_value_ratio_or_plain_unit_is_never_touched_by_scale():
    # This is the "no ratios" requirement -- a P/E, F-Score, etc. must render identically no
    # matter what scale is selected.
    for scale in ("auto", "normal", "B", "M", "K"):
        assert fmt.fmt_value({"value_scale": scale}, 15.234, "ratio") == "15.23"
        assert fmt.fmt_value({"value_scale": scale}, 7.0, None) == "7.00"


def test_fmt_value_usd_defaults_to_auto_when_no_value_scale_in_context():
    # No "value_scale" key at all (a template that never set it) -- must default to "auto",
    # matching context.get(..., "auto")'s own fallback, never raise.
    assert fmt.fmt_value({}, 1_234_567_890.0, "usd") == "$1.23B"


def test_fmt_value_usd_auto_matches_compact_money_with_dollar_prefix():
    assert fmt.fmt_value({"value_scale": "auto"}, 1_234_567_890.0, "usd") == "$1.23B"


def test_fmt_value_usd_normal_is_full_number_with_dollar_prefix():
    assert fmt.fmt_value({"value_scale": "normal"}, 1_234_567_890.0, "usd") == "$1,234,567,890.00"


def test_fmt_value_usd_forced_millions():
    assert fmt.fmt_value({"value_scale": "M"}, 45_300_000.0, "usd") == "$45.3M"


def test_fmt_value_native_currency_code_gets_a_badge_not_a_dollar_sign():
    out = str(fmt.fmt_value({"value_scale": "auto"}, 500_000_000.0, "cad"))
    assert "$" not in out
    assert "500.0M" in out
    assert "ccy-badge" in out and "CAD" in out


def test_fmt_value_native_currency_code_forced_billions():
    out = str(fmt.fmt_value({"value_scale": "B"}, 1_500_000_000.0, "cad"))
    assert "1.50B" in out
    assert "CAD" in out


def test_fmt_value_usd_code_uppercase_still_gets_dollar_sign_not_a_badge():
    # "USD"/"usd" -- the 3-letter-code branch must not double up with the plain usd branch.
    out = str(fmt.fmt_value({"value_scale": "auto"}, 100.0, "USD"))
    assert out == "$100.00"
