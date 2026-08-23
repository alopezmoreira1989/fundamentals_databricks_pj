"""Template-render regression test for the General Screener's Columns-disclosure auto-open bug
and the Currency selector's masthead layout.

Renders `_screen_main.html` directly (Django's template engine, no DuckDB/network), with a
context shaped exactly like `views.screen()` builds it. Confirmed root cause of the reported
regression (2026-08-23): the Columns `<details open>` condition used to key off `col_explicit`
(true after ANY form field changes, since `col_on` is an unconditional hidden field in the same
form) rather than whether the user's actual column selection differs from the defaults — so the
panel kept reopening on a reload/bookmark/mode-switch even when the user only touched Sector,
Currency, etc. Fixed via `cols_customized` (see views.py); this test locks that behavior in.
"""

from __future__ import annotations

import pytest
from django.template.loader import render_to_string
from fundamentals_screener.dtos import ScreenTableRow

pytestmark = pytest.mark.django_db


def _base_context(**overrides) -> dict:
    ctx = {
        "mode": "general",
        "shared_qs": "",
        "metrics": ("Market Cap (Live)", "P/E (TTM, live)", "Current Ratio", "Debt / Equity", "Free Cash Flow"),
        "sectors": ("Industrials",),
        "countries": ("United States",),
        "markets": ("US", "CA"),
        "industries": (),
        "q": "",
        "sector": "",
        "index": "",
        "country": "",
        "market": "",
        "industry": "",
        "show_currency_selector": True,
        "target_currencies": ("CAD", "USD"),
        "selected_currency": "",
        "selected_scale": "auto",
        "value_scale": "auto",
        "cols": ["Market Cap (Live)", "P/E (TTM, live)", "Current Ratio", "Debt / Equity"],
        "cols_customized": False,
        "sort_key": "ticker",
        "sort_dir": "asc",
        "filter_rows": [{"metric": "", "min": "", "max": ""}],
        "has_active_filters": False,
        "active_filter_count": 0,
        "optional_desc_columns": (("sector", "Sector"), ("industry", "Industry"),
                                   ("country", "Country"), ("market", "Market")),
        "visible_desc": ["sector", "industry", "country", "market"],
        "desc_explicit": False,
        "desc_form_hidden": [],
        "desc_headers": [],
        "metric_headers": [],
        "rows": (),
        "total": 0,
        "error": None,
        "page": 1,
        "num_pages": 1,
        "has_prev": False,
        "has_next": False,
        "page_range": range(1, 2),
        "querystring": "",
    }
    ctx.update(overrides)
    return ctx


def _render(**overrides) -> str:
    return render_to_string("fundamentals_screener/_screen_main.html", _base_context(**overrides))


def test_renders_without_error_and_leaks_no_template_syntax():
    html = _render()
    assert "{%" not in html and "{#" not in html


def test_columns_panel_closed_by_default_even_with_default_columns_selected():
    # The exact reported regression: default columns are non-empty, but the user never
    # customized them -- the panel must NOT auto-open.
    html = _render(cols_customized=False)
    details_block = html.split('name="scr_toggle"')[1].split("</details>")[0]
    assert " open" not in details_block.split(">")[0]


def test_columns_panel_open_when_selection_differs_from_defaults():
    html = _render(cols_customized=True)
    details_block = html.split('name="scr_toggle"')[1].split("</details>")[0]
    assert " open" in details_block.split(">")[0]


def test_currency_select_has_a_visible_currency_label():
    html = _render(show_currency_selector=True, target_currencies=("CAD", "USD"))
    assert '<label for="scr-ccy" class="form-label">Currency</label>' in html
    assert 'id="scr-ccy"' in html
    assert ">Native<" in html  # the default option, not the old "Native (no conversion)" text
    assert "Native (no conversion)" not in html


def test_currency_select_hidden_when_only_one_currency():
    html = _render(show_currency_selector=False, target_currencies=())
    assert 'id="scr-ccy"' not in html


def test_currency_select_marks_the_chosen_currency():
    html = _render(show_currency_selector=True, target_currencies=("CAD", "USD"), selected_currency="USD")
    assert '<option value="USD" selected>USD</option>' in html


def test_scale_select_always_renders_with_a_visible_label():
    # Unlike Currency, Scale never depends on FX data availability -- it must render regardless
    # of show_currency_selector.
    html = _render(show_currency_selector=False, target_currencies=())
    assert '<label for="scr-scale" class="form-label">Scale</label>' in html
    assert 'id="scr-scale"' in html
    assert '<option value="">Auto</option>' in html


def test_scale_select_marks_the_chosen_scale():
    html = _render(selected_scale="M")
    assert '<option value="M" selected>Millions</option>' in html


def test_scale_select_auto_is_the_implicit_default_no_option_marked_selected_but_auto():
    html = _render(selected_scale="auto")
    block = html.split('id="scr-scale"')[1].split("</select>")[0]
    assert "selected" not in block  # "Auto" wins by being the first, unmarked option


# ── Default rendering: General Screener cells auto-compact, matching the confirmed
#    default-behavior decision (large $ figures should be readable out of the box) ────────────

def _row_with_market_cap(value: float, unit: str = "usd") -> dict:
    row = ScreenTableRow(
        ticker="BIGCO", name="Big Co", sector="Industrials", industry="Machinery",
        country="United States", market="US", has_logo=False,
        values={"Market Cap (Live)": value}, units={"Market Cap (Live)": unit},
    )
    return {"row": row, "desc_cells": [row.sector, row.industry, row.country, row.market],
            "cells": [(value, unit)]}


def test_default_scale_auto_compacts_a_large_market_cap_cell():
    html = _render(rows=[_row_with_market_cap(1_234_567_890.0)], total=1)
    assert "1.23B" in html
    assert "1,234,567,890.00" not in html


def test_forced_normal_scale_renders_the_full_raw_number():
    html = _render(
        value_scale="normal", selected_scale="normal",
        rows=[_row_with_market_cap(1_234_567_890.0)], total=1,
    )
    assert "1,234,567,890.00" in html
    assert "1.23B" not in html
