"""Tests for the Investor Presets read model (CompanyListingRepository.preset_screen) and the
service layer built on top of it (services.get_preset_screen/get_preset_definition).

Self-contained: an in-memory DuckDB connection with a hand-written dashboard_metrics table,
injected into CompanyListingRepository(connection=...), plus a monkeypatched
repositories.company_listing.load_meta (not data_source.get_meta — same import-binding caveat
as test_net_net.py). No Django settings, no fixture files.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.repositories import company_listing as company_listing_module
from fundamentals_screener.repositories.company_listing import CompanyListingRepository

from fundamentals_screener import services

_META = {
    "tickers": [
        {"ticker": t, "company": f"{t} Corp", "sector": "Industrials", "industry": "Machinery",
         "country": "United States", "market": "US"}
        for t in (
            "GRAHAM1", "GRAHAM2", "GRAHAMFAIL",
            "GRAHAMEARN_SUSTAINED", "GRAHAMEARN_ONELOSS", "GRAHAMEARN_SHORTHISTORY",
            "GRAHAMDIV_SUSTAINED", "GRAHAMDIV_FLOOR3", "GRAHAMDIV_BELOWFLOOR",
            "GRAHAMDIV_STOPPED", "GRAHAMDIV_GAP",
            "GRAHAMEPS_SUSTAINED", "GRAHAMEPS_TOOLOW", "GRAHAMEPS_MISSING_BASE",
            "GRAHAMEPS_NEGATIVE_BASE", "GRAHAM_MULTIYEAR_FAILSOTHER",
            "GRAHAM_LEVEL_MODERATE_ONLY", "GRAHAM_STRICT_EPS_ENDPOINT_ONLY",
            "BUFF1", "BUFFNULLPASS", "BUFFNULLBUTFAILS", "BUFFTOOLOWMOS",
            "BUFFROE_SUSTAINED", "BUFFROE_ONEDIP", "BUFFROE_SHORTHISTORY", "BUFFROE_FAILSOTHER",
            "BUFFETT_LEVEL_MODERATE_ONLY",
            "LYNCH1", "LYNCHFAIL", "LYNCHPEGFAIL", "LYNCH_LEVEL_MODERATE_ONLY",
        )
    ]
}

# 5 years of ROE % >= 15 (FY2020-2024) — the "sustained ROE" criterion's passing shape, added to
# every existing Buffett fixture ticker so each test still isolates exactly the ONE criterion it
# was written to check, rather than incidentally failing for a second, unintended reason (no ROE
# data at all) once the multi-year filter applies to every `preset="buffett"` call.
_SUSTAINED_ROE_ROWS = [
    (ticker, "ROE %", "percent", year, 18.0, "FY")
    for ticker in ("BUFF1", "BUFFNULLPASS", "BUFFNULLBUTFAILS", "BUFFTOOLOWMOS")
    for year in (2020, 2021, 2022, 2023, 2024)
]

# Graham's 3 new multi-year criteria (issue #278) — dedicated tickers, one group per criterion,
# each holding the OTHER two new criteria (plus the existing CR/P-E/P-B test) at a passing value
# so a test targeting one criterion isn't incidentally failing on a different, untested one —
# same isolation rationale as _SUSTAINED_ROE_ROWS above.
_GRAHAM_NEW_TICKERS = (
    "GRAHAMEARN_SUSTAINED", "GRAHAMEARN_ONELOSS", "GRAHAMEARN_SHORTHISTORY",
    "GRAHAMDIV_SUSTAINED", "GRAHAMDIV_FLOOR3", "GRAHAMDIV_BELOWFLOOR",
    "GRAHAMDIV_STOPPED", "GRAHAMDIV_GAP",
    "GRAHAMEPS_SUSTAINED", "GRAHAMEPS_TOOLOW", "GRAHAMEPS_MISSING_BASE", "GRAHAMEPS_NEGATIVE_BASE",
)

# ticker, metric, unit, fiscal_year, value, period_type
_GRAHAM_MULTIYEAR_ROWS = [
    # Passing latest-FY (Current Ratio/P/E/P/B) rows for every new dedicated ticker except
    # GRAHAM_MULTIYEAR_FAILSOTHER, which deliberately fails this branch (below) to prove the new
    # multi-year checks AND with, not replace, the existing predicate.
    *[
        row
        for ticker in _GRAHAM_NEW_TICKERS
        for row in (
            (ticker, "Current Ratio", "ratio", 2024, 2.5, "FY"),
            (ticker, "P/E", "ratio", 2024, 10.0, "FY"),
            (ticker, "P/B", "ratio", 2024, 1.2, "FY"),
        )
    ],
    ("GRAHAM_MULTIYEAR_FAILSOTHER", "Current Ratio", "ratio", 2024, 1.0, "FY"),
    ("GRAHAM_MULTIYEAR_FAILSOTHER", "P/E", "ratio", 2024, 10.0, "FY"),
    ("GRAHAM_MULTIYEAR_FAILSOTHER", "P/B", "ratio", 2024, 1.2, "FY"),

    # Dividend Yield % passing baseline (5 contiguous current years) — added to GRAHAM1/GRAHAM2/
    # GRAHAMFAIL (pre-existing, now also subject to Graham's multi-year AND) plus every new
    # ticker whose own test targets a different (earnings/EPS growth) criterion.
    *[
        (ticker, "Dividend Yield %", "percent", year, 2.0, "FY")
        for ticker in (
            "GRAHAM1", "GRAHAM2", "GRAHAMFAIL",
            "GRAHAMEARN_SUSTAINED", "GRAHAMEARN_ONELOSS", "GRAHAMEARN_SHORTHISTORY",
            "GRAHAMEPS_SUSTAINED", "GRAHAMEPS_TOOLOW", "GRAHAMEPS_MISSING_BASE",
            "GRAHAMEPS_NEGATIVE_BASE", "GRAHAM_MULTIYEAR_FAILSOTHER",
        )
        for year in (2020, 2021, 2022, 2023, 2024)
    ],

    # GRAHAMDIV_SUSTAINED: 5 contiguous current years — passes.
    ("GRAHAMDIV_SUSTAINED", "Dividend Yield %", "percent", 2020, 2.0, "FY"),
    ("GRAHAMDIV_SUSTAINED", "Dividend Yield %", "percent", 2021, 2.0, "FY"),
    ("GRAHAMDIV_SUSTAINED", "Dividend Yield %", "percent", 2022, 2.0, "FY"),
    ("GRAHAMDIV_SUSTAINED", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAMDIV_SUSTAINED", "Dividend Yield %", "percent", 2024, 2.0, "FY"),

    # GRAHAMDIV_FLOOR3: only 3 contiguous current years — passes via the tolerance floor
    # (min_years=3), the Phase-0-flagged deliberate exception to the full 5-year window.
    ("GRAHAMDIV_FLOOR3", "Dividend Yield %", "percent", 2022, 2.0, "FY"),
    ("GRAHAMDIV_FLOOR3", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAMDIV_FLOOR3", "Dividend Yield %", "percent", 2024, 2.0, "FY"),

    # GRAHAMDIV_BELOWFLOOR: only 2 contiguous current years — below the floor, fails.
    ("GRAHAMDIV_BELOWFLOOR", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAMDIV_BELOWFLOOR", "Dividend Yield %", "percent", 2024, 2.0, "FY"),

    # GRAHAMDIV_STOPPED: 5 real years of history, but the most recent 2 (2023-2024, relative to
    # this ticker's own latest reported FY of 2024 from its Current Ratio/P-E/P-B rows) are
    # missing — dividends stopped. Must fail: the recency gate, not just "5 non-null rows
    # somewhere in the past".
    ("GRAHAMDIV_STOPPED", "Dividend Yield %", "percent", 2018, 2.0, "FY"),
    ("GRAHAMDIV_STOPPED", "Dividend Yield %", "percent", 2019, 2.0, "FY"),
    ("GRAHAMDIV_STOPPED", "Dividend Yield %", "percent", 2020, 2.0, "FY"),
    ("GRAHAMDIV_STOPPED", "Dividend Yield %", "percent", 2021, 2.0, "FY"),
    ("GRAHAMDIV_STOPPED", "Dividend Yield %", "percent", 2022, 2.0, "FY"),

    # GRAHAMDIV_GAP: a gap in the middle (2022 missing) of an otherwise 5-year record — the
    # contiguous run counting back from 2024 is only {2024, 2023} = length 2, below the floor;
    # proves a gap truncates the run rather than a plain non-null COUNT masking it.
    ("GRAHAMDIV_GAP", "Dividend Yield %", "percent", 2020, 2.0, "FY"),
    ("GRAHAMDIV_GAP", "Dividend Yield %", "percent", 2021, 2.0, "FY"),
    ("GRAHAMDIV_GAP", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAMDIV_GAP", "Dividend Yield %", "percent", 2024, 2.0, "FY"),
]

# ticker, metric, unit, fiscal_year, value, period_type
_METRIC_ROWS = [
    # GRAHAM1: passes outright — CR>=2, P/E<=15, and P/B<=1.5 via the direct branch.
    ("GRAHAM1", "Current Ratio", "ratio", 2024, 2.5, "FY"),
    ("GRAHAM1", "P/E", "ratio", 2024, 10.0, "FY"),
    ("GRAHAM1", "P/B", "ratio", 2024, 1.2, "FY"),

    # GRAHAM2: fails the direct P/B test (2.0 > 1.5) but passes via P/E * P/B = 10 * 2.0 = 20 <= 22.5.
    ("GRAHAM2", "Current Ratio", "ratio", 2024, 3.0, "FY"),
    ("GRAHAM2", "P/E", "ratio", 2024, 10.0, "FY"),
    ("GRAHAM2", "P/B", "ratio", 2024, 2.0, "FY"),

    # GRAHAMFAIL: fails both the direct P/B test and the product test at every conservatism
    # level (P/E=25 exceeds even Relaxed's 20 P/E cap; P/B=5.0 and P/E*P/B=125 exceed even
    # Relaxed's loosest P/B/product caps of 2.5/30) — a robust, level-independent failure.
    ("GRAHAMFAIL", "Current Ratio", "ratio", 2024, 2.5, "FY"),
    ("GRAHAMFAIL", "P/E", "ratio", 2024, 25.0, "FY"),
    ("GRAHAMFAIL", "P/B", "ratio", 2024, 5.0, "FY"),

    # BUFF1: passes all four criteria outright.
    ("BUFF1", "Debt / Equity", "ratio", 2024, 0.3, "FY"),
    ("BUFF1", "Gross Margin %", "percent", 2024, 55.0, "FY"),
    ("BUFF1", "Net Margin %", "percent", 2024, 25.0, "FY"),
    ("BUFF1", "MoS % (Owner Earnings, FY)", "percent", 2024, 30.0, "FY"),

    # BUFFNULLPASS: MoS is NULL (never reported — e.g. a Financials/Real Estate ticker where the
    # Owner Earnings model is sector-gated upstream) but the other three criteria hold — must
    # still pass ("not applicable", not "fails").
    ("BUFFNULLPASS", "Debt / Equity", "ratio", 2024, 0.1, "FY"),
    ("BUFFNULLPASS", "Gross Margin %", "percent", 2024, 60.0, "FY"),
    ("BUFFNULLPASS", "Net Margin %", "percent", 2024, 22.0, "FY"),
    # (no MoS row at all for this ticker)

    # BUFFNULLBUTFAILS: MoS is also NULL, but Debt/Equity fails (0.9 > 0.5) — proves the NULL
    # passthrough is scoped to MoS only, not a blanket exemption from the other hard criteria.
    ("BUFFNULLBUTFAILS", "Debt / Equity", "ratio", 2024, 0.9, "FY"),
    ("BUFFNULLBUTFAILS", "Gross Margin %", "percent", 2024, 60.0, "FY"),
    ("BUFFNULLBUTFAILS", "Net Margin %", "percent", 2024, 22.0, "FY"),

    # BUFFTOOLOWMOS: MoS is present but below the 25% floor — must fail (NULL passes, a real
    # low value does not).
    ("BUFFTOOLOWMOS", "Debt / Equity", "ratio", 2024, 0.1, "FY"),
    ("BUFFTOOLOWMOS", "Gross Margin %", "percent", 2024, 60.0, "FY"),
    ("BUFFTOOLOWMOS", "Net Margin %", "percent", 2024, 22.0, "FY"),
    ("BUFFTOOLOWMOS", "MoS % (Owner Earnings, FY)", "percent", 2024, 5.0, "FY"),

    # LYNCH1: passes all criteria, including PEG < 1 (issue #280).
    ("LYNCH1", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("LYNCH1", "Current Ratio", "ratio", 2024, 1.5, "FY"),
    ("LYNCH1", "ROE %", "percent", 2024, 20.0, "FY"),
    ("LYNCH1", "EPS CAGR (5Y) %", "percent", 2024, 20.0, "FY"),
    ("LYNCH1", "PEG", "ratio", 2024, 0.75, "FY"),

    # LYNCHFAIL: ROE too low — PEG/EPS CAGR are given passing values too, so this fails for
    # exactly the one reason the test targets (ROE), not incidentally for a missing PEG.
    ("LYNCHFAIL", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("LYNCHFAIL", "Current Ratio", "ratio", 2024, 1.5, "FY"),
    ("LYNCHFAIL", "ROE %", "percent", 2024, 5.0, "FY"),
    ("LYNCHFAIL", "EPS CAGR (5Y) %", "percent", 2024, 20.0, "FY"),
    ("LYNCHFAIL", "PEG", "ratio", 2024, 0.75, "FY"),

    # LYNCHPEGFAIL: passes Debt/Equity, Current Ratio, and ROE, but PEG >= 1 — proves PEG
    # composes with the rest of Lynch's predicate rather than being ignored.
    ("LYNCHPEGFAIL", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("LYNCHPEGFAIL", "Current Ratio", "ratio", 2024, 1.5, "FY"),
    ("LYNCHPEGFAIL", "ROE %", "percent", 2024, 20.0, "FY"),
    ("LYNCHPEGFAIL", "EPS CAGR (5Y) %", "percent", 2024, 20.0, "FY"),
    ("LYNCHPEGFAIL", "PEG", "ratio", 2024, 1.5, "FY"),

    # BUFFROE_SUSTAINED: passes the other 3 latest-FY criteria AND has ROE % >= 15 in every one
    # of the last 5 fiscal years — the "sustained" criterion's passing shape.
    ("BUFFROE_SUSTAINED", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("BUFFROE_SUSTAINED", "Gross Margin %", "percent", 2024, 50.0, "FY"),
    ("BUFFROE_SUSTAINED", "Net Margin %", "percent", 2024, 25.0, "FY"),
    ("BUFFROE_SUSTAINED", "ROE %", "percent", 2020, 16.0, "FY"),
    ("BUFFROE_SUSTAINED", "ROE %", "percent", 2021, 17.0, "FY"),
    ("BUFFROE_SUSTAINED", "ROE %", "percent", 2022, 18.0, "FY"),
    ("BUFFROE_SUSTAINED", "ROE %", "percent", 2023, 19.0, "FY"),
    ("BUFFROE_SUSTAINED", "ROE %", "percent", 2024, 20.0, "FY"),

    # BUFFROE_ONEDIP: same other 3 criteria, but ROE % dips below 15 in one of the last 5 years
    # (2022) — must fail: "every year", not "most years".
    ("BUFFROE_ONEDIP", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("BUFFROE_ONEDIP", "Gross Margin %", "percent", 2024, 50.0, "FY"),
    ("BUFFROE_ONEDIP", "Net Margin %", "percent", 2024, 25.0, "FY"),
    ("BUFFROE_ONEDIP", "ROE %", "percent", 2020, 16.0, "FY"),
    ("BUFFROE_ONEDIP", "ROE %", "percent", 2021, 17.0, "FY"),
    ("BUFFROE_ONEDIP", "ROE %", "percent", 2022, 10.0, "FY"),
    ("BUFFROE_ONEDIP", "ROE %", "percent", 2023, 19.0, "FY"),
    ("BUFFROE_ONEDIP", "ROE %", "percent", 2024, 20.0, "FY"),

    # BUFFROE_SHORTHISTORY: same other 3 criteria, ROE % >= 15 in EVERY year on record, but only
    # 3 years of history exist (2022-2024) — must fail: insufficient history is excluded, not
    # silently passed just because nothing on record ever dipped below 15.
    ("BUFFROE_SHORTHISTORY", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("BUFFROE_SHORTHISTORY", "Gross Margin %", "percent", 2024, 50.0, "FY"),
    ("BUFFROE_SHORTHISTORY", "Net Margin %", "percent", 2024, 25.0, "FY"),
    ("BUFFROE_SHORTHISTORY", "ROE %", "percent", 2022, 18.0, "FY"),
    ("BUFFROE_SHORTHISTORY", "ROE %", "percent", 2023, 19.0, "FY"),
    ("BUFFROE_SHORTHISTORY", "ROE %", "percent", 2024, 20.0, "FY"),

    # BUFFROE_FAILSOTHER: 5 sustained years of ROE % >= 15, but a real Debt/Equity failure —
    # must fail overall, proving the multi-year AND-composition with the existing latest-FY
    # predicate, not just the multi-year piece in isolation.
    ("BUFFROE_FAILSOTHER", "Debt / Equity", "ratio", 2024, 0.9, "FY"),
    ("BUFFROE_FAILSOTHER", "Gross Margin %", "percent", 2024, 50.0, "FY"),
    ("BUFFROE_FAILSOTHER", "Net Margin %", "percent", 2024, 25.0, "FY"),
    ("BUFFROE_FAILSOTHER", "ROE %", "percent", 2020, 16.0, "FY"),
    ("BUFFROE_FAILSOTHER", "ROE %", "percent", 2021, 17.0, "FY"),
    ("BUFFROE_FAILSOTHER", "ROE %", "percent", 2022, 18.0, "FY"),
    ("BUFFROE_FAILSOTHER", "ROE %", "percent", 2023, 19.0, "FY"),
    ("BUFFROE_FAILSOTHER", "ROE %", "percent", 2024, 20.0, "FY"),

    # ── Conservatism-level mechanism proofs (Strict/Moderate/Relaxed, issue: presets levels) ──
    # GRAHAM_LEVEL_MODERATE_ONLY: passes Moderate (7y earnings/7y dividend-floor5/7y EPS growth)
    # but fails Strict (needs 10y earnings) — proves `level` changes the actual query bounds,
    # not just the criteria-list display copy.
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Current Ratio", "ratio", 2024, 2.5, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "P/E", "ratio", 2024, 10.0, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "P/B", "ratio", 2024, 1.2, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Dividend Yield %", "percent", 2020, 2.0, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Dividend Yield %", "percent", 2021, 2.0, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Dividend Yield %", "percent", 2022, 2.0, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "Dividend Yield %", "percent", 2024, 2.0, "FY"),

    # GRAHAM_STRICT_EPS_ENDPOINT_ONLY: passes Strict's 10y earnings/10y dividend, and would pass
    # a single-year-endpoint EPS growth test (2014 -> 2024) — but Strict's real, literal test
    # uses 3-year averages at each end, and this ticker has no data in either 3-year window (see
    # eps_growth_avg_tickers's own test coverage below).
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Current Ratio", "ratio", 2024, 2.5, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "P/E", "ratio", 2024, 10.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "P/B", "ratio", 2024, 1.2, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2015, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2016, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2017, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2018, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2019, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2020, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2021, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2022, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2023, 2.0, "FY"),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Dividend Yield %", "percent", 2024, 2.0, "FY"),

    # BUFFETT_LEVEL_MODERATE_ONLY: passes Moderate (5y sustained ROE) but fails Strict (needs
    # 10y) — same level-mechanism proof as GRAHAM_LEVEL_MODERATE_ONLY, for Buffett.
    ("BUFFETT_LEVEL_MODERATE_ONLY", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "Gross Margin %", "percent", 2024, 60.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "Net Margin %", "percent", 2024, 30.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "MoS % (Owner Earnings, FY)", "percent", 2024, 40.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2020, 20.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2021, 20.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2022, 20.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2023, 20.0, "FY"),
    ("BUFFETT_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2024, 20.0, "FY"),

    # LYNCH_LEVEL_MODERATE_ONLY: ROE=17% clears Moderate's >15% bar but not Strict's >20% —
    # Lynch has no multi-year checks, so this is a plain latest-FY threshold difference (the
    # simplest possible "level changes the query" proof for this preset).
    ("LYNCH_LEVEL_MODERATE_ONLY", "Debt / Equity", "ratio", 2024, 0.2, "FY"),
    ("LYNCH_LEVEL_MODERATE_ONLY", "Current Ratio", "ratio", 2024, 2.0, "FY"),
    ("LYNCH_LEVEL_MODERATE_ONLY", "ROE %", "percent", 2024, 17.0, "FY"),
    ("LYNCH_LEVEL_MODERATE_ONLY", "EPS CAGR (5Y) %", "percent", 2024, 20.0, "FY"),
    ("LYNCH_LEVEL_MODERATE_ONLY", "PEG", "ratio", 2024, 0.5, "FY"),
] + _SUSTAINED_ROE_ROWS + _GRAHAM_MULTIYEAR_ROWS

# ticker, concept, period_type, fiscal_year, value — dashboard_data (raw statement concepts),
# distinct from dashboard_metrics above: Net Income ("positive earnings") and EPS Diluted
# ("EPS growth") are raw line items, never published as derived metrics (see
# sustained_concept_tickers/eps_growth_tickers's own docstrings for why).
_DATA_ROWS = [
    # Net Income passing baseline: 5 straight positive FYs (2020-2024) — added to every ticker
    # whose own test targets a different (dividend/EPS-growth) criterion, plus GRAHAM1/GRAHAM2/
    # GRAHAMFAIL (pre-existing, now also subject to the multi-year AND).
    *[
        (ticker, "Net Income", "FY", year, 100.0)
        for ticker in (
            "GRAHAM1", "GRAHAM2", "GRAHAMFAIL",
            "GRAHAMDIV_SUSTAINED", "GRAHAMDIV_FLOOR3", "GRAHAMDIV_BELOWFLOOR",
            "GRAHAMDIV_STOPPED", "GRAHAMDIV_GAP",
            "GRAHAMEPS_SUSTAINED", "GRAHAMEPS_TOOLOW", "GRAHAMEPS_MISSING_BASE",
            "GRAHAMEPS_NEGATIVE_BASE", "GRAHAM_MULTIYEAR_FAILSOTHER",
        )
        for year in (2020, 2021, 2022, 2023, 2024)
    ],

    # EPS Diluted passing baseline: FY2019 1.00 -> FY2024 1.40, a 1.4x (>= the 1.33x bar) growth
    # — added to every ticker whose own test targets a different (earnings/dividend) criterion.
    *[
        (ticker, "EPS Diluted", "FY", year, value)
        for ticker in (
            "GRAHAM1", "GRAHAM2", "GRAHAMFAIL",
            "GRAHAMEARN_SUSTAINED", "GRAHAMEARN_ONELOSS", "GRAHAMEARN_SHORTHISTORY",
            "GRAHAMDIV_SUSTAINED", "GRAHAMDIV_FLOOR3", "GRAHAMDIV_BELOWFLOOR",
            "GRAHAMDIV_STOPPED", "GRAHAMDIV_GAP", "GRAHAM_MULTIYEAR_FAILSOTHER",
        )
        for year, value in ((2019, 1.00), (2024, 1.40))
    ],

    # GRAHAMEARN_SUSTAINED: 5 straight positive years — passes.
    ("GRAHAMEARN_SUSTAINED", "Net Income", "FY", 2020, 100.0),
    ("GRAHAMEARN_SUSTAINED", "Net Income", "FY", 2021, 100.0),
    ("GRAHAMEARN_SUSTAINED", "Net Income", "FY", 2022, 100.0),
    ("GRAHAMEARN_SUSTAINED", "Net Income", "FY", 2023, 100.0),
    ("GRAHAMEARN_SUSTAINED", "Net Income", "FY", 2024, 100.0),

    # GRAHAMEARN_ONELOSS: same 5-year window, one real loss (2022) — must fail: "every year",
    # not "most years" (mirrors BUFFROE_ONEDIP's own rationale).
    ("GRAHAMEARN_ONELOSS", "Net Income", "FY", 2020, 100.0),
    ("GRAHAMEARN_ONELOSS", "Net Income", "FY", 2021, 100.0),
    ("GRAHAMEARN_ONELOSS", "Net Income", "FY", 2022, -50.0),
    ("GRAHAMEARN_ONELOSS", "Net Income", "FY", 2023, 100.0),
    ("GRAHAMEARN_ONELOSS", "Net Income", "FY", 2024, 100.0),

    # GRAHAMEARN_SHORTHISTORY: all-positive but only 3 years on record — must fail: insufficient
    # history isn't tolerated for earnings the way it is for dividends (no floor here, matches
    # Buffett's ROE treatment — mirrors BUFFROE_SHORTHISTORY's own rationale).
    ("GRAHAMEARN_SHORTHISTORY", "Net Income", "FY", 2022, 100.0),
    ("GRAHAMEARN_SHORTHISTORY", "Net Income", "FY", 2023, 100.0),
    ("GRAHAMEARN_SHORTHISTORY", "Net Income", "FY", 2024, 100.0),

    # GRAHAMEPS_SUSTAINED: FY2019 1.00 -> FY2024 1.40, a 1.4x (>= 1.33x) growth — passes.
    ("GRAHAMEPS_SUSTAINED", "EPS Diluted", "FY", 2019, 1.00),
    ("GRAHAMEPS_SUSTAINED", "EPS Diluted", "FY", 2024, 1.40),

    # GRAHAMEPS_TOOLOW: FY2019 1.00 -> FY2024 1.20, only 1.2x growth — below the 1.33x bar, fails.
    ("GRAHAMEPS_TOOLOW", "EPS Diluted", "FY", 2019, 1.00),
    ("GRAHAMEPS_TOOLOW", "EPS Diluted", "FY", 2024, 1.20),

    # GRAHAMEPS_MISSING_BASE: only the latest FY is reported, no FY2019 (5-years-ago) row at
    # all — "no proof, no pass", matches sustained_metric_tickers's own exact-count convention.
    ("GRAHAMEPS_MISSING_BASE", "EPS Diluted", "FY", 2024, 1.40),

    # GRAHAMEPS_NEGATIVE_BASE: FY2019 is a loss (-0.50) recovering to a real FY2024 profit — a
    # turnaround, not "33% growth"; both endpoints must be positive, so this fails.
    ("GRAHAMEPS_NEGATIVE_BASE", "EPS Diluted", "FY", 2019, -0.50),
    ("GRAHAMEPS_NEGATIVE_BASE", "EPS Diluted", "FY", 2024, 1.40),

    # ── Conservatism-level mechanism proofs (see the matching _METRIC_ROWS block above) ──
    # GRAHAM_LEVEL_MODERATE_ONLY: 7 straight positive years (2018-2024) — clears Moderate's 7y
    # earnings bar but not Strict's 10y one; EPS Diluted spans exactly Moderate's 7y endpoint
    # window (2017 -> 2024, 1.4x growth).
    *[
        ("GRAHAM_LEVEL_MODERATE_ONLY", "Net Income", "FY", year, 100.0)
        for year in (2018, 2019, 2020, 2021, 2022, 2023, 2024)
    ],
    ("GRAHAM_LEVEL_MODERATE_ONLY", "EPS Diluted", "FY", 2017, 1.00),
    ("GRAHAM_LEVEL_MODERATE_ONLY", "EPS Diluted", "FY", 2024, 1.40),

    # GRAHAM_STRICT_EPS_ENDPOINT_ONLY: 10 straight positive years (2015-2024) — clears Strict's
    # 10y earnings bar. EPS Diluted has only the two endpoint years (2014, 2024) — enough for
    # the single-year-endpoint method, not enough for the 3-year-average method Strict actually
    # uses (see eps_growth_avg_tickers's own test coverage below).
    *[
        ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "Net Income", "FY", year, 100.0)
        for year in (2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)
    ],
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "EPS Diluted", "FY", 2014, 1.00),
    ("GRAHAM_STRICT_EPS_ENDPOINT_ONLY", "EPS Diluted", "FY", 2024, 1.40),

    # eps_growth_avg_tickers unit-test fixtures — direct-method tests only (not wired through
    # preset_screen/_META, mirrors uninterrupted_dividend_tickers's own direct-call tests above).
    # For a years=10 span ending at fiscal_year 2024: END window = 2022-2024, BASE window =
    # 2015-2017 (years-1=9 to years-3=7 years before latest), per eps_growth_avg_tickers's own
    # window math.
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2015, 1.00),
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2016, 1.00),
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2017, 1.00),
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2022, 1.30),
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2023, 1.35),
    ("EPSAVG_PASS", "EPS Diluted", "FY", 2024, 1.40),

    # EPSAVG_TOOLOW: end avg 1.10 vs base avg 1.00 — real growth, but below the 1.33x bar.
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2015, 1.00),
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2016, 1.00),
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2017, 1.00),
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2022, 1.05),
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2023, 1.10),
    ("EPSAVG_TOOLOW", "EPS Diluted", "FY", 2024, 1.15),

    # EPSAVG_MISSING_BASE_YEAR: only 2 of the 3 base-window years present (2015 missing) — the
    # base average requires all 3 real rows, "no proof, no pass".
    ("EPSAVG_MISSING_BASE_YEAR", "EPS Diluted", "FY", 2016, 1.00),
    ("EPSAVG_MISSING_BASE_YEAR", "EPS Diluted", "FY", 2017, 1.00),
    ("EPSAVG_MISSING_BASE_YEAR", "EPS Diluted", "FY", 2022, 1.30),
    ("EPSAVG_MISSING_BASE_YEAR", "EPS Diluted", "FY", 2023, 1.35),
    ("EPSAVG_MISSING_BASE_YEAR", "EPS Diluted", "FY", 2024, 1.40),

    # EPSAVG_NEGATIVE_BASE: base avg is negative (a real historical loss period) — even though
    # the end avg is strongly positive, a smoothed "growth" figure off a net-negative base isn't
    # meaningful, so this fails.
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2015, -0.50),
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2016, -0.50),
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2017, -0.50),
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2022, 1.30),
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2023, 1.35),
    ("EPSAVG_NEGATIVE_BASE", "EPS Diluted", "FY", 2024, 1.40),

    # EPSAVG_NEGATIVE_END: end avg is negative (a recent slide into losses) — fails on the other
    # side of the same positivity gate.
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2015, 1.00),
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2016, 1.00),
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2017, 1.00),
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2022, -0.50),
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2023, -0.50),
    ("EPSAVG_NEGATIVE_END", "EPS Diluted", "FY", 2024, -0.50),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " period_type VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?)", _METRIC_ROWS)
    conn.execute(
        "CREATE TABLE dashboard_data ("
        " ticker VARCHAR, concept VARCHAR, period_type VARCHAR, fiscal_year INTEGER,"
        " value DOUBLE)"
    )
    conn.executemany("INSERT INTO dashboard_data VALUES (?,?,?,?,?)", _DATA_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    return CompanyListingRepository(connection=con)


def test_graham_passes_via_direct_pb_test(repo):
    rows, total, _ = repo.preset_screen(preset="graham", level="relaxed")
    tickers = {r.ticker for r in rows}
    assert "GRAHAM1" in tickers
    assert total == len(rows)


def test_graham_passes_via_pe_times_pb_product_test(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAM2" in {r.ticker for r in rows}


def test_graham_fails_both_branches(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAMFAIL" not in {r.ticker for r in rows}


# ── Graham: positive earnings, several years ────────────────────────────────────────────────
def test_sustained_concept_tickers_requires_every_recent_year_to_clear_the_threshold(repo):
    qualifying = repo.sustained_concept_tickers(
        tickers=["GRAHAMEARN_SUSTAINED", "GRAHAMEARN_ONELOSS"],
        concept="Net Income", min_value=0.0, years=5,
    )
    assert qualifying == frozenset({"GRAHAMEARN_SUSTAINED"})


def test_sustained_concept_tickers_excludes_insufficient_history(repo):
    qualifying = repo.sustained_concept_tickers(
        tickers=["GRAHAMEARN_SHORTHISTORY"], concept="Net Income", min_value=0.0, years=5,
    )
    assert qualifying == frozenset()


def test_graham_positive_earnings_passes_on_five_straight_positive_years(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAMEARN_SUSTAINED" in {r.ticker for r in rows}


def test_graham_positive_earnings_fails_on_one_loss_year(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAMEARN_ONELOSS" not in {r.ticker for r in rows}


def test_graham_positive_earnings_fails_with_insufficient_history(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAMEARN_SHORTHISTORY" not in {r.ticker for r in rows}


# ── Graham: uninterrupted dividend, several years ───────────────────────────────────────────
def test_uninterrupted_dividend_tickers_passes_five_contiguous_current_years(repo):
    qualifying = repo.uninterrupted_dividend_tickers(
        tickers=["GRAHAMDIV_SUSTAINED"], metric="Dividend Yield %", min_value=0.0, years=5,
        min_years=3,
    )
    assert qualifying == frozenset({"GRAHAMDIV_SUSTAINED"})


def test_uninterrupted_dividend_tickers_passes_via_the_tolerance_floor(repo):
    """A newer payer with only 3 (of a possible 5) contiguous current years still passes —
    the Phase-0-flagged deliberate exception to hard-requiring the full window."""
    qualifying = repo.uninterrupted_dividend_tickers(
        tickers=["GRAHAMDIV_FLOOR3"], metric="Dividend Yield %", min_value=0.0, years=5,
        min_years=3,
    )
    assert qualifying == frozenset({"GRAHAMDIV_FLOOR3"})


def test_uninterrupted_dividend_tickers_fails_below_the_floor(repo):
    qualifying = repo.uninterrupted_dividend_tickers(
        tickers=["GRAHAMDIV_BELOWFLOOR"], metric="Dividend Yield %", min_value=0.0, years=5,
        min_years=3,
    )
    assert qualifying == frozenset()


def test_uninterrupted_dividend_tickers_fails_when_no_longer_a_current_payer(repo):
    """5 real years of dividend history don't count if the company stopped paying 2+ years ago
    — the recency gate, not just "5 non-null rows somewhere in the past"."""
    qualifying = repo.uninterrupted_dividend_tickers(
        tickers=["GRAHAMDIV_STOPPED"], metric="Dividend Yield %", min_value=0.0, years=5,
        min_years=3,
    )
    assert qualifying == frozenset()


def test_uninterrupted_dividend_tickers_a_gap_truncates_the_run(repo):
    """A gap in the middle of an otherwise 5-year record truncates the contiguous run to just
    the 2 years after the gap — below the floor — proving this isn't a plain non-null COUNT."""
    qualifying = repo.uninterrupted_dividend_tickers(
        tickers=["GRAHAMDIV_GAP"], metric="Dividend Yield %", min_value=0.0, years=5, min_years=3,
    )
    assert qualifying == frozenset()


def test_uninterrupted_dividend_tickers_empty_input_returns_empty(repo):
    assert repo.uninterrupted_dividend_tickers(
        tickers=[], metric="Dividend Yield %", min_value=0.0, years=5, min_years=3,
    ) == frozenset()


def test_graham_uninterrupted_dividend_passes_and_fails_as_expected(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    tickers = {r.ticker for r in rows}
    assert "GRAHAMDIV_SUSTAINED" in tickers
    assert "GRAHAMDIV_FLOOR3" in tickers
    assert "GRAHAMDIV_BELOWFLOOR" not in tickers
    assert "GRAHAMDIV_STOPPED" not in tickers
    assert "GRAHAMDIV_GAP" not in tickers


# ── Graham: EPS growth >= 33%, several years ────────────────────────────────────────────────
def test_eps_growth_tickers_passes_on_real_growth(repo):
    qualifying = repo.eps_growth_tickers(
        tickers=["GRAHAMEPS_SUSTAINED"], concept="EPS Diluted", years=5, min_growth=1.33,
    )
    assert qualifying == frozenset({"GRAHAMEPS_SUSTAINED"})


def test_eps_growth_tickers_fails_below_the_growth_bar(repo):
    qualifying = repo.eps_growth_tickers(
        tickers=["GRAHAMEPS_TOOLOW"], concept="EPS Diluted", years=5, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_tickers_fails_without_the_base_year(repo):
    qualifying = repo.eps_growth_tickers(
        tickers=["GRAHAMEPS_MISSING_BASE"], concept="EPS Diluted", years=5, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_tickers_fails_on_a_negative_base_year(repo):
    """A loss-to-profit turnaround isn't "33% growth" — both endpoints must be positive."""
    qualifying = repo.eps_growth_tickers(
        tickers=["GRAHAMEPS_NEGATIVE_BASE"], concept="EPS Diluted", years=5, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_tickers_empty_input_returns_empty(repo):
    assert repo.eps_growth_tickers(
        tickers=[], concept="EPS Diluted", years=5, min_growth=1.33,
    ) == frozenset()


def test_graham_eps_growth_passes_and_fails_as_expected(repo):
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    tickers = {r.ticker for r in rows}
    assert "GRAHAMEPS_SUSTAINED" in tickers
    assert "GRAHAMEPS_TOOLOW" not in tickers
    assert "GRAHAMEPS_MISSING_BASE" not in tickers
    assert "GRAHAMEPS_NEGATIVE_BASE" not in tickers


def test_graham_multiyear_criteria_compose_with_the_other_latest_fy_criteria(repo):
    """Passing all 3 new multi-year criteria doesn't exempt a ticker from the existing latest-FY
    Current Ratio/P-E/P-B test — a real Current Ratio failure still excludes it (mirrors
    BUFFROE_FAILSOTHER's own rationale)."""
    rows, _, _ = repo.preset_screen(preset="graham", level="relaxed")
    assert "GRAHAM_MULTIYEAR_FAILSOTHER" not in {r.ticker for r in rows}


def test_buffett_null_mos_passes_when_other_criteria_hold(repo):
    """The NULL-passthrough this preset requires: a ticker with no reported MoS (sector-gated
    upstream, e.g. Financials/Real Estate) still passes on its other three criteria."""
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    tickers = {r.ticker for r in rows}
    assert "BUFFNULLPASS" in tickers
    row = next(r for r in rows if r.ticker == "BUFFNULLPASS")
    assert row.values["MoS % (Owner Earnings, FY)"] is None


def test_buffett_null_mos_does_not_exempt_the_other_criteria(repo):
    """The NULL passthrough is scoped to MoS only — a real Debt/Equity failure still excludes
    the ticker even though its MoS is also NULL."""
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFNULLBUTFAILS" not in {r.ticker for r in rows}


def test_buffett_a_real_low_mos_value_fails(repo):
    """A present-but-too-low MoS must fail, unlike a NULL one — proves the OR-NULL clause
    isn't accidentally a blanket "ignore MoS" bypass."""
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFTOOLOWMOS" not in {r.ticker for r in rows}


def test_sustained_metric_tickers_requires_every_recent_year_to_clear_the_threshold(repo):
    qualifying = repo.sustained_metric_tickers(
        tickers=["BUFFROE_SUSTAINED", "BUFFROE_ONEDIP"], metric="ROE %", min_value=15.0, years=5,
    )
    assert qualifying == frozenset({"BUFFROE_SUSTAINED"})


def test_sustained_metric_tickers_excludes_insufficient_history(repo):
    """A ticker with only 3 years of (all-passing) history doesn't count as "5 years sustained"
    just because nothing on record ever dipped below the threshold."""
    qualifying = repo.sustained_metric_tickers(
        tickers=["BUFFROE_SHORTHISTORY"], metric="ROE %", min_value=15.0, years=5,
    )
    assert qualifying == frozenset()


def test_sustained_metric_tickers_empty_input_returns_empty(repo):
    assert repo.sustained_metric_tickers(tickers=[], metric="ROE %", min_value=15.0, years=5) == frozenset()


def test_buffett_sustained_roe_passes_when_every_recent_year_clears_threshold(repo):
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFROE_SUSTAINED" in {r.ticker for r in rows}


def test_buffett_sustained_roe_fails_on_one_off_year_dip(repo):
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFROE_ONEDIP" not in {r.ticker for r in rows}


def test_buffett_sustained_roe_fails_with_insufficient_history(repo):
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFROE_SHORTHISTORY" not in {r.ticker for r in rows}


def test_buffett_sustained_roe_composes_with_the_other_latest_fy_criteria(repo):
    """5 sustained years of ROE doesn't exempt a ticker from the other criteria — a real
    Debt/Equity failure still excludes it."""
    rows, _, _ = repo.preset_screen(preset="buffett", level="moderate")
    assert "BUFFROE_FAILSOTHER" not in {r.ticker for r in rows}


def test_lynch_passes_and_fails_as_expected(repo):
    rows, _, _ = repo.preset_screen(preset="lynch", level="moderate")
    tickers = {r.ticker for r in rows}
    assert "LYNCH1" in tickers
    assert "LYNCHFAIL" not in tickers


def test_lynch_peg_below_one_passes(repo):
    rows, _, _ = repo.preset_screen(preset="lynch", level="moderate")
    row = next(r for r in rows if r.ticker == "LYNCH1")
    assert row.values["PEG"] == 0.75
    assert row.values["EPS CAGR (5Y) %"] == 20.0


def test_lynch_peg_at_or_above_one_fails(repo):
    """PEG < 1 composes with the rest of Lynch's predicate — a real Debt/Equity, Current
    Ratio, and ROE pass doesn't exempt a ticker from a PEG >= 1 failure."""
    rows, _, _ = repo.preset_screen(preset="lynch", level="moderate")
    assert "LYNCHPEGFAIL" not in {r.ticker for r in rows}


def test_lynch_eps_cagr_criterion_is_live_not_pending():
    definition = services.get_preset_definition("lynch")
    assert all(c.status == "live" for c in definition.criteria)


# ── Conservatism levels (Strict / Moderate / Relaxed) ───────────────────────────────────────
def test_preset_screen_defaults_to_strict_when_level_omitted(repo):
    """No `level` kwarg -> "strict", the toughest tier and the intended default. GRAHAM1 clears
    Relaxed's 5-year multi-year bars but not Strict's 10-year ones."""
    rows, _, _ = repo.preset_screen(preset="graham")
    assert "GRAHAM1" not in {r.ticker for r in rows}


def test_preset_screen_falls_back_to_strict_for_an_unrecognized_level(repo):
    default_tickers = {r.ticker for r in repo.preset_screen(preset="graham")[0]}
    garbage_tickers = {
        r.ticker for r in repo.preset_screen(preset="graham", level="not-a-real-level")[0]
    }
    assert garbage_tickers == default_tickers


def test_graham_level_moderate_passes_but_strict_fails(repo):
    """Proves `level` changes the actual query bounds (not just the criteria-list copy):
    GRAHAM_LEVEL_MODERATE_ONLY has 7 years of positive earnings — enough for Moderate's 7y bar,
    not Strict's 10y one — while its other criteria pass at every level."""
    moderate_tickers = {r.ticker for r in repo.preset_screen(preset="graham", level="moderate")[0]}
    strict_tickers = {r.ticker for r in repo.preset_screen(preset="graham", level="strict")[0]}
    assert "GRAHAM_LEVEL_MODERATE_ONLY" in moderate_tickers
    assert "GRAHAM_LEVEL_MODERATE_ONLY" not in strict_tickers


def test_buffett_level_moderate_passes_but_strict_fails(repo):
    """Same level-mechanism proof as Graham's, for Buffett: 5 years of sustained ROE clears
    Moderate's 5y bar but not Strict's 10y one."""
    moderate_tickers = {r.ticker for r in repo.preset_screen(preset="buffett", level="moderate")[0]}
    strict_tickers = {r.ticker for r in repo.preset_screen(preset="buffett", level="strict")[0]}
    assert "BUFFETT_LEVEL_MODERATE_ONLY" in moderate_tickers
    assert "BUFFETT_LEVEL_MODERATE_ONLY" not in strict_tickers


def test_lynch_level_moderate_passes_but_strict_fails(repo):
    """Lynch has no multi-year checks, so this is the simplest possible proof: ROE=17% clears
    Moderate's >15% bar but not Strict's >20% one — a plain latest-FY threshold difference."""
    moderate_tickers = {r.ticker for r in repo.preset_screen(preset="lynch", level="moderate")[0]}
    strict_tickers = {r.ticker for r in repo.preset_screen(preset="lynch", level="strict")[0]}
    assert "LYNCH_LEVEL_MODERATE_ONLY" in moderate_tickers
    assert "LYNCH_LEVEL_MODERATE_ONLY" not in strict_tickers


def test_graham_strict_eps_growth_uses_the_3yr_average_method_not_single_year_endpoints(repo):
    """GRAHAM_STRICT_EPS_ENDPOINT_ONLY clears Strict's 10y earnings/10y dividend bars, and would
    pass a single-year-endpoint EPS growth test (2014 -> 2024, 1.4x) — but Strict's real,
    literal test uses 3-year averages at each end, which this ticker has no data for (only 2 EPS
    Diluted rows total). If Strict mistakenly dispatched to eps_growth_tickers instead of
    eps_growth_avg_tickers, this ticker would incorrectly pass."""
    rows, _, _ = repo.preset_screen(preset="graham", level="strict")
    assert "GRAHAM_STRICT_EPS_ENDPOINT_ONLY" not in {r.ticker for r in rows}


def test_eps_growth_avg_tickers_passes_on_real_3yr_average_growth(repo):
    qualifying = repo.eps_growth_avg_tickers(
        tickers=["EPSAVG_PASS"], concept="EPS Diluted", years=10, min_growth=1.33,
    )
    assert qualifying == frozenset({"EPSAVG_PASS"})


def test_eps_growth_avg_tickers_fails_below_the_growth_bar(repo):
    qualifying = repo.eps_growth_avg_tickers(
        tickers=["EPSAVG_TOOLOW"], concept="EPS Diluted", years=10, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_avg_tickers_fails_with_an_incomplete_base_window(repo):
    """The base 3-year average requires all 3 real FY rows — 2 of 3 doesn't count."""
    qualifying = repo.eps_growth_avg_tickers(
        tickers=["EPSAVG_MISSING_BASE_YEAR"], concept="EPS Diluted", years=10, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_avg_tickers_fails_on_a_negative_base_average(repo):
    qualifying = repo.eps_growth_avg_tickers(
        tickers=["EPSAVG_NEGATIVE_BASE"], concept="EPS Diluted", years=10, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_avg_tickers_fails_on_a_negative_end_average(repo):
    qualifying = repo.eps_growth_avg_tickers(
        tickers=["EPSAVG_NEGATIVE_END"], concept="EPS Diluted", years=10, min_growth=1.33,
    )
    assert qualifying == frozenset()


def test_eps_growth_avg_tickers_empty_input_returns_empty(repo):
    assert repo.eps_growth_avg_tickers(
        tickers=[], concept="EPS Diluted", years=10, min_growth=1.33,
    ) == frozenset()


def test_graham_criteria_labels_reflect_the_resolved_level_numbers():
    strict = services.get_preset_definition("graham", "strict")
    moderate = services.get_preset_definition("graham", "moderate")
    strict_div = next(c for c in strict.criteria if "dividend" in c.label.lower())
    moderate_div = next(c for c in moderate.criteria if "dividend" in c.label.lower())
    assert strict_div.label == "Uninterrupted dividend, 10 straight years"
    assert moderate_div.label == "Uninterrupted dividend, 5+ of last 7 years"
    strict_eps = next(c for c in strict.criteria if "EPS growth" in c.label)
    assert strict_eps.label == "EPS growth ≥ 33% over 10y (3-year averages)"
    moderate_eps = next(c for c in moderate.criteria if "EPS growth" in c.label)
    assert moderate_eps.label == "EPS growth ≥ 33% over 7y"


def test_buffett_criteria_labels_reflect_the_resolved_level_numbers():
    strict = services.get_preset_definition("buffett", "strict")
    relaxed = services.get_preset_definition("buffett", "relaxed")
    strict_roe = next(c for c in strict.criteria if "ROE" in c.label)
    relaxed_roe = next(c for c in relaxed.criteria if "ROE" in c.label)
    assert strict_roe.label == "ROE ≥ 15% sustained, 10 years"
    assert relaxed_roe.label == "ROE ≥ 12% sustained, 3 years"


def test_lynch_criteria_labels_reflect_the_resolved_level_numbers():
    strict = services.get_preset_definition("lynch", "strict")
    moderate = services.get_preset_definition("lynch", "moderate")
    strict_peg = next(c for c in strict.criteria if "PEG" in c.label)
    moderate_peg = next(c for c in moderate.criteria if "PEG" in c.label)
    assert strict_peg.label == "PEG < 0.75"
    assert moderate_peg.label == "PEG < 1"


def test_preset_levels_are_relaxed_moderate_strict():
    """Display order matches the Net-Net Finder's own level-pill order (relaxed -> moderate ->
    strict, increasing conservatism left to right) — the two features must agree on one order,
    even though Investor Presets' actual default is "strict" (a separate concern from display
    order; see the fallback logic in preset_screen/preset_thresholds/views._presets_screen)."""
    assert services.preset_levels() == ("relaxed", "moderate", "strict")


def test_get_preset_screen_level_defaults_to_strict(repo, monkeypatch):
    monkeypatch.setattr(services, "CompanyListingRepository", lambda: repo)
    default_tickers = {r.ticker for r in services.get_preset_screen("graham").rows}
    strict_tickers = {
        r.ticker for r in services.get_preset_screen("graham", level="strict").rows
    }
    assert default_tickers == strict_tickers
    assert "GRAHAM1" not in default_tickers


def test_unrecognized_preset_returns_empty_not_a_crash(repo):
    rows, total, columns = repo.preset_screen(preset="not-a-real-preset")
    assert rows == ()
    assert total == 0
    assert columns == ()


def test_pagination_bounds_the_page_but_total_counts_everything(repo):
    rows, total, _ = repo.preset_screen(preset="lynch", level="moderate", page=1, page_size=1)
    assert len(rows) == 1
    # LYNCH1 and LYNCH_LEVEL_MODERATE_ONLY both pass at "moderate" in this fixture.
    assert total == 2


# ── service layer ───────────────────────────────────────────────────────────────────────────
def test_service_stats_reflect_the_definitions_own_criteria_counts(repo, monkeypatch):
    monkeypatch.setattr(services, "CompanyListingRepository", lambda: repo)
    result = services.get_preset_screen("buffett")
    definition = services.get_preset_definition("buffett")
    live = sum(1 for c in definition.criteria if c.status == "live")
    pending = sum(1 for c in definition.criteria if c.status == "pending")
    assert result.stats.live_criteria_count == live
    assert result.stats.pending_criteria_count == pending


def test_service_falls_back_to_graham_for_an_unrecognized_preset(repo, monkeypatch):
    monkeypatch.setattr(services, "CompanyListingRepository", lambda: repo)
    result = services.get_preset_screen("not-a-real-preset")
    assert {c.key for c in result.columns} == {"Current Ratio", "P/E", "P/B"}


def test_preset_keys_are_graham_buffett_lynch():
    assert services.preset_keys() == ("graham", "buffett", "lynch")


def test_buffett_roe_criterion_is_live_not_pending():
    definition = services.get_preset_definition("buffett")
    roe_criterion = next(c for c in definition.criteria if "ROE" in c.label)
    assert roe_criterion.status == "live"


def test_graham_multiyear_criteria_are_live_not_pending():
    definition = services.get_preset_definition("graham")
    assert all(c.status == "live" for c in definition.criteria)
