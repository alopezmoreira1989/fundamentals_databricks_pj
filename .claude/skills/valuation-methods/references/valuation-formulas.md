# Valuation formulas (reference)

Ground truth from `fundamentals_pipeline/20__transformation/23__intrinsic_value.py` and
`fundamentals_pipeline/00__config/valuation_assumptions.json`. Re-read those before relying on this;
values captured at skill-authoring time. All methods are NumPy-vectorized and computed for both FY
and TTM bases.

## `defaults` block (valuation_assumptions.json)

```json
"defaults": {
  "graham":          { "magic_number": 22.5 },
  "graham_revised":  { "base_pe": 8.5, "growth_multiplier": 2.0, "aaa_yield_norm": 4.4,
                       "graham_aaa_yield": 0.055, "growth_cap": 0.15 },
  "dcf":             { "wacc": 0.09, "growth_stage1": 0.08, "growth_terminal": 0.025,
                       "horizon_years": 10, "use_owner_earnings": false },
  "owner_earnings":  { "multiple": 15.0, "discount_rate": 0.09, "method": "multiple" },
  "margin_of_safety":{ "warn_threshold": 0.10, "buy_threshold": 0.33 }
}
```

`overrides[TICKER]` shallow-merges over `defaults` (e.g. `"AAPL": {"dcf": {"growth_stage1": 0.06}}`).
Some tickers carry `dcf.skip` / `owner_earnings.skip` (e.g. financials like JPM, BRK.B) — those
methods return NULL for them by design.

## Graham Number

```
GN = sqrt(magic_number × EPS × BVPS)        magic_number = 22.5
BVPS = Total Stockholders Equity / shares
```
NULL unless `EPS > 0` and `BVPS > 0`. Also suppressed for **distorted book**: Retained Earnings
negative, or `price / BVPS > 10` (distorted P/B). The distortion guard is why Graham Number is
excluded from the dashboard's valuation "football field" — it is often a wild outlier.

## Graham Revised

```
g_company = company's trailing 5y EPS CAGR (point-in-time)   ; NaN → dcf.growth_stage1
g_eff     = clip(min(g_company, graham_revised.growth_cap), 0, None)   growth_cap = 0.15
GRV       = EPS × (base_pe + growth_multiplier × g_eff × 100)
                × aaa_yield_norm / (graham_aaa_yield × 100)
```
NULL unless `EPS > 0`. The growth `g` is each company's own **trailing 5-year EPS CAGR**, derived
point-in-time in 23 (ending at each row's `fiscal_year`, no lookahead; longest span available in
[3,5]y; both endpoints' EPS must be > 0). It is **floored at 0** (Graham's `8.5` base is the
no-growth P/E) and capped at `growth_cap`. When the CAGR is undefined (< 3y of positive EPS history)
it falls back to the `dcf.growth_stage1` assumption — so the per-ticker `dcf` overrides remain the
backstop. DCF itself still uses `dcf.growth_stage1` directly; only Graham Revised uses `g_eff`.
`graham_aaa_yield` is the current AAA corporate yield (set to 0.055 / 5.5% as of 2026-06) and must be
refreshed periodically — IV scales as `1/Y`, so a stale-low yield inflates every GRV.

## DCF (2-stage)

```
starting_cf = Owner Earnings dollars  if dcf.use_owner_earnings else FCF
Stage 1 (t = 1..horizon_years):
    CF_t   = CF_{t-1} × (1 + growth_stage1)
    PV1   += CF_t / (1 + wacc)^t
Terminal (Gordon):
    TV      = CF_horizon × (1 + growth_terminal) / (wacc - growth_terminal)
    PV_term = TV / (1 + wacc)^horizon_years
Equity bridge, per share:
    IV/share = (PV1 + PV_term - (LT Debt + ST Debt) + (Cash + ST Investments)) / shares
```
NULL unless `shares > 0`, `starting_cf > 0`, and `wacc > growth_terminal`; also NULL when `dcf.skip`
is set for the ticker. Debt and cash missing values are treated as 0 in the bridge only.

## Owner Earnings (Buffett, 1986)

```
OE_dollars = Net Income + D&A + SBC - CapEx - ΔWorkingCapital   (missing components -> 0)
method = "multiple":   value = OE_dollars × owner_earnings.multiple
method = "perpetuity": value = OE_dollars / owner_earnings.discount_rate
IV/share = value / shares
```
Defaults: `multiple = 15.0`, `discount_rate = 0.09`, `method = "multiple"`. NULL unless
`OE_dollars > 0` and `shares > 0`; NULL when `owner_earnings.skip` is set. Also exported as the
absolute dollar metric "Owner Earnings (FY)" / "Owner Earnings (TTM)".

## Margin of Safety

```
MoS % = (IV_per_share - price_close) / IV_per_share × 100
```
Computed per method. NULL when `price_close` is missing. Negative MoS (price above IV = overvalued)
is valid and shown negative. Dashboard banding (`lib/signals.py`): good above ~30%, bad below 0%,
warn between — those thresholds are presentation only, separate from the `warn_threshold` /
`buy_threshold` in the JSON.

## Where banding lives (not here)

`lib/signals.py` has `signal_absolute`, `threshold_text`, `signal_vs_history` — color/threshold
logic only. No intrinsic-value math. Keep the split: computation in 23, presentation in signals.py.
