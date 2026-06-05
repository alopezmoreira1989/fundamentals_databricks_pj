---
name: valuation-methods
description: The four intrinsic-value methods computed in 20_transformation/23__intrinsic_value.py (Graham Number, Graham Revised, two-stage DCF, Owner Earnings) plus Margin of Safety, how each reads its assumptions from 00_config/valuation_assumptions.json, and the edge-case rules that return NULL instead of a misleading number. Use when asked to add or change a valuation method, adjust valuation assumptions (WACC, growth, multiple), explain how Graham Number, DCF, Owner Earnings, or intrinsic value is computed, or debug a NULL or absurd per-share value. Note: the formulas live in 23__intrinsic_value.py (pipeline), not lib/signals.py, which only does threshold color banding.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# valuation-methods

## CRITICAL

- **Formulas live in `fundamentals_pipeline/20_transformation/23__intrinsic_value.py`** (NumPy-vectorized, computes FY and TTM variants, writes `main.financials.financials_intrinsic_value`). `fundamentals_pipeline/60_streamlit_app/lib/signals.py` does NOT compute valuations — it only bands MoS and other metrics into good/warn/bad colors for the dashboard. Edit formulas in 23; edit color thresholds in signals.py.
- **Assumptions come from `00_config/valuation_assumptions.json`**, merged per ticker as `defaults` overlaid by `overrides[ticker]`. Validate the JSON shape with [[pipeline-preflight]] before a run — a bad `dcf` block crashes 23.
- **Edge cases must return NULL, never a misleading number.** Negative/zero EPS or book value, distorted book (Retained Earnings negative or P/B over 10), `WACC <= growth_terminal`, non-positive starting cash flow, or non-positive shares all yield NULL. Preserve this when editing.
- 23 is a stateful pipeline notebook (writes Delta). Plan the change, flag any `dbutils`/`%run`/`spark` dependency, then implement.

## The four methods (summary)

Full formulas, inputs, and the exact edge-case guards are in `references/valuation-formulas.md`.

| Method | One-line formula | Key guard |
|---|---|---|
| Graham Number | `sqrt(magic × EPS × BVPS)`, magic = 22.5 | NULL if EPS or BVPS not positive, or book distorted (RE negative, P/B over 10) |
| Graham Revised | `EPS × (base_pe + mult × g × 100) × aaa_norm / (aaa_yield × 100)` | NULL if EPS not positive; growth capped at `growth_cap` |
| DCF (2-stage) | explicit-horizon discounted CF + Gordon terminal, bridged to equity per share | NULL if shares/CF not positive or `WACC <= growth_terminal`; per-ticker `skip` flag |
| Owner Earnings | `NI + D&A + SBC − CapEx − ΔWC`, then `× multiple` or `/ discount_rate`, per share | NULL if OE or shares not positive; per-ticker `skip` flag |
| Margin of Safety | `(IV − price) / IV × 100` | NULL if no market price; negative MoS (overvalued) is valid |

DCF can use FCF or Owner Earnings as its starting cash flow via `dcf.use_owner_earnings`.

## Assumptions file shape

`valuation_assumptions.json` top-level: `_comment`, `_units`, `defaults`, `overrides`.
`defaults` subsections: `graham`, `graham_revised`, `dcf`, `owner_earnings`, `margin_of_safety`.
`defaults.dcf` numeric fields: `wacc`, `growth_stage1`, `growth_terminal`, `horizon_years`,
plus the `use_owner_earnings` flag. `overrides[TICKER]` shallow-merges over `defaults` (e.g.
`"AAPL": {"dcf": {"growth_stage1": 0.06}}`). See `references/valuation-formulas.md` for the full
defaults block and the Gordon-model constraint `growth_terminal < wacc`.

## Recipes

### Add a new valuation method
1. Add its assumption subsection to `defaults` (and document units in `_units`) in `valuation_assumptions.json`.
2. Compute it vectorized in `23__intrinsic_value.py` alongside the existing four; return NaN for every edge case rather than a wrong number.
3. Emit it into the long output (FY and TTM if meaningful) and, if it should color-band, add a threshold in `lib/signals.py` (`_LOWER_IS_BETTER` / `_HIGHER_IS_BETTER` / a special case like MoS).
4. Add the metric to `metrics_hierarchy.json` (category `Intrinsic Value`) so it renders.

### Tune an assumption for one ticker
Add/extend `overrides[TICKER]` with only the fields that differ — the rest inherit from `defaults`.
For maturing names, a lower `dcf.growth_stage1` is the usual lever (AAPL precedent).

### Debug a NULL or absurd per-share value
Walk the guards in `references/valuation-formulas.md` for that method: most NULLs are an intentional
edge-case return (negative EPS/book, `wacc <= growth_terminal`, a `skip` flag). An absurd value is
usually a missing debt/cash bridge input or a stale share count — check the inputs the method reads.

## What NOT to do

- Don't move valuation math into `lib/signals.py` — it must stay in the pipeline (23) so the Streamlit app reads precomputed values from Delta and needs no Spark.
- Don't replace an edge-case NULL with 0 or a clamped number — a misleading "value" is worse than an honest blank.
- Don't set `growth_terminal >= wacc` — the Gordon terminal value blows up; 23 already NULLs this, keep it.
- Don't translate the Spanish `_note` fields in `valuation_assumptions.json`.

## Related

- [[pipeline-preflight]] — validates `valuation_assumptions.json` well-formedness (including `growth_terminal < wacc`) before a run.
- [[external-benchmark-validation]] — sanity-check intrinsic values / multiples against an external reference.
