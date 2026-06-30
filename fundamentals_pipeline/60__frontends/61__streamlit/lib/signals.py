"""Health traffic-light for derived metrics (conservative Graham-style thresholds)."""
from __future__ import annotations

from statistics import mean

from .format import is_missing

# (green_max, red_min) for "lower is better"; or (green_min, red_max) for "higher is better".
# good = better than green, bad = worse than red, warn = in between.
_LOWER_IS_BETTER = {          # more expensive/risky the higher the value
    "P/E": (15, 25), "P/S": (2, 5), "P/FCF": (15, 25), "P/B": (1.5, 3),
    "EV/EBITDA": (15, 25),
    # Tangible Value / Goodwill Risk (lower = less acquisition residue / cheaper on tangibles).
    "Goodwill / Total Assets %":    (15, 30),   # green ≤15% of assets, red ≥30% — acquisition-heavy threshold
    "Goodwill / Tangible Equity %": (50, 100),  # green ≤50%, red ≥100% (Goodwill exceeds tangible equity)
    "Goodwill / Market Cap %":      (20, 50),   # green ≤20% of price is goodwill, red ≥50%
    "Price / Tangible Book Value":  (1.5, 3),   # Graham's preferred P/B substitute — same band as P/B
    "Debt / Equity": (0.5, 1.0), "Debt / Assets": (0.3, 0.5),
    # Net Debt / EBITDA: lower = healthier. A NEGATIVE value (net cash) is < green_max,
    # so it correctly reads "good" — better than zero leverage, no special-casing needed.
    "Net Debt / EBITDA": (3.0, 4.0),   # green ≤ 3× (healthy), red ≥ 4× (stretched)
    # Capital Returns — payout ratios (sustainability: a higher payout is riskier).
    # Stored as FRACTIONS (0.85 = 85%), same 0–1.x scale as Debt/Equity — not percents.
    # Quality & Risk — Accruals Ratio (earnings quality: lower = cash-backed earnings).
    "Accruals Ratio":        (0.05, 0.15),  # green ≤ 5% of assets, red ≥ 15% (poor quality)
    "Dividend Payout Ratio": (0.6, 0.9),   # green ≤ 60% of NI, red ≥ 90%
    "Buyback Payout Ratio":  (0.6, 1.0),   # green ≤ 60% of NI, red ≥ 100%
    "Total Payout Ratio":    (0.8, 1.2),   # green ≤ 80% of NI, red ≥ 120% (over-distributing)
    "Payout / FCF":          (0.8, 1.2),   # green ≤ 80% of FCF, red ≥ 120%
}
_HIGHER_IS_BETTER = {         # better the higher the value
    "Current Ratio": (2.0, 1.5),
    # Financial Health — Coverage (more coverage = safer).
    "Interest Coverage": (6.0, 2.0),   # green ≥ 6× EBIT covers interest, red ≤ 2× (distress)
    "Cash Flow to Debt": (0.4, 0.15),  # green ≥ 40% of debt retired per yr of OCF, red ≤ 15%
    "ROE %": (15, 8), "ROIC %": (15, 8), "ROCE %": (15, 8),
    "ROA %": (10, 5), "CROIC %": (15, 8),
    # Tangible Returns (higher = real capital efficiency, intangible base stripped out).
    "ROTCE %": (15, 8),                          # mirrors ROCE % — same conservative band
    "Return on Tangible Equity %": (15, 8),      # mirrors ROE %
    "Gross Margin %": (40, 20), "Operating Margin %": (20, 10),
    "Net Margin %": (20, 10), "FCF Margin %": (15, 8),
    "Op Cash Flow Margin %": (15, 8),
    "Earnings Yield %": (8, 4), "FCF Yield %": (6, 3),
    "Op Cash Flow Yield %": (6, 3), "Sales Yield %": (10, 5),
    "Book Yield %": (8, 4), "EBITDA Yield %": (8, 4),
    # Capital Returns — yields + dividend coverage (higher is better). The payout
    # RATIOS live in _LOWER_IS_BETTER (a lower payout is more sustainable).
    "Dividend Yield %": (4, 1), "Buyback Yield %": (4, 1),
    "Shareholder Yield %": (6, 2), "Net Buyback Yield %": (3, 0),
    # Keyed on the base name WITHOUT "(FCF)": signal_absolute/threshold_text strip a
    # trailing " (...)" (to tolerate (FY)/(TTM)), so "Dividend Coverage (FCF)" is looked
    # up as "Dividend Coverage". Keying it with the suffix would never match.
    "Dividend Coverage": (2.0, 1.0),   # green ≥ 2× FCF covers dividend, red ≤ 1×
    # Quality & Risk — composite scores (higher = healthier / safer).
    "Altman Z-Score":    (3.0, 1.8),   # > 3 safe, < 1.8 distress zone
    "Piotroski F-Score": (7.0, 3.0),   # ≥ 7 strong, ≤ 3 weak (0–9 scale)
}


def signal_absolute(metric: str, value) -> str | None:
    if is_missing(value):
        return None
    base = metric.split(" (")[0].strip()      # tolerates (FY)/(TTM) suffixes
    if base in _LOWER_IS_BETTER:
        g, b = _LOWER_IS_BETTER[base]
        return "good" if value <= g else ("bad" if value >= b else "warn")
    if base in _HIGHER_IS_BETTER:
        g, b = _HIGHER_IS_BETTER[base]
        return "good" if value >= g else ("bad" if value <= b else "warn")
    if base.startswith("MoS %"):              # intrinsic margin of safety
        return "good" if value > 30 else ("bad" if value < 0 else "warn")
    return None


def threshold_text(metric: str) -> str:
    """Graham threshold text for the row tooltip."""
    base = metric.split(" (")[0].strip()
    if base in _LOWER_IS_BETTER:
        g, b = _LOWER_IS_BETTER[base]
        return f"{base}: green ≤ {g:g}, red ≥ {b:g} (lower is better)"
    if base in _HIGHER_IS_BETTER:
        g, b = _HIGHER_IS_BETTER[base]
        return f"{base}: green ≥ {g:g}, red ≤ {b:g} (higher is better)"
    if base.startswith("MoS %"):
        return "MoS %: green > 30, amber 0–30, red < 0"
    return ""


def signal_vs_history(value, history, tol: float = 0.20) -> str | None:
    """For Valuation multiples: today vs the series average (≈10 years)."""
    hist = [h for h in history[:-1] if not is_missing(h)]  # excludes the current year
    if is_missing(value) or len(hist) < 3:
        return None
    avg = mean(hist)
    if avg == 0:
        return None
    dev = (value - avg) / abs(avg)
    return "bad" if dev > tol else ("good" if dev < -tol else "warn")
