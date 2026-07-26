"""Django system checks — run on every ``manage.py runserver``/``check``/``migrate`` etc.
(see ``apps.py``'s ``ready()``), so a real gap fails loudly there rather than rendering as a
silent broken-image icon on the Investor Presets page.
"""

from __future__ import annotations

from pathlib import Path

from django.core.checks import Error, register

_PORTRAIT_DIR = Path(__file__).parent / "static" / "fundamentals_screener" / "portraits"
_PORTRAIT_FILES = ("graham.png", "buffett.png", "lynch.png")

_ICON_DIR = Path(__file__).parent / "static" / "fundamentals_screener" / "icons"
_ICON_FILES = ("general.png", "netnet.png")


@register()
def check_investor_preset_portraits(app_configs, **kwargs):
    """Every Investor Presets school needs its portrait PNG on disk — a missing file is a
    packaging/deploy gap, not something to degrade silently past (see the "Investor Presets
    Screener" milestone: no SVG/monogram fallback is planned since the real art always ships
    alongside the code that references it)."""
    errors = []
    for filename in _PORTRAIT_FILES:
        if not (_PORTRAIT_DIR / filename).is_file():
            errors.append(
                Error(
                    f"Missing Investor Presets portrait: {_PORTRAIT_DIR / filename}",
                    hint=(
                        "Place the full-resolution portrait PNGs at "
                        "fundamentals_screener/static/fundamentals_screener/portraits/"
                        "{graham,buffett,lynch}.png."
                    ),
                    id="fundamentals_screener.E001",
                )
            )
    return errors


@register()
def check_mode_icons(app_configs, **kwargs):
    """Same "fail loudly, no silent broken-image icon" convention as the Investor Presets
    portraits check above, for the General Screener / Net-Net Finder hero illustrations."""
    errors = []
    for filename in _ICON_FILES:
        if not (_ICON_DIR / filename).is_file():
            errors.append(
                Error(
                    f"Missing screener-mode icon: {_ICON_DIR / filename}",
                    hint=(
                        "Place the icon PNGs at "
                        "fundamentals_screener/static/fundamentals_screener/icons/"
                        "{general,netnet}.png."
                    ),
                    id="fundamentals_screener.E002",
                )
            )
    return errors
