"""Company logo tag — a Logo.dev CDN hotlink, or an editorial monogram fallback.

The Logo.dev publishable key comes from ``settings.LOGO_DEV_KEY`` (empty ⇒ always monogram —
the host project doesn't need this set for the app to work, it just always shows the
monogram fallback). ``has_logo`` (from the meta artifact) suppresses a known miss up front;
the ``<img onerror>`` still swaps to the monogram if the CDN 404s at render time. No bytes are
cached — it's a runtime hotlink.
"""

from __future__ import annotations

from urllib.parse import quote

from django import template
from django.conf import settings

register = template.Library()

_LOGO_BASE = "https://img.logo.dev/ticker"


@register.inclusion_tag("fundamentals_screener/_logo.html")
def company_logo(ticker: str, name: str = "", has_logo: object = None, size: int = 72) -> dict[str, object]:
    key = getattr(settings, "LOGO_DEV_KEY", "") or ""
    url = ""
    if key and has_logo is not False and ticker:
        url = f"{_LOGO_BASE}/{quote(ticker)}?token={quote(key)}&size={size * 2}&format=png"
    letter = (name or ticker or "").strip()[:1].upper() or "•"
    return {"url": url, "letter": letter, "size": size, "small": size <= 32}
