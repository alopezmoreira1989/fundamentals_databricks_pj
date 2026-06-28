"""Yahoo Finance per-ticker news headlines (RSS) for the company Overview tab.

A *runtime* network fetch (the only one besides Logo.dev / Release artifacts) — kept
fully graceful: any failure (offline, feed down/deprecated, malformed XML, timeout)
returns ``[]`` so the Overview tab shows "no recent news" instead of erroring. No new
dependency — ``requests`` is already required; RSS is parsed with the stdlib
``xml.etree``. The pure ``_parse_feed`` is split out from the cached network wrapper so
it can be unit-tested without a network or the Streamlit runtime.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests
import streamlit as st

# Yahoo's legacy per-symbol headline feed. Semi-deprecated and occasionally flaky — hence
# the defensive empty-on-error contract. region/lang pin US English headlines.
_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
# Yahoo 403s requests with no (or a python-urllib) User-Agent — send a browser-ish one.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; alm-equity-fundamentals/1.0)"}
_NEWS_TTL = 1800   # 30 min — headlines move slower than prices; spare Yahoo needless hits.


def _fmt_pubdate(raw) -> str:
    """RFC-822 pubDate → 'Jun 23, 2025'. '' on missing/unparseable."""
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).strftime("%b %d, %Y")
    except Exception:
        return ""


def _parse_feed(content: bytes, limit: int) -> list[dict]:
    """Parse RSS bytes → up to `limit` {title, link, published} dicts (newest first).

    Pure (no network, no Streamlit): items missing a title or a non-http link are skipped
    so nothing renders an empty/unsafe anchor. Raises on malformed XML — the caller guards.
    """
    root = ET.fromstring(content)
    out: list[dict] = []
    for item in root.iterfind(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link.startswith(("http://", "https://")):
            continue
        out.append({"title": title, "link": link, "published": _fmt_pubdate(item.findtext("pubDate"))})
        if len(out) >= limit:
            break
    return out


@st.cache_data(ttl=_NEWS_TTL, max_entries=256, show_spinner=False)
def fetch_yahoo_news(ticker: str, limit: int = 8) -> list[dict]:
    """Latest Yahoo Finance headlines for `ticker`. Returns [] on any error (never raises)."""
    if not ticker:
        return []
    url = _RSS_URL.format(ticker=requests.utils.quote(ticker))
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=8)
        resp.raise_for_status()
        return _parse_feed(resp.content, limit)
    except Exception:
        return []
