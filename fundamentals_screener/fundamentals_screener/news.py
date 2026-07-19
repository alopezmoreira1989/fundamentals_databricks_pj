"""Yahoo Finance per-ticker news headlines (RSS) for the company Overview.

A runtime external fetch (like the Logo.dev hotlink), kept fully graceful: any failure
(offline, feed down/deprecated, malformed XML, timeout) yields ``()`` so the Overview shows
"no recent news" instead of erroring. Results are cached (Django's cache framework, 30-min
TTL) so a burst of viewers triggers at most one Yahoo hit per ticker per window. The pure
``_parse_feed`` is split out so it could be unit-tested without a network. No new dependency
beyond stdlib ``xml.etree`` — ``requests`` is already required.

**Cache backend matters here.** This uses ``django.core.cache`` generically — it works
correctly with whatever backend the host configures. Django's *default* backend
(``LocMemCache``) is process-local: under this package's reference CGI deployment (a fresh
process per request, see the package README), an in-memory cache never actually persists
between requests, so this degrades to "always fetch live" rather than erroring — not broken,
just not actually caching. Configure a real backend — the simplest zero-dependency choice is
Django's built-in ``django.core.cache.backends.filebased.FileBasedCache`` (just files on
disk, survives across CGI process instances) — see the README for the exact settings.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests
from django.core.cache import cache

logger = logging.getLogger(__name__)

# Yahoo's legacy per-symbol headline feed — semi-deprecated and occasionally flaky, hence the
# empty-on-error contract. region/lang pin US English headlines.
_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
# Yahoo 403s a python-urllib User-Agent — send a browser-ish one.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; fundamentals-screener/1.0)"}
_TTL = 1800  # 30 min — headlines move slower than prices; spare Yahoo needless hits.


@dataclass(frozen=True)
class NewsItem:
    title: str
    link: str
    published: str


def _format_date(raw: str | None) -> str:
    """RFC-822 pubDate → 'Jun 23, 2025'; '' on missing/unparseable."""
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).strftime("%b %d, %Y")
    except (TypeError, ValueError):
        return ""


def _parse_feed(content: bytes, limit: int) -> tuple[NewsItem, ...]:
    """Parse RSS bytes → up to ``limit`` items (newest first). Pure (no network).

    Items missing a title or with a non-http(s) link are skipped, so nothing renders an empty
    or unsafe anchor. Raises on malformed XML — the caller guards."""
    root = ElementTree.fromstring(content)
    items: list[NewsItem] = []
    for item in root.iterfind(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link.startswith(("http://", "https://")):
            continue
        items.append(NewsItem(title=title, link=link, published=_format_date(item.findtext("pubDate"))))
        if len(items) >= limit:
            break
    return tuple(items)


def fetch_yahoo_news(ticker: str, limit: int = 8) -> tuple[NewsItem, ...]:
    """Latest Yahoo Finance headlines for ``ticker`` (cached). Returns ``()`` on any error."""
    if not ticker:
        return ()
    cache_key = f"fundamentals_screener:news:{ticker}:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        resp = requests.get(_RSS_URL.format(ticker=quote(ticker)), headers=_HEADERS, timeout=6)
        resp.raise_for_status()
        items = _parse_feed(resp.content, limit)
    except (requests.RequestException, ElementTree.ParseError):
        logger.warning("news fetch failed for %s", ticker, exc_info=True)
        items = ()
    cache.set(cache_key, items, _TTL)
    return items
