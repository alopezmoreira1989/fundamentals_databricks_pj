"""Unit tests for the pure RSS parser in 60__frontends/61__streamlit/lib/news.py.

Only ``_parse_feed`` / ``_fmt_pubdate`` are exercised — no network, no Streamlit
runtime. ``lib.news`` imports ``streamlit`` (for ``st.cache_data``), so the whole
module is skipped where streamlit is absent.
"""

from __future__ import annotations

import pytest

pytest.importorskip("streamlit")
from lib import news  # noqa: E402

_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>AAPL News</title>
  <item>
    <title>Apple unveils new product</title>
    <link>https://finance.yahoo.com/news/apple-1.html</link>
    <pubDate>Mon, 23 Jun 2025 14:30:00 +0000</pubDate>
  </item>
  <item>
    <title>Analysts react</title>
    <link>https://finance.yahoo.com/news/apple-2.html</link>
    <pubDate>Tue, 24 Jun 2025 09:00:00 +0000</pubDate>
  </item>
</channel></rss>"""


def test_parse_feed_extracts_items():
    items = news._parse_feed(_SAMPLE, limit=8)
    assert len(items) == 2
    assert items[0]["title"] == "Apple unveils new product"
    assert items[0]["link"] == "https://finance.yahoo.com/news/apple-1.html"
    assert items[0]["published"] == "Jun 23, 2025"


def test_parse_feed_respects_limit():
    assert len(news._parse_feed(_SAMPLE, limit=1)) == 1


def test_parse_feed_skips_missing_title_or_nonhttp_link():
    bad = b"""<rss version="2.0"><channel>
      <item><title></title><link>https://x.com/a</link></item>
      <item><title>No link</title><link></link></item>
      <item><title>Bad scheme</title><link>javascript:alert(1)</link></item>
      <item><title>Good</title><link>https://x.com/good</link></item>
    </channel></rss>"""
    items = news._parse_feed(bad, limit=8)
    assert [i["title"] for i in items] == ["Good"]


def test_fmt_pubdate_graceful_on_garbage():
    assert news._fmt_pubdate("not a date") == ""
    assert news._fmt_pubdate(None) == ""
    assert news._fmt_pubdate("") == ""
