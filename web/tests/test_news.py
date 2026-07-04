"""Company news (Yahoo RSS) + logo tag tests — offline (feed parsing is pure; the network
fetch is monkeypatched; the logo tag is a pure function)."""

from __future__ import annotations

import pytest
from apps.companies.templatetags.logos import company_logo
from infrastructure import news

_RSS = b"""<?xml version="1.0"?><rss><channel>
  <item><title>Apple hits a high</title><link>https://ex.com/a</link><pubDate>Mon, 23 Jun 2025 10:00:00 +0000</pubDate></item>
  <item><title>Bad protocol</title><link>ftp://ex.com/x</link></item>
  <item><title></title><link>https://ex.com/empty</link></item>
  <item><title>Second story</title><link>https://ex.com/b</link></item>
</channel></rss>"""


def test_parse_feed_skips_bad_items_and_limits():
    items = news._parse_feed(_RSS, limit=5)
    assert [i.title for i in items] == ["Apple hits a high", "Second story"]  # bad link + empty title dropped
    assert items[0].link == "https://ex.com/a"
    assert items[0].published == "Jun 23, 2025"
    assert len(news._parse_feed(_RSS, limit=1)) == 1  # honours the limit


def test_fetch_yahoo_news_caches_and_degrades(monkeypatch):
    from django.core.cache import cache

    cache.clear()
    calls: list[str] = []

    class _Resp:
        content = _RSS

        def raise_for_status(self) -> None:
            pass

    monkeypatch.setattr(news.requests, "get", lambda url, **kw: (calls.append(url), _Resp())[1])
    first = news.fetch_yahoo_news("AAPL")
    second = news.fetch_yahoo_news("AAPL")  # served from cache → no second network call
    assert first and first[0].title == "Apple hits a high"
    assert first == second and len(calls) == 1

    cache.clear()

    def _boom(url, **kw):
        raise news.requests.RequestException("feed down")

    monkeypatch.setattr(news.requests, "get", _boom)
    assert news.fetch_yahoo_news("MSFT") == ()  # any error → empty, never raises


@pytest.mark.django_db
def test_company_news_endpoint(client, monkeypatch):
    from apps.companies import services

    monkeypatch.setattr(
        services, "get_company_news", lambda t: (news.NewsItem("Headline", "https://ex.com/n", "Jun 01, 2025"),)
    )
    resp = client.get("/companies/AAPL/news/")
    assert resp.status_code == 200
    payload = resp.json()["news"]
    assert payload == [{"title": "Headline", "link": "https://ex.com/n", "published": "Jun 01, 2025"}]


def test_company_logo_tag(settings):
    settings.LOGO_DEV_KEY = "pk_test"
    ctx = company_logo("AAPL", "Apple", True, 60)
    assert "img.logo.dev/ticker/AAPL" in ctx["url"] and "pk_test" in ctx["url"]
    assert ctx["letter"] == "A"
    settings.LOGO_DEV_KEY = ""  # no key → monogram (empty url)
    assert company_logo("AAPL", "Apple", True)["url"] == ""
    settings.LOGO_DEV_KEY = "pk"  # known miss → monogram even with a key
    assert company_logo("MSFT", "Microsoft", False)["url"] == ""
