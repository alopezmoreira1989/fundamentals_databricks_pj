"""fmt.edgar_filings_url — issue #318's Filings tab link-out. Plain function call, no Django
app registry/settings needed (the filter never touches settings)."""

from __future__ import annotations

from fundamentals_screener.templatetags.fmt import edgar_filings_url


def test_edgar_filings_url_builds_the_edgar_browse_link():
    url = edgar_filings_url("0000320193", "10-K")
    assert url == "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=10-K"


def test_edgar_filings_url_none_when_cik_missing():
    assert edgar_filings_url(None, "10-K") is None
    assert edgar_filings_url("", "10-K") is None
