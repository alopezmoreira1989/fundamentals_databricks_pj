"""SEC EDGAR per-filing list (10-K/10-Q) for the company Filings tab.

A runtime external fetch (like the Yahoo news headlines), kept fully graceful: any failure
(offline, SEC down, unknown ticker, malformed JSON) yields ``()`` so the tab shows "no filings
found" instead of erroring. No CIK is persisted anywhere in this pipeline (`11__fetch_sec_xbrl.py`
resolves it transiently per-ingestion-run) — the ticker→CIK map is instead fetched here directly
from SEC's own `company_tickers.json` index and cached with a long TTL (CIK assignments are
essentially static; the file only grows). The per-ticker filing list comes from SEC's
`submissions/CIK{cik}.json`, cached per-ticker with a shorter TTL. Only `filings.recent` is read
(SEC's own most-recent-first slice, capped at ~1000 entries) — `filings.files`' older, paginated
history is not fetched, since no filer has anywhere near 1000 10-K/10-Q filings.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests
from django.core.cache import cache

logger = logging.getLogger(__name__)

_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
# SEC requires a real, identifying User-Agent (org + contact email) on every request.
_HEADERS = {"User-Agent": "alm-equity-fundamentals research al.lopez.moreira@gmail.com"}
_CIK_MAP_TTL = 86400  # 1 day
_FILINGS_TTL = 3600  # 1 hour
_FORMS = ("10-K", "10-Q")
_LIMIT = 40
_CIK_MAP_CACHE_KEY = "sec_cik_map"


@dataclass(frozen=True, slots=True)
class Filing:
    form: str
    filing_date: str
    report_date: str
    description: str
    url: str


def _load_cik_map() -> dict[str, str]:
    cached = cache.get(_CIK_MAP_CACHE_KEY)
    if cached is not None:
        return cached
    try:
        resp = requests.get(_TICKER_MAP_URL, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        cik_map = {entry["ticker"].upper(): str(entry["cik_str"]).zfill(10) for entry in data.values()}
    except (requests.RequestException, ValueError, KeyError):
        logger.warning("SEC ticker index fetch failed", exc_info=True)
        return {}
    cache.set(_CIK_MAP_CACHE_KEY, cik_map, _CIK_MAP_TTL)
    return cik_map


def fetch_filings(ticker: str, limit: int = _LIMIT) -> tuple[Filing, ...]:
    """Latest 10-K/10-Q filings for `ticker`, newest first (cached). Returns `()` on any error
    or if the ticker isn't in SEC's own index (e.g. not yet admitted, or a non-US filer)."""
    if not ticker:
        return ()
    cache_key = f"filings:{ticker}:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    cik = _load_cik_map().get(ticker.upper())
    if cik is None:
        return ()
    try:
        resp = requests.get(_SUBMISSIONS_URL.format(cik=cik), headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        recent = resp.json().get("filings", {}).get("recent", {})
    except (requests.RequestException, ValueError):
        logger.warning("SEC submissions fetch failed for %s", ticker, exc_info=True)
        return ()
    cik_bare = str(int(cik))
    filings: list[Filing] = []
    # strict=False (default): tolerate a malformed response with mismatched array lengths by
    # truncating to the shortest rather than raising — this fetch must degrade, never crash.
    for form, filing_date, report_date, accession, primary_doc, description in zip(  # noqa: B905
        recent.get("form", []),
        recent.get("filingDate", []),
        recent.get("reportDate", []),
        recent.get("accessionNumber", []),
        recent.get("primaryDocument", []),
        recent.get("primaryDocDescription", []),
    ):
        if form not in _FORMS:
            continue
        accession_nodash = accession.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_bare}/{accession_nodash}/{primary_doc}"
        filings.append(
            Filing(
                form=form,
                filing_date=filing_date,
                report_date=report_date,
                description=description or form,
                url=url,
            )
        )
        if len(filings) >= limit:
            break
    result = tuple(filings)
    cache.set(cache_key, result, _FILINGS_TTL)
    return result
