"""End-to-end integration tests spanning the whole web stack.

Two kinds of flow, both black-box through the Django test client (real URL routing, middleware,
views, templates):

* **Read-model pages** (home / screener / company / valuation) rendered end-to-end
  view → service → repository → DuckDB over the fixture artifacts. Assertions use
  ``response.context`` (the DTOs the template received) rather than scraping HTML, so they test
  the wiring, not the markup.
* **The authenticated user journey** — signup → browse a company (cross-app history side effect)
  → watchlist → favorite → revisit the pages — exercising the ORM apps and the read model
  together, with the service layer as the source of truth for what each POST persisted.

Fixture-gated (``artifacts_from_fixtures`` skips when the fixtures are absent); the user-data
flows need the DB (``django_db``).
"""

from __future__ import annotations

import pytest
from apps.favorites import services as favorite_services
from apps.history import services as history_services
from apps.watchlists import services as watchlist_services
from django.contrib.auth import get_user_model
from django.urls import reverse

pytestmark = pytest.mark.django_db

User = get_user_model()
TICKER = "AAPL"
PW = "integration-pw-123!"


# ── landing page + help (static, no fixtures needed) ─────────────────────────────────────
def test_home_page_renders_product_intro_and_entry_points(client):
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.content.decode()
    # Real product intro (not the old "Phase 1 skeleton" placeholder) with entry points.
    assert "Phase 1" not in body
    assert "Open the screener" in body
    assert f'href="{reverse("screener:screen")}"' in body
    assert f'href="{reverse("help")}"' in body


def test_home_cta_is_auth_aware(client):
    assert "Create a free account" in client.get("/").content.decode()  # anonymous
    user = User.objects.create_user(username="cta", email="cta@example.com", password=PW)
    client.force_login(user)
    body = client.get("/").content.decode()
    assert "Your watchlists" in body and "Create a free account" not in body


def test_help_page_renders_usage_docs(client):
    resp = client.get(reverse("help"))
    assert resp.status_code == 200
    body = resp.content.decode()
    for anchor in ("#screening", "#company", "#valuation", "#watchlists"):
        assert anchor in body


def test_nav_links_to_help_on_interior_page(artifacts_from_fixtures, client):
    # Consistent nav: Help is reachable from an interior (screener) page too.
    body = client.get(reverse("screener:screen")).content.decode()
    assert f'href="{reverse("help")}"' in body


def test_screener_page_renders_universe(artifacts_from_fixtures, client):
    resp = client.get("/screener/")
    assert resp.status_code == 200
    # The full stack produced real rows + filter options for the template.
    assert resp.context["total"] > 100
    assert len(resp.context["rows"]) > 0
    assert resp.context["metrics"] and resp.context["sectors"]


def test_screener_metric_filter_orders_and_paginates(artifacts_from_fixtures, client):
    metric = next(m for m in _client_metrics(client))
    resp = client.get("/screener/", {"metric": metric, "page": "1"})
    assert resp.status_code == 200
    assert resp.context["selected"] == metric
    values = [r.metric_value for r in resp.context["rows"] if r.metric_value is not None]
    assert values == sorted(values, reverse=True)  # ordered by the metric, desc


def test_screener_bad_bounds_show_inline_error_but_still_render(artifacts_from_fixtures, client):
    resp = client.get("/screener/", {"min": "abc"})
    assert resp.status_code == 200
    assert "must be numbers" in resp.context["error"]
    assert len(resp.context["rows"]) > 0  # table still renders; bounds ignored
    # An unparseable page number falls back to page 1 rather than erroring.
    assert client.get("/screener/", {"page": "not-a-number"}).context["page"] == 1


def test_screener_data_tolerates_bad_limit(artifacts_from_fixtures, client):
    metric = _client_metrics(client)[0]
    resp = client.get("/screener/data/", {"metric": metric, "limit": "not-an-int"})
    assert resp.status_code == 200  # unparseable limit falls back to the default, not a 400
    assert resp.json()["metric"] == metric


def test_company_page_renders_and_404s(artifacts_from_fixtures, client):
    resp = client.get(f"/companies/{TICKER}/")
    assert resp.status_code == 200
    assert resp.context["detail"].summary.ticker == TICKER
    assert client.get("/companies/notareal/").status_code == 404


def test_company_page_has_statement_tabs(artifacts_from_fixtures, client):
    body = client.get(f"/companies/{TICKER}/").content.decode()
    # Tab nav + panes for each reported statement (Streamlit-style navigation).
    for label in ("Overview", "Income Statement", "Balance Sheet", "Cash Flow"):
        assert label in body
    assert 'data-bs-target="#pane-income-statement"' in body
    assert 'id="pane-balance-sheet"' in body
    # Statement grids carry the fiscal-year columns, the Revenue line, and per-row sparklines.
    assert "Line item" in body and "Revenue" in body
    assert 'class="sparkline"' in body


def test_company_page_has_price_and_quarterly_tabs(artifacts_from_fixtures, client):
    body = client.get(f"/companies/{TICKER}/").content.decode()
    assert "Price" in body and 'id="pane-price"' in body
    assert "<svg" in body and "polyline" in body  # inline price chart
    assert "Quarterly" in body and 'id="pane-quarterly"' in body


def test_company_page_valuation_tab_and_no_iv_in_derived(artifacts_from_fixtures, client):
    body = client.get(f"/companies/{TICKER}/").content.decode()
    # Valuation is its own tab (football field + MoS + multiples)...
    assert 'id="pane-valuation"' in body and "Margin of safety" in body
    assert "Multiples" in body and "P/E" in body  # valuation multiples moved here
    # ...and intrinsic-value metrics no longer duplicated in the Derived-metrics tab.
    assert "Intrinsic Value" not in body


def test_valuation_page_renders_with_data_and_empty_for_unknown(artifacts_from_fixtures, client):
    resp = client.get(f"/valuation/{TICKER}/")
    assert resp.status_code == 200
    assert resp.context["ticker"] == TICKER
    assert resp.context["points"]  # a real ticker has Margin-of-Safety rows
    # An unknown ticker degrades to an empty page (200), not a 404 — the valuation page's contract.
    empty = client.get("/valuation/notareal/")
    assert empty.status_code == 200
    assert list(empty.context["points"]) == []


def test_protected_pages_redirect_anonymous_to_login(client):
    for url in ("/watchlist/", "/favorites/", "/history/"):
        resp = client.get(url)
        assert resp.status_code == 302
        assert "/accounts/login/" in resp["Location"], url


# ── full authenticated journey across the ORM apps + read model ──────────────────────────
def test_signup_browse_watchlist_favorite_history_journey(artifacts_from_fixtures, client):
    # 1. Sign up through the real form → authenticated + redirected home.
    resp = client.post(
        "/accounts/signup/",
        {"username": "trader", "email": "trader@example.com", "password1": PW, "password2": PW},
    )
    assert resp.status_code == 302
    user = User.objects.get(username="trader")

    # 2. Viewing a company page renders AND records browsing history (cross-app side effect).
    page = client.get(f"/companies/{TICKER}/")
    assert page.status_code == 200
    assert page.context["detail"].summary.ticker == TICKER
    assert TICKER in history_services.recent_tickers(user)

    # 3. Add to the (default) watchlist via the POST endpoint → persisted for this user.
    assert client.post("/watchlist/add/", {"ticker": TICKER.lower()}).status_code == 302
    default = watchlist_services.get_default(user)
    assert TICKER in watchlist_services.list_tickers(default)

    # 4. Favorite it → persisted, and the company page now reflects it.
    assert client.post("/favorites/add/", {"ticker": TICKER.lower()}).status_code == 302
    assert favorite_services.contains(user, TICKER)
    assert client.get(f"/companies/{TICKER}/").context["in_favorites"] is True

    # 5. The user-data pages all render for the logged-in user.
    for url in ("/watchlist/", "/favorites/", "/history/"):
        assert client.get(url).status_code == 200


def test_history_deduplicates_across_repeat_views(artifacts_from_fixtures, client):
    user = User.objects.create_user(username="rv", email="rv@example.com", password=PW)
    client.force_login(user)
    for _ in range(3):
        assert client.get(f"/companies/{TICKER}/").status_code == 200
    # Repeated views bump recency, they don't duplicate the entry.
    assert history_services.recent_tickers(user).count(TICKER) == 1


def _client_metrics(client) -> list[str]:
    """Metric names the screener offers (from the rendered page context)."""
    return list(client.get("/screener/").context["metrics"])
