"""The Update model + Updates views/admin/feed (the public "development journal" section).

Unlike every other test in this suite (in-memory DuckDB fixtures, zero Django configuration —
see their own docstrings), these tests exercise a real Django model against a real (in-memory
SQLite) database via pytest-django + tests/settings.py — the Update model is the one piece of
this package backed by Django's ORM rather than the read-only parquet/DuckDB data layer. See
docs/adr/0013-updates-development-journal-model.md for why.

Historical entries seeded by migrations/0002_seed_initial_updates.py are present in every test's
DB here (migrations run once for the whole session; only rows a test itself creates are rolled
back) -- assertions are written to tolerate that seed data rather than assume an empty table.
"""

from __future__ import annotations

from datetime import date

import pytest
from django.contrib import admin
from django.db import IntegrityError
from django.test import Client
from django.urls import reverse
from fundamentals_screener.models import Update

pytestmark = pytest.mark.django_db


def _make_update(**kwargs):
    defaults = dict(
        title="Test Update",
        slug="test-update",
        summary="A test summary.",
        content="Some *emphasis* here.",
        category="pipeline",
        published_at=date(2026, 1, 1),
        is_published=True,
    )
    defaults.update(kwargs)
    return Update.objects.create(**defaults)


def test_published_update_appears_in_index():
    _make_update()
    resp = Client().get(reverse("fundamentals_screener:updates_list"))
    assert resp.status_code == 200
    assert "Test Update" in resp.content.decode()


def test_unpublished_update_does_not_appear_in_index():
    _make_update(slug="draft-only", title="Draft Only Update", is_published=False)
    resp = Client().get(reverse("fundamentals_screener:updates_list"))
    assert "Draft Only Update" not in resp.content.decode()


def test_unpublished_update_detail_page_404s():
    _make_update(slug="draft-only", is_published=False)
    resp = Client().get(reverse("fundamentals_screener:update_detail", args=["draft-only"]))
    assert resp.status_code == 404


def test_unknown_slug_404s():
    resp = Client().get(reverse("fundamentals_screener:update_detail", args=["does-not-exist"]))
    assert resp.status_code == 404


def test_published_update_detail_page_renders():
    _make_update()
    resp = Client().get(reverse("fundamentals_screener:update_detail", args=["test-update"]))
    assert resp.status_code == 200
    assert "Test Update" in resp.content.decode()


def test_update_detail_renders_markdown_content():
    _make_update(content="Some *emphasis* and `inline code`.")
    resp = Client().get(reverse("fundamentals_screener:update_detail", args=["test-update"]))
    body = resp.content.decode()
    assert "<em>emphasis</em>" in body
    assert "<code>inline code</code>" in body


def test_slug_resolves_to_the_correct_update():
    _make_update(title="First Entry", slug="first-entry")
    _make_update(title="Second Entry", slug="second-entry")
    resp = Client().get(reverse("fundamentals_screener:update_detail", args=["second-entry"]))
    assert "Second Entry" in resp.content.decode()


def test_index_orders_newest_first():
    _make_update(title="Older Marker Entry", slug="older-marker", published_at=date(2020, 1, 1))
    _make_update(title="Newer Marker Entry", slug="newer-marker", published_at=date(2030, 1, 1))
    body = Client().get(reverse("fundamentals_screener:updates_list")).content.decode()
    assert body.index("Newer Marker Entry") < body.index("Older Marker Entry")


def test_navigation_includes_updates_link():
    resp = Client().get(reverse("fundamentals_screener:about"))
    assert reverse("fundamentals_screener:updates_list") in resp.content.decode()


def test_updates_feed_lists_only_published():
    _make_update(title="Feed Visible Entry", slug="feed-visible")
    _make_update(title="Feed Hidden Entry", slug="feed-hidden", is_published=False)
    resp = Client().get(reverse("fundamentals_screener:updates_feed"))
    assert resp.status_code == 200
    assert resp["Content-Type"].startswith("application/rss+xml")
    body = resp.content.decode()
    assert "Feed Visible Entry" in body
    assert "Feed Hidden Entry" not in body


def test_slug_is_unique():
    _make_update(slug="dup-slug")
    with pytest.raises(IntegrityError):
        _make_update(slug="dup-slug", title="A Different Title")


def test_update_model_registered_in_admin():
    assert admin.site.is_registered(Update)


def test_seeded_historical_updates_are_published_and_in_range():
    # pytest-django wraps each test in its own rolled-back transaction, so no other test's
    # _make_update() rows leak in here -- only the migration-seeded historical entries exist.
    seeded = Update.objects.filter(is_published=True)
    assert 8 <= seeded.count() <= 12
