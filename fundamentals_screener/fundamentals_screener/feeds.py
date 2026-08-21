"""RSS feed for the Updates section.

Uses Django's built-in syndication framework (django.contrib.syndication) — part of core
Django, no new dependency, no django.contrib.sites requirement (Feed falls back to the
request's own host via RequestSite when the sites framework isn't installed).
"""

from __future__ import annotations

from datetime import datetime, timezone

from django.contrib.syndication.views import Feed
from django.urls import reverse

from .models import Update


class UpdatesFeed(Feed):
    title = "Fundamentals Screener — Updates"
    description = "Development journal: what changed, why, and how, across this project."

    def link(self):
        return reverse("fundamentals_screener:updates_list")

    def items(self):
        return Update.objects.filter(is_published=True)[:20]

    def item_title(self, item):
        return item.title

    def item_description(self, item):
        return item.summary

    def item_link(self, item):
        return reverse("fundamentals_screener:update_detail", args=[item.slug])

    def item_pubdate(self, item):
        return datetime.combine(item.published_at, datetime.min.time(), tzinfo=timezone.utc)

    def item_categories(self, item):
        return [item.get_category_display()]
