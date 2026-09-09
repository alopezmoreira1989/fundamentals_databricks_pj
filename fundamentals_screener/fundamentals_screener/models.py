"""The Update model — public development-journal entries (the "Updates" section).

The only persisted Django model in this package. Every other view in `fundamentals_screener`
reads external, precomputed data (parquet artifacts via DuckDB — see `repository.py`/
`data_source.py`) and this app was deliberately built with no ORM/database dependency
(docs/adr/0008-retire-web-consolidate-on-fundamentals-screener.md). Adding one small,
self-contained model for a genuinely different kind of content — a handful of maintainer-
authored journal entries, not financial data — is a real architectural change, not an
oversight; see docs/adr/0013-updates-development-journal-model.md for why it was made and why
it doesn't reopen the DB-free decision for anything else in this package.

`content` is Markdown, rendered via the `markdownify` template filter
(`templatetags/markdown_extras.py`). It is written exclusively through Django admin by the
project maintainer — never user-submitted — so the filter renders it with `mark_safe` and no
additional HTML sanitization, the same trust level as any other admin-managed copy in this
package's templates.
"""

from __future__ import annotations

from django.db import models
from django.urls import reverse


class Update(models.Model):
    CATEGORY_CHOICES = [
        ("pipeline", "Data Pipeline"),
        ("architecture", "Architecture"),
        ("frontend", "Frontend"),
        ("testing", "Testing & CI"),
        ("markets", "Market Expansion"),
        ("ml", "Machine Learning"),
    ]

    title = models.CharField(max_length=200)
    slug = models.SlugField(max_length=220, unique=True)
    summary = models.CharField(
        max_length=300,
        help_text="One or two sentences — shown on the Updates index and used as the meta description.",
    )
    content = models.TextField(
        help_text="Markdown. Supports headings, paragraphs, lists, links, inline code, code blocks, and emphasis.",
    )
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    published_at = models.DateField(help_text="Shown to readers and used for ordering (newest first).")
    is_published = models.BooleanField(
        default=False,
        help_text="Only published updates are visible on the public site, in the RSS feed, and via the API.",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-published_at", "-id"]

    def __str__(self) -> str:
        return self.title

    def get_absolute_url(self) -> str:
        return reverse("fundamentals_screener:update_detail", args=[self.slug])
