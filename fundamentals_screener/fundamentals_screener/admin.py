from __future__ import annotations

from django.contrib import admin

from .models import Update


@admin.register(Update)
class UpdateAdmin(admin.ModelAdmin):
    list_display = ("title", "category", "published_at", "is_published", "updated_at")
    list_filter = ("category", "is_published")
    search_fields = ("title", "summary", "content")
    prepopulated_fields = {"slug": ("title",)}
    date_hierarchy = "published_at"
    ordering = ("-published_at",)
    fields = ("title", "slug", "summary", "content", "category", "published_at", "is_published")
