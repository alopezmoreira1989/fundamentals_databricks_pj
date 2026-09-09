from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Update",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(max_length=200)),
                ("slug", models.SlugField(max_length=220, unique=True)),
                (
                    "summary",
                    models.CharField(
                        help_text="One or two sentences — shown on the Updates index and used as the meta description.",
                        max_length=300,
                    ),
                ),
                (
                    "content",
                    models.TextField(
                        help_text="Markdown. Supports headings, paragraphs, lists, links, inline code, code blocks, and emphasis.",
                    ),
                ),
                (
                    "category",
                    models.CharField(
                        choices=[
                            ("pipeline", "Data Pipeline"),
                            ("architecture", "Architecture"),
                            ("frontend", "Frontend"),
                            ("testing", "Testing & CI"),
                            ("markets", "Market Expansion"),
                            ("ml", "Machine Learning"),
                        ],
                        max_length=20,
                    ),
                ),
                (
                    "published_at",
                    models.DateField(help_text="Shown to readers and used for ordering (newest first)."),
                ),
                (
                    "is_published",
                    models.BooleanField(
                        default=False,
                        help_text="Only published updates are visible on the public site, in the RSS feed, and via the API.",
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-published_at", "-id"],
            },
        ),
    ]
