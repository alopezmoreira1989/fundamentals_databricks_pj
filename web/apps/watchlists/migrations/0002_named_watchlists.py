"""Evolve the single implicit watchlist into named lists.

Data-preserving: create a ``Watchlist`` per model, give every user that already has items a
default 'Watchlist' list, reassign their existing items into it, then drop the old direct
``user`` FK / unique constraint on ``WatchlistItem``.
"""

import uuid

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models

DEFAULT_WATCHLIST_NAME = "Watchlist"


def create_default_lists(apps, schema_editor):
    Watchlist = apps.get_model("watchlists", "Watchlist")
    WatchlistItem = apps.get_model("watchlists", "WatchlistItem")
    # One default list per user that currently owns items; reassign those items into it.
    user_ids = WatchlistItem.objects.values_list("user_id", flat=True).distinct()
    for user_id in user_ids:
        watchlist = Watchlist.objects.create(user_id=user_id, name=DEFAULT_WATCHLIST_NAME)
        WatchlistItem.objects.filter(user_id=user_id, watchlist__isnull=True).update(watchlist=watchlist)


def drop_default_lists(apps, schema_editor):
    Watchlist = apps.get_model("watchlists", "Watchlist")
    WatchlistItem = apps.get_model("watchlists", "WatchlistItem")
    # Reverse: point items back at their owning user, then remove the created lists.
    for item in WatchlistItem.objects.select_related("watchlist"):
        item.user_id = item.watchlist.user_id
        item.save(update_fields=["user"])
    Watchlist.objects.filter(name=DEFAULT_WATCHLIST_NAME).delete()


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("watchlists", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Watchlist",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="watchlists",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["name"],
                "constraints": [
                    models.UniqueConstraint(fields=("user", "name"), name="uniq_watchlist_user_name")
                ],
            },
        ),
        # Add the FK nullable so existing rows can be backfilled before it becomes required.
        migrations.AddField(
            model_name="watchlistitem",
            name="watchlist",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="items",
                to="watchlists.watchlist",
            ),
        ),
        migrations.RunPython(create_default_lists, drop_default_lists),
        # Old (user, ticker) uniqueness no longer holds — a ticker may live in several lists.
        migrations.RemoveConstraint(
            model_name="watchlistitem",
            name="uniq_watchlist_user_ticker",
        ),
        migrations.RemoveField(
            model_name="watchlistitem",
            name="user",
        ),
        migrations.AlterField(
            model_name="watchlistitem",
            name="watchlist",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="items",
                to="watchlists.watchlist",
            ),
        ),
        migrations.AddConstraint(
            model_name="watchlistitem",
            constraint=models.UniqueConstraint(
                fields=("watchlist", "ticker"), name="uniq_watchlist_item_ticker"
            ),
        ),
    ]
