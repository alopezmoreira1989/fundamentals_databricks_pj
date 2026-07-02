from django.apps import AppConfig


class WatchlistsConfig(AppConfig):
    # Imported as `apps.watchlists`; label is the last component, `watchlists`.
    name = "apps.watchlists"
    label = "watchlists"
    default_auto_field = "django.db.models.BigAutoField"
