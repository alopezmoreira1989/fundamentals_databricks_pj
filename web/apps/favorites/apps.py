from django.apps import AppConfig


class FavoritesConfig(AppConfig):
    # Imported as `apps.favorites`; label is the last component, `favorites`.
    name = "apps.favorites"
    label = "favorites"
    default_auto_field = "django.db.models.BigAutoField"
