from django.apps import AppConfig


class UsersConfig(AppConfig):
    # The app is imported as `apps.users`; its label is the last component, `users`
    # (so AUTH_USER_MODEL = "users.User"). Set explicitly to pin it.
    name = "apps.users"
    label = "users"
    # Framework default; irrelevant for User itself, which declares a UUID primary key.
    default_auto_field = "django.db.models.BigAutoField"
