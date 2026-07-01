"""Development settings."""
from .base import *  # noqa: F401,F403

DEBUG = True
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "0.0.0.0"]

# Emails to the console during development.
EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
