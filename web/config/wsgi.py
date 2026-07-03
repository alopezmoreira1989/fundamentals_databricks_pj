"""WSGI entry point.

Gunicorn (prod) and any WSGI server import ``config.wsgi:application``. The settings module
defaults to production; local/dev processes override ``DJANGO_SETTINGS_MODULE`` explicitly
(manage.py defaults to ``config.settings.dev``).
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")

application = get_wsgi_application()
