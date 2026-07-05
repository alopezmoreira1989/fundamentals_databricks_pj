#!/usr/bin/env sh
set -e

# Single dev instance behind docker-compose — no multi-machine migration race to worry about.
python manage.py migrate --noinput
exec python manage.py runserver 0.0.0.0:8000
