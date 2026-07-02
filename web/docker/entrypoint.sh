#!/usr/bin/env sh
set -e

# Phase 1: no `migrate` yet — there are no models. The custom user model and the first
# migration are introduced in Phase 4 (users app) before any migration runs.
exec python manage.py runserver 0.0.0.0:8000
