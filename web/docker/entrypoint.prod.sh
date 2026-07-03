#!/usr/bin/env sh
# Production entrypoint. Database migrations are NOT run here — they run once per deploy via
# fly.toml's `release_command` (a single release machine), avoiding a race across app machines.
set -e

# Warm the artifact cache so /readyz reports ready without a cold request-path download.
# Non-fatal: a transient GitHub Release / network hiccup must not block boot — reads self-heal
# (stale-while-revalidate) and /readyz gates traffic until the cache fills.
python manage.py warm_artifact_cache \
    || echo "warm_artifact_cache failed at boot (continuing; /readyz will gate traffic until it fills)"

# `--forwarded-allow-ips=*` trusts Fly's proxy X-Forwarded-* headers, so Django's
# SECURE_PROXY_SSL_HEADER correctly sees the original HTTPS scheme behind TLS termination.
exec gunicorn config.wsgi:application \
    --bind "0.0.0.0:${PORT:-8080}" \
    --workers "${GUNICORN_WORKERS:-2}" \
    --timeout "${GUNICORN_TIMEOUT:-60}" \
    --forwarded-allow-ips="*"
