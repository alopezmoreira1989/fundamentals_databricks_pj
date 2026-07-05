#!/usr/bin/env sh
# Production entrypoint. Database migrations normally run once per deploy via a platform release
# hook (e.g. Fly's `release_command`), a single machine, before any app instance takes traffic —
# avoiding a migration race across multiple app machines. Set RUN_MIGRATIONS_ON_BOOT=true only on
# platforms without such a hook AND that never run more than one instance of this service (e.g.
# Render's free plan, which has no paid-only pre-deploy command) — see docs/deploy-render.md.
set -e

if [ "${RUN_MIGRATIONS_ON_BOOT:-false}" = "true" ]; then
    python manage.py migrate --noinput
fi

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
