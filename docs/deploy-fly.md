# Deploying the web layer to Fly.io

The Django `web/` app runs on Fly.io as a single containerized process fronted by Fly's proxy
(TLS termination + load balancing). This is the runbook for the artifacts in the repo:

| File | Purpose |
| --- | --- |
| `fly.toml` (repo root) | Fly app config: build, `release_command` migrations, `/readyz` health check, VM size. At the repo root so the Docker build context is the repo root. |
| `web/docker/Dockerfile.prod` | Multi-stage production image: installs `fundamentals_pipeline` + `web/requirements/prod.txt` into a venv, bakes `collectstatic`, runs gunicorn as a non-root user. |
| `web/docker/entrypoint.prod.sh` | Warms the artifact cache (non-fatal), then execs gunicorn. |

The app reads its analytical data from the published GitHub Release parquet artifacts (via
DuckDB, cached on the machine's local disk) and stores user/app data in **PostgreSQL**. No
Databricks access at request time.

## Prerequisites

- [`flyctl`](https://fly.io/docs/flyctl/install/) installed and `fly auth login` done.
- Run all commands **from the repo root** (where `fly.toml` lives).
- Edit `fly.toml`: set `app` (your app name) and `primary_region` (`fly platform regions`).

## One-time setup

```sh
# 1. Create the app (no deploy yet). Use the same name you put in fly.toml.
fly apps create fundamentals-web

# 2. Provision managed Postgres and attach it — this sets the DATABASE_URL secret on the app
#    (issue #152). Pick a size; the smallest is fine to start.
fly postgres create --name fundamentals-db --region iad
fly postgres attach fundamentals-db --app fundamentals-web

# 3. App secrets (never put these in fly.toml — it is committed).
fly secrets set \
  DJANGO_SECRET_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(64))')" \
  DJANGO_ALLOWED_HOSTS="fundamentals-web.fly.dev" \
  DJANGO_CSRF_TRUSTED_ORIGINS="https://fundamentals-web.fly.dev"   # add custom domains too

# 4. (Optional) Error tracking — inert if omitted.
fly secrets set SENTRY_DSN="https://<key>@<org>.ingest.sentry.io/<project>"
```

`DJANGO_ALLOWED_HOSTS` **must** include the host Fly serves the app under (e.g.
`fundamentals-web.fly.dev`) or Django will reject every request (and the health check) with a
400. Add any custom domain here too. `DJANGO_CSRF_TRUSTED_ORIGINS` must list the same host(s)
**scheme-qualified** (`https://…`) or login/signup form POSTs fail CSRF origin checks with a 403.

The public REST API is rate-limited per client IP (`API_THROTTLE_ANON`, default `120/min`);
`DJANGO_NUM_PROXIES` (default `1`) tells Django how many proxy hops to trust so the throttle
keys on the real client IP rather than Fly's proxy. Note the limit is **per gunicorn worker**
(in-process counter) — for a hard global limit, rate-limit at the edge instead.

## Deploy

```sh
# Tag the Sentry release with the git SHA so error spikes pin to the deploy that shipped them.
fly secrets set APP_RELEASE="$(git rev-parse --short HEAD)"   # optional; skip if not using Sentry

fly deploy
```

What happens:

1. Fly builds `web/docker/Dockerfile.prod` with the repo root as context.
2. The **release machine** runs `python manage.py migrate --noinput` once, before any new
   machine takes traffic (see `[deploy]` in `fly.toml`).
3. App machines boot: `entrypoint.prod.sh` warms the artifact cache, then starts gunicorn.
4. Fly routes traffic to a machine only once `GET /readyz` returns 200 (DB reachable + core
   artifacts cached). The 30s `grace_period` covers the boot-time cache warm.

## Verify

```sh
fly logs                                   # structured JSON access/error logs, one line per request
curl https://fundamentals-web.fly.dev/healthz   # {"status":"ok"}
curl https://fundamentals-web.fly.dev/readyz    # {"status":"ready", "checks":[...], "cache_metrics":{...}}
fly open                                    # open the landing page
```

## Operations

- **Uptime monitoring / alerting** (the external half of #157): point an external monitor
  (Fly's own metrics, Better Uptime, Pingdom, etc.) at `GET /readyz` and alert on non-200. The
  in-app probes and structured logs are already live; only the monitor + alert routing is
  configured here, outside the codebase.
- **Artifact freshness** is automatic: cached artifacts revalidate in the background once older
  than `ARTIFACTS_TTL` (stale-while-revalidate), so a fresh publish is picked up without a
  redeploy. To eliminate the one-request lag right after a publish, re-warm on the publish
  cadence: `fly ssh console -C "python manage.py warm_artifact_cache"` (or a scheduled machine).
- **Scaling**: `fly scale count 2` (more machines) / `fly scale vm shared-cpu-2x --memory 2048`
  (bigger machines — raise memory if DuckDB queries over the full universe pressure the 1 GB
  default). Tune gunicorn with the `GUNICORN_WORKERS` / `GUNICORN_TIMEOUT` env vars.
- **Rollback**: `fly releases` then `fly deploy --image <previous-image-ref>`, or
  `fly releases rollback`.

## Gotchas

- **Secrets vs env**: `fly.toml [env]` is committed and public — only non-secret config goes
  there. Everything sensitive is a `fly secrets set`.
- **Health check over HTTP**: Fly's checks hit the internal port in plain HTTP. `prod.py` exempts
  `/healthz` and `/readyz` from `SECURE_SSL_REDIRECT` (`SECURE_REDIRECT_EXEMPT`) so they aren't
  301'd to https and marked unhealthy. All real traffic is still force-redirected to HTTPS.
- **The artifact cache is ephemeral** (machine-local, re-warmed on boot). That's intentional —
  it is derivable from the Release. Mount a Fly volume at the cache dir only if you want to skip
  the boot-time warm; it is not required.
