# Web layer (development image).
# Build context is the REPO ROOT so the image can install the sibling `fundamentals_pipeline`
# package (the project's single source of truth) alongside the Django dependencies.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DJANGO_SETTINGS_MODULE=config.settings.dev

WORKDIR /app

# libpq for psycopg; build-essential for any source builds.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 1) Install the shared pipeline package (editable) — needs pyproject + the package itself.
COPY pyproject.toml ./
COPY fundamentals_pipeline ./fundamentals_pipeline
RUN pip install --no-cache-dir -e .

# 2) Install the web-layer dependencies.
COPY web/requirements ./web/requirements
RUN pip install --no-cache-dir -r web/requirements/dev.txt

# 3) Bring in the web project.
COPY web ./web

WORKDIR /app/web
EXPOSE 8000
ENTRYPOINT ["./docker/entrypoint.sh"]
