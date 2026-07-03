"""OpenAPI schema tests: the committed schema is current and warning-free, and the docs render.

The schema is generated purely by static introspection of the viewsets/serializers — no database
or artifacts needed. ``read_text`` normalises newlines on both sides, so the byte-for-byte compare
is line-ending agnostic across Linux CI and Windows.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from django.conf import settings
from django.core.management import call_command

SCHEMA_PATH = Path(settings.BASE_DIR) / "api-schema.yml"
_REGEN = "python manage.py spectacular --api-version v1 --file api-schema.yml --validate --fail-on-warn"


def test_committed_schema_is_current_and_warning_free(tmp_path):
    out = tmp_path / "schema.yml"
    # fail_on_warn ⇒ any unannotated endpoint/response breaks the build, not just drift.
    call_command("spectacular", api_version="v1", file=str(out), validate=True, fail_on_warn=True)
    assert out.read_text(encoding="utf-8") == SCHEMA_PATH.read_text(encoding="utf-8"), (
        f"OpenAPI schema drift — regenerate with:\n  {_REGEN}"
    )


def test_schema_covers_every_endpoint():
    doc = yaml.safe_load(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert doc["openapi"].startswith("3.")
    assert set(doc["paths"]) == {
        "/api/v1/companies/",
        "/api/v1/companies/{id}/",
        "/api/v1/screener/",
        "/api/v1/valuation/{id}/",
    }
    # every operation documents its 200 plus the shared 400/404 error envelope
    for path, ops in doc["paths"].items():
        codes = set(ops["get"]["responses"])
        assert {"200", "400", "404"} <= codes, f"{path} missing error responses"


@pytest.mark.parametrize("url", ["/api/schema/", "/api/docs/", "/api/redoc/"])
def test_docs_endpoints_render(client, url):
    resp = client.get(url)
    assert resp.status_code == 200
