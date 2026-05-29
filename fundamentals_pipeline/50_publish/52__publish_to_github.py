# Databricks notebook source
# MAGIC %md
# MAGIC # 50_publish / 52__publish_to_github
# MAGIC
# MAGIC Uploads the three artifacts written by `51__export_dashboard_data` to a
# MAGIC GitHub Release tagged `data-YYYY-MM-DD`, then moves the floating `latest`
# MAGIC tag/release to point at the new artifacts so the public Streamlit app can
# MAGIC always pull from `.../releases/download/latest/<file>`.
# MAGIC
# MAGIC **Idempotent.** If a release with today's date tag already exists, its
# MAGIC assets are deleted and re-uploaded. The `latest` release is always
# MAGIC delete-and-recreated.
# MAGIC
# MAGIC **Required Databricks secret:** `github_pat` in scope `github` — a
# MAGIC fine-grained Personal Access Token with `Contents: Read and write` on the
# MAGIC target repo. Create via the Databricks CLI:
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope github
# MAGIC databricks secrets put-secret github github_pat
# MAGIC ```
# MAGIC
# MAGIC Databricks-only: uses `dbutils.secrets`.

# COMMAND ----------

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OWNER = "alopezmoreira1989"
REPO  = "fundamentals_databricks_pj"

ARTIFACTS = [
    Path("/tmp/dashboard_data.parquet"),
    Path("/tmp/dashboard_metrics.parquet"),
    Path("/tmp/dashboard_meta.json"),
]

LATEST_TAG = "latest"
DATED_TAG  = f"data-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read secret, pre-flight checks

# COMMAND ----------

TOKEN = dbutils.secrets.get(scope="github", key="github_pat")
HEADERS = {
    "Accept":               "application/vnd.github+json",
    "Authorization":        f"Bearer {TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
}
API_BASE = f"https://api.github.com/repos/{OWNER}/{REPO}"

for f in ARTIFACTS:
    if not f.exists():
        raise FileNotFoundError(f"Missing artifact {f} — did 51__export_dashboard_data run first?")

# Sanity check: confirm the token sees the repo.
resp = requests.get(API_BASE, headers=HEADERS, timeout=10)
resp.raise_for_status()
print(f"✓ Authenticated to {OWNER}/{REPO} (private={resp.json().get('private')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers

# COMMAND ----------

def get_release_by_tag(tag: str) -> dict | None:
    r = requests.get(f"{API_BASE}/releases/tags/{tag}", headers=HEADERS, timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def delete_release(release: dict) -> None:
    rid = release["id"]
    requests.delete(f"{API_BASE}/releases/{rid}", headers=HEADERS, timeout=10).raise_for_status()
    print(f"  ✓ deleted release id={rid}")


def delete_ref(tag: str) -> None:
    # Releases keep their git tag around even after the release is deleted.
    # Delete the tag ref so the next create_release() can recreate it cleanly.
    r = requests.delete(f"{API_BASE}/git/refs/tags/{tag}", headers=HEADERS, timeout=10)
    if r.status_code in (204, 422):  # 422 = ref didn't exist
        print(f"  ✓ removed tag ref refs/tags/{tag}")
    else:
        r.raise_for_status()


def create_release(tag: str, name: str, body: str, target_commitish: str = "main") -> dict:
    payload = {
        "tag_name":         tag,
        "target_commitish": target_commitish,
        "name":             name,
        "body":             body,
        "draft":            False,
        "prerelease":       False,
    }
    r = requests.post(f"{API_BASE}/releases", headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()
    rel = r.json()
    print(f"  ✓ created release {tag} (id={rel['id']})")
    return rel


def upload_asset(release: dict, file_path: Path) -> None:
    # GitHub uses a separate upload host; the URL template includes a `{?name,label}` suffix.
    upload_url = release["upload_url"].split("{", 1)[0]
    content_type = (
        "application/octet-stream" if file_path.suffix == ".parquet" else "application/json"
    )
    with file_path.open("rb") as fh:
        r = requests.post(
            f"{upload_url}?name={file_path.name}",
            headers={**HEADERS, "Content-Type": content_type},
            data=fh.read(),
            timeout=60,
        )
    r.raise_for_status()
    size_kb = file_path.stat().st_size / 1024
    print(f"  ✓ uploaded {file_path.name} ({size_kb:.1f} KB)")


def delete_dangling_drafts(tag: str) -> None:
    # `get_release_by_tag` queries /releases/tags/<tag>, which does NOT resolve DRAFT
    # releases — so a half-finished prior run can leave a draft with tag_name=<tag> that
    # the delete-and-recreate below never cleans. A draft also shadows the public download
    # URL (drafts don't expose `releases/download/<tag>/…` → the Streamlit app 404s). Sweep
    # the full release list and drop any draft carrying this tag before we recreate it.
    r = requests.get(f"{API_BASE}/releases", headers=HEADERS, params={"per_page": 100}, timeout=10)
    r.raise_for_status()
    for rel in r.json():
        if rel.get("draft") and rel.get("tag_name") == tag:
            print(f"  ! dangling draft for tag {tag} (id={rel['id']}) — deleting")
            delete_release(rel)


def verify_public_download(tag: str, asset_name: str, attempts: int = 6, delay: float = 5.0) -> None:
    # Mirror exactly what the public app does: an UNAUTHENTICATED GET of the floating-tag
    # download URL. Availability is eventually consistent right after publishing, so retry
    # with a fixed backoff before failing the job loudly (better than a silently-404ing app).
    url = f"https://github.com/{OWNER}/{REPO}/releases/download/{tag}/{asset_name}"
    last = None
    for i in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=20)  # follows the redirect to the signed asset URL
            if resp.status_code == 200:
                print(f"  ✓ public URL reachable: {asset_name} ({len(resp.content):,} bytes)")
                return
            last = resp.status_code
        except requests.RequestException as exc:
            last = repr(exc)
        if i < attempts:
            print(f"    …not ready (last={last}), retry {i}/{attempts - 1} in {delay:.0f}s")
            time.sleep(delay)
    raise RuntimeError(
        f"`{tag}` release published but its public download still fails after {attempts} tries "
        f"(last={last}): {url} — the Streamlit app would 404. Check the release isn't a draft "
        f"and the assets uploaded."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Replace the dated release for today

# COMMAND ----------

meta = json.loads(Path("/tmp/dashboard_meta.json").read_text())
release_body = (
    f"Automated dashboard data export.\n\n"
    f"- **Build timestamp:** {meta['build_timestamp']}\n"
    f"- **Tickers:** {len(meta['tickers'])}\n"
    f"- **Financials rows:** {meta['row_counts']['financials']:,}\n"
    f"- **Metrics rows:** {meta['row_counts']['metrics']:,}\n"
    f"- **Schema version:** {meta['schema_version']}\n"
)

print(f"Replacing dated release {DATED_TAG}...")
existing = get_release_by_tag(DATED_TAG)
if existing:
    delete_release(existing)
    delete_ref(DATED_TAG)

dated_release = create_release(DATED_TAG, name=f"Dashboard data — {DATED_TAG}", body=release_body)
for f in ARTIFACTS:
    upload_asset(dated_release, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Move the `latest` floating release

# COMMAND ----------

print(f"Replacing floating release {LATEST_TAG}...")
existing_latest = get_release_by_tag(LATEST_TAG)
if existing_latest:
    delete_release(existing_latest)
delete_dangling_drafts(LATEST_TAG)  # catch drafts the tag lookup above can't see
delete_ref(LATEST_TAG)              # always clear the tag ref (tolerates 422 if absent)

latest_release = create_release(
    LATEST_TAG,
    name=f"latest → {DATED_TAG}",
    body=(
        f"Floating tag — always points to the most recent dashboard data export.\n\n"
        f"Current backing build: **{DATED_TAG}** ({meta['build_timestamp']})\n\n"
        f"The public Streamlit app fetches its data from this tag."
    ),
)
for f in ARTIFACTS:
    upload_asset(latest_release, f)

# Fail the job if `latest` didn't actually become publicly fetchable — otherwise the
# app silently 404s and shows "datos aún no publicados" despite a "successful" run.
if latest_release.get("draft"):
    raise RuntimeError(f"`{LATEST_TAG}` came back as a draft — the app would 404. Aborting.")
verify_public_download(LATEST_TAG, ARTIFACTS[-1].name)  # dashboard_meta.json (smallest)

print(f"\n✓ Published {len(ARTIFACTS)} artifact(s) to {DATED_TAG} and {LATEST_TAG}")
print(f"  https://github.com/{OWNER}/{REPO}/releases/tag/{LATEST_TAG}")
