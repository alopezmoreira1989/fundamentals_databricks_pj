"""Admin hardening: IP allowlist for ADMIN_URL_PATH (GitHub issue #181, item 3).

config.middleware.AdminIPAllowlistMiddleware no-ops when ADMIN_IP_ALLOWLIST is empty (the
default) and 403s otherwise. ADMIN_URL_PATH itself is resolved into urlpatterns at import time,
so these tests exercise the middleware against the default "/admin/" route rather than
reconfiguring the path per test.
"""

from __future__ import annotations

from config.middleware import _client_ip
from django.test import RequestFactory


def test_admin_reachable_when_allowlist_empty(client):
    # Default settings: no ADMIN_IP_ALLOWLIST configured -> middleware never blocks. The admin
    # redirects unauthenticated requests to login (302), which is proof the middleware let it
    # through rather than 403ing it itself.
    resp = client.get("/admin/")
    assert resp.status_code != 403


def test_admin_blocked_for_ip_outside_allowlist(client, settings):
    settings.ADMIN_IP_ALLOWLIST = ["203.0.113.0/24"]  # TEST-NET-3, doesn't include 127.0.0.1
    resp = client.get("/admin/")  # Django test client defaults REMOTE_ADDR to 127.0.0.1
    assert resp.status_code == 403


def test_admin_allowed_for_ip_inside_allowlist(client, settings):
    settings.ADMIN_IP_ALLOWLIST = ["127.0.0.1/32"]
    resp = client.get("/admin/")
    assert resp.status_code != 403


def test_non_admin_paths_never_blocked(client, settings):
    settings.ADMIN_IP_ALLOWLIST = ["203.0.113.0/24"]
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_client_ip_prefers_forwarded_header_for_single_proxy_hop(settings):
    settings.NUM_PROXIES = 1
    req = RequestFactory().get("/admin/", HTTP_X_FORWARDED_FOR="198.51.100.7", REMOTE_ADDR="10.0.0.1")
    assert _client_ip(req) == "198.51.100.7"


def test_client_ip_trusts_only_num_proxies_hops_from_the_right(settings):
    # A client-supplied fake prefix ("attacker") shouldn't be trusted: only the entry NUM_PROXIES
    # hops from the right (the one the proxy itself appended) is the real client IP.
    settings.NUM_PROXIES = 1
    req = RequestFactory().get(
        "/admin/", HTTP_X_FORWARDED_FOR="attacker-spoofed, 198.51.100.7", REMOTE_ADDR="10.0.0.1"
    )
    assert _client_ip(req) == "198.51.100.7"


def test_client_ip_falls_back_to_remote_addr_without_forwarded_header(settings):
    req = RequestFactory().get("/admin/", REMOTE_ADDR="10.0.0.1")
    assert _client_ip(req) == "10.0.0.1"
