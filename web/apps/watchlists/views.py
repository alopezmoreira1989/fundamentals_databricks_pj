"""Watchlist views — all login-required. The page lists the user's tickers (with company
names); add/remove are POST-only and redirect back to a safe ``next`` (default: the page).
"""

from __future__ import annotations

from typing import cast

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_POST

from apps.companies import services as company_services
from apps.users.models import User

from . import services


def _safe_next(request: HttpRequest, default: str) -> str:
    """Return the POSTed ``next`` if it's a safe local URL, else ``default``."""
    nxt = request.POST.get("next", "")
    if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
        return nxt
    return default


@login_required
def watchlist_page(request: HttpRequest) -> HttpResponse:
    """The user's watchlist: each ticker with its company summary (if known)."""
    user = cast(User, request.user)  # @login_required guarantees an authenticated User
    tickers = services.list_tickers(user)
    items = [(t, company_services.get_company_summary(t)) for t in tickers]
    return render(request, "watchlists/list.html", {"items": items})


@login_required
@require_POST
def add(request: HttpRequest) -> HttpResponse:
    ticker = request.POST.get("ticker", "").strip().upper()
    if ticker:
        services.add(cast(User, request.user), ticker)
    return redirect(_safe_next(request, reverse("watchlists:list")))


@login_required
@require_POST
def remove(request: HttpRequest) -> HttpResponse:
    ticker = request.POST.get("ticker", "").strip().upper()
    if ticker:
        services.remove(cast(User, request.user), ticker)
    return redirect(_safe_next(request, reverse("watchlists:list")))
