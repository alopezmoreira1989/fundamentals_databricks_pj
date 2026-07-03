"""Watchlist views — all login-required. An overview lists the user's named lists; each list has
its own page. Create/rename/delete lists and add/remove tickers are POST-only and redirect back to
a safe ``next`` (default: the overview). Add/remove take an optional ``watchlist`` id (blank → the
user's default list), so the original single-list controls keep working unchanged.
"""

from __future__ import annotations

from typing import cast

from django.contrib.auth.decorators import login_required
from django.http import Http404, HttpRequest, HttpResponse
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
def overview(request: HttpRequest) -> HttpResponse:
    """All of the user's lists, each with its item count."""
    user = cast(User, request.user)  # @login_required guarantees an authenticated User
    watchlists = services.list_watchlists_with_counts(user)
    return render(request, "watchlists/overview.html", {"watchlists": watchlists})


@login_required
def detail(request: HttpRequest, watchlist_id: str) -> HttpResponse:
    """One list: each ticker with its company summary (if known)."""
    user = cast(User, request.user)
    watchlist = services.get_watchlist(user, watchlist_id)
    if watchlist is None:
        raise Http404("unknown watchlist")
    items = [(t, company_services.get_company_summary(t)) for t in services.list_tickers(watchlist)]
    return render(request, "watchlists/detail.html", {"watchlist": watchlist, "items": items})


@login_required
@require_POST
def create(request: HttpRequest) -> HttpResponse:
    name = request.POST.get("name", "").strip()
    if name:
        services.create(cast(User, request.user), name)
    return redirect(_safe_next(request, reverse("watchlists:overview")))


@login_required
@require_POST
def rename(request: HttpRequest, watchlist_id: str) -> HttpResponse:
    name = request.POST.get("name", "").strip()
    if name:
        services.rename(cast(User, request.user), watchlist_id, name)
    return redirect(_safe_next(request, reverse("watchlists:overview")))


@login_required
@require_POST
def delete(request: HttpRequest, watchlist_id: str) -> HttpResponse:
    services.delete(cast(User, request.user), watchlist_id)
    return redirect(_safe_next(request, reverse("watchlists:overview")))


@login_required
@require_POST
def add(request: HttpRequest) -> HttpResponse:
    ticker = request.POST.get("ticker", "").strip().upper()
    watchlist_id = request.POST.get("watchlist") or None
    new_list_name = request.POST.get("watchlist_name", "").strip()
    if ticker:
        user = cast(User, request.user)
        if new_list_name:  # picker's "new list" field: create-or-reuse the named list, then add
            services.add_to_named_list(user, ticker, new_list_name)
        else:
            services.add(user, ticker, watchlist_id)
    return redirect(_safe_next(request, reverse("watchlists:overview")))


@login_required
@require_POST
def remove(request: HttpRequest) -> HttpResponse:
    ticker = request.POST.get("ticker", "").strip().upper()
    watchlist_id = request.POST.get("watchlist") or None
    if ticker:
        services.remove(cast(User, request.user), ticker, watchlist_id)
    return redirect(_safe_next(request, reverse("watchlists:overview")))
