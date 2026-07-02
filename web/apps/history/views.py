"""History views — all login-required. The page lists the user's recently-viewed companies
(most-recent first); ``clear`` is POST-only and empties the history, redirecting back.
Recording a view is a side effect of visiting a company page (see ``apps.companies.views``),
not an endpoint here.
"""

from __future__ import annotations

from typing import cast

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from apps.companies import services as company_services
from apps.users.models import User

from . import services


@login_required
def history_page(request: HttpRequest) -> HttpResponse:
    """The user's recently-viewed companies, most-recent first."""
    user = cast(User, request.user)  # @login_required guarantees an authenticated User
    tickers = services.recent_tickers(user)
    items = [(t, company_services.get_company_summary(t)) for t in tickers]
    return render(request, "history/list.html", {"items": items})


@login_required
@require_POST
def clear(request: HttpRequest) -> HttpResponse:
    services.clear(cast(User, request.user))
    return redirect(reverse("history:list"))
