"""Users auth views. Login/logout use Django's built-in class-based views (wired in urls.py);
signup validates via ``SignupForm`` then creates through the service layer and logs the user in.
"""

from __future__ import annotations

from django.contrib.auth import login
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from . import services
from .forms import SignupForm


def signup(request: HttpRequest) -> HttpResponse:
    """Register a new account, then sign the user in and send them home."""
    if request.user.is_authenticated:
        return redirect("home")
    if request.method == "POST":
        form = SignupForm(request.POST)
        if form.is_valid():
            data = form.cleaned_data
            user = services.create_user(
                username=data["username"],
                email=data["email"],
                password=data["password1"],
            )
            login(request, user)  # single configured backend ⇒ inferred automatically
            return redirect("home")
    else:
        form = SignupForm()
    return render(request, "registration/signup.html", {"form": form})
