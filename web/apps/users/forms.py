"""Auth forms for the users app.

``SignupForm`` extends Django's ``UserCreationForm`` only to *validate* the input (username +
email uniqueness, password confirmation + validators). Actual creation is delegated to the
service layer by the view, so the view→service path stays intact.
"""

from __future__ import annotations

from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm

User = get_user_model()


class SignupForm(UserCreationForm):
    email = forms.EmailField(required=True)

    # Defined standalone (not inheriting UserCreationForm.Meta, which django-stubs can't see);
    # password1/password2 are declared on UserCreationForm itself, not via Meta.fields.
    class Meta:
        model = User
        fields = ("username", "email")
