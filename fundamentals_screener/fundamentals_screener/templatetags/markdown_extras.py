from __future__ import annotations

import markdown as _markdown
from django import template
from django.utils.safestring import mark_safe

register = template.Library()


@register.filter(name="markdownify")
def markdownify(text: str):
    """Render Update.content (Markdown) to HTML.

    Content is written exclusively through Django admin by the project maintainer — never
    user-submitted — so this renders with mark_safe and no additional HTML sanitization, the
    same trust level as any other admin-authored copy in this package's templates.
    """
    html = _markdown.markdown(text or "", extensions=["fenced_code", "tables", "sane_lists"])
    return mark_safe(html)
