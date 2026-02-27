from __future__ import annotations

import re
from pathlib import Path

from peter.config.settings import Settings
from peter.domain.errors import ValidationError


_slug_re = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = _slug_re.sub("-", s).strip("-")
    return s or "site"


def validate_site_code(code: str) -> str:
    c = (code or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{3,20}", c):
        raise ValidationError("site_code must be 3-20 chars, A-Z0-9 only")
    return c


def site_folder_name(site_code: str, site_name: str) -> str:
    return f"{validate_site_code(site_code)}__{slugify(site_name)}"


def sites_root(settings: Settings) -> Path:
    return (settings.QA_ROOT / "SITES").resolve()


def site_root(settings: Settings, folder_name: str) -> Path:
    # folder_name is stored in DB, but we still treat it carefully.
    root = sites_root(settings)
    p = (root / folder_name).resolve()
    try:
        p.relative_to(root)
    except ValueError as e:
        raise ValidationError("Invalid folder_name: escapes SITES root") from e
    return p
