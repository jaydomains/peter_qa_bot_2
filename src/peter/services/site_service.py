from __future__ import annotations

import sqlite3

from peter.config.settings import Settings
from peter.db.repositories.site_repo import SiteRepository
from peter.domain.errors import ValidationError
from peter.domain.models import Site
from peter.storage.filestore import ensure_site_folders
from peter.storage.paths import site_folder_name, validate_site_code


class SiteService:
    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings
        self.repo = SiteRepository(conn)

    def get_site_or_raise(self, site_code: str) -> Site:
        code = validate_site_code(site_code)
        site = self.repo.get_by_code(code)
        if not site:
            raise ValidationError(f"Unknown site_code: {code} (create it first)")
        # Ensure folders exist (useful if created in an older version)
        ensure_site_folders(self.settings, folder_name=site.folder_name)
        return site

    def create_site(self, *, site_code: str, site_name: str, address: str = "") -> Site:
        code = validate_site_code(site_code)
        name = (site_name or "").strip()
        if not name:
            raise ValidationError("site_name is required")

        existing = self.repo.get_by_code(code)
        if existing:
            # idempotent-ish: ensure folders exist and return existing
            ensure_site_folders(self.settings, folder_name=existing.folder_name)
            return existing

        folder = site_folder_name(code, name)
        site = self.repo.create(site_code=code, site_name=name, address=address or "", folder_name=folder)
        ensure_site_folders(self.settings, folder_name=folder)
        return site

    def list_sites(self) -> list[Site]:
        return self.repo.list_all()
