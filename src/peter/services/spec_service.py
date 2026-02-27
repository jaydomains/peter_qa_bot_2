from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


class SpecService:
    """Placeholder for Milestone M2."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings

    def ingest_spec(self, site_code: str, version_label: str, file_path):
        raise NotImplementedError("M2 not implemented yet")
