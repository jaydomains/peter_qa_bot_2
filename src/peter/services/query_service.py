from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


class QueryService:
    """Placeholder for Milestone M5."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings

    def summary(self, site_code: str, days: int = 30) -> str:
        raise NotImplementedError("M5 not implemented yet")

    def latest(self, site_code: str) -> str:
        raise NotImplementedError("M5 not implemented yet")

    def fails(self, site_code: str, days: int = 30) -> str:
        raise NotImplementedError("M5 not implemented yet")

    def top_issues(self, site_code: str, days: int = 30) -> str:
        raise NotImplementedError("M5 not implemented yet")
