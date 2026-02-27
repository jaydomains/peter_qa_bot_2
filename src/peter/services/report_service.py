from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


class ReportService:
    """Placeholder for Milestone M3/M4."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings

    def ingest_report(self, site_code: str, report_code: str, file_path):
        raise NotImplementedError("M3/M4 not implemented yet")
