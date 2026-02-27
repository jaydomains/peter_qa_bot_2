from __future__ import annotations

import sqlite3


class ReportIssueRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def delete_for_report(self, report_id: int) -> int:
        cur = self.conn.execute("DELETE FROM issues WHERE report_id = ?", (report_id,))
        return int(cur.rowcount)
