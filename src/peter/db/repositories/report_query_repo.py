from __future__ import annotations

import sqlite3


class ReportQueryRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def latest_report_row(self, *, site_id: int):
        return self.conn.execute(
            """
            SELECT id, report_code, received_at, result
            FROM reports
            WHERE site_id = ?
            ORDER BY received_at DESC
            LIMIT 1
            """,
            (site_id,),
        ).fetchone()

    def summary_counts(self, *, site_id: int, days: int):
        return self.conn.execute(
            """
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN result='PASS' THEN 1 ELSE 0 END) as pass,
              SUM(CASE WHEN result='WARN' THEN 1 ELSE 0 END) as warn,
              SUM(CASE WHEN result='FAIL' THEN 1 ELSE 0 END) as fail,
              SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM reports
            WHERE site_id = ?
              AND received_at >= datetime('now', ?)
            """,
            (site_id, f"-{int(days)} days"),
        ).fetchone()

    def fails_since(self, *, site_id: int, days: int):
        return self.conn.execute(
            """
            SELECT id, report_code, received_at
            FROM reports
            WHERE site_id = ?
              AND result='FAIL'
              AND received_at >= datetime('now', ?)
            ORDER BY received_at DESC
            """,
            (site_id, f"-{int(days)} days"),
        ).fetchall()

    def top_issues_since(self, *, site_id: int, days: int, limit: int = 10):
        return self.conn.execute(
            """
            SELECT i.category as category, COUNT(*) as n
            FROM issues i
            JOIN reports r ON r.id = i.report_id
            WHERE r.site_id = ?
              AND r.received_at >= datetime('now', ?)
            GROUP BY i.category
            ORDER BY n DESC
            LIMIT ?
            """,
            (site_id, f"-{int(days)} days", int(limit)),
        ).fetchall()
