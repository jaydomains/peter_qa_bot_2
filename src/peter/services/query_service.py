from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


from peter.db.repositories.report_query_repo import ReportQueryRepository
from peter.db.repositories.site_repo import SiteRepository
from peter.domain.errors import ValidationError


class QueryService:
    """Milestone M5: site-scoped query engine."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings
        self.site_repo = SiteRepository(conn)
        self.qrepo = ReportQueryRepository(conn)

    def _site_id(self, site_code: str) -> int:
        code = (site_code or "").strip().upper()
        site = self.site_repo.get_by_code(code)
        if not site:
            raise ValidationError(f"Unknown site_code: {code}")
        return site.id

    def summary(self, site_code: str, days: int = 30) -> str:
        site_id = self._site_id(site_code)
        r = self.qrepo.summary_counts(site_id=site_id, days=days)
        return (
            f"SUMMARY last {days}d\n"
            f"total={r['total']} pass={r['pass'] or 0} warn={r['warn'] or 0} fail={r['fail'] or 0} pending={r['pending'] or 0}\n"
        )

    def latest(self, site_code: str) -> str:
        site_id = self._site_id(site_code)
        r = self.qrepo.latest_report_row(site_id=site_id)
        if not r:
            return "No reports for this site.\n"
        return f"LATEST report={r['report_code']} received_at={r['received_at']} result={r['result']}\n"

    def fails(self, site_code: str, days: int = 30) -> str:
        site_id = self._site_id(site_code)
        rows = self.qrepo.fails_since(site_id=site_id, days=days)
        lines = [f"FAILS last {days}d"]
        if not rows:
            lines.append("- None")
        else:
            for r in rows:
                lines.append(f"- {r['report_code']} received_at={r['received_at']}")
        return "\n".join(lines) + "\n"

    def top_issues(self, site_code: str, days: int = 30) -> str:
        site_id = self._site_id(site_code)
        rows = self.qrepo.top_issues_since(site_id=site_id, days=days, limit=10)
        lines = [f"TOP ISSUES last {days}d"]
        if not rows:
            lines.append("- None")
        else:
            for r in rows:
                lines.append(f"- {r['category']}: {r['n']}")
        return "\n".join(lines) + "\n"
