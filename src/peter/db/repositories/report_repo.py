from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ReportRow:
    id: int
    site_id: int
    report_code: str
    filename: str
    sha256: str
    stored_path: str
    extracted_text_path: str | None
    received_at: str
    result: str | None
    review_md_path: str | None
    review_json_path: str | None


class ReportRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_by_site_sha(self, site_id: int, sha256: str) -> ReportRow | None:
        r = self.conn.execute(
            """
            SELECT id, site_id, report_code, filename, sha256, stored_path,
                   NULL as extracted_text_path, received_at, result, review_md_path, review_json_path
            FROM reports
            WHERE site_id = ? AND sha256 = ?
            """,
            (site_id, sha256),
        ).fetchone()
        if not r:
            return None
        return ReportRow(
            id=int(r["id"]),
            site_id=int(r["site_id"]),
            report_code=str(r["report_code"]),
            filename=str(r["filename"]),
            sha256=str(r["sha256"]),
            stored_path=str(r["stored_path"]),
            extracted_text_path=None,
            received_at=str(r["received_at"]),
            result=(str(r["result"]) if r["result"] is not None else None),
            review_md_path=(str(r["review_md_path"]) if r["review_md_path"] else None),
            review_json_path=(str(r["review_json_path"]) if r["review_json_path"] else None),
        )

    def insert(
        self,
        *,
        site_id: int,
        report_code: str,
        filename: str,
        sha256: str,
        stored_path: str,
        result: str | None,
        review_md_path: str | None,
        review_json_path: str | None,
        observed_site_name_raw: str | None = None,
        observed_site_name_display: str | None = None,
        observed_address: str | None = None,
        observed_supplier_client: str | None = None,
        observed_contractor_on_site: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO reports (
              site_id, report_code, filename, sha256, stored_path,
              result, review_md_path, review_json_path,
              observed_site_name_raw, observed_site_name_display, observed_address,
              observed_supplier_client, observed_contractor_on_site
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                report_code,
                filename,
                sha256,
                stored_path,
                result,
                review_md_path,
                review_json_path,
                observed_site_name_raw,
                observed_site_name_display,
                observed_address,
                observed_supplier_client,
                observed_contractor_on_site,
            ),
        )
        return int(cur.lastrowid)

    def update_result_and_paths(
        self,
        *,
        report_id: int,
        result: str,
        review_md_path: str | None,
        review_json_path: str | None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE reports
            SET result = ?, review_md_path = ?, review_json_path = ?
            WHERE id = ?
            """,
            (result, review_md_path, review_json_path, report_id),
        )
