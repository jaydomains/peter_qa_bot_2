from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class SpecRow:
    id: int
    site_id: int
    version_label: str
    filename: str
    sha256: str
    stored_path: str
    extracted_text_path: str | None
    checklist_json_path: str | None
    is_active: bool


class SpecRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_by_site_sha(self, site_id: int, sha256: str) -> SpecRow | None:
        r = self.conn.execute(
            """
            SELECT id, site_id, version_label, filename, sha256, stored_path,
                   extracted_text_path, checklist_json_path, is_active
            FROM specs
            WHERE site_id = ? AND sha256 = ?
            """,
            (site_id, sha256),
        ).fetchone()
        if not r:
            return None
        return SpecRow(
            id=int(r["id"]),
            site_id=int(r["site_id"]),
            version_label=str(r["version_label"]),
            filename=str(r["filename"]),
            sha256=str(r["sha256"]),
            stored_path=str(r["stored_path"]),
            extracted_text_path=(str(r["extracted_text_path"]) if r["extracted_text_path"] else None),
            checklist_json_path=(str(r["checklist_json_path"]) if r["checklist_json_path"] else None),
            is_active=bool(int(r["is_active"])),
        )

    def deactivate_all_for_site(self, site_id: int) -> None:
        self.conn.execute("UPDATE specs SET is_active = 0 WHERE site_id = ?", (site_id,))

    def insert(
        self,
        *,
        site_id: int,
        version_label: str,
        filename: str,
        sha256: str,
        stored_path: str,
        extracted_text_path: str | None,
        checklist_json_path: str | None,
        is_active: bool,
    ) -> SpecRow:
        cur = self.conn.execute(
            """
            INSERT INTO specs (
              site_id, version_label, filename, sha256, stored_path,
              extracted_text_path, checklist_json_path, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                version_label,
                filename,
                sha256,
                stored_path,
                extracted_text_path,
                checklist_json_path,
                1 if is_active else 0,
            ),
        )
        spec_id = int(cur.lastrowid)
        return SpecRow(
            id=spec_id,
            site_id=site_id,
            version_label=version_label,
            filename=filename,
            sha256=sha256,
            stored_path=stored_path,
            extracted_text_path=extracted_text_path,
            checklist_json_path=checklist_json_path,
            is_active=is_active,
        )

    def set_site_active_spec(self, site_id: int, spec_id: int) -> None:
        self.conn.execute("UPDATE sites SET active_spec_id = ? WHERE id = ?", (spec_id, site_id))
