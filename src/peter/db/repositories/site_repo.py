from __future__ import annotations

import sqlite3
from typing import Iterable

from peter.domain.models import Site


class SiteRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(self, *, site_code: str, site_name: str, address: str, folder_name: str) -> Site:
        cur = self.conn.execute(
            """
            INSERT INTO sites (site_code, site_name, address, folder_name)
            VALUES (?, ?, ?, ?)
            """,
            (site_code, site_name, address, folder_name),
        )
        site_id = int(cur.lastrowid)
        return Site(
            id=site_id,
            site_code=site_code,
            site_name=site_name,
            address=address,
            folder_name=folder_name,
            active_spec_id=None,
        )

    def list_all(self) -> list[Site]:
        rows = self.conn.execute(
            "SELECT id, site_code, site_name, address, folder_name, active_spec_id FROM sites ORDER BY site_code"
        ).fetchall()
        return [
            Site(
                id=int(r["id"]),
                site_code=str(r["site_code"]),
                site_name=str(r["site_name"]),
                address=str(r["address"] or ""),
                folder_name=str(r["folder_name"]),
                active_spec_id=(int(r["active_spec_id"]) if r["active_spec_id"] is not None else None),
            )
            for r in rows
        ]

    def get_by_code(self, site_code: str) -> Site | None:
        r = self.conn.execute(
            "SELECT id, site_code, site_name, address, folder_name, active_spec_id FROM sites WHERE site_code = ?",
            (site_code,),
        ).fetchone()
        if not r:
            return None
        return Site(
            id=int(r["id"]),
            site_code=str(r["site_code"]),
            site_name=str(r["site_name"]),
            address=str(r["address"] or ""),
            folder_name=str(r["folder_name"]),
            active_spec_id=(int(r["active_spec_id"]) if r["active_spec_id"] is not None else None),
        )
