from __future__ import annotations

import sqlite3
from importlib.resources import files


def init_db(conn: sqlite3.Connection) -> None:
    """Initialize DB schema (idempotent)."""
    sql = (files("peter.db") / "schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)
