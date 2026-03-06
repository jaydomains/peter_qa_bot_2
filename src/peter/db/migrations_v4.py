from __future__ import annotations

import sqlite3


def migrate(conn: sqlite3.Connection) -> None:
    # Add new columns if missing
    def cols(table: str) -> set[str]:
        return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    sc = cols("sites")
    if "site_name_raw" not in sc:
        conn.execute("ALTER TABLE sites ADD COLUMN site_name_raw TEXT")
    if "supplier_client" not in sc:
        conn.execute("ALTER TABLE sites ADD COLUMN supplier_client TEXT")
    if "contractor_on_site" not in sc:
        conn.execute("ALTER TABLE sites ADD COLUMN contractor_on_site TEXT")

    rc = cols("reports")
    for name, ddl in [
        ("observed_site_name_raw", "observed_site_name_raw TEXT"),
        ("observed_site_name_display", "observed_site_name_display TEXT"),
        ("observed_address", "observed_address TEXT"),
        ("observed_supplier_client", "observed_supplier_client TEXT"),
        ("observed_contractor_on_site", "observed_contractor_on_site TEXT"),
    ]:
        if name not in rc:
            conn.execute(f"ALTER TABLE reports ADD COLUMN {ddl}")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS site_aliases (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_id INTEGER NOT NULL,
          alias_code TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          CONSTRAINT fk_alias_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_site_alias_site_id ON site_aliases(site_id);
        """
    )
