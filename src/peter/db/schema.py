from __future__ import annotations

import sqlite3
from importlib.resources import files


def init_db(conn: sqlite3.Connection) -> None:
    """Initialize DB schema and apply lightweight migrations.

    This project intentionally avoids a heavy migration tool for now; we keep a
    single schema.sql plus a few in-code migrations.
    """

    sql = (files("peter.db") / "schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)

    # Apply migrations based on schema_version
    vrow = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    version = int(vrow["version"]) if vrow else 0

    # If an existing DB has version 1, migrate reports.result to nullable.
    if version < 2:
        _migrate_v1_to_v2(conn)
        conn.execute("UPDATE schema_version SET version = 2, applied_at = datetime('now') WHERE id = 1")
        version = 2

    # v3: add email_attachments table
    if version < 3:
        _migrate_v2_to_v3(conn)
        conn.execute("UPDATE schema_version SET version = 3, applied_at = datetime('now') WHERE id = 1")
        version = 3

    # v4: site identity fields + site_aliases + report observed identity
    if version < 4:
        _migrate_v3_to_v4(conn)
        conn.execute("UPDATE schema_version SET version = 4, applied_at = datetime('now') WHERE id = 1")
        version = 4

    # v5: add sites.project_type
    if version < 5:
        _migrate_v4_to_v5(conn)
        conn.execute("UPDATE schema_version SET version = 5, applied_at = datetime('now') WHERE id = 1")
        version = 5

    # v6: issue_confirmations table
    if version < 6:
        _migrate_v5_to_v6(conn)
        conn.execute("UPDATE schema_version SET version = 6, applied_at = datetime('now') WHERE id = 1")
        version = 6

    # v7: issue_confirmation_items + add confirmation fields to issues
    if version < 7:
        _migrate_v6_to_v7(conn)
        conn.execute("UPDATE schema_version SET version = 7, applied_at = datetime('now') WHERE id = 1")


def _migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    """Migration: add site identity fields + report observed identity + site_aliases."""

    from peter.db.migrations_v4 import migrate

    migrate(conn)


def _migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    # Add columns to issues for linkage + disposition
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(issues)").fetchall()}
    if "confirmation_qid" not in cols:
        conn.execute("ALTER TABLE issues ADD COLUMN confirmation_qid TEXT")
    if "confirmation_status" not in cols:
        conn.execute("ALTER TABLE issues ADD COLUMN confirmation_status TEXT")
    if "confirmation_decision" not in cols:
        conn.execute("ALTER TABLE issues ADD COLUMN confirmation_decision TEXT")
    if "confirmation_confirmed_by" not in cols:
        conn.execute("ALTER TABLE issues ADD COLUMN confirmation_confirmed_by TEXT")
    if "confirmation_confirmed_at" not in cols:
        conn.execute("ALTER TABLE issues ADD COLUMN confirmation_confirmed_at TEXT")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS issue_confirmation_items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          confirmation_id INTEGER NOT NULL,
          issue_id INTEGER,
          issue_category TEXT,
          issue_severity TEXT,
          issue_excerpt TEXT,
          decision TEXT CHECK (decision IN ('USED','NOT_USED','MORE_INFO')),
          decided_at TEXT,
          decided_by TEXT,
          CONSTRAINT fk_ici_conf FOREIGN KEY (confirmation_id) REFERENCES issue_confirmations(id) ON DELETE CASCADE,
          CONSTRAINT fk_ici_issue FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ici_conf_id ON issue_confirmation_items(confirmation_id);
        CREATE INDEX IF NOT EXISTS idx_ici_issue_id ON issue_confirmation_items(issue_id);
        """
    )


def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS issue_confirmations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email_event_id INTEGER,
          report_id INTEGER,
          qid TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL CHECK (status IN ('PENDING','CONFIRMED_USED','CONFIRMED_NOT_USED','NEEDS_MORE_INFO','REJECTED','CANCELLED')),
          prompt TEXT,
          response_text TEXT,
          confirmed_by TEXT,
          confirmed_at TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          CONSTRAINT fk_ic_email FOREIGN KEY (email_event_id) REFERENCES email_events(id) ON DELETE SET NULL,
          CONSTRAINT fk_ic_report FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ic_report_id ON issue_confirmations(report_id);
        CREATE INDEX IF NOT EXISTS idx_ic_email_event_id ON issue_confirmations(email_event_id);
        """
    )


def _migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(sites)").fetchall()}
    if "project_type" not in cols:
        conn.execute("ALTER TABLE sites ADD COLUMN project_type TEXT")


def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    """Migration: add email_attachments audit table."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS email_attachments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email_event_id INTEGER NOT NULL,
          filename TEXT NOT NULL,
          content_type TEXT,
          sha256 TEXT NOT NULL,
          stored_path TEXT,
          quarantined INTEGER NOT NULL DEFAULT 0 CHECK (quarantined IN (0,1)),
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          CONSTRAINT fk_email_att_event FOREIGN KEY (email_event_id) REFERENCES email_events(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_email_att_event_id ON email_attachments(email_event_id);
        CREATE INDEX IF NOT EXISTS idx_email_att_sha ON email_attachments(sha256);
        """
    )


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """Migration: allow reports.result to be NULL (placeholder records before analysis)."""

    # Check current table_info to see if result is NOT NULL
    cols = conn.execute("PRAGMA table_info(reports)").fetchall()
    result_col = next((c for c in cols if c["name"] == "result"), None)
    if not result_col:
        return
    notnull = int(result_col["notnull"])  # 1 if NOT NULL
    if notnull == 0:
        return

    # Rebuild reports table (SQLite cannot drop NOT NULL directly)
    conn.executescript(
        """
        PRAGMA foreign_keys=OFF;

        ALTER TABLE reports RENAME TO reports_old;

        CREATE TABLE reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_id INTEGER NOT NULL,
          report_code TEXT NOT NULL,
          filename TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          stored_path TEXT NOT NULL,
          inspection_datetime TEXT,
          issued_datetime TEXT,
          received_at TEXT NOT NULL DEFAULT (datetime('now')),
          spec_id_used INTEGER,
          result TEXT CHECK (result IN ('PASS','WARN','FAIL')),
          review_md_path TEXT,
          review_json_path TEXT,
          UNIQUE(site_id, sha256),
          CONSTRAINT fk_reports_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
          CONSTRAINT fk_reports_spec FOREIGN KEY (spec_id_used) REFERENCES specs(id)
        );

        INSERT INTO reports (
          id, site_id, report_code, filename, sha256, stored_path,
          inspection_datetime, issued_datetime, received_at, spec_id_used,
          result, review_md_path, review_json_path
        )
        SELECT
          id, site_id, report_code, filename, sha256, stored_path,
          inspection_datetime, issued_datetime, received_at, spec_id_used,
          result, review_md_path, review_json_path
        FROM reports_old;

        DROP TABLE reports_old;

        CREATE INDEX IF NOT EXISTS idx_reports_site_id ON reports(site_id);
        CREATE INDEX IF NOT EXISTS idx_reports_site_result ON reports(site_id, result);
        CREATE INDEX IF NOT EXISTS idx_reports_received_at ON reports(received_at);

        PRAGMA foreign_keys=ON;
        """
    )
