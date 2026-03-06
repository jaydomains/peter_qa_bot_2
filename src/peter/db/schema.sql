PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS schema_version (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL,
  applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
INSERT OR IGNORE INTO schema_version (id, version) VALUES (1, 6);

CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_code TEXT NOT NULL UNIQUE,
  site_name TEXT NOT NULL,
  site_name_raw TEXT,
  address TEXT,
  supplier_client TEXT,
  contractor_on_site TEXT,
  project_type TEXT CHECK (project_type IN ('NEW_WORK','REDEC')),
  folder_name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  active_spec_id INTEGER,
  CONSTRAINT fk_sites_active_spec FOREIGN KEY (active_spec_id) REFERENCES specs(id)
);
CREATE INDEX IF NOT EXISTS idx_sites_site_code ON sites(site_code);

CREATE TABLE IF NOT EXISTS specs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  version_label TEXT NOT NULL,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  stored_path TEXT NOT NULL,
  extracted_text_path TEXT,
  checklist_json_path TEXT,
  uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
  is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0,1)),
  UNIQUE(site_id, version_label),
  UNIQUE(site_id, sha256),
  CONSTRAINT fk_specs_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_specs_site_id ON specs(site_id);
CREATE INDEX IF NOT EXISTS idx_specs_active ON specs(site_id, is_active);
-- Enforce a single active spec per site
CREATE UNIQUE INDEX IF NOT EXISTS ux_specs_one_active_per_site ON specs(site_id) WHERE is_active = 1;

CREATE TABLE IF NOT EXISTS reports (
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

  observed_site_name_raw TEXT,
  observed_site_name_display TEXT,
  observed_address TEXT,
  observed_supplier_client TEXT,
  observed_contractor_on_site TEXT,

  UNIQUE(site_id, sha256),
  CONSTRAINT fk_reports_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
  CONSTRAINT fk_reports_spec FOREIGN KEY (spec_id_used) REFERENCES specs(id)
);
CREATE INDEX IF NOT EXISTS idx_reports_site_id ON reports(site_id);
CREATE INDEX IF NOT EXISTS idx_reports_site_result ON reports(site_id, result);
CREATE INDEX IF NOT EXISTS idx_reports_received_at ON reports(received_at);

CREATE TABLE IF NOT EXISTS issues (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_id INTEGER NOT NULL,
  issue_type TEXT NOT NULL CHECK (issue_type IN ('SPEC_DEVIATION','BEST_PRACTICE_RISK','INTERNAL_GOVERNANCE','CONTINUITY_NOTE','INFO')),
  category TEXT NOT NULL,
  description TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('LOW','MED','HIGH','CRITICAL')),
  is_blocking INTEGER NOT NULL DEFAULT 0 CHECK (is_blocking IN (0,1)),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT fk_issues_report FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_issues_report_id ON issues(report_id);
CREATE INDEX IF NOT EXISTS idx_issues_type_sev ON issues(issue_type, severity);

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

CREATE TABLE IF NOT EXISTS feedback_signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  report_id INTEGER,
  source TEXT NOT NULL CHECK (source IN ('client_email','internal_email','manual')),
  signal_type TEXT NOT NULL,
  description TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT fk_feedback_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
  CONSTRAINT fk_feedback_report FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_feedback_site_id ON feedback_signals(site_id);

CREATE TABLE IF NOT EXISTS email_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER,
  graph_message_id TEXT,
  internet_message_id TEXT,
  conversation_id TEXT,
  subject TEXT,
  from_address TEXT,
  to_addresses TEXT,
  cc_addresses TEXT,
  has_external_recipients INTEGER NOT NULL DEFAULT 0 CHECK (has_external_recipients IN (0,1)),
  command_type TEXT,
  archived_eml_path TEXT,
  received_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT fk_email_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_email_received_at ON email_events(received_at);
CREATE INDEX IF NOT EXISTS idx_email_site_id ON email_events(site_id);

-- Email attachments (audit + idempotency)
CREATE TABLE IF NOT EXISTS site_aliases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  alias_code TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT fk_alias_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_site_alias_site_id ON site_aliases(site_id);

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

CREATE TRIGGER IF NOT EXISTS trg_sites_updated_at
AFTER UPDATE ON sites
FOR EACH ROW
BEGIN
  UPDATE sites SET updated_at = datetime('now') WHERE id = OLD.id;
END;
