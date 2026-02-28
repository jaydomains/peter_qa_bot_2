from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from pathlib import Path

from peter.config.settings import Settings
from peter.db.connection import get_connection
from peter.db.schema import init_db
from peter.services.report_service import ReportService
from peter.services.site_service import SiteService
from peter.services.spec_service import SpecService

log = logging.getLogger("peter.daemon")


@dataclass(frozen=True)
class DaemonConfig:
    tick_seconds: float = 5.0
    crash_backoff_seconds: float = 10.0


class _Stop:
    requested: bool = False


def _handle_stop(signum: int, _frame) -> None:
    _Stop.requested = True
    log.info("Received signal %s; shutting down...", signum)


def _safe_move(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        # Avoid overwriting if someone drops the same name twice.
        ts = time.strftime("%Y%m%d-%H%M%S")
        dest = dest.with_name(f"{dest.stem}__{ts}{dest.suffix}")
    src.rename(dest)


def process_inbox_once(*, settings: Settings) -> None:
    """Process any PDFs dropped into data/INBOX.

    Layout (Option 2):
      data/INBOX/spec/<SITE>/<VERSION>.pdf
      data/INBOX/report/<SITE>/<INSPECTION_REF>.pdf

    On success, files are moved to:
      data/INBOX/processed/... (mirrors structure)

    On error, files are moved to:
      data/INBOX/quarantine/...
    """

    inbox = settings.DATA_DIR / "INBOX"
    spec_root = inbox / "spec"
    report_root = inbox / "report"

    spec_files = sorted(spec_root.glob("*/*.pdf"))
    report_files = sorted(report_root.glob("*/*.pdf"))

    if not spec_files and not report_files:
        return

    with get_connection(settings.DB_PATH) as conn:
        init_db(conn)
        site_svc = SiteService(conn, settings)
        spec_svc = SpecService(conn, settings)
        report_svc = ReportService(conn, settings)

        for path in spec_files:
            site_code = path.parent.name
            version = path.stem.strip()

            try:
                log.info("INBOX spec: site=%s version=%s file=%s", site_code, version, str(path))
                # Ensure site exists (idempotent if already created).
                site_svc.get_site_or_raise(site_code)
                spec_svc.ingest_spec(site_code=site_code, version_label=version, file_path=path)

                dest = inbox / "processed" / "spec" / site_code / path.name
                _safe_move(path, dest)
                log.info("INBOX spec OK -> %s", str(dest))
            except Exception:
                log.exception("INBOX spec FAILED: %s", str(path))
                dest = inbox / "quarantine" / "spec" / site_code / path.name
                try:
                    _safe_move(path, dest)
                    log.info("INBOX spec quarantined -> %s", str(dest))
                except Exception:
                    log.exception("INBOX spec quarantine move failed for %s", str(path))

        for path in report_files:
            site_code = path.parent.name
            inspection_ref = path.stem.strip()

            try:
                log.info("INBOX report: site=%s ref=%s file=%s", site_code, inspection_ref, str(path))
                site_svc.get_site_or_raise(site_code)
                report_svc.ingest_report(site_code=site_code, report_code=inspection_ref, file_path=path)

                dest = inbox / "processed" / "report" / site_code / path.name
                _safe_move(path, dest)
                log.info("INBOX report OK -> %s", str(dest))
            except Exception:
                log.exception("INBOX report FAILED: %s", str(path))
                dest = inbox / "quarantine" / "report" / site_code / path.name
                try:
                    _safe_move(path, dest)
                    log.info("INBOX report quarantined -> %s", str(dest))
                except Exception:
                    log.exception("INBOX report quarantine move failed for %s", str(path))


def run(*, cfg: DaemonConfig | None = None) -> int:
    """Run the long-lived daemon loop."""

    if cfg is None:
        cfg = DaemonConfig()

    # Signal handlers must be set in the main thread.
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    settings = Settings.load()
    settings.ensure_paths_exist()

    log.info("PETER daemon starting (tick=%.1fs) data_dir=%s", cfg.tick_seconds, str(settings.DATA_DIR))

    while not _Stop.requested:
        try:
            process_inbox_once(settings=settings)
            time.sleep(cfg.tick_seconds)
        except Exception:
            log.exception("Unhandled error in daemon loop; retrying in %.1fs", cfg.crash_backoff_seconds)
            time.sleep(cfg.crash_backoff_seconds)

    log.info("PETER daemon stopped")
    return 0
