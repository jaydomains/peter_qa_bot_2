from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Tuple

from peter.config.settings import Settings
from peter.db.connection import get_connection
from peter.db.schema import init_db

from .state import ConversationState

ALLOWED_ACTIONS = {
    "none",
    "create_site",
    "ingest_spec",
    "ingest_report",
    "list_reports",
    "ask_qa",
}

REQUIRED_SLOTS: Dict[str, List[str]] = {
    "create_site": ["site_code", "site_name", "address"],
    "ingest_spec": ["spec_version"],
    "ingest_report": ["report_code"],
    "ask_qa": ["question"],
}


@contextmanager
def _service_context() -> Tuple[Any, Settings]:
    settings = Settings.load()
    settings.ensure_paths_exist()
    with get_connection(settings.DB_PATH) as conn:
        init_db(conn)
        yield conn, settings


def validate_slots(action: str, slots: Dict[str, Any], state: ConversationState) -> List[str]:
    required = REQUIRED_SLOTS.get(action, [])
    missing = [slot for slot in required if not slots.get(slot)]

    if action in {"ingest_spec", "ingest_report", "list_reports", "ask_qa"} and not state.site_code:
        missing.append("site_code (set an active site first)")

    if action == "ingest_spec" and not slots.get("file_path"):
        missing.append("file_path (upload the spec PDF)")

    if action == "ingest_report" and not slots.get("file_path"):
        missing.append("file_path (upload the report PDF)")

    if action == "ask_qa" and not (state.report_code or slots.get("report_code")):
        missing.append("report_code (ingest or specify a report first)")

    return missing


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value).strip()


def run_action(action: str, slots: Dict[str, Any], state: ConversationState) -> str:
    try:
        action = _safe_str(action) or "none"
        if action not in ALLOWED_ACTIONS:
            return f"Unknown action: {action}"

        if action == "none":
            return ""

        missing = validate_slots(action, slots, state)
        if missing:
            return f"Missing required fields for {action}: {', '.join(missing)}"

        if action == "create_site":
            from peter.services.site_service import SiteService

            with _service_context() as (conn, settings):
                svc = SiteService(conn, settings)
                site = svc.create_site(
                    site_code=_safe_str(slots["site_code"]),
                    site_name=_safe_str(slots["site_name"]),
                    address=_safe_str(slots["address"]),
                )

            state.site_code = site.site_code
            state.site_name = site.site_name
            state.address = site.address
            state.save()

            return f"Site created: {site.site_code} — {site.site_name}"

        if action == "ingest_spec":
            from peter.services.spec_service import SpecService

            file_path = Path(_safe_str(slots.get("file_path")))
            with _service_context() as (conn, settings):
                svc = SpecService(conn, settings)
                spec = svc.ingest_spec(
                    site_code=state.site_code,
                    version_label=_safe_str(slots["spec_version"]),
                    file_path=file_path,
                )

            state.spec_version = spec.version_label
            state.save()

            return f"Spec ingested: version {state.spec_version}"

        if action == "ingest_report":
            from peter.services.report_service import ReportService

            file_path = Path(_safe_str(slots.get("file_path")))
            with _service_context() as (conn, settings):
                svc = ReportService(conn, settings)
                out = svc.ingest_report(
                    site_code=state.site_code,
                    report_code=_safe_str(slots["report_code"]),
                    file_path=file_path,
                )

            state.report_code = _safe_str(slots["report_code"])
            state.save()

            status = out.get("status", "ok") if isinstance(out, dict) else "ok"
            return f"Report ingest {status}: {state.report_code}"

        if action == "list_reports":
            with _service_context() as (conn, _settings):
                rows = conn.execute(
                    """
                    SELECT r.report_code, r.received_at, r.result
                    FROM reports r
                    JOIN sites s ON s.id = r.site_id
                    WHERE s.site_code = ?
                    ORDER BY r.received_at DESC
                    LIMIT 10
                    """,
                    (state.site_code,),
                ).fetchall()

            if not rows:
                return "No reports found for this site."

            lines = [f"Reports for {state.site_code}:"]
            for r in rows:
                result = r["result"] or "pending"
                received = r["received_at"] or "?"
                lines.append(f"• {r['report_code']} — {received} ({result})")
            return "\n".join(lines)

        if action == "ask_qa":
            from peter.interfaces.qa.ask import answer_report_question

            report_code = slots.get("report_code") or state.report_code
            with _service_context() as (conn, settings):
                answer = answer_report_question(
                    conn=conn,
                    settings=settings,
                    site_code=state.site_code,
                    report_code=_safe_str(report_code),
                    question=_safe_str(slots["question"]),
                )
            return answer

        return f"Unknown action: {action}"

    except Exception as exc:
        return f"Action failed: {type(exc).__name__}: {exc}"
