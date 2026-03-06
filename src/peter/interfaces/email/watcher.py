from __future__ import annotations

import base64
import time
import os
import re
import tempfile
import urllib.parse
import urllib.request
import zipfile
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from peter.util.hashing import sha256_bytes
from peter.db.repositories.email_attachment_repo import EmailAttachmentRepository

from peter.config.settings import Settings
from peter.db.connection import get_connection
from peter.db.schema import init_db
from peter.db.repositories.email_repo import EmailEventRepository
from peter.db.repositories.site_repo import SiteRepository
from peter.interfaces.email.classifier import parse_subject
from peter.interfaces.email.confirm_commands import parse_confirm_subject, parse_confirm_freeform
from peter.interfaces.qa.openai_ask import ask_openai_responses
from peter.interfaces.email.quarantine_queue import load_quarantine_item, save_quarantine_item, update_status, list_items
from peter.interfaces.email.report_identity import infer_from_pdf_bytes
from peter.interfaces.email.graph_auth import client_credentials_token
from peter.interfaces.email.graph_client import GraphClient


def _hosts_allowed() -> list[str]:
    raw = os.getenv(
        "PETER_LINK_ALLOWLIST",
        "fieldwire.com,*.fieldwire.com,sharepoint.com,*.sharepoint.com,1drv.ms,onedrive.live.com,drive.google.com",
    )
    return [h.strip().lower() for h in raw.split(",") if h.strip()]


def _host_matches(host: str, pattern: str) -> bool:
    host = (host or "").lower().strip(".")
    pattern = (pattern or "").lower().strip(".")
    if not host or not pattern:
        return False
    if pattern.startswith("*."):
        return host == pattern[2:] or host.endswith("." + pattern[2:])
    return host == pattern or host.endswith("." + pattern)


def _is_allowed_url(url: str) -> bool:
    try:
        p = urllib.parse.urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").lower()
        if not host:
            return False
        for pat in _hosts_allowed():
            if _host_matches(host, pat):
                return True
        return False
    except Exception:
        return False


def _extract_urls_from_body(body: dict[str, Any] | None) -> list[str]:
    if not body:
        return []
    content = str(body.get("content") or "")
    if not content.strip():
        return []

    # Very simple URL extraction; good enough to start.
    urls = re.findall(r"https?://[^\s\)\]\>\"']+", content, flags=re.I)
    # Trim common trailing punctuation
    cleaned: list[str] = []
    for u in urls:
        u2 = u.rstrip(".,;:)")
        if u2:
            cleaned.append(u2)
    # Dedupe preserving order
    out: list[str] = []
    seen: set[str] = set()
    for u in cleaned:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _download_url_limited(url: str, *, max_bytes: int) -> bytes:
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "PETER-QA/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise RuntimeError(f"Download exceeded limit ({max_bytes} bytes)")
    return data


def _looks_like_pdf(data: bytes) -> bool:
    return (data or b"").startswith(b"%PDF-")


def _looks_like_zip(data: bytes) -> bool:
    return (data or b"").startswith(b"PK\x03\x04")


def _extract_pdfs_from_zip_bytes(zbytes: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:  # type: ignore[name-defined]
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if name.lower().endswith(".pdf"):
                out.append((name, zf.read(info)))
    return out


def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


from peter.interfaces.email.recipient_policy import (
    assert_internal_only,
    build_sanitized_reply_recipients,
)
from peter.services.site_service import SiteService
from peter.services.spec_service import SpecService
from peter.services.report_service import ReportService
from peter.services.query_service import QueryService
from peter.storage.filestore import ensure_site_folders


def _infer_site_and_ref_from_pdf_bytes(pdf_bytes: bytes) -> tuple[str | None, str | None]:
    """Best-effort infer (site_code, report_ref) from the PDF content.

    We rely on your standardized template labels (e.g. "SITE CODE:", "REPORT #:").
    """

    try:
        import re
        import tempfile
        from pathlib import Path

        from peter.parsing.pdf_text import extract_pdf_text, has_meaningful_text

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "attachment.pdf"
            p.write_bytes(pdf_bytes)
            text = extract_pdf_text(p)

        if not has_meaningful_text(text):
            return None, None

        # Normalize whitespace
        norm = re.sub(r"[\t\r]+", " ", text)

        m_site = re.search(r"(?im)^\s*SITE\s*CODE\s*:\s*([A-Z0-9_-]{3,20})\b", norm)
        site = m_site.group(1).strip().upper() if m_site else None

        m_ref = re.search(
            r"(?im)^\s*(?:INSPECTION\s*REFERENCE|REPORT\s*#|REPORT\s*NO\.?|REPORT\s*NUMBER)\s*:\s*([^\n]+)",
            norm,
        )
        ref = None
        if m_ref:
            ref_raw = m_ref.group(1).strip().upper()
            m_num = re.search(r"\b(\d{2,3})\b", ref_raw)
            ref = (m_num.group(1) if m_num else ref_raw.replace(" ", "")).strip().upper()

        return site, ref
    except Exception:
        return None, None


def _extract_addrs(msg: dict[str, Any], field: str) -> list[str]:
    out: list[str] = []
    for x in msg.get(field, []) or []:
        addr = (x.get("emailAddress") or {}).get("address")
        if addr:
            out.append(str(addr).lower())
    return out


def _normalize_subject_key(s: str) -> str:
    # Uppercase, keep alnum only.
    return re.sub(r"[^A-Z0-9]+", "", (s or "").upper())


def _infer_site_code_from_subject(*, subject: str, site_repo: SiteRepository) -> str | None:
    """Infer a known site_code from the subject by scanning for any existing code.

    This avoids guessing formats. It works as long as the subject contains the code
    as a contiguous token (case-insensitive), even if punctuation is used.
    """
    subj_key = _normalize_subject_key(subject)
    if not subj_key:
        return None

    try:
        sites = site_repo.list_all()
    except Exception:
        sites = []

    # Prefer longest codes first to avoid partial matches.
    codes = sorted({s.site_code for s in sites if getattr(s, "site_code", None)}, key=lambda x: -len(x))
    for code in codes:
        ck = _normalize_subject_key(code)
        if ck and ck in subj_key:
            return str(code).strip().upper()
    return None


def _is_openai_outage(exc: BaseException) -> bool:
    """Return True if this looks like a transient OpenAI outage/quota/rate-limit.

    We use urllib-based calls (see AskLLMError) so errors are mostly strings.
    Keep this conservative: only treat known transient conditions as outage.
    """

    msg = (str(exc) or "").lower()

    # Try to extract HTTP code from common message formats.
    # Example: "Ask request failed: HTTP 429 Too Many Requests ..."
    m = re.search(r"\bhttp\s+(\d{3})\b", msg)
    code = int(m.group(1)) if m else None

    if code in (408, 409, 425, 429, 500, 502, 503, 504):
        return True

    needles = [
        "insufficient_quota",
        "quota exceeded",
        "insufficient quota",
        "rate limit",
        "too many requests",
        "temporarily unavailable",
        "service unavailable",
        "gateway timeout",
        "timed out",
        "timeout",
        "connection reset",
        "connection error",
        "name or service not known",
        "temporary failure in name resolution",
    ]
    return any(n in msg for n in needles)


def _friendly_error_via_llm(*, settings: Settings, subject: str, body: str, error_id: str, exc: BaseException) -> str | None:
    enabled = os.getenv("PETER_EMAIL_FRIENDLY_ERRORS", "1").strip().lower() in ("1", "true", "yes")
    if not enabled:
        return None
    api_key = (settings.OPENAI_API_KEY or "").strip()
    if not api_key:
        return None

    system = (
        "You are PETER, an internal QA assistant. Rewrite internal system errors into a polite, clear message for technicians. "
        "Do NOT include stack traces, code, or secrets. "
        "Explain what went wrong in plain language and give exact next steps. "
        "If a site code is missing/unknown, instruct them how to format the subject. "
        "End with 'Reference: <error_id>' so support can trace it."
    )

    user = (
        f"EMAIL SUBJECT: {subject}\n\n"
        f"EMAIL BODY (excerpt):\n{(body or '')[:1200]}\n\n"
        f"ERROR_ID: {error_id}\n"
        f"EXCEPTION_TYPE: {type(exc).__name__}\n"
        f"EXCEPTION_MESSAGE: {str(exc)[:500]}\n\n"
        "Write the technician-facing reply."
    )

    try:
        return ask_openai_responses(
            api_key=api_key,
            model=os.getenv("PETER_EMAIL_ERROR_MODEL", "gpt-4.1"),
            system=system,
            user=user,
        ).strip() + "\n"
    except Exception:
        return None


def _has_external(addrs: list[str], *, internal_domain: str) -> bool:
    dom = internal_domain.lower()
    for a in addrs or []:
        if not a.lower().endswith("@" + dom):
            return True
    return False


@dataclass
class EmailWatcher:
    settings: Settings

    def run_forever(self) -> None:
        while True:
            self.run_once()
            time.sleep(self.settings.POLL_SECONDS)

    def run_once(self) -> dict[str, Any] | None:
        token = client_credentials_token(
            tenant_id=self.settings.GRAPH_TENANT_ID,
            client_id=self.settings.GRAPH_CLIENT_ID,
            client_secret=self.settings.GRAPH_CLIENT_SECRET,
        )
        graph = GraphClient(token=token)

        with get_connection(self.settings.DB_PATH) as conn:
            init_db(conn)
            site_repo = SiteRepository(conn)
            email_repo = EmailEventRepository(conn)
            email_att_repo = EmailAttachmentRepository(conn)
            site_svc = SiteService(conn, self.settings)
            spec_svc = SpecService(conn, self.settings)
            report_svc = ReportService(conn, self.settings)
            query_svc = QueryService(conn, self.settings)

            msgs = graph.list_unread_messages(mailbox=self.settings.BOT_MAILBOX, top=10)
            stats: dict[str, Any] = {"unread": len(msgs), "processed": 0, "commands": {}}
            # Process any queued items from prior OpenAI outages (best-effort)
            try:
                if os.getenv("PETER_API_QUEUE_ENABLED", "1").strip().lower() in ("1", "true", "yes"):
                    for it in list_items(data_dir=Path(self.settings.DATA_DIR), status="PENDING_API", limit=3):
                        try:
                            sc = str(it.meta.get("site_code") or "").strip().upper()
                            rc = str(it.meta.get("report_code") or "").strip().upper()
                            msg_id = str(it.meta.get("graph_message_id") or "").strip()
                            if not (sc and rc and msg_id):
                                update_status(item=it, status="FAILED", extra={"error": "missing meta"})
                                continue

                            # Backoff
                            retry_count = int(it.meta.get("retry_count") or 0)
                            last_retry_at = str(it.meta.get("last_retry_at") or "").strip()
                            base = int(os.getenv("PETER_API_QUEUE_BACKOFF_SECONDS", "60"))
                            cap = int(os.getenv("PETER_API_QUEUE_BACKOFF_MAX_SECONDS", "1800"))
                            wait_s = min(cap, base * (2 ** min(retry_count, 6)))
                            if last_retry_at:
                                try:
                                    # last_retry_at stored as '%Y-%m-%d %H:%M:%S'
                                    import datetime as _dt
                                    t0 = _dt.datetime.strptime(last_retry_at, "%Y-%m-%d %H:%M:%S")
                                    if (_dt.datetime.utcnow() - t0).total_seconds() < wait_s:
                                        continue
                                except Exception:
                                    pass

                            # Try to draft the final reply now.
                            vision_note = ""
                            if os.getenv("PETER_VISION_ENABLED", "").strip().lower() in ("1", "true", "yes"):
                                try:
                                    out_v = report_svc.analyze_report_visuals(site_code=sc, report_code=rc, reset=True)
                                    n = len(out_v.get("omission_issues_created") or [])
                                    vision_json = out_v.get("vision_json")
                                    vision_note = (
                                        "\n\nVISUAL ANALYSIS (auto)\n"
                                        f"- omissions flagged: {n}\n"
                                        f"- artifact: {vision_json}\n"
                                    )
                                except Exception:
                                    pass

                            from peter.interfaces.email.llm_reply import draft_email_reply_llm

                            final_text = draft_email_reply_llm(
                                conn=conn,
                                settings=self.settings,
                                site_code=sc,
                                report_code=rc,
                                vision_text=vision_note,
                            )

                            # Send follow-up reply in-thread
                            draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=msg_id)
                            draft_id = draft["id"]
                            payload = {
                                "body": {"contentType": "Text", "content": final_text},
                            }
                            graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload=payload)
                            graph.send_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id)

                            update_status(
                                item=it,
                                status="PROCESSED",
                                extra={
                                    "processed_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                                    "retry_count": int(it.meta.get("retry_count") or 0),
                                },
                            )
                        except Exception as e:
                            # Update retry metadata
                            try:
                                update_status(
                                    item=it,
                                    status="PENDING_API" if _is_openai_outage(e) else "FAILED",
                                    extra={
                                        "retry_count": int(it.meta.get("retry_count") or 0) + 1,
                                        "last_retry_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                                        "last_error": str(e)[:300],
                                    },
                                )
                            except Exception:
                                pass
                            if _is_openai_outage(e):
                                continue
                            update_status(item=it, status="FAILED", extra={"error": str(e)[:300]})
            except Exception:
                pass

            for m in msgs:
                mid = m["id"]
                # Dedupe: if we've seen this graph_message_id, skip.
                if email_repo.exists_graph_message_id(mid):
                    try:
                        graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                    except Exception:
                        pass
                    continue
                subject = (m.get("subject") or "").strip()
                stats["processed"] += 1

                # Manual TDS override command (subject-based): TDS | VENDOR | CODE | URL
                try:
                    from peter.interfaces.email.tds_cmd import parse_tds_subject

                    vendor, code, url = parse_tds_subject(subject)
                    if vendor and code and url:
                        from peter.knowledge.tds_library import fetch_and_store_tds

                        rec = fetch_and_store_tds(qa_root=self.settings.QA_ROOT, vendor=vendor, product_key=code, url=url)
                        graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                        return {"unread": len(msgs), "processed": stats["processed"], "commands": {"TDS": 1}}
                except Exception:
                    pass

                # Handle quarantine confirmation commands first.
                conf = parse_confirm_subject(subject)
                if conf.kind == "NONE":
                    # Free-text confirmations: look for QID + confirm/reject/type in the email body.
                    try:
                        full0 = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=mid, select="body")
                        body0 = str(((full0.get("body") or {}).get("content") or "")).strip()
                    except Exception:
                        body0 = ""
                    conf2 = parse_confirm_freeform(subject, body0)
                    if conf2.kind in ("CONFIRM", "REJECT") and conf2.qid:
                        conf = conf2

                if conf.kind in ("CONFIRM", "REJECT") and conf.qid:
                    from_addr_cmd = ((m.get("from") or {}).get("emailAddress") or {}).get("address", "").lower()
                    try:
                        item = load_quarantine_item(data_dir=Path(self.settings.DATA_DIR), qid=conf.qid)

                        # Authorization: internal + original sender or forced-cc member
                        allowed = False
                        if from_addr_cmd and from_addr_cmd.endswith("@" + self.settings.INTERNAL_DOMAIN):
                            orig = str(item.meta.get("original_from") or "").lower()
                            forced = [a.strip().lower() for a in (self.settings.REVIEW_DLIST or [])]
                            if from_addr_cmd == orig or from_addr_cmd in forced:
                                allowed = True

                        if not allowed:
                            # Mark read, ignore
                            graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                            continue

                        if conf.kind == "REJECT":
                            update_status(item=item, status="REJECTED", extra={"rejected_by": from_addr_cmd})
                            graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                            continue

                        # CONFIRM
                        site = (conf.site or str(item.meta.get("detected_site") or "")).strip().upper()
                        rep = (conf.report or str(item.meta.get("detected_report") or "")).strip().upper()
                        rep = rep.replace("R", "") if rep.startswith("R") and rep[1:].isdigit() else rep
                        rep = rep.zfill(3) if rep.isdigit() else rep

                        # Project type confirmation (once per site)
                        require_ptype = bool(item.meta.get("require_project_type"))
                        ptype = (conf.project_type or str(item.meta.get("project_type") or "").strip().upper().replace("-", "_"))
                        if ptype in ("NEW", "NEWWORK"):
                            ptype = "NEW_WORK"
                        if require_ptype and ptype not in ("NEW_WORK", "REDEC"):
                            # Ask again; keep item pending.
                            update_status(item=item, status="PENDING_CONFIRMATION", extra={"needs_project_type": True})
                            reply_text = (
                                "CONFIRMATION NEEDED\n"
                                f"Quarantine ID: {item.qid}\n\n"
                                "Please confirm whether this project is NEW WORK or REDEC.\n\n"
                                "Reply with one of:\n"
                                f"- CONFIRM {item.qid} | SITE={site} | REPORT={rep} | TYPE=NEW_WORK\n"
                                f"- CONFIRM {item.qid} | SITE={site} | REPORT={rep} | TYPE=REDEC\n"
                            )

                            # Send reply and mark read
                            try:
                                draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                                draft_id = draft["id"]
                                graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload={"body": {"contentType": "Text", "content": reply_text}})
                                graph.send_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id)
                            except Exception:
                                pass
                            graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                            continue

                        # Ensure site exists (first-time site onboarding confirmation flow)
                        try:
                            site_svc.get_site_or_raise(site)
                        except Exception:
                            # Create site using extracted PDF identity if available
                            pdf_name = str(item.meta.get("pdf_site_name_display") or item.meta.get("pdf_site_name_raw") or site).strip()
                            created = site_svc.create_site(site_code=site, site_name=pdf_name)
                            # Persist additional metadata if provided
                            try:
                                conn.execute(
                                    """
                                    UPDATE sites
                                    SET site_name_raw = COALESCE(site_name_raw, ?),
                                        address = COALESCE(address, ?),
                                        supplier_client = COALESCE(supplier_client, ?),
                                        contractor_on_site = COALESCE(contractor_on_site, ?),
                                        project_type = COALESCE(project_type, ?)
                                    WHERE id = ?
                                    """,
                                    (
                                        (item.meta.get("pdf_site_name_raw") if isinstance(item.meta, dict) else None),
                                        (item.meta.get("pdf_address") if isinstance(item.meta, dict) else None),
                                        (item.meta.get("pdf_supplier_client") if isinstance(item.meta, dict) else None),
                                        (item.meta.get("pdf_contractor_on_site") if isinstance(item.meta, dict) else None),
                                        (ptype if require_ptype else None),
                                        int(created.id),
                                    ),
                                )
                            except Exception:
                                pass

                        # Ingest as QA_REPORT
                        out = report_svc.ingest_report(site_code=site, report_code=rep, file_path=item.file_path)

                        # Always triage + vision after confirm so a reply can be sent.
                        try:
                            report_svc.triage_report_text(site_code=site, report_code=rep, reset=True)
                        except Exception:
                            pass

                        vision_note = ""
                        if os.getenv("PETER_VISION_ENABLED", "").strip().lower() in ("1", "true", "yes"):
                            try:
                                out_v = report_svc.analyze_report_visuals(site_code=site, report_code=rep, reset=True)
                                n = len(out_v.get("omission_issues_created") or [])
                                vision_json = out_v.get("vision_json")
                                vision_note = (
                                    "\n\nVISION CHECK (auto)\n"
                                    f"- visual omissions flagged: {n}\n"
                                    f"- artifact: {vision_json}\n"
                                )
                            except Exception as e:
                                vision_note = f"\n\nVISION CHECK (auto)\n- ERROR: {str(e)[:200]}\n"

                        # Draft and send a confirmation reply (internal-only)
                        reply_text = ""
                        try:
                            use_llm = os.getenv("PETER_EMAIL_DRAFT_USE_OPENAI", "").strip().lower() in ("1", "true", "yes")
                            if use_llm and self.settings.OPENAI_API_KEY:
                                from peter.interfaces.email.llm_reply import draft_email_reply_llm

                                reply_text = draft_email_reply_llm(
                                    conn=conn,
                                    settings=self.settings,
                                    site_code=site,
                                    report_code=rep,
                                    vision_text=vision_note,
                                )
                            else:
                                from peter.interfaces.qa.ask import answer_report_question

                                reply_text = (
                                    answer_report_question(
                                        conn=conn,
                                        settings=self.settings,
                                        site_code=site,
                                        report_code=rep,
                                        question=(
                                            "Summarize the QA status for this report and list required next actions to address blocking issues. "
                                            "Be concise and use bullets where helpful."
                                        ),
                                        mode="recommend",
                                    ).rstrip()
                                    + vision_note
                                    + "\n"
                                )
                        except Exception:
                            reply_text = f"CONFIRM OK {item.qid}: site={site} report={rep} status={out.get('status')} report_id={out.get('report_id')}" + vision_note

                        update_status(
                            item=item,
                            status="CONFIRMED",
                            extra={
                                "confirmed_by": from_addr_cmd,
                                "site": site,
                                "report": rep,
                                "ingest": out,
                                "site_identity": {
                                    "site_name_raw": item.meta.get("pdf_site_name_raw") if isinstance(item.meta, dict) else None,
                                    "site_name_display": item.meta.get("pdf_site_name_display") if isinstance(item.meta, dict) else None,
                                    "address": item.meta.get("pdf_address") if isinstance(item.meta, dict) else None,
                                    "supplier_client": item.meta.get("pdf_supplier_client") if isinstance(item.meta, dict) else None,
                                    "contractor_on_site": item.meta.get("pdf_contractor_on_site") if isinstance(item.meta, dict) else None,
                                },
                            },
                        )

                        # Send a new internal-only message (not in-thread) to the confirmer.
                        try:
                            draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                            draft_id = draft["id"]

                            # Subject tag
                            subj = f"CONFIRM OK {item.qid}"
                            graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload={"subject": subj})

                            to_list, cc_list = build_sanitized_reply_recipients(
                                internal_domain=self.settings.INTERNAL_DOMAIN,
                                original_from=from_addr_cmd,
                                original_to=[],
                                original_cc=[],
                                bot_mailbox=self.settings.BOT_MAILBOX,
                                forced_cc=list(self.settings.REVIEW_DLIST),
                            )
                            assert_internal_only(to_list, cc_list, internal_domain=self.settings.INTERNAL_DOMAIN)

                            payload = {
                                "toRecipients": [{"emailAddress": {"address": a}} for a in to_list],
                                "ccRecipients": [{"emailAddress": {"address": a}} for a in cc_list],
                                "body": {"contentType": "Text", "content": reply_text},
                            }
                            graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload=payload)
                            graph.send_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id)
                        except Exception:
                            pass

                        graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                        continue
                    except Exception:
                        # If command processing fails, do not loop forever.
                        try:
                            graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                        except Exception:
                            pass
                        continue

                cmd = parse_subject(subject)

                # Conversational email mode: site code in subject, natural language in body.
                # If the deterministic parser didn't produce a site_code, infer it from the subject.
                inferred_site = None
                try:
                    if not cmd.site_code:
                        inferred_site = _infer_site_code_from_subject(subject=subject, site_repo=site_repo)
                except Exception:
                    inferred_site = None

                if cmd.kind == "UNKNOWN" and inferred_site:
                    # Treat as ASSIST for that site; body will be used as the request later.
                    from peter.interfaces.email.classifier import ParsedCommand

                    cmd = ParsedCommand("ASSIST", inferred_site, None)

                stats["commands"][cmd.kind] = int(stats["commands"].get(cmd.kind, 0)) + 1

                # If subject is not recognized, attempt attachment-driven inference
                # for the common workflow: technicians CC the bot on existing threads.
                if cmd.kind == "UNKNOWN":
                    try:
                        atts = graph.list_attachments(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                        pdf_candidates: list[bytes] = []
                        for meta in atts or []:
                            att_id = meta.get("id")
                            if not att_id:
                                continue
                            att = graph.get_attachment(mailbox=self.settings.BOT_MAILBOX, message_id=mid, attachment_id=att_id)
                            if not (att.get("@odata.type", "").endswith("fileAttachment") and att.get("contentBytes")):
                                continue
                            name = str(att.get("name") or meta.get("name") or "").lower()
                            ctype = str(att.get("contentType") or meta.get("contentType") or "").lower()
                            if ctype == "application/pdf" or name.endswith(".pdf"):
                                pdf_candidates.append(base64.b64decode(att["contentBytes"]))

                        if len(pdf_candidates) == 1:
                            rid = infer_from_pdf_bytes(pdf_candidates[0])
                            if rid:
                                from peter.interfaces.email.classifier import ParsedCommand

                                cmd = ParsedCommand("QA_REPORT", rid.site_code, rid.report_no)
                    except Exception:
                        # Keep UNKNOWN if anything fails.
                        pass

                from_addr = ((m.get("from") or {}).get("emailAddress") or {}).get("address", "").lower()
                to_addrs = _extract_addrs(m, "toRecipients")
                cc_addrs = _extract_addrs(m, "ccRecipients")

                all_rcpts = [from_addr] + to_addrs + cc_addrs
                has_ext = _has_external(all_rcpts, internal_domain=self.settings.INTERNAL_DOMAIN)

                # Resolve site id if possible
                site_id = None
                if cmd.site_code:
                    site = site_repo.get_by_code(cmd.site_code)
                    site_id = site.id if site else None

                # Archive original message MIME (best-effort)
                archived_eml_path = None
                try:
                    mime_bytes = graph.get_message_mime(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                    # store under site email archive if we can resolve site, else global data dir
                    if cmd.site_code and site_id:
                        site = site_repo.get_by_code(cmd.site_code)
                        if not site:
                            auto = os.getenv("PETER_AUTO_CREATE_SITES", "").strip().lower() in ("1", "true", "yes")
                            if auto:
                                site_svc.create_site(site_code=cmd.site_code, site_name=cmd.site_code)
                                site = site_repo.get_by_code(cmd.site_code)
                        if not site:
                            raise RuntimeError(f"Unknown site_code: {cmd.site_code}")
                        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
                        eml_name = f"{site.site_code}__EMAIL__{cmd.kind}__{mid}.eml"
                        eml_path = sandbox.build_path("04_email_archive", eml_name)
                        eml_path.write_bytes(mime_bytes)
                        archived_eml_path = str(eml_path.relative_to(self.settings.QA_ROOT))
                    else:
                        g = Path(self.settings.DATA_DIR) / "email_quarantine"
                        g.mkdir(parents=True, exist_ok=True)
                        eml_path = g / f"UNKNOWN__EMAIL__{cmd.kind}__{mid}.eml"
                        eml_path.write_bytes(mime_bytes)
                        archived_eml_path = str(eml_path)
                except Exception:
                    archived_eml_path = None

                event_id = email_repo.insert_event(
                    site_id=site_id,
                    graph_message_id=mid,
                    internet_message_id=m.get("internetMessageId"),
                    conversation_id=m.get("conversationId"),
                    subject=subject,
                    from_address=from_addr,
                    to_addresses=to_addrs,
                    cc_addresses=cc_addrs,
                    has_external_recipients=has_ext,
                    command_type=cmd.kind,
                    archived_eml_path=archived_eml_path,
                )

                # Build internal-only reply content
                reply_text = ""
                should_send = True
                try:
                    if cmd.kind == "NEW_SITE" and cmd.site_code and cmd.arg:
                        site_svc.create_site(site_code=cmd.site_code, site_name=cmd.arg)
                        reply_text = f"OK created site {cmd.site_code}"

                    elif cmd.kind in {"SPEC_UPDATE", "QA_REPORT"}:
                        if not cmd.site_code:
                            raise RuntimeError("Missing site_code")

                        # Resolve site sandbox for quarantine/archival.
                        # Technician-friendly onboarding: for first-time sites, require confirmation
                        # before committing a new site to memory.
                        site = site_repo.get_by_code(cmd.site_code)
                        require_first = os.getenv("PETER_EMAIL_REQUIRE_SITE_CONFIRM_FIRST_TIME", "1").strip().lower() in ("1", "true", "yes")
                        if not site and cmd.kind == "QA_REPORT" and require_first:
                            site = None
                        elif not site:
                            auto = os.getenv("PETER_AUTO_CREATE_SITES", "").strip().lower() in ("1", "true", "yes")
                            if auto:
                                # Placeholder name; you can formalize later.
                                site_svc.create_site(site_code=cmd.site_code, site_name=cmd.site_code)
                                site = site_repo.get_by_code(cmd.site_code)
                            if not site:
                                raise RuntimeError(f"Unknown site_code: {cmd.site_code}")

                        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name) if site else None

                        attachments = graph.list_attachments(mailbox=self.settings.BOT_MAILBOX, message_id=mid)

                        # Download all fileAttachments with contentBytes (best-effort)
                        downloaded: list[dict[str, Any]] = []
                        for meta in attachments:
                            att_id = meta.get("id")
                            if not att_id:
                                continue
                            att = graph.get_attachment(
                                mailbox=self.settings.BOT_MAILBOX,
                                message_id=mid,
                                attachment_id=att_id,
                            )
                            if not (att.get("@odata.type", "").endswith("fileAttachment") and att.get("contentBytes")):
                                # Quarantine metadata-only attachments
                                try:
                                    email_att_repo.insert(
                                        email_event_id=event_id,
                                        filename=str(att.get("name") or meta.get("name") or "attachment"),
                                        content_type=str(att.get("contentType") or meta.get("contentType") or ""),
                                        sha256="".ljust(64, "0"),
                                        stored_path=None,
                                        quarantined=True,
                                    )
                                except Exception:
                                    pass
                                continue

                            data = base64.b64decode(att["contentBytes"])
                            name = str(att.get("name") or meta.get("name") or "attachment")
                            ctype = str(att.get("contentType") or meta.get("contentType") or "")
                            sha = sha256_bytes(data)

                            downloaded.append({"name": name, "content_type": ctype, "sha": sha, "data": data})

                        # Classify PDFs / ZIPs
                        def _is_pdf(item: dict[str, Any]) -> bool:
                            ctype = (item.get("content_type") or "").lower()
                            name = (item.get("name") or "").lower()
                            return ctype == "application/pdf" or name.endswith(".pdf")

                        def _is_zip(item: dict[str, Any]) -> bool:
                            ctype = (item.get("content_type") or "").lower()
                            name = (item.get("name") or "").lower()
                            return ctype in ("application/zip", "application/x-zip-compressed") or name.endswith(".zip")

                        pdfs = [d for d in downloaded if _is_pdf(d)]
                        zips = [d for d in downloaded if _is_zip(d)]
                        others = [d for d in downloaded if (d not in pdfs and d not in zips)]

                        # If no PDFs but a single ZIP is present, extract PDFs from ZIP.
                        if not pdfs and len(zips) == 1:
                            try:
                                z = zips[0]
                                extracted = []
                                with zipfile.ZipFile(io.BytesIO(z["data"])) as zf:
                                    for info in zf.infolist():
                                        if info.is_dir():
                                            continue
                                        if info.filename.lower().endswith(".pdf"):
                                            extracted.append(
                                                {
                                                    "name": info.filename,
                                                    "content_type": "application/pdf",
                                                    "sha": sha256_bytes(zf.read(info)),
                                                    "data": zf.read(info),
                                                }
                                            )
                                pdfs = extracted
                            except Exception:
                                pdfs = []

                        # If this is the first time we've seen the site, require confirmation
                        # before committing the site + ingesting.
                        if site is None and cmd.kind == "QA_REPORT" and require_first:
                            if len(pdfs) != 1:
                                reply_text = (
                                    f"NEW SITE (confirmation required)\n\n"
                                    f"I received a QA report for site code '{cmd.site_code}', but this site is not in my database yet.\n"
                                    f"Please resend with exactly 1 PDF (or 1 ZIP containing 1 PDF).\n"
                                    f"PDFs detected: {len(pdfs)}\n"
                                )
                            else:
                                p = pdfs[0]
                                # Extract identity from PDF
                                rid = None
                                try:
                                    rid = infer_from_pdf_bytes(p["data"])
                                except Exception:
                                    rid = None

                                claimed_site = (cmd.site_code or "").strip().upper()
                                claimed_rep = (cmd.arg or "").strip().upper().replace(" ", "")
                                if claimed_rep.startswith("R") and claimed_rep[1:].isdigit():
                                    claimed_rep = claimed_rep[1:]
                                if claimed_rep.isdigit():
                                    claimed_rep = claimed_rep.zfill(3)

                                detected_site = (rid.site_code if rid else claimed_site)
                                detected_rep = (rid.report_no if rid else claimed_rep)

                                item = save_quarantine_item(
                                    data_dir=Path(self.settings.DATA_DIR),
                                    filename=p["name"],
                                    content=p["data"],
                                    meta={
                                        "original_from": from_addr,
                                        "graph_message_id": mid,
                                        "subject": subject,
                                        "claimed_site": claimed_site,
                                        "claimed_report": claimed_rep,
                                        "detected_site": detected_site,
                                        "detected_report": detected_rep,
                                        "pdf_site_name_raw": (rid.site_name_raw if rid else None),
                                        "pdf_site_name_display": (rid.site_name_display if rid else None),
                                        "pdf_address": (rid.address if rid else None),
                                        "pdf_supplier_client": (rid.supplier_client if rid else None),
                                        "pdf_contractor_on_site": (rid.contractor_on_site if rid else None),
                                        "require_project_type": True,
                                        "note": "First-time site: confirm site identity before commit",
                                    },
                                )

                                # Audit attachment record
                                try:
                                    email_att_repo.insert(
                                        email_event_id=event_id,
                                        filename=p["name"],
                                        content_type=p["content_type"],
                                        sha256=p["sha"],
                                        stored_path=str(item.file_path),
                                        quarantined=True,
                                    )
                                except Exception:
                                    pass

                                # Compose confirmation request
                                lines = [
                                    "NEW SITE (confirmation required)",
                                    f"Quarantine ID: {item.qid}",
                                    "",
                                    f"Proposed site reference: {detected_site}",
                                ]
                                if rid and rid.site_name_display:
                                    lines.append(f"Site name: {rid.site_name_display}")
                                if rid and rid.address:
                                    lines.append(f"Address: {rid.address}")
                                if rid and rid.supplier_client:
                                    lines.append(f"Supplier / Client: {rid.supplier_client}")
                                if rid and rid.contractor_on_site:
                                    lines.append(f"Contractor on site: {rid.contractor_on_site}")
                                if detected_rep:
                                    lines.append(f"Report number: {detected_rep}")

                                lines += [
                                    "",
                                    "Project type (required):",
                                    "- NEW_WORK  (new build / new work)",
                                    "- REDEC     (redecoration / repaint)",
                                    "",
                                    "Reply with:",
                                    f"- CONFIRM {item.qid} | SITE={detected_site} | REPORT={detected_rep} | TYPE=NEW_WORK",
                                    f"- CONFIRM {item.qid} | SITE={detected_site} | REPORT={detected_rep} | TYPE=REDEC",
                                    f"- REJECT {item.qid}",
                                    "",
                                    "(You can also reply in plain text like: 'Confirm Q-... redecorations')",
                                ]
                                reply_text = "\n".join(lines) + "\n"

                        else:
                            # Quarantine non-PDF attachments always
                            for o in others:
                                try:
                                    qname = f"{site.site_code}__EMAIL__{cmd.kind}__{mid}__{o['sha'][:12]}__{o['name']}".replace("/", "_")
                                    qpath = sandbox.build_path("99_quarantine", qname)
                                    qpath.write_bytes(o["data"])
                                    sandbox.build_path("99_quarantine", qname + ".reason.txt").write_text(
                                        "Non-PDF email attachment quarantined.\n", encoding="utf-8"
                                    )

                                    email_att_repo.insert(
                                        email_event_id=event_id,
                                        filename=o["name"],
                                        content_type=o["content_type"],
                                        sha256=o["sha"],
                                        stored_path=str(qpath.relative_to(self.settings.QA_ROOT)),
                                        quarantined=True,
                                    )
                                except Exception:
                                    pass

                        # If still no PDFs, attempt link-based fetch from the email body.
                        if not pdfs:
                            try:
                                max_mb = int(os.getenv("PETER_LINK_MAX_MB", "80"))
                                max_bytes = max_mb * 1024 * 1024
                                full = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=mid, select="body")
                                urls = _extract_urls_from_body(full.get("body"))
                                for url in urls:
                                    if not _is_allowed_url(url):
                                        continue
                                    data = _download_url_limited(url, max_bytes=max_bytes)
                                    if _looks_like_pdf(data):
                                        pdfs = [
                                            {
                                                "name": Path(urllib.parse.urlparse(url).path).name or "linked.pdf",
                                                "content_type": "application/pdf",
                                                "sha": sha256_bytes(data),
                                                "data": data,
                                            }
                                        ]
                                        break
                                    if _looks_like_zip(data):
                                        # Extract PDFs from ZIP
                                        with zipfile.ZipFile(io.BytesIO(data)) as zf:
                                            extracted = []
                                            for info in zf.infolist():
                                                if info.is_dir():
                                                    continue
                                                if info.filename.lower().endswith(".pdf"):
                                                    b = zf.read(info)
                                                    extracted.append(
                                                        {
                                                            "name": info.filename,
                                                            "content_type": "application/pdf",
                                                            "sha": sha256_bytes(b),
                                                            "data": b,
                                                        }
                                                    )
                                        if len(extracted) == 1:
                                            pdfs = extracted
                                            break
                            except Exception:
                                pass

                        # Enforce exactly one PDF for SPEC_UPDATE / QA_REPORT
                        if len(pdfs) != 1:
                            # Quarantine PDF(s) too, for manual review
                            for p in pdfs:
                                try:
                                    qname = f"{site.site_code}__EMAIL__{cmd.kind}__{mid}__{p['sha'][:12]}__{p['name']}".replace("/", "_")
                                    qpath = sandbox.build_path("99_quarantine", qname)
                                    qpath.write_bytes(p["data"])
                                    sandbox.build_path("99_quarantine", qname + ".reason.txt").write_text(
                                        f"PDF attachment quarantined: expected exactly 1 PDF, got {len(pdfs)}.\n",
                                        encoding="utf-8",
                                    )
                                    email_att_repo.insert(
                                        email_event_id=event_id,
                                        filename=p["name"],
                                        content_type=p["content_type"],
                                        sha256=p["sha"],
                                        stored_path=str(qpath.relative_to(self.settings.QA_ROOT)),
                                        quarantined=True,
                                    )
                                except Exception:
                                    pass

                            reply_text = f"ERROR: Expected exactly 1 PDF attachment for {cmd.kind}, got {len(pdfs)}. Attachments quarantined."

                        else:
                            # Exactly one PDF -> validate identity to prevent cross-contamination
                            p = pdfs[0]

                            detected = None
                            try:
                                detected = infer_from_pdf_bytes(p["data"])
                            except Exception:
                                detected = None

                            claimed_site_raw = (cmd.site_code or "").strip().upper()

                            # Technician-friendly: normalize site codes that include spaces/punctuation.
                            # Default behavior: require confirmation if normalization changes the code.
                            def _normalize_site_code(x: str) -> str:
                                return re.sub(r"[^A-Z0-9]+", "", (x or "").upper())

                            claimed_site = claimed_site_raw
                            norm_site = _normalize_site_code(claimed_site_raw)

                            claimed_rep = (cmd.arg or "").strip().upper().replace(" ", "")
                            # normalize claimed report to 3-digit numeric if possible
                            if claimed_rep.startswith("R") and claimed_rep[1:].isdigit():
                                claimed_rep = claimed_rep[1:]
                            if claimed_rep.isdigit():
                                claimed_rep = claimed_rep.zfill(3)

                            normalize_mode = os.getenv("PETER_EMAIL_SITE_CODE_NORMALIZE", "confirm").strip().lower()
                            if norm_site and norm_site != claimed_site_raw and normalize_mode in ("1", "true", "yes", "confirm"):
                                # Quarantine and ask for confirmation of normalized site code.
                                item = save_quarantine_item(
                                    data_dir=Path(self.settings.DATA_DIR),
                                    filename=p["name"],
                                    content=p["data"],
                                    meta={
                                        "original_from": from_addr,
                                        "graph_message_id": mid,
                                        "subject": subject,
                                        "claimed_site": claimed_site_raw,
                                        "claimed_report": claimed_rep,
                                        "detected_site": norm_site,
                                        "detected_report": claimed_rep,
                                        "note": "Site code normalized from subject",
                                    },
                                )
                                reply_text = (
                                    f"QUARANTINED (site code needs confirmation)\n"
                                    f"Quarantine ID: {item.qid}\n\n"
                                    f"Provided site code: {claimed_site_raw}\n"
                                    f"Normalized site code: {norm_site}\n\n"
                                    f"Reply with one of:\n"
                                    f"- CONFIRM {item.qid} | SITE={norm_site} | REPORT={claimed_rep}\n"
                                    f"- REJECT {item.qid}\n\n"
                                    f"File saved: {str(item.file_path)}\n"
                                )
                                # Skip further processing for this message; reply will be sent below.
                                # Mark as quarantined attachment for audit
                                detected = None
                                out_path = None
                                # Jump to reply send by setting should_send and leaving other branches.
                                # We do this by using a sentinel prefix and later skip ingest when reply_text starts with QUARANTINED.
                                
                            claimed_site = norm_site or claimed_site_raw

                            if reply_text.startswith("QUARANTINED (site code needs confirmation)"):
                                # site-code normalization quarantine already prepared above
                                pass
                            elif detected and claimed_site and claimed_rep and (detected.site_code != claimed_site or detected.report_no != claimed_rep):
                                # Quarantine and ask for confirmation
                                item = save_quarantine_item(
                                    data_dir=Path(self.settings.DATA_DIR),
                                    filename=p["name"],
                                    content=p["data"],
                                    meta={
                                        "original_from": from_addr,
                                        "graph_message_id": mid,
                                        "subject": subject,
                                        "claimed_site": claimed_site,
                                        "claimed_report": claimed_rep,
                                        "detected_site": detected.site_code,
                                        "detected_report": detected.report_no,
                                    },
                                )
                                reply_text = (
                                    f"QUARANTINED (needs confirmation)\n"
                                    f"Quarantine ID: {item.qid}\n\n"
                                    f"Claimed: site={claimed_site} report={claimed_rep}\n"
                                    f"Detected from PDF: site={detected.site_code} report={detected.report_no}\n\n"
                                    f"Reply with one of:\n"
                                    f"- CONFIRM {item.qid}\n"
                                    f"- CONFIRM {item.qid} | SITE={detected.site_code} | REPORT={detected.report_no}\n"
                                    f"- REJECT {item.qid}\n\n"
                                    f"File saved: {str(item.file_path)}\n"
                                )
                            elif detected is None:
                                # If we cannot confidently detect identity, quarantine and ask for confirmation
                                item = save_quarantine_item(
                                    data_dir=Path(self.settings.DATA_DIR),
                                    filename=p["name"],
                                    content=p["data"],
                                    meta={
                                        "original_from": from_addr,
                                        "graph_message_id": mid,
                                        "subject": subject,
                                        "claimed_site": claimed_site,
                                        "claimed_report": claimed_rep,
                                        "detected_site": None,
                                        "detected_report": None,
                                    },
                                )
                                reply_text = (
                                    f"QUARANTINED (cannot determine site/report from PDF)\n"
                                    f"Quarantine ID: {item.qid}\n\n"
                                    f"Claimed: site={claimed_site} report={claimed_rep}\n\n"
                                    f"Reply with:\n"
                                    f"- CONFIRM {item.qid} | SITE=<SITE> | REPORT=<NNN>\n"
                                    f"- REJECT {item.qid}\n\n"
                                    f"File saved: {str(item.file_path)}\n"
                                )

                            out_path: Path | None = None

                            # If we set a QUARANTINED reply, skip ingest.
                            if reply_text.startswith("QUARANTINED"):
                                # Record the quarantined attachment location for audit
                                try:
                                    email_att_repo.insert(
                                        email_event_id=event_id,
                                        filename=p["name"],
                                        content_type=p["content_type"],
                                        sha256=p["sha"],
                                        stored_path=str(item.file_path) if "item" in locals() else None,
                                        quarantined=True,
                                    )
                                except Exception:
                                    pass
                            else:
                                # Write to email_drop for ingestion (idempotent by sha)
                                drop = Path(self.settings.DATA_DIR) / "email_drop"
                                drop.mkdir(parents=True, exist_ok=True)
                                safe_name = f"{cmd.site_code}__{cmd.kind}__{p['sha'][:12]}__{p['name']}".replace("/", "_")
                                out_path = drop / safe_name
                                if not out_path.exists():
                                    out_path.write_bytes(p["data"])

                                email_att_repo.insert(
                                    email_event_id=event_id,
                                    filename=p["name"],
                                    content_type=p["content_type"],
                                    sha256=p["sha"],
                                    stored_path=str(out_path),
                                    quarantined=False,
                                )

                                if cmd.kind == "SPEC_UPDATE":
                                    spec = spec_svc.ingest_spec(
                                        site_code=cmd.site_code,
                                        version_label=cmd.arg or "REV01",
                                        file_path=out_path,
                                    )
                                    reply_text = f"OK spec ingested site={cmd.site_code} spec_id={spec.id} version={spec.version_label}"
                                else:
                                    rc = (cmd.arg or "R01").strip().upper().replace(" ", "")
                                    out = report_svc.ingest_report(site_code=cmd.site_code, report_code=rc, file_path=out_path)

                                # Always (re)triage so the reply reflects the latest deterministic issues/result.
                                try:
                                    report_svc.triage_report_text(site_code=cmd.site_code, report_code=rc, reset=True)
                                except Exception:
                                    pass

                                # Always run Vision (if enabled + API key present)
                                vision_note = ""
                                vision_enabled = os.getenv("PETER_VISION_ENABLED", "").strip().lower() in ("1", "true", "yes")
                                if vision_enabled:
                                    try:
                                        out_v = report_svc.analyze_report_visuals(site_code=cmd.site_code, report_code=rc, reset=True)
                                        n = len(out_v.get("omission_issues_created") or [])
                                        vision_json = out_v.get("vision_json")
                                        vision_note = (
                                            "\n\nVISUAL ANALYSIS (auto)\n"
                                            f"- omissions flagged: {n}\n"
                                            f"- artifact: {vision_json}\n"
                                        )

                                        # Add short, human-readable vision findings (critical + notable)
                                        try:
                                            from peter.interfaces.email.vision_summary import summarize_vision_json

                                            max_notable = int(os.getenv("PETER_EMAIL_VISION_MAX_NOTABLE", "5"))
                                            min_conf = float(os.getenv("PETER_EMAIL_VISION_NOTABLE_MIN_CONF", "0.85"))
                                            vs = summarize_vision_json(
                                                vision_json_path=str(vision_json),
                                                max_notable=max_notable,
                                                notable_min_conf=min_conf,
                                            )

                                            if vs.blocking:
                                                vision_note += "\nVISUAL ANALYSIS — Critical photo findings (Immediate action required)\n" + "\n".join(vs.blocking[:10]) + "\n"
                                            if vs.notable:
                                                vision_note += "\nVISUAL ANALYSIS — Notable observations (non-critical)\n" + "\n".join(vs.notable) + "\n"
                                        except Exception:
                                            pass
                                    except Exception as e:
                                        vision_note = f"\n\nVISION CHECK (auto)\n- ERROR: {str(e)[:200]}\n"

                                # Compose a human-like operational reply.
                                # Prefer LLM drafting if enabled; fall back to heuristic ask.

                                # Gather any spec deviation issues that likely need technician confirmation.
                                confirm_notes = ""
                                try:
                                    row2 = conn.execute(
                                        """
                                        SELECT r.id
                                        FROM reports r
                                        JOIN sites s ON s.id = r.site_id
                                        WHERE s.site_code = ? AND r.report_code = ?
                                        ORDER BY r.received_at DESC
                                        LIMIT 1
                                        """,
                                        (cmd.site_code.strip().upper(), rc),
                                    ).fetchone()
                                    if row2:
                                        rid2 = int(row2["id"])
                                        devs = conn.execute(
                                            """
                                            SELECT severity, category, description
                                            FROM issues
                                            WHERE report_id = ? AND issue_type = 'SPEC_DEVIATION'
                                            ORDER BY created_at DESC
                                            LIMIT 20
                                            """,
                                            (rid2,),
                                        ).fetchall()
                                        if devs:
                                            # Create a confirmation task (QID) and send a dedicated internal-only email
                                            # to the technician sender + always-cc list.
                                            try:
                                                from peter.interfaces.email.quarantine_queue import new_quarantine_id

                                                qid = new_quarantine_id()
                                                prompt_lines = [
                                                    "CONFIRMATION NEEDED (spec deviation / site stock)",
                                                    f"Reference: {qid}",
                                                    f"Site: {cmd.site_code}",
                                                    f"Report: {rc}",
                                                    "",
                                                    "We detected potential spec deviations from on-site labels/photos.",
                                                    "Some may be empty drums/decanting. Please confirm:",
                                                    "",
                                                ]
                                                for d in devs[:10]:
                                                    cat = str(d["category"])
                                                    sev = str(d["severity"])
                                                    desc = str(d["description"] or "")
                                                    prompt_lines.append(f"- [{sev}] {cat}: {desc[:220]}")

                                                prompt_lines += [
                                                    "",
                                                    "Reply with one of:",
                                                    f"- CONFIRM {qid} | DECISION=USED",
                                                    f"- CONFIRM {qid} | DECISION=NOT_USED",
                                                    f"- CONFIRM {qid} | DECISION=MORE_INFO",
                                                    f"- REJECT {qid}",
                                                    "",
                                                    "(Free text is OK too, e.g. 'Confirm Q-... not used, empty drums only')",
                                                ]
                                                prompt = "\n".join(prompt_lines) + "\n"

                                                # Persist to DB so replies can be processed later.
                                                try:
                                                    conn.execute(
                                                        """
                                                        INSERT INTO issue_confirmations(email_event_id, report_id, qid, status, prompt)
                                                        VALUES (?, ?, ?, 'PENDING', ?)
                                                        """,
                                                        (event_id, rid2, qid, prompt),
                                                    )
                                                except Exception:
                                                    pass

                                                # Send dedicated internal confirmation email (reply in-thread but recipients sanitized)
                                                try:
                                                    draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                                                    draft_id = draft["id"]

                                                    # Subject tag
                                                    graph.update_message(
                                                        mailbox=self.settings.BOT_MAILBOX,
                                                        message_id=draft_id,
                                                        payload={"subject": f"CONFIRM {qid}"},
                                                    )

                                                    to_list, cc_list = build_sanitized_reply_recipients(
                                                        internal_domain=self.settings.INTERNAL_DOMAIN,
                                                        original_from=from_addr,
                                                        original_to=to_addrs,
                                                        original_cc=cc_addrs,
                                                        bot_mailbox=self.settings.BOT_MAILBOX,
                                                        forced_cc=list(self.settings.REVIEW_DLIST),
                                                    )
                                                    assert_internal_only(to_list, cc_list, internal_domain=self.settings.INTERNAL_DOMAIN)

                                                    payload = {
                                                        "toRecipients": [{"emailAddress": {"address": a}} for a in to_list],
                                                        "ccRecipients": [{"emailAddress": {"address": a}} for a in cc_list],
                                                        "body": {"contentType": "Text", "content": prompt},
                                                    }
                                                    graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload=payload)
                                                    graph.send_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id)
                                                except Exception:
                                                    pass

                                                # Also append a short note to the main reply so the thread shows it was requested.
                                                confirm_notes = (
                                                    "\n\nCONFIRMATION REQUEST (internal)\n"
                                                    f"- reference: {qid}\n"
                                                    "A confirmation request was sent to the reporting technician." 
                                                )
                                            except Exception:
                                                # fallback to inline only
                                                lines = [
                                                    "\n\nCONFIRMATION REQUEST (internal)",
                                                    "We detected potential spec deviations from on-site labels/photos.",
                                                    "Some may be empty drums/decanting. Please confirm.",
                                                ]
                                                for d in devs[:10]:
                                                    cat = str(d["category"])
                                                    sev = str(d["severity"])
                                                    desc = str(d["description"] or "")
                                                    lines.append(f"- [{sev}] {cat}: {desc[:180]}")
                                                lines += [
                                                    "\nReply with one of:",
                                                    "- CONFIRM: Used / applied",
                                                    "- CONFIRM: Not used (empty drums / decanting)",
                                                    "- NEEDS MORE INFO: and what evidence is missing",
                                                ]
                                                confirm_notes = "\n".join(lines)
                                except Exception:
                                    confirm_notes = ""

                                try:
                                    use_llm = os.getenv("PETER_EMAIL_DRAFT_USE_OPENAI", "").strip().lower() in ("1", "true", "yes")
                                    if use_llm and self.settings.OPENAI_API_KEY:
                                        from peter.interfaces.email.llm_reply import draft_email_reply_llm

                                        # Provide the vision note as evidence input (includes page refs).
                                        reply_text = draft_email_reply_llm(
                                            conn=conn,
                                            settings=self.settings,
                                            site_code=cmd.site_code,
                                            report_code=rc,
                                            vision_text=vision_note,
                                        )
                                        reply_text = (reply_text or "").rstrip() + (confirm_notes or "") + "\n"
                                    else:
                                        from peter.interfaces.qa.ask import answer_report_question

                                        reply_text = (
                                            answer_report_question(
                                                conn=conn,
                                                settings=self.settings,
                                                site_code=cmd.site_code,
                                                report_code=rc,
                                                question=(
                                                    "Summarize the QA status for this report and list required next actions. "
                                                    "Be concise and use bullets where helpful."
                                                ),
                                                mode="recommend",
                                            ).rstrip()
                                            + vision_note
                                            + (confirm_notes or "")
                                            + "\n"
                                        )
                                except Exception as e:
                                    # If OpenAI is down/quota exceeded, queue this report for reprocessing.
                                    if _is_openai_outage(e):
                                        # Queue metadata only (do NOT duplicate the PDF bytes).
                                        report_id = None
                                        stored_path = None
                                        try:
                                            if isinstance(out, dict):
                                                report_id = out.get("report_id")
                                                stored_path = out.get("stored_path")
                                        except Exception:
                                            pass

                                        item = save_quarantine_item(
                                            data_dir=Path(self.settings.DATA_DIR),
                                            filename=str(p.get("name") or "report.pdf"),
                                            content=b"",
                                            meta={
                                                "status": "PENDING_API",
                                                "kind": "PENDING_API_QA_REPORT",
                                                "graph_message_id": mid,
                                                "site_code": cmd.site_code,
                                                "report_code": rc,
                                                "report_id": report_id,
                                                "stored_path": stored_path,
                                                "retry_count": 0,
                                                "last_retry_at": None,
                                                "note": "Queued due to OpenAI outage/quota; will reprocess automatically",
                                            },
                                        )

                                        reply_text = (
                                            "RECEIVED (queued)\n"
                                            f"- site: {cmd.site_code}\n"
                                            f"- report: {rc}\n"
                                            "- status: OpenAI temporarily unavailable (quota/rate limit).\n"
                                            "- action: queued for automatic reprocessing.\n"
                                            f"- reference: {item.qid}\n"
                                        )
                                    else:
                                        reply_text = f"OK report ingested site={cmd.site_code} report={rc} status={out['status']} report_id={out['report_id']}" + vision_note

                    elif cmd.kind == "QUERY":
                        if not cmd.site_code or not cmd.arg:
                            raise RuntimeError("QUERY missing site or command")
                        q = cmd.arg.strip().upper()
                        if q == "SUMMARY":
                            reply_text = query_svc.summary(cmd.site_code, days=30)
                        elif q == "LATEST":
                            reply_text = query_svc.latest(cmd.site_code)
                        elif q.startswith("FAILS"):
                            parts = q.split()
                            days = int(parts[1]) if len(parts) > 1 else 30
                            reply_text = query_svc.fails(cmd.site_code, days=days)
                        elif q.startswith("TOP ISSUES"):
                            parts = q.split()
                            days = int(parts[-1]) if parts[-1].isdigit() else 30
                            reply_text = query_svc.top_issues(cmd.site_code, days=days)
                        else:
                            reply_text = "Unsupported QUERY. Use SUMMARY, LATEST, FAILS <NDAYS>, TOP ISSUES <NDAYS>"

                    elif cmd.kind == "REPLY":
                        if not cmd.site_code or not cmd.arg:
                            raise RuntimeError("REPLY missing site or report ref")
                        # Draft a fresh internal QA reply for an already-ingested report.
                        rc = cmd.arg.strip().upper().replace(" ", "")
                        try:
                            report_svc.triage_report_text(site_code=cmd.site_code, report_code=rc, reset=True)
                        except Exception:
                            pass

                        vision_note = ""
                        if os.getenv("PETER_VISION_ENABLED", "").strip().lower() in ("1", "true", "yes"):
                            try:
                                out_v = report_svc.analyze_report_visuals(site_code=cmd.site_code, report_code=rc, reset=True)
                                n = len(out_v.get("omission_issues_created") or [])
                                vision_json = out_v.get("vision_json")
                                vision_note = (
                                    "\n\nVISION CHECK (auto)\n"
                                    f"- visual omissions flagged: {n}\n"
                                    f"- artifact: {vision_json}\n"
                                )
                            except Exception as e:
                                vision_note = f"\n\nVISION CHECK (auto)\n- ERROR: {str(e)[:200]}\n"

                        # Compose reply
                        use_llm = os.getenv("PETER_EMAIL_DRAFT_USE_OPENAI", "").strip().lower() in ("1", "true", "yes")
                        if use_llm and self.settings.OPENAI_API_KEY:
                            from peter.interfaces.email.llm_reply import draft_email_reply_llm

                            reply_text = draft_email_reply_llm(
                                conn=conn,
                                settings=self.settings,
                                site_code=cmd.site_code,
                                report_code=rc,
                                vision_text=vision_note,
                            )
                        else:
                            from peter.interfaces.qa.ask import answer_report_question

                            reply_text = (
                                answer_report_question(
                                    conn=conn,
                                    settings=self.settings,
                                    site_code=cmd.site_code,
                                    report_code=rc,
                                    question=(
                                        "Draft the internal QA reply email for this report, including key issues and required next actions."
                                    ),
                                    mode="recommend",
                                ).rstrip()
                                + vision_note
                                + "\n"
                            )

                    elif cmd.kind == "ASSIST":
                        if not cmd.site_code:
                            raise RuntimeError("ASSIST missing site")
                        from peter.interfaces.email.assist import run_assist

                        # Prefer message body as the freeform request; fall back to subject arg.
                        req = (cmd.arg or "").strip()
                        try:
                            full = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=mid, select="body")
                            body = (full.get("body") or {}).get("content") or ""
                            body_req = str(body).strip()
                            if body_req:
                                req = body_req
                        except Exception:
                            pass

                        if not req:
                            raise RuntimeError("ASSIST missing request (empty body and no subject arg)")

                        reply_text = run_assist(conn=conn, settings=self.settings, site_code=cmd.site_code, request=req)
                    else:
                        # If subject is not recognized:
                        # - If there is a known site code in the subject, treat as ASSIST (conversational).
                        # - Otherwise, send a friendly instruction (internal-only) on how to format subjects.
                        if cmd.kind == "ASSIST" and cmd.site_code:
                            # Prefer message body as the request.
                            from peter.interfaces.email.assist import run_assist

                            req = (cmd.arg or "").strip()
                            try:
                                full = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=mid, select="body")
                                body = (full.get("body") or {}).get("content") or ""
                                if str(body).strip():
                                    req = str(body).strip()
                            except Exception:
                                pass

                            if not req:
                                raise RuntimeError("ASSIST missing request (empty email body)")

                            reply_text = run_assist(conn=conn, settings=self.settings, site_code=cmd.site_code, request=req)
                        else:
                            # Still mark read to avoid loops, but if internal-only we can reply with guidance.
                            # Keep it simple and actionable.
                            reply_text = (
                                "I couldn't identify which site/project this email refers to.\n\n"
                                "Please include the SITE CODE in the email subject (e.g. '... <SITECODE> ...').\n"
                                "Then write your question normally in the email body (e.g. 'latest QA update? top issues?').\n\n"
                                "Example subject formats:\n"
                                "- QA update <SITECODE>\n"
                                "- Re: <SITECODE> QA reports\n"
                            )
                            # We *do* send this guidance reply (internal-only recipients will be enforced later).
                            should_send = True
                except Exception as e:
                    from peter.interfaces.email.error_format import make_error_id, format_error_email, format_trace_for_logs

                    error_id = make_error_id()
                    # Log full traceback with an id the user can search.
                    import logging

                    logging.getLogger("peter.email").error(format_trace_for_logs(error_id=error_id, exc=e))

                    # Friendly technician-facing error (LLM), fallback to structured error.
                    body_excerpt = ""
                    try:
                        full = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=mid, select="body")
                        body_excerpt = str(((full.get("body") or {}).get("content") or "")).strip()
                    except Exception:
                        body_excerpt = ""

                    friendly = _friendly_error_via_llm(
                        settings=self.settings,
                        subject=subject,
                        body=body_excerpt,
                        error_id=error_id,
                        exc=e,
                    )
                    reply_text = friendly or format_error_email(cmd=str(cmd), stage="handler", error_id=error_id, exc=e)

                if not should_send:
                    # Still mark read to avoid loops.
                    try:
                        graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                    except Exception:
                        pass
                    continue

                # Create reply draft (this preserves in-thread metadata + quoted original)
                draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                draft_id = draft["id"]

                # Update subject to include a result tag.
                try:
                    # Best-effort: infer result tag from DB.
                    row = conn.execute(
                        """
                        SELECT r.result
                        FROM reports r
                        JOIN sites s ON s.id = r.site_id
                        WHERE s.site_code = ? AND r.report_code = ?
                        ORDER BY r.received_at DESC
                        LIMIT 1
                        """,
                        ((cmd.site_code or "").strip().upper(), (cmd.arg or "").strip().upper().replace(" ", "")),
                    ).fetchone()
                    res = (str(row["result"]) if row and row["result"] else "PENDING").upper()
                    tag = f"[PETER: {res}]"
                    subj = (subject or "").strip()
                    if tag not in subj:
                        graph.update_message(
                            mailbox=self.settings.BOT_MAILBOX,
                            message_id=draft_id,
                            payload={"subject": f"{subj} {tag}".strip()},
                        )
                except Exception:
                    pass

                # Preserve the quoted original body by prepending our reply content
                # instead of overwriting the entire body.
                try:
                    draft_msg = graph.get_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, select="body")
                    body = draft_msg.get("body") or {}
                    ctype = str(body.get("contentType") or "Text")
                    original_content = str(body.get("content") or "")

                    if ctype.lower() == "html":
                        reply_html = "<p>" + _escape_html(reply_text).replace("\n", "<br>") + "</p><hr>"
                        merged = reply_html + original_content
                        reply_body = {"contentType": "HTML", "content": merged}
                    else:
                        merged = (reply_text or "") + "\n\n" + original_content
                        reply_body = {"contentType": "Text", "content": merged}
                except Exception:
                    # Fallback: overwrite body if we cannot fetch draft body
                    reply_body = {"contentType": "Text", "content": reply_text}

                to_list, cc_list = build_sanitized_reply_recipients(
                    internal_domain=self.settings.INTERNAL_DOMAIN,
                    original_from=from_addr,
                    original_to=to_addrs,
                    original_cc=cc_addrs,
                    bot_mailbox=self.settings.BOT_MAILBOX,
                    forced_cc=list(self.settings.REVIEW_DLIST),
                )
                assert_internal_only(to_list, cc_list, internal_domain=self.settings.INTERNAL_DOMAIN)

                payload = {
                    "toRecipients": [{"emailAddress": {"address": a}} for a in to_list],
                    "ccRecipients": [{"emailAddress": {"address": a}} for a in cc_list],
                    "body": reply_body,
                }

                graph.update_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id, payload=payload)
                graph.send_message(mailbox=self.settings.BOT_MAILBOX, message_id=draft_id)
                graph.mark_read(mailbox=self.settings.BOT_MAILBOX, message_id=mid)

            return stats


def main() -> None:
    settings = Settings.load()
    settings.ensure_paths_exist()
    EmailWatcher(settings).run_forever()


if __name__ == "__main__":
    main()
