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
from peter.interfaces.email.confirm_commands import parse_confirm_subject
from peter.interfaces.email.quarantine_queue import load_quarantine_item, save_quarantine_item, update_status
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

    def run_once(self) -> None:
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

                # Handle quarantine confirmation commands first (subject-based)
                conf = parse_confirm_subject(subject)
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

                        # Ingest as QA_REPORT
                        out = report_svc.ingest_report(site_code=site, report_code=rep, file_path=item.file_path)
                        try:
                            report_svc.triage_report_text(site_code=site, report_code=rep, reset=True)
                        except Exception:
                            pass
                        update_status(item=item, status="CONFIRMED", extra={"confirmed_by": from_addr_cmd, "site": site, "report": rep, "ingest": out})

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

                        # Resolve site sandbox for quarantine/archival
                        site = site_repo.get_by_code(cmd.site_code)
                        if not site:
                            raise RuntimeError(f"Unknown site_code: {cmd.site_code}")
                        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

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

                            claimed_site = (cmd.site_code or "").strip().upper()
                            claimed_rep = (cmd.arg or "").strip().upper().replace(" ", "")
                            # normalize claimed report to 3-digit numeric if possible
                            if claimed_rep.startswith("R") and claimed_rep[1:].isdigit():
                                claimed_rep = claimed_rep[1:]
                            if claimed_rep.isdigit():
                                claimed_rep = claimed_rep.zfill(3)

                            if detected and claimed_site and claimed_rep and (detected.site_code != claimed_site or detected.report_no != claimed_rep):
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
                                            "\n\nVISION CHECK (auto)\n"
                                            f"- visual omissions flagged: {n}\n"
                                            f"- artifact: {vision_json}\n"
                                        )

                                        # Add short, human-readable vision findings (blocking + notable)
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
                                                vision_note += "\nVISION — Blocking photo findings\n" + "\n".join(vs.blocking[:10]) + "\n"
                                            if vs.notable:
                                                vision_note += "\nVISION — Notable observations (non-blocking)\n" + "\n".join(vs.notable) + "\n"
                                        except Exception:
                                            pass
                                    except Exception as e:
                                        vision_note = f"\n\nVISION CHECK (auto)\n- ERROR: {str(e)[:200]}\n"

                                # Compose a human-like operational reply.
                                # Prefer LLM drafting if enabled; fall back to heuristic ask.
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
                                    else:
                                        from peter.interfaces.qa.ask import answer_report_question

                                        reply_text = (
                                            answer_report_question(
                                                conn=conn,
                                                settings=self.settings,
                                                site_code=cmd.site_code,
                                                report_code=rc,
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
                    else:
                        # If subject is not recognized, do NOT send any email.
                        # We will attempt attachment-driven inference for a single PDF.
                        should_send = False
                        reply_text = "IGNORED: Unrecognized subject format (no reply sent)."
                except Exception as e:
                    reply_text = f"ERROR: {e}"

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


def main() -> None:
    settings = Settings.load()
    settings.ensure_paths_exist()
    EmailWatcher(settings).run_forever()


if __name__ == "__main__":
    main()
