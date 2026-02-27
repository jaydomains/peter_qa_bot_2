from __future__ import annotations

import base64
import time
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
from peter.interfaces.email.graph_auth import client_credentials_token
from peter.interfaces.email.graph_client import GraphClient
from peter.interfaces.email.recipient_policy import (
    assert_internal_only,
    build_sanitized_reply_recipients,
)
from peter.services.site_service import SiteService
from peter.services.spec_service import SpecService
from peter.services.report_service import ReportService
from peter.services.query_service import QueryService


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
                cmd = parse_subject(subject)

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

                        # Classify PDFs
                        def _is_pdf(item: dict[str, Any]) -> bool:
                            ctype = (item.get("content_type") or "").lower()
                            name = (item.get("name") or "").lower()
                            return ctype == "application/pdf" or name.endswith(".pdf")

                        pdfs = [d for d in downloaded if _is_pdf(d)]
                        others = [d for d in downloaded if not _is_pdf(d)]

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
                            # Exactly one PDF -> ingest
                            p = pdfs[0]

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
                                reply_text = f"OK report ingested site={cmd.site_code} report={rc} status={out['status']} report_id={out['report_id']}"

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
                        reply_text = "Unrecognized subject format. Expected: QA REPORT | <SITE_CODE> | R01"
                except Exception as e:
                    reply_text = f"ERROR: {e}"

                # Create reply draft
                draft = graph.create_reply_draft(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                draft_id = draft["id"]

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
                    "body": {"contentType": "Text", "content": reply_text},
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
