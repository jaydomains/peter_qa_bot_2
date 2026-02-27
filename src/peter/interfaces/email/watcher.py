from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
            site_svc = SiteService(conn, self.settings)
            spec_svc = SpecService(conn, self.settings)
            report_svc = ReportService(conn, self.settings)
            query_svc = QueryService(conn, self.settings)

            msgs = graph.list_unread_messages(mailbox=self.settings.BOT_MAILBOX, top=10)
            for m in msgs:
                mid = m["id"]
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

                # TODO: archive original message as .eml (Graph supports $value or MIME content endpoints)
                email_repo.insert_event(
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
                    archived_eml_path=None,
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
                        attachments = graph.list_attachments(mailbox=self.settings.BOT_MAILBOX, message_id=mid)
                        pdfs = [a for a in attachments if (a.get("contentType") or "").lower() == "application/pdf"]
                        if not pdfs:
                            reply_text = "No PDF attachment found."
                        else:
                            # take first pdf (tighten later)
                            att_meta = pdfs[0]
                            att = graph.get_attachment(
                                mailbox=self.settings.BOT_MAILBOX,
                                message_id=mid,
                                attachment_id=att_meta["id"],
                            )
                            if att.get("@odata.type", "").endswith("fileAttachment") and att.get("contentBytes"):
                                data = base64.b64decode(att["contentBytes"])
                                name = att.get("name") or "attachment.pdf"
                                # Write to a temp drop folder under data/ for ingestion
                                drop = Path(self.settings.DATA_DIR) / "email_drop"
                                drop.mkdir(parents=True, exist_ok=True)
                                out_path = drop / f"{cmd.site_code}__{cmd.kind}__{mid}__{name}"
                                out_path.write_bytes(data)

                                if cmd.kind == "SPEC_UPDATE":
                                    spec = spec_svc.ingest_spec(site_code=cmd.site_code, version_label=cmd.arg or "REV01", file_path=out_path)
                                    reply_text = f"OK spec ingested site={cmd.site_code} spec_id={spec.id} version={spec.version_label}"
                                else:
                                    rc = (cmd.arg or "R01").strip().upper().replace(" ", "")
                                    out = report_svc.ingest_report(site_code=cmd.site_code, report_code=rc, file_path=out_path)
                                    reply_text = f"OK report ingested site={cmd.site_code} report={rc} status={out['status']} report_id={out['report_id']}"
                            else:
                                reply_text = "Attachment type unsupported (expected fileAttachment with contentBytes)."

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
