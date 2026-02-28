from __future__ import annotations

import argparse
import sys

from pathlib import Path

from peter.config.logging import configure_logging
from peter.config.settings import Settings
from peter.db.connection import get_connection
from peter.db.schema import init_db
from peter.services.site_service import SiteService
from peter.services.spec_service import SpecService
from peter.services.report_service import ReportService
from peter.services.query_service import QueryService


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="peter", description="PETER - Decorative Coatings QA System (CLI)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s_create = sub.add_parser("create-site", help="Create a new site")
    s_create.add_argument("--code", required=True)
    s_create.add_argument("--name", required=True)
    s_create.add_argument("--address", default="")

    sub.add_parser("list-sites", help="List sites")

    sp_ingest = sub.add_parser("ingest-spec", help="Ingest a spec PDF for a site")
    sp_ingest.add_argument("--code", required=True)
    sp_ingest.add_argument("--version", required=True, help="e.g. REV01")
    sp_ingest.add_argument("--file", required=True, help="Path to spec PDF")

    rp_ingest = sub.add_parser("ingest-report", help="Ingest a QA report PDF for a site")
    rp_ingest.add_argument("--code", required=True)
    rp_ingest.add_argument("--report-code", required=True, help="e.g. R03")
    rp_ingest.add_argument("--file", required=True, help="Path to report PDF")

    ar = sub.add_parser("analyze-report", help="Run visual verification on a report (Vision)")
    ar.add_argument("--code", required=True)
    ar.add_argument("--report-code", required=True)
    ar.add_argument("--reset", action="store_true", help="Delete existing issues for this report before re-analysis")

    sr = sub.add_parser("summarize-report", help="Summarize a report (text-only) and list deterministic flags")
    sr.add_argument("--code", required=True)
    sr.add_argument("--report-code", required=True)

    tr = sub.add_parser("triage-report", help="Create DB issues + set report result from text-only flags")
    tr.add_argument("--code", required=True)
    tr.add_argument("--report-code", required=True)
    tr.add_argument("--reset", action="store_true", help="Delete existing issues for this report before triage")

    dr = sub.add_parser("demo-report", help="One-shot demo output: summary + flags + top issues")
    dr.add_argument("--code", required=True)
    dr.add_argument("--report-code", required=True)
    dr.add_argument("--days", type=int, default=365)

    ask = sub.add_parser("ask", help="Ask questions about a specific report (grounded by default)")
    ask.add_argument("--code", required=True)
    ask.add_argument("--report-code", required=True)
    ask.add_argument("--question", required=True)
    ask.add_argument("--mode", choices=["grounded", "recommend"], default="grounded")

    lr = sub.add_parser("list-reports", help="List reports for a site")
    lr.add_argument("--code", required=True)
    lr.add_argument("--limit", type=int, default=20)

    ia = sub.add_parser("image-audit", help="Audit report pages for photo/table/labels (no defect inference)")
    ia.add_argument("--code", required=True)
    ia.add_argument("--report-code", required=True)

    sub.add_parser("daemon", help="Run long-lived service loop")
    sub.add_parser("inbox-once", help="Process filesystem INBOX once and exit")

    ep = sub.add_parser("email-poll", help="Run Microsoft Graph inbox poll loop (Phase 2)")

    q = sub.add_parser("query", help="Query site history")
    q.add_argument("--code", required=True)
    q.add_argument("--type", required=True, choices=["SUMMARY", "LATEST", "FAILS", "TOP_ISSUES"])
    q.add_argument("--days", type=int, default=30)

    return p


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)

    settings = Settings.load()
    settings.ensure_paths_exist()

    with get_connection(settings.DB_PATH) as conn:
        init_db(conn)

        site_svc = SiteService(conn, settings)
        spec_svc = SpecService(conn, settings)
        report_svc = ReportService(conn, settings)
        query_svc = QueryService(conn, settings)

        if args.cmd == "create-site":
            site = site_svc.create_site(site_code=args.code, site_name=args.name, address=args.address)
            print(f"OK created site: {site.site_code} ({site.site_name}) folder={site.folder_name}")
            return 0

        if args.cmd == "list-sites":
            sites = site_svc.list_sites()
            for s in sites:
                print(f"{s.site_code}\t{s.site_name}\t{s.folder_name}")
            return 0

        if args.cmd == "ingest-spec":
            path = Path(args.file).expanduser().resolve()
            spec = spec_svc.ingest_spec(site_code=args.code, version_label=args.version, file_path=path)
            print(
                f"OK spec ingested: site={args.code} spec_id={spec.id} version={spec.version_label} active={spec.is_active}"
            )
            if spec.extracted_text_path is None:
                print("NOTE: PDF text extraction not available; extracted_text_path/checklist_json_path are empty.")
            return 0

        if args.cmd == "ingest-report":
            path = Path(args.file).expanduser().resolve()
            out = report_svc.ingest_report(site_code=args.code, report_code=args.report_code, file_path=path)
            print(
                f"OK report ingested: site={args.code} report={args.report_code} status={out['status']} report_id={out['report_id']}"
            )
            if out.get("extracted_text_path") is None:
                print("NOTE: no meaningful text extracted (OCR/Vision path needed later).")
            return 0

        if args.cmd == "analyze-report":
            out = report_svc.analyze_report_visuals(site_code=args.code, report_code=args.report_code, reset=bool(args.reset))
            print(
                f"OK visual analysis: report_id={out['report_id']} omissions={len(out['omission_issues_created'])} vision_json={out['vision_json']}"
            )
            return 0

        if args.cmd == "summarize-report":
            out = report_svc.summarize_report_text(site_code=args.code, report_code=args.report_code)
            print(out)
            return 0

        if args.cmd == "triage-report":
            out = report_svc.triage_report_text(site_code=args.code, report_code=args.report_code, reset=bool(args.reset))
            print(out)
            return 0

        if args.cmd == "demo-report":
            site_code = (args.code or "").strip().upper()
            report_code = (args.report_code or "").strip()

            # Show latest stored report row (result + stored path).
            row = conn.execute(
                """
                SELECT r.id, r.sha256, r.stored_path, r.received_at, r.result
                FROM reports r
                JOIN sites s ON s.id = r.site_id
                WHERE s.site_code = ? AND r.report_code = ?
                ORDER BY r.received_at DESC
                LIMIT 1
                """,
                (site_code, report_code),
            ).fetchone()

            if not row:
                raise SystemExit(f"Report not found for site={site_code} report_code={report_code}")

            print(
                "DEMO REPORT\n"
                f"site={site_code} report={report_code} report_id={int(row['id'])}\n"
                f"received_at={row['received_at']} result={row['result']}\n"
                f"stored_path={row['stored_path']}\n"
                f"sha256={row['sha256']}\n"
            )

            # Summary + deterministic flags
            print(report_svc.summarize_report_text(site_code=site_code, report_code=report_code))

            # Top issues (requires triage/analysis to have populated issues)
            print(query_svc.top_issues(site_code, days=int(args.days)))
            return 0

        if args.cmd == "ask":
            from peter.interfaces.qa.ask import answer_report_question

            print(
                answer_report_question(
                    conn=conn,
                    settings=settings,
                    site_code=args.code,
                    report_code=args.report_code,
                    question=args.question,
                    mode=args.mode,
                )
            )
            return 0

        if args.cmd == "list-reports":
            site_code = (args.code or "").strip().upper()
            limit = max(1, int(args.limit))
            rows = conn.execute(
                """
                SELECT r.report_code, r.received_at, r.result, r.sha256
                FROM reports r
                JOIN sites s ON s.id = r.site_id
                WHERE s.site_code = ?
                ORDER BY r.received_at DESC
                LIMIT ?
                """,
                (site_code, limit),
            ).fetchall()
            if not rows:
                print("No reports for this site.")
                return 0
            print(f"REPORTS site={site_code} (latest {len(rows)})")
            for r in rows:
                print(f"- {r['report_code']} received_at={r['received_at']} result={r['result']} sha={str(r['sha256'])[:12]}")
            return 0

        if args.cmd == "image-audit":
            out = report_svc.image_audit(site_code=args.code, report_code=args.report_code)
            print(out)
            return 0

        if args.cmd == "daemon":
            from peter.daemon import run as daemon_run

            return int(daemon_run())

        if args.cmd == "inbox-once":
            from peter.daemon import process_inbox_once

            settings.ensure_paths_exist()
            process_inbox_once(settings=settings)
            print("OK inbox processed")
            return 0

        if args.cmd == "email-poll":
            from peter.interfaces.email.watcher import main as email_main

            email_main()
            return 0

        if args.cmd == "query":
            if args.type == "SUMMARY":
                out = query_svc.summary(args.code, days=args.days)
            elif args.type == "LATEST":
                out = query_svc.latest(args.code)
            elif args.type == "FAILS":
                out = query_svc.fails(args.code, days=args.days)
            else:
                out = query_svc.top_issues(args.code, days=args.days)
            print(out)
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
