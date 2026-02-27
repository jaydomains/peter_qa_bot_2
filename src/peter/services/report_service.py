from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


import re
import shutil
from pathlib import Path

from peter.db.repositories.site_repo import SiteRepository
from peter.db.repositories.report_repo import ReportRepository
from peter.domain.errors import ValidationError
import json

from peter.parsing.pdf_text import extract_pdf_text, has_meaningful_text
from peter.parsing.pdf_render import render_pdf_pages
from peter.storage.filestore import ensure_site_folders
from peter.util.hashing import sha256_file
from peter.vision.openai_vision import analyze_page_image, VisionError
from peter.db.repositories.issue_repo import IssueRepository


class ReportService:
    """Milestones:
    - M3: report ingestion (storage + idempotency + text extraction)
    - M4: visual verification (Vision on every report + text/photo cross-check)
    """

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings
        self.site_repo = SiteRepository(conn)
        self.report_repo = ReportRepository(conn)
        self.issue_repo = IssueRepository(conn)

    def _validate_report_code(self, report_code: str) -> str:
        rc = (report_code or "").strip().upper().replace(" ", "")
        if not re.fullmatch(r"R\d{2}", rc):
            raise ValidationError("report_code must look like R01 / R12")
        return rc

    def ingest_report(self, *, site_code: str, report_code: str, file_path: Path) -> dict:
        site_code = (site_code or "").strip().upper()
        rc = self._validate_report_code(report_code)
        path = Path(file_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValidationError(f"Report file not found: {path}")
        if path.suffix.lower() != ".pdf":
            raise ValidationError("Report must be a PDF")

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

        sha = sha256_file(path)
        existing = self.report_repo.get_by_site_sha(site.id, sha)
        if existing:
            return {
                "status": "duplicate",
                "report_id": existing.id,
                "overall_result": existing.result,
                "sha256": sha,
            }

        safe_filename = f"{site.site_code}__REPORT__{rc}__{sha[:12]}.pdf"
        stored_pdf = sandbox.build_path("02_reports", safe_filename)
        shutil.copy2(path, stored_pdf)

        # Extract text for later analysis (M4)
        text = extract_pdf_text(stored_pdf)
        meaningful = has_meaningful_text(text)
        extracted_rel = None
        if meaningful:
            txt_name = f"{site.site_code}__REPORT__{rc}__{sha[:12]}.txt"
            txt_path = sandbox.build_path("00_admin", txt_name)
            txt_path.write_text(text, encoding="utf-8")
            extracted_rel = str(txt_path.relative_to(self.settings.QA_ROOT))

        stored_rel = str(stored_pdf.relative_to(self.settings.QA_ROOT))
        report_id = self.report_repo.insert(
            site_id=site.id,
            report_code=rc,
            filename=safe_filename,
            sha256=sha,
            stored_path=stored_rel,
            result=None,
            review_md_path=None,
            review_json_path=None,
        )

        return {
            "status": "ok",
            "report_id": report_id,
            "overall_result": None,
            "sha256": sha,
            "stored_path": stored_rel,
            "extracted_text_path": extracted_rel,
        }

    def analyze_report_visuals(self, *, site_code: str, report_code: str) -> dict:
        """Run Vision across all pages and create blocking issues for visual omissions.

        Policy: visual-only findings do NOT auto-fail; they set overall to WARN and create blocking issues.
        """

        site_code = (site_code or "").strip().upper()
        rc = self._validate_report_code(report_code)

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

        # Find report row by sha is best, but for now pick latest by report_code.
        row = self.conn.execute(
            """
            SELECT id, sha256, stored_path
            FROM reports
            WHERE site_id = ? AND report_code = ?
            ORDER BY received_at DESC
            LIMIT 1
            """,
            (site.id, rc),
        ).fetchone()
        if not row:
            raise ValidationError(f"Report not found for site={site_code} report_code={rc}")

        report_id = int(row["id"])
        sha = str(row["sha256"])
        stored_rel = str(row["stored_path"])

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
        pdf_path = sandbox.build_path(stored_rel.split("/", 1)[1]) if stored_rel.startswith("SITES/") else (self.settings.QA_ROOT / stored_rel)
        pdf_path = pdf_path.resolve()

        pages_dir = sandbox.ensure_dir("03_reviews", f"{site.site_code}__{rc}__{sha[:12]}__pages")
        rendered = render_pdf_pages(pdf_path, out_dir=pages_dir, prefix=f"{site.site_code}__{rc}__{sha[:12]}", dpi=300)

        api_key = self.settings.OPENAI_API_KEY
        model = os.getenv("PETER_VISION_MODEL", "gpt-4.1")

        # Naive defect extraction from text (v0): keyword presence.
        extracted_text = ""
        txt_files = list(sandbox.build_path("00_admin").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.txt"))
        if txt_files:
            extracted_text = txt_files[0].read_text(encoding="utf-8", errors="replace").lower()
        reported_defects = set()
        for k in ["crack", "cracking", "flaking", "peeling", "blister", "efflores", "delamination", "damp", "mould", "mold", "stain"]:
            if k in extracted_text:
                reported_defects.add(k)

        vision_results = []
        omissions = []
        for idx, img_path in enumerate(rendered.page_paths, start=1):
            try:
                vr = analyze_page_image(api_key=api_key, model=model, page_number=idx, image_path=img_path)
            except VisionError as e:
                vision_results.append({"page": idx, "error": str(e)})
                continue

            vision_results.append(
                {
                    "page": idx,
                    "summary": vr.summary,
                    "findings": [f.__dict__ for f in vr.findings],
                }
            )

            for f in vr.findings:
                key = f.defect.lower()
                # very simple matching in v0
                if any(k in key for k in ["crack", "flak", "peel", "blister", "efflores", "delamin", "damp", "mould", "stain"]):
                    # If not mentioned in reported defects at all, treat as omission.
                    if not any(k in extracted_text for k in ["crack", "flak", "peel", "blister", "efflores", "delamin", "damp", "mould", "stain"]):
                        omissions.append((idx, f))

        # Persist vision artifact
        vision_name = f"{site.site_code}__{rc}__{sha[:12]}__vision.json"
        vision_path = sandbox.build_path("03_reviews", vision_name)
        vision_path.write_text(json.dumps({"report_id": report_id, "pages": vision_results}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        created_issue_ids: list[int] = []
        if omissions:
            for page_num, finding in omissions:
                sev = finding.severity
                desc = (
                    f"Visual omission: photo/page evidence suggests '{finding.defect}' (confidence={finding.confidence:.2f}) "
                    f"but it does not appear to be recorded in the report text. Page {page_num}. Notes: {finding.notes}"
                )
                issue_id = self.issue_repo.insert(
                    report_id=report_id,
                    issue_type="BEST_PRACTICE_RISK",
                    category="Visual omission",
                    description=desc,
                    severity=sev,
                    is_blocking=True,
                )
                created_issue_ids.append(issue_id)

            # set overall result to WARN (policy)
            self.report_repo.update_result_and_paths(
                report_id=report_id,
                result="WARN",
                review_md_path=None,
                review_json_path=str(vision_path.relative_to(self.settings.QA_ROOT)),
            )

        return {
            "report_id": report_id,
            "vision_json": str(vision_path),
            "omission_issues_created": created_issue_ids,
            "overall_result_set": "WARN" if created_issue_ids else None,
        }
