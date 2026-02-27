from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


import json
import os
import re
import shutil
from pathlib import Path

from peter.db.repositories.issue_repo import IssueRepository
from peter.db.repositories.report_issue_repo import ReportIssueRepository
from peter.db.repositories.report_repo import ReportRepository
from peter.db.repositories.site_repo import SiteRepository
from peter.domain.errors import ValidationError
from peter.parsing.pdf_render import render_pdf_pages
from peter.parsing.pdf_text import extract_pdf_text, has_meaningful_text
from peter.storage.filestore import ensure_site_folders
from peter.util.hashing import sha256_file
from peter.vision.openai_vision import VisionError, analyze_page_image
from peter.vision.image_audit import audit_page_image, ImageAuditError


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
        self.report_issue_repo = ReportIssueRepository(conn)

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
            # Best-effort locate extracted text file for user feedback
            extracted_rel = None
            try:
                sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
                txt_files = list(sandbox.build_path("00_admin").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.txt"))
                if txt_files:
                    extracted_rel = str(txt_files[0].relative_to(self.settings.QA_ROOT))
            except Exception:
                extracted_rel = None

            return {
                "status": "duplicate",
                "report_id": existing.id,
                "overall_result": existing.result,
                "sha256": sha,
                "extracted_text_path": extracted_rel,
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

    def summarize_report_text(self, *, site_code: str, report_code: str) -> str:
        """Text-only baseline summary + deterministic flags.

        This is intentionally conservative (no vision).
        """

        site_code = (site_code or "").strip().upper()
        rc = self._validate_report_code(report_code)

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

        row = self.conn.execute(
            """
            SELECT id, sha256
            FROM reports
            WHERE site_id = ? AND report_code = ?
            ORDER BY received_at DESC
            LIMIT 1
            """,
            (site.id, rc),
        ).fetchone()
        if not row:
            raise ValidationError(f"Report not found for site={site_code} report_code={rc}")

        sha = str(row["sha256"])
        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

        # Prefer cached extracted text file
        txt_files = list(sandbox.build_path("00_admin").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.txt"))
        raw_text = ""
        if txt_files:
            raw_text = txt_files[0].read_text(encoding="utf-8", errors="replace")
        else:
            # fall back: extract from stored PDF
            pdf_files = list(sandbox.build_path("02_reports").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.pdf"))
            if not pdf_files:
                raise ValidationError("Could not locate stored PDF for report")
            raw_text = extract_pdf_text(pdf_files[0])

        from peter.analysis.text_clean import clean_extracted_text
        from peter.analysis.summary_flags import build_flags, extract_section_excerpt

        clean = clean_extracted_text(raw_text)
        flags = build_flags(clean)

        parts: list[str] = []
        parts.append(f"REPORT SUMMARY (text-only)\nsite={site.site_code} report={rc} sha={sha}")

        ex = extract_section_excerpt(clean, "Executive Summary", window=700)
        if ex:
            parts.append("\nEXECUTIVE SUMMARY (excerpt)\n" + ex)

        ex2 = extract_section_excerpt(clean, "Test Summary", window=900)
        if ex2:
            parts.append("\nTEST SUMMARY (excerpt)\n" + ex2)

        parts.append("\nFLAGS")
        if not flags:
            parts.append("- None")
        else:
            for fl in flags:
                parts.append(f"- {fl.title} [{fl.key}]")
                for ev in fl.evidence:
                    parts.append(f"    • {ev}")

        return "\n".join(parts) + "\n"

    def image_audit(self, *, site_code: str, report_code: str) -> str:
        """Audit pages for photos/tables/labels. No defect inference."""

        site_code = (site_code or "").strip().upper()
        rc = self._validate_report_code(report_code)

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

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

        sha = str(row["sha256"])
        stored_rel = str(row["stored_path"])

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
        pdf_path = (
            sandbox.build_path(stored_rel.split("/", 1)[1])
            if stored_rel.startswith("SITES/")
            else (self.settings.QA_ROOT / stored_rel)
        ).resolve()

        pages_dir = sandbox.ensure_dir("03_reviews", f"{site.site_code}__{rc}__{sha[:12]}__pages")
        rendered = render_pdf_pages(
            pdf_path,
            out_dir=pages_dir,
            prefix=f"{site.site_code}__{rc}__{sha[:12]}",
            dpi=300,
        )

        api_key = self.settings.OPENAI_API_KEY
        model = os.getenv("PETER_VISION_MODEL", "gpt-4.1")

        lines = [f"IMAGE AUDIT\nsite={site.site_code} report={rc} sha={sha}"]

        for idx, img_path in enumerate(rendered.page_paths, start=1):
            try:
                a = audit_page_image(api_key=api_key, model=model, page_number=idx, image_path=img_path)
                lines.append(
                    f"- PDF page {a.pdf_page_number}: photos~{a.photo_count_estimate} table/form={a.has_table_or_form} labels={a.has_labels_or_callouts}"
                )
            except ImageAuditError as e:
                lines.append(f"- PDF page {idx}: ERROR {str(e)[:160]}")

        return "\n".join(lines) + "\n"

    def analyze_report_visuals(self, *, site_code: str, report_code: str, reset: bool = False) -> dict:
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
        if reset:
            self.report_issue_repo.delete_for_report(report_id)
        sha = str(row["sha256"])
        stored_rel = str(row["stored_path"])

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)
        pdf_path = sandbox.build_path(stored_rel.split("/", 1)[1]) if stored_rel.startswith("SITES/") else (self.settings.QA_ROOT / stored_rel)
        pdf_path = pdf_path.resolve()

        pages_dir = sandbox.ensure_dir("03_reviews", f"{site.site_code}__{rc}__{sha[:12]}__pages")
        rendered = render_pdf_pages(pdf_path, out_dir=pages_dir, prefix=f"{site.site_code}__{rc}__{sha[:12]}", dpi=300)

        api_key = self.settings.OPENAI_API_KEY
        model = os.getenv("PETER_VISION_MODEL", "gpt-4.1")

        # Extract defects mentioned in text (canonical taxonomy)
        extracted_text = ""
        txt_files = list(sandbox.build_path("00_admin").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.txt"))
        if txt_files:
            extracted_text = txt_files[0].read_text(encoding="utf-8", errors="replace")

        from peter.analysis.text_defects import extract_text_defects

        reported_defects = extract_text_defects(extracted_text)

        vision_results = []
        omissions = []  # visual omissions only (PHOTO basis)
        observations = []  # non-blocking photo observations
        moisture_findings = []  # PAGE_TEXT_OR_TABLE moisture findings to merge
        for idx, img_path in enumerate(rendered.page_paths, start=1):
            try:
                vr = analyze_page_image(api_key=api_key, model=model, page_number=idx, image_path=img_path)
            except VisionError as e:
                vision_results.append({"page": idx, "error": str(e)[:2000]})
                continue

            vision_results.append(
                {
                    "page": idx,
                    "summary": vr.summary,
                    "findings": [f.__dict__ for f in vr.findings],
                }
            )

            from peter.analysis.defect_taxonomy import (
                CanonicalDefect,
                MUST_NOT_MISS_VISUAL_TIER1,
                MUST_NOT_MISS_VISUAL_TIER2,
            )

            for f in vr.findings:
                canonical: set[CanonicalDefect] = set()
                for c in (f.canonical_defects or []):
                    try:
                        canonical.add(CanonicalDefect(c))
                    except Exception:
                        continue

                is_severe = f.severity in ("HIGH", "CRITICAL")
                conf = float(f.confidence)
                basis = (getattr(f, "evidence_basis", "PHOTO") or "PHOTO").upper()

                # Tiered confidence rules for PHOTO omissions
                tier1 = any(c in MUST_NOT_MISS_VISUAL_TIER1 for c in canonical)
                tier2 = any(c in MUST_NOT_MISS_VISUAL_TIER2 for c in canonical)

                # 1) Visual omission: ONLY when PHOTO basis
                if basis == "PHOTO":
                    block = False
                    if tier1 and conf >= 0.80:
                        block = True
                    if tier2 and (conf >= 0.90 or is_severe):
                        block = True
                    if is_severe and conf >= 0.80:
                        block = True

                    if block:
                        if not (canonical & reported_defects):
                            omissions.append((idx, f))
                    else:
                        # Non-blocking photo observations (Bucket A)
                        if canonical:
                            observations.append((idx, f))

                # 2) Non-visual moisture findings from tables/text: collect then merge into one blocker
                if basis == "PAGE_TEXT_OR_TABLE":
                    if CanonicalDefect.DAMPNESS_MOULD_ALGAE in canonical and conf >= 0.80:
                        moisture_findings.append((idx, f))

        # Persist vision artifact
        vision_name = f"{site.site_code}__{rc}__{sha[:12]}__vision.json"
        vision_path = sandbox.build_path("03_reviews", vision_name)
        vision_path.write_text(json.dumps({"report_id": report_id, "pages": vision_results}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        created_issue_ids: list[int] = []

        def set_warn_if_needed() -> None:
            # policy: visual/table risks set WARN, never auto-FAIL.
            self.report_repo.update_result_and_paths(
                report_id=report_id,
                result="WARN",
                review_md_path=None,
                review_json_path=str(vision_path.relative_to(self.settings.QA_ROOT)),
            )

        if omissions:
            for page_num, finding in omissions:
                sev = finding.severity
                canon = ",".join(getattr(finding, "canonical_defects", []) or [])
                desc = (
                    f"Visual omission (PHOTO): canonical=[{canon}] evidence suggests '{finding.defect}' (confidence={finding.confidence:.2f}) "
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
            set_warn_if_needed()

        # Merge moisture findings into a single blocking issue per report (reduce noise)
        if moisture_findings:
            # severity = max severity observed (CRITICAL > HIGH > MED > LOW)
            sev_rank = {"LOW": 1, "MED": 2, "HIGH": 3, "CRITICAL": 4}
            sev = max((f.severity for _, f in moisture_findings), key=lambda s: sev_rank.get(s, 0))

            lines = []
            for page_num, finding in moisture_findings:
                lines.append(
                    f"- Page {page_num}: {finding.defect} (severity={finding.severity} confidence={finding.confidence:.2f}) notes={finding.notes}"
                )

            desc = (
                "Best practice risk (PAGE_TEXT_OR_TABLE): canonical=[DAMPNESS_MOULD_ALGAE]. "
                "Moisture readings / notes indicate elevated substrate moisture risk:\n" + "\n".join(lines)
            )

            issue_id = self.issue_repo.insert(
                report_id=report_id,
                issue_type="BEST_PRACTICE_RISK",
                category="Moisture risk (reported)",
                description=desc,
                severity=sev,
                is_blocking=True,
            )
            created_issue_ids.append(issue_id)
            set_warn_if_needed()

        # Record non-blocking photo observations (Bucket A)
        for page_num, finding in observations:
            sev = finding.severity
            canon = ",".join(getattr(finding, "canonical_defects", []) or [])
            desc = (
                f"Visual observation (PHOTO): canonical=[{canon}] '{finding.defect}' (confidence={finding.confidence:.2f}). "
                f"Page {page_num}. Notes: {finding.notes}"
            )
            self.issue_repo.insert(
                report_id=report_id,
                issue_type="INFO",
                category="Visual observation",
                description=desc,
                severity=sev,
                is_blocking=False,
            )

        return {
            "report_id": report_id,
            "vision_json": str(vision_path),
            "omission_issues_created": created_issue_ids,
            "overall_result_set": "WARN" if created_issue_ids else None,
        }
