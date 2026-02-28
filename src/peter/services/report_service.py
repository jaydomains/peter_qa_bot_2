from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


import json
import logging
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


log = logging.getLogger("peter.report_service")


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
        """Normalize + validate a report/inspection reference.

        Historically we used codes like R01 / R12.
        Real-world inspection references are often purely numeric (e.g. "002").

        Accept:
          - R01, r01
          - 01
          - 002
        Normalize:
          - Uppercase
          - Remove spaces
          - If numeric only, keep as-is (zero-padded)
        """

        rc = (report_code or "").strip().upper().replace(" ", "")
        if re.fullmatch(r"R\d{2,3}", rc):
            return rc
        if re.fullmatch(r"\d{2,3}", rc):
            return rc
        raise ValidationError("report_code must look like R01 / R12 / 002")

    def _template_extract_site_and_ref(self, text: str) -> tuple[str | None, str | None]:
        """Best-effort extraction of site code + inspection reference from report text.

        This is used to validate that a dropped/emailed PDF matches the expected
        site/ref (helps catch misfiled attachments).

        Expected template labels (case-insensitive):
          - SITE CODE:
          - INSPECTION REFERENCE:  (or REPORT # / REPORT NO)

        Returns: (site_code, ref) or (None, None) if not found.
        """

        raw = (text or "")
        if not raw.strip():
            return None, None

        # Normalize whitespace to make regex less brittle.
        norm = re.sub(r"[\t\r]+", " ", raw)

        m_site = re.search(r"(?im)^\s*SITE\s*CODE\s*:\s*([A-Z0-9_-]{3,20})\b", norm)
        site = m_site.group(1).strip().upper() if m_site else None

        # Try multiple labels for the report identifier.
        m_ref = re.search(
            r"(?im)^\s*(?:INSPECTION\s*REFERENCE|REPORT\s*#|REPORT\s*NO\.?|REPORT\s*NUMBER)\s*:\s*([^\n]+)",
            norm,
        )
        ref = None
        if m_ref:
            ref_raw = m_ref.group(1).strip().upper()
            # Common format: "PRSVNQA - 002" → keep just the numeric part for matching.
            m_num = re.search(r"\b(\d{2,3})\b", ref_raw)
            ref = m_num.group(1) if m_num else ref_raw.replace(" ", "")

        return site, ref

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
            # Optional template validation (warn by default; strict if env is set).
            mode = os.getenv("PETER_VALIDATE_REPORT_TEMPLATE", "").strip().lower()
            if mode in ("1", "true", "warn", "strict"):
                extracted_site, extracted_ref = self._template_extract_site_and_ref(text)

                mismatches: list[str] = []
                if extracted_site and extracted_site != site.site_code:
                    mismatches.append(f"site_code_in_pdf={extracted_site} expected={site.site_code}")
                if extracted_ref and extracted_ref != rc:
                    mismatches.append(f"ref_in_pdf={extracted_ref} expected={rc}")

                if mismatches:
                    msg = "Report template fields mismatch: " + "; ".join(mismatches)
                    if mode == "strict":
                        raise ValidationError(msg)
                    log.warning(msg)

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

    def _load_report_text(self, *, site, rc: str, sha: str) -> str:
        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

        # Prefer cached extracted text file
        txt_files = list(sandbox.build_path("00_admin").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.txt"))
        if txt_files:
            return txt_files[0].read_text(encoding="utf-8", errors="replace")

        # fall back: extract from stored PDF
        pdf_files = list(sandbox.build_path("02_reports").glob(f"{site.site_code}__REPORT__{rc}__{sha[:12]}*.pdf"))
        if not pdf_files:
            raise ValidationError("Could not locate stored PDF for report")
        return extract_pdf_text(pdf_files[0])

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
        raw_text = self._load_report_text(site=site, rc=rc, sha=sha)

        from peter.analysis.text_clean import clean_extracted_text
        from peter.analysis.summary_flags import build_flags, extract_section_excerpt

        clean = clean_extracted_text(raw_text)

        # Prefer narrative sections for evidence (reduce table noise).
        evidence_text = (
            extract_section_excerpt(clean, "Executive Summary", window=2500)
            or extract_section_excerpt(clean, "Concerns", window=1500)
            or clean
        )

        flags = build_flags(evidence_text)

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

    def triage_report_text(self, *, site_code: str, report_code: str, reset: bool = False) -> str:
        """Persist text-only flags into DB issues and set a report result.

        Policy (initial):
        - Create BEST_PRACTICE_RISK issues for detected flags.
        - Set report result to WARN if any issues created; otherwise PASS.
        - Never auto-FAIL from text-only triage.
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

        report_id = int(row["id"])
        sha = str(row["sha256"])

        if reset:
            self.issue_repo.delete_for_report(report_id)

        from peter.analysis.text_clean import clean_extracted_text
        from peter.analysis.summary_flags import build_flags

        raw_text = self._load_report_text(site=site, rc=rc, sha=sha)
        clean = clean_extracted_text(raw_text)

        evidence_text = (
            extract_section_excerpt(clean, "Executive Summary", window=2500)
            or extract_section_excerpt(clean, "Concerns", window=1500)
            or clean
        )

        flags = build_flags(evidence_text)

        # Map deterministic flags to issue severity/blocking.
        # Keep it conservative; tune later.
        sev_map = {
            "MOISTURE_FAIL": ("HIGH", True),
            "MOISTURE_HIGH": ("MED", True),
            "DELAMINATION": ("HIGH", True),
            "CRACKING": ("MED", False),
            "BLISTERING": ("MED", False),
            "PEELING_FLAKING": ("MED", False),
        }

        created = 0
        for fl in flags:
            severity, blocking = sev_map.get(fl.key, ("LOW", False))
            desc = fl.title
            if fl.evidence:
                desc += "\n\nEvidence:\n" + "\n".join(f"- {e}" for e in fl.evidence[:5])
            self.issue_repo.insert(
                report_id=report_id,
                issue_type="BEST_PRACTICE_RISK",
                category=fl.title,
                description=desc,
                severity=severity,
                is_blocking=bool(blocking),
            )
            created += 1

        # Result policy: WARN if any issues created, else PASS.
        result = "WARN" if created else "PASS"
        self.report_repo.update_result_and_paths(report_id=report_id, result=result, review_md_path=None, review_json_path=None)

        return f"OK triage report_id={report_id} site={site.site_code} report={rc} sha={sha} issues_created={created} result={result}"

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
