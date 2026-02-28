from __future__ import annotations

import sqlite3

from peter.config.settings import Settings


import json
import re
import shutil
from pathlib import Path

from peter.db.repositories.site_repo import SiteRepository
from peter.db.repositories.spec_repo import SpecRepository, SpecRow
from peter.domain.errors import ValidationError
from peter.parsing.pdf_text import extract_pdf_text, PdfTextExtractionError
from peter.storage.filestore import ensure_site_folders
from peter.storage.isolation import SiteSandbox
from peter.util.hashing import sha256_file
from peter.knowledge.checklist_builder import build_decorative_checklist


class SpecService:
    """Milestone M2: spec ingestion."""

    def __init__(self, conn: sqlite3.Connection, settings: Settings):
        self.conn = conn
        self.settings = settings
        self.site_repo = SiteRepository(conn)
        self.spec_repo = SpecRepository(conn)

    def _validate_version(self, version_label: str) -> str:
        v = (version_label or "").strip().upper().replace(" ", "")

        # Accept both REV* and V* forms and normalize to REV*.
        # Examples: V1 -> REV1, V01 -> REV01
        m_v = re.fullmatch(r"V([0-9A-Z]{1,8})", v)
        if m_v:
            return "REV" + m_v.group(1)

        # Accept REV01, REV1, REVA, etc
        if re.fullmatch(r"REV[0-9A-Z]{1,8}", v):
            return v

        raise ValidationError("version_label must look like V1 / REV01 / REV1 / REVA")

    def ingest_spec(self, *, site_code: str, version_label: str, file_path: Path) -> SpecRow:
        site_code = (site_code or "").strip().upper()
        vlabel = self._validate_version(version_label)
        path = Path(file_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValidationError(f"Spec file not found: {path}")
        if path.suffix.lower() != ".pdf":
            raise ValidationError("Spec must be a PDF")

        site = self.site_repo.get_by_code(site_code)
        if not site:
            raise ValidationError(f"Unknown site_code: {site_code}")

        sandbox = ensure_site_folders(self.settings, folder_name=site.folder_name)

        sha = sha256_file(path)
        existing = self.spec_repo.get_by_site_sha(site.id, sha)
        if existing:
            # idempotent: ensure active spec points at this if requested
            return existing

        # Store PDF under 01_spec with enforced naming
        safe_filename = f"{site.site_code}__SPEC__{vlabel}__{sha[:12]}.pdf"
        stored_pdf = sandbox.build_path("01_spec", safe_filename)
        shutil.copy2(path, stored_pdf)

        # Try to extract text (may fail due to missing deps)
        extracted_text_rel: str | None = None
        checklist_rel: str | None = None
        spec_text: str = ""
        try:
            spec_text = extract_pdf_text(stored_pdf)
            extracted_name = f"{site.site_code}__SPEC__{vlabel}__{sha[:12]}.txt"
            extracted_path = sandbox.build_path("00_admin", extracted_name)
            extracted_path.write_text(spec_text, encoding="utf-8")
            extracted_text_rel = str(extracted_path.relative_to(self.settings.QA_ROOT))

            checklist = build_decorative_checklist(spec_text)
            checklist_name = f"{site.site_code}__CHECKLIST__{vlabel}__{sha[:12]}.json"
            checklist_path = sandbox.build_path("00_admin", checklist_name)
            checklist_path.write_text(json.dumps(checklist, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            checklist_rel = str(checklist_path.relative_to(self.settings.QA_ROOT))
        except PdfTextExtractionError:
            # Spec still ingested, but without extracted text/checklist.
            pass

        # Activate this spec: deactivate all others first (site-scoped)
        self.spec_repo.deactivate_all_for_site(site.id)

        stored_rel = str(stored_pdf.relative_to(self.settings.QA_ROOT))
        spec = self.spec_repo.insert(
            site_id=site.id,
            version_label=vlabel,
            filename=safe_filename,
            sha256=sha,
            stored_path=stored_rel,
            extracted_text_path=extracted_text_rel,
            checklist_json_path=checklist_rel,
            is_active=True,
        )
        self.spec_repo.set_site_active_spec(site.id, spec.id)
        return spec
