from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from peter.parsing.pdf_text import extract_pdf_text, has_meaningful_text


@dataclass(frozen=True)
class ReportIdentity:
    site_code: str
    report_no: str  # 3-digit

    # Stable header identity (text-extractable in Fieldwire reports)
    site_name_raw: str | None = None
    site_name_display: str | None = None
    address: str | None = None

    supplier_client: str | None = None
    contractor_on_site: str | None = None

    @property
    def display_ref(self) -> str:
        return f"{self.site_code} - {self.report_no}"


def _z3(s: str) -> str:
    s2 = re.sub(r"\D+", "", s or "")
    return s2.zfill(3) if s2 else ""


def infer_from_pdf_bytes(pdf_bytes: bytes) -> ReportIdentity | None:
    """Infer site identity from report PDF.

    Returns:
      - site_code + report_no (3-digit)
      - plus best-effort extraction of:
        - site_name_raw / site_name_display (from first header line)
        - address (from second header line)
        - supplier_client, contractor_on_site (from labeled fields)

    All text is assumed extractable (Fieldwire template).
    """

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "report.pdf"
        p.write_bytes(pdf_bytes)
        text = extract_pdf_text(p)

    if not has_meaningful_text(text):
        return None

    norm = re.sub(r"[\t\r]+", " ", text)

    # Prefer explicit report number label
    m_rn = re.search(r"(?im)REPORT\s*#\s*:\s*([^\n]+)", norm)
    report_no = _z3(m_rn.group(1)) if m_rn else ""

    # Try to find an explicit inspection reference line, but don't require start-of-line.
    m_ref = re.search(r"(?im)INSPECTION\s*REFERENCE\s*:\s*([^\n]+)", norm)
    if not m_ref:
        m_ref = re.search(r"(?im)\bNAME\b\s*:?\s*([^\n]+)", norm)

    site_code = ""
    ref_text = m_ref.group(1).strip().upper() if m_ref else ""

    # Robust fallback: find patterns like PRSVNQA - 006 anywhere.
    m_pair = re.search(r"\b([A-Z]{3,12}[A-Z0-9_-]{0,10})\s*-\s*(\d{1,3})\b", norm.upper())
    if m_pair:
        site_code = m_pair.group(1).strip().upper()
        if not report_no:
            report_no = _z3(m_pair.group(2))

    # If we have a reference line, use it to refine.
    if ref_text:
        m_sc = re.search(r"\b([A-Z]{3,12}[A-Z0-9_-]{0,10})\b", ref_text)
        if m_sc:
            site_code = site_code or m_sc.group(1)
        if not report_no:
            m_num = re.search(r"\b(\d{1,3})\b", ref_text)
            if m_num:
                report_no = _z3(m_num.group(1))

    if not (site_code and report_no):
        return None

    # --- Extract stable header fields ---
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    header1 = lines[0] if lines else ""
    header2 = lines[1] if len(lines) > 1 else ""

    site_name_raw = header1 or None
    address = header2 or None

    # Header line format examples:
    #   "PR - NEWINBOSCH ILEX —"
    #   "PLA - De Drift Shopping Centre —"
    site_name_display = None
    m_name = re.search(r"\b[A-Z]{2,4}\s*-\s*(.+?)\s*[—-]\s*$", header1)
    if m_name:
        site_name_display = m_name.group(1).strip()

    # Supplier/contractor (best-effort)
    supplier = None
    contractor = None
    m_sup = re.search(r"(?im)^\s*Supplier\s*/\s*Client\s*:\s*(.+)$", norm)
    if m_sup:
        supplier = m_sup.group(1).strip()
    m_con = re.search(r"(?im)^\s*Contractor\s+On\s+Site\s*:\s*(.+)$", norm)
    if m_con:
        contractor = m_con.group(1).strip()

    return ReportIdentity(
        site_code=site_code,
        report_no=report_no,
        site_name_raw=site_name_raw,
        site_name_display=site_name_display,
        address=address,
        supplier_client=supplier,
        contractor_on_site=contractor,
    )
