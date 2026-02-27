from __future__ import annotations

from pathlib import Path


class PdfTextExtractionError(RuntimeError):
    pass


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from a PDF.

    NOTE: On minimal Linux images, we may not have poppler (pdftotext) or a
    Python PDF library available. This function currently requires a dependency
    to be installed.

    Recommended install options:
    - apt-get install poppler-utils  (provides pdftotext)
    - OR add a Python dependency like pypdf (requires pip/venv)
    """

    # Dependency-free PDF text extraction is not realistically reliable.
    # We fail loudly with an actionable message.
    raise PdfTextExtractionError(
        "PDF text extraction is not available in this environment. "
        "Install 'poppler-utils' (pdftotext) or add a Python PDF library (e.g. pypdf)."
    )
