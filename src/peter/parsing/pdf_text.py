from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path


class PdfTextExtractionError(RuntimeError):
    pass


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from a PDF using Poppler's pdftotext.

    Uses `-layout` to preserve tables where possible and `-nopgbrk` to avoid
    hard page breaks in the output.
    """

    pdf_path = Path(pdf_path).expanduser().resolve()
    if not pdf_path.exists():
        raise PdfTextExtractionError(f"PDF not found: {pdf_path}")

    exe = shutil.which("pdftotext")
    if not exe:
        raise PdfTextExtractionError("pdftotext not found (install poppler)")

    with tempfile.TemporaryDirectory() as td:
        out_txt = Path(td) / "out.txt"
        # pdftotext <PDF> <TXT>
        cmd = [exe, "-layout", "-nopgbrk", str(pdf_path), str(out_txt)]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
        except subprocess.CalledProcessError as e:
            raise PdfTextExtractionError(f"pdftotext failed: {e.stderr.strip()}") from e
        except subprocess.TimeoutExpired as e:
            raise PdfTextExtractionError("pdftotext timed out") from e

        return out_txt.read_text(encoding="utf-8", errors="replace")


def has_meaningful_text(text: str, *, min_chars: int = 300, min_alnum_ratio: float = 0.15) -> bool:
    t = (text or "").strip()
    if len(t) < min_chars:
        return False
    alnum = sum(1 for c in t if c.isalnum())
    ratio = alnum / max(1, len(t))
    return ratio >= min_alnum_ratio
