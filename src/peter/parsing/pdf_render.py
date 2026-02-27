from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


class PdfRenderError(RuntimeError):
    pass


@dataclass(frozen=True)
class RenderedPages:
    page_paths: list[Path]
    dpi: int


def pdf_page_count(pdf_path: Path) -> int:
    pdf_path = Path(pdf_path).expanduser().resolve()
    exe = shutil.which("pdfinfo")
    if not exe:
        raise PdfRenderError("pdfinfo not found (install poppler)")

    try:
        r = subprocess.run([exe, str(pdf_path)], check=True, capture_output=True, text=True, timeout=60)
    except subprocess.CalledProcessError as e:
        raise PdfRenderError(f"pdfinfo failed: {e.stderr.strip()}") from e

    m = re.search(r"^Pages:\s+(\d+)\s*$", r.stdout, flags=re.MULTILINE)
    if not m:
        raise PdfRenderError("Could not parse page count from pdfinfo output")
    return int(m.group(1))


def render_pdf_pages(
    pdf_path: Path,
    *,
    out_dir: Path,
    prefix: str,
    dpi: int = 300,
    first_page: int = 1,
    last_page: int | None = None,
) -> RenderedPages:
    """Render PDF pages to PNG using pdftoppm.

    Output filenames: {prefix}-{page:02d}.png (pdftoppm style).
    """

    pdf_path = Path(pdf_path).expanduser().resolve()
    out_dir = Path(out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    exe = shutil.which("pdftoppm")
    if not exe:
        raise PdfRenderError("pdftoppm not found (install poppler)")

    if last_page is None:
        last_page = pdf_page_count(pdf_path)

    cmd = [
        exe,
        "-png",
        "-r",
        str(dpi),
        "-f",
        str(first_page),
        "-l",
        str(last_page),
        str(pdf_path),
        str(out_dir / prefix),
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
    except subprocess.CalledProcessError as e:
        raise PdfRenderError(f"pdftoppm failed: {e.stderr.strip()}") from e

    # Collect expected outputs
    page_paths: list[Path] = []
    for p in range(first_page, last_page + 1):
        # pdftoppm uses -1, -2 etc without padding
        candidate = out_dir / f"{prefix}-{p}.png"
        if candidate.exists():
            page_paths.append(candidate)
    if not page_paths:
        raise PdfRenderError("No pages rendered")

    return RenderedPages(page_paths=page_paths, dpi=dpi)
