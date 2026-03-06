from __future__ import annotations

import re


def infer_material_pages_from_text(extracted_text: str) -> set[int]:
    """Infer which PDF pages belong to the 'Material on site' section.

    Robust approach:
    - Find the first page where the header 'Material on site' appears.
    - Then include subsequent pages until a likely new major section header appears,
      or until end-of-document.

    This avoids missing continuation pages where technicians include batch/label photos.
    """

    text = extracted_text or ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Build a rough page->lines map using Fieldwire page markers.
    # We commonly see:
    #   - "Page 1 of 23" (most common)
    #   - "pg. 1" (legacy)
    page_lines: dict[int, list[str]] = {}
    current_pg: int | None = None

    for ln in lines:
        m = re.search(r"\bpage\s+(\d{1,3})\s+of\s+\d{1,3}\b", ln, flags=re.I)
        if not m:
            m = re.search(r"\bpg\.\s*(\d{1,3})\b", ln, flags=re.I)
        if m:
            try:
                current_pg = int(m.group(1))
            except Exception:
                # keep previous
                current_pg = current_pg
        if current_pg is not None:
            page_lines.setdefault(current_pg, []).append(ln)

    if not page_lines:
        return set()

    max_pg = max(page_lines.keys())

    # Find start page (allow plural/synonyms)
    start = None
    start_pat = re.compile(
        r"\b(materials?\s+on\s+site|material\s+on\s+site|products?\s+on\s+site|materials?\s+used)\b",
        flags=re.I,
    )
    for pg in range(1, max_pg + 1):
        chunk = "\n".join(page_lines.get(pg, []))
        if start_pat.search(chunk):
            start = pg
            break

    if not start:
        return set()

    # Stop headers (best-effort). If none found, include to end.
    stop_headers = [
        "executive summary",
        "concerns",
        "test summary",
        "moisture",
        "dft",
        "site overview",
        "progress overview",
        "reference panel",
        "appendix",
    ]

    pages: set[int] = set()
    pages.add(start)

    for pg in range(start + 1, max_pg + 1):
        chunk = "\n".join(page_lines.get(pg, [])).lower()
        # If the material header repeats, keep going.
        if start_pat.search(chunk):
            pages.add(pg)
            continue

        # Heuristic: stop if another major section header appears.
        if any(re.search(rf"\b{re.escape(h)}\b", chunk) for h in stop_headers):
            break

        pages.add(pg)

    return pages
