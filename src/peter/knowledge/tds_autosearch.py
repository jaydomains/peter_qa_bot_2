from __future__ import annotations

import re
import urllib.parse
import urllib.request


def _extract_pdf_links(html: str, *, base_url: str) -> list[str]:
    # naive href finder
    hrefs = re.findall(r"href=[\"']([^\"']+)[\"']", html, flags=re.I)
    out: list[str] = []
    for h in hrefs:
        if ".pdf" not in h.lower():
            continue
        out.append(urllib.parse.urljoin(base_url, h))
    # dedupe
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def autosearch_pdf_urls(*, vendor: str, product_key: str, domains: list[str]) -> list[str]:
    """Best-effort HTML search for PDF links on allowed domains.

    This is intentionally simple and may fail for JS-heavy sites.
    We try a few guessable search endpoints.
    """

    q = urllib.parse.quote_plus(f"{vendor} {product_key} data sheet pdf")

    candidates: list[str] = []
    for dom in domains:
        dom = dom.strip()
        if not dom:
            continue
        # Try common search URL patterns
        for path in [f"https://{dom}/?s={q}", f"https://{dom}/search?q={q}", f"https://{dom}/search?query={q}"]:
            try:
                req = urllib.request.Request(path, headers={"User-Agent": "PETER-QA/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    html = resp.read(800_000).decode("utf-8", errors="replace")
                candidates.extend(_extract_pdf_links(html, base_url=path))
            except Exception:
                continue

    # Prefer urls that contain the product key
    pk = product_key.strip().lower()
    scored = []
    for u in candidates:
        s = 0
        if pk and pk in u.lower():
            s += 5
        if "tds" in u.lower() or "datasheet" in u.lower() or "data-sheet" in u.lower():
            s += 2
        scored.append((s, u))

    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[str] = []
    seen: set[str] = set()
    for _, u in scored:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out[:10]
