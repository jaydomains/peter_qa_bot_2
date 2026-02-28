from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TdsRecord:
    vendor: str
    product_key: str
    pdf_path: Path
    txt_path: Path | None
    source_url: str


def library_root(qa_root: Path) -> Path:
    return (Path(qa_root) / "LIBRARY" / "TDS").resolve()


def _host_matches(host: str, pattern: str) -> bool:
    host = (host or "").lower().strip(".")
    pattern = (pattern or "").lower().strip(".")
    if not host or not pattern:
        return False
    if pattern.startswith("*."):
        return host == pattern[2:] or host.endswith("." + pattern[2:])
    return host == pattern or host.endswith("." + pattern)


def is_allowed_tds_url(url: str) -> bool:
    raw = os.getenv("PETER_TDS_ALLOWLIST", "").strip()
    if not raw:
        return False
    pats = [p.strip().lower() for p in raw.split(",") if p.strip()]
    try:
        u = urllib.parse.urlparse(url)
        if u.scheme not in ("http", "https"):
            return False
        host = (u.hostname or "").lower()
        if not host:
            return False
        return any(_host_matches(host, pat) for pat in pats)
    except Exception:
        return False


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _download_limited(url: str, *, max_bytes: int) -> bytes:
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "PETER-QA/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise RuntimeError(f"TDS download exceeded limit ({max_bytes} bytes)")
    return data


def _looks_like_pdf(data: bytes) -> bool:
    return (data or b"").startswith(b"%PDF-")


def store_tds_pdf(
    *,
    qa_root: Path,
    vendor: str,
    product_key: str,
    pdf_bytes: bytes,
    source_url: str,
) -> TdsRecord:
    v = (vendor or "UNKNOWN").strip().upper().replace(" ", "_")
    k = (product_key or "UNKNOWN").strip().upper().replace(" ", "_")

    root = library_root(Path(qa_root)) / v / k
    root.mkdir(parents=True, exist_ok=True)

    pdf_path = root / "tds.pdf"
    pdf_path.write_bytes(pdf_bytes)

    (root / "source_url.txt").write_text(source_url + "\n", encoding="utf-8")
    (root / "fetched_at.txt").write_text(time.strftime("%Y-%m-%d %H:%M:%S") + "\n", encoding="utf-8")
    (root / "sha256.txt").write_text(_sha256_bytes(pdf_bytes) + "\n", encoding="utf-8")

    # Best-effort text extraction
    txt_path = None
    try:
        from peter.parsing.pdf_text import extract_pdf_text

        txt = extract_pdf_text(pdf_path)
        txt_path = root / "tds.txt"
        txt_path.write_text(txt, encoding="utf-8")
    except Exception:
        txt_path = None

    meta: dict[str, Any] = {
        "vendor": v,
        "product_key": k,
        "source_url": source_url,
    }
    (root / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    return TdsRecord(vendor=v, product_key=k, pdf_path=pdf_path, txt_path=txt_path, source_url=source_url)


def fetch_and_store_tds(
    *,
    qa_root: Path,
    vendor: str,
    product_key: str,
    url: str,
) -> TdsRecord:
    if not is_allowed_tds_url(url):
        raise RuntimeError(f"TDS URL not allowed: {url}")

    max_mb = int(os.getenv("PETER_TDS_MAX_MB", "40"))
    max_bytes = max_mb * 1024 * 1024

    b = _download_limited(url, max_bytes=max_bytes)
    if not _looks_like_pdf(b):
        raise RuntimeError("Downloaded content is not a PDF")

    return store_tds_pdf(qa_root=qa_root, vendor=vendor, product_key=product_key, pdf_bytes=b, source_url=url)
