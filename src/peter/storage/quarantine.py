from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from peter.storage.isolation import SiteSandbox


@dataclass(frozen=True)
class QuarantinedFile:
    stored_path: Path
    reason_path: Path


def quarantine_bytes(
    *,
    sandbox: SiteSandbox,
    filename: str,
    data: bytes,
    reason: str,
    prefix: str = "",
) -> QuarantinedFile:
    qdir = sandbox.ensure_dir("99_quarantine")
    fname = f"{prefix}{filename}" if prefix else filename
    stored = sandbox.build_path("99_quarantine", fname)
    stored.write_bytes(data)
    reason_path = sandbox.build_path("99_quarantine", fname + ".reason.txt")
    reason_path.write_text(reason.strip() + "\n", encoding="utf-8")
    return QuarantinedFile(stored_path=stored, reason_path=reason_path)
