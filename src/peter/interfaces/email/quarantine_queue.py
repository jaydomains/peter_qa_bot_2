from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QuarantineItem:
    qid: str
    status: str  # PENDING_CONFIRMATION|CONFIRMED|REJECTED|PENDING_API|PROCESSED
    created_at: str
    meta_path: Path
    file_path: Path
    meta: dict[str, Any]


def new_quarantine_id() -> str:
    # Timestamped for human handling + short random suffix
    ts = time.strftime("%Y%m%d-%H%M%S")
    # 16 bits of entropy is enough to avoid collisions in practice here
    import secrets

    suf = secrets.token_hex(2)
    return f"Q-{ts}-{suf}"


def quarantine_root(data_dir: Path) -> Path:
    return (Path(data_dir) / "QUARANTINE_QUEUE").resolve()


def save_quarantine_item(*, data_dir: Path, filename: str, content: bytes, meta: dict[str, Any]) -> QuarantineItem:
    root = quarantine_root(Path(data_dir))
    root.mkdir(parents=True, exist_ok=True)

    qid = new_quarantine_id()
    item_dir = root / qid
    item_dir.mkdir(parents=True, exist_ok=True)

    safe_name = (filename or "attachment.bin").replace("/", "_")
    file_path = item_dir / safe_name
    file_path.write_bytes(content)

    meta_path = item_dir / "meta.json"
    meta2 = dict(meta)
    meta2.setdefault("qid", qid)
    meta2.setdefault("status", "PENDING_CONFIRMATION")
    meta2.setdefault("created_at", time.strftime("%Y-%m-%d %H:%M:%S"))
    meta_path.write_text(json.dumps(meta2, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return QuarantineItem(
        qid=qid,
        status=str(meta2["status"]),
        created_at=str(meta2["created_at"]),
        meta_path=meta_path,
        file_path=file_path,
        meta=meta2,
    )


def load_quarantine_item(*, data_dir: Path, qid: str) -> QuarantineItem:
    item_dir = quarantine_root(Path(data_dir)) / qid
    meta_path = item_dir / "meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"Quarantine item not found: {qid}")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    # Find the first non-meta file
    file_path = None
    for p in sorted(item_dir.iterdir()):
        if p.name == "meta.json":
            continue
        if p.is_file():
            file_path = p
            break
    if file_path is None:
        raise FileNotFoundError(f"No file present for quarantine item: {qid}")

    return QuarantineItem(
        qid=str(meta.get("qid") or qid),
        status=str(meta.get("status") or ""),
        created_at=str(meta.get("created_at") or ""),
        meta_path=meta_path,
        file_path=file_path,
        meta=dict(meta),
    )


def update_status(*, item: QuarantineItem, status: str, extra: dict[str, Any] | None = None) -> None:
    meta = dict(item.meta)
    meta["status"] = status
    if extra:
        meta.update(extra)
    item.meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def list_items(*, data_dir: Path, status: str, limit: int = 20) -> list[QuarantineItem]:
    root = quarantine_root(Path(data_dir))
    if not root.exists():
        return []

    out: list[QuarantineItem] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        meta_path = d / "meta.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(meta.get("status") or "").strip().upper() != status.strip().upper():
            continue
        try:
            item = load_quarantine_item(data_dir=Path(data_dir), qid=d.name)
            out.append(item)
        except Exception:
            continue
        if len(out) >= max(1, int(limit)):
            break

    return out
