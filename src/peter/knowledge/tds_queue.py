from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QueueItem:
    key: str
    path: Path
    payload: dict[str, Any]


def queue_root(data_dir: Path) -> Path:
    return (Path(data_dir) / "TDS_QUEUE").resolve()


def _safe_key(vendor: str, product_key: str) -> str:
    v = (vendor or "UNKNOWN").strip().upper().replace(" ", "_")
    k = (product_key or "UNKNOWN").strip().upper().replace(" ", "_")
    return f"{v}__{k}"


def enqueue(*, data_dir: Path, vendor: str, product_key: str, hints: dict[str, Any] | None = None) -> QueueItem:
    root = queue_root(Path(data_dir))
    root.mkdir(parents=True, exist_ok=True)

    key = _safe_key(vendor, product_key)
    path = root / f"{key}.json"

    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        return QueueItem(key=key, path=path, payload=payload)

    payload: dict[str, Any] = {
        "vendor": (vendor or "UNKNOWN").strip().upper(),
        "product_key": (product_key or "UNKNOWN").strip().upper(),
        "status": "PENDING",
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "hints": hints or {},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return QueueItem(key=key, path=path, payload=payload)


def list_items(*, data_dir: Path, status: str | None = None, limit: int = 50) -> list[QueueItem]:
    root = queue_root(Path(data_dir))
    if not root.exists():
        return []
    out: list[QueueItem] = []
    for p in sorted(root.glob("*.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if status and str(payload.get("status")) != status:
            continue
        out.append(QueueItem(key=p.stem, path=p, payload=payload))
        if len(out) >= limit:
            break
    return out


def update(*, item: QueueItem, patch: dict[str, Any]) -> QueueItem:
    payload = dict(item.payload)
    payload.update(patch)
    item.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return QueueItem(key=item.key, path=item.path, payload=payload)
