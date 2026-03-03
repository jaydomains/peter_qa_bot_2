from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

BASE_DIR = os.environ.get(
    "PETER_TELEGRAM_STATE_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/telegram_state")),
)
os.makedirs(BASE_DIR, exist_ok=True)


@dataclass
class ConversationState:
    chat_id: int
    site_code: Optional[str] = None
    site_name: Optional[str] = None
    address: Optional[str] = None
    spec_version: Optional[str] = None
    report_code: Optional[str] = None
    history: list[Dict[str, str]] = field(default_factory=list)
    pending_action: Optional[str] = None
    pending_step: Optional[str] = None
    pending_data: Dict[str, str] = field(default_factory=dict)

    def _path(self) -> str:
        return os.path.join(BASE_DIR, f"{self.chat_id}.json")

    def to_dict(self, redact_history: bool = False) -> Dict[str, Any]:
        data = asdict(self)
        if redact_history:
            data["history"] = [
                {"role": item.get("role", ""), "content": "…"}
                for item in (self.history or [])
            ]
        return data

    def save(self) -> None:
        path = self._path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(asdict(self), fh, indent=2, ensure_ascii=False)
        os.replace(tmp, path)

    @classmethod
    def load(cls, chat_id: int) -> "ConversationState":
        path = os.path.join(BASE_DIR, f"{chat_id}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                return cls(**json.load(fh))
        return cls(chat_id=int(chat_id))

    def reset(self) -> None:
        self.site_code = None
        self.site_name = None
        self.address = None
        self.spec_version = None
        self.report_code = None
        self.history = []
        self.pending_action = None
        self.pending_step = None
        self.pending_data = {}
        self.save()

    def append_history(self, role: str, content: str) -> None:
        self.history.append({"role": str(role), "content": str(content)})
        self.history = self.history[-20:]
        self.save()
