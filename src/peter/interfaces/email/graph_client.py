from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


class GraphClientError(RuntimeError):
    pass


@dataclass
class GraphClient:
    token: str
    base_url: str = "https://graph.microsoft.com/v1.0"

    def _req(self, method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
        except Exception as e:
            raise GraphClientError(f"Graph request failed: {e}") from e

        if not raw:
            return {}
        return json.loads(raw)

    def list_unread_messages(self, *, mailbox: str, top: int = 10) -> list[dict[str, Any]]:
        # Inbox unread only
        qs = urllib.parse.urlencode(
            {
                "$filter": "isRead eq false",
                "$top": str(top),
                "$orderby": "receivedDateTime desc",
            }
        )
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/mailFolders/Inbox/messages?{qs}"
        out = self._req("GET", url)
        return out.get("value", []) or []

    def list_attachments(self, *, mailbox: str, message_id: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/messages/{message_id}/attachments"
        out = self._req("GET", url)
        return out.get("value", []) or []

    def get_attachment(self, *, mailbox: str, message_id: str, attachment_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/messages/{message_id}/attachments/{attachment_id}"
        return self._req("GET", url)

    def create_reply_draft(self, *, mailbox: str, message_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/messages/{message_id}/createReply"
        return self._req("POST", url, {})

    def update_message(self, *, mailbox: str, message_id: str, payload: dict[str, Any]) -> None:
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/messages/{message_id}"
        self._req("PATCH", url, payload)

    def send_message(self, *, mailbox: str, message_id: str) -> None:
        url = f"{self.base_url}/users/{urllib.parse.quote(mailbox)}/messages/{message_id}/send"
        self._req("POST", url, {})

    def mark_read(self, *, mailbox: str, message_id: str) -> None:
        self.update_message(mailbox=mailbox, message_id=message_id, payload={"isRead": True})
