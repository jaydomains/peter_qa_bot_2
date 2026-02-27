from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ImageAuditError(RuntimeError):
    pass


@dataclass(frozen=True)
class PageAudit:
    pdf_page_number: int
    photo_count_estimate: int
    has_table_or_form: bool
    has_labels_or_callouts: bool
    notes: str


def _b64_data_url_png(path: Path) -> str:
    b = Path(path).read_bytes()
    return "data:image/png;base64," + base64.b64encode(b).decode("ascii")


def audit_page_image(*, api_key: str, model: str, page_number: int, image_path: Path) -> PageAudit:
    if not api_key:
        raise ImageAuditError("OPENAI_API_KEY not set")

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "pdf_page_number",
            "photo_count_estimate",
            "has_table_or_form",
            "has_labels_or_callouts",
            "notes",
        ],
        "properties": {
            "pdf_page_number": {"type": "integer", "minimum": 1},
            "photo_count_estimate": {"type": "integer", "minimum": 0},
            "has_table_or_form": {"type": "boolean"},
            "has_labels_or_callouts": {"type": "boolean"},
            "notes": {"type": "string"},
        },
    }

    prompt = (
        "You are auditing a QA report page image. Do NOT infer defects. "
        "Only describe page structure. Count the number of distinct photographs visible on the page (estimate). "
        "Also indicate whether there is a table/form and whether there are labels/callouts on/near photos. "
        "Return STRICT JSON only."
    )

    body: dict[str, Any] = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": _b64_data_url_png(image_path)},
                    {"type": "input_text", "text": f"PDF page number: {page_number}"},
                ],
            }
        ],
        "text": {
            "format": {"type": "json_schema", "name": "page_audit", "schema": schema, "strict": True}
        },
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        details = ""
        try:
            details = e.read().decode("utf-8", errors="replace")
        except Exception:
            details = ""
        raise ImageAuditError(f"Image audit failed: HTTP {e.code} {e.reason} {details}".strip()) from e

    data = json.loads(raw)

    out_json = None
    out_text = None
    for item in data.get("output", []) or []:
        for c in item.get("content", []) or []:
            if c.get("type") == "output_json" and "json" in c:
                out_json = c.get("json")
                break
            if c.get("type") in ("output_text", "text") and "text" in c:
                out_text = c.get("text")
        if out_json is not None:
            break

    if out_json is None and out_text:
        out_json = json.loads(out_text)

    if out_json is None:
        raise ImageAuditError("No JSON output found")

    return PageAudit(
        pdf_page_number=int(out_json["pdf_page_number"]),
        photo_count_estimate=int(out_json["photo_count_estimate"]),
        has_table_or_form=bool(out_json["has_table_or_form"]),
        has_labels_or_callouts=bool(out_json["has_labels_or_callouts"]),
        notes=str(out_json["notes"]),
    )
