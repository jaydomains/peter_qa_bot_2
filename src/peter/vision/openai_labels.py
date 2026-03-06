from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import urllib.request
import urllib.error


class LabelsVisionError(RuntimeError):
    pass


@dataclass(frozen=True)
class LabelProduct:
    page_number: int
    raw_text: str
    product_code: str | None
    brand: str | None
    confidence: float
    notes: str


def _post_responses(*, api_key: str, model: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
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
        raise LabelsVisionError(f"OpenAI error HTTP {e.code}: {details}".strip())
    except Exception as e:
        raise LabelsVisionError(str(e))
    return json.loads(raw)


def extract_label_products(*, api_key: str, model: str, page_number: int, image_path: Path) -> list[LabelProduct]:
    """A label-focused vision pass.

    Use on pages that likely contain product containers/labels.
    """

    b = Path(image_path).read_bytes()
    img_b64 = base64.b64encode(b).decode("ascii")

    # Correct MIME for the data URL (pages are usually PNG renders)
    suf = str(Path(image_path).suffix or "").lower()
    mime = "image/png" if suf == ".png" else "image/jpeg"

    prompt = (
        "You are extracting paint product identifiers AND traceability sticker text from images. "
        "Look for paint drums/buckets/containers and any barcode/lot/batch stickers on lids/labels. "
        "Return a JSON array of observed products/containers. "
        "For each item include raw_text (include any visible batch/lot/barcode printed text if readable), "
        "optional product_code (e.g. PP700/PU800) if clearly visible, optional brand, confidence, notes. "
        "Do not invent codes or numbers. If nothing is visible, return an empty array."
    )

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["products"],
        "properties": {
            "products": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["raw_text", "product_code", "brand", "confidence", "notes"],
                    "properties": {
                        "raw_text": {"type": "string"},
                        "product_code": {"type": ["string", "null"]},
                        "brand": {"type": ["string", "null"]},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "notes": {"type": "string"},
                    },
                },
            }
        }
    }

    payload: dict[str, Any] = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": f"data:{mime};base64,{img_b64}"},
                ],
            }
        ],
        "temperature": float(os.getenv("PETER_LABELS_TEMPERATURE", "0.0")),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "label_products",
                "schema": schema,
                "strict": True,
            }
        },
    }

    data = _post_responses(api_key=api_key, model=model, payload=payload)

    out_json = None
    out_text = data.get("output_text")
    if not out_text:
        # fallback scan
        for item in data.get("output", []) or []:
            for c in item.get("content", []) or []:
                if c.get("type") == "output_json" and "json" in c:
                    out_json = c.get("json")
                    break
                if c.get("type") == "output_text" and c.get("text"):
                    out_text = c["text"]
            if out_json is not None:
                break

    if out_json is None and not out_text:
        return []

    obj = None
    if out_json is not None:
        obj = out_json
    else:
        try:
            obj = json.loads(out_text)
        except Exception:
            return []

    arr = obj.get("products") if isinstance(obj, dict) else None
    if not isinstance(arr, list):
        return []

    prods: list[LabelProduct] = []
    for it in arr or []:
        raw = str(it.get("raw_text") or "").strip()
        if not raw:
            continue
        prods.append(
            LabelProduct(
                page_number=page_number,
                raw_text=raw,
                product_code=(str(it.get("product_code")).strip().upper().replace(" ", "") if it.get("product_code") else None),
                brand=(str(it.get("brand")).strip().upper() if it.get("brand") else None),
                confidence=float(it.get("confidence") or 0.0),
                notes=str(it.get("notes") or ""),
            )
        )

    return prods
