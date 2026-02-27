from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class VisionError(RuntimeError):
    pass


@dataclass(frozen=True)
class VisionFinding:
    defect: str
    canonical_defects: list[str]
    confidence: float  # 0..1
    severity: str  # LOW|MED|HIGH|CRITICAL
    notes: str


@dataclass(frozen=True)
class VisionPageResult:
    page_number: int
    findings: list[VisionFinding]
    summary: str


def _b64_data_url_png(path: Path) -> str:
    b = Path(path).read_bytes()
    return "data:image/png;base64," + base64.b64encode(b).decode("ascii")


def analyze_page_image(
    *,
    api_key: str,
    model: str,
    page_number: int,
    image_path: Path,
) -> VisionPageResult:
    """Analyze a rendered page image using OpenAI Responses API.

    Returns structured findings (JSON).
    """

    if not api_key:
        raise VisionError("OPENAI_API_KEY not set")

    canonical_enum = [
        "CRACKING",
        "PEELING_FLAKING",
        "BLISTERING",
        "EFFLORESCENCE",
        "DAMPNESS_MOULD_ALGAE",
        "DELAMINATION",
        "RUST_STAINING",
        "POOR_COVERAGE_EXPOSED_SUBSTRATE",
        "UNEVEN_SHEEN",
        "TEXTURE_INCONSISTENCY",
    ]

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["page_number", "summary", "findings"],
        "properties": {
            "page_number": {"type": "integer", "minimum": 1},
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["defect", "canonical_defects", "confidence", "severity", "notes"],
                    "properties": {
                        "defect": {"type": "string"},
                        "canonical_defects": {
                            "type": "array",
                            "items": {"type": "string", "enum": canonical_enum},
                        },
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "severity": {"type": "string", "enum": ["LOW", "MED", "HIGH", "CRITICAL"]},
                        "notes": {"type": "string"},
                    },
                },
            },
        },
    }

    prompt = (
        "You are PETER, a QA assistant for decorative architectural coatings. "
        "Perform a meticulous visual inspection of the provided report page image. "
        "Do NOT skim. Enumerate every visible coating/substrate defect or risk indicator you can see "
        "in photos and in any page visuals (e.g., cracking, flaking, blistering, peeling, efflorescence, "
        "uneven sheen, lap marks, thin coverage, staining, dampness signs, delamination). "
        "If there are no relevant visual defects, return an empty findings array. "
        "Return STRICT JSON that conforms to the provided schema."
    )

    body: dict[str, Any] = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": _b64_data_url_png(image_path)},
                    {"type": "input_text", "text": f"Page number: {page_number}"},
                ],
            }
        ],
        # New Responses API: formatting moved under text.format
        "text": {
            "format": {
                "type": "json_schema",
                "name": "vision_page_result",
                "schema": schema,
                "strict": True,
            }
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
        raise VisionError(f"Vision request failed: HTTP {e.code} {e.reason} {details}".strip()) from e
    except Exception as e:
        raise VisionError(f"Vision request failed: {e}") from e

    data = json.loads(raw)

    # Responses API output parsing:
    # - Some deployments return content type=output_json with field json
    # - Others may return output_text with JSON string under text
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
        try:
            out_json = json.loads(out_text)
        except Exception as e:
            raise VisionError(f"Could not parse JSON output: {e}. Raw text: {out_text[:500]}") from e

    if out_json is None:
        raise VisionError(f"No JSON output found in response. Keys: {list(data.keys())}")

    findings = [VisionFinding(**f) for f in (out_json.get("findings") or [])]
    return VisionPageResult(page_number=int(out_json["page_number"]), findings=findings, summary=str(out_json["summary"]))
