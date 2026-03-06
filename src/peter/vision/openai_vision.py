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
    evidence_basis: str  # PHOTO|PAGE_TEXT_OR_TABLE|LABEL_ONLY
    confidence: float  # 0..1
    severity: str  # LOW|MED|HIGH|CRITICAL
    notes: str


@dataclass(frozen=True)
class VisionObservedProduct:
    page_number: int
    raw_text: str
    product_code: str | None
    brand: str | None
    confidence: float
    notes: str


@dataclass(frozen=True)
class VisionPageResult:
    page_number: int
    findings: list[VisionFinding]
    observed_products: list[VisionObservedProduct]
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
        "required": ["page_number", "summary", "findings", "observed_products"],
        "properties": {
            "page_number": {"type": "integer", "minimum": 1},
            "summary": {"type": "string"},
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "defect",
                        "canonical_defects",
                        "evidence_basis",
                        "confidence",
                        "severity",
                        "notes",
                    ],
                    "properties": {
                        "defect": {"type": "string"},
                        "canonical_defects": {
                            "type": "array",
                            "items": {"type": "string", "enum": canonical_enum},
                        },
                        "evidence_basis": {
                            "type": "string",
                            "enum": ["PHOTO", "PAGE_TEXT_OR_TABLE", "LABEL_ONLY"],
                        },
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "severity": {"type": "string", "enum": ["LOW", "MED", "HIGH", "CRITICAL"]},
                        "notes": {"type": "string"},
                    },
                },
            },
            "observed_products": {
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
            },
        },
    }

    prompt = (
        "You are PETER, a QA assistant for decorative architectural coatings. "
        "Perform a meticulous inspection of the provided report page image. Do NOT skim. "
        "1) DEFECT/RISK FINDINGS: Enumerate each defect/risk indicator you can infer from: (a) the PHOTOS themselves, (b) any PAGE TEXT/TABLES, "
        "or (c) LABELS that indicate a concern. "
        "For each finding, you MUST: (1) assign one or more canonical_defects from the allowed enum; "
        "(2) set evidence_basis to exactly one of: PHOTO, PAGE_TEXT_OR_TABLE, LABEL_ONLY. "
        "Rules: if you rely on a table (e.g., moisture readings) use PAGE_TEXT_OR_TABLE. If you rely only on a label "
        "that mentions a defect but it is not visibly clear, use LABEL_ONLY. Only use PHOTO when the defect is visually observable. "
        "2) OBSERVED PRODUCTS: If you see paint drums/buckets/containers, read the label text and extract observed_products entries. "
        "For each observed product provide raw_text, optional product_code if clearly visible (e.g. PP950/PU800), optional brand, confidence and notes. "
        "Do NOT invent product codes. If no products are visible, return an empty observed_products array. "
        "Allowed canonical_defects: CRACKING, PEELING_FLAKING, BLISTERING, EFFLORESCENCE, DAMPNESS_MOULD_ALGAE, "
        "DELAMINATION, RUST_STAINING, POOR_COVERAGE_EXPOSED_SUBSTRATE, UNEVEN_SHEEN, TEXTURE_INCONSISTENCY. "
        "If none apply, return an empty findings array. Return STRICT JSON matching the schema."
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
    observed_products = [
        VisionObservedProduct(
            page_number=int(out_json["page_number"]),
            raw_text=str(p.get("raw_text") or "").strip(),
            product_code=(str(p.get("product_code")).strip().upper() if p.get("product_code") else None),
            brand=(str(p.get("brand")).strip().upper() if p.get("brand") else None),
            confidence=float(p.get("confidence") or 0.0),
            notes=str(p.get("notes") or ""),
        )
        for p in (out_json.get("observed_products") or [])
        if str(p.get("raw_text") or "").strip()
    ]

    return VisionPageResult(
        page_number=int(out_json["page_number"]),
        findings=findings,
        observed_products=observed_products,
        summary=str(out_json["summary"]),
    )
