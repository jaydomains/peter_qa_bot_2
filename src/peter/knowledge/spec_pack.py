from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from peter.interfaces.qa.openai_ask import ask_openai_responses


@dataclass(frozen=True)
class SpecPack:
    spec_type: str  # NEW_WORK|REDEC|UNKNOWN
    supplier_prefix: str | None  # PR|PLA|SAB (best-effort)
    allowed_products: list[dict[str, Any]]  # {code,name,brand,role(optional),aliases[]}
    role_rules: list[dict[str, Any]]  # {kind, product_code(optional), text, severity, requires_confirmation}


def _extract_prefix_from_text(spec_text: str) -> str | None:
    t = (spec_text or "")
    # Best-effort: look for PLASCON / PROMINENT / SABRE indicators
    low = t.lower()
    if "plascon" in low:
        return "PLA"
    if "prominent" in low:
        return "PR"
    if "sabre" in low:
        return "SAB"
    return None


def extract_spec_pack(*, spec_text: str) -> SpecPack:
    """Use OpenAI to parse a spec PDF (text) into a structured JSON pack.

    This is used for:
    - product allowlists (beyond naive extraction)
    - role mismatch rules (spot prime vs full prime etc.)
    - spec type classification (NEW_WORK vs REDEC)

    If OpenAI isn't configured, raise.
    """

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    model = os.getenv("PETER_SPEC_PACK_MODEL", "gpt-4.1")

    system = (
        "You are PETER, a QA spec parser for decorative architectural coatings. "
        "Your job is to extract a strict JSON spec pack. "
        "Do not output any text except valid JSON."
    )

    user = (
        "Parse the following SPEC TEXT into JSON with this schema:\n\n"
        "{\n"
        "  \"spec_type\": \"NEW_WORK\"|\"REDEC\"|\"UNKNOWN\",\n"
        "  \"allowed_products\": [\n"
        "    {\"code\": string|null, \"name\": string, \"brand\": string|null, \"role\": string|null, \"aliases\": [string]}\n"
        "  ],\n"
        "  \"role_rules\": [\n"
        "    {\"kind\": string, \"product_code\": string|null, \"text\": string, \"severity\": \"CRITICAL\"|\"MAJOR\"|\"MINOR\"|\"INFO\", \"requires_confirmation\": boolean}\n"
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "- spec_type: NEW_WORK if it describes new plaster/new work; REDEC if it describes redecorations/repaint/existing coatings; else UNKNOWN.\n"
        "- allowed_products: include primer/topcoat systems where possible; prefer codes like PP700, PP950, PWC520, etc.\n"
        "- role_rules: capture constraints like 'spot prime only', 'full coat required if >50% patch priming', 'do not use as primer', etc.\n"
        "- requires_confirmation: true if the rule could be ambiguous in real-world (e.g. drums on site vs applied).\n\n"
        "SPEC TEXT:\n"
        + (spec_text[:45000])
    )

    raw = ask_openai_responses(api_key=api_key, model=model, system=system, user=user)

    data = json.loads(raw)

    spec_type = str(data.get("spec_type") or "UNKNOWN").strip().upper()
    if spec_type not in ("NEW_WORK", "REDEC", "UNKNOWN"):
        spec_type = "UNKNOWN"

    allowed = data.get("allowed_products") or []
    if not isinstance(allowed, list):
        allowed = []

    role_rules = data.get("role_rules") or []
    if not isinstance(role_rules, list):
        role_rules = []

    # Normalize
    norm_allowed: list[dict[str, Any]] = []
    for p in allowed:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name") or "").strip()
        if not name:
            continue
        code = p.get("code")
        code_s = str(code).strip().upper().replace(" ", "") if code else None
        brand = str(p.get("brand") or "").strip() or None
        role = str(p.get("role") or "").strip() or None
        aliases = p.get("aliases") if isinstance(p.get("aliases"), list) else []
        aliases2 = [str(a).strip() for a in aliases if str(a).strip()]
        norm_allowed.append({"code": code_s, "name": name, "brand": brand, "role": role, "aliases": aliases2})

    norm_rules: list[dict[str, Any]] = []
    for r in role_rules:
        if not isinstance(r, dict):
            continue
        text = str(r.get("text") or "").strip()
        if not text:
            continue
        kind = str(r.get("kind") or "RULE").strip().upper()
        pc = r.get("product_code")
        pc_s = str(pc).strip().upper().replace(" ", "") if pc else None
        sev = str(r.get("severity") or "INFO").strip().upper()
        if sev not in ("CRITICAL", "MAJOR", "MINOR", "INFO"):
            sev = "INFO"
        reqc = bool(r.get("requires_confirmation") if "requires_confirmation" in r else True)
        norm_rules.append({"kind": kind, "product_code": pc_s, "text": text, "severity": sev, "requires_confirmation": reqc})

    prefix = _extract_prefix_from_text(spec_text)

    return SpecPack(
        spec_type=spec_type,
        supplier_prefix=prefix,
        allowed_products=norm_allowed,
        role_rules=norm_rules,
    )
