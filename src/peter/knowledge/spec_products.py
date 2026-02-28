from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from peter.interfaces.qa.openai_ask import ask_openai_responses


@dataclass(frozen=True)
class AllowedProduct:
    raw_mention: str
    brand: str | None
    product: str
    code: str | None
    kind: str  # PAINT|UNKNOWN
    aliases: list[str]


def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def extract_candidate_mentions(spec_text: str) -> list[str]:
    """Deterministically extract candidate product mentions from spec text.

    This is intentionally recall-oriented; we refine later.
    """

    t = spec_text or ""
    cands: set[str] = set()

    # Common vendor tokens (extend later)
    tokens = ["PLASCON", "KANSAI", "PP950", "PLASCOSAFE", "MULTI SURFACE", "UNIVERSAL UNDERCOAT"]

    for line in t.splitlines():
        l = _normalize_spaces(line).upper()
        if not l:
            continue

        if any(tok in l for tok in tokens):
            # Keep a bounded snippet
            snippet = l[:140]
            # Try to remove obvious non-product phrasing
            snippet = re.sub(r"\b(SPECIFICATION|GENERAL|PLEASE NOTE|RECOMMEND(?:ED|ATIONS)?)\b.*$", "", snippet).strip()
            if len(snippet) >= 6:
                cands.add(snippet)

    # Also pick up patterns like "Plascon Professional Plaster Primer - PP950"
    for m in re.finditer(r"(?i)\b(Plascon[^\n]{0,80})", t):
        s = _normalize_spaces(m.group(1))
        if 6 <= len(s) <= 120:
            cands.add(s.upper())

    return sorted(cands)


def extract_allowed_products(
    *,
    spec_text: str,
    use_openai: bool = True,
    model: str = "gpt-4.1",
) -> list[AllowedProduct]:
    """Extract paint product allowlist from spec.

    Strategy:
    1) Deterministic parse of common spec product lines (recommended, stable).
    2) Optional OpenAI normalization (off by default in production unless you
       trust prompts/cost).

    Strict policy: paint products only (exclude fillers, sealants, thinners, etc.).
    """

    mentions = extract_candidate_mentions(spec_text)

    # Deterministic extraction first
    exclude_terms = {
        "POLYFILLA",
        "SIKA",
        "SEALANT",
        "COMPOUND",
        "FILL",
        "FILLER",
        "KNOT SEAL",
        "THINNER",
        "CLEANER",
        "WATERPROOF",
        "WATERPROOFING",
        "MENDALL",
        # Non-product noise
        "CONSULTANT",
        "COLOUR REFERENCE",
        "COLOUR SYSTEM",
        "COATING APPLICATION",
        "COATING SYSTEM",
        "PROJECTS DEPARTMENT",
        "GUARANTEE",
        "FAX NO",
        "LTD",
        "(PTY)",
    }

    out_det: list[AllowedProduct] = []
    for m in mentions:
        u = m.upper()
        if any(t in u for t in exclude_terms):
            continue

        # Look for explicit product code in parentheses e.g. (PP950) or (PU800)
        m_code = re.search(r"\(([A-Z]{1,6}\s*\d{0,6}[A-Z0-9/-]{0,10})\)", u)
        code = _normalize_spaces(m_code.group(1)).upper() if m_code else ""

        # Try to extract after "PLASCON" token
        prod = u
        if "PLASCON" in u:
            prod = u.split("PLASCON", 1)[1].strip(" -")
        prod = re.sub(r"\(.*?\)", "", prod).strip(" -")
        prod = _normalize_spaces(prod)

        # Keep only plausible paint product phrases
        if len(prod) < 4:
            continue
        if "APPLY" in prod[:8]:
            prod = prod.replace("APPLY", "").strip(" -")

        # Require this to look like an instruction line (reduces noise)
        if "APPLY" not in u and not code:
            continue

        # Heuristic: paint products often include these words
        paint_hint = any(x in u for x in ["TOPCOAT", "UNDERCOAT", "PRIMER", "SHEEN", "PAINT", "VELVAGLO", "LOW SHEEN"]) or bool(code)
        if not paint_hint:
            continue

        # Brand: Kansai Plascon / Plascon
        brand = "KANSAI PLASCON" if "KANSAI" in u else "PLASCON"
        product = f"{prod} ({code})".strip() if code else prod

        out_det.append(AllowedProduct(raw_mention=u, brand=brand, product=product, code=(code or None), kind="PAINT", aliases=[]))

    # Deduplicate deterministic list
    seen2: set[tuple[str | None, str]] = set()
    uniq_det: list[AllowedProduct] = []
    for p in out_det:
        k = (p.brand, p.product)
        if k in seen2:
            continue
        seen2.add(k)
        uniq_det.append(p)

    # Optional OpenAI refinement (can be enabled; deterministic list is always included)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not (use_openai and api_key and spec_text.strip()):
        return uniq_det

    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": ["paint_products"],
        "properties": {
            "paint_products": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["raw_mention", "product"],
                    "properties": {
                        "raw_mention": {"type": "string"},
                        "brand": {"type": ["string", "null"]},
                        "product": {"type": "string"},
                        "code": {"type": ["string", "null"]},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                    },
                },
            }
        },
    }

    system = (
        "You extract an allowlist of PAINT products from a paint specification. "
        "ONLY include products that are explicitly mentioned in the provided text or mention list. "
        "Do NOT invent products. Repair materials must be excluded. "
        "Each output item must include raw_mention exactly as it appears in the mention list or spec excerpt."
    )

    user = (
        "SPEC TEXT (excerpt, may include product mentions):\n"
        + (spec_text[:12000])
        + "\n\n"
        + "CANDIDATE MENTIONS (uppercase snippets):\n"
        + "\n".join(f"- {m}" for m in mentions[:200])
        + "\n\n"
        + "Return JSON matching schema with paint_products."
    )

    # Use Responses API with a schema by piggy-backing our helper (text only).
    # We can't enforce json_schema in this helper without extending it, so we instruct strict JSON.
    raw = ask_openai_responses(api_key=api_key, model=model, system=system, user=user)
    try:
        data = json.loads(raw)
    except Exception:
        return [AllowedProduct(raw_mention=m, brand=None, product=m, code=None, kind="UNKNOWN", aliases=[]) for m in mentions]

    out2: list[AllowedProduct] = []
    for it in (data.get("paint_products") or []):
        rm = _normalize_spaces(str(it.get("raw_mention") or "")).upper()
        if not rm:
            continue
        prod = _normalize_spaces(str(it.get("product") or "")).upper()
        if not prod:
            continue
        brand = it.get("brand")
        brand_s = _normalize_spaces(str(brand)).upper() if brand else None
        aliases = [
            _normalize_spaces(str(a)).upper()
            for a in (it.get("aliases") or [])
            if _normalize_spaces(str(a))
        ]
        out2.append(AllowedProduct(raw_mention=rm, brand=brand_s, product=prod, code=(str(it.get("code") or "").strip().upper() or None), kind="PAINT", aliases=aliases))

    # Merge deterministic + openai outputs (openai may normalize names)
    merged = uniq_det[:]
    seen = {(p.brand, p.product) for p in merged}
    for p in out2:
        k = (p.brand, p.product)
        if k in seen:
            continue
        seen.add(k)
        merged.append(p)

    return merged
