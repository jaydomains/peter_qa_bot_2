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

    If use_openai is True and OPENAI_API_KEY present, uses an LLM to normalize
    and classify products (paint only) based strictly on provided text.

    Output is conservative: prefer fewer, higher-confidence entries.
    """

    mentions = extract_candidate_mentions(spec_text)

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not (use_openai and api_key and spec_text.strip()):
        # Fallback: return raw mentions as UNKNOWN products
        out: list[AllowedProduct] = []
        for m in mentions:
            out.append(AllowedProduct(raw_mention=m, brand=None, product=m, kind="UNKNOWN", aliases=[]))
        return out

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
        return [AllowedProduct(raw_mention=m, brand=None, product=m, kind="UNKNOWN", aliases=[]) for m in mentions]

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
        out2.append(AllowedProduct(raw_mention=rm, brand=brand_s, product=prod, kind="PAINT", aliases=aliases))

    # Deduplicate by (brand, product)
    seen: set[tuple[str | None, str]] = set()
    uniq: list[AllowedProduct] = []
    for p in out2:
        k = (p.brand, p.product)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(p)

    return uniq
