from __future__ import annotations

import re

from peter.analysis.defect_taxonomy import CanonicalDefect


_TEXT_PATTERNS: list[tuple[CanonicalDefect, re.Pattern[str]]] = [
    (CanonicalDefect.CRACKING, re.compile(r"\bcrack(?:ing|s)?\b", re.I)),
    (CanonicalDefect.PEELING_FLAKING, re.compile(r"\b(peel(?:ing)?|flak(?:ing|es)?)\b", re.I)),
    (CanonicalDefect.BLISTERING, re.compile(r"\bblister(?:ing|s)?\b", re.I)),
    (CanonicalDefect.EFFLORESCENCE, re.compile(r"\beffloresc(?:ence|ing)\b|\bsalt\s+bloom\b", re.I)),
    (CanonicalDefect.DAMPNESS_MOULD_ALGAE, re.compile(r"\b(damp|wet\s+patch|moisture|mould|mold|algae)\b", re.I)),
    (CanonicalDefect.DELAMINATION, re.compile(r"\bdelaminat(?:ion|ing)\b", re.I)),
    (CanonicalDefect.RUST_STAINING, re.compile(r"\brust\b|\brust\s+bleed\b", re.I)),
    (CanonicalDefect.POOR_COVERAGE_EXPOSED_SUBSTRATE, re.compile(r"\b(thin\s+coverage|poor\s+coverage|holidays|pinhol(?:e|ing)|exposed\s+substrate)\b", re.I)),
    (CanonicalDefect.UNEVEN_SHEEN, re.compile(r"\b(uneven\s+sheen|flashing|patchy\s+sheen)\b", re.I)),
    (CanonicalDefect.TEXTURE_INCONSISTENCY, re.compile(r"\b(texture\s+inconsisten|rough\s+patch|stippl(?:e|ed))\b", re.I)),
]


def extract_text_defects(text: str) -> set[CanonicalDefect]:
    t = text or ""
    out: set[CanonicalDefect] = set()
    for d, pat in _TEXT_PATTERNS:
        if pat.search(t):
            out.add(d)
    return out
