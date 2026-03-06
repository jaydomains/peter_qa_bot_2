from __future__ import annotations

import re


def extract_batch_candidates(raw_text: str) -> list[str]:
    """Extract batch/lot candidate tokens from label raw_text.

    Batch codes are not consistently labeled. We pull plausible ID-like tokens.

    Heuristics:
    - long-ish alphanumeric tokens (>=6) containing digits
    - tokens with hyphens/slashes
    - ignore obvious product codes like PP700 unless longer context
    """

    t = (raw_text or "")
    # Normalize
    t2 = re.sub(r"[\t\r]+", " ", t)

    cands: set[str] = set()

    # Common explicit labels
    for m in re.finditer(r"(?i)\b(batch|lot)\s*(?:no\.?|number|#|:)?\s*([A-Z0-9][A-Z0-9\-/]{4,30})\b", t2):
        cands.add(m.group(2).upper())

    # Generic ID-like tokens
    for tok in re.findall(r"\b[A-Z0-9][A-Z0-9\-/]{5,30}\b", t2.upper()):
        if not any(ch.isdigit() for ch in tok):
            continue
        # Filter out common short product codes
        if re.fullmatch(r"[A-Z]{1,4}\d{2,4}", tok) and len(tok) <= 6:
            continue
        cands.add(tok)

    out = sorted(cands)
    return out[:10]
