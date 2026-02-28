from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Flag:
    key: str
    title: str
    evidence: list[str]


def _iter_sentences(text: str) -> list[str]:
    """Very lightweight sentence splitter.

    We prefer shorter, human-readable evidence in summaries; splitting into
    sentences produces cleaner snippets than matching arbitrary PDF-extracted
    line fragments.
    """

    t = (text or "").strip()
    if not t:
        return []

    # Normalize whitespace and keep newlines as potential boundaries.
    t = re.sub(r"[\t\r]+", " ", t)

    # Split on sentence-ish boundaries or line breaks.
    parts = re.split(r"(?<=[.!?])\s+|\n+", t)
    out: list[str] = []
    for p in parts:
        s = re.sub(r"\s+", " ", (p or "").strip())
        if not s:
            continue
        # Drop table-like / noisy fragments.
        if len(s) > 260:
            continue
        if s.count("\t") >= 2:
            continue
        out.append(s)
    return out


def _evidence_lines(text: str, pattern: re.Pattern[str], *, max_lines: int = 3) -> list[str]:
    ev: list[str] = []
    for sent in _iter_sentences(text):
        if pattern.search(sent):
            ev.append(sent)
            if len(ev) >= max_lines:
                break
    return ev


def build_flags(clean_text: str) -> list[Flag]:
    """Build deterministic flags from extracted text.

    Intended for narrative sections (e.g. Executive Summary). If you pass the
    entire PDF-extracted text (including tables), you may get noisy matches.
    """

    t = clean_text or ""

    flags: list[Flag] = []

    rules: list[tuple[str, str, re.Pattern[str]]] = [
        ("CRACKING", "Cracking mentioned", re.compile(r"\bcrack(?:ing|s)?\b", re.I)),
        ("DELAMINATION", "Delamination mentioned", re.compile(r"\bdelaminat(?:ion|ing)\b", re.I)),
        ("MOISTURE_HIGH", "High moisture mentioned", re.compile(r"\bHIGH\s+moisture\b|\bmoisture\s+content\b", re.I)),
        # Avoid matching generic PASS/FAIL tables; prefer narrative phrases.
        ("MOISTURE_FAIL", "Moisture FAIL / not acceptable indicated", re.compile(r"\bmoisture\b.{0,40}\bfail\b|\bfail\b.{0,40}\bmoisture\b|\bnot\s+acceptable\b", re.I)),
        # DFT often appears as: "DFT tests ... was noted to be low" (gap can be long)
        ("DFT_LOW", "Low DFT mentioned", re.compile(r"\bDFT\b.{0,160}\blow\b|\blow\b.{0,80}\bDFT\b|\bdry\s+film\s+thickness\b.{0,160}\blow\b", re.I)),
        ("BLISTERING", "Blistering/bubbling mentioned", re.compile(r"\bblister(?:ing)?\b|\bbubbl(?:e|ing)\b", re.I)),
        ("PEELING_FLAKING", "Peeling/flaking mentioned", re.compile(r"\bpeel(?:ing)?\b|\bflak(?:ing|es)?\b", re.I)),
    ]

    for key, title, pat in rules:
        ev = _evidence_lines(t, pat)
        if ev:
            flags.append(Flag(key=key, title=title, evidence=ev))

    return flags


def extract_section_excerpt(text: str, heading: str, *, window: int = 600) -> str | None:
    """Return a short excerpt around a heading (best-effort)."""
    t = text or ""
    i = t.lower().find(heading.lower())
    if i == -1:
        return None
    start = max(0, i)
    return t[start : min(len(t), i + window)].strip()
