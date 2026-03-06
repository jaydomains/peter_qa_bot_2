from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ConfirmCommand:
    kind: str  # CONFIRM|REJECT|NONE
    qid: str | None
    site: str | None
    report: str | None
    project_type: str | None  # NEW_WORK|REDEC
    decision: str | None  # USED|NOT_USED|MORE_INFO


def coerce_project_type(v: str | None) -> str | None:
    if not v:
        return None
    vv = str(v).strip().upper().replace("-", "_").replace(" ", "")
    if vv in ("NEW", "NEWWORK", "NEW_WORK", "NEWWORKS", "NEWBUILD", "NEW_BUILD"):
        return "NEW_WORK"
    if vv in ("REDEC", "REDECORATION", "REDECORATIONS", "REPAINT"):
        return "REDEC"
    return None


def parse_confirm_subject(subject: str) -> ConfirmCommand:
    s = (subject or "").strip()
    if not s:
        return ConfirmCommand("NONE", None, None, None, None, None)

    m = re.match(r"^(CONFIRM|REJECT)\s+(Q-\d{8}-\d{6}-[0-9a-fA-F]{4})\s*(?:\|\s*(.*))?$", s, flags=re.I)
    if not m:
        return ConfirmCommand("NONE", None, None, None, None, None)

    kind = m.group(1).upper()
    qid = m.group(2)
    rest = (m.group(3) or "").strip()

    site = None
    report = None
    ptype = None
    decision = None
    if rest:
        # parse tokens like SITE=PRSVNQA | REPORT=006
        parts = [p.strip() for p in rest.split("|") if p.strip()]
        for p in parts:
            if "=" not in p:
                continue
            k, v = [x.strip() for x in p.split("=", 1)]
            k = k.upper()
            v = v.strip().upper().replace(" ", "")
            if k == "SITE":
                site = v
            elif k in ("REPORT", "REF", "REPORTNO"):
                report = v
            elif k in ("TYPE", "PROJECT", "PROJECTTYPE"):
                vv = v.replace("-", "_")
                if vv in ("NEW", "NEWWORK", "NEW_WORK"):
                    ptype = "NEW_WORK"
                elif vv in ("REDEC", "REDECORATION", "REDECORATIONS"):
                    ptype = "REDEC"
            elif k in ("DECISION", "USED"):
                vv = v.replace("-", "_")
                if vv in ("USED", "APPLIED", "YES"):
                    decision = "USED"
                elif vv in ("NOTUSED", "NOT_USED", "NO", "EMPTYDRUMS", "EMPTY_DRUMS", "DECANTING"):
                    decision = "NOT_USED"
                elif vv in ("MOREINFO", "MORE_INFO", "NEEDSMOREINFO"):
                    decision = "MORE_INFO"

    return ConfirmCommand(kind, qid, site, report, ptype, decision)


def parse_confirm_freeform(subject: str, body: str) -> ConfirmCommand:
    """Parse free-text confirmation replies.

    Important: replies often include quoted prior messages containing BOTH CONFIRM and REJECT options.
    We therefore:
    - Prefer an explicit command line in the *new* text (e.g. "CONFIRM Q-... | ...")
    - Fall back to keyword inference only if no explicit command line is found.
    """

    subj = (subject or "").strip()
    body0 = (body or "").strip()
    s = (subj + "\n" + body0).strip()
    if not s:
        return ConfirmCommand("NONE", None, None, None, None, None)

    # 1) Look for explicit command lines first (prefer CONFIRM if both appear)
    cmd_lines = []
    for ln in body0.splitlines():
        lns = ln.strip()
        if not lns:
            continue
        # Skip common quoted lines
        if lns.startswith(">"):
            continue
        m = re.match(r"^(CONFIRM|REJECT)\s+(Q-\d{8}-\d{6}-[0-9a-fA-F]{4})\s*(?:\|\s*(.*))?$", lns, flags=re.I)
        if m:
            cmd_lines.append(lns)

    for prefer in ("CONFIRM", "REJECT"):
        for ln in cmd_lines:
            if ln.upper().startswith(prefer + " "):
                # Reuse the subject parser for consistent token parsing
                return parse_confirm_subject(ln)

    # 2) Otherwise, find a QID anywhere and infer kind/ptype/decision from keywords
    m = re.search(r"\b(Q-\d{8}-\d{6}-[0-9a-fA-F]{4})\b", s, flags=re.I)
    if not m:
        return ConfirmCommand("NONE", None, None, None, None, None)
    qid = m.group(1)

    low = s.lower()

    # Determine kind (prefer CONFIRM if both present; avoid quoted option lists causing accidental REJECT)
    kind = "NONE"
    if "confirm" in low or "approved" in low or low.strip() in ("yes", "y"):
        kind = "CONFIRM"
    elif "reject" in low or "decline" in low:
        kind = "REJECT"

    # Project type inference from free text
    ptype = None
    if "redec" in low or "re-decoration" in low or "redecoration" in low:
        ptype = "REDEC"
    if "new work" in low or "newwork" in low or "new-build" in low or "new build" in low:
        ptype = "NEW_WORK"

    # Try to infer decision
    decision = None
    if "not used" in low or "empty drum" in low or "empty drums" in low or "decant" in low:
        decision = "NOT_USED"
    elif "used" in low or "applied" in low:
        decision = "USED"
    elif "more info" in low or "need more" in low or "needs more" in low:
        decision = "MORE_INFO"

    return ConfirmCommand(kind, qid, None, None, ptype, decision)

