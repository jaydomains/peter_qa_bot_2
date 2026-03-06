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

    Supports replies where the subject is e.g. "Re: ..." but includes a QID,
    and the body contains words like "confirm"/"reject" and optionally project type.
    """

    s = ((subject or "") + "\n" + (body or "")).strip()
    if not s:
        return ConfirmCommand("NONE", None, None, None, None, None)

    m = re.search(r"\b(Q-\d{8}-\d{6}-[0-9a-fA-F]{4})\b", s, flags=re.I)
    if not m:
        return ConfirmCommand("NONE", None, None, None, None, None)
    qid = m.group(1)

    low = s.lower()
    kind = "NONE"
    if "reject" in low or "decline" in low:
        kind = "REJECT"
    elif "confirm" in low or "approved" in low or low.strip() in ("yes", "y"):
        kind = "CONFIRM"

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

