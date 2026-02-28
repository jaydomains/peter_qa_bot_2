from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedCommand:
    kind: str  # NEW_SITE|SPEC_UPDATE|QA_REPORT|QUERY|UNKNOWN
    site_code: str | None
    arg: str | None


def parse_subject(subject: str) -> ParsedCommand:
    """Deterministic parsing.

    Expected formats:
      NEW SITE | <SITE_CODE> | <SiteName>
      SPEC UPDATE | <SITE_CODE> | <Rev>
      QA REPORT | <SITE_CODE> | <RXX>
      QUERY | <SITE_CODE> | <COMMAND>
      REPLY | <SITE_CODE> | <REPORT_REF>
      ASSIST | <SITE_CODE> | <FREEFORM REQUEST>
    """

    s = (subject or "").strip()
    parts = [p.strip() for p in s.split("|")]
    if len(parts) < 1:
        return ParsedCommand("UNKNOWN", None, None)

    cmd = parts[0].upper()
    if cmd in {"NEW SITE", "SPEC UPDATE", "QA REPORT", "QUERY", "REPLY", "ASSIST"} and len(parts) >= 3:
        kind = {
            "NEW SITE": "NEW_SITE",
            "SPEC UPDATE": "SPEC_UPDATE",
            "QA REPORT": "QA_REPORT",
            "QUERY": "QUERY",
            "REPLY": "REPLY",
            "ASSIST": "ASSIST",
        }[cmd]
        site_code = parts[1].upper()
        arg = "|".join(parts[2:]).strip()
        return ParsedCommand(kind, site_code, arg)

    return ParsedCommand("UNKNOWN", None, None)
