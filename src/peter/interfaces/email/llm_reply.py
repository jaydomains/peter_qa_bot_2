from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
import re

from peter.config.settings import Settings
from peter.domain.errors import ValidationError
from peter.interfaces.qa.openai_ask import ask_openai_responses


@dataclass(frozen=True)
class EvidencePack:
    metadata: str
    exec_excerpt: str
    blocking_issues: str
    other_issues: str
    vision: str


def _build_evidence_pack(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    site_code: str,
    report_code: str,
    vision_text: str,
) -> EvidencePack:
    sc = (site_code or "").strip().upper()
    rc_in = (report_code or "").strip().upper().replace(" ", "")

    # Normalize report_code so R001 and 001 refer to the same logical report when applicable.
    rc = rc_in
    try:
        from peter.services.report_service import ReportService

        rc = ReportService(conn, settings)._validate_report_code(rc_in)
    except Exception:
        rc = rc_in

    # Try exact match first; then fall back between R### and ### forms.
    row = conn.execute(
        """
        SELECT r.id, r.sha256, r.stored_path, r.received_at, r.result
        FROM reports r
        JOIN sites s ON s.id = r.site_id
        WHERE s.site_code = ? AND r.report_code = ?
        ORDER BY r.received_at DESC
        LIMIT 1
        """,
        (sc, rc),
    ).fetchone()

    if not row:
        alt = None
        if re.fullmatch(r"R\d{2,3}", rc):
            alt = rc[1:].zfill(3)
        elif re.fullmatch(r"\d{2,3}", rc):
            alt = "R" + rc.zfill(3)
        if alt:
            row = conn.execute(
                """
                SELECT r.id, r.sha256, r.stored_path, r.received_at, r.result
                FROM reports r
                JOIN sites s ON s.id = r.site_id
                WHERE s.site_code = ? AND r.report_code = ?
                ORDER BY r.received_at DESC
                LIMIT 1
                """,
                (sc, alt),
            ).fetchone()
    if not row:
        raise ValidationError(f"Report not found for site={sc} report_code={rc}")

    report_id = int(row["id"])

    meta = (
        f"site={sc}\n"
        f"report_code={rc}\n"
        f"received_at={row['received_at']}\n"
        f"result={row['result']}\n"
        f"sha256={row['sha256']}\n"
        f"stored_path={row['stored_path']}\n"
    )

    # Executive summary excerpt (reuse existing summary generator)
    exec_excerpt = "(not available)"
    try:
        from peter.services.report_service import ReportService

        svc = ReportService(conn, settings)
        summary = svc.summarize_report_text(site_code=sc, report_code=rc)
        marker = "EXECUTIVE SUMMARY (excerpt)"
        if marker in summary:
            exec_excerpt = summary.split(marker, 1)[1].strip()
            # Keep bounded
            exec_excerpt = exec_excerpt[:3500].strip()
    except Exception:
        exec_excerpt = "(not available)"

    # Issues
    issues_rows = conn.execute(
        """
        SELECT issue_type, category, severity, is_blocking, description
        FROM issues
        WHERE report_id = ?
        ORDER BY is_blocking DESC,
                 CASE severity WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3 WHEN 'MED' THEN 2 ELSE 1 END DESC,
                 created_at DESC
        """,
        (report_id,),
    ).fetchall()

    blocking_lines: list[str] = []
    other_lines: list[str] = []
    for r in issues_rows[:40]:
        block = bool(int(r["is_blocking"] or 0))
        desc = str(r["description"] or "")
        desc = desc.replace("\r", " ").strip()
        line = f"- [{r['severity']}] [{'blocking' if block else 'non-blocking'}] {r['category']} ({r['issue_type']}): {desc[:380]}"
        (blocking_lines if block else other_lines).append(line)

    blocking_text = "\n".join(blocking_lines) if blocking_lines else "(none)"
    other_text = "\n".join(other_lines) if other_lines else "(none)"

    vision_text = (vision_text or "").strip() or "(not available)"

    return EvidencePack(
        metadata=meta,
        exec_excerpt=exec_excerpt,
        blocking_issues=blocking_text,
        other_issues=other_text,
        vision=vision_text,
    )


def draft_email_reply_llm(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    site_code: str,
    report_code: str,
    vision_text: str,
) -> str:
    """Draft a human-like QA email reply using OpenAI, grounded in evidence."""

    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise ValidationError("OPENAI_API_KEY not set")

    pack = _build_evidence_pack(
        conn=conn,
        settings=settings,
        site_code=site_code,
        report_code=report_code,
        vision_text=vision_text,
    )

    # Conditional depth
    has_blocking = pack.blocking_issues.strip() not in ("", "(none)")
    depth = "deep" if has_blocking or ("VISION — Blocking" in pack.vision) else "executive"

    model = os.getenv("PETER_EMAIL_DRAFT_MODEL", "gpt-4.1")

    system = (
        "You are PETER, the QA lead for decorative architectural coatings, replacing a human reviewer. "
        "You MUST be grounded: only use the EVIDENCE provided. "
        "Do not invent defects, products, test results, or requirements. "
        "If something is not explicitly supported by evidence, say you cannot confirm it. "
        "Write like a competent human QA reviewer: specific, decisive, and practical. "
        "Be more helpful than a summary: interpret what the findings imply for warranty/compliance and what evidence is required next. "
        "Scope: paint only (ignore repair materials unless directly relevant to paint performance). "
        "Always include a short EVIDENCE section at the end listing which sources you relied on."
    )

    user = (
        f"DEPTH: {depth}\n"
        "Write an email reply suitable for internal QA stakeholders.\n"
        "Tone: professional, direct, human.\n\n"
        "EVIDENCE (only source of truth):\n"
        "--- METADATA ---\n"
        f"{pack.metadata}\n"
        "--- EXEC_SUMMARY ---\n"
        f"{pack.exec_excerpt}\n\n"
        "--- BLOCKING_ISSUES (DB) ---\n"
        f"{pack.blocking_issues}\n\n"
        "--- OTHER_ISSUES (DB) ---\n"
        f"{pack.other_issues}\n\n"
        "--- VISION ---\n"
        f"{pack.vision}\n\n"
        "OUTPUT REQUIREMENTS:\n"
        "- Start with: OVERALL STATUS: PASS/WARN/FAIL (choose based on evidence; if result in metadata is set, respect it)\n"
        "- Then: Key findings (bullets).\n"
        "  - You MUST explicitly address EVERY item in BLOCKING_ISSUES, even if the executive summary does not mention it.\n"
        "  - For each key finding, cite source inline: (Source: EXEC_SUMMARY) or (Source: BLOCKING_ISSUES) or (Source: OTHER_ISSUES) or (Source: VISION pX).\n"
        "- Then: Required actions (bullets).\n"
        "  - Each blocking issue must have at least one required action tied to it.\n"
        "  - If the blocking issue is a spec deviation/coating system mismatch, include a warranty/compliance implication and request for evidence (invoices/POs/photos/logs).\n"
        "- If DEPTH is deep: include a short 'What to verify next visit' section.\n"
        "- End with: EVIDENCE (short list of what you used).\n"
    )

    return ask_openai_responses(api_key=api_key, model=model, system=system, user=user).strip() + "\n"
