from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from peter.config.settings import Settings
from peter.domain.errors import ValidationError
from peter.interfaces.qa.openai_ask import ask_openai_responses


@dataclass(frozen=True)
class AssistPlan:
    kind: str  # summary|first_n|last_n|range
    n: int
    order: str  # asc|desc
    days: int | None
    include_products: bool


def _parse_plan_llm(*, request: str) -> AssistPlan:
    # Lightweight LLM-based plan. Falls back to defaults if parsing fails.
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        # default: last 5
        return AssistPlan(kind="last_n", n=5, order="desc", days=None, include_products=True)

    system = (
        "You are a routing assistant. Convert the user's request into a JSON plan. "
        "Only output valid JSON."
    )
    user = (
        "Request:\n"
        f"{request}\n\n"
        "Output JSON with keys: kind(one of first_n,last_n,summary), n(int 1-20), order(asc|desc), days(optional int), include_products(boolean)."
    )
    raw = ask_openai_responses(api_key=api_key, model=os.getenv("PETER_EMAIL_ASSIST_MODEL", "gpt-4.1"), system=system, user=user)
    try:
        data = json.loads(raw)
        kind = str(data.get("kind") or "last_n")
        n = int(data.get("n") or 5)
        n = max(1, min(20, n))
        order = str(data.get("order") or ("asc" if kind == "first_n" else "desc")).lower()
        if order not in ("asc", "desc"):
            order = "desc"
        days = data.get("days")
        days_i = int(days) if isinstance(days, (int, float, str)) and str(days).strip().isdigit() else None
        include_products = bool(data.get("include_products") if "include_products" in data else True)
        if kind not in ("first_n", "last_n", "summary"):
            kind = "last_n"
        return AssistPlan(kind=kind, n=n, order=order, days=days_i, include_products=include_products)
    except Exception:
        return AssistPlan(kind="last_n", n=5, order="desc", days=None, include_products=True)


def _extract_products_from_vision(*, settings: Settings, review_json_path: str | None) -> list[str]:
    if not review_json_path:
        return []
    p = (settings.QA_ROOT / review_json_path).resolve()
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

    found: list[str] = []
    for page in data.get("pages") or []:
        for op in page.get("observed_products") or []:
            code = (op.get("product_code") or "").strip().upper()
            raw = (op.get("raw_text") or "").strip().upper()
            if code:
                found.append(code)
            elif raw:
                # pull a likely code token
                m = re.search(r"\b([A-Z]{1,5}\s*\d{2,4})\b", raw)
                found.append(m.group(1).replace(" ", "") if m else raw[:40])

    # dedupe
    out: list[str] = []
    seen: set[str] = set()
    for x in found:
        x = x.replace(" ", "").strip()
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def run_assist(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    site_code: str,
    request: str,
) -> str:
    sc = (site_code or "").strip().upper()
    req = (request or "").strip()
    if not sc or not req:
        raise ValidationError("ASSIST requires site_code and request")

    site = conn.execute("SELECT id, site_code, folder_name FROM sites WHERE site_code=?", (sc,)).fetchone()
    if not site:
        raise ValidationError(f"Unknown site_code: {sc}")

    plan = _parse_plan_llm(request=req)

    # Fetch report list
    order = "ASC" if plan.order == "asc" else "DESC"
    where_days = ""
    params: list[Any] = [site["id"]]
    if plan.days is not None:
        where_days = " AND received_at >= datetime('now', ?) "
        params.append(f"-{int(plan.days)} days")

    rows = conn.execute(
        f"""
        SELECT id, report_code, received_at, result, sha256, stored_path, review_json_path
        FROM reports
        WHERE site_id = ? {where_days}
        ORDER BY received_at {order}
        LIMIT ?
        """,
        tuple(params + [plan.n]),
    ).fetchall()

    if not rows:
        return f"No reports found for site={sc}."

    # Build evidence bundle
    report_blocks: list[str] = []
    all_products: list[str] = []

    for r in rows:
        rid = int(r["id"])
        issues = conn.execute(
            """
            SELECT issue_type, category, severity, is_blocking
            FROM issues
            WHERE report_id = ?
            ORDER BY is_blocking DESC,
                     CASE severity WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3 WHEN 'MED' THEN 2 ELSE 1 END DESC,
                     created_at DESC
            LIMIT 20
            """,
            (rid,),
        ).fetchall()

        issues_lines = []
        for it in issues:
            block = "blocking" if int(it["is_blocking"] or 0) else "non-blocking"
            issues_lines.append(f"- [{it['severity']}] [{block}] {it['category']} ({it['issue_type']})")

        prods = _extract_products_from_vision(settings=settings, review_json_path=r["review_json_path"])
        all_products.extend(prods)

        report_blocks.append(
            "\n".join(
                [
                    f"REPORT {r['report_code']} received_at={r['received_at']} result={r['result']} sha={str(r['sha256'])[:12]}",
                    "ISSUES:",
                    *(issues_lines or ["- (none)"]),
                    "PRODUCTS (vision labels, best-effort):",
                    *( [f"- {p}" for p in prods] if prods else ["- (none)"] ),
                ]
            )
        )

    # Dedup products
    prod_out: list[str] = []
    seenp: set[str] = set()
    for p in all_products:
        if p in seenp:
            continue
        seenp.add(p)
        prod_out.append(p)

    evidence = (
        f"SITE={sc}\n"
        f"PLAN={plan}\n\n"
        + "\n\n".join(report_blocks)
        + "\n\nUNIQUE_PRODUCTS:\n"
        + ("\n".join(f"- {p}" for p in prod_out) if prod_out else "- (none)")
    )

    # Compose final answer with LLM (grounded)
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        # fallback: return evidence dump
        return evidence

    system = (
        "You are PETER, an internal QA assistant. You MUST be grounded: use only the evidence provided. "
        "Answer the user's request in a human, practical way. "
        "When you summarize issues, group recurring themes. "
        "Be concise but not shallow. "
        "Include a short EVIDENCE section listing which reports you used (report codes)."
    )

    user = (
        f"USER REQUEST:\n{req}\n\n"
        f"EVIDENCE:\n{evidence}\n\n"
        "Output format:\n"
        "- Answer\n"
        "- Key patterns\n"
        "- Actionable next steps\n"
        "- EVIDENCE (report codes)\n"
    )

    return ask_openai_responses(
        api_key=api_key,
        model=os.getenv("PETER_EMAIL_ASSIST_MODEL", "gpt-4.1"),
        system=system,
        user=user,
    ).strip() + "\n"
