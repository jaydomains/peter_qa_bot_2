from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

from openai import OpenAI

from .state import ConversationState

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

ALLOWED_ACTIONS = {"none", "create_site", "ingest_spec", "ingest_report", "list_reports", "ask_qa"}

SYSTEM_PROMPT = """
You are PETER, a coatings QA assistant embedded in a Telegram bot.

Output requirements:
- Always return valid JSON (no markdown fences) with keys: reply (string), action (string), slots (object).
- action must be one of: none, create_site, ingest_spec, ingest_report, list_reports, ask_qa.
- slots may only contain fields needed for the chosen action (site_code, site_name, address, spec_version, report_code, question).

Conversation policy:
- Respect the current conversation state provided in STATE/PENDING. If PENDING indicates the bot is already waiting for the user to answer a question, respond how the assistant should speak to the user to collect that info and set action="none".
- Do not fire an action until all required slots are known. Instead, ask concise follow-up questions one at a time.
- If there is no active site_code, guide the user to create or select a site (or tell them to run /newsite) and keep action="none".
- Prefer determinism: reuse known values from STATE unless the user explicitly changes them.
- When the user request maps to an existing command (/newsite, /addspec, /addreport, /listreports, /askqa), you may mention it, but still produce a structured reply/action.
- Never echo raw STATE or PENDING JSON to the user. Replies must be natural-language sentences.
- Keep replies short (1-2 sentences) and actionable.
""".strip()


def _safe_json_loads(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}

    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                return {}
        return {}


def _recent_history(history: List[Dict[str, str]], limit: int = 6) -> str:
    if not history:
        return "(none)"
    tail = history[-limit:]
    lines = []
    for turn in tail:
        role = turn.get("role", "?")
        content = (turn.get("content", "") or "").strip()
        content = content.replace("\n", " ")
        if len(content) > 200:
            content = content[:200] + "…"
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _sanitize_response(data: Dict[str, Any]) -> Dict[str, Any]:
    reply = str(data.get("reply") or "Unclear request.").strip()

    action = str(data.get("action") or "none").strip()
    if action not in ALLOWED_ACTIONS:
        action = "none"

    slots = data.get("slots")
    if not isinstance(slots, dict):
        slots = {}
    else:
        clean = {}
        for key, value in slots.items():
            if key in {"site_code", "site_name", "address", "spec_version", "report_code", "question"}:
                clean[key] = value
        slots = clean

    return {"reply": reply or "Unclear request.", "action": action, "slots": slots}


def call_llm(state: ConversationState, user_message: str) -> Dict[str, Any]:
    state_summary = {
        "site_code": state.site_code,
        "site_name": state.site_name,
        "address": state.address,
        "spec_version": state.spec_version,
        "report_code": state.report_code,
    }
    pending_summary = {
        "pending_action": state.pending_action,
        "pending_step": state.pending_step,
    }
    history_block = _recent_history(state.history)

    response = client.responses.create(
        model=os.environ.get("PETER_LLM_MODEL", "gpt-4.1"),
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"STATE:\n{json.dumps(state_summary)}\n"
                    f"PENDING:\n{json.dumps(pending_summary)}\n"
                    f"RECENT:\n{history_block}\n\nUSER:\n{user_message}"
                ),
            },
        ],
        response_format={"type": "json_object"},
    )

    raw = _safe_json_loads(getattr(response, "output_text", "") or "")
    if not isinstance(raw, dict):
        raw = {}

    return _sanitize_response(raw)
