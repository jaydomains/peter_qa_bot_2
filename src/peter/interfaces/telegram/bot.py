from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from peter.storage.layout import Layout

from .actions import run_action
from .llm_dialog import call_llm
from .state import ConversationState

logger = logging.getLogger(__name__)


def get_allowed_users() -> set[int]:
    raw = os.environ.get("PETER_TELEGRAM_ALLOWED_USERS", "").strip()
    if not raw:
        return set()
    allowed: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            allowed.add(int(part))
    return allowed


def is_allowed(user_id: int) -> bool:
    allowed = get_allowed_users()
    return (not allowed) or (user_id in allowed)


def _chat_id(update: Update) -> int:
    assert update.effective_chat is not None
    return int(update.effective_chat.id)


def _user_id(update: Update) -> int:
    assert update.effective_user is not None
    return int(update.effective_user.id)


def _layout() -> Layout:
    return Layout.from_env()


def _ensure_event_loop() -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


async def _run_action_safe(action: str, slots: Dict[str, Any], state: ConversationState) -> str:
    return await asyncio.to_thread(run_action, action, slots, state)


def _format_action_result(result: str) -> str:
    if not result:
        return "✅ Done."
    lowered = result.lower()
    if "failed" in lowered or "error" in lowered:
        return f"❌ {result}"
    return f"✅ {result}"


def _begin_pending(state: ConversationState, action: str, step: str) -> None:
    state.pending_action = action
    state.pending_step = step
    state.pending_data = {}
    state.save()


def _clear_pending(state: ConversationState) -> None:
    state.pending_action = None
    state.pending_step = None
    state.pending_data = {}
    state.save()


def _require_site(state: ConversationState) -> tuple[bool, str]:
    if not state.site_code:
        return False, "No active site. Run /newsite to create one first."
    return True, ""


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    state.reset()

    await update.message.reply_text(
        "PETER QA Assistant Ready.\n\n"
        "BotFather commands:\n"
        "• /newsite — start site setup\n"
        "• /addspec — ingest a spec\n"
        "• /addreport — ingest a QA report\n"
        "• /listreports — list site reports\n"
        "• /askqa — ask a QA question\n"
        "• /status — show current state\n"
        "• /reset — clear conversation"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))

    lines = ["📊 Current State"]
    if state.site_code:
        lines.append(f"• Site: {state.site_code} — {state.site_name or 'unnamed'}")
        if state.address:
            lines.append(f"  Address: {state.address}")
    else:
        lines.append("• No active site. Use /newsite to create one.")

    if state.spec_version:
        lines.append(f"• Last spec version: {state.spec_version}")
    if state.report_code:
        lines.append(f"• Last report code: {state.report_code}")

    if state.pending_action:
        lines.append(
            f"• Pending: {state.pending_action.replace('_', ' ')} (waiting for {state.pending_step or 'next input'})"
        )
    else:
        lines.append("• Pending: none")

    await update.message.reply_text("\n".join(lines))


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    state.reset()
    await update.message.reply_text("State reset.")


async def new_site(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    _clear_pending(state)
    _begin_pending(state, "create_site", "site_code")
    await update.message.reply_text("Creating a site. What is the site code? (e.g. ABC123)")


async def add_spec(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    ok, msg = _require_site(state)
    if not ok:
        await update.message.reply_text(msg)
        return

    _clear_pending(state)
    _begin_pending(state, "ingest_spec", "spec_version")
    await update.message.reply_text("Spec ingest: send the spec version/identifier.")


async def add_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    ok, msg = _require_site(state)
    if not ok:
        await update.message.reply_text(msg)
        return

    _clear_pending(state)
    _begin_pending(state, "ingest_report", "report_code")
    await update.message.reply_text("Report ingest: send the report code or version.")


async def list_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    ok, msg = _require_site(state)
    if not ok:
        await update.message.reply_text(msg)
        return

    result = await _run_action_safe("list_reports", {}, state)
    await update.message.reply_text(result or "No reports found.")


async def ask_qa(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    state = ConversationState.load(_chat_id(update))
    ok, msg = _require_site(state)
    if not ok:
        await update.message.reply_text(msg)
        return

    _clear_pending(state)
    _begin_pending(state, "ask_qa", "question")
    await update.message.reply_text("Ask QA: what's your question?")


async def _handle_pending_text(state: ConversationState, text: str) -> str:
    action = state.pending_action or ""
    step = state.pending_step or ""
    data = state.pending_data or {}

    if action == "create_site":
        if step == "site_code":
            state.pending_data = {"site_code": text.strip()}
            state.pending_step = "site_name"
            state.save()
            return "Got it. What's the site name?"
        if step == "site_name":
            state.pending_data["site_name"] = text.strip()
            state.pending_step = "address"
            state.save()
            return "Thanks. What's the site address?"
        if step == "address":
            state.pending_data["address"] = text.strip()
            slots = state.pending_data.copy()
            result = await _run_action_safe("create_site", slots, state)
            _clear_pending(state)
            return _format_action_result(result)

    if action == "ingest_spec":
        if step == "spec_version":
            state.pending_data = {"spec_version": text.strip()}
            state.pending_step = "awaiting_upload"
            state.save()
            return "Version noted. Please upload the spec PDF now."
        if step == "awaiting_upload":
            return "Waiting for the spec PDF upload. Attach the file to continue."

    if action == "ingest_report":
        if step == "report_code":
            state.pending_data = {"report_code": text.strip()}
            state.pending_step = "awaiting_upload"
            state.save()
            return "Report code noted. Upload the QA report PDF next."
        if step == "awaiting_upload":
            return "Waiting for the report PDF upload. Attach the file to continue."

    if action == "ask_qa":
        if step == "question":
            slots = {"question": text.strip()}
            result = await _run_action_safe("ask_qa", slots, state)
            _clear_pending(state)
            return result or "Answered."

    return "Still working on the current command. Use /reset to start over if needed."


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    if not update.message or not update.message.text:
        return

    chat_id = _chat_id(update)
    user_text = update.message.text.strip()

    state = ConversationState.load(chat_id)
    state.append_history("user", user_text)

    if state.pending_action:
        reply = await _handle_pending_text(state, user_text)
        state.append_history("assistant", reply)
        await update.message.reply_text(reply)
        return

    try:
        llm_response = call_llm(state, user_text)

        reply = str(llm_response.get("reply") or "Unclear request.").strip()
        action = str(llm_response.get("action") or "none").strip()
        slots = llm_response.get("slots") or {}
        if not isinstance(slots, dict):
            slots = {}

        if action != "none":
            action_result = await _run_action_safe(action, slots, state)
            if action_result:
                lowered = action_result.lower()
                if "failed" in lowered or "error" in lowered:
                    recovery = call_llm(
                        state,
                        f"Action failed: {action_result}. Inform the user and suggest next steps.",
                    )
                    reply = recovery.get("reply", action_result)
                else:
                    reply = f"{reply}\n\n✅ {action_result}".strip()

        state.append_history("assistant", reply)

    except Exception:
        logger.exception("Bot error")
        reply = "Internal error. Please retry or use /reset."

    await update.message.reply_text(reply)


def _downloads_dir() -> Path:
    layout = _layout()
    downloads = layout.downloads_dir()
    downloads.mkdir(parents=True, exist_ok=True)
    return downloads


async def _route_pending_upload(state: ConversationState, document_name: str, tmp_path: Path) -> str:
    action = state.pending_action
    if not action or not state.site_code:
        return "⚠️ No active site. Use /newsite first."

    layout = _layout()
    layout.ensure_site_dirs(state.site_code)

    if action == "ingest_spec":
        dest_dir = layout.spec_inbox(state.site_code)
        slots = {"spec_version": (state.pending_data or {}).get("spec_version", "")}
        waiting_text = "Spec queued for ingest."
    else:
        dest_dir = layout.report_inbox(state.site_code)
        slots = {"report_code": (state.pending_data or {}).get("report_code", "")}
        waiting_text = "Report queued for ingest."

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / document_name
    tmp_path.replace(dest_path)

    slots["file_path"] = str(dest_path)
    result = await _run_action_safe(action, slots, state)
    _clear_pending(state)

    return f"📁 File routed to {dest_dir}.\n{waiting_text}\n{_format_action_result(result)}"


async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_allowed(_user_id(update)):
        await update.message.reply_text("Unauthorized.")
        return

    message = update.message
    document = message.document if message else None
    if not document or not document.file_name.lower().endswith(".pdf"):
        await message.reply_text("⚠️ Please upload a PDF file only.")
        return

    await message.reply_text(f"📄 Receiving file: {document.file_name}…")

    downloads = _downloads_dir()
    tmp_path = downloads / f"{_chat_id(update)}_{document.file_name}"

    telegram_file = await context.bot.get_file(document.file_id)
    await telegram_file.download_to_drive(str(tmp_path))

    state = ConversationState.load(_chat_id(update))
    state.append_history("user", f"[User uploaded PDF: {document.file_name}]")

    try:
        if state.pending_action in {"ingest_spec", "ingest_report"} and state.pending_step == "awaiting_upload":
            reply = await _route_pending_upload(state, document.file_name, tmp_path)
        else:
            reply = await _handle_pdf_via_llm(state, document.file_name, tmp_path)

        state.append_history("assistant", reply)
        state.save()

    except Exception as exc:
        logger.exception("PDF handler error")
        reply = f"❌ Error processing file: {exc}"

    await message.reply_text(reply)


async def _handle_pdf_via_llm(state: ConversationState, filename: str, tmp_path: Path) -> str:
    layout = _layout()
    layout.ensure_site_dirs(state.site_code or "default")

    llm_response = call_llm(
        state,
        f"User uploaded a PDF file named: {filename}. Based on the current state, where should this file go?",
    )
    reply = llm_response.get("reply", "File received.")
    action = llm_response.get("action", "none")
    slots = llm_response.get("slots") if isinstance(llm_response.get("slots"), dict) else {}

    routed = False
    if state.site_code:
        if action == "ingest_spec":
            dest_dir = layout.spec_inbox(state.site_code)
        elif action == "ingest_report":
            dest_dir = layout.report_inbox(state.site_code)
        else:
            dest_dir = None

        if dest_dir:
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / filename
            tmp_path.replace(dest_path)
            slots = dict(slots)
            slots.setdefault("spec_version", state.spec_version)
            slots.setdefault("report_code", state.report_code)
            slots["file_path"] = str(dest_path)
            action_result = await _run_action_safe(action, slots, state)
            reply += "\n\n📁 File routed to site INBOX."
            if action_result:
                reply += f"\n{action_result}"
            routed = True

    if not routed:
        fallback = layout.downloads_dir() / f"unrouted_{filename}"
        tmp_path.replace(fallback)
        reply += "\n\n⚠️ File saved to downloads. Please specify if this is a spec or report."

    return reply


def run_bot() -> None:
    token = os.environ.get("PETER_TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("PETER_TELEGRAM_TOKEN not set")

    logging.basicConfig(
        level=os.environ.get("PETER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    _ensure_event_loop()

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("newsite", new_site))
    app.add_handler(CommandHandler("addspec", add_spec))
    app.add_handler(CommandHandler("addreport", add_report))
    app.add_handler(CommandHandler("listreports", list_reports))
    app.add_handler(CommandHandler("askqa", ask_qa))
    app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("PETER bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    run_bot()
