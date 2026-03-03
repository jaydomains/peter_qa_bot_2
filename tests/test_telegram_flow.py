from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest


@pytest.fixture()
def telegram_modules(tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "data"
    state_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PETER_TELEGRAM_STATE_DIR", str(state_dir))
    monkeypatch.setenv("PETER_DATA_DIR", str(data_dir))

    # Reload modules so they pick up the temp paths
    from peter.interfaces.telegram import state as state_mod

    importlib.reload(state_mod)

    from peter.interfaces.telegram import bot as bot_mod

    importlib.reload(bot_mod)

    return state_mod, bot_mod


@pytest.mark.asyncio
async def test_create_site_pending_flow(telegram_modules, monkeypatch):
    state_mod, bot = telegram_modules

    state = state_mod.ConversationState(chat_id=123)
    bot._begin_pending(state, "create_site", "site_code")

    reply = await bot._handle_pending_text(state, "PLA10OVQA")
    assert "site name" in reply.lower()
    assert state.pending_step == "site_name"

    reply = await bot._handle_pending_text(state, "TenOnV")
    assert "address" in reply.lower()
    assert state.pending_step == "address"

    async def fake_run(action, slots, the_state):
        assert action == "create_site"
        assert slots["site_code"] == "PLA10OVQA"
        assert slots["site_name"] == "TenOnV"
        assert slots["address"] == "Cape Town"
        return "Site created"

    monkeypatch.setattr(bot, "_run_action_safe", fake_run)

    reply = await bot._handle_pending_text(state, "Cape Town")
    assert "site created" in reply.lower()
    assert state.pending_action is None
    assert state.pending_step is None


@pytest.mark.asyncio
async def test_handle_message_skips_llm_when_pending(telegram_modules, monkeypatch):
    state_mod, bot = telegram_modules

    state = state_mod.ConversationState.load(999)
    state.pending_action = "create_site"
    state.pending_step = "site_name"
    state.pending_data = {"site_code": "PLA10OVQA"}
    state.save()

    def fake_call_llm(*args, **kwargs):
        raise AssertionError("call_llm should not be invoked when pending")

    monkeypatch.setattr(bot, "call_llm", fake_call_llm)

    class DummyMessage:
        def __init__(self, text):
            self.text = text
            self.replies: list[str] = []

        async def reply_text(self, text: str) -> None:
            self.replies.append(text)

    class DummyUpdate:
        def __init__(self, text: str):
            self.message = DummyMessage(text)
            self.effective_chat = SimpleNamespace(id=999)
            self.effective_user = SimpleNamespace(id=42)

    class DummyContext:
        pass

    update = DummyUpdate("TenOnV")
    await bot.handle_message(update, DummyContext())

    assert update.message.replies, "bot should have replied"
    assert "site address" in update.message.replies[-1].lower()
