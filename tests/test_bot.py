from __future__ import annotations

import pytest
from telethon import functions, types

from teledigest.telegram_client import set_bot_menu_commands


@pytest.mark.asyncio
async def test_set_bot_menu_commands_calls_telethon_correctly():
    # Fake Telethon client: calling it awaits our AsyncMock
    called = {}

    async def fake_client(request):
        called["request"] = request
        return True  # Telethon returns Bool for SetBotCommandsRequest[web:47]

    # Run
    await set_bot_menu_commands(fake_client)

    req = called["request"]
    assert isinstance(req, functions.bots.SetBotCommandsRequest)
    assert isinstance(req.scope, types.BotCommandScopeDefault)
    assert req.lang_code == "en"

    cmds = req.commands
    assert [c.command for c in cmds] == ["status", "today", "help", "auth"]
    assert cmds[0].description == "Check system status"
    assert cmds[1].description == "Request today's summary"
    assert cmds[2].description == "Get help info"
    assert cmds[3].description == "Set authentication"
