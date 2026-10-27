"""Phase 0 golden contracts (plan: simplify-discord-mcp, Phase 0).

Captures two offline-reproducible behaviors as golden fixtures under
``tests/fixtures/`` and asserts the live code still matches them byte for
byte:

1. ``list_tools.json`` — the MCP tool surface from
   ``create_server(Settings(...))`` built offline with constructor-arg
   ``Settings`` (``_env_file=None`` + fake IDs; never real ``.env``
   secrets): tool names + ``inputSchema`` + descriptions, sorted by name.
   Fixed IDs mirror ``tests/test_server.py`` (owner 222 / guild 333 /
   channel 444); the owner UID is interpolated into several descriptions,
   so these exact IDs are part of the contract.
2. ``thyca_prompt_respond.txt`` / ``thyca_prompt_need_context.txt`` — the
   exact rendered text of ``thyca.build_conversation_prompt`` for the
   ``respond`` and ``need_context`` routings on one fixed conversation
   (data reused from ``tests/test_thyca_prompt.py``: guild 11 / channel 22
   / owner 33 / bot 44 / other 55, source message 701, job ``job-7-7-7``).
   The routing hash is ``sha256(b"golden-fixture-conversation")`` — a
   stable, obviously-synthetic 64-hex fingerprint, not a real ledger value.

Regeneration (deliberate updates only — review the diff afterwards)::

    THYCA_GOLDEN_UPDATE=1 uv run pytest tests/test_golden_contracts.py -q

Without the env var the tests only read and compare; they never write.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from config import Settings
from server import create_server
from thyca import build_conversation_prompt

FIXTURES = Path(__file__).resolve().parent / "fixtures"
LIST_TOOLS_FIXTURE = FIXTURES / "list_tools.json"
PROMPT_FIXTURES = {
    "respond": FIXTURES / "thyca_prompt_respond.txt",
    "need_context": FIXTURES / "thyca_prompt_need_context.txt",
}

# Fixed list_tools inputs (mirror tests/test_server.py fake IDs).
TOOL_OWNER_UID = 222
TOOL_GUILD_ID = 333
TOOL_CHANNEL_ID = 444

# Fixed prompt inputs (reused from tests/test_thyca_prompt.py).
GUILD_ID = 11
CHANNEL_ID = 22
OWNER_UID = 33
BOT_UID = 44
OTHER_UID = 55
SOURCE_MESSAGE_ID = 701
JOB_REF = "job-7-7-7"
CONTEXT_SHA256 = hashlib.sha256(b"golden-fixture-conversation").hexdigest()

REGENERATE = os.environ.get("THYCA_GOLDEN_UPDATE") == "1"


def _golden_settings(tmp_path: Path) -> Settings:
    """Offline-only Settings: constructor args beat env; no .env is read."""
    return Settings(
        _env_file=None,
        discord_token="test-token",
        owner_uid=TOOL_OWNER_UID,
        guild_id=TOOL_GUILD_ID,
        default_channel_id=TOOL_CHANNEL_ID,
        audit_path=tmp_path / "audit.jsonl",
        db_path=tmp_path / "state.db",
        watch_channel_ids=[TOOL_CHANNEL_ID],
    )


def _fixed_state() -> dict:
    return {
        "assistant_uid": str(BOT_UID),
        "owner_uid": str(OWNER_UID),
        "participants": [
            {"uid": str(OWNER_UID), "display_name": "Olivia"},
            {"uid": str(BOT_UID), "display_name": "HelperBot"},
            {"uid": str(OTHER_UID), "display_name": "Ramon"},
        ],
        "messages": [
            {"id": "601", "sender_uid": str(OWNER_UID), "text": "kickoff at noon"},
            {
                "id": "602",
                "sender_uid": str(BOT_UID),
                "text": "noted, noon kickoff",
                "reply_to": "601",
            },
            {
                "id": str(SOURCE_MESSAGE_ID),
                "sender_uid": str(OTHER_UID),
                "text": "hey all, what did we decide?",
                "reply_to": "602",
            },
        ],
        "new_message_id": str(SOURCE_MESSAGE_ID),
    }


def _fixed_entry() -> dict:
    return {
        "channel_id": CHANNEL_ID,
        "message_id": SOURCE_MESSAGE_ID,
        "author_uid": OTHER_UID,
        "content": "hey all, what did we decide?",
    }


def _render(participation: str) -> str:
    return build_conversation_prompt(
        _fixed_state(),
        _fixed_entry(),
        JOB_REF,
        owner_uid=OWNER_UID,
        guild_id=GUILD_ID,
        bot_uid=BOT_UID,
        participation=participation,
        context_sha256=CONTEXT_SHA256,
    )


async def test_list_tools_golden(tmp_path):
    """Offline tool surface (names + schemas + descriptions) is unchanged."""
    mcp = create_server(_golden_settings(tmp_path))
    actual = sorted(
        (
            {
                "name": tool.name,
                "description": tool.description or "",
                "inputSchema": tool.inputSchema,
            }
            for tool in await mcp.list_tools()
        ),
        key=lambda item: item["name"],
    )
    if REGENERATE:
        LIST_TOOLS_FIXTURE.write_text(
            json.dumps({"tools": actual}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return
    expected = json.loads(LIST_TOOLS_FIXTURE.read_text(encoding="utf-8"))
    assert actual == expected["tools"]


def test_prompt_golden_respond():
    """Routed ``respond`` render is byte-identical to its golden fixture."""
    prompt = _render("respond")
    assert prompt == prompt.strip()
    assert len(prompt) <= 4000
    assert "TASK ROUTED respond" in prompt
    if REGENERATE:
        PROMPT_FIXTURES["respond"].write_text(prompt, encoding="utf-8")
        return
    assert prompt == PROMPT_FIXTURES["respond"].read_text(encoding="utf-8")


def test_prompt_golden_need_context():
    """Routed ``need_context`` render is byte-identical to its fixture."""
    prompt = _render("need_context")
    assert prompt == prompt.strip()
    assert len(prompt) <= 4000
    assert "TASK ROUTED need_context" in prompt
    if REGENERATE:
        PROMPT_FIXTURES["need_context"].write_text(prompt, encoding="utf-8")
        return
    assert prompt == PROMPT_FIXTURES["need_context"].read_text(encoding="utf-8")
