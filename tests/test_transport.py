"""Transport-surface contract tests (v6): neutral tools over a fake gateway.

Covers the caller-agnostic Discord gateway path end to end with NO Thyca
HTTP, NO session mapping, and NO Responder: the real lifespan runs in pure
gateway mode (``thyca_session_mapping_path`` never created), and
``server.core.DiscordGateway`` is patched to a ``FakeGateway`` implementing
the same async surface (``is_ready``/``channel_name``/``history``/
``fetch_message``/``send``).

Also covers the golden tool surface (names + schemas + deprecated flags) and
the module-boundary rule: ``tools/`` and ``services/`` must not import the
caller-coupled subsystems (sessions/responder/thyca/delivery/participation/
context/conversation/jev) or the discord.py SDK itself.
"""

from __future__ import annotations

import ast
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from config import Settings
from outbox import append_entry
from server import create_server
from services import context_reader, message_sender, outbox_service

BOT_UID = 111
OWNER_UID = 222
GUILD_ID = 333
CHANNEL_ID = 444

SRC = Path(__file__).resolve().parent.parent / "src"


# --- FakeDiscordGateway -------------------------------------------------------


class _GWMessage:
    """In-memory stand-in for adapters.discord_gateway.GatewayMessage."""

    def __init__(self, mid, author_uid, author_name, content, ts, reply_to=None):
        self.id = mid
        self.channel_id = CHANNEL_ID
        self.author_id = author_uid
        self.author_name = author_name
        self.author_is_bot = author_uid == BOT_UID
        self.content = content
        self.timestamp = ts
        self.jump_url = f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/{mid}"
        self.reply_to_message_id = reply_to
        self.mentions = []

    def to_row(self):
        # Same shape as GatewayMessage.to_row.
        return {
            "message_id": self.id,
            "channel_id": self.channel_id,
            "author_uid": self.author_id,
            "author_name": self.author_name,
            "author_is_bot": self.author_is_bot,
            "content": self.content,
            "reply_to_message_id": self.reply_to_message_id,
            "timestamp": self.timestamp,
            "jump_url": self.jump_url,
        }


def _not_found(ident):
    from policy.scope_guard import ScopedNotFound

    return ScopedNotFound(f"discord object {ident} not in scope")


def _scope_denied(channel_id):
    from policy.scope_guard import ScopeDenied

    return ScopeDenied(f"refusing to send outside scoped guild: {channel_id}")


class FakeGateway:
    """Fake DiscordGateway: in-memory channel store, records send calls."""

    def __init__(self):
        self.ready = True
        self.channels: dict[int, dict] = {}
        self.send_calls: list[dict] = []
        self._next_id = 9000

    def is_ready(self) -> bool:
        return self.ready

    def add_channel(self, cid, name=None, messages=()):
        self.channels[cid] = {"name": name, "messages": list(messages)}

    async def channel_name(self, channel_id):
        if channel_id not in self.channels:
            raise _not_found(channel_id)
        return self.channels[channel_id]["name"]

    async def history(self, channel_id, *, before_message_id=None, limit=50):
        if channel_id not in self.channels:
            raise _not_found(channel_id)
        rows = sorted(self.channels[channel_id]["messages"], key=lambda m: m.id)
        if before_message_id is not None:
            rows = [m for m in rows if m.id < before_message_id]
        return rows[-limit:]

    async def fetch_message(self, channel_id, message_id):
        for m in self.channels.get(channel_id, {}).get("messages", []):
            if m.id == message_id:
                return m
        raise _not_found(message_id)

    async def send(self, channel_id, content, *, reply_to_message_id=None):
        if channel_id not in self.channels:
            raise _scope_denied(channel_id)
        call = {
            "channel_id": channel_id,
            "content": content,
            "reply_to_message_id": reply_to_message_id,
        }
        self.send_calls.append(call)
        self._next_id += 1
        msg = _GWMessage(
            self._next_id,
            BOT_UID,
            "GatewayBot",
            content,
            "2026-02-03T04:05:06+00:00",
            reply_to=reply_to_message_id,
        )
        self.channels[channel_id]["messages"].append(msg)
        return msg


# --- fixtures -----------------------------------------------------------------


def _settings_kwargs(tmp_path):
    """Pure-gateway fixture: NO mapping file, NO delivery journal."""
    return dict(
        _env_file=None,
        discord_token="test-token",
        owner_uid=OWNER_UID,
        guild_id=GUILD_ID,
        default_channel_id=CHANNEL_ID,
        session_id="discord-test",
        audit_path=tmp_path / "audit.jsonl",
        outbox_path=tmp_path / "outbox.jsonl",
        outbox_state_path=tmp_path / "outbox_state.json",
        watch_channel_ids=[CHANNEL_ID],
        thyca_session_mapping_path=tmp_path / "discord_session.json",
        delivery_state_path=tmp_path / "delivery_state.json",
    )


@pytest.fixture
def settings(tmp_path):
    return Settings(**_settings_kwargs(tmp_path))


class _FakeListenerBot:
    """Bare-minimum bot surface for the lifespan (listener + login + ready)."""

    def __init__(self):
        import asyncio

        self.user = SimpleNamespace(id=BOT_UID, display_name="GatewayBot", bot=True)
        self.listeners: dict[str, list] = {}
        self.events: list[str] = []
        self._closed = False
        self._ready = False
        self._close_event = asyncio.Event()

    def add_listener(self, func, name):
        self.events.append("add_listener")
        self.listeners.setdefault(name, []).append(func)

    def remove_listener(self, func, name):
        self.events.append("remove_listener")
        try:
            self.listeners.get(name, []).remove(func)
        except ValueError:
            pass

    async def login(self, token):
        self.events.append("login")

    async def connect(self):
        self.events.append("connect")
        await self._close_event.wait()

    async def wait_until_ready(self):
        self.events.append("wait_ready")
        self._ready = True

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
        self.events.append("close")
        self._closed = True
        self._close_event.set()

    def get_guild(self, gid):
        return None  # neutral-path tests never resolve real guild objects


@asynccontextmanager
async def running_transport(monkeypatch, settings, fake_gateway):
    """Real lifespan in PURE gateway mode + injected FakeGateway."""
    bot = _FakeListenerBot()
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr(
        "server.core.DiscordGateway", lambda bot_, s: fake_gateway
    )
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        yield mcp, ctx


async def call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def _mkmsg(mid, uid=OWNER_UID, name="Owner", content="hi", ts="2026-02-03T04:05:00+00:00"):
    return _GWMessage(mid, uid, name, content, ts)


# --- golden tool surface ------------------------------------------------------


GOLDEN_SURFACE = {
    # name: (deprecated, required_args, optional_args)
    "ack_outbox": (False, ["up_to_seq"], []),
    "add_poll_task": (False, ["name"], ["channel_id", "seconds"]),
    "get_message": (False, ["channel_id", "message_id"], []),
    "guild_info": (False, [], []),
    "list_members": (False, [], ["limit"]),
    "list_tasks": (False, [], []),
    "list_text_channels": (False, [], []),
    "read_context": (
        True,
        ["source_message_id", "job_ref"],
        ["channel_id", "limit", "before_message_id"],
    ),
    "read_messages": (False, ["channel_id"], ["before_message_id", "limit"]),
    "read_outbox": (False, [], ["limit", "unread_only"]),
    "record_no_reply": (True, ["source_message_id", "job_ref", "reason"], ["channel_id"]),
    "remove_poll_task": (False, ["name"], []),
    "resolve_uncertain_drop": (True, ["source_message_id"], ["channel_id"]),
    "send_message": (
        False,
        ["content"],
        ["channel_id", "source_message_id", "job_ref", "reply_to_message_id"],
    ),
    "start_task": (False, ["name"], []),
    "stop_task": (False, ["name"], []),
    "whoami": (False, ["uid"], []),
}


async def test_golden_tool_surface_names_and_schemas(settings):
    mcp = create_server(settings)
    tools = await mcp.list_tools()
    assert sorted(t.name for t in tools) == sorted(GOLDEN_SURFACE)
    for tool in tools:
        deprecated, required, optional = GOLDEN_SURFACE[tool.name]
        meta = tool.meta or {}
        assert bool(meta.get("deprecated")) is deprecated, tool.name
        schema = tool.inputSchema
        props = sorted(schema.get("properties", {}))
        assert sorted(schema.get("required", [])) == sorted(required), tool.name
        assert props == sorted(required + optional), tool.name
        desc = tool.description or ""
        if deprecated:
            assert "[DEPRECATED]" in desc, tool.name
        # send_message is live but documents its deprecated correlated branch.


async def test_golden_read_messages_schema_bounds(settings):
    mcp = create_server(settings)
    tools = {t.name: t for t in await mcp.list_tools()}
    schema = tools["read_messages"].inputSchema
    assert schema["properties"]["channel_id"]["type"] == "integer"
    assert schema["properties"]["limit"]["default"] == context_reader.READ_LIMIT_MAX


# --- send_message contract (fake gateway) --------------------------------------


async def test_send_message_contract_receipt_and_reply(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general", messages=[_mkmsg(700)])
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(
            mcp,
            "send_message",
            {"channel_id": CHANNEL_ID, "content": "hello", "reply_to_message_id": 700},
        )
        assert res["channel_id"] == CHANNEL_ID
        assert res["message_id"] == 9001
        assert res["reply_to_message_id"] == 700
        assert res["ts"] == "2026-02-03T04:05:06+00:00"
        assert res["jump_url"].endswith("/9001")
        (call,) = gateway.send_calls
        assert call["reply_to_message_id"] == 700
        assert call["content"] == "hello"


async def test_send_message_scope_denied_for_foreign_channel(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool(
                "send_message", {"channel_id": 999, "content": "nope"}
            )
        assert gateway.send_calls == []


async def test_send_message_rejects_invalid_args(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        from mcp.server.fastmcp.exceptions import ToolError

        for args in (
            {"channel_id": CHANNEL_ID, "content": ""},
            {"channel_id": CHANNEL_ID, "content": "x" * 2001},
            {"channel_id": CHANNEL_ID, "content": "x", "reply_to_message_id": 0},
        ):
            with pytest.raises(ToolError):
                await mcp.call_tool("send_message", args)
        assert gateway.send_calls == []


async def test_send_message_service_level_receipt(settings):
    """The service layer over a bare FakeGateway — no server, no Thyca."""
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    res = await message_sender.send_message(
        gateway, channel_id=CHANNEL_ID, content="direct", reply_to_message_id=None
    )
    assert res == {
        "message_id": 9001,
        "channel_id": CHANNEL_ID,
        "jump_url": f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/9001",
        "ts": "2026-02-03T04:05:06+00:00",
        "reply_to_message_id": None,
    }


# --- read_messages / get_message contract -------------------------------------


async def test_read_messages_envelope_markdown_and_rows(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(
        CHANNEL_ID,
        name="general",
        messages=[
            _mkmsg(10, OWNER_UID, "Owner", "first"),
            _mkmsg(11, BOT_UID, "GatewayBot", "second", "2026-02-03T04:05:01+00:00"),
            _mkmsg(12, 555, "Rando", "# not a heading", "2026-02-03T04:05:02+00:00"),
        ],
    )
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(mcp, "read_messages", {"channel_id": CHANNEL_ID})
        assert res["ok"] is True
        data = res["data"]
        assert data["count"] == 3 and data["channel_name"] == "general"
        assert [m["message_id"] for m in data["messages"]] == [10, 11, 12]
        md = data["markdown"]
        assert md.startswith("# #general · 3 messages (oldest → newest)")
        assert "**Owner** (`222`)" in md
        assert "msg `10`" in md and "2026-02-03 04:05 UTC" in md
        # Heading-injection neutralized: user '# ...' is escaped in the body.
        assert "\n\\# not a heading\n" in md


async def test_read_messages_pagination_and_bounds(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(
        CHANNEL_ID,
        name="general",
        messages=[_mkmsg(m) for m in (1, 2, 3, 4, 5)],
    )
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(
            mcp,
            "read_messages",
            {"channel_id": CHANNEL_ID, "before_message_id": 4, "limit": 2},
        )
        assert res["ok"] is True
        assert [m["message_id"] for m in res["data"]["messages"]] == [2, 3]
        res = await call_json(mcp, "read_messages", {"channel_id": CHANNEL_ID, "limit": 51})
        assert res["ok"] is False and res["error"]["code"] == "validation"
        res = await call_json(mcp, "read_messages", {"channel_id": CHANNEL_ID, "limit": 0})
        assert res["ok"] is False and res["error"]["code"] == "validation"
        # bool is not an int: rejected at the StrictInt boundary (ToolError).
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError):
            await mcp.call_tool(
                "read_messages", {"channel_id": CHANNEL_ID, "limit": True}
            )


async def test_read_messages_scope_denied_envelope(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(mcp, "read_messages", {"channel_id": 999})
        assert res["ok"] is False
        assert res["error"]["code"] == "not_found"
        assert res["ok"] is False and "token" not in res["error"]["message"].lower()


async def test_get_message_envelope(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(
        CHANNEL_ID,
        name="general",
        messages=[_mkmsg(77, OWNER_UID, "Owner", "answer me")],
    )
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(
            mcp, "get_message", {"channel_id": CHANNEL_ID, "message_id": 77}
        )
        assert res["ok"] is True
        assert res["data"]["message"]["content"] == "answer me"
        assert "answer me" in res["data"]["markdown"]
        res = await call_json(
            mcp, "get_message", {"channel_id": CHANNEL_ID, "message_id": 999}
        )
        assert res["ok"] is False and res["error"]["code"] == "not_found"


async def test_neutral_tools_audit_without_content_or_token(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general", messages=[_mkmsg(5)])
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        await call_json(mcp, "read_messages", {"channel_id": CHANNEL_ID})
        await call_json(
            mcp, "send_message", {"channel_id": CHANNEL_ID, "content": "S3CR3T_BODY"}
        )
        await call_json(mcp, "get_message", {"channel_id": CHANNEL_ID, "message_id": 5})
    text = settings.audit_path.read_text(encoding="utf-8")
    recs = [json.loads(line) for line in text.splitlines() if line.strip()]
    assert {r["tool"] for r in recs} >= {"read_messages", "send_message", "get_message"}
    assert all(r["ok"] is True for r in recs)
    assert "S3CR3T_BODY" not in text  # metadata-only audits, never raw content
    assert "test-token" not in text


async def test_pure_gateway_lifespan_has_no_responder_no_session_io(settings, monkeypatch):
    """v6: lifespan never builds the Responder and never touches Thyca state."""
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    async with running_transport(monkeypatch, settings, gateway) as (_mcp, ctx):
        assert ctx["responder"] is None
        assert {t.name for t in ctx["tasks"].list_tasks()} == {"heartbeat"}
        assert not settings.thyca_session_mapping_path.exists()
        assert not settings.delivery_state_path.exists()


# --- outbox contract -----------------------------------------------------------


async def test_read_outbox_returns_markdown_with_seq(settings, monkeypatch):
    gateway = FakeGateway()
    gateway.add_channel(CHANNEL_ID, name="general")
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="realtime",
        channel_id=CHANNEL_ID,
        message_id=42,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content="captured hello",
        author_is_bot=False,
        reply_to_message_id=41,
    )
    async with running_transport(monkeypatch, settings, gateway) as (mcp, _ctx):
        res = await call_json(mcp, "read_outbox", {})
        assert res["acked"] == 0 and res["latest"] == 1
        (entry,) = res["entries"]
        assert entry["message_id"] == 42 and entry["content"] == "captured hello"
        md = res["markdown"]
        assert md.startswith("# outbox · 1 entries")
        assert "captured hello" in md and "seq `1`" in md and "· owner" in md
        assert "reply-to `41`" in md
        # Pure-seq ack: no responder guard.
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 1}) == {"acked": 1}
        res = await call_json(mcp, "read_outbox", {})
        assert res["entries"] == [] and res["acked"] == 1


def test_outbox_service_level_read_and_ack(settings, tmp_path):
    """Service level: no gateway, no Thyca — plain files + pure seq cursor."""
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="realtime",
        channel_id=CHANNEL_ID,
        message_id=7,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content="body",
        author_is_bot=False,
        reply_to_message_id=None,
    )
    result = outbox_service.read_outbox(
        settings.outbox_path, settings.outbox_state_path
    )
    assert result["entries"][0]["message_id"] == 7
    assert "body" in result["markdown"]
    assert outbox_service.ack_outbox(
        settings.outbox_path, settings.outbox_state_path, up_to_seq=1
    ) == {"acked": 1}


# --- module boundary ------------------------------------------------------------


FORBIDDEN_MODULES = {
    "sessions",
    "responder",
    "thyca",
    "delivery",
    "delivery_schema",
    "delivery_storage",
    "participation",
    "context",
    "conversation",
    "jev",
    "instance_lock",
    "discord",  # SDK detail stays inside the adapter
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module.split(".")[0])
    return mods


def test_tools_and_services_never_import_coupled_modules():
    """The neutral path must not import sessions/responder/thyca/delivery/etc.

    Direct-import check (transitive config->sessions via Settings is allowed —
    Settings is the shared config object; the rule targets the caller-coupled
    subsystems themselves).
    """
    offenders: list[str] = []
    for pkg in ("tools", "services"):
        for path in sorted((SRC / pkg).glob("*.py")):
            if path.name == "__init__.py":
                continue
            bad = _imported_modules(path) & FORBIDDEN_MODULES
            if bad:
                offenders.append(f"{pkg}/{path.name}: {sorted(bad)}")
            # job_ref must not appear as a code identifier (docstrings may
            # document its absence) — check AST args/names, not raw text.
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.arg) and node.arg == "job_ref":
                    offenders.append(f"{pkg}/{path.name}: job_ref param")
                if isinstance(node, ast.Name) and node.id == "job_ref":
                    offenders.append(f"{pkg}/{path.name}: job_ref name")
    assert offenders == []


def test_adapter_is_the_only_discord_import_in_new_path():
    """discord.py usage must stay inside adapters/ + the legacy surface."""
    for pkg in ("tools", "services"):
        for path in sorted((SRC / pkg).glob("*.py")):
            if path.name == "__init__.py":
                continue
            assert "discord" not in _imported_modules(path), path
