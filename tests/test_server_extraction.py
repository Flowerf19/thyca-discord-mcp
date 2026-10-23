"""T4 ownership tests: server_ticks factories + root fresh-audit wiring.

Covers the extracted background ticks (heartbeat, poll) at the factory level
with fake Discord objects and tmp-only settings, plus the root late-patch
proof: patching ``server.append_audit`` AFTER ``create_server`` still
reroutes tick audits (the root passes a fresh-resolving callable, never a
stale binding or module namespace).

No network, no real .env/outbox/audit files (all paths are tmp fixtures with
_env_file=None and fake IDs/token).
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest

from config import Settings
from outbox import get_poll_cursor, read_entries
from server import create_server
from server_ticks import (
    make_heartbeat_tick,
    make_poll_tick,
)

BOT_UID = 111
OWNER_UID = 222
GUILD_ID = 333
CHANNEL_ID = 444


class FakeUser:
    def __init__(self, uid, display_name, bot=False):
        self.id = uid
        self.display_name = display_name
        self.bot = bot


class FakeMessage:
    def __init__(
        self, mid, author, content="", channel_id=CHANNEL_ID, guild_id=GUILD_ID,
        reference=None, created_at=None,
    ):
        self.id = mid
        self.author = author
        self.content = content
        self.channel = SimpleNamespace(id=channel_id)
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        scope = guild_id if guild_id is not None else "@me"
        self.jump_url = f"https://discord.com/channels/{scope}/{channel_id}/{mid}"
        self.reference = reference
        # SDK messages always carry a tz-aware creation timestamp (UTC).
        self.created_at = created_at or datetime.now(timezone.utc)


class FakeGuild:
    def __init__(self, gid, channels=None, member_count=10):
        self.id = gid
        self._channels = dict(channels or {})
        self.member_count = member_count

    def get_channel(self, cid):
        return self._channels.get(cid)


class FakeBot:
    """Minimal bot surface: ready flag, guild lookup, listener no-ops."""

    def __init__(self, guild, *, user=None, fetch_map=None):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self.events: list[str] = []
        self.listeners: dict[str, list] = {}
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

    async def login(self, token):  # noqa: ARG002 - signature mirrors discord.py
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
        if self._guild is not None and self._guild.id == gid:
            return self._guild
        return None

    async def fetch_channel(self, cid):
        self.events.append(f"fetch_channel:{cid}")
        try:
            return self._fetch_map[cid]
        except KeyError:
            raise LookupError(f"channel {cid} not found") from None


def make_history(messages=(), exc=None):
    """Fake TextChannel.history: records kwargs, yields configured messages."""
    calls: list[dict] = []
    state = {"messages": list(messages), "exc": exc}

    def history(**kwargs):
        calls.append(kwargs)

        async def gen():
            if state["exc"] is not None:
                raise state["exc"]
            for m in state["messages"]:
                yield m

        return gen()

    return history, calls, state


def make_text_channel(cid, guild_id=GUILD_ID, history=None):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    channel.history = history or make_history()[0]
    return channel


class FakeResponder:
    def __init__(self, settings=None, bot=None):
        self.settings = settings
        self.bot = bot
        self.ticks = 0
        self.closed = False

    async def tick(self):
        self.ticks += 1
        return {"status": "idle"}

    async def close(self):
        self.closed = True

    def guard_ack(self, up_to_seq):  # noqa: ARG002 - signature mirrors Responder
        return None

    def correlated_job(self, source_message_id, job_ref, channel_id):
        raise AssertionError("extraction isolation: no correlated sends here")


def _settings_kwargs(tmp_path):
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
        responder_check_seconds=0.05,
        recovery_poll_seconds=30.0,
        thyca_session_mapping_path=tmp_path / "discord_session.json",
        delivery_state_path=tmp_path / "delivery_state.json",
    )


@pytest.fixture
def settings(tmp_path):
    return Settings(**_settings_kwargs(tmp_path))


def _audit_spy():
    calls: list[dict] = []

    def sink(path, **kwargs):
        calls.append({"path": path, **kwargs})
        return {"ok": kwargs.get("ok", True)}

    return sink, calls


def read_audit(settings):
    if not settings.audit_path.exists():
        return []
    lines = settings.audit_path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


@asynccontextmanager
async def running_server(monkeypatch, settings, bot):
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    # v6: the deprecated Responder is built best-effort only when the Thyca
    # session mapping file exists; touch it so the fake is constructed.
    settings.thyca_session_mapping_path.touch()
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        # The deprecated auto loops are never registered; nothing to park.
        yield mcp, ctx, bot


# --- heartbeat factory ------------------------------------------------------


async def test_heartbeat_none_returns_quietly(settings):
    sink, calls = _audit_spy()
    tick = make_heartbeat_tick(settings, lambda: None, sink)
    assert await tick() is None
    assert calls == []


async def test_heartbeat_success_audits_members(settings):
    guild = FakeGuild(GUILD_ID, member_count=42)
    bot = FakeBot(guild)
    sink, calls = _audit_spy()
    tick = make_heartbeat_tick(settings, lambda: bot, sink)
    assert await tick() is None
    assert len(calls) == 1
    assert calls[0]["tool"] == "heartbeat"
    assert calls[0]["ok"] is True
    assert calls[0]["args"] == {"members": 42}
    assert calls[0]["actor"] == "system"


async def test_heartbeat_failure_audits_repr_and_survives(settings):
    bot = FakeBot(FakeGuild(999))  # scoped guild missing -> LookupError
    sink, calls = _audit_spy()
    tick = make_heartbeat_tick(settings, lambda: bot, sink)
    assert await tick() is None  # recorded, not raised; loop survives
    assert len(calls) == 1
    assert calls[0]["tool"] == "heartbeat"
    assert calls[0]["ok"] is False
    assert calls[0]["args"] == {}
    assert "333" in calls[0]["error"]  # same error-repr policy as before


async def test_heartbeat_factory_no_io_until_tick(settings, tmp_path):
    bot = FakeBot(FakeGuild(GUILD_ID, member_count=7))
    sink, calls = _audit_spy()
    tick = make_heartbeat_tick(settings, lambda: bot, sink)
    assert calls == []
    assert not settings.audit_path.exists()
    assert callable(tick)
    await tick()
    assert len(calls) == 1


# --- poll factory -----------------------------------------------------------


def _ready_bot(guild):
    bot = FakeBot(guild)
    bot._ready = True
    return bot


async def test_poll_first_tick_captures_newest20_reversed(settings):
    # History yields newest-first; the tick reverses to append old->new.
    msgs = [
        FakeMessage(103, FakeUser(OWNER_UID, "Owner"), "c3"),
        FakeMessage(102, FakeUser(999, "Rando"), "c2"),
        FakeMessage(101, FakeUser(BOT_UID, "ThycaBot", bot=True), "self-kept"),
    ]
    history, calls, _ = make_history(msgs)
    channel = make_text_channel(CHANNEL_ID, history=history)
    bot = _ready_bot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    sink, audits = _audit_spy()
    tick = make_poll_tick(settings, CHANNEL_ID, lambda: bot, sink)
    assert await tick() is None
    assert calls[0] == {"limit": 20, "oldest_first": False}
    data = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
    assert [e["message_id"] for e in data["entries"]] == [101, 102, 103]
    assert [e["actor_kind"] for e in data["entries"]] == ["self", "other", "owner"]
    assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 103
    assert audits == []


async def test_poll_later_tick_pages_after_cursor(settings):
    from outbox import set_poll_cursor as _set

    _set(settings.outbox_state_path, CHANNEL_ID, 200)
    msgs = [FakeMessage(201, FakeUser(OWNER_UID, "Owner"), "next")]
    history, calls, _ = make_history(msgs)
    channel = make_text_channel(CHANNEL_ID, history=history)
    bot = _ready_bot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    sink, audits = _audit_spy()
    tick = make_poll_tick(settings, CHANNEL_ID, lambda: bot, sink)
    await tick()
    assert calls[0]["limit"] == 100
    assert calls[0]["oldest_first"] is True
    assert calls[0]["after"].id == 200
    assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 201
    assert audits == []


async def test_poll_error_audits_and_leaves_cursor(settings):
    history, _, _ = make_history(exc=RuntimeError("history boom"))
    channel = make_text_channel(CHANNEL_ID, history=history)
    bot = _ready_bot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    sink, audits = _audit_spy()
    tick = make_poll_tick(settings, CHANNEL_ID, lambda: bot, sink)
    assert await tick() is None  # contained, loop survives
    assert len(audits) == 1
    assert audits[0]["tool"] == "poll_tick"
    assert audits[0]["ok"] is False
    assert audits[0]["args"] == {"channel_id": CHANNEL_ID}
    assert "history boom" in audits[0]["error"]
    assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) is None


async def test_poll_cancel_propagates_without_audit(settings):
    def _cancelled():
        raise asyncio.CancelledError

    sink, audits = _audit_spy()
    tick = make_poll_tick(settings, CHANNEL_ID, _cancelled, sink)
    with pytest.raises(asyncio.CancelledError):
        await tick()
    assert audits == []


async def test_poll_factory_no_io_until_tick(settings):
    history, calls, _ = make_history([FakeMessage(1, FakeUser(OWNER_UID, "Owner"), "x")])
    channel = make_text_channel(CHANNEL_ID, history=history)
    bot = _ready_bot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    sink, audits = _audit_spy()
    tick = make_poll_tick(settings, CHANNEL_ID, lambda: bot, sink)
    assert calls == []
    assert audits == []
    assert not settings.outbox_path.exists()
    assert not settings.audit_path.exists()
    await tick()
    assert len(calls) == 1


# --- root
# --- root late-patch proof ----------------------------------------------------


async def test_root_late_audit_patch_reroutes_all_ticks(settings, monkeypatch):
    """Patching server.append_audit AFTER create_server still captures tick audits."""
    history, _, state = make_history([FakeMessage(501, FakeUser(OWNER_UID, "Owner"), "m")])
    channel = make_text_channel(CHANNEL_ID, history=history)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        late: list[dict] = []

        def _late_sink(path, **kwargs):
            late.append({"tool": kwargs.get("tool"), "ok": kwargs.get("ok")})
            return {"tool": kwargs.get("tool")}

        # Late patch: after create_server + lifespan + tick construction.
        monkeypatch.setattr("server.core.append_audit", _late_sink)

        # Heartbeat via the registered native loop coro.
        await ctx["tasks"]._loops["heartbeat"].coro()
        assert {"tool": "heartbeat", "ok": True} in late

        # Poll via a tool-registered task whose history now fails.
        res = await mcp.call_tool("add_poll_task", {"name": "late-poll"})
        assert res is not None
        await ctx["tasks"].stop("late-poll")
        state["exc"] = RuntimeError("late poll boom")
        await ctx["tasks"]._loops["late-poll"].coro()
        assert {"tool": "poll_tick", "ok": False} in late

        # v6: the deprecated auto-reply tick is never registered by the
        # lifespan; its factory-level audit/sink coverage stands above.


async def test_root_registers_heartbeat_before_worker_loops(settings, monkeypatch):
    """Register order preserved: heartbeat only, worker loops retired."""
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        names = [t.name for t in ctx["tasks"].list_tasks()]
        assert names == ["heartbeat"]
