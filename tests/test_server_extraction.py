"""T4 ownership tests: server_ticks factories + root fresh-audit wiring.

Covers the extracted background ticks (heartbeat, poll, auto-reply) at the
factory level with fake Discord objects and tmp-only settings, plus the root
late-patch proof: patching ``server.append_audit`` AFTER
``create_server`` still reroutes tick audits (the root passes a
fresh-resolving callable, never a stale binding or module namespace).

No network, no real .env/outbox/audit files (all paths are tmp fixtures with
_env_file=None and fake IDs/token). No existing tests are modified.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest

from config import Settings
from server import create_server
from server_ticks import (
    make_auto_reply_tick,
    make_heartbeat_tick,
    make_poll_tick,
)
from store import StoreError, get_poll_cursor, read_entries
from tasks import TaskRegistry

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
        self, mid, author, content="", channel_id=CHANNEL_ID, guild_id=GUILD_ID, reference=None
    ):
        self.id = mid
        self.author = author
        self.content = content
        self.channel = SimpleNamespace(id=channel_id)
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        scope = guild_id if guild_id is not None else "@me"
        self.jump_url = f"https://discord.com/channels/{scope}/{channel_id}/{mid}"
        self.reference = reference


class FakeGuild:
    def __init__(self, gid, channels=None, member_count=10):
        self.id = gid
        self._channels = dict(channels or {})
        self.member_count = member_count

    def get_channel(self, cid):
        return self._channels.get(cid)


class FakeBot:
    """Minimal bot surface: ready flag, guild lookup, event capture."""

    def __init__(self, guild, *, user=None, fetch_map=None):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self.events: list[str] = []
        self.listeners: dict[str, list] = {}
        self._closed = False
        self._ready = False
        self._setup = False  # mirrors Client._ready existing after _async_setup_hook
        # Mirrors the gateway READY dispatch: set only by a successful start.
        self._ready_event = asyncio.Event()
        self._close_event = asyncio.Event()

    def event(self, func):
        self.events.append("event")
        self.listeners.setdefault(func.__name__, []).append(func)
        return func

    async def start(self, token):  # noqa: ARG002 - signature mirrors discord.py
        self.events.append("start")
        self._setup = True  # mirrors _async_setup_hook at login() entry
        # Successful gateway connect dispatches READY, then stays open.
        self._ready_event.set()
        await self._close_event.wait()

    async def wait_until_ready(self):
        self.events.append("wait_ready")
        if not self._setup:
            # Mirrors the real Client: raises before login() creates _ready.
            raise RuntimeError(
                "Client has not been properly initialised. "
                "Please use the login method before calling this method"
            )
        await self._ready_event.wait()
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
        audit_path=tmp_path / "audit.jsonl",
        db_path=tmp_path / "state.db",
        watch_channel_ids=[CHANNEL_ID],
        responder_check_seconds=0.05,
        recovery_poll_seconds=30.0,
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
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
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
    data = read_entries(settings.db_path, unread_only=False)
    assert [e["message_id"] for e in data["entries"]] == [101, 102, 103]
    assert [e["actor_kind"] for e in data["entries"]] == ["self", "other", "owner"]
    assert get_poll_cursor(settings.db_path, CHANNEL_ID) == 103
    assert audits == []


async def test_poll_later_tick_pages_after_cursor(settings):
    from store import set_poll_cursor as _set

    _set(settings.db_path, CHANNEL_ID, 200)
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
    assert get_poll_cursor(settings.db_path, CHANNEL_ID) == 201
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
    assert get_poll_cursor(settings.db_path, CHANNEL_ID) is None


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
    assert not settings.db_path.exists()
    assert not settings.audit_path.exists()
    await tick()
    assert len(calls) == 1


# --- auto-reply factory -----------------------------------------------------


async def test_auto_success_no_audit(settings):
    responder = FakeResponder()
    sink, audits = _audit_spy()
    tick, on_error = make_auto_reply_tick(settings, responder, sink)
    assert callable(on_error)  # native Loop error handler, registered by the lifespan
    assert await tick() is None
    assert responder.ticks == 1
    assert audits == []


async def test_auto_failure_propagates_and_handler_audits_coarse(settings):
    class Failing:
        async def tick(self):
            raise StoreError("store exploded CANARY_A1")

    sink, audits = _audit_spy()
    tick, on_error = make_auto_reply_tick(settings, Failing(), sink)
    with pytest.raises(StoreError, match="store exploded"):
        await tick()  # propagates: the native Loop ends failed, never retries
    assert audits == []  # the tick itself never audits; the handler does
    await on_error(StoreError("store exploded CANARY_A1"))
    assert len(audits) == 1
    assert audits[0]["tool"] == "auto_reply_tick"
    assert audits[0]["ok"] is False
    assert audits[0]["args"] == {"error_kind": "delivery_error"}
    assert audits[0]["error"] == "auto_reply_tick failed kind=delivery_error"
    assert "CANARY_A1" not in str(audits)


async def test_auto_error_handler_dead_sink_signals_without_raise(settings, capsys):
    poison = "POISON_CANARY_B2"
    sink_canary = "SINK_CANARY_B2"

    class Failing:
        async def tick(self):
            raise RuntimeError(f"native poison {poison}")

    def _dead_sink(*args, **kwargs):  # noqa: ARG001 - signature mirrors append_audit
        raise OSError(f"audit sink dead {sink_canary}")

    tick, on_error = make_auto_reply_tick(settings, Failing(), _dead_sink)
    with pytest.raises(RuntimeError, match="native poison"):
        await tick()
    await on_error(RuntimeError(f"native poison {poison}"))  # must not raise
    err = capsys.readouterr().err
    assert err.count("thyca-discord-mcp: auto_reply_tick audit failed") == 1
    assert poison not in err
    assert sink_canary not in err


async def test_auto_cancel_propagates_without_audit(settings):
    class Cancelling:
        async def tick(self):
            raise asyncio.CancelledError

    sink, audits = _audit_spy()
    tick, _on_error = make_auto_reply_tick(settings, Cancelling(), sink)
    with pytest.raises(asyncio.CancelledError):
        await tick()
    assert audits == []


async def test_auto_factory_no_io_until_tick(settings):
    responder = FakeResponder()
    sink, audits = _audit_spy()
    tick, on_error = make_auto_reply_tick(settings, responder, sink)
    assert responder.ticks == 0
    assert audits == []
    assert callable(on_error)
    await tick()
    assert responder.ticks == 1


async def test_oserror_tick_failure_reaches_handler_and_stops_loop(settings):
    """Regression: OSError must reach @loop.error, not backoff-retry forever.

    Without ``loop.clear_exception_types()`` (lifespan wiring) the native Loop
    retries OSError/GatewayNotFound/aiohttp errors with backoff and never
    calls the error handler, breaking fail-closed stopping. With it, the
    failure audits coarse exactly once and the task ends failed.
    """

    class FailingOSError:
        async def tick(self):
            raise OSError("gateway reset CANARY_OS1")

    sink, audits = _audit_spy()
    tick, on_error = make_auto_reply_tick(settings, FailingOSError(), sink)
    registry = TaskRegistry()
    loop = registry.register("auto-reply", tick, seconds=60.0, protected=True)
    # Lifespan wiring under test: handler + cleared retry set.
    loop.error(on_error)
    loop.clear_exception_types()
    registry.start("auto-reply")
    try:
        for _ in range(100):
            if loop.failed():
                break
            await asyncio.sleep(0.05)
        assert loop.failed() is True
        assert registry.status("auto-reply").running is False
        assert len(audits) == 1  # exactly one handler call: no retry
        assert audits[0]["tool"] == "auto_reply_tick"
        assert audits[0]["args"] == {"error_kind": "unknown"}
        assert "CANARY_OS1" not in str(audits)
    finally:
        await registry.stop("auto-reply", allow_protected=True)


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

        # Auto via the parked worker with a failing tick.
        async def _boom():
            raise ValueError("late auto boom")

        ctx["responder"].tick = _boom
        loop = ctx["tasks"]._loops["auto-reply"]
        try:
            await loop.coro()
        except ValueError as exc:
            failure = exc
        else:
            pytest.fail("failing auto tick must propagate, never contain")
        # Native wiring: the Loop routes the failure to the registered handler.
        await loop._error(failure)
        assert {"tool": "auto_reply_tick", "ok": False} in late


async def test_root_registers_heartbeat_before_worker_loops(settings, monkeypatch):
    """Register order preserved: heartbeat first, workers only after ready."""
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        names = [t.name for t in ctx["tasks"].list_tasks()]
        assert names[0] == "heartbeat"
        assert "auto-reply" in names
        assert "auto-reply-recovery" in names
