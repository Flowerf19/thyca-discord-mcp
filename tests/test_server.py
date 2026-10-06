"""Mock-based integration tests for server wiring (phase 2, v4 outbox).

Uses the native FastMCP public API (``list_tools``/``call_tool``) and the real
installed lifespan (``mcp.settings.lifespan``) with a fake Discord bot — no
network, no real .env/outbox/audit files (all paths are tmp fixtures).

v5 isolation: this module covers the base 14-tool MCP protocol only. The
``running_server`` helper (and every direct-lifespan test) patches
``server.Responder`` with ``FakeResponder`` and parks the
always-on auto loops before their first tick, so no worker logic runs here.
``FakeResponder.guard_ack`` is a deliberate no-op: these tests isolate the
underlying ack monotonic API only — the mandatory production guard is proven
with the real Responder in tests/test_server_responder.py. The patch lives
only in this module (never a global autouse fixture) so it cannot hide the
real worker elsewhere.
"""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from config import Settings
from outbox import (
    OutboxError,
    append_entry,
    get_poll_cursor,
    read_entries,
    set_poll_cursor,
)
from server import create_server

BOT_UID = 111
OWNER_UID = 222
GUILD_ID = 333
CHANNEL_ID = 444

EXPECTED_TOOLS = [
    "ack_outbox",
    "add_poll_task",
    "guild_info",
    "list_members",
    "list_tasks",
    "list_text_channels",
    "read_context",
    "read_outbox",
    "record_no_reply",
    "remove_poll_task",
    "resolve_uncertain_drop",
    "send_message",
    "start_task",
    "stop_task",
    "whoami",
]

# Pre-routing tools: every one of the old 12 must remain with compatible
# behavior; the 3 additions (read_context, record_no_reply,
# resolve_uncertain_drop) are covered in
# tests/test_observation_server.py.
EXPECTED_LEGACY_TOOLS = [
    "ack_outbox",
    "add_poll_task",
    "guild_info",
    "list_members",
    "list_tasks",
    "list_text_channels",
    "read_outbox",
    "remove_poll_task",
    "send_message",
    "start_task",
    "stop_task",
    "whoami",
]


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
        # Native SDK jump-URL shape (verified discord.py format); DMs use @me.
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
    """Minimal bot surface used by server.py: login/connect/ready/channels/listeners."""

    def __init__(
        self,
        guild,
        *,
        user=None,
        fetch_map=None,
        connect_exc=None,
        login_exc=None,
        ready_exc=None,
        close_exc=None,
    ):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self._connect_exc = connect_exc
        self._login_exc = login_exc
        self._ready_exc = ready_exc
        self._close_exc = close_exc
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
        if self._login_exc is not None:
            raise self._login_exc

    async def connect(self):
        self.events.append("connect")
        if self._connect_exc is not None:
            raise self._connect_exc
        await self._close_event.wait()

    async def wait_until_ready(self):
        self.events.append("wait_ready")
        if self._ready_exc is not None:
            raise self._ready_exc
        self._ready = True

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
        self.events.append("close")
        self._closed = True
        self._close_event.set()
        # Raised after marking closed, like real Client.close failing late
        # (e.g. in http.close) after _closed was already set.
        if self._close_exc is not None:
            raise self._close_exc

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


def _owner():
    return FakeUser(OWNER_UID, "Owner")


def _other(uid=999, name="Rando"):
    return FakeUser(uid, name)


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


def _deleted_not_found():
    """Real strict discord.NotFound (404/10008 Unknown Message), no network."""
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def make_text_channel(cid, guild_id=GUILD_ID, history=None):
    """Spec'd TextChannel mock: passes isinstance, records sends, proves reads."""
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    sent: list[FakeMessage] = []
    store: dict = {"deleted": set(), "exc": None, "calls": []}

    async def fake_send(content, **kwargs):
        msg = FakeMessage(
            9000 + len(sent),
            FakeUser(BOT_UID, "ThycaBot", bot=True),
            content,
            channel_id=cid,
            guild_id=guild_id,
            reference=kwargs.get("reference"),
        )
        sent.append(msg)
        return msg

    async def fake_fetch(mid):
        # READ support for the pre-send existence proof (ID/scope only).
        store["calls"].append(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_not_found()
        return FakeMessage(
            mid,
            FakeUser(OWNER_UID, "Owner"),
            "",
            channel_id=cid,
            guild_id=guild_id,
        )

    channel.send = fake_send
    channel.fetch_message = fake_fetch
    channel.history = history or make_history()[0]
    channel.sent = sent
    channel.source_store = store
    return channel


class FakeResponder:
    """No-op worker stand-in: keeps v4 MCP-protocol tests isolated (see module doc)."""

    def __init__(self, settings, bot):
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
        return None  # no-op: v4 file isolates the underlying ack monotonic API only

    def correlated_job(self, source_message_id, job_ref, channel_id):
        raise AssertionError("v4 isolation: correlated send is covered in test_server_responder")


class FailingCloseResponder(FakeResponder):
    """FakeResponder whose close() fails: cleanup-failure shutdown coverage."""

    async def close(self):
        raise RuntimeError("responder close boom")


class RefusingGuardResponder(FakeResponder):
    """FakeResponder whose guard always refuses: proves responder state clears."""

    def guard_ack(self, up_to_seq):  # noqa: ARG002 - signature mirrors Responder
        raise ValueError("ack beyond safe prefix")


class FailingRemoveListenerBot(FakeBot):
    """FakeBot whose remove_listener fails: teardown-isolation coverage."""

    def remove_listener(self, func, name):
        self.events.append("remove_listener")
        raise RuntimeError("remove boom")


def _settings_kwargs(tmp_path):
    """Fixture kwargs: tmp/fake-only, immune to ambient .env and env vars."""
    return dict(
        # No real dotenv/config reads: constructor pins everything the suite
        # depends on (init args beat env, and _env_file=None disables .env).
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
        responder_check_seconds=1.0,
        recovery_poll_seconds=30.0,
        # v5 worker paths stay tmp-only even though FakeResponder never reads them.
        thyca_session_mapping_path=tmp_path / "discord_session.json",
        delivery_state_path=tmp_path / "delivery_state.json",
    )


def _make_settings(tmp_path, **overrides):
    kwargs = _settings_kwargs(tmp_path)
    kwargs.update(overrides)
    return Settings(**kwargs)


@pytest.fixture
def settings(tmp_path):
    return _make_settings(tmp_path)


@asynccontextmanager
async def running_server(monkeypatch, settings, bot):
    """Enter the real installed lifespan with build_bot + Responder patched to fakes.

    The always-on auto loops are parked immediately after enter: there is no
    await between their start() calls inside the lifespan and these stops, so
    their first tick can never run. v4 protocol assertions stay race-free;
    default-on auto behavior is proven in tests/test_server_responder.py.
    """
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx, bot


async def call_json(mcp, name, args=None):
    """Call a tool via native FastMCP API and parse its JSON-string payload."""
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def read_audit(settings):
    if not settings.audit_path.exists():
        return []
    lines = settings.audit_path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def find_audit(records, tool, ok):
    return [r for r in records if r["tool"] == tool and r["ok"] is ok]


async def add_and_hold(mcp, ctx, name, **kwargs):
    """Add a poll task via the tool, then stop its loop for manual-only ticks.

    History must be empty at setup: the loop's immediate first tick then either
    runs as a no-op or is cancelled before running — the outbox/cursor stay
    clean either way. Returns (status, tick_coro). The registry attribute path
    is read from tasks.py (TaskRegistry._loops) and Loop.coro is public.
    """
    status = await call_json(mcp, "add_poll_task", {"name": name, **kwargs})
    registry = ctx["tasks"]
    await registry.stop(name)
    return status, registry._loops[name].coro


def seed_outbox(settings, *mids):
    for mid in mids:
        append_entry(
            settings.outbox_path,
            settings.outbox_state_path,
            type="message",
            channel_id=CHANNEL_ID,
            message_id=mid,
            author_uid=OWNER_UID,
            author_name="Owner",
            actor_kind="owner",
            content=f"m{mid}",
        )


# --- tool surface -----------------------------------------------------------


async def test_tool_count_and_names(settings):
    mcp = create_server(settings)
    tools = await mcp.list_tools()
    assert sorted(t.name for t in tools) == EXPECTED_TOOLS


async def test_new_tool_descriptions_carry_essentials(settings):
    mcp = create_server(settings)
    descs = {t.name: t.description or "" for t in await mcp.list_tools()}
    assert "30" in descs["add_poll_task"]  # minimum interval, no push/summary wording
    assert "no push" in descs["add_poll_task"].lower()
    assert "heartbeat" in descs["remove_poll_task"]
    assert "message_id" in descs["read_outbox"]  # dedup key
    assert "USER.md" in descs["read_outbox"]  # profile rule
    assert "latest" in descs["ack_outbox"]  # beyond-latest rejection


async def test_identity_policy_visible_in_tool_descriptions(settings):
    mcp = create_server(settings)
    descs = {t.name: t.description or "" for t in await mcp.list_tools()}
    assert sorted(descs) == EXPECTED_TOOLS  # 15 tools: old 12 intact + 3 additions
    for name in ("whoami", "read_outbox", "send_message"):
        desc = descs[name]
        assert str(settings.owner_uid) in desc  # parameterized owner UID (222 in tests)
        assert "author_uid" in desc  # metadata-only authentication requirement
        assert "NEVER" in desc  # display-name/content-claim rejection
        assert "USER.md" in desc
        assert "does NOT mean" in desc  # auto-loaded profile does not auth the sender


# --- add/remove poll tasks: channel guards ----------------------------------


async def test_add_poll_task_default_channel(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        status = await call_json(mcp, "add_poll_task", {"name": "poll-main"})
        assert status["name"] == "poll-main"
        assert status["running"] is True
        assert status["interval_seconds"] == 60.0
        names = {t["name"] for t in await call_json(mcp, "list_tasks")}
        # v5: the two protected auto loops are always registered by default.
        assert names == {"heartbeat", "auto-reply", "auto-reply-recovery", "poll-main"}
        assert await call_json(mcp, "remove_poll_task", {"name": "poll-main"}) == {
            "removed": "poll-main"
        }
    assert find_audit(read_audit(settings), "add_poll_task", True)
    assert find_audit(read_audit(settings), "remove_poll_task", True)


async def test_add_poll_task_explicit_channel_fetch_fallback(settings, monkeypatch):
    channel = make_text_channel(555)
    bot = FakeBot(FakeGuild(GUILD_ID, {}), fetch_map={555: channel})  # cache miss
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        status = await call_json(mcp, "add_poll_task", {"name": "p", "channel_id": 555})
        assert status["running"] is True
    assert "fetch_channel:555" in bot.events
    assert find_audit(read_audit(settings), "add_poll_task", True)


async def test_add_poll_task_rejects_dm(settings, monkeypatch):
    dm = MagicMock(spec=discord.DMChannel)
    dm.id = 777
    bot = FakeBot(FakeGuild(GUILD_ID, {777: dm}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("add_poll_task", {"name": "p", "channel_id": 777})
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_add_poll_task_rejects_nontext(settings, monkeypatch):
    voice = MagicMock(spec=discord.VoiceChannel)
    voice.id = 778
    voice.guild = SimpleNamespace(id=GUILD_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {778: voice}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("add_poll_task", {"name": "p", "channel_id": 778})
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_add_poll_task_rejects_out_of_guild(settings, monkeypatch):
    evil = make_text_channel(666, guild_id=999)
    bot = FakeBot(FakeGuild(GUILD_ID, {666: evil}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("add_poll_task", {"name": "p", "channel_id": 666})
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_add_poll_task_rejects_dm_via_fetch_fallback(settings, monkeypatch):
    dm = MagicMock(spec=discord.DMChannel)
    dm.id = 777
    bot = FakeBot(FakeGuild(GUILD_ID, {}), fetch_map={777: dm})
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("add_poll_task", {"name": "p", "channel_id": 777})
    assert find_audit(read_audit(settings), "add_poll_task", False)


# --- add/remove poll tasks: names and intervals ------------------------------


@pytest.mark.parametrize("bad", ["", "   "])
async def test_add_poll_task_rejects_blank_name(settings, monkeypatch, bad):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="non-empty"):
            await mcp.call_tool("add_poll_task", {"name": bad})
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_add_poll_task_rejects_heartbeat_name(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="reserved"):
            await mcp.call_tool("add_poll_task", {"name": "heartbeat"})
    assert find_audit(read_audit(settings), "add_poll_task", False)


@pytest.mark.parametrize("bad", [0, -5, 29.9, float("nan"), float("inf")])
async def test_add_poll_task_rejects_bad_interval(settings, monkeypatch, bad):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="must be finite"):
            await mcp.call_tool("add_poll_task", {"name": "p", "seconds": bad})
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_add_poll_task_min_interval_boundary(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        status = await call_json(mcp, "add_poll_task", {"name": "p", "seconds": 30.0})
        assert status["interval_seconds"] == 30.0


async def test_add_poll_task_rejects_duplicate(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        await call_json(mcp, "add_poll_task", {"name": "p"})
        with pytest.raises(ToolError, match="already registered"):
            await mcp.call_tool("add_poll_task", {"name": "p"})


async def test_remove_poll_task_unknown_and_heartbeat(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="unknown task"):
            await mcp.call_tool("remove_poll_task", {"name": "nope"})
        with pytest.raises(ToolError, match="reserved"):
            await mcp.call_tool("remove_poll_task", {"name": "heartbeat"})
        assert ctx["tasks"].status("heartbeat").running is True
    assert find_audit(read_audit(settings), "remove_poll_task", False)


# --- poll ticks --------------------------------------------------------------


async def test_poll_first_tick_captures_newest20_reversed(settings, monkeypatch):
    hist, calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [FakeMessage(30 - i, _other(), f"m{30 - i}") for i in range(10)]
        await tick()
        assert len(calls) == 1
        assert calls[0]["limit"] == 20
        assert calls[0]["oldest_first"] is False  # never True: would page from channel start
        assert "after" not in calls[0]
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [e["message_id"] for e in got["entries"]] == list(range(21, 31))
        assert {e["type"] for e in got["entries"]} == {"poll"}
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 30


async def test_poll_later_tick_pages_after_cursor(settings, monkeypatch):
    hist, calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    set_poll_cursor(settings.outbox_state_path, CHANNEL_ID, 100)
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [FakeMessage(mid, _other(), f"m{mid}") for mid in (101, 102, 103)]
        await tick()
        assert len(calls) == 1
        assert calls[0]["limit"] == 100
        assert calls[0]["oldest_first"] is True
        after = calls[0]["after"]
        assert isinstance(after, discord.Object) and after.id == 100
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [e["message_id"] for e in got["entries"]] == [101, 102, 103]
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 103


async def test_poll_tags_self_owner_other(settings, monkeypatch):
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [  # newest-first, as Discord returns them
            FakeMessage(203, _other(), "from other"),
            FakeMessage(202, _owner(), "from owner"),
            FakeMessage(201, FakeUser(BOT_UID, "ThycaBot", bot=True), "from self"),
        ]
        await tick()
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [(e["message_id"], e["actor_kind"]) for e in got["entries"]] == [
            (201, "self"),  # poll keeps self, unlike realtime capture
            (202, "owner"),
            (203, "other"),
        ]


async def test_poll_tick_error_audits_and_keeps_cursor(settings, monkeypatch):
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    set_poll_cursor(settings.outbox_state_path, CHANNEL_ID, 50)
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["exc"] = RuntimeError("boom")
        assert await tick() is None  # error audits, loop survives
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 50
        hstate["exc"] = None  # next tick retries and succeeds
        hstate["messages"] = [FakeMessage(51, _other(), "m51")]
        await tick()
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 51
    recs = find_audit(read_audit(settings), "poll_tick", False)
    assert len(recs) == 1 and recs[0]["args"] == {"channel_id": CHANNEL_ID}


async def test_poll_readd_resumes_cursor(settings, monkeypatch):
    hist, calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [FakeMessage(50, _other(), "m50")]
        await tick()
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 50
        assert await call_json(mcp, "remove_poll_task", {"name": "p"}) == {"removed": "p"}
        hstate["messages"] = []  # keep the re-added loop's first tick a no-op
        _, tick2 = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [FakeMessage(mid, _other(), f"m{mid}") for mid in (51, 52)]
        await tick2()
        assert calls[-1]["after"].id == 50  # cursor retained across remove/re-add
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 52


# --- realtime capture --------------------------------------------------------


async def test_realtime_capture_and_filtering(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(1, _owner(), "do it"))  # kept: owner
        await handler(FakeMessage(2, _other(), ""))  # kept: empty content
        await handler(FakeMessage(7, _other(), None))  # kept: None content -> ""
        await handler(
            FakeMessage(3, FakeUser(BOT_UID, "ThycaBot", bot=True), "self")
        )  # dropped: self
        await handler(FakeMessage(4, _other(), "x", guild_id=999))  # dropped: scope
        await handler(FakeMessage(5, _other(), "x", guild_id=None))  # dropped: DM
        await handler(FakeMessage(6, _other(), "x", channel_id=123))  # dropped: unwatched
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        rows = [(e["message_id"], e["actor_kind"], e["content"], e["type"]) for e in got["entries"]]
        assert rows == [
            (1, "owner", "do it", "message"),
            (2, "other", "", "message"),
            (7, "other", "", "message"),
        ]


async def test_realtime_failure_audited_without_secrets(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        settings.outbox_state_path.write_text("corrupt{", encoding="utf-8")
        await handler(FakeMessage(1, _owner(), "hi"))  # must not raise; failure is audited
    recs = find_audit(read_audit(settings), "on_message", False)
    assert len(recs) == 1
    assert recs[0]["args"] == {"channel_id": CHANNEL_ID, "message_id": 1}
    assert "test-token" not in json.dumps(recs[0])


# --- read/ack outbox (work without the gateway) ------------------------------


async def test_read_ack_outbox_via_tools(settings):
    seed_outbox(settings, 11, 12, 13)
    mcp = create_server(settings)  # no lifespan: read/ack need no bot
    page = await call_json(mcp, "read_outbox", {})
    assert [e["seq"] for e in page["entries"]] == [1, 2, 3]  # oldest first
    assert page["acked"] == 0 and page["latest"] == 3
    assert await call_json(mcp, "ack_outbox", {"up_to_seq": 2}) == {"acked": 2}
    unread = await call_json(mcp, "read_outbox", {"unread_only": True})
    assert [e["seq"] for e in unread["entries"]] == [3]
    with pytest.raises(ToolError, match="beyond latest"):
        await mcp.call_tool("ack_outbox", {"up_to_seq": 99})
    audit = read_audit(settings)
    assert find_audit(audit, "read_outbox", True)
    assert find_audit(audit, "ack_outbox", True)
    assert find_audit(audit, "ack_outbox", False)


async def test_new_tools_audit_success_and_error(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        await call_json(mcp, "add_poll_task", {"name": "p"})
        with pytest.raises(ToolError, match="reserved"):
            await mcp.call_tool("add_poll_task", {"name": "heartbeat"})
        await call_json(mcp, "remove_poll_task", {"name": "p"})
        with pytest.raises(ToolError, match="unknown task"):
            await mcp.call_tool("remove_poll_task", {"name": "p"})
        await call_json(mcp, "read_outbox", {})
        await call_json(mcp, "ack_outbox", {"up_to_seq": 0})
        with pytest.raises(ToolError, match="beyond latest"):
            await mcp.call_tool("ack_outbox", {"up_to_seq": 5})
    audit = read_audit(settings)
    for tool in ("add_poll_task", "remove_poll_task", "read_outbox", "ack_outbox"):
        assert find_audit(audit, tool, True), f"missing success audit for {tool}"
    for tool in ("add_poll_task", "remove_poll_task", "ack_outbox"):
        assert find_audit(audit, tool, False), f"missing error audit for {tool}"


# --- v1 regression: send_message uses the same scoped helper -----------------


async def test_send_message_scoped_helper_regression(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    evil = make_text_channel(666, guild_id=999)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel, 666: evil}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        res = await call_json(mcp, "send_message", {"content": "hi"})
        assert res["channel_id"] == CHANNEL_ID
        assert "message_id" in res and "jump_url" in res
        assert channel.sent[0].content == "hi"
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("send_message", {"content": "x", "channel_id": 666})


# --- lifespan: startup order, early failure, shutdown -------------------------


async def test_lifespan_startup_order_and_listener(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        # Listener registered after build, before login/connect; login before connect.
        assert bot.events[:4] == ["add_listener", "login", "connect", "wait_ready"]
        assert "on_message" in bot.listeners
        assert ctx["bot"] is bot
        assert ctx["tasks"].status("heartbeat").running is True
        # v5 always-on: real Responder slot + both protected loops running.
        assert isinstance(ctx["responder"], FakeResponder)
        assert ctx["tasks"].status("auto-reply").running is True
        assert ctx["tasks"].status("auto-reply-recovery").running is True
    assert ctx["tasks"].status("heartbeat").running is False
    assert "remove_listener" in bot.events
    assert bot.is_closed()


async def test_lifespan_early_gateway_failure(settings, monkeypatch):
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        connect_exc=discord.LoginFailure("bad token"),
    )

    async def hang_ready():
        bot.events.append("wait_ready")
        await asyncio.sleep(3600)

    bot.wait_until_ready = hang_ready
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(RuntimeError, match="gateway failed before ready"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - startup must raise first
    assert "login" in bot.events
    assert bot.is_closed()
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


def _leftover_gateway_tasks():
    return [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]


async def test_lifespan_login_failure_closes_bot_and_listener(settings, monkeypatch):
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        login_exc=discord.LoginFailure("bad token"),
    )
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(discord.LoginFailure, match="bad token"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - login must raise first
    assert bot.is_closed()  # HTTP session owner closed, not leaked
    assert bot.listeners.get("on_message") == []
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert _leftover_gateway_tasks() == []


async def test_lifespan_ready_failure_cleans_gateway(settings, monkeypatch):
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        ready_exc=RuntimeError("ready boom"),
    )
    tasks_seen: dict[str, asyncio.Task] = {}
    orig_connect = bot.connect

    async def spy_connect():
        tasks_seen["gateway"] = asyncio.current_task()
        await orig_connect()

    bot.connect = spy_connect
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(RuntimeError, match="ready boom"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - readiness must raise first
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    assert bot.events == [
        "add_listener",
        "login",
        "connect",
        "wait_ready",
        "remove_listener",
        "close",
    ]
    assert tasks_seen["gateway"].done()  # hanging connect settled, not leaked
    assert _leftover_gateway_tasks() == []


async def test_lifespan_cancel_during_readiness_wait_cleans_all(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    ready_entered = asyncio.Event()
    tasks_seen: dict[str, asyncio.Task] = {}

    async def hang_ready():
        tasks_seen["ready"] = asyncio.current_task()
        bot.events.append("wait_ready")
        ready_entered.set()  # lifespan is now parked inside asyncio.wait
        await asyncio.sleep(3600)

    orig_connect = bot.connect

    async def spy_connect():
        tasks_seen["gateway"] = asyncio.current_task()
        await orig_connect()

    bot.wait_until_ready = hang_ready
    bot.connect = spy_connect
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)

    async def _run():
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - cancelled while waiting for ready

    task = asyncio.create_task(_run(), name="lifespan-under-test")
    await asyncio.wait_for(ready_entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    assert tasks_seen["gateway"].cancelled()
    assert tasks_seen["ready"].cancelled()
    assert _leftover_gateway_tasks() == []


async def test_lifespan_cancel_during_login_cleans_all(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    login_entered = asyncio.Event()
    login_release = asyncio.Event()

    async def gated_login(token):  # noqa: ARG002 - signature mirrors discord.py
        bot.events.append("login")
        login_entered.set()  # lifespan is now parked inside login
        await login_release.wait()

    bot.login = gated_login
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)

    async def _run():
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - cancelled while logging in

    task = asyncio.create_task(_run(), name="lifespan-under-test")
    await asyncio.wait_for(login_entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    # No gateway tasks were ever created on this path.
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert _leftover_gateway_tasks() == []


async def test_lifespan_close_error_still_settles_tasks(settings, monkeypatch):
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        close_exc=RuntimeError("close boom"),
    )
    tasks_seen: dict[str, asyncio.Task] = {}
    orig_connect = bot.connect

    async def spy_connect():
        tasks_seen["gateway"] = asyncio.current_task()
        await orig_connect()

    bot.connect = spy_connect
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(RuntimeError, match="close boom"):
        async with mcp.settings.lifespan(mcp) as ctx:
            assert ctx["bot"] is bot
    # Listener removal ran before the close attempt; the close error stays visible.
    assert bot.listeners.get("on_message") == []
    assert bot.events == [
        "add_listener",
        "login",
        "connect",
        "wait_ready",
        "remove_listener",
        "close",
    ]
    assert tasks_seen["gateway"].done()
    assert _leftover_gateway_tasks() == []


async def test_lifespan_startup_refuses_state_behind_log(settings, monkeypatch):
    seed_outbox(settings, 11, 12)  # log highest seq 2, state seq 2
    tampered = json.loads(settings.outbox_state_path.read_bytes())
    tampered["seq"] = 1  # restart with state behind the log
    settings.outbox_state_path.write_text(json.dumps(tampered), encoding="utf-8")
    state_before = settings.outbox_state_path.read_bytes()
    log_before = settings.outbox_path.read_bytes()
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(OutboxError, match="behind the log"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - startup guard must raise first
    # No capture path ran: nothing logged in, no listener, no new writes;
    # the fresh bot is still closed by the shared finally.
    assert bot.events == ["remove_listener", "close"]
    assert bot.listeners.get("on_message") is None
    assert bot.is_closed()
    assert settings.outbox_state_path.read_bytes() == state_before
    assert settings.outbox_path.read_bytes() == log_before
    assert _leftover_gateway_tasks() == []


async def test_lifespan_startup_refuses_malformed_log(settings, monkeypatch):
    seed_outbox(settings, 11)  # valid nonzero state + one good entry
    with settings.outbox_path.open("a", encoding="utf-8") as f:
        f.write("oops\n")
    state_before = settings.outbox_state_path.read_bytes()
    log_before = settings.outbox_path.read_bytes()
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    with pytest.raises(OutboxError, match="corrupt outbox log"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - startup guard must raise first
    assert bot.events == ["remove_listener", "close"]
    assert bot.listeners.get("on_message") is None
    assert bot.is_closed()
    assert settings.outbox_state_path.read_bytes() == state_before
    assert settings.outbox_path.read_bytes() == log_before
    assert _leftover_gateway_tasks() == []


async def test_lifespan_shutdown_stops_dynamic_loops(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        await call_json(mcp, "add_poll_task", {"name": "p1"})
        await call_json(mcp, "add_poll_task", {"name": "p2"})
        # running_server parks the always-on auto loops; user loops still run.
        running = {t.name for t in ctx["tasks"].list_tasks() if t.running}
        assert running == {"heartbeat", "p1", "p2"}
    assert all(not t.running for t in ctx["tasks"].list_tasks())
    for name in ("heartbeat", "p1", "p2"):
        task = ctx["tasks"]._loops[name].get_task()
        assert task is None or task.done()
    assert bot.is_closed()


# --- state-loss / corruption consistency guard -------------------------------
# The guard (_check_outbox_consistent) refuses ack/cursor advances when state
# and log disagree, instead of resetting state or duplicating seqs. It relies
# on core read_entries raising for missing/zeroed state next to a non-empty
# log, acked > seq, and corrupt data (post-review core behavior).


async def test_ack_outbox_refuses_state_loss_without_reset(settings):
    seed_outbox(settings, 11, 12)
    settings.outbox_state_path.unlink()  # state loss: log intact, state gone
    mcp = create_server(settings)
    with pytest.raises(ToolError):  # must refuse even up_to_seq=0 (no fresh reset)
        await mcp.call_tool("ack_outbox", {"up_to_seq": 0})
    assert not settings.outbox_state_path.exists()  # no reset write happened
    assert find_audit(read_audit(settings), "ack_outbox", False)


async def test_ack_outbox_refuses_corrupt_state_untouched(settings):
    seed_outbox(settings, 11)
    settings.outbox_state_path.write_text("corrupt{", encoding="utf-8")
    mcp = create_server(settings)
    with pytest.raises(ToolError, match="corrupt"):
        await mcp.call_tool("ack_outbox", {"up_to_seq": 1})
    assert settings.outbox_state_path.read_text(encoding="utf-8") == "corrupt{"
    assert find_audit(read_audit(settings), "ack_outbox", False)


async def test_ack_outbox_refuses_acked_beyond_seq(settings):
    seed_outbox(settings, 11)
    before = settings.outbox_state_path.read_bytes()
    tampered = json.loads(before)
    tampered["acked"] = tampered["seq"] + 5  # impossible: acked beyond latest
    settings.outbox_state_path.write_text(json.dumps(tampered), encoding="utf-8")
    mcp = create_server(settings)
    with pytest.raises(ToolError):
        await mcp.call_tool("ack_outbox", {"up_to_seq": 1})
    # The guard must reject before ack mutates anything: file keeps the tampered
    # bytes verbatim (core never silently rewrites it on this path).
    assert (
        json.loads(settings.outbox_state_path.read_text(encoding="utf-8"))["acked"]
        == (tampered["acked"])
    )
    assert find_audit(read_audit(settings), "ack_outbox", False)


async def test_poll_tick_refuses_state_loss(settings, monkeypatch):
    hist, calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    seed_outbox(settings, 11, 12)
    log_before = settings.outbox_path.read_bytes()
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")  # start healthy, then lose state
        settings.outbox_state_path.unlink()
        hstate["messages"] = [FakeMessage(50, _other(), "m50")]
        assert await tick() is None  # guard audits, loop survives
        assert calls == []  # no history fetch on state loss
        assert not settings.outbox_state_path.exists()  # no cursor/state write
        assert settings.outbox_path.read_bytes() == log_before  # no appends
    recs = find_audit(read_audit(settings), "poll_tick", False)
    assert len(recs) == 1 and recs[0]["args"] == {"channel_id": CHANNEL_ID}


async def test_poll_tick_refuses_corrupt_log(settings, monkeypatch):
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    seed_outbox(settings, 11)
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        with settings.outbox_path.open("a", encoding="utf-8") as f:
            f.write("oops\n")  # corrupt the log after start, before tick
        hstate["messages"] = [FakeMessage(50, _other(), "m50")]
        assert await tick() is None
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) is None
    assert find_audit(read_audit(settings), "poll_tick", False)


async def test_add_poll_task_refuses_corrupt_state(settings, monkeypatch):
    # Corruption is injected AFTER a healthy startup: since the lifespan
    # startup guard validates state/log agreement first, pre-startup corruption
    # would now fail at boot instead of reaching the tool's runtime guard.
    # Same assertions as before — still the same runtime-guard coverage.
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        settings.outbox_state_path.write_text("corrupt{", encoding="utf-8")
        with pytest.raises(ToolError, match="corrupt"):
            await mcp.call_tool("add_poll_task", {"name": "p"})
        # v5: the two protected auto loops are always registered by default.
        assert {t.name for t in ctx["tasks"].list_tasks()} == {
            "heartbeat",
            "auto-reply",
            "auto-reply-recovery",
        }
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_fresh_outbox_unaffected_by_guard(settings, monkeypatch):
    # Missing state + missing log is a genuinely fresh outbox: guard passes.
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    assert not settings.outbox_path.exists() and not settings.outbox_state_path.exists()
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [FakeMessage(5, _owner(), "first")]
        await tick()  # first-tick path, cursor advances from nothing
        assert get_poll_cursor(settings.outbox_state_path, CHANNEL_ID) == 5
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 1}) == {"acked": 1}


# --- v5 phase 1: capture metadata (author_is_bot, reply_to_message_id) --------


def _other_bot(uid=888, name="OtherBot"):
    return FakeUser(uid, name, bot=True)


async def test_realtime_other_bot_captured_with_true(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(11, _owner(), "human owner"))
        await handler(FakeMessage(12, _other(), "human other"))
        await handler(FakeMessage(13, _other_bot(), "bot beep"))
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        rows = [(e["message_id"], e["actor_kind"], e["author_is_bot"]) for e in got["entries"]]
        assert rows == [
            (11, "owner", False),
            (12, "other", False),
            (13, "other", True),  # other bots still captured (v4 contract)
        ]
        assert all(e["reply_to_message_id"] is None for e in got["entries"])


async def test_realtime_self_excluded_regardless_of_bot_flag(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(21, FakeUser(BOT_UID, "ThycaBot", bot=True), "self"))
        await handler(FakeMessage(22, FakeUser(BOT_UID, "ThycaBot", bot=False), "self"))
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert got["entries"] == []
        assert got["latest"] == 0


async def test_realtime_reply_reference_recorded(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        ref = SimpleNamespace(message_id=31)
        await handler(FakeMessage(32, _other(), "reply", reference=ref))
        await handler(FakeMessage(33, _other(), "top-level"))
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [(e["message_id"], e["reply_to_message_id"]) for e in got["entries"]] == [
            (32, 31),
            (33, None),
        ]
        assert [e["author_is_bot"] for e in got["entries"]] == [False, False]


async def test_poll_self_and_other_bots_kept_with_true(settings, monkeypatch):
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [  # newest-first, as Discord returns them
            FakeMessage(43, _other_bot(), "other bot"),
            FakeMessage(42, _owner(), "owner"),
            FakeMessage(41, FakeUser(BOT_UID, "ThycaBot", bot=True), "self"),
        ]
        await tick()
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [(e["message_id"], e["actor_kind"], e["author_is_bot"]) for e in got["entries"]] == [
            (41, "self", True),  # poll keeps self, unlike realtime
            (42, "owner", False),
            (43, "other", True),
        ]


async def test_poll_reply_reference_recorded(settings, monkeypatch):
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _, tick = await add_and_hold(mcp, ctx, "p")
        hstate["messages"] = [  # newest-first, as Discord returns them
            FakeMessage(52, _other(), "top", reference=None),
            FakeMessage(51, _other(), "reply", reference=SimpleNamespace(message_id=50)),
        ]
        await tick()
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [(e["message_id"], e["reply_to_message_id"]) for e in got["entries"]] == [
            (51, 50),
            (52, None),
        ]


# --- v5: reserved system tasks, protected loops, metadata-only send audit ----


@pytest.mark.parametrize("reserved", ["auto-reply", "auto-reply-recovery"])
async def test_add_poll_task_rejects_reserved_auto_names(settings, monkeypatch, reserved):
    # Reserved-name rejection runs before channel resolution: even an
    # out-of-guild channel reports "reserved", and nothing is registered.
    evil = make_text_channel(666, guild_id=999)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID), 666: evil}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="reserved"):
            await mcp.call_tool("add_poll_task", {"name": reserved, "channel_id": 666})
        assert {t.name for t in ctx["tasks"].list_tasks()} == {
            "heartbeat",
            "auto-reply",
            "auto-reply-recovery",
        }
    assert find_audit(read_audit(settings), "add_poll_task", False)


async def test_stop_task_refuses_protected_loops(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    # Unparked lifespan: the protected loops must be running to prove the
    # refusal is about protection, not idleness.
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["tasks"].status("auto-reply").running is True
        with pytest.raises(ToolError, match="protected"):
            await mcp.call_tool("stop_task", {"name": "auto-reply"})
        with pytest.raises(ToolError, match="protected"):
            await mcp.call_tool("stop_task", {"name": "auto-reply-recovery"})
        assert ctx["tasks"].status("auto-reply").running is True
        assert ctx["tasks"].status("auto-reply-recovery").running is True
    assert find_audit(read_audit(settings), "stop_task", False)


async def test_remove_poll_task_refuses_protected_loops(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="protected"):
            await mcp.call_tool("remove_poll_task", {"name": "auto-reply"})
        with pytest.raises(ToolError, match="protected"):
            await mcp.call_tool("remove_poll_task", {"name": "auto-reply-recovery"})
        assert {t.name for t in ctx["tasks"].list_tasks()} == {
            "heartbeat",
            "auto-reply",
            "auto-reply-recovery",
        }
    assert find_audit(read_audit(settings), "remove_poll_task", False)


async def test_start_task_restarts_stopped_protected_loop(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        # running_server parks the auto loops; the tool may restart them.
        assert ctx["tasks"].status("auto-reply").running is False
        status = await call_json(mcp, "start_task", {"name": "auto-reply"})
        assert status["running"] is True
        with pytest.raises(ToolError, match="already running"):
            await mcp.call_tool("start_task", {"name": "auto-reply"})


async def test_send_message_audit_is_metadata_only(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    evil = make_text_channel(666, guild_id=999)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel, 666: evil}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        res = await call_json(mcp, "send_message", {"content": "hi SECRET_CANARY_X7"})
        assert res["channel_id"] == CHANNEL_ID
        assert set(res) == {"message_id", "channel_id", "jump_url"}  # legacy shape kept
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool(
                "send_message", {"content": "nope SECRET_CANARY_X7", "channel_id": 666}
            )
    audit_text = settings.audit_path.read_text(encoding="utf-8")
    assert "SECRET_CANARY_X7" not in audit_text
    assert '"content":' not in audit_text  # no raw content key in send audits
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1
    assert ok_recs[0]["args"]["content_len"] == len("hi SECRET_CANARY_X7")
    assert (
        ok_recs[0]["args"]["content_sha256"] == hashlib.sha256(b"hi SECRET_CANARY_X7").hexdigest()
    )
    assert ok_recs[0]["args"]["channel_id"] == CHANNEL_ID


@pytest.mark.parametrize(
    "extra",
    [{"source_message_id": 123}, {"job_ref": "job-1-2-3"}],
)
async def test_send_message_rejects_partial_correlation(settings, monkeypatch, extra):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool("send_message", {"content": "hi", **extra})
        assert channel.sent == []  # rejected before any SDK send
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1
    assert recs[0]["args"]["error_kind"] == "validation_error"
    assert recs[0]["error"] == "send_message failed kind=validation_error"


# --- v5 fixes: cleanup never replaces the primary outcome (finding 3) -------


async def test_lifespan_cancel_during_login_with_cleanup_failure_stays_cancelled(
    settings, monkeypatch
):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    login_entered = asyncio.Event()

    async def gated_login(token):  # noqa: ARG002 - signature mirrors discord.py
        bot.events.append("login")
        login_entered.set()  # lifespan is now parked inside login
        await asyncio.sleep(3600)

    bot.login = gated_login
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FailingCloseResponder)
    mcp = create_server(settings)

    async def _run():
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - cancelled while logging in

    task = asyncio.create_task(_run(), name="lifespan-under-test")
    await asyncio.wait_for(login_entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as excinfo:
        await task
    assert task.cancelled()  # the cleanup failure did NOT replace the cancellation
    secondaries = getattr(excinfo.value, "thyca_cleanup_errors", ())
    assert len(secondaries) == 1
    assert isinstance(secondaries[0], RuntimeError)
    assert "responder close boom" in str(secondaries[0])
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert _leftover_gateway_tasks() == []


async def test_lifespan_startup_failure_with_cleanup_failures_keeps_primary(settings, monkeypatch):
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        login_exc=discord.LoginFailure("bad token"),
        close_exc=RuntimeError("bot close boom"),
    )
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FailingCloseResponder)
    mcp = create_server(settings)
    with pytest.raises(discord.LoginFailure, match="bad token") as excinfo:
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - login must raise first
    # Both cleanup failures retained as secondary evidence; primary intact.
    secondaries = getattr(excinfo.value, "thyca_cleanup_errors", ())
    assert [str(e) for e in secondaries] == ["responder close boom", "bot close boom"]
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert _leftover_gateway_tasks() == []


# --- v5 fixes: teardown steps independently protected (finding 4) ------------


async def test_lifespan_listener_removal_failure_still_tears_down(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FailingRemoveListenerBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", RefusingGuardResponder)
    mcp = create_server(settings)
    with pytest.raises(RuntimeError, match="remove boom"):
        async with mcp.settings.lifespan(mcp):
            # Guard active while running: the refusing responder rejects ACK.
            with pytest.raises(ToolError, match="beyond safe prefix"):
                await mcp.call_tool("ack_outbox", {"up_to_seq": 0})
    # Removal failed, but every remaining step still ran: bot closed, no
    # gateway leaks, and BOTH state entries cleared (send sees no bot, ack
    # sees no guard). The cleanup failure still surfaces on a clean exit.
    assert bot.is_closed()
    assert "close" in bot.events
    assert _leftover_gateway_tasks() == []
    with pytest.raises(ToolError, match="not ready"):
        await mcp.call_tool("send_message", {"content": "x"})
    assert await call_json(mcp, "ack_outbox", {"up_to_seq": 0}) == {"acked": 0}


# --- v5 fixes: tmp/fake-only settings, no real dotenv/env reads (finding 8) --


def test_settings_fixture_disables_dotenv_source(tmp_path, monkeypatch):
    """The fixture construction must not select or read any dotenv file."""
    import pathlib

    from pydantic_settings.sources import DotEnvSettingsSource

    selected: list = []
    env_stats: list = []
    file_reads: list = []
    orig_init = DotEnvSettingsSource.__init__
    orig_is_file = pathlib.Path.is_file

    def spy_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        selected.append(self.env_file)

    def spy_is_file(self):
        if self.name == ".env":
            env_stats.append(str(self))
        return orig_is_file(self)

    def no_real_reads(self, *args, **kwargs):
        # Guard: the control below must attempt (not perform) a real read.
        file_reads.append(True)
        return {}

    monkeypatch.setattr(DotEnvSettingsSource, "__init__", spy_init)
    monkeypatch.setattr(pathlib.Path, "is_file", spy_is_file)
    monkeypatch.setattr(DotEnvSettingsSource, "_read_env_file", no_real_reads)
    _make_settings(tmp_path)
    assert selected == [None]  # dotenv source disabled
    assert env_stats == []  # never even stats a dotenv path
    assert file_reads == []  # zero real file reads

    # Control: the same kwargs WITHOUT _env_file select the project .env and
    # stat its path (any actual read stays blocked by the guard above).
    selected.clear()
    env_stats.clear()
    file_reads.clear()
    legacy_kwargs = _settings_kwargs(tmp_path)
    legacy_kwargs.pop("_env_file")
    Settings(**legacy_kwargs)
    assert len(selected) == 1 and selected[0] is not None
    assert env_stats != []


def test_settings_fixture_pins_env_sensitive_defaults(tmp_path, monkeypatch):
    """Explicit fixture values win over ambient environment variables."""
    monkeypatch.setenv("SESSION_ID", "env-hijack")
    monkeypatch.setenv("RESPONDER_CHECK_SECONDS", "999")
    monkeypatch.setenv("RECOVERY_POLL_SECONDS", "999")
    made = _make_settings(tmp_path)
    assert made.session_id == "discord-test"
    assert made.responder_check_seconds == 1.0
    assert made.recovery_poll_seconds == 30.0
