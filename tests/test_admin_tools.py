"""Admin tools v2/v3 tests: messages/channels + members/roles (31-tool server).

Offline fakes only (no network, no real .env): the real installed lifespan
runs with build_bot patched to a FakeBot and Responder parked like
tests/test_server.py. Every tool gets a happy path plus at least one
refusal/error; audit assertions prove metadata-only logging (no content,
no raw reason text).
"""

from __future__ import annotations

import asyncio
import datetime
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from config import Settings
from server import create_server

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
CATEGORY_ID = 445
VOICE_ID = 446
ROLE_ID = 777

NEW_TOOLS = [
    "read_history",
    "edit_message",
    "delete_message",
    "purge",
    "create_text_channel",
    "create_voice_channel",
    "delete_channel",
    "get_member",
    "set_nick",
    "add_role",
    "remove_role",
    "create_role",
    "list_roles",
    "timeout",
    "kick",
    "ban",
    "unban",
]


class FakeUser:
    def __init__(self, uid, display_name, bot=False):
        self.id = uid
        self.display_name = display_name
        self.bot = bot


class FakeRole:
    def __init__(self, rid, name, position, members=(), mentionable=False):
        self.id = rid
        self.name = name
        self.position = position
        self.members = list(members)
        self.mentionable = mentionable


class FakeMember:
    def __init__(self, uid, display_name, nick=None, roles=(), joined_at=None, bot=False):
        self.id = uid
        self.display_name = display_name
        self.nick = nick
        self.roles = list(roles)
        self.joined_at = joined_at
        self.bot = bot
        self.calls: list = []

    async def edit(self, *, nick=..., **kwargs):
        self.calls.append(("edit", {"nick": nick, **kwargs}))
        self.nick = nick
        return self

    async def add_roles(self, *roles, reason=None, atomic=True):
        self.calls.append(("add_roles", {"roles": roles, "reason": reason}))
        for r in roles:
            if r not in self.roles:
                self.roles.append(r)

    async def remove_roles(self, *roles, reason=None, atomic=True):
        self.calls.append(("remove_roles", {"roles": roles, "reason": reason}))
        for r in roles:
            if r in self.roles:
                self.roles.remove(r)

    async def timeout(self, until, *, reason=None):
        self.calls.append(("timeout", {"until": until, "reason": reason}))
        self.timed_out_until = until

    async def kick(self, *, reason=None):
        self.calls.append(("kick", {"reason": reason}))

    async def ban(self, *, delete_message_seconds=0, reason=None):
        self.calls.append(
            ("ban", {"delete_message_seconds": delete_message_seconds, "reason": reason})
        )


class FakeMessage:
    def __init__(
        self,
        mid,
        author,
        content="",
        channel_id=CHANNEL_ID,
        guild_id=GUILD_ID,
        reference=None,
        created_at=None,
    ):
        self.id = mid
        self.author = author
        self.content = content
        self.channel = SimpleNamespace(id=channel_id)
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        self.jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{mid}"
        self.reference = reference
        self.created_at = created_at or datetime.datetime(
            2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        self.edited_to = None
        self.deleted = False

    async def edit(self, *, content=None):
        if self.author.id != BOT_UID:
            # Discord only lets the bot edit its own messages.
            raise _forbidden()
        self.edited_to = content
        self.content = content
        return self

    async def delete(self, *, delay=None):
        self.deleted = True


def _not_found(code=10008):
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": code, "message": "Unknown"},
    )


def _forbidden():
    return discord.Forbidden(
        SimpleNamespace(status=403, reason="Forbidden"),
        {"code": 50013, "message": "Missing Permissions"},
    )


def make_text_channel(cid, guild_id=GUILD_ID, messages=(), exc=None):
    """MagicMock TextChannel: isinstance passes, purge/history/fetch scripted."""
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    channel.name = f"chan-{cid}"
    channel.topic = None
    channel.position = 0
    channel.category_id = None
    store = {"messages": list(messages), "exc": exc, "calls": []}
    channel.source_store = store

    async def fake_history(*, limit=100, oldest_first=False, **kwargs):
        store["calls"].append({"limit": limit, "oldest_first": oldest_first, **kwargs})
        msgs = sorted(store["messages"], key=lambda m: m.id, reverse=True)[:limit]
        ordered = msgs if not oldest_first else list(reversed(msgs))
        for m in ordered:
            yield m

    async def fake_fetch(mid):
        store["calls"].append(("fetch", mid))
        if store["exc"] is not None:
            raise store["exc"]
        for m in store["messages"]:
            if m.id == mid:
                return m
        raise _not_found()

    async def fake_purge(*, limit=100, bulk=True, **kwargs):
        store["calls"].append({"purge": {"limit": limit, "bulk": bulk, **kwargs}})
        if store["exc"] is not None:
            raise store["exc"]
        doomed = sorted(store["messages"], key=lambda m: m.id, reverse=True)[:limit]
        for m in doomed:
            m.deleted = True
        return doomed

    async def fake_send(content, **kwargs):
        msg = FakeMessage(9000 + len(store["messages"]), FakeUser(BOT_UID, "Bot", True), content)
        store["messages"].append(msg)
        return msg

    channel.history = fake_history
    channel.fetch_message = fake_fetch
    channel.purge = fake_purge
    channel.send = fake_send
    return channel


def make_deletable_channel(cid, spec, guild_id=GUILD_ID, exc=None):
    channel = MagicMock(spec=spec)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    channel.name = f"chan-{cid}"
    channel.deleted = False
    channel.category_id = None
    channel.position = 0

    async def fake_delete(*, reason=None):
        if exc is not None:
            raise exc
        channel.deleted = True
        channel.delete_reason = reason

    channel.delete = fake_delete
    return channel


class FakeGuild:
    def __init__(self, gid, channels=None, members=None, roles=None, member_count=10):
        self.id = gid
        self.name = f"guild-{gid}"
        self._channels = dict(channels or {})
        self._members = dict(members or {})
        self._roles = {r.id: r for r in (roles or [])}
        self.member_count = member_count
        self.calls: list = []

    def get_channel(self, cid):
        return self._channels.get(cid)

    def get_member(self, uid):
        return self._members.get(uid)

    def get_role(self, rid):
        return self._roles.get(rid)

    @property
    def roles(self):
        return sorted(self._roles.values(), key=lambda r: r.position)

    @property
    def text_channels(self):
        return [c for c in self._channels.values() if isinstance(c, discord.TextChannel)]

    @property
    def voice_channels(self):
        return [c for c in self._channels.values() if isinstance(c, discord.VoiceChannel)]

    @property
    def members(self):
        return list(self._members.values())

    @property
    def chunked(self):
        return self.member_count == len(self._members)

    async def fetch_members(self, limit=None):
        for member in list(self._members.values())[:limit]:
            yield member

    async def fetch_member(self, uid):
        try:
            return self._members[uid]
        except KeyError:
            raise _not_found(code=10007) from None

    async def create_text_channel(self, name, *, category=None, topic=None, **kwargs):
        self.calls.append(("create_text_channel", {"name": name, "category": category}))
        cid = 500 + len(self.calls)
        channel = make_text_channel(cid)
        channel.name = name
        channel.topic = topic
        if category is not None:
            channel.category_id = category.id
        self._channels[cid] = channel
        return channel

    async def create_voice_channel(self, name, *, category=None, **kwargs):
        self.calls.append(("create_voice_channel", {"name": name, "category": category}))
        channel = make_deletable_channel(600 + len(self.calls), discord.VoiceChannel)
        channel.name = name
        if category is not None:
            channel.category_id = category.id
        return channel

    async def create_role(self, *, name=None, permissions=None, colour=None, **kwargs):
        recorded = {"name": name, "permissions": permissions, **kwargs}
        if colour is not None:
            recorded["colour"] = colour
        self.calls.append(("create_role", recorded))
        rid = 700 + len(self.calls)
        role = FakeRole(rid, name, position=len(self._roles), mentionable=kwargs.get("mentionable"))
        self._roles[rid] = role
        return role

    async def unban(self, user, *, reason=None):
        self.calls.append(("unban", {"user": user, "reason": reason}))
        if getattr(user, "id", None) == 424242:
            raise _not_found(code=10026)

    async def ban(self, user, *, delete_message_seconds=0, reason=None):
        self.calls.append(
            (
                "ban",
                {"user": user, "delete_message_seconds": delete_message_seconds, "reason": reason},
            )
        )
        if getattr(user, "id", None) == 424242:
            raise _not_found(code=10026)


class FakeBot:
    def __init__(self, guild, *, user=None, fetch_map=None):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self.events: list[str] = []
        self.listeners: dict[str, list] = {}
        self._closed = False
        self._ready = False
        self._setup = False
        self._ready_event = asyncio.Event()
        self._close_event = asyncio.Event()

    def event(self, func):
        self.events.append("event")
        self.listeners.setdefault(func.__name__, []).append(func)
        return func

    async def start(self, token):
        self.events.append("start")
        self._setup = True
        self._ready_event.set()
        await self._close_event.wait()

    async def wait_until_ready(self):
        self.events.append("wait_ready")
        await self._ready_event.wait()
        self._ready = True

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
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


class FakeResponder:
    def __init__(self, settings, bot):
        self.settings = settings
        self.bot = bot

    async def tick(self):
        return {"status": "idle"}

    async def close(self):
        pass

    def guard_ack(self, up_to_seq):
        return None

    def correlated_job(self, source_message_id, job_ref, channel_id):
        raise AssertionError("admin tests never correlate")


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
        responder_check_seconds=1.0,
        recovery_poll_seconds=30.0,
    )


@pytest.fixture
def settings(tmp_path):
    return Settings(**_settings_kwargs(tmp_path))


def _member(uid=OTHER_UID, name="Rando", **kwargs):
    kwargs.setdefault("roles", [FakeRole(1, "@everyone", 0)])
    kwargs.setdefault("joined_at", datetime.datetime(2020, 1, 2, tzinfo=datetime.timezone.utc))
    return FakeMember(uid, name, **kwargs)


def _guild_with(channels=None, members=None, roles=None, **kwargs):
    members = members if members is not None else {OTHER_UID: _member()}
    return FakeGuild(GUILD_ID, channels or {}, members=members, roles=roles, **kwargs)


@asynccontextmanager
async def running_server(monkeypatch, settings, bot_or_guild):
    # Accepts a FakeBot or a bare FakeGuild (wrapped in a FakeBot).
    bot = bot_or_guild if isinstance(bot_or_guild, FakeBot) else FakeBot(bot_or_guild)
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", FakeResponder)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx, bot


async def call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def read_audit(settings):
    if not settings.audit_path.exists():
        return []
    return [
        json.loads(line) for line in settings.audit_path.read_text().splitlines() if line.strip()
    ]


def find_audit(records, tool, ok):
    return [r for r in records if r["tool"] == tool and r["ok"] is ok]


# --- read_history ------------------------------------------------------------


async def test_read_history_newest_first_with_kinds(settings, monkeypatch):
    msgs = [
        FakeMessage(30, FakeUser(OTHER_UID, "Rando"), "third"),
        FakeMessage(
            29,
            FakeUser(OWNER_UID, "Olivia"),
            "second",
            reference=SimpleNamespace(message_id=28),
        ),
        FakeMessage(28, FakeUser(BOT_UID, "ThycaBot", True), "first"),
    ]
    channel = make_text_channel(CHANNEL_ID, messages=msgs)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "read_history", {})
    assert [e["message_id"] for e in got] == [30, 29, 28]
    assert [(e["author_uid"], e["kind"]) for e in got] == [
        (OTHER_UID, "other"),
        (OWNER_UID, "owner"),
        (BOT_UID, "self"),
    ]
    assert got[0]["content"] == "third"
    assert got[1]["reply_to_message_id"] == 28
    assert got[2]["reply_to_message_id"] is None
    assert all("timestamp" in e and "T" in e["timestamp"] for e in got)
    assert set(got[0]) == {
        "message_id",
        "author_uid",
        "author_name",
        "kind",
        "content",
        "timestamp",
        "reply_to_message_id",
    }


async def test_read_history_refuses_out_of_guild(settings, monkeypatch):
    evil = make_text_channel(666, guild_id=999)
    async with running_server(monkeypatch, settings, _guild_with({666: evil})) as (mcp, *_):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("read_history", {"channel_id": 666})
    assert find_audit(read_audit(settings), "read_history", False)


async def test_read_history_limit_clamped(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        await call_json(mcp, "read_history", {"limit": 500})
        await call_json(mcp, "read_history", {"limit": 0})
    assert channel.source_store["calls"][-2]["limit"] == 100
    assert channel.source_store["calls"][-1]["limit"] == 1


# --- edit_message ------------------------------------------------------------


async def test_edit_message_happy(settings, monkeypatch):
    msg = FakeMessage(50, FakeUser(BOT_UID, "ThycaBot", True), "old")
    channel = make_text_channel(CHANNEL_ID, messages=[msg])
    bot = FakeBot(_guild_with({CHANNEL_ID: channel}))
    async with running_server(monkeypatch, settings, bot) as (mcp, *_):
        got = await call_json(mcp, "edit_message", {"message_id": 50, "content": "new"})
    assert got == {"message_id": 50, "channel_id": CHANNEL_ID}
    assert msg.edited_to == "new"
    recs = find_audit(read_audit(settings), "edit_message", True)
    assert recs[0]["args"] == {"channel_id": CHANNEL_ID, "message_id": 50, "content_len": 3}


async def test_edit_message_forbidden_surfaces(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID, exc=_forbidden())
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool("edit_message", {"message_id": 50, "content": "x"})
    assert find_audit(read_audit(settings), "edit_message", False)


async def test_edit_message_rejects_empty_content(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="1\\.\\.2000"):
            await mcp.call_tool("edit_message", {"message_id": 50, "content": ""})
    assert find_audit(read_audit(settings), "edit_message", False)


# --- delete_message ----------------------------------------------------------


async def test_delete_message_happy(settings, monkeypatch):
    msg = FakeMessage(51, FakeUser(OWNER_UID, "Olivia"), "bye")
    channel = make_text_channel(CHANNEL_ID, messages=[msg])
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "delete_message", {"message_id": 51})
    assert got == {"deleted": 51, "channel_id": CHANNEL_ID}
    assert msg.deleted is True


async def test_delete_message_not_found_surfaces(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID, messages=[])
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="NotFound|404|10008"):
            await mcp.call_tool("delete_message", {"message_id": 123})
    assert find_audit(read_audit(settings), "delete_message", False)


# --- purge -------------------------------------------------------------------


async def test_purge_returns_count_only(settings, monkeypatch):
    msgs = [FakeMessage(60 + i, FakeUser(OTHER_UID, "Rando"), f"m{i}") for i in range(5)]
    channel = make_text_channel(CHANNEL_ID, messages=msgs)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "purge", {"channel_id": CHANNEL_ID, "limit": 3})
    assert got == {"deleted": 3}
    assert channel.source_store["calls"][-1]["purge"] == {"limit": 3, "bulk": True}
    assert find_audit(read_audit(settings), "purge", True)[0]["args"] == {
        "channel_id": CHANNEL_ID,
        "limit": 3,
    }


async def test_purge_refuses_out_of_guild(settings, monkeypatch):
    evil = make_text_channel(666, guild_id=999)
    async with running_server(monkeypatch, settings, _guild_with({666: evil})) as (mcp, *_):
        with pytest.raises(ToolError, match="refusing to send outside scoped guild"):
            await mcp.call_tool("purge", {"channel_id": 666})
    assert find_audit(read_audit(settings), "purge", False)


# --- create_text_channel -----------------------------------------------------


async def test_create_text_channel_happy(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, ctx, bot):
        got = await call_json(mcp, "create_text_channel", {"name": "news"})
    assert got["name"] == "news"
    assert got["topic"] is None
    assert ("create_text_channel", {"name": "news", "category": None}) in bot._guild.calls


async def test_create_text_channel_with_category(settings, monkeypatch):
    cat = MagicMock(spec=discord.CategoryChannel)
    cat.id = CATEGORY_ID
    cat.guild = SimpleNamespace(id=GUILD_ID)
    async with running_server(monkeypatch, settings, _guild_with({CATEGORY_ID: cat})) as (
        mcp,
        ctx,
        bot,
    ):
        got = await call_json(
            mcp, "create_text_channel", {"name": "news", "category_id": CATEGORY_ID}
        )
    assert got["category_id"] == CATEGORY_ID


async def test_create_text_channel_rejects_non_category(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="not a CategoryChannel"):
            await mcp.call_tool("create_text_channel", {"name": "x", "category_id": CHANNEL_ID})
    assert find_audit(read_audit(settings), "create_text_channel", False)


async def test_create_text_channel_rejects_blank_name(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="non-empty"):
            await mcp.call_tool("create_text_channel", {"name": "  "})
    recs = find_audit(read_audit(settings), "create_text_channel", False)
    assert recs[0]["args"]["name_len"] == 2


# --- create_voice_channel ----------------------------------------------------


async def test_create_voice_channel_happy(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, ctx, bot):
        got = await call_json(mcp, "create_voice_channel", {"name": "lounge"})
    assert got["name"] == "lounge"
    assert ("create_voice_channel", {"name": "lounge", "category": None}) in bot._guild.calls


async def test_create_voice_channel_rejects_missing_category(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="not a CategoryChannel"):
            await mcp.call_tool("create_voice_channel", {"name": "x", "category_id": 12345})
    assert find_audit(read_audit(settings), "create_voice_channel", False)


# --- delete_channel ----------------------------------------------------------


async def test_delete_channel_text_happy(settings, monkeypatch):
    channel = make_deletable_channel(CHANNEL_ID, discord.TextChannel)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "delete_channel", {"channel_id": CHANNEL_ID})
    assert got == {"deleted": CHANNEL_ID}
    assert channel.deleted is True


async def test_delete_channel_category_happy(settings, monkeypatch):
    cat = make_deletable_channel(CATEGORY_ID, discord.CategoryChannel)
    async with running_server(monkeypatch, settings, _guild_with({CATEGORY_ID: cat})) as (mcp, *_):
        got = await call_json(mcp, "delete_channel", {"channel_id": CATEGORY_ID})
    assert got == {"deleted": CATEGORY_ID}
    assert cat.deleted is True


async def test_delete_channel_refuses_dm(settings, monkeypatch):
    dm = MagicMock(spec=discord.DMChannel)
    dm.id = 777
    async with running_server(monkeypatch, settings, _guild_with({777: dm})) as (mcp, *_):
        with pytest.raises(ToolError, match="outside scoped guild"):
            await mcp.call_tool("delete_channel", {"channel_id": 777})
    assert find_audit(read_audit(settings), "delete_channel", False)


# --- get_member --------------------------------------------------------------


async def test_get_member_happy(settings, monkeypatch):
    roles = [FakeRole(1, "@everyone", 0), FakeRole(ROLE_ID, "Mod", 3)]
    member = _member(uid=OTHER_UID, name="Rando", nick="Ran", roles=roles, bot=False)
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "get_member", {"uid": OTHER_UID})
    assert got == {
        "uid": OTHER_UID,
        "display_name": "Rando",
        "nick": "Ran",
        "kind": "other",
        "roles": [
            {"id": 1, "name": "@everyone", "position": 0},
            {"id": ROLE_ID, "name": "Mod", "position": 3},
        ],
        "joined_at": "2020-01-02T00:00:00+00:00",
        "bot": False,
    }


async def test_get_member_fetch_fallback(settings, monkeypatch):
    """Cache miss resolves via guild.fetch_member (resolve_display_name cascade)."""
    guild = _guild_with(members={})
    guild._members[OTHER_UID] = _member()
    guild.get_member = lambda uid: None  # force the cache-miss path
    async with running_server(monkeypatch, settings, guild) as (mcp, *_):
        got = await call_json(mcp, "get_member", {"uid": OTHER_UID})
    assert got["uid"] == OTHER_UID and got["kind"] == "other"


async def test_get_member_unknown_is_error(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with(members={})) as (mcp, *_):
        with pytest.raises(ToolError, match="NotFound|404|10007"):
            await mcp.call_tool("get_member", {"uid": 12345})
    assert find_audit(read_audit(settings), "get_member", False)


# --- set_nick ----------------------------------------------------------------


async def test_set_nick_happy(settings, monkeypatch):
    member = _member()
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "set_nick", {"uid": OTHER_UID, "nick": "Ran"})
    assert got["nick"] == "Ran"
    assert member.calls[-1] == ("edit", {"nick": "Ran"})


async def test_set_nick_empty_clears(settings, monkeypatch):
    member = _member(nick="Old")
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "set_nick", {"uid": OTHER_UID, "nick": ""})
    assert got["nick"] is None
    assert member.calls[-1] == ("edit", {"nick": None})


async def test_set_nick_unknown_member_is_error(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with(members={})) as (mcp, *_):
        with pytest.raises(ToolError, match="NotFound|404"):
            await mcp.call_tool("set_nick", {"uid": 12345, "nick": "x"})
    assert find_audit(read_audit(settings), "set_nick", False)


# --- add_role / remove_role --------------------------------------------------


async def test_add_role_happy(settings, monkeypatch):
    role = FakeRole(ROLE_ID, "Mod", 3)
    member = _member()
    guild = _guild_with(members={OTHER_UID: member}, roles=[FakeRole(1, "@everyone", 0), role])
    async with running_server(monkeypatch, settings, guild) as (mcp, *_):
        got = await call_json(mcp, "add_role", {"uid": OTHER_UID, "role_id": ROLE_ID})
    assert {"id": ROLE_ID, "name": "Mod", "position": 3} in got["roles"]
    assert member.calls[-1][0] == "add_roles"


async def test_add_role_unknown_role_is_error(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="not found in scoped guild"):
            await mcp.call_tool("add_role", {"uid": OTHER_UID, "role_id": 424242})
    assert find_audit(read_audit(settings), "add_role", False)


async def test_remove_role_happy(settings, monkeypatch):
    role = FakeRole(ROLE_ID, "Mod", 3)
    member = _member(roles=[FakeRole(1, "@everyone", 0), role])
    guild = _guild_with(members={OTHER_UID: member}, roles=[FakeRole(1, "@everyone", 0), role])
    async with running_server(monkeypatch, settings, guild) as (mcp, *_):
        got = await call_json(mcp, "remove_role", {"uid": OTHER_UID, "role_id": ROLE_ID})
    assert all(r["id"] != ROLE_ID for r in got["roles"])
    assert member.calls[-1][0] == "remove_roles"


async def test_remove_role_unknown_role_is_error(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="not found in scoped guild"):
            await mcp.call_tool("remove_role", {"uid": OTHER_UID, "role_id": 424242})
    assert find_audit(read_audit(settings), "remove_role", False)


# --- create_role / list_roles ------------------------------------------------


async def test_create_role_defaults(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, ctx, bot):
        got = await call_json(mcp, "create_role", {"name": "Helpers"})
    assert got["name"] == "Helpers"
    assert set(got) == {"id", "name", "position"}
    (kind, kwargs) = bot._guild.calls[-1]
    assert kind == "create_role"
    assert kwargs["permissions"] == discord.Permissions(0)
    assert "colour" not in kwargs
    assert kwargs["mentionable"] is False


async def test_create_role_with_colour_and_mentionable(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, ctx, bot):
        await call_json(
            mcp,
            "create_role",
            {"name": "Red", "permissions": 8, "colour": 16711680, "mentionable": True},
        )
    (kind, kwargs) = bot._guild.calls[-1]
    assert kwargs["permissions"] == discord.Permissions(8)
    assert kwargs["colour"] == discord.Colour(16711680)
    assert kwargs["mentionable"] is True


async def test_create_role_rejects_blank_name(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="non-empty"):
            await mcp.call_tool("create_role", {"name": ""})
    assert find_audit(read_audit(settings), "create_role", False)


async def test_list_roles_includes_everyone_in_position_order(settings, monkeypatch):
    roles = [
        FakeRole(1, "@everyone", 0, members=[_member(), _member(uid=1001, name="A")]),
        FakeRole(ROLE_ID, "Mod", 3, members=[_member()], mentionable=True),
    ]
    async with running_server(monkeypatch, settings, _guild_with(roles=roles)) as (mcp, *_):
        got = await call_json(mcp, "list_roles", {})
    assert got == [
        {"id": 1, "name": "@everyone", "position": 0, "member_count": 2, "mentionable": False},
        {"id": ROLE_ID, "name": "Mod", "position": 3, "member_count": 1, "mentionable": True},
    ]


async def test_list_roles_wrong_guild_is_error(settings, monkeypatch):
    bot = FakeBot(FakeGuild(999, {}, members={}))
    async with running_server(monkeypatch, settings, bot) as (mcp, *_):
        with pytest.raises(ToolError, match="not in cache"):
            await mcp.call_tool("list_roles", {})
    assert find_audit(read_audit(settings), "list_roles", False)


# --- timeout / kick / ban / unban --------------------------------------------


async def test_timeout_sets_timedelta(settings, monkeypatch):
    member = _member()
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "timeout", {"uid": OTHER_UID, "minutes": 10})
    assert got == {"uid": OTHER_UID, "minutes": 10}
    assert member.calls[-1] == (
        "timeout",
        {"until": datetime.timedelta(minutes=10), "reason": None},
    )


async def test_timeout_zero_clears(settings, monkeypatch):
    member = _member()
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "timeout", {"uid": OTHER_UID, "minutes": 0})
    assert got == {"uid": OTHER_UID, "minutes": 0}
    assert member.calls[-1] == ("timeout", {"until": None, "reason": None})


async def test_timeout_rejects_negative(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="non-negative"):
            await mcp.call_tool("timeout", {"uid": OTHER_UID, "minutes": -1})
    assert find_audit(read_audit(settings), "timeout", False)


async def test_kick_happy(settings, monkeypatch):
    member = _member()
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        got = await call_json(mcp, "kick", {"uid": OTHER_UID})
    assert got == {"uid": OTHER_UID, "kicked": True}
    assert member.calls[-1] == ("kick", {"reason": None})


async def test_kick_unknown_member_is_error(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with(members={})) as (mcp, *_):
        with pytest.raises(ToolError, match="NotFound|404"):
            await mcp.call_tool("kick", {"uid": 12345})
    assert find_audit(read_audit(settings), "kick", False)


async def test_ban_happy_no_message_deletion(settings, monkeypatch):
    member = _member()
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        ctx,
        bot,
    ):
        got = await call_json(mcp, "ban", {"uid": OTHER_UID})
    assert got == {"uid": OTHER_UID, "banned": True}
    (kind, kwargs) = bot._guild.calls[-1]
    assert kind == "ban"
    assert kwargs["user"].id == OTHER_UID
    assert kwargs["delete_message_seconds"] == 0
    assert kwargs["reason"] is None


async def test_ban_nonmember_succeeds_via_object(settings, monkeypatch):
    """Guild-level ban needs no member resolution (already-left users bannable)."""
    async with running_server(monkeypatch, settings, _guild_with(members={})) as (mcp, ctx, bot):
        got = await call_json(mcp, "ban", {"uid": 12345})
    assert got == {"uid": 12345, "banned": True}
    (kind, kwargs) = bot._guild.calls[-1]
    assert kind == "ban" and kwargs["user"].id == 12345


async def test_ban_not_found_surfaces(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with(members={})) as (mcp, *_):
        with pytest.raises(ToolError, match="NotFound|404"):
            await mcp.call_tool("ban", {"uid": 424242})
    assert find_audit(read_audit(settings), "ban", False)


async def test_unban_happy(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, ctx, bot):
        got = await call_json(mcp, "unban", {"uid": OTHER_UID})
    assert got == {"uid": OTHER_UID, "unbanned": True}
    (kind, kwargs) = bot._guild.calls[-1]
    assert kind == "unban" and kwargs["user"].id == OTHER_UID and kwargs["reason"] is None


async def test_unban_not_found_surfaces(settings, monkeypatch):
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError, match="NotFound|404"):
            await mcp.call_tool("unban", {"uid": 424242})
    assert find_audit(read_audit(settings), "unban", False)


# --- fix round: required channel_id, Forbidden surface, StrictInt boundary ---


async def test_purge_requires_channel_id(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="(?i)missing|required|field"):
            await mcp.call_tool("purge", {"limit": 3})


async def test_delete_channel_requires_channel_id(settings, monkeypatch):
    channel = make_deletable_channel(CHANNEL_ID, discord.TextChannel)
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="(?i)missing|required|field"):
            await mcp.call_tool("delete_channel", {})
    assert channel.deleted is False


async def test_edit_message_other_user_forbidden(settings, monkeypatch):
    msg = FakeMessage(55, FakeUser(OTHER_UID, "Rando"), "theirs")
    channel = make_text_channel(CHANNEL_ID, messages=[msg])
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool(
                "edit_message", {"message_id": 55, "content": "hijack", "channel_id": CHANNEL_ID}
            )
    assert msg.edited_to is None
    assert find_audit(read_audit(settings), "edit_message", False)


async def test_kick_forbidden_surfaces(settings, monkeypatch):
    member = _member()

    async def boom(*, reason=None):
        raise _forbidden()

    member.kick = boom
    async with running_server(monkeypatch, settings, _guild_with(members={OTHER_UID: member})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool("kick", {"uid": OTHER_UID})
    assert find_audit(read_audit(settings), "kick", False)


async def test_ban_forbidden_surfaces(settings, monkeypatch):
    guild = _guild_with()

    async def boom(user, *, delete_message_seconds=0, reason=None):
        raise _forbidden()

    guild.ban = boom
    async with running_server(monkeypatch, settings, guild) as (mcp, *_):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool("ban", {"uid": OTHER_UID})
    assert find_audit(read_audit(settings), "ban", False)


async def test_purge_forbidden_surfaces(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID, exc=_forbidden())
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool("purge", {"channel_id": CHANNEL_ID, "limit": 3})
    assert find_audit(read_audit(settings), "purge", False)


async def test_delete_channel_forbidden_surfaces(settings, monkeypatch):
    channel = make_deletable_channel(CHANNEL_ID, discord.TextChannel, exc=_forbidden())
    async with running_server(monkeypatch, settings, _guild_with({CHANNEL_ID: channel})) as (
        mcp,
        *_,
    ):
        with pytest.raises(ToolError, match="Forbidden|403|50013"):
            await mcp.call_tool("delete_channel", {"channel_id": CHANNEL_ID})
    assert channel.deleted is False
    assert find_audit(read_audit(settings), "delete_channel", False)


async def test_delete_channel_refuses_thread_via_fetch(settings, monkeypatch):
    thread = MagicMock(spec=discord.Thread)
    thread.id = 888
    thread.guild = SimpleNamespace(id=GUILD_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {}), fetch_map={888: thread})
    async with running_server(monkeypatch, settings, bot) as (mcp, *_):
        with pytest.raises(ToolError, match="outside scoped guild"):
            await mcp.call_tool("delete_channel", {"channel_id": 888})
    assert "fetch_channel:888" in bot.events
    assert find_audit(read_audit(settings), "delete_channel", False)


async def test_strictint_rejects_bool_uid(settings, monkeypatch):
    # Boundary rejection runs before the tool body: no audit record, like the
    # correlated StrictInt params.
    async with running_server(monkeypatch, settings, _guild_with()) as (mcp, *_):
        with pytest.raises(ToolError):
            await mcp.call_tool("get_member", {"uid": True})
    assert not find_audit(read_audit(settings), "get_member", True)


# --- audit is metadata-only; identity policy on all new tools ----------------


async def test_edit_and_kick_audits_hide_content_and_reason(settings, monkeypatch):
    msg = FakeMessage(50, FakeUser(BOT_UID, "ThycaBot", True), "old")
    channel = make_text_channel(CHANNEL_ID, messages=[msg])
    member = _member()
    guild = _guild_with({CHANNEL_ID: channel}, members={OTHER_UID: member})
    async with running_server(monkeypatch, settings, guild) as (mcp, *_):
        await call_json(mcp, "edit_message", {"message_id": 50, "content": "SECRET_CANARY_X7"})
        await call_json(mcp, "kick", {"uid": OTHER_UID, "reason": "SECRET_REASON_Y9"})
    text = settings.audit_path.read_text(encoding="utf-8")
    assert "SECRET_CANARY_X7" not in text
    assert "SECRET_REASON_Y9" not in text
    assert '"content"' not in text and '"reason"' not in text
    edit_recs = find_audit(read_audit(settings), "edit_message", True)
    assert edit_recs[0]["args"]["content_len"] == len("SECRET_CANARY_X7")
    kick_recs = find_audit(read_audit(settings), "kick", True)
    assert kick_recs[0]["args"]["reason_len"] == len("SECRET_REASON_Y9")


async def test_all_new_tools_carry_identity_policy(settings):
    mcp = create_server(settings)
    descs = {t.name: t.description or "" for t in await mcp.list_tools()}
    assert sorted(descs) == sorted(
        [
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
            "send_message",
            "start_task",
            "stop_task",
            "whoami",
            *NEW_TOOLS,
        ]
    )
    for name in NEW_TOOLS:
        assert str(OWNER_UID) in descs[name], name
        assert "author_uid" in descs[name], name
