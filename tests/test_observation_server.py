"""MCP observation-tool tests: record_no_reply + read_context (14-tool server).

Drives the REAL installed lifespan (patched only to inject a fake Discord
bot and fake Jev deps into the otherwise-REAL Responder) via the native
FastMCP ``list_tools``/``call_tool`` API. Delivery is REAL (tmp files);
seeded mid-flight jobs carry synthetic submitted fingerprints (never None)
per the positive-proof requirement. No network, no real .env, no paid
calls, no prod writes.

Channel history is a SEMANTIC fake: it honors ``before``/``after``/
``limit``/``oldest_first`` exactly like the verified native behavior
(``oldest_first=False`` + ``before`` yields newest-first below the bound;
``oldest_first=True`` pages forward from ``after``/start, ignoring
``before``). A regression test proves ``read_context`` returns
source-adjacent recents, never oldest-ever rows.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

import delivery
from config import Settings
from delivery import (
    enqueue_job,
    get_job,
    initialize_delivery_state,
    mark_turn_submitting,
    mark_uncertain,
    record_send_intent,
)
from outbox import append_entry
from responder import Responder
from server import create_server

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 555
GUILD_ID = 333
CHANNEL_ID = 444
OTHER_CHANNEL = 445
FAKE_SID = "2026-01-02T03-04-05_ab12"
SOURCE_MID = 700000000000000007

OLD_TOOLS = [
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


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _jump(mid: int) -> str:
    return f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/{mid}"


# --- semantic fakes ------------------------------------------------------------


class _FakeUser:
    def __init__(self, uid, display_name, bot=False):
        self.id = uid
        self.display_name = display_name
        self.bot = bot


class _FakeMessage:
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
        scope = guild_id if guild_id is not None else "@me"
        self.jump_url = f"https://discord.com/channels/{scope}/{channel_id}/{mid}"
        self.reference = reference
        self.created_at = created_at


class _SemanticHistory:
    """Parameter-honoring history iterator (verified native semantics).

    ``oldest_first=False`` (default): rows with ``after < id < before``,
    newest-first, up to ``limit``. ``oldest_first=True``: rows with
    ``id > after`` (default start 0, ``before`` ignored), oldest-first, up
    to ``limit`` -- exactly the native footgun the collector fix addresses.
    """

    def __init__(self, store, *, limit, before, after, oldest_first):
        self._rows = sorted(store, key=lambda m: m.id)
        self._limit = limit
        self._before = before
        self._after = after
        self._oldest_first = oldest_first
        self.calls: list = []

    def _bound_id(self, bound):
        if bound is None:
            return None
        mid = getattr(bound, "id", None)
        if not isinstance(mid, int) or isinstance(mid, bool):
            raise TypeError("history bound must carry an int id")
        return mid

    async def _collect(self):
        before_id = self._bound_id(self._before)
        after_id = self._bound_id(self._after)
        if self._oldest_first:
            floor = after_id if after_id is not None else 0
            picked = [m for m in self._rows if m.id > floor][: self._limit]
            return picked
        ceil = before_id if before_id is not None else (1 << 63)
        floor = after_id if after_id is not None else 0
        picked = [m for m in self._rows if floor < m.id < ceil]
        picked.reverse()
        return picked[: self._limit]

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in await self._collect():
            yield message


def _deleted_exc():
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def _make_channel(cid, guild_id=GUILD_ID, history=()):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    store: dict = {
        "deleted": set(),
        "exc": None,
        "fetch_calls": [],
        "history": list(history),
        "history_calls": 0,
        "sent": [],
    }

    async def fake_fetch(mid):
        store["fetch_calls"].append(mid)
        hook = store.get("fetch_hook")
        if hook is not None:
            await hook(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_exc()
        return SimpleNamespace(
            id=mid, channel=SimpleNamespace(id=cid), guild=SimpleNamespace(id=guild_id)
        )

    def fake_history(*, limit=100, before=None, after=None, oldest_first=False):
        store["history_calls"] += 1
        return _SemanticHistory(
            store["history"],
            limit=limit,
            before=before,
            after=after,
            oldest_first=oldest_first,
        )

    async def fake_send(content, **kwargs):
        msg = _FakeMessage(
            9000 + len(store["sent"]),
            _FakeUser(BOT_UID, "B", True),
            channel_id=cid,
            guild_id=guild_id,
            reference=kwargs.get("reference"),
        )
        store["sent"].append((content, kwargs))
        return msg

    channel.fetch_message = fake_fetch
    channel.history = fake_history
    channel.send = fake_send
    channel.store = store
    return channel


class _FakeGuild:
    def __init__(self, gid, channels=None):
        self.id = gid
        self._channels = dict(channels or {})

    def get_channel(self, cid):
        channel = self._channels.get(cid)
        if channel is None:
            channel = _make_channel(cid, self.id)
            self._channels[cid] = channel
        return channel

    def get_member(self, uid):
        return None


class _FakeBot:
    def __init__(self, guild, fetch_map=None):
        self.user = _FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self.listeners: dict = {}
        self._ready = False
        self._closed = False
        self._close_event = asyncio.Event()

    def add_listener(self, func, name):
        self.listeners.setdefault(name, []).append(func)

    def remove_listener(self, func, name):
        try:
            self.listeners.get(name, []).remove(func)
        except ValueError:
            pass

    async def login(self, token):
        pass

    async def connect(self):
        await self._close_event.wait()

    async def wait_until_ready(self):
        self._ready = True

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
        self._closed = True
        self._close_event.set()

    def get_guild(self, gid):
        return self._guild if gid == self._guild.id else None

    async def fetch_channel(self, cid):
        if cid in self._fetch_map:
            return self._fetch_map[cid]
        raise LookupError("channel not found")

    def get_user(self, uid):
        return None

    async def fetch_user(self, uid):
        raise LookupError("user not found")


class _StubAdapter:
    async def decide(self, state):
        raise AssertionError("MCP tool tests never classify")


async def _stub_collector(entry):
    raise AssertionError("MCP tool tests never collect")


# Fixed synthetic activation identity for the injected fake ledger: every
# Responder in this module binds the same marker, so restart-style second
# constructions rebind idempotently (never conflict, never rewrite).
# Fake injected fixture only, never a production bypass.
_STUB_LEDGER_ID = "1e" * 16


class _StubLedger:
    @property
    def ledger_id(self) -> str:
        return _STUB_LEDGER_ID

    async def classify(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never classify")


class _StubNative:
    async def get_session_snapshot(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never snapshot")

    async def submit_turn(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never POST")

    async def get_trace_evidence(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never trace")


# --- harness --------------------------------------------------------------------


def _settings_kwargs(tmp_path):
    return dict(
        _env_file=None,
        discord_token="fake",
        owner_uid=OWNER_UID,
        guild_id=GUILD_ID,
        default_channel_id=CHANNEL_ID,
        session_id="discord-test",
        audit_path=tmp_path / "audit.jsonl",
        outbox_path=tmp_path / "outbox.jsonl",
        outbox_state_path=tmp_path / "outbox_state.json",
        watch_channel_ids=[CHANNEL_ID],
        thyca_session_mapping_path=tmp_path / "mapping.json",
        delivery_state_path=tmp_path / "delivery.json",
        responder_check_seconds=0.05,
        thyca_request_timeout_seconds=5.0,
        recovery_poll_seconds=30.0,
    )


@pytest.fixture
def settings(tmp_path):
    settings = Settings(**_settings_kwargs(tmp_path))
    settings.thyca_session_mapping_path.write_text(
        json.dumps(
            {
                "native_id": FAKE_SID,
                "title": settings.session_id,
                "audit_label": settings.session_id,
                "created_at": "2026-01-02T03:04:05+00:00",
                "path": f"/tmp/fake-sessions/{FAKE_SID}.jsonl",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(settings.thyca_session_mapping_path, 0o600)
    initialize_delivery_state(
        settings.delivery_state_path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    return settings


@asynccontextmanager
async def _running(monkeypatch, settings, bot):
    """Real lifespan with a fake bot and stub-Jev REAL Responder; loops parked."""
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr(
        "server.core.Responder",
        lambda s, b: Responder(
            s,
            b,
            client=_StubNative(),
            jev_adapter=_StubAdapter(),
            context_collector=_stub_collector,
            participation_ledger=_StubLedger(),
        ),
    )
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx, bot


async def _call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def _audit_text(settings):
    if not settings.audit_path.exists():
        return ""
    return settings.audit_path.read_text(encoding="utf-8")


def _audit_records(settings, tool):
    out = []
    if not settings.audit_path.exists():
        return out
    for line in settings.audit_path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("tool") == tool:
            out.append(record)
    return out


def _seed_need_context(settings, mid=SOURCE_MID, seq=1, job_ref="job-need-1"):
    """Queued job + Jev need_context bind + turn_submitting WITH synthetic fp.

    The submitted fingerprint is synthetic but well-formed (64-hex + len);
    None is never used (positive proof is never weakened for fixtures).
    """
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
        author_is_bot=False,
    )
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=OWNER_UID,
        actor_kind="owner",
        seq=seq,
    )
    delivery.bind_jev_decision(
        settings.delivery_state_path,
        CHANNEL_ID,
        mid,
        context_sha256=_sha(f"ctx-{mid}"),
        attempt_id=_sha(f"att-{mid}")[:32],
        model="jev-1.13.0",
        participation="need_context",
    )
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL_ID,
        mid,
        job_ref=job_ref,
        pre_turn_index=0,
        submitted_text_sha256=_sha(f"post-{mid}"),
        submitted_text_len=100,
    )
    return job_ref


# --- tool surface -----------------------------------------------------------------


async def test_tool_count_15_old_12_unchanged(settings):
    mcp = create_server(settings)
    tools = await mcp.list_tools()
    names = sorted(t.name for t in tools)
    assert len(names) == 15
    for old in OLD_TOOLS:
        assert old in names
    assert "read_context" in names and "record_no_reply" in names
    assert "resolve_uncertain_drop" in names
    descs = {t.name: t.description or "" for t in tools}
    # Old tool contracts intact (identity policy + correlation wording).
    assert str(OWNER_UID) in descs["send_message"]
    assert "author_uid" in descs["whoami"]
    assert "USER.md" in descs["read_outbox"]
    # New tools carry their contracts.
    assert "no_reply_needed" in descs["record_no_reply"]
    assert "NEVER terminal" in descs["record_no_reply"]
    assert "zero send evidence" in descs["resolve_uncertain_drop"]
    assert "operator_approved_drop" in descs["resolve_uncertain_drop"]
    assert "1..20" in descs["read_context"]
    assert "never auto-forwarded" in descs["read_context"]


async def test_manual_send_unchanged(settings, monkeypatch):
    """Unrelated manual sends bypass correlation/Jev entirely (plain send)."""
    channel = _make_channel(CHANNEL_ID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(mcp, "send_message", {"content": "plain manual"})
    assert payload["channel_id"] == CHANNEL_ID
    assert payload["message_id"] == 9000
    assert channel.store["sent"][0][0] == "plain manual"
    assert channel.store["sent"][0][1] == {}  # no reply reference on manual sends


# --- record_no_reply ----------------------------------------------------------------


async def test_record_no_reply_declared_not_terminal_no_ack(settings, monkeypatch):
    ref = _seed_need_context(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
        # Assert inside the lifespan: orderly shutdown marks in-flight uncertain.
        job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
        assert job["stage"] == "turn_submitting"  # NEVER terminal at tool time
        assert job["no_reply_declaration"]["reason"] == "no_reply_needed"
        assert job["no_reply_declaration"]["job_ref"] == ref
        assert job["send_intents"] == [] and job["send_receipts"] == []
    assert payload == {
        "status": "declared",
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
        "job_ref": ref,
        "reason": "no_reply_needed",
    }


async def test_record_no_reply_idempotent_same_reason(settings, monkeypatch):
    ref = _seed_need_context(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        first = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "missing_context"},
        )
        second = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "missing_context"},
        )
    assert first["status"] == second["status"] == "declared"
    job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
    assert job["no_reply_declaration"]["reason"] == "missing_context"


async def test_record_no_reply_rejects_mismatched_reason(settings, monkeypatch):
    ref = _seed_need_context(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "missing_context"},
            )
    job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
    assert job["no_reply_declaration"]["reason"] == "no_reply_needed"  # first wins


@pytest.mark.parametrize(
    "args",
    [
        {"source_message_id": 999, "job_ref": "job-need-1", "reason": "no_reply_needed"},
        {"source_message_id": SOURCE_MID, "job_ref": "wrong-ref", "reason": "no_reply_needed"},
        {
            "source_message_id": SOURCE_MID,
            "job_ref": "job-need-1",
            "reason": "no_reply_needed",
            "channel_id": OTHER_CHANNEL,
        },
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "reason": "bogus"},
        {"source_message_id": 0, "job_ref": "job-need-1", "reason": "no_reply_needed"},
        {"source_message_id": SOURCE_MID, "job_ref": "", "reason": "no_reply_needed"},
    ],
)
async def test_record_no_reply_rejects_bad_correlation(settings, monkeypatch, args):
    _seed_need_context(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(mcp, "record_no_reply", args)
    job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
    assert job.get("no_reply_declaration") is None  # zero mutation on reject


async def test_record_no_reply_invalid_reason_canary_not_audited(settings, monkeypatch):
    ref = _seed_need_context(settings)
    canary = "CANARY-PRIVATE-REASON-xyz"
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": canary},
            )
    assert canary not in _audit_text(settings)
    failures = [r for r in _audit_records(settings, "record_no_reply") if not r["ok"]]
    assert failures and failures[0]["args"].get("reason_len") == len(canary)


async def test_record_no_reply_rejects_send_evidence(settings, monkeypatch):
    ref = _seed_need_context(settings)
    record_send_intent(
        settings.delivery_state_path,
        CHANNEL_ID,
        SOURCE_MID,
        send_ref="s1",
        content_len=2,
        content_hash=_sha("hi"),
    )
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
    assert (
        get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID).get("no_reply_declaration")
        is None
    )


async def test_record_no_reply_rejects_respond_binding(settings, monkeypatch):
    """Delivery enforces bound-need_context: a respond job cannot declare."""
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=SOURCE_MID,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content="m",
        author_is_bot=False,
    )
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=SOURCE_MID,
        author_uid=OWNER_UID,
        actor_kind="owner",
        seq=1,
    )
    delivery.bind_jev_decision(
        settings.delivery_state_path,
        CHANNEL_ID,
        SOURCE_MID,
        context_sha256=_sha("ctx"),
        attempt_id=_sha("att")[:32],
        model="jev-1.13.0",
        participation="respond",
    )
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL_ID,
        SOURCE_MID,
        job_ref="job-r",
        pre_turn_index=0,
        submitted_text_sha256=_sha("p"),
        submitted_text_len=10,
    )
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": "job-r", "reason": "no_reply_needed"},
            )


async def test_record_no_reply_source_deleted_skips(settings, monkeypatch):
    ref = _seed_need_context(settings)
    channel = _make_channel(CHANNEL_ID)
    channel.store["deleted"].add(SOURCE_MID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
    assert payload["status"] == "skipped" and payload["reason"] == "source_deleted"
    job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"
    assert job.get("no_reply_declaration") is None


async def test_record_no_reply_source_unavailable_no_mutation(settings, monkeypatch):
    ref = _seed_need_context(settings)
    channel = _make_channel(CHANNEL_ID)
    channel.store["exc"] = discord.Forbidden(
        SimpleNamespace(status=403, reason="Forbidden"), {"code": 50013, "message": "x"}
    )
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
        # Assert inside the lifespan: shutdown marks in-flight uncertain.
        job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
        assert job["stage"] == "turn_submitting"
        assert job.get("no_reply_declaration") is None


async def test_record_no_reply_post_await_race_rejected(settings, monkeypatch):
    """A terminal landing during the source await fails the post-await guard."""

    ref = _seed_need_context(settings)
    channel = _make_channel(CHANNEL_ID)

    async def race(mid):
        mark_uncertain(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID, kind="timeout")

    channel.store["fetch_hook"] = race
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
    job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
    assert job["stage"] == "uncertain"
    assert job.get("no_reply_declaration") is None


async def test_record_no_reply_latched_job_rejected(settings, monkeypatch):
    """A prior ambiguous send hold fences later declarations for the lifetime."""
    _seed_need_context(settings, job_ref="job-latch")
    channel = _make_channel(CHANNEL_ID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        # Ambiguous send hold: force the intent write to fail uncertain by
        # racing a terminal between correlation and intent is complex here;
        # instead drive the real latch via a failing receipt path is also
        # complex. Direct approach: poison then send once to latch via SDK
        # failure, which _preserve_uncertain always latches.
        async def boom_send(content, **kwargs):
            raise discord.HTTPException(
                SimpleNamespace(status=500, reason="x"), {"message": "boom"}
            )

        channel.send = boom_send
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "send_message",
                {"content": "chunk", "source_message_id": SOURCE_MID, "job_ref": "job-latch"},
            )
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {
                    "source_message_id": SOURCE_MID,
                    "job_ref": "job-latch",
                    "reason": "no_reply_needed",
                },
            )


# --- read_context -------------------------------------------------------------------


def _history_store(base_mid, count, author_uid=OTHER_UID, name="Ramon"):
    import datetime

    store = []
    for i in range(count):
        mid = base_mid - count + i
        store.append(
            _FakeMessage(
                mid,
                _FakeUser(author_uid, name),
                content=f"history body {mid}",
                reference=None,
                created_at=datetime.datetime(2026, 10, 5, 12, 0, 0),
            )
        )
    return store


async def test_read_context_returns_source_adjacent_recents(settings, monkeypatch):
    """Semantic regression: newest-before-source, never oldest-ever rows."""
    ref = _seed_need_context(settings)
    store = _history_store(SOURCE_MID, 100)
    channel = _make_channel(CHANNEL_ID, history=store)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 5},
        )
    assert payload["count"] == 5
    got = [m["message_id"] for m in payload["messages"]]
    assert got == [SOURCE_MID - 5, SOURCE_MID - 4, SOURCE_MID - 3, SOURCE_MID - 2, SOURCE_MID - 1]
    first = payload["messages"][0]
    assert first["author_uid"] == OTHER_UID and first["author_name"] == "Ramon"
    assert first["content"] == f"history body {SOURCE_MID - 5}"
    assert first["reply_to_message_id"] is None
    assert "2026-10-05" in first["timestamp"]
    # Bodies are served to the agent but never touch the audit log.
    assert f"history body {SOURCE_MID - 5}" not in _audit_text(settings)
    records = [r for r in _audit_records(settings, "read_context") if r["ok"]]
    assert records and records[0]["args"]["returned_count"] == 5
    assert records[0]["args"]["message_ids"] == got


async def test_read_context_paginates_backwards_never_future(settings, monkeypatch):
    ref = _seed_need_context(settings)
    store = _history_store(SOURCE_MID, 40)
    channel = _make_channel(CHANNEL_ID, history=store)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        page1 = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 10},
        )
        oldest = page1["messages"][0]["message_id"]
        page2 = await _call_json(
            mcp,
            "read_context",
            {
                "source_message_id": SOURCE_MID,
                "job_ref": ref,
                "before_message_id": oldest,
                "limit": 10,
            },
        )
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "read_context",
                {
                    "source_message_id": SOURCE_MID,
                    "job_ref": ref,
                    "before_message_id": SOURCE_MID + 1,
                    "limit": 10,
                },
            )
    assert [m["message_id"] for m in page1["messages"]][-1] == SOURCE_MID - 1
    assert [m["message_id"] for m in page2["messages"]][-1] == oldest - 1
    assert all(m["message_id"] < SOURCE_MID for m in page2["messages"])


@pytest.mark.parametrize(
    "args",
    [
        {"source_message_id": 999, "job_ref": "job-need-1"},
        {"source_message_id": SOURCE_MID, "job_ref": "wrong"},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "limit": 0},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "limit": 21},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "channel_id": OTHER_CHANNEL},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "before_message_id": 0},
    ],
)
async def test_read_context_rejects_bad_input(settings, monkeypatch, args):
    _seed_need_context(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(mcp, "read_context", args)


async def test_read_context_validates_each_message(settings, monkeypatch):
    """A malformed row fails the call; no partial context is served."""
    import datetime

    ref = _seed_need_context(settings)
    good = _history_store(SOURCE_MID, 3)
    bad = _FakeMessage(
        SOURCE_MID - 4,
        _FakeUser(OTHER_UID, "R"),
        content="bad row",
        channel_id=OTHER_CHANNEL,  # scope violation inside the page
        created_at=datetime.datetime(2026, 10, 5, 12, 0, 0),
    )
    channel = _make_channel(CHANNEL_ID, history=[bad, *good])
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "read_context",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 10},
            )


async def test_read_context_never_touches_other_channel(settings, monkeypatch):
    ref = _seed_need_context(settings)
    main = _make_channel(CHANNEL_ID, history=_history_store(SOURCE_MID, 5))
    other = _make_channel(OTHER_CHANNEL, history=_history_store(SOURCE_MID, 5))
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: main, OTHER_CHANNEL: other}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 5},
        )
    assert payload["count"] == 5
    assert main.store["history_calls"] == 1
    assert other.store["history_calls"] == 0
    assert all(m["message_id"] < SOURCE_MID for m in payload["messages"])


async def test_read_context_post_await_race_rejected(settings, monkeypatch):
    """A terminal landing during the history await fails the freshness guard."""
    ref = _seed_need_context(settings)
    channel = _make_channel(CHANNEL_ID, history=_history_store(SOURCE_MID, 5))
    orig_history = channel.history

    def racing_history(**kwargs):
        mark_uncertain(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID, kind="timeout")
        return orig_history(**kwargs)

    channel.history = racing_history
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "read_context",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 5},
            )


# --- resolve_uncertain_drop -------------------------------------------------------


def _seed_uncertain(settings, mid=SOURCE_MID, seq=1):
    """Queued job driven to uncertain with zero send evidence."""
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=OWNER_UID,
        actor_kind="owner",
        seq=seq,
    )
    mark_uncertain(settings.delivery_state_path, CHANNEL_ID, mid, kind="trace_unverified")


async def test_resolve_uncertain_drop_happy_path(settings, monkeypatch):
    _seed_uncertain(settings)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(mcp, "resolve_uncertain_drop", {"source_message_id": SOURCE_MID})
        job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
        assert job["stage"] == "skipped"
        assert job["skip_reason"] == "operator_approved_drop"
        assert job["error"] is None
    assert payload == {
        "status": "skipped",
        "reason": "operator_approved_drop",
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
        "seq": 1,
    }
    audited = _audit_records(settings, "resolve_uncertain_drop")
    assert audited and all(r["ok"] is True for r in audited)


async def test_resolve_uncertain_drop_rejects(settings, monkeypatch):
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):  # unknown job
            await _call_json(mcp, "resolve_uncertain_drop", {"source_message_id": 1})
        with pytest.raises(ToolError):  # out of scope
            await _call_json(
                mcp,
                "resolve_uncertain_drop",
                {"source_message_id": SOURCE_MID, "channel_id": CHANNEL_ID + 1},
            )
        with pytest.raises(ToolError):  # bad id
            await _call_json(mcp, "resolve_uncertain_drop", {"source_message_id": -5})
    audited = _audit_records(settings, "resolve_uncertain_drop")
    assert audited and all(r["ok"] is False for r in audited)


async def test_resolve_uncertain_drop_rejects_non_uncertain(settings, monkeypatch):
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=SOURCE_MID,
        author_uid=OWNER_UID,
        actor_kind="owner",
        seq=1,
    )
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError):  # queued, never uncertain
            await _call_json(mcp, "resolve_uncertain_drop", {"source_message_id": SOURCE_MID})
        job = get_job(settings.delivery_state_path, CHANNEL_ID, SOURCE_MID)
        assert job["stage"] == "queued"
