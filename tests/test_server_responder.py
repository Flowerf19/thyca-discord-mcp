"""V5 server<->responder integration: always-on lifecycle + agent-owned MCP send.

Uses the REAL Responder and the REAL delivery journal with a fake Thyca backend
and fake Discord objects — no network, no real .env/state (all paths are tmp
fixtures with a fake SID/token). The native FastMCP public API
(``list_tools``/``call_tool``) and the real installed lifespan drive every test.

Positive auto-flow tests in this file encode the approved end-to-end behavior
(listener -> native auto loop -> fake POST -> same-loop MCP send callback ->
intent + SDK receipt -> Trace proof -> worker completion -> controller ACK).
They REQUIRE the final Core/Worker/Client APIs; while a peer API is pending
they fail with the real dependency error (reported, never mocked away).
"""

import asyncio
import errno
import hashlib
import json
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from config import Settings
from delivery import (
    DeliveryDurabilityUncertain,
    DeliveryError,
    enqueue_job,
    get_job,
    initialize_delivery_state,
    list_jobs,
    mark_turn_submitting,
    mark_uncertain,
    skip_job,
)
from outbox import append_entry, read_entries
from responder import Responder
from server import create_server
from thyca import ThycaBusyError

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
FAKE_SID = "2026-01-02T03-04-05_ab12"


# --- fakes (self-contained, native-faithful) ---------------------------------


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
    def __init__(self, gid, channels=None, member_count=10, members=None):
        self.id = gid
        self._channels = dict(channels or {})
        self.member_count = member_count
        self._members = dict(members or {})

    def get_channel(self, cid):
        return self._channels.get(cid)

    def get_member(self, uid):
        return self._members.get(uid)

    async def fetch_member(self, uid):
        try:
            return self._members[uid]
        except KeyError:
            raise LookupError(f"member {uid} not found") from None


class FakeBot:
    """Minimal bot surface: login/connect/ready/channels/listeners/user lookup."""

    def __init__(self, guild, *, user=None, fetch_map=None, login_exc=None, users=None):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self._login_exc = login_exc
        self._users = dict(users or {})
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

    def get_user(self, uid):
        return self._users.get(uid)

    async def fetch_user(self, uid):
        try:
            return self._users[uid]
        except KeyError:
            raise LookupError(f"user {uid} not found") from None


def make_history(messages=()):
    """Fake TextChannel.history: records kwargs, yields configured messages."""
    calls: list[dict] = []
    state = {"messages": list(messages)}

    def history(**kwargs):
        calls.append(kwargs)

        async def gen():
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
    """Spec'd TextChannel mock with native-faithful send (reference + hooks)."""
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    sent: list[FakeMessage] = []
    hooks: dict = {"send_exc": None, "send_hook": None}
    store: dict = {"deleted": set(), "exc": None, "calls": []}

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

    async def fake_send(content, **kwargs):
        if hooks["send_exc"] is not None:
            raise hooks["send_exc"]
        if hooks["send_hook"] is not None:
            return await hooks["send_hook"](content, **kwargs)
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

    # isinstance(channel, discord.TextChannel) passes via the Mock spec (v4 pattern).
    channel.send = fake_send
    channel.fetch_message = fake_fetch
    channel.history = history or make_history()[0]
    channel.sent = sent
    channel.hooks = hooks
    channel.source_store = store
    return channel


class FakeThycaBackend:
    """Scripted native Thyca backend with same-loop MCP send callback.

    ``submit_turn`` reads the FRESH job (job_ref) from the persisted delivery
    store — never fabricated — then calls back into the SAME server/loop via
    ``mcp.call_tool('send_message', ...)`` before the POST returns. Trace
    evidence mirrors the real client contract: exact own-request SHA/len,
    per-call content hashes, and parsed tool-result JSON (message_id /
    channel_id / jump_url) for 1:1 intent+receipt matching.
    """

    def __init__(self, delivery_path, *, pre_max=3, reply_text="fake agent reply"):
        self.delivery_path = delivery_path
        self.pre_max = pre_max
        self.reply_text = reply_text
        self.mcp = None
        self.posts: list[str] = []
        self.tool_calls: list[dict] = []
        self.trace_requests: list[int] = []
        self.call_seq = 0
        self.posted = asyncio.Event()
        # Own-trace uncorrelated send: THIS turn's trace reports an extra
        # uncorrelated send (legacy_uncorrelated_count=1) -> hold uncertain.
        self.own_trace_uncorrelated_content: str | None = None
        # Foreign-manual send: another SID sharing the pool sends a plain
        # manual message. It hits the SDK but never appears in OUR trace
        # (count stays 0) -> not attributed, our job may still complete.
        self.foreign_manual_content: str | None = None

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        assert native_sid == FAKE_SID
        total = self.pre_max + 1 + len(self.posts)
        return {
            "native_sid": FAKE_SID,
            "native_id_match": True,
            "title_match": True,
            "title_len": 5,
            "title_sha256": "0" * 64,
            "running": False,
            "turn_count": total,
            "turn_indices": list(range(total)),
            "pre_max_turn_index": (total - 1) if total else None,
            "message_count": total,
            "proof": "fake",
        }

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        assert native_sid == FAKE_SID
        self.posts.append(text)
        subs = [j for j in list_jobs(self.delivery_path) if j["stage"] == "turn_submitting"]
        assert len(subs) == 1
        job = subs[0]
        assert job["job_ref"] in text  # prompt carries the nonce delimiters
        res = await self.mcp.call_tool(
            "send_message",
            {
                "content": self.reply_text,
                "channel_id": job["channel_id"],
                "source_message_id": job["message_id"],
                "job_ref": job["job_ref"],
            },
        )
        blocks = res[0] if isinstance(res, tuple) else res
        payload = json.loads(blocks[0].text)
        self.call_seq += 1
        self.tool_calls.append(
            {
                "call_id": f"call_{self.call_seq}",
                "content": self.reply_text,
                "job_ref": job["job_ref"],
                "source_mid": job["message_id"],
                "channel_id": job["channel_id"],
                "result": payload,
            }
        )
        if self.own_trace_uncorrelated_content is not None:
            # Uncorrelated send on OUR trace: the native evidence below
            # reports it, so the worker must hold (never complete).
            await self.mcp.call_tool(
                "send_message",
                {
                    "content": self.own_trace_uncorrelated_content,
                    "channel_id": job["channel_id"],
                },
            )
        if self.foreign_manual_content is not None:
            # Foreign SID manual: SDK side effect only, absent from our
            # trace -> never attributed to this job.
            await self.mcp.call_tool(
                "send_message",
                {"content": self.foreign_manual_content, "channel_id": job["channel_id"]},
            )
        self.posted.set()
        return {"cancelled": False, "completed": True, "native_id_match": True}

    async def get_trace_evidence(
        self, native_sid, turn_index, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        assert native_sid == FAKE_SID
        self.trace_requests.append(turn_index)
        submitted = self.posts[-1]
        sends = []
        for call in self.tool_calls:
            if call["job_ref"] != job_ref:
                continue
            res = call["result"]
            sends.append(
                {
                    "tool_call_id": call["call_id"],
                    "channel_match": True,
                    "source_mid_match": True,
                    "job_ref_match": True,
                    "correlated": True,
                    "result_present": True,
                    "result_message_id": res["message_id"],
                    "result_channel_match": res["channel_id"] == channel_id,
                    "jump_present": True,
                    "jump_ids_match": True,
                    "result_ok": True,
                    "content_len": len(call["content"]),
                    "content_sha256": hashlib.sha256(call["content"].encode()).hexdigest(),
                }
            )
        return {
            "request_input_sha256": hashlib.sha256(submitted.encode()).hexdigest(),
            "request_input_len": len(submitted),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
            "all_sends_confirmed": True,
            "legacy_uncorrelated_count": 1
            if self.own_trace_uncorrelated_content is not None
            else 0,
            "sends": sends,
        }


class BusyThycaBackend:
    """Backend whose snapshot reports busy: tick must hold without any POST."""

    def __init__(self):
        self.posts: list[str] = []

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        raise ThycaBusyError()

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        raise AssertionError("no POST must be attempted while the native side is busy")

    async def get_trace_evidence(
        self, native_sid, turn_index, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        raise AssertionError("no trace proof exists without a POST")


class PoisonThycaBackend:
    """Backend whose snapshot raises a NON-Thyca error: the tick must propagate it.

    A plain RuntimeError (never ThycaClientError) bypasses the worker's
    snapshot/busy handling, so the server auto-tick wrapper is the layer
    that audits coarse + stops the loop. Used with the REAL Responder."""

    def __init__(self, exc):
        self.exc = exc
        self.snapshots = 0

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        self.snapshots += 1
        raise self.exc

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        raise AssertionError("unreachable: snapshot poison fires first")

    async def get_trace_evidence(
        self, native_sid, turn_index, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        raise AssertionError("unreachable: snapshot poison fires first")


# --- fixtures / helpers ------------------------------------------------------


def _settings_kwargs(tmp_path):
    """Fixture kwargs: tmp/fake-only, immune to ambient .env and env vars."""
    return dict(
        # No real dotenv/config reads: constructor pins everything the suite
        # depends on (init args beat env, and _env_file=None disables .env).
        _env_file=None,
        discord_token="fake-token-unit-only",
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
        responder_check_seconds=0.05,
        thyca_request_timeout_seconds=5.0,
        recovery_poll_seconds=30.0,
    )


def _make_settings(tmp_path, **overrides):
    kwargs = _settings_kwargs(tmp_path)
    kwargs.update(overrides)
    return Settings(**kwargs)


@pytest.fixture
def settings(tmp_path):
    return _make_settings(tmp_path)


def write_mapping(settings, *, sid=FAKE_SID):
    """Write a valid 0600 native mapping for the configured session id."""
    settings.thyca_session_mapping_path.write_text(
        json.dumps(
            {
                "native_id": sid,
                "title": settings.session_id,
                "audit_label": settings.session_id,
                "created_at": "2026-01-02T03:04:05+00:00",
                "path": f"/fake/native/{sid}.json",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(settings.thyca_session_mapping_path, 0o600)


def init_delivery(settings, *, sid=FAKE_SID):
    return initialize_delivery_state(
        settings.delivery_state_path,
        native_session_id=sid,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )


# -- legacy Jev stub (module-only autouse; preserves pre-routing regressions) --
# Every Responder built in THIS module gets a fake respond-only Jev stack
# unless the test passes explicit deps. The fake binds a REAL delivery
# jev_decision (respond, 32-hex attempt) so the full submit/POST/trace/ACK
# path runs unchanged; the fake provider is never called (it raises). New
# routing tests do NOT use this fixture (explicit deps only).


class _LegacyJevAdapter:
    def __init__(self):
        self.calls = 0

    async def decide(self, state):
        self.calls += 1
        raise AssertionError("legacy fake ledger must never call the provider")


def _legacy_collector(owner_uid, bot_uid):
    async def collect(entry):
        mid = entry["message_id"]
        author = entry["author_uid"]
        parts = [
            {"uid": str(owner_uid), "display_name": "Owner"},
            {"uid": str(bot_uid), "display_name": "Bot"},
        ]
        if str(author) not in (str(owner_uid), str(bot_uid)):
            parts.append(
                {
                    "uid": str(author),
                    "display_name": entry.get("author_name") or f"uid-{author}",
                }
            )
        return {
            "assistant_uid": str(bot_uid),
            "owner_uid": str(owner_uid),
            "participants": parts,
            "messages": [{"id": str(mid), "sender_uid": str(author), "text": entry["content"]}],
            "new_message_id": str(mid),
        }

    return collect


# Fixed synthetic activation identity for the legacy fake ledger: every
# Responder in this module binds the same marker, so restart-style second
# constructions rebind idempotently (never conflict, never rewrite).
_LEGACY_LEDGER_ID = "1e" * 16


class _LegacyLedger:
    def __init__(self):
        self.calls = 0

    @property
    def ledger_id(self) -> str:
        return _LEGACY_LEDGER_ID

    async def classify(self, adapter, state, *, channel_id, message_id, author_uid):
        self.calls += 1
        from context import context_fingerprint

        attempt = hashlib.sha256(f"{channel_id}:{message_id}".encode()).hexdigest()[:32]
        return {
            "status": "decided",
            "fingerprint": context_fingerprint(state),
            "attempt_id": attempt,
            "model": "jev-1.13.0",
            "participation": "respond",
        }


@pytest.fixture(autouse=True)
def _inject_legacy_jev(monkeypatch):
    orig_init = Responder.__init__

    def patched(
        self,
        settings,
        bot,
        *,
        client=None,
        source_checker=None,
        jev_adapter=None,
        context_collector=None,
        participation_ledger=None,
    ):
        if jev_adapter is None:
            jev_adapter = _LegacyJevAdapter()
        if context_collector is None:
            context_collector = _legacy_collector(settings.owner_uid, bot.user.id)
        if participation_ledger is None:
            participation_ledger = _LegacyLedger()
        return orig_init(
            self,
            settings,
            bot,
            client=client,
            source_checker=source_checker,
            jev_adapter=jev_adapter,
            context_collector=context_collector,
            participation_ledger=participation_ledger,
        )

    monkeypatch.setattr(Responder, "__init__", patched)


def seed_active_job(settings, *, mid=701, seq=1, author_uid=OWNER_UID, actor_kind="owner"):
    """Outbox entry + queued job advanced to turn_submitting (POST started)."""
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        author_name="Owner" if author_uid == OWNER_UID else "Rando",
        actor_kind=actor_kind,
        content=f"m{mid}",
        author_is_bot=False,
    )
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        actor_kind=actor_kind,
        seq=seq,
    )
    job_ref = f"job-{CHANNEL_ID}-{mid}-{seq}"
    mark_turn_submitting(
        settings.delivery_state_path, CHANNEL_ID, mid, job_ref=job_ref, pre_turn_index=3
    )
    return job_ref


@asynccontextmanager
async def parked_server(monkeypatch, settings, bot):
    """Real lifespan with the REAL Responder; v6 never registers auto loops."""
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        yield mcp, ctx, bot


def wire_auto_loops(ctx, settings):
    """Recreate the retired lifespan loop wiring for deprecated-path tests.

    v6: the lifespan never registers the auto-reply worker or recovery poll —
    tests that exercise the loop wrapper itself (audit, self-stop on failure)
    rebuild the same objects here and may then ``start``/``coro()`` them.
    """
    import server.core as _core
    from server_ticks import make_auto_reply_tick, make_poll_tick

    def sink(*args, **kwargs):  # same late-resolution shape as _audit_sink
        return _core.append_audit(*args, **kwargs)

    tick, holder = make_auto_reply_tick(settings, ctx["responder"], sink)
    holder["loop"] = ctx["tasks"].register(
        "auto-reply", tick, seconds=settings.responder_check_seconds, protected=True
    )
    ctx["tasks"].register(
        "auto-reply-recovery",
        make_poll_tick(
            settings, settings.default_channel_id, lambda: ctx["bot"], sink
        ),
        seconds=settings.recovery_poll_seconds,
        protected=True,
    )


async def call_json(mcp, name, args=None):
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


# --- lifecycle: always-on default with the real Responder --------------------


async def test_lifespan_builds_responder_but_no_auto_loops(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        # v6: the deprecated worker path is kept but NEVER auto-started —
        # the dedicated bot owns the worker now.
        assert isinstance(ctx["responder"], Responder)
        assert ctx["responder"].native_sid == FAKE_SID
        assert ctx["tasks"].status("heartbeat").running is True
        assert {t.name for t in ctx["tasks"].list_tasks()} == {"heartbeat"}
    assert all(not t.running for t in ctx["tasks"].list_tasks())
    assert bot.is_closed()


async def test_missing_mapping_runs_pure_gateway(settings, monkeypatch):
    init_delivery(settings)  # journal fine, mapping absent
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    # v6: no Thyca session mapping -> pure Discord gateway mode. The lifespan
    # still starts Discord + outbox; deprecated correlated tools report
    # "auto controller not running" instead of failing startup.
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["responder"] is None
        assert ctx["tasks"].status("heartbeat").running is True
        with pytest.raises(ToolError, match="auto controller not running"):
            await mcp.call_tool(
                "record_no_reply",
                {"source_message_id": 1, "job_ref": "x", "reason": "no_reply_needed"},
            )
    assert bot.is_closed()
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


async def test_broken_mapping_still_runs_pure_gateway(settings, monkeypatch):
    """Corrupt legacy state no longer blocks startup (fail-open deprecated path)."""
    settings.thyca_session_mapping_path.write_text("corrupt{", encoding="utf-8")
    init_delivery(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["responder"] is None
        assert ctx["tasks"].status("heartbeat").running is True
    assert bot.is_closed()


async def test_delivery_sid_mismatch_keeps_gateway_up(settings, monkeypatch):
    write_mapping(settings, sid=FAKE_SID)
    init_delivery(settings, sid="2026-03-04T05-06-07_cd34")  # outside-SID journal
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    # v6: the deprecated worker path is best-effort — a broken legacy mapping
    # or journal disables it instead of blocking Discord transport startup.
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["responder"] is None
        assert ctx["tasks"].status("heartbeat").running is True
    assert bot.is_closed()


async def test_login_failure_closes_real_responder_no_orphans(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)  # mid-flight at shutdown -> uncertain, not dropped
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        login_exc=discord.LoginFailure("bad token"),
    )
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    with pytest.raises(discord.LoginFailure, match="bad token"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - login must raise first
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert bot.listeners.get("on_message") == []
    assert bot.is_closed()
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "uncertain"
    assert job["error"]["kind"] == "delivery_unknown"
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


# --- lifespan cleanup without sys.exception (3.10-safe exc_info) ----------------


class _FailingCloseRealResponder(Responder):
    """REAL Responder whose close() fails: cleanup-failure shutdown coverage."""

    async def close(self):
        raise RuntimeError("responder close boom")


async def test_lifespan_cleanup_without_sys_exception_keeps_primary(settings, monkeypatch):
    """3.10-safe cleanup: sys.exc_info()[1] keeps the primary with no sys.exception.

    sys.exception is 3.11+ only; the lifespan cleanup must use the 3.10-safe
    sys.exc_info()[1]. Deleting sys.exception on this 3.14 runtime proves it
    behaviorally: the OLD implementation would raise AttributeError here and
    mask the primary. (No 3.10 runtime is executed in this suite; the
    deletion fault-injects the 3.10 attribute surface onto 3.14. Neither the
    stdlib asyncio/logging nor the installed discord/mcp packages reference
    sys.exception, so the deletion only faults the server code under test.)
    """
    assert hasattr(sys, "exception")  # fault-injection precondition on 3.14
    monkeypatch.delattr(sys, "exception")
    assert not hasattr(sys, "exception")
    with pytest.raises(AttributeError):
        sys.exception()  # effectiveness proof of the fault injection
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    primary_exc = discord.LoginFailure("bad token")
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        login_exc=primary_exc,
    )
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", _FailingCloseRealResponder)
    mcp = create_server(settings)
    with pytest.raises(discord.LoginFailure, match="bad token") as excinfo:
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - login must raise first
    assert excinfo.value is primary_exc  # the SAME primary object, never replaced
    secondaries = getattr(excinfo.value, "thyca_cleanup_errors", ())
    assert [str(e) for e in secondaries] == ["responder close boom"]
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    assert bot.listeners.get("on_message") == []
    assert bot.is_closed()
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


async def test_lifespan_cancel_without_sys_exception_stays_cancelled(settings, monkeypatch):
    """Cancelled startup with cleanup failure still presents as cancelled (no sys.exception)."""
    monkeypatch.delattr(sys, "exception")
    assert not hasattr(sys, "exception")
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    login_entered = asyncio.Event()

    async def gated_login(token):  # noqa: ARG002 - signature mirrors discord.py
        bot.events.append("login")
        login_entered.set()  # lifespan is now parked inside login
        await asyncio.sleep(3600)

    bot.login = gated_login
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", _FailingCloseRealResponder)
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
    assert [str(e) for e in secondaries] == ["responder close boom"]
    assert bot.is_closed()
    assert bot.listeners.get("on_message") == []
    assert bot.events == ["add_listener", "login", "remove_listener", "close"]
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


def test_server_source_has_no_sys_exception_attribute():
    """Narrow static complement: server.py must not reference sys.exception.

    Behavioral proof lives in the two lifespan tests above (they delete the
    attribute at runtime); this pins the 3.10-safe surface against
    reintroduction. It complements -- never replaces -- those tests.
    """
    import ast as _ast

    source = Path(__file__).resolve().parent.parent / "src" / "server" / "core.py"
    tree = _ast.parse(source.read_text(encoding="utf-8"))
    hits = [
        node.lineno
        for node in _ast.walk(tree)
        if isinstance(node, _ast.Attribute)
        and node.attr == "exception"
        and isinstance(node.value, _ast.Name)
        and node.value.id == "sys"
    ]
    assert hits == []


async def test_shutdown_stops_protected_loops_and_clears_state(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        # v6: rebuild the retired wiring, then let start_task drive it.
        wire_auto_loops(ctx, settings)
        assert ctx["tasks"].status("auto-reply").running is False
        await call_json(mcp, "start_task", {"name": "auto-reply"})
        await call_json(mcp, "start_task", {"name": "auto-reply-recovery"})
        assert ctx["tasks"].status("auto-reply").running is True
    # Internal cleanup forces protected stops; worker closed; state cleared.
    assert all(not t.running for t in ctx["tasks"].list_tasks())
    assert bot.is_closed()
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "uncertain"


async def test_auto_tick_failure_audits_coarse_and_stops_loop(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    # Corrupt journal: the failing tick audits coarse, and shutdown surfaces the
    # close() corruption instead of swallowing it as a clean exit.
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
            wire_auto_loops(ctx, settings)
            settings.delivery_state_path.write_text("corrupt{", encoding="utf-8")
            await ctx["tasks"]._loops["auto-reply"].coro()
            assert ctx["tasks"].status("auto-reply").running is False
    recs = find_audit(read_audit(settings), "auto_reply_tick", False)
    assert len(recs) == 1
    assert recs[0]["args"] == {"error_kind": "delivery_error"}
    assert recs[0]["error"] == "auto_reply_tick failed kind=delivery_error"


async def test_failing_auto_loop_stops_itself_without_reset(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        async with mcp.settings.lifespan(mcp) as ctx:
            # v6: rebuild the retired wiring, then let the real loop run.
            wire_auto_loops(ctx, settings)
            ctx["tasks"].start("auto-reply")
            assert ctx["tasks"].status("auto-reply").running is True
            settings.delivery_state_path.write_text("corrupt{", encoding="utf-8")
            await asyncio.sleep(1.0)  # ~20 ticks at 0.05s; first failure stops loop
            assert ctx["tasks"].status("auto-reply").running is False
            # Fail closed: corrupt bytes preserved verbatim, never reset.
            assert settings.delivery_state_path.read_text(encoding="utf-8") == "corrupt{"
    assert find_audit(read_audit(settings), "auto_reply_tick", False)


# --- correlated send: agent-owned MCP path -----------------------------------


async def test_correlated_send_records_intent_sdk_receipt(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
        assert res["channel_id"] == CHANNEL_ID
        assert res["source_message_id"] == 701
        assert res["reply_to_message_id"] == 701
        assert res["jump_url"].startswith("https://discord.com/channels/")
        (sent,) = channel.sent
        assert isinstance(sent.reference, discord.MessageReference)
        assert sent.reference.message_id == 701
        assert sent.reference.channel_id == CHANNEL_ID
        assert sent.reference.guild_id == GUILD_ID
        assert sent.reference.fail_if_not_exists is True
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert len(job["send_intents"]) == 1
        assert job["send_intents"][0]["content_len"] == len("auto reply")
        assert job["send_intents"][0]["content_hash"] == hashlib.sha256(b"auto reply").hexdigest()
        assert len(job["send_receipts"]) == 1
        assert job["send_receipts"][0]["sent_message_id"] == res["message_id"]
        assert job["send_receipts"][0]["reply_to_message_id"] == 701
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1
    assert ok_recs[0]["args"]["source_message_id"] == 701
    assert "content" not in ok_recs[0]["args"]


async def test_correlated_send_partial_pair_rejected(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool("send_message", {"content": "hi", "source_message_id": 701})
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool("send_message", {"content": "hi", "job_ref": job_ref})
        assert channel.sent == []
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["send_intents"] == [] and job["send_receipts"] == []


async def test_correlated_send_unknown_job_rejected(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 4242,
                    "job_ref": "job-444-4242-9",
                },
            )
        assert channel.sent == []


async def test_correlated_send_job_ref_mismatch_rejected(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "job-444-701-999",
                },
            )
        assert channel.sent == []
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["send_intents"] == []


async def test_correlated_send_wrong_channel_rejected(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    other = make_text_channel(555)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel, 555: other}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": 555,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == [] and other.sent == []


@pytest.mark.parametrize("stage", ["queued", "skipped"])
async def test_correlated_send_stage_ineligible_rejected(settings, monkeypatch, stage):
    write_mapping(settings)
    init_delivery(settings)
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=702,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content="m702",
        author_is_bot=False,
    )
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=702,
        author_uid=OWNER_UID,
        actor_kind="owner",
        seq=1,
    )
    if stage == "skipped":
        skip_job(settings.delivery_state_path, CHANNEL_ID, 702, reason="out_of_scope")
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 702,
                    "job_ref": "job-444-702-1",
                },
            )
        assert channel.sent == []


@pytest.mark.parametrize("bad", ["", "x" * 2001])
async def test_correlated_send_text_bounds_rejected_before_intent(settings, monkeypatch, bad):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="1..2000"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": bad,
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == []
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["send_intents"] == []


@pytest.mark.parametrize("bad_mid", [0, -5])
async def test_correlated_send_invalid_source_mid_rejected(settings, monkeypatch, bad_mid):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="invalid source_message_id"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": bad_mid,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == []


async def test_correlated_send_empty_job_ref_rejected(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="invalid job_ref"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "",
                },
            )
        assert channel.sent == []


async def test_correlated_send_sdk_error_marks_uncertain_no_retry(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    channel.hooks["send_exc"] = discord.DiscordException("native boom")
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="reply uncertain"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "transport_error"
        assert len(job["send_intents"]) == 1  # intent persisted before the SDK attempt
        assert job["send_receipts"] == []
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1
    assert recs[0]["args"]["error_kind"] == "discord_error"
    assert "native boom" not in json.dumps(recs[0])


async def test_correlated_send_wrong_receipt_marks_uncertain(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)

    async def wrong_receipt(content, **kwargs):
        return FakeMessage(
            9100,
            FakeUser(BOT_UID, "ThycaBot", bot=True),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=SimpleNamespace(message_id=123456),  # SDK echoed another MID
        )

    channel.hooks["send_hook"] = wrong_receipt
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="receipt mismatch"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "delivery_unknown"
        assert len(job["send_intents"]) == 1
        assert job["send_receipts"] == []  # wrong receipt never journaled


async def test_correlated_send_cancel_journals_uncertain_and_propagates(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    channel.hooks["send_exc"] = asyncio.CancelledError()
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        # Precise: FastMCP must propagate CancelledError unwrapped (a broad
        # BaseException assertion would also accept a wrapped ToolError).
        with pytest.raises(asyncio.CancelledError):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "delivery_unknown"
        assert len(job["send_intents"]) == 1
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1  # journaled before cleanup, then propagated


async def test_manual_send_does_not_attach_active_job(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        res = await call_json(mcp, "send_message", {"content": "operator note"})
        assert set(res) == {
            "message_id",
            "channel_id",
            "jump_url",
            "ts",
            "reply_to_message_id",
        }  # v6 receipt shape
        (sent,) = channel.sent
        assert sent.reference is None  # plain legacy send, no correlation
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "turn_submitting"  # untouched by the manual send
        assert job["send_intents"] == [] and job["send_receipts"] == []


async def test_concurrent_manual_sends_never_deadlock(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    channel = make_text_channel(CHANNEL_ID)
    gate = asyncio.Event()

    async def gated_send(content, **kwargs):
        await gate.wait()
        return FakeMessage(
            9200 + len(channel.sent),
            FakeUser(BOT_UID, "ThycaBot", bot=True),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=kwargs.get("reference"),
        )

    channel.hooks["send_hook"] = gated_send
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        t1 = asyncio.create_task(mcp.call_tool("send_message", {"content": "one"}))
        t2 = asyncio.create_task(mcp.call_tool("send_message", {"content": "two"}))
        await asyncio.sleep(0.1)
        assert not t1.done() and not t2.done()  # both parked at the gate, none stuck
        gate.set()
        r1 = await asyncio.wait_for(t1, timeout=5)
        r2 = await asyncio.wait_for(t2, timeout=5)
        assert r1 and r2


async def test_send_audits_never_carry_content_or_canary(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        await call_json(mcp, "send_message", {"content": "note CANARY_SEND_X9"})
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool(
                "send_message",
                {"content": "oops CANARY_SEND_X9", "source_message_id": 701},
            )
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "oops CANARY_SEND_X9",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "job-444-701-xyz",
                },
            )
    audit_text = settings.audit_path.read_text(encoding="utf-8")
    assert "CANARY_SEND_X9" not in audit_text
    assert '"content":' not in audit_text


# --- ACK guard: controller owns the safe prefix ------------------------------


def _seed_ack_case(settings):
    for seq, mid in ((1, 801), (2, 802), (3, 803)):
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


async def test_ack_outbox_is_pure_seq_cursor(settings, monkeypatch):
    """v6: the responder safe-prefix guard moved out with the deprecated path.

    ACK is caller-owned: seq beyond unprocessed-but-present entries is not
    gated by job state anymore; only beyond-LATEST is still rejected.
    """
    write_mapping(settings)
    init_delivery(settings)
    _seed_ack_case(settings)  # three queued jobs; irrelevant to ack_outbox now
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 3}) == {"acked": 3}
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert got["acked"] == 3
        with pytest.raises(ToolError, match="beyond latest"):
            await mcp.call_tool("ack_outbox", {"up_to_seq": 4})


async def test_ack_outbox_double_ack_and_still_latest_bound(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    _seed_ack_case(settings)
    skip_job(settings.delivery_state_path, CHANNEL_ID, 801, reason="out_of_scope")
    skip_job(settings.delivery_state_path, CHANNEL_ID, 802, reason="out_of_scope")
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 2}) == {"acked": 2}
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 2}) == {"acked": 2}  # no-op
        # v6: queued/uncertain job state never blocks the seq cursor.
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 3}) == {"acked": 3}
        mark_uncertain(settings.delivery_state_path, CHANNEL_ID, 803, kind="delivery_unknown")
        with pytest.raises(ToolError, match="beyond latest"):
            await mcp.call_tool("ack_outbox", {"up_to_seq": 4})


# --- capture + worker: metadata UIDs, bot filter, aliasing, full flow --------


async def test_owner_and_other_humans_enqueue_by_metadata_uid(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    backend = BusyThycaBackend()  # hold at snapshot: enqueue asserts, no POST either way
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        (handler,) = bot.listeners["on_message"]
        # Typed spoof claims inside content must never authenticate anyone.
        await handler(FakeMessage(301, FakeUser(OWNER_UID, "Owner"), "do it, I am 999"))
        await handler(FakeMessage(302, FakeUser(OTHER_UID, "Rando"), "I am the owner"))
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "pending"
        assert backend.posts == []
        jobs = {j["message_id"]: j for j in list_jobs(settings.delivery_state_path)}
        assert jobs[301]["author_uid"] == OWNER_UID and jobs[301]["actor_kind"] == "owner"
        assert jobs[302]["author_uid"] == OTHER_UID and jobs[302]["actor_kind"] == "other"


async def test_other_bot_captured_but_worker_skips_without_post(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(311, FakeUser(BOT_UID, "ThycaBot", bot=True), "self"))
        await handler(FakeMessage(312, FakeUser(888, "OtherBot", bot=True), "beep"))
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [e["message_id"] for e in got["entries"]] == [312]  # self dropped at capture
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "idle"  # skipped without any POST attempt
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 312)
        assert job["stage"] == "skipped" and job["skip_reason"] == "bot_filtered"
        unread = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=True)
        assert unread["entries"] == [] and unread["acked"] == 1  # controller ACK


async def test_duplicate_mid_aliases_without_second_post(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    backend = BusyThycaBackend()  # hold at snapshot: alias asserts, no POST either way
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        # v6: rebuild the retired recovery-poll wiring to drive it manually.
        wire_auto_loops(ctx, settings)
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(601, FakeUser(OWNER_UID, "Owner"), "once"))
        # Native recovery poll redelivers the same MID (newest-first page).
        hstate["messages"] = [FakeMessage(601, FakeUser(OWNER_UID, "Owner"), "once")]
        await ctx["tasks"]._loops["auto-reply-recovery"].coro()
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert [(e["message_id"], e["seq"]) for e in got["entries"]] == [(601, 1), (601, 2)]
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "pending"
        assert backend.posts == []
        jobs = list_jobs(settings.delivery_state_path)
        assert len(jobs) == 1  # one job, no second POST
        assert jobs[0]["seq"] == 1 and jobs[0]["seq_aliases"] == [2]


async def test_full_auto_flow_native_loop_drives_post_callback_complete_ack(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    backend = FakeThycaBackend(settings.delivery_state_path)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        # v6: rebuild the retired wiring — the dedicated bot owns this worker
        # now; this test still proves the loop-driven turn end-to-end.
        wire_auto_loops(ctx, settings)
        ctx["tasks"].start("auto-reply")
        # Default-on: the native loop (not a manual POST) must drive the turn.
        assert ctx["tasks"].status("auto-reply").running is True
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(501, FakeUser(OWNER_UID, "Owner"), "hello thyca"))
        await asyncio.wait_for(backend.posted.wait(), timeout=5)
        await asyncio.sleep(0.5)  # let the completing tick finish past the callback
        assert len(backend.posts) == 1
        # EXPECTATION CHANGE (peer worker contract, not a weakening): the
        # worker-generated job_ref is a caller-opaque random32-hex nonce per
        # the accepted delivery contract, never a pinned
        # `job-{ch}-{mid}-{seq}` format (this test seeds that legacy shape
        # only when IT acts as the submitting caller via seed_active_job).
        # The prompt must still carry the exact live nonce from the store.
        live_ref = get_job(settings.delivery_state_path, CHANNEL_ID, 501)["job_ref"]
        assert live_ref and live_ref in backend.posts[0]
        (call,) = backend.tool_calls
        assert backend.trace_requests == [4, 4]  # bind check + verify before complete
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 501)
        assert job["stage"] == "completed"
        assert job["turn_index"] == 4
        assert len(job["send_intents"]) == 1 and len(job["send_receipts"]) == 1
        assert job["send_receipts"][0]["sent_message_id"] == call["result"]["message_id"]
        assert job["send_receipts"][0]["reply_to_message_id"] == 501
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert got["acked"] == 1  # controller ACK, never the model
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1 and ok_recs[0]["args"]["source_message_id"] == 501


async def test_own_trace_uncorrelated_send_holds_not_complete(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    backend = FakeThycaBackend(settings.delivery_state_path)
    backend.own_trace_uncorrelated_content = "own trace uncorrelated"
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(502, FakeUser(OTHER_UID, "Rando"), "hi"))
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "uncertain"
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 502)
        assert job["stage"] == "uncertain"
        assert len(channel.sent) == 2  # correlated + own-trace uncorrelated both hit the SDK
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert got["acked"] == 0  # held: never complete, never acked


# --- F1: delivery-durability faults close the job, never duplicate the send ---


def _fsync_fault(monkeypatch, *, fail_on):
    """Fail the Nth delivery parent-dir fsync with the REAL uncertain error.

    The stub raises exactly what the real hook raises on an OSError after a
    committed os.replace: DeliveryDurabilityUncertain (committed=True). Calls
    before/after the fault delegate to the real fsync, so the uncertain-hold
    write itself lands durably. Install AFTER seeding so counting starts at
    the call under test (seed writes must not consume the fault budget).
    """
    import delivery as delivery_mod

    real = delivery_mod._fsync_parent_dir
    calls = {"n": 0}

    def stub(parent, state_path):
        calls["n"] += 1
        if calls["n"] == fail_on:
            raise DeliveryDurabilityUncertain(state_path)
        return real(parent, state_path)

    monkeypatch.setattr("delivery._fsync_parent_dir", stub)
    return calls


async def test_intent_durability_fault_preserves_intent_no_sdk_send(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _fsync_fault(monkeypatch, fail_on=1)  # the intent commit is the 1st fsync
        args = {
            "content": "auto reply",
            "channel_id": CHANNEL_ID,
            "source_message_id": 701,
            "job_ref": job_ref,
        }
        with pytest.raises(ToolError, match="delivery uncertain"):
            await mcp.call_tool("send_message", args)
        assert channel.sent == []  # fault struck before the SDK side effect
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "delivery_unknown"
        assert len(job["send_intents"]) == 1  # committed intent PRESERVED, never rewound
        assert job["send_receipts"] == []
        # Identical retry correlation-rejects (uncertain is ineligible): no
        # fresh intent, no SDK send, never a duplicate.
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool("send_message", args)
        assert channel.sent == []
        retry = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert retry["stage"] == "uncertain" and len(retry["send_intents"]) == 1
    recs = find_audit(read_audit(settings), "send_message", False)
    assert [r["args"]["error_kind"] for r in recs] == ["delivery_error", "validation_error"]


async def test_receipt_durability_fault_preserves_receipt_single_sdk_send(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        _fsync_fault(monkeypatch, fail_on=2)  # intent ok (1st), receipt commit fails (2nd)
        args = {
            "content": "auto reply",
            "channel_id": CHANNEL_ID,
            "source_message_id": 701,
            "job_ref": job_ref,
        }
        with pytest.raises(ToolError, match="delivery uncertain"):
            await mcp.call_tool("send_message", args)
        assert len(channel.sent) == 1  # exactly ONE SDK send
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "delivery_unknown"
        assert len(job["send_intents"]) == 1
        assert len(job["send_receipts"]) == 1  # committed receipt PRESERVED
        assert job["send_receipts"][0]["sent_message_id"] == channel.sent[0].id
        # Identical retry correlation-rejects: still exactly one SDK send total.
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool("send_message", args)
        assert len(channel.sent) == 1  # never two
    recs = find_audit(read_audit(settings), "send_message", False)
    assert [r["args"]["error_kind"] for r in recs] == ["delivery_error", "validation_error"]


# --- F1b: double storage fault latches the send fence, hold unpersistable ---


def _double_storage_fault(monkeypatch, settings, *, fail_on):
    """fsync fault on commit N + PRE-commit ENOSPC on the uncertain-hold write.

    First fault: the Nth delivery parent-dir fsync raises the REAL
    DeliveryDurabilityUncertain (committed file kept) -- fail_on=1 faults
    the intent commit, fail_on=2 the receipt commit (same semantics as
    _fsync_fault; install AFTER seeding like the F1 tests). Second fault:
    os.replace raises PRE-commit OSError ENOSPC, but ONLY for a
    delivery-state commit whose OWN tmp payload already carries an
    uncertain stage (the _preserve_uncertain hold) AND only after the
    first fault fired. Every other replace -- the authoritative first
    commit, unrelated paths -- delegates to the real os.replace, and the
    payload is inspected as owned JSON (never printed raw).
    Returns (fsync_calls, replace_calls) counters for no-further-write proof.
    """
    import delivery as delivery_mod

    real_fsync = delivery_mod._fsync_parent_dir
    real_replace = os.replace
    fsync_calls = {"n": 0}
    replace_calls = {"n": 0, "held": 0}
    state_path = Path(settings.delivery_state_path)

    def fsync_stub(parent, state_path_arg):
        fsync_calls["n"] += 1
        if fsync_calls["n"] == fail_on:
            raise DeliveryDurabilityUncertain(state_path_arg)
        return real_fsync(parent, state_path_arg)

    def replace_stub(src, dst, *args, **kwargs):
        if Path(dst) == state_path:
            replace_calls["n"] += 1
            fired = fsync_calls["n"] >= fail_on
            holds_uncertain = False
            try:
                payload = json.loads(Path(src).read_text(encoding="utf-8"))
                jobs = payload.get("jobs", {})
                holds_uncertain = any(
                    isinstance(job, dict) and job.get("stage") == "uncertain"
                    for job in jobs.values()
                )
            except Exception:
                holds_uncertain = False
            if fired and holds_uncertain:
                replace_calls["held"] += 1
                # _save_state's pre-commit path unlinks our tmp; raise the
                # raw OSError here so the hold genuinely cannot persist.
                raise OSError(errno.ENOSPC, "No space left on device")
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr("delivery._fsync_parent_dir", fsync_stub)
    monkeypatch.setattr(os, "replace", replace_stub)
    return fsync_calls, replace_calls


@pytest.mark.parametrize(
    "case",
    [
        {"fail_on": 1, "sends": 0, "stage": "delivering", "receipts": 0},
        {"fail_on": 2, "sends": 1, "stage": "sent", "receipts": 1},
    ],
    ids=["intent-commit", "receipt-commit"],
)
async def test_double_storage_fault_latches_no_retry_resend(settings, monkeypatch, case):
    """Unpersistable uncertain hold still fences ALL retries (send_blocked latch).

    First fault: the intent (fail_on=1) or receipt (fail_on=2) commit lands
    but its namespace fsync faults -> DeliveryDurabilityUncertain. Second
    fault: the _preserve_uncertain hold write itself hits PRE-commit ENOSPC,
    so the journal keeps the pre-hold stage (delivering/sent) with the
    committed intent (+receipt) authoritative. The process-lifetime
    send_blocked latch -- set BEFORE any journal read/write -- must then
    reject every same-args retry with bounded "delivery uncertain" (0 new
    SDK sends, 0 new intents/receipts, 0 further stage writes), while an
    unrelated manual send still works. The latch is failure fencing, NOT
    authentication, and a crash loses this memory (an unwritable disk
    cannot promise exactly-once; the worker still never blind-POSTs).
    """
    content = f"auto reply CANARY_LATCH_{case['fail_on']}Q"
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        # Scoped fault: restore BEFORE parked_server exits so Responder.close
        # can persist unknowns (fail-loud OSError there is correct behavior).
        with monkeypatch.context() as storage_patch:
            _, replaces = _double_storage_fault(storage_patch, settings, fail_on=case["fail_on"])
            args = {
                "content": content,
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            }
            with pytest.raises(ToolError) as first_err:
                await mcp.call_tool("send_message", args)
            assert "delivery uncertain" in str(first_err.value)
            assert content not in str(first_err.value)  # bounded error, no raw content
            # First commit authoritative despite the failed hold: exact SDK
            # count, committed intent (+receipt) preserved, pre-hold stage kept.
            assert len(channel.sent) == case["sends"]
            job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
            assert job["stage"] == case["stage"]
            assert len(job["send_intents"]) == 1
            assert len(job["send_receipts"]) == case["receipts"]
            assert replaces["held"] == 1  # the hold write hit ENOSPC exactly once
            frozen_replaces = replaces["n"]
            # Retry + third attempt: correlation still ACCEPTS (the stage is a
            # live delivering/sent, not terminal uncertain) but the latch
            # rejects -- "delivery uncertain", never a resend, never a write.
            for _ in range(2):
                with pytest.raises(ToolError) as retry_err:
                    await mcp.call_tool("send_message", args)
                assert "delivery uncertain" in str(retry_err.value)
                assert content not in str(retry_err.value)
            assert len(channel.sent) == case["sends"]  # exactly 0 additional SDK sends
            retry = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
            assert retry["stage"] == case["stage"]
            assert len(retry["send_intents"]) == 1
            assert len(retry["send_receipts"]) == case["receipts"]
            assert replaces["n"] == frozen_replaces  # 0 additional stage writes
            # Latch is (channel, source MID) specific: a wrong job_ref still
            # correlation-rejects (canary never reaches the audit)...
            wrong_canary = f"CANARY_WRONGREF_{case['fail_on']}Q"
            with pytest.raises(ToolError, match="correlation rejected"):
                await mcp.call_tool("send_message", {**args, "job_ref": f"bogus-{wrong_canary}"})
            # ...and an unrelated manual send bypasses the fence unchanged.
            manual = await call_json(mcp, "send_message", {"content": "manual note"})
            assert manual["channel_id"] == CHANNEL_ID
            assert len(channel.sent) == case["sends"] + 1
    audit_text = settings.audit_path.read_text(encoding="utf-8")
    assert content not in audit_text  # bounded metadata only, no raw content
    assert wrong_canary not in audit_text
    failed = find_audit(read_audit(settings), "send_message", False)
    assert [r["args"].get("error_kind") for r in failed] == ["delivery_error"] * 3 + [
        "validation_error"
    ]
    assert "content" not in failed[0]["args"]
    assert failed[0]["error"] == "send_message failed kind=delivery_error"


POISON_CANARY_F2 = "CANARY_POISON_F2_QZ"
SINK_CANARY_F2 = "CANARY_SINK_F2_QZ"
AUDIT_FAILED_SIGNAL = "thyca-discord-mcp: auto_reply_tick audit failed"


def _seed_queued_job(settings, *, mid=701, seq=1):
    """Outbox entry + queued job (no POST started): the tick reaches snapshot."""
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


def _poison_tick_harness(monkeypatch, settings, bot):
    """REAL Responder over a poison backend + an audit sink raising OSError."""
    backend = PoisonThycaBackend(RuntimeError(f"native poison {POISON_CANARY_F2}"))

    def _dead_sink(*args, **kwargs):  # noqa: ARG001 - signature mirrors append_audit
        raise OSError(f"audit sink dead {SINK_CANARY_F2}")

    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    monkeypatch.setattr("server.core.append_audit", _dead_sink)
    return backend


def _assert_no_canary_leak(settings, stderr):
    assert POISON_CANARY_F2 not in stderr
    assert SINK_CANARY_F2 not in stderr
    audit_text = (
        settings.audit_path.read_text(encoding="utf-8") if settings.audit_path.exists() else ""
    )
    assert POISON_CANARY_F2 not in audit_text
    assert SINK_CANARY_F2 not in audit_text


async def test_poison_tick_dead_sink_stops_loop_direct(settings, monkeypatch, capsys):
    write_mapping(settings)
    init_delivery(settings)
    _seed_queued_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    _poison_tick_harness(monkeypatch, settings, bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        wire_auto_loops(ctx, settings)
        loop = ctx["tasks"]._loops["auto-reply"]
        stops: list[int] = []
        orig_stop = loop.stop

        def spy_stop(*args, **kwargs):
            stops.append(1)
            return orig_stop(*args, **kwargs)

        loop.stop = spy_stop
        assert await loop.coro() is None  # failure contained, never raised
        assert stops == [1]  # loop.stop() ran despite the dead audit sink
    err = capsys.readouterr().err
    assert err.count(AUDIT_FAILED_SIGNAL) == 1  # fixed signal only, exactly once
    _assert_no_canary_leak(settings, err)


async def test_poison_tick_dead_sink_stops_native_loop(settings, monkeypatch, capsys):
    write_mapping(settings)
    init_delivery(settings)
    _seed_queued_job(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    backend = _poison_tick_harness(monkeypatch, settings, bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        wire_auto_loops(ctx, settings)
        ctx["tasks"].start("auto-reply")
        assert ctx["tasks"].status("auto-reply").running is True
        for _ in range(200):  # native 0.05s loop must stop itself after failing
            if not ctx["tasks"].status("auto-reply").running:
                break
            await asyncio.sleep(0.05)
        assert ctx["tasks"].status("auto-reply").running is False
        assert backend.snapshots >= 1  # the poison actually fired
    err = capsys.readouterr().err
    assert err.count(AUDIT_FAILED_SIGNAL) == 1  # one failure, one fixed signal, no retry
    _assert_no_canary_leak(settings, err)


# --- F5: bool/str source MID rejected at the MCP boundary --------------------


async def test_bool_and_str_source_mid_rejected_int_control_accepted(settings, monkeypatch):
    # MID 1 is journaled on purpose: under the old lax-int coercion True/"1"
    # would BOTH have become int 1 and hit this very job. StrictInt must
    # reject them before the tool body (zero sends, zero audits); int 1 works.
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings, mid=1, seq=1)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        for bad in (True, "1"):
            with pytest.raises(ToolError):
                await mcp.call_tool(
                    "send_message",
                    {
                        "content": "auto reply",
                        "channel_id": CHANNEL_ID,
                        "source_message_id": bad,
                        "job_ref": job_ref,
                    },
                )
        assert channel.sent == []  # rejected before any SDK send
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 1)
        assert job["send_intents"] == [] and job["send_receipts"] == []
        # Rejected before the tool body audits (heartbeat may audit; send must not).
        assert find_audit(read_audit(settings), "send_message", True) == []
        assert find_audit(read_audit(settings), "send_message", False) == []
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 1,
                "job_ref": job_ref,
            },
        )
        assert res["source_message_id"] == 1 and res["reply_to_message_id"] == 1
        assert len(channel.sent) == 1
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1  # only the int control audited


# --- F6: receipt scope mismatches hold uncertain without resend --------------


def _scoped_bot_user():
    return FakeUser(BOT_UID, "ThycaBot", bot=True)


def _mismatch_receipt(case, content):
    """Craft an SDK receipt echo per mismatch case (MID 701 job assumed)."""
    if case == "reviewer_repro":
        # EXACT reviewer repro: correct source MID, WRONG reference
        # channel/guild, non-Discord HTTPS jump URL.
        msg = FakeMessage(
            9100,
            _scoped_bot_user(),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=SimpleNamespace(message_id=701, channel_id=555, guild_id=999),
        )
        msg.jump_url = "https://not-discord.example/abc"
        return msg
    if case == "wrong_channel":
        return FakeMessage(
            9100,
            _scoped_bot_user(),
            content,
            channel_id=555,
            guild_id=GUILD_ID,
            reference=SimpleNamespace(message_id=701, channel_id=555, guild_id=GUILD_ID),
        )
    if case == "jump_ids_wrong_segment":
        msg = FakeMessage(
            9100,
            _scoped_bot_user(),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=SimpleNamespace(message_id=701, channel_id=CHANNEL_ID, guild_id=GUILD_ID),
        )
        msg.jump_url = f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/12345"
        return msg
    if case == "dm_style_jump":
        msg = FakeMessage(
            9100,
            _scoped_bot_user(),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=SimpleNamespace(message_id=701, channel_id=CHANNEL_ID, guild_id=GUILD_ID),
        )
        msg.jump_url = f"https://discord.com/channels/@me/{CHANNEL_ID}/9100"
        return msg
    if case == "missing_reference":
        return FakeMessage(
            9100, _scoped_bot_user(), content, channel_id=CHANNEL_ID, guild_id=GUILD_ID
        )
    raise AssertionError(f"unknown mismatch case: {case}")


@pytest.mark.parametrize(
    "case",
    [
        "reviewer_repro",
        "wrong_channel",
        "jump_ids_wrong_segment",
        "dm_style_jump",
        "missing_reference",
    ],
)
async def test_correlated_send_receipt_scope_mismatch_holds_uncertain(settings, monkeypatch, case):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    calls: list[str] = []

    async def bad_echo(content, **kwargs):
        calls.append(content)
        return _mismatch_receipt(case, content)

    channel.hooks["send_hook"] = bad_echo
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        args = {
            "content": "auto reply",
            "channel_id": CHANNEL_ID,
            "source_message_id": 701,
            "job_ref": job_ref,
        }
        with pytest.raises(ToolError, match="receipt mismatch"):
            await mcp.call_tool("send_message", args)
        assert calls == ["auto reply"]  # single SDK attempt, never repeated
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"
        assert job["error"]["kind"] == "delivery_unknown"
        assert len(job["send_intents"]) == 1
        assert job["send_receipts"] == []  # mismatched receipt never journaled
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool("send_message", args)
        assert calls == ["auto reply"]  # retry rejected: still no resend


async def test_correlated_send_fully_scoped_receipt_journals_sent(settings, monkeypatch):
    # Positive control: exact scope everywhere (reference IDs + jump-URL IDs)
    # journals the receipt and leaves the job sent (worker owns completion).
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
        assert (
            res["jump_url"]
            == f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/{res['message_id']}"
        )
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "sent"
        assert len(job["send_intents"]) == 1 and len(job["send_receipts"]) == 1
        assert job["send_receipts"][0]["reply_to_message_id"] == 701


# --- F7: unvalidated job_ref stays len/hash in audits; accepted IDs raw ------


async def test_invalid_job_ref_canary_never_reaches_audit(settings, monkeypatch):
    canary = "CANARY_JOBREF_F7_XQ"
    bogus_ref = f"bogus-{canary}-ref"
    write_mapping(settings)
    init_delivery(settings)
    seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": bogus_ref,
                },
            )
        assert channel.sent == []
    assert canary not in settings.audit_path.read_text(encoding="utf-8")
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1
    args = recs[0]["args"]
    assert "job_ref" not in args  # rejected IDs never audited raw
    assert args["job_ref_len"] == len(bogus_ref)
    assert args["job_ref_sha256"] == hashlib.sha256(bogus_ref.encode()).hexdigest()
    assert args["source_message_id"] == 701  # strict-positive MID is metadata-safe raw
    assert recs[0]["error"] == "send_message failed kind=validation_error"


async def test_accepted_correlation_ids_audited_raw(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    job_ref = seed_active_job(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1
    # Control: once correlated_job ACCEPTS them, both IDs are retained raw.
    assert ok_recs[0]["args"]["source_message_id"] == 701
    assert ok_recs[0]["args"]["job_ref"] == job_ref


# --- foreign-manual non-attribution (separated from own-trace case) ----------


async def test_foreign_manual_send_not_attributed_to_auto_job(settings, monkeypatch):
    write_mapping(settings)
    init_delivery(settings)
    backend = FakeThycaBackend(settings.delivery_state_path)
    backend.foreign_manual_content = "foreign sid manual"
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(503, FakeUser(OTHER_UID, "Rando"), "hi"))
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "completed"
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 503)
        assert job["stage"] == "completed"
        assert len(job["send_intents"]) == 1 and len(job["send_receipts"]) == 1
        assert len(channel.sent) == 2  # correlated + foreign manual both hit the SDK
        got = read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)
        assert got["acked"] == 1  # foreign send never blocked completion or ACK


# --- F8: tmp/fake-only settings, no real dotenv/env reads --------------------


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
    assert made.responder_check_seconds == 0.05
    assert made.recovery_poll_seconds == 30.0
