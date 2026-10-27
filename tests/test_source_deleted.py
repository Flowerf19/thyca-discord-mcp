"""Deleted-source handling: pre-send existence proof + offline regressions.

All tests fake (tmp settings, _env_file=None, no network/Discord/Thyca).
Covers: strict 404/10008 deletion vs unavailable scope, responder pre-POST
skip with zero POSTs, MCP pre-send gate skip, send-evidence failure
(no skip when a send landed), cancellation preservation, safe-prefix ACK.
No uncertain stage, no intents/receipts, no trace proof exist anymore.
"""

from __future__ import annotations

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
from responder import Responder
from server import create_server
from source_messages import (
    SourceDeleted,
    SourceUnavailable,
    build_source_checker,
    check_source_message,
)
from store import Store, StoreError, append_entry, read_entries

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
FAKE_SID = "2026-01-02T03-04-05_ab12"
FAKE_DAY = "2026-01-02"


def _nf(status, code):
    return discord.NotFound(
        SimpleNamespace(status=status, reason="R"), {"code": code, "message": "M"}
    )


def _deleted_exc():
    return _nf(404, 10008)


class FakeUser:
    def __init__(self, uid, name="U", bot=False):
        self.id = uid
        self.display_name = name
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


def make_text_channel(cid, guild_id=GUILD_ID):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    sent: list = []
    store: dict = {"deleted": set(), "exc": None, "calls": [], "attempts": 0}

    async def fake_send(content, **kwargs):
        # Count INVOCATIONS (not just successes): a raised SDK error still
        # proves an attempt reached the native surface.
        store["attempts"] += 1
        if store.get("send_exc") is not None:
            raise store["send_exc"]
        msg = FakeMessage(
            9000 + len(sent),
            FakeUser(BOT_UID, "B", True),
            channel_id=cid,
            guild_id=guild_id,
            reference=kwargs.get("reference"),
        )
        sent.append(msg)
        return msg

    async def fake_fetch(mid):
        store["calls"].append(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_exc()
        return FakeMessage(mid, FakeUser(OWNER_UID, "O"), channel_id=cid, guild_id=guild_id)

    channel.send = fake_send
    channel.fetch_message = fake_fetch
    channel.sent = sent
    channel.source_store = store
    return channel


class FakeGuild:
    def __init__(self, gid, channels=None):
        self.id = gid
        self._channels = dict(channels or {})

    def get_channel(self, cid):
        ch = self._channels.get(cid)
        if ch is None:
            ch = make_text_channel(cid, self.id)
            self._channels[cid] = ch
        return ch

    def get_member(self, uid):
        return None


class FakeBot:
    def __init__(self, guild, user=None):
        self.user = user or FakeUser(BOT_UID, "B", True)
        self._guild = guild
        self.listeners: dict = {}
        self._ready = False
        self._closed = False
        self._setup = False  # mirrors Client._ready existing after _async_setup_hook
        # Mirrors the gateway READY dispatch: set only by a successful start.
        self._ready_event = asyncio.Event()
        self._close_event = asyncio.Event()

    def event(self, func):
        self.listeners.setdefault(func.__name__, []).append(func)
        return func

    async def start(self, token):  # noqa: ARG002 - signature mirrors discord.py
        self._setup = True  # mirrors _async_setup_hook at login() entry
        # Successful gateway connect dispatches READY, then stays open.
        self._ready_event.set()
        await self._close_event.wait()

    async def wait_until_ready(self):
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
        self._closed = True
        self._close_event.set()

    def get_guild(self, gid):
        return self._guild if self._guild is not None and self._guild.id == gid else None

    async def fetch_channel(self, cid):
        raise LookupError(f"channel {cid} not found")


class ScriptClient:
    """Minimal scripted native client (snapshot/POST/trace only)."""

    def __init__(self, pre_max=3):
        self.pre_max = pre_max
        self.submit_calls: list = []
        self.submit_fn = None

    async def get_session_snapshot(self, sid, **kw):
        total = self.pre_max + 1 + len(self.submit_calls)
        return {
            "native_sid": sid,
            "native_id_match": True,
            "running": False,
            "turn_count": total,
            "turn_indices": list(range(total)),
            "pre_max_turn_index": (total - 1) if total else None,
        }

    async def submit_turn(self, sid, text, **kw):
        self.submit_calls.append(text)
        if self.submit_fn is not None:
            return await self.submit_fn(sid, text)
        # Clean idle turn: the index follows the gate pre_max exactly so
        # the snapshot-poll close accepts it without trace proof.
        turn_index = self.pre_max + len(self.submit_calls)
        return {
            "cancelled": False,
            "completed": True,
            "native_id_match": True,
            "turn_index": turn_index,
            "running": False,
            "stored_text_match": True,
        }

    async def get_bot_dm(self, bot_id):
        return {
            "native_sid": FAKE_SID,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }


def _settings_kwargs(tmp_path):
    return dict(
        _env_file=None,
        discord_token="fake",
        owner_uid=OWNER_UID,
        guild_id=GUILD_ID,
        default_channel_id=CHANNEL_ID,
        audit_path=tmp_path / "audit.jsonl",
        db_path=tmp_path / "state.db",
        watch_channel_ids=[CHANNEL_ID],
        responder_check_seconds=0.05,
        thyca_request_timeout_seconds=5.0,
        recovery_poll_seconds=30.0,
    )


@pytest.fixture
def settings(tmp_path):
    s = Settings(**_settings_kwargs(tmp_path))
    with Store.open(s.db_path) as store:
        store.adopt_session(expected_current=None, new_sid=FAKE_SID, day=FAKE_DAY)
    return s


def _append(settings, mid, seq_note="", author_uid=OWNER_UID, kind="owner", name="O"):
    append_entry(
        settings.db_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        author_name=name,
        actor_kind=kind,
        content=f"m{mid}{seq_note}",
        author_is_bot=False,
    )
    entry_seq = read_entries(settings.db_path, unread_only=False)["entries"][-1]["seq"]
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, mid, entry_seq)


@asynccontextmanager
async def _parked(monkeypatch, settings, bot, backend=None):
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    if backend is not None:
        monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx


def _get_job(settings, mid, channel_id=CHANNEL_ID):
    with Store.open(settings.db_path) as store:
        return store.get_job(channel_id, mid)


def _submit(settings, mid, ref, channel_id=CHANNEL_ID, pre_max=3):
    """Move the ingested job row (created by _append) to submitted."""
    with Store.open(settings.db_path) as store:
        return store.mark_submitted(
            channel_id, mid, job_ref=ref, native_sid=FAKE_SID, pre_turn_index=pre_max
        )


def _record_send(settings, mid, ref, sent_mid=9001, channel_id=CHANNEL_ID):
    with Store.open(settings.db_path) as store:
        return store.record_send(channel_id, mid, job_ref=ref, sent_message_id=sent_mid)


def _skip(settings, mid, reason, channel_id=CHANNEL_ID):
    with Store.open(settings.db_path) as store:
        return store.skip_job(channel_id, mid, reason=reason)


def _fail(settings, mid, kind, channel_id=CHANNEL_ID, http_status=None):
    with Store.open(settings.db_path) as store:
        return store.mark_failed(channel_id, mid, kind=kind, http_status=http_status)


async def _call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def _audit(settings):
    if not settings.audit_path.exists():
        return []
    return [
        json.loads(line) for line in settings.audit_path.read_text().splitlines() if line.strip()
    ]


# --- A: source_messages unit -------------------------------------------------


async def test_check_proves_existing_message():
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    check = build_source_checker(bot, GUILD_ID)
    assert await check(CHANNEL_ID, 701) is None


async def test_strict_404_10008_is_deleted():
    channel = make_text_channel(CHANNEL_ID)
    channel.source_store["deleted"].add(701)
    with pytest.raises(SourceDeleted):
        await check_source_message(channel, 701)


@pytest.mark.parametrize(
    "exc",
    [
        _nf(404, 10003),
        _nf(404, 0),
        _nf(400, 50035),
        discord.Forbidden(SimpleNamespace(status=403, reason="F"), {"code": 50013, "message": "x"}),
        discord.HTTPException(SimpleNamespace(status=500, reason="E"), {"code": 0, "message": "x"}),
        discord.HTTPException(SimpleNamespace(status=502, reason="B"), {"code": 0, "message": "x"}),
        asyncio.TimeoutError(),
        OSError("reset"),
    ],
)
async def test_non_deletion_failures_are_unavailable(exc):
    channel = make_text_channel(CHANNEL_ID)

    async def boom(mid):
        raise exc

    channel.fetch_message = boom
    with pytest.raises(SourceUnavailable):
        await check_source_message(channel, 701)


async def test_bool_status_code_never_deletes():
    channel = make_text_channel(CHANNEL_ID)

    async def boom(mid):
        raise _nf(True, 10008)

    channel.fetch_message = boom
    with pytest.raises(SourceUnavailable):
        await check_source_message(channel, 701)


async def test_cancellation_propagates():
    channel = make_text_channel(CHANNEL_ID)

    async def cancelled(mid):
        raise asyncio.CancelledError()

    channel.fetch_message = cancelled
    with pytest.raises(asyncio.CancelledError):
        await check_source_message(channel, 701)


# -- legacy Jev stub (module-only autouse; preserves pre-routing regressions) --
# Every Responder built in THIS module gets a fake respond-only Jev stack
# unless the test passes explicit deps. The fake binds a REAL delivery
# jev_decision (respond, 32-hex attempt) so the full submit/POST/trace/ACK
# path runs unchanged; the fake provider is never called (it raises). New
# routing tests do NOT use this fixture (explicit deps only).


class _LegacyJevAdapter:
    """Synthetic provider stub: never called by the legacy fake ledger."""

    def __init__(self):
        self.calls = 0

    async def decide(self, state):
        self.calls += 1
        raise AssertionError("legacy fake ledger must never call the provider")


def _legacy_collector(owner_uid, bot_uid):
    """Single-message canonical snapshot matching the frozen entry."""

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
class _LegacyLedger:
    """Respond-only stub: deterministic decided record, no provider call.

    Returns the store-shape Jev record (context_sha256 key, no model) so
    the full submit/POST/snapshot/ACK path runs unchanged; the fake
    provider is never called (it raises).
    """

    def __init__(self):
        self.calls = 0

    async def classify(self, adapter, state, *, channel_id, message_id, author_uid):
        self.calls += 1
        from context import context_fingerprint

        attempt = hashlib.sha256(f"{channel_id}:{message_id}".encode()).hexdigest()[:32]
        return {
            "status": "decided",
            "context_sha256": context_fingerprint(state),
            "attempt_id": attempt,
            "participation": "respond",
        }

    def get(self, channel_id, message_id):
        return None


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


# --- B: responder pre-POST gate ----------------------------------------------


async def _responder(settings, bot=None, client=None, source_checker=None):
    bot = bot or FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    client = client or ScriptClient()
    kw = {} if source_checker is None else {"source_checker": source_checker}
    return Responder(settings, bot, client=client, **kw), bot, client


async def test_pre_post_deletion_skips_with_zero_post(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, client = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    out = await inst.tick()
    assert out["status"] == "skipped" and out["reason"] == "source_deleted"
    assert client.submit_calls == []
    job = _get_job(settings, 701)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"
    assert job["job_ref"] is None and job["send_count"] == 0
    assert out["acked"] == 1


async def test_pre_post_unavailable_holds_queued_zero_post(settings):
    async def unknown(cid, mid):
        raise SourceUnavailable("unknown")

    inst, _, client = await _responder(settings, source_checker=unknown)
    _append(settings, 701)
    out = await inst.tick()
    assert out["status"] == "pending" and out["reason"] == "source_unavailable"
    assert client.submit_calls == []
    job = _get_job(settings, 701)
    assert job["stage"] == "queued" and job["job_ref"] is None
    assert out["acked"] == 0


async def test_default_checker_is_live_not_noop(settings):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    inst = Responder(settings, bot, client=ScriptClient())
    _append(settings, 701)
    await inst.tick()
    # default bot-backed checker passed an existing source: a POST was attempted
    assert len(inst._client.submit_calls) == 1


async def test_default_checker_skips_deleted(settings):
    channel = make_text_channel(CHANNEL_ID)
    channel.source_store["deleted"].add(701)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    client = ScriptClient()
    inst = Responder(settings, bot, client=client)
    _append(settings, 701)
    out = await inst.tick()
    assert out["status"] == "skipped" and client.submit_calls == []


async def test_next_queued_proceeds_after_skip(settings):
    async def gone_once():
        calls = []

        async def check(cid, mid):
            calls.append(mid)
            if mid == 701:
                raise SourceDeleted("gone")

        return check

    inst, _, client = await _responder(settings, source_checker=await gone_once())
    _append(settings, 701)
    _append(settings, 702)

    out = await inst.tick()
    assert out["status"] == "skipped"
    # 701 skipped with zero POST; second tick must POST for 702. The clean
    # turn records no outcome (no sends, no record_no_reply), so it fails
    # failed/turn_no_outcome -- but the POST proves the queue advanced.
    out2 = await inst.tick()
    assert out2["status"] == "failed" and out2["reason"] == "turn_no_outcome"
    assert len(client.submit_calls) == 1
    assert _get_job(settings, 701)["stage"] == "skipped"


async def test_deletion_during_submit_observed_as_skip(settings):
    async def exists(cid, mid):
        return None

    inst, _, client = await _responder(settings, source_checker=exists)
    _append(settings, 701)

    async def submit_and_delete(sid, text):
        _skip(settings, 701, "source_deleted")
        return {
            "cancelled": False,
            "completed": True,
            "native_id_match": True,
            "turn_index": 4,
            "running": False,
            "stored_text_match": True,
        }

    client.submit_fn = submit_and_delete
    out = await inst.tick()
    assert out["status"] == "skipped" and out["reason"] == "source_deleted"
    job = _get_job(settings, 701)
    assert job["stage"] == "skipped"
    assert job["job_ref"] and job["pre_turn_index"] == 3  # nonce/index preserved
    out2 = await inst.tick()  # no retry / re-POST / completion
    assert len(client.submit_calls) == 1 and out2["status"] == "idle"


async def test_cancel_after_skip_propagates_and_preserves(settings):
    async def exists(cid, mid):
        return None

    inst, _, client = await _responder(settings, source_checker=exists)
    _append(settings, 701)

    async def submit_cancel(sid, text):
        _skip(settings, 701, "source_deleted")
        raise asyncio.CancelledError()

    client.submit_fn = submit_cancel
    with pytest.raises(asyncio.CancelledError):
        await inst.tick()
    job = _get_job(settings, 701)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"


async def test_skip_refused_with_evidence_fails(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, _ = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    _submit(settings, 701, "f" * 32)
    _record_send(settings, 701, "f" * 32, sent_mid=9001)
    out = inst._skip_source_deleted(CHANNEL_ID, 701)
    assert out["status"] == "failed" and out["reason"] == "send_evidence_present"
    job = _get_job(settings, 701)
    assert job["stage"] == "failed" and job["send_count"] == 1


async def test_failed_job_with_deleted_source_stays_terminal(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, client = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    _submit(settings, 701, "f" * 32)
    _fail(settings, 701, "transport_error")
    out = await inst.tick()
    assert out["status"] == "idle" and out["acked"] == 1
    assert _get_job(settings, 701)["stage"] == "failed"
    assert client.submit_calls == []


async def test_sent_job_with_deleted_source_completes(settings):
    """A reply WAS delivered: the worker completes sent (never a skip)."""

    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, client = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    _submit(settings, 701, "f" * 32)
    _record_send(settings, 701, "f" * 32, sent_mid=9001)
    out = await inst.tick()
    assert out["status"] == "sent" and out["acked"] == 1
    job = _get_job(settings, 701)
    assert job["stage"] == "done" and job["outcome"] == "sent"
    assert job["send_count"] == 1


async def test_aliases_safe_ack_on_skip(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, _ = await _responder(settings, source_checker=gone)
    _append(settings, 701)  # Phase 2a: redelivery dedupes (first write wins),
    # so the same-MID second frame no longer exists; single row, single job.
    out = await inst.tick()
    assert out["status"] == "skipped"
    job = _get_job(settings, 701)
    assert job["stage"] == "skipped"
    assert out["acked"] == 1


# --- C: delivery gate --------------------------------------------------------


def _seed_outbox_job(db, channel_id, mid, seq):
    append_entry(
        db,
        type="message",
        channel_id=channel_id,
        message_id=mid,
        author_uid=7,
        author_name="O",
        actor_kind="owner",
        content=f"m{mid}",
        author_is_bot=False,
    )
    with Store.open(db) as store:
        store.enqueue_job(channel_id, mid, seq)


def test_skip_source_deleted_refuses_evidence_and_terminals(tmp_path):
    db = tmp_path / "state.db"
    for mid, seq in ((10, 1), (11, 2), (12, 3), (13, 4)):
        _seed_outbox_job(db, 1, mid, seq)
    with Store.open(db) as store:
        # queued zero-evidence: ok + idempotent
        store.skip_job(1, 10, reason="source_deleted")
        store.skip_job(1, 10, reason="source_deleted")
        assert store.get_job(1, 10)["stage"] == "skipped"
        # evidence refuses
        store.mark_submitted(1, 11, job_ref="f" * 32, native_sid=FAKE_SID, pre_turn_index=0)
        store.record_send(1, 11, job_ref="f" * 32, sent_message_id=9001)
        with pytest.raises(StoreError):
            store.skip_job(1, 11, reason="source_deleted")
        # failed refuses
        store.mark_submitted(1, 12, job_ref="e" * 32, native_sid=FAKE_SID, pre_turn_index=0)
        store.mark_failed(1, 12, kind="timeout")
        with pytest.raises(StoreError):
            store.skip_job(1, 12, reason="source_deleted")
        # other reasons keep approved behavior
        store.skip_job(1, 13, reason="out_of_scope")
        assert store.get_job(1, 13)["skip_reason"] == "out_of_scope"


# --- D: server MCP gate ------------------------------------------------------


def _seed_active(settings, mid, ref):
    entry = append_entry(
        settings.db_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=OWNER_UID,
        author_name="O",
        actor_kind="owner",
        content=f"m{mid}",
        author_is_bot=False,
    )
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, mid, entry["seq"])
        store.mark_submitted(CHANNEL_ID, mid, job_ref=ref, native_sid=FAKE_SID, pre_turn_index=3)


async def test_mcp_gate_skips_deleted_zero_sdk(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    channel.source_store["send_exc"] = _deleted_exc()  # reply reference reports deletion
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        _seed_active(settings, 701, ref)
        payload = await _call_json(
            mcp,
            "send_message",
            {
                "content": "reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": ref,
            },
        )
        assert payload["status"] == "skipped" and payload["reason"] == "source_deleted"
        assert payload["channel_id"] == CHANNEL_ID and payload["source_message_id"] == 701
        assert channel.sent == []
        job = _get_job(settings, 701)
        assert job["stage"] == "skipped" and job["send_count"] == 0
    recs = [r for r in _audit(settings) if r["tool"] == "send_message" and r["ok"]]
    assert recs and recs[-1]["args"]["source_message_id"] == 701
    blob = json.dumps(recs[-1])
    assert "reply" not in blob and "Unknown Message" not in blob and "Traceback" not in blob


async def test_mcp_gate_evidence_fails_no_send(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    channel.source_store["send_exc"] = _deleted_exc()
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        _seed_active(settings, 701, ref)
        _record_send(settings, 701, ref, sent_mid=9001)
        with pytest.raises(ToolError):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "chunk2",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": ref,
                },
            )
        assert channel.sent == []
        job = _get_job(settings, 701)
        assert job["stage"] == "failed" and job["send_count"] == 1


async def test_mcp_gate_send_timeout_fails_no_record(settings, monkeypatch):
    """No pre-flight fetch exists anymore: the SDK send IS the probe, and an
    ambiguous send failure fails the job (never a blind retry)."""
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    channel.source_store["send_exc"] = asyncio.TimeoutError()
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        _seed_active(settings, 701, ref)
        with pytest.raises(ToolError):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": ref,
                },
            )
        assert channel.sent == []
        assert channel.source_store["attempts"] == 1
        job = _get_job(settings, 701)
        assert job["stage"] == "failed" and job["fail_kind"] == "transport_error"
        assert job["send_count"] == 0


async def test_mcp_gate_repeat_skip_rejected_no_send(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    channel.source_store["send_exc"] = _deleted_exc()
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        _seed_active(settings, 701, ref)
        await _call_json(
            mcp,
            "send_message",
            {
                "content": "reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": ref,
            },
        )
        with pytest.raises(ToolError):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "again",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": ref,
                },
            )
        assert channel.sent == []
        assert _get_job(settings, 701)["send_count"] == 0


async def test_mcp_gate_stray_success_against_terminal(settings, monkeypatch):
    """An SDK success that lands after the job terminalized still returns
    the posted receipt (the send DID happen); the stage guard refuses to
    record it, and the terminal outcome stands."""
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        _seed_active(settings, 701, ref)

        async def send_then_complete(content, **kwargs):
            msg = await make_text_channel(CHANNEL_ID).send(content, **kwargs)
            with Store.open(settings.db_path) as store:
                store.complete_job(CHANNEL_ID, 701, outcome="no_reply")
            return msg

        channel.send = send_then_complete
        payload = await _call_json(
            mcp,
            "send_message",
            {
                "content": "hi",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": ref,
            },
        )
        assert payload["source_message_id"] == 701
        job = _get_job(settings, 701)
        assert (job["stage"], job["outcome"]) == ("done", "no_reply")
        assert job["send_count"] == 0  # stage guard refused the late record


async def test_tool_description_carries_stop_instruction(settings, monkeypatch):
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        tools = await mcp.list_tools()
        send = next(t for t in tools if t.name == "send_message")
        assert "source_deleted" in send.description and "fallback" in send.description


async def test_full_flow_deletion_during_submit_skips_via_gate(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    backend = ScriptClient()
    backend.mcp = None

    async def submit_delete_then_gate(sid, text):
        with Store.open(settings.db_path) as store:
            subs = store.list_jobs(stages={"submitted"})
        assert len(subs) == 1
        job = subs[0]
        channel.source_store["send_exc"] = _deleted_exc()
        res = await backend.mcp.call_tool(
            "send_message",
            {
                "content": "late reply",
                "channel_id": job["channel_id"],
                "source_message_id": job["message_id"],
                "job_ref": job["job_ref"],
            },
        )
        blocks = res[0] if isinstance(res, tuple) else res
        payload = json.loads(blocks[0].text)
        assert payload["status"] == "skipped" and payload["reason"] == "source_deleted"
        return {
            "cancelled": False,
            "completed": True,
            "native_id_match": True,
            "turn_index": 4,
            "running": False,
            "stored_text_match": True,
        }

    backend.submit_fn = submit_delete_then_gate
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(701, FakeUser(OWNER_UID, "O"), "hello thyca"))
        # drive the worker directly (loops parked): checker passes (exists),
        # POST deletes+skips via the real MCP gate, reconcile observes skip.
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "skipped" and outcome["reason"] == "source_deleted"
        job = _get_job(settings, 701)
        assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"
        assert job["send_count"] == 0
        assert channel.sent == [] and len(backend.submit_calls) == 1
        outcome2 = await ctx["responder"].tick()
        assert len(backend.submit_calls) == 1 and outcome2["status"] == "idle"


async def test_manual_sends_unchanged(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        payload = await _call_json(mcp, "send_message", {"content": "manual hello"})
        assert payload["channel_id"] == CHANNEL_ID and payload["message_id"] == 9000
        assert len(channel.sent) == 1
