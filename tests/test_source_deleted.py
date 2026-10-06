"""Deleted-source no-reply handling: pre-send existence proof + offline regressions.

All tests fake (tmp settings, _env_file=None, no network/Discord/Thyca).
Covers: strict 404/10008 deletion vs unavailable scope, responder pre-POST
skip with zero POST/nonce/intent, MCP pre-send gate skip, evidence holds,
post-intent 404 stays uncertain, cancellation preservation, safe-prefix ACK.
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

from config import Settings
from delivery import (
    DeliveryError,
    enqueue_job,
    get_job,
    initialize_delivery_state,
    list_jobs,
    load_delivery_state,
    mark_turn_submitting,
    mark_uncertain,
    processed_seq_prefix,
    record_send_intent,
    record_send_receipt,
    requeue_definitive_busy,
    skip_job,
)
from outbox import append_entry, read_entries
from responder import Responder
from server import create_server
from source_messages import (
    SourceDeleted,
    SourceUnavailable,
    build_source_checker,
    check_source_message,
)

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
FAKE_SID = "2026-01-02T03-04-05_ab12"


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
        return self._guild if self._guild is not None and self._guild.id == gid else None

    async def fetch_channel(self, cid):
        raise LookupError(f"channel {cid} not found")


class ScriptClient:
    """Minimal scripted native client (snapshot/POST/trace only)."""

    def __init__(self, pre_max=3):
        self.pre_max = pre_max
        self.submit_calls: list = []
        self.submit_fn = None
        self.trace_fn = None

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
        return {"cancelled": False, "completed": True, "native_id_match": True}

    async def get_trace_evidence(
        self, sid, idx, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        if self.trace_fn is not None:
            return await self.trace_fn(sid, idx, job_ref, source_message_id, channel_id)
        submitted = self.submit_calls[-1]
        return {
            "request_input_sha256": hashlib.sha256(submitted.encode()).hexdigest(),
            "request_input_len": len(submitted),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
            "all_sends_confirmed": True,
            "legacy_uncorrelated_count": 0,
            "sends": [],
        }


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
    s = Settings(**_settings_kwargs(tmp_path))
    s.thyca_session_mapping_path.write_text(
        json.dumps(
            {
                "native_id": FAKE_SID,
                "title": s.session_id,
                "audit_label": s.session_id,
                "created_at": "2026-01-02T03:04:05+00:00",
                "path": f"/fake/{FAKE_SID}.json",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(s.thyca_session_mapping_path, 0o600)
    initialize_delivery_state(
        s.delivery_state_path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    return s


def _append(settings, mid, seq_note="", author_uid=OWNER_UID, kind="owner", name="O"):
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        author_name=name,
        actor_kind=kind,
        content=f"m{mid}{seq_note}",
        author_is_bot=False,
    )
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        actor_kind=kind,
        seq=read_entries(settings.outbox_path, settings.outbox_state_path, unread_only=False)[
            "latest"
        ],
    )


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


async def test_scope_mismatch_is_unavailable():
    channel = make_text_channel(CHANNEL_ID)

    async def wrong_scope(mid):
        return FakeMessage(mid, FakeUser(1, "x"), channel_id=999999, guild_id=GUILD_ID)

    channel.fetch_message = wrong_scope
    with pytest.raises(SourceUnavailable):
        await check_source_message(channel, 701)


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


async def test_malformed_scope_is_unavailable():
    channel = make_text_channel(CHANNEL_ID)

    async def malformed(mid):
        return SimpleNamespace(id=mid)  # no channel/guild scope

    channel.fetch_message = malformed
    with pytest.raises(SourceUnavailable):
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
_LEGACY_LEDGER_ID = "1e" * 16


class _LegacyLedger:
    """Respond-only stub: deterministic decided record, no provider call."""

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
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"
    assert job["job_ref"] is None and job["send_intents"] == [] and job["trace_refs"] is None
    assert out["acked"] == 1


async def test_pre_post_unavailable_holds_queued_zero_post(settings):
    async def unknown(cid, mid):
        raise SourceUnavailable("unknown")

    inst, _, client = await _responder(settings, source_checker=unknown)
    _append(settings, 701)
    out = await inst.tick()
    assert out["status"] == "pending" and out["reason"] == "source_unavailable"
    assert client.submit_calls == []
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
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

    async def trace_ok(sid, idx, job_ref, source_mid, channel_id):
        submitted = client.submit_calls[-1]
        return {
            "request_input_sha256": hashlib.sha256(submitted.encode()).hexdigest(),
            "request_input_len": len(submitted),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
            "all_sends_confirmed": True,
            "legacy_uncorrelated_count": 0,
            "sends": [],
        }

    client.trace_fn = trace_ok
    out = await inst.tick()
    assert out["status"] == "skipped"
    # 701 skipped with zero POST; second tick must POST for 702 (no intents -> uncertain,
    # but a POST proves the queue advanced past the skip)
    out2 = await inst.tick()
    assert out2["status"] == "uncertain"  # 702 POSTed once, zero intents held
    assert len(client.submit_calls) == 1
    assert get_job(settings.delivery_state_path, CHANNEL_ID, 701)["stage"] == "skipped"


async def test_deletion_during_submit_observed_as_skip(settings):
    async def exists(cid, mid):
        return None

    inst, _, client = await _responder(settings, source_checker=exists)
    _append(settings, 701)

    async def submit_and_delete(sid, text):
        skip_job(settings.delivery_state_path, CHANNEL_ID, 701, reason="source_deleted")
        return {"cancelled": False, "completed": True, "native_id_match": True}

    client.submit_fn = submit_and_delete
    out = await inst.tick()
    assert out["status"] == "skipped" and out["reason"] == "source_deleted"
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "skipped" and job["turn_index"] is None
    assert job["job_ref"] and job["submitted_text_sha256"]  # nonce/input metadata preserved
    out2 = await inst.tick()  # no retry / re-POST / completion
    assert len(client.submit_calls) == 1 and out2["status"] == "idle"


async def test_cancel_after_skip_propagates_and_preserves(settings):
    async def exists(cid, mid):
        return None

    inst, _, client = await _responder(settings, source_checker=exists)
    _append(settings, 701)

    async def submit_cancel(sid, text):
        skip_job(settings.delivery_state_path, CHANNEL_ID, 701, reason="source_deleted")
        raise asyncio.CancelledError()

    client.submit_fn = submit_cancel
    with pytest.raises(asyncio.CancelledError):
        await inst.tick()
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"


async def test_skip_refused_with_evidence_holds_uncertain(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, _ = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    mark_turn_submitting(
        settings.delivery_state_path, CHANNEL_ID, 701, job_ref="f" * 32, pre_turn_index=3
    )
    record_send_intent(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        send_ref="r1",
        content_len=5,
        content_hash="a" * 64,
    )
    out = inst._skip_source_deleted(CHANNEL_ID, 701)
    assert out["status"] == "uncertain" and out["reason"] == "send_evidence_present"
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "uncertain" and len(job["send_intents"]) == 1


async def test_uncertain_job_with_deleted_source_stays_uncertain(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, client = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        job_ref="f" * 32,
        pre_turn_index=3,
        submitted_text_sha256="c" * 64,
        submitted_text_len=10,
    )
    mark_uncertain(settings.delivery_state_path, CHANNEL_ID, 701, kind="transport_error")
    out = await inst.tick()
    assert out["status"] == "uncertain" and out["acked"] == 0
    assert get_job(settings.delivery_state_path, CHANNEL_ID, 701)["stage"] == "uncertain"
    assert client.submit_calls == []


async def test_partial_receipt_source_deleted_holds(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, client = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        job_ref="f" * 32,
        pre_turn_index=3,
        submitted_text_sha256="c" * 64,
        submitted_text_len=10,
    )
    record_send_intent(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        send_ref="r1",
        content_len=5,
        content_hash="a" * 64,
    )
    record_send_receipt(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        send_ref="r1",
        sent_message_id=9001,
        delivered_channel_id=CHANNEL_ID,
        jump_url=f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/9001",
        reply_to_message_id=701,
    )

    async def trace_partial(sid, idx, job_ref, source_mid, channel_id):
        submitted = client.submit_calls[-1] if client.submit_calls else "x"
        return {
            "request_input_sha256": hashlib.sha256(submitted.encode()).hexdigest(),
            "request_input_len": len(submitted),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
            "all_sends_confirmed": True,
            "legacy_uncorrelated_count": 0,
            "sends": [],
        }

    client.trace_fn = trace_partial
    out = await inst.tick()
    assert out["status"] == "uncertain" and out["acked"] == 0
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "uncertain" and len(job["send_receipts"]) == 1


async def test_aliases_safe_ack_on_skip(settings):
    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst, _, _ = await _responder(settings, source_checker=gone)
    _append(settings, 701)
    _append(settings, 701, seq_note="redelivered")  # same MID, second seq -> alias
    out = await inst.tick()
    assert out["status"] == "skipped"
    job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
    assert job["stage"] == "skipped" and len(job["seq_aliases"]) == 1
    assert out["acked"] == 2


# --- C: delivery gate --------------------------------------------------------


def test_skip_source_deleted_refuses_evidence_and_terminals(tmp_path):
    path = tmp_path / "d.json"
    initialize_delivery_state(
        path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    enqueue_job(path, channel_id=1, message_id=10, author_uid=7, actor_kind="owner", seq=1)
    enqueue_job(path, channel_id=1, message_id=11, author_uid=7, actor_kind="owner", seq=2)
    enqueue_job(path, channel_id=1, message_id=12, author_uid=7, actor_kind="owner", seq=3)
    # queued zero-evidence: ok + idempotent
    skip_job(path, 1, 10, reason="source_deleted")
    skip_job(path, 1, 10, reason="source_deleted")
    assert get_job(path, 1, 10)["stage"] == "skipped"
    # evidence refuses
    mark_turn_submitting(path, 1, 11, job_ref="f" * 32, pre_turn_index=0)
    record_send_intent(path, 1, 11, send_ref="r", content_len=1, content_hash="b" * 64)
    with pytest.raises(DeliveryError):
        skip_job(path, 1, 11, reason="source_deleted")
    # uncertain refuses
    mark_turn_submitting(path, 1, 12, job_ref="e" * 32, pre_turn_index=0)
    mark_uncertain(path, 1, 12, kind="timeout")
    with pytest.raises(DeliveryError):
        skip_job(path, 1, 12, reason="source_deleted")
    # other reasons keep approved behavior
    enqueue_job(path, channel_id=1, message_id=13, author_uid=7, actor_kind="owner", seq=4)
    skip_job(path, 1, 13, reason="out_of_scope")
    assert get_job(path, 1, 13)["skip_reason"] == "out_of_scope"


# --- D: server MCP gate ------------------------------------------------------


def _seed_active(settings, mid, ref):
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=OWNER_UID,
        author_name="O",
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
        seq=1,
    )
    mark_turn_submitting(
        settings.delivery_state_path, CHANNEL_ID, mid, job_ref=ref, pre_turn_index=3
    )


async def test_mcp_gate_skips_deleted_zero_sdk(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["deleted"].add(701)
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
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
        assert channel.sent == [] and channel.source_store["calls"] == [701]
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "skipped" and job["send_intents"] == []
    recs = [r for r in _audit(settings) if r["tool"] == "send_message" and r["ok"]]
    assert recs and recs[-1]["args"]["source_message_id"] == 701
    blob = json.dumps(recs[-1])
    assert "reply" not in blob and "Unknown Message" not in blob and "Traceback" not in blob


async def test_mcp_gate_evidence_holds_uncertain_no_send(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    record_send_intent(
        settings.delivery_state_path,
        CHANNEL_ID,
        701,
        send_ref="r1",
        content_len=5,
        content_hash="a" * 64,
    )
    channel.source_store["deleted"].add(701)
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain" and len(job["send_intents"]) == 1


async def test_mcp_gate_unavailable_no_sdk_no_mutation(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["exc"] = asyncio.TimeoutError()
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "turn_submitting" and job["send_intents"] == []


async def test_mcp_gate_repeat_skip_rejected_no_send(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["deleted"].add(701)
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
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
        assert get_job(settings.delivery_state_path, CHANNEL_ID, 701)["send_intents"] == []


async def test_mcp_gate_post_intent_404_stays_uncertain(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["send_exc"] = _deleted_exc()  # SDK fails AFTER intent
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain"  # never converted to skip
        assert len(job["send_intents"]) == 1 and job["skip_reason"] is None


async def test_mcp_gate_stale_stage_rejects_skip(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["deleted"].add(701)
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        # concurrent hold landed during the existence read: skip must not fire
        mark_uncertain(settings.delivery_state_path, CHANNEL_ID, 701, kind="transport_error")
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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "uncertain" and job["skip_reason"] is None


async def test_mcp_gate_send_blocked_latch_withholds_skip(settings, monkeypatch):
    import server.core as server_mod

    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    channel.source_store["send_exc"] = RuntimeError("sdk boom")
    real_get_job = server_mod.get_job

    def flaky_get_job(*a, **k):
        raise DeliveryError("state fault")

    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        # SDK failure while the journal read faults: latch set, job untouched.
        monkeypatch.setattr(server_mod, "get_job", flaky_get_job)
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
        monkeypatch.setattr(server_mod, "get_job", real_get_job)
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "delivering" and len(job["send_intents"]) == 1
        # latched: even with the source now deleted, no skip and no new send.
        channel.source_store["deleted"].add(701)
        channel.source_store["send_exc"] = None
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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "delivering" and len(job["send_intents"]) == 1
        assert job["skip_reason"] is None and channel.sent == []


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
        subs = [
            j for j in list_jobs(settings.delivery_state_path) if j["stage"] == "turn_submitting"
        ]
        assert len(subs) == 1
        job = subs[0]
        channel.source_store["deleted"].add(job["message_id"])
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
        return {"cancelled": False, "completed": True, "native_id_match": True}

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
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "skipped" and job["turn_index"] is None
        assert job["send_intents"] == [] and job["send_receipts"] == []
        assert channel.sent == [] and len(backend.submit_calls) == 1
        outcome2 = await ctx["responder"].tick()
        assert len(backend.submit_calls) == 1 and outcome2["status"] == "idle"


async def test_source_present_guard_fences_latched_race_no_second_intent(settings, monkeypatch):
    """Fresh-guard positive branch: A parks in the source-present GET while B
    records the one intent, fails the SDK, and faults the hold write (job
    stays delivering, latch set). After faults restore and A resumes PRESENT,
    A must reject before a second intent/SDK: exactly 1 attempt, 1 intent,
    evidence preserved, no completion. Real FastMCP + real delivery + real
    Responder correlation; no live I/O."""
    import server.core as server_mod

    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    ref = "f" * 32
    _seed_active(settings, 701, ref)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = {"n": 0}
    orig_fetch = channel.fetch_message

    async def gated_fetch(mid):
        calls["n"] += 1
        if calls["n"] == 1:
            entered.set()
            await release.wait()
        return await orig_fetch(mid)

    channel.fetch_message = gated_fetch
    channel.source_store["send_exc"] = RuntimeError("sdk boom")
    real_mark_uncertain = server_mod.mark_uncertain

    def flaky_mark_uncertain(*a, **k):
        raise DeliveryError("hold write fault")

    call_args = {
        "content": "hi",
        "channel_id": CHANNEL_ID,
        "source_message_id": 701,
        "job_ref": ref,
    }
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        task_a = asyncio.create_task(mcp.call_tool("send_message", dict(call_args)))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            monkeypatch.setattr(server_mod, "mark_uncertain", flaky_mark_uncertain)
            with pytest.raises(ToolError):
                await mcp.call_tool("send_message", dict(call_args))
            assert channel.source_store["attempts"] == 1
            job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
            assert job["stage"] == "delivering" and len(job["send_intents"]) == 1
        finally:
            # Restore BEFORE teardown/release so A resumes against the
            # real guard; always release so the task can never dangle.
            monkeypatch.setattr(server_mod, "mark_uncertain", real_mark_uncertain)
            channel.source_store["send_exc"] = None
            release.set()
        with pytest.raises(ToolError):
            await asyncio.wait_for(task_a, timeout=5)
        assert channel.source_store["attempts"] == 1
        assert channel.sent == []
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["stage"] == "delivering" and len(job["send_intents"]) == 1
        assert job["skip_reason"] is None and job["stage"] != "completed"


async def test_source_present_guard_rejects_stale_nonce_zero_send(settings, monkeypatch):
    """Nonce rotation during the source-present GET (supported caller flow:
    requeue_definitive_busy + mark_turn_submitting with a new ref) must make
    the old call fresh-correlation-reject with 0 intent/SDK."""
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    old_ref = "f" * 32
    _seed_active(settings, 701, old_ref)
    entered = asyncio.Event()
    release = asyncio.Event()
    orig_fetch = channel.fetch_message
    gated = {"on": True}

    async def gated_fetch(mid):
        if gated["on"]:
            gated["on"] = False
            entered.set()
            await release.wait()
        return await orig_fetch(mid)

    channel.fetch_message = gated_fetch
    call_args = {
        "content": "hi",
        "channel_id": CHANNEL_ID,
        "source_message_id": 701,
        "job_ref": old_ref,
    }
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        task_a = asyncio.create_task(mcp.call_tool("send_message", dict(call_args)))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            requeue_definitive_busy(settings.delivery_state_path, CHANNEL_ID, 701, http_status=409)
            new_ref = "e" * 32
            mark_turn_submitting(
                settings.delivery_state_path,
                CHANNEL_ID,
                701,
                job_ref=new_ref,
                pre_turn_index=3,
            )
        finally:
            release.set()
        with pytest.raises(ToolError):
            await asyncio.wait_for(task_a, timeout=5)
        assert channel.source_store["attempts"] == 0 and channel.sent == []
        job = get_job(settings.delivery_state_path, CHANNEL_ID, 701)
        assert job["job_ref"] == new_ref and job["stage"] == "turn_submitting"
        assert job["send_intents"] == []


def test_corrupt_source_deleted_skip_with_evidence_fails_load_no_ack(tmp_path):
    """Valid-JSON contradictory tombstone (skipped/source_deleted + send
    evidence) must fail load AND safe-prefix computation before any ACK, while
    the identical legacy out_of_scope skip keeps loading/ACKing as before and
    a zero-evidence source_deleted skip still loads/ACKs."""
    path = tmp_path / "d.json"
    initialize_delivery_state(
        path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    enqueue_job(path, channel_id=1, message_id=10, author_uid=7, actor_kind="owner", seq=1)
    mark_turn_submitting(path, 1, 10, job_ref="f" * 32, pre_turn_index=0)
    record_send_intent(path, 1, 10, send_ref="r", content_len=1, content_hash="b" * 64)
    record_send_receipt(
        path,
        1,
        10,
        send_ref="r",
        sent_message_id=9001,
        delivered_channel_id=1,
        jump_url="https://discord.com/channels/9/1/9001",
        reply_to_message_id=10,
    )
    skip_job(path, 1, 10, reason="out_of_scope")  # legacy + evidence: supported
    entries = [{"channel_id": 1, "message_id": 10, "seq": 1}]
    assert load_delivery_state(path)["jobs"]["1:10"]["skip_reason"] == "out_of_scope"
    assert processed_seq_prefix(path, entries, 0) == 1  # positive control
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["1:10"]["skip_reason"] = "source_deleted"  # ONLY field flipped
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError):
        load_delivery_state(bad)
    with pytest.raises(DeliveryError):
        processed_seq_prefix(bad, entries, 0)
    # Zero-evidence source_deleted still loads and ACKs (no over-rejection).
    clean = tmp_path / "clean.json"
    initialize_delivery_state(
        clean,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    enqueue_job(clean, channel_id=1, message_id=11, author_uid=7, actor_kind="owner", seq=1)
    skip_job(clean, 1, 11, reason="source_deleted")
    assert load_delivery_state(clean)["jobs"]["1:11"]["skip_reason"] == "source_deleted"
    assert processed_seq_prefix(clean, [{"channel_id": 1, "message_id": 11, "seq": 1}], 0) == 1


async def test_manual_sends_unchanged(settings, monkeypatch):
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _parked(monkeypatch, settings, bot) as (mcp, _):
        payload = await _call_json(mcp, "send_message", {"content": "manual hello"})
        assert payload["channel_id"] == CHANNEL_ID and payload["message_id"] == 9000
        assert len(channel.sent) == 1
