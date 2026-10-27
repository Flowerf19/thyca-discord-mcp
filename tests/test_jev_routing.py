"""Jev routing integration tests: REAL store + REAL ledger, fakes elsewhere.

Every test runs the real SQLite store and the real DB-backed
``ParticipationLedger`` (tmp paths) with a fake Jev adapter (scripted
``JevDecision`` objects, never the network), a fake canonical collector
(scripted snapshots), a fake native ``ThycaClient``, and a fake
bot/source surface. No live Discord/Thyca/Jev, no real .env (all settings
use ``_env_file=None``), no paid calls.

Completion is snapshot-driven (no trace proof): after the POST the worker
reloads the job -- a recorded send or no_reply stands; otherwise the POST
result itself must prove a clean idle turn, else later ticks poll
``get_session_snapshot`` until idle and close the same way. A completed
idle turn with no outcome fails (``turn_no_outcome``); ambiguous
send/POST outcomes fail; restarts fail ``submitted`` jobs.

Coverage: respond completion with the SAME canonical snapshot feeding Jev
and the prompt, observe as a durable zero-POST terminal with safe ACK,
need_context via agent send and via terminal no_reply, 409/restart
cache reuse with no rebill, the crash-pending retry-once rule at worker
level, cancellation/unknown/global-block holds, pre/post-paid deletion,
edit/context-change holds, FIFO + ACK, nonce races, classifier privacy
(sender/addressee never identity), full-context 4000 budget with no
truncation, pre-classify UID binding for ALL routes.

Deleted (reasons): the trace-evidence proof block and every
intent/receipt/trace/nonce-binding assertion, the uncertain-hold and
retry-latch suites, the ledger<->journal marker/loss-guard/binding
matrix, and ``uncertain_legacy_never_classifies`` -- those stages and
concepts no longer exist.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest

from config import Settings
from context import ContextUnavailable, context_fingerprint
from conversation import project_conversation_state
from jev import JEV_MODEL, JevDecision, JevError, build_questions
from participation import ParticipationLedger
from responder import Responder, ResponderError
from source_messages import SourceDeleted
from store import Store, append_entry
from thyca import ThycaBusyError

SID = "2026-01-02T03-04-05_ab12"
SID2 = "2026-01-03T04-05-06_cd34"
DAY = "2026-01-02"
GUILD = 111000000000000001
CHANNEL = 222000000000000002
OWNER = 333000000000000003
BOT = 444000000000000004
OTHER = 555000000000000005
THIRD = 666000000000000006
SOURCE_MID = 777000000000000007
SENT_BASE = 888000000000000000


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _jump(mid: int) -> str:
    return f"https://discord.com/channels/{GUILD}/{CHANNEL}/{mid}"


def _settings(tmp_path, **overrides):
    kwargs: dict = {
        "discord_token": "fake-token",
        "owner_uid": OWNER,
        "guild_id": GUILD,
        "default_channel_id": CHANNEL,
        "audit_path": tmp_path / "audit.jsonl",
        "db_path": tmp_path / "state.db",
        "_env_file": None,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def _prepare(tmp_path):
    settings = _settings(tmp_path)
    with Store.open(settings.db_path) as store:
        store.adopt_session(expected_current=None, new_sid=SID, day=DAY)
    return settings


def _entry_fields(**overrides):
    base = {
        "type": "message",
        "channel_id": CHANNEL,
        "message_id": SOURCE_MID,
        "author_uid": OTHER,
        "author_name": "Ramon",
        "actor_kind": "other",
        "content": "hey all, what did we decide?",
        "author_is_bot": False,
    }
    base.update(overrides)
    return base


def _append(settings, **overrides):
    return append_entry(settings.db_path, **_entry_fields(**overrides))


def _job(settings, mid=SOURCE_MID):
    with Store.open(settings.db_path) as store:
        return store.get_job(CHANNEL, mid)


# --- fake bot/source surface (reads only, never a send) ----------------------


def _deleted_exc():
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def _make_source_channel(cid, guild_id=GUILD):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    store: dict = {"deleted": set(), "exc": None, "calls": []}

    async def fake_fetch(mid):
        store["calls"].append(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_exc()
        return SimpleNamespace(
            id=mid, channel=SimpleNamespace(id=cid), guild=SimpleNamespace(id=guild_id)
        )

    channel.fetch_message = fake_fetch
    channel.source_store = store
    return channel


class _FakeGuild:
    def __init__(self, gid):
        self.id = gid
        self._channels: dict = {}

    def get_channel(self, cid):
        channel = self._channels.get(cid)
        if channel is None:
            channel = _make_source_channel(cid, self.id)
            self._channels[cid] = channel
        return channel

    def get_member(self, uid):
        return None


class _FakeBot:
    """Read-only bot surface: user identity + guild/channel resolution."""

    def __init__(self, bot_uid=BOT, guild_id=GUILD):
        self.user = SimpleNamespace(id=bot_uid, bot=True, display_name="Bot")
        self._guild = _FakeGuild(guild_id)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        raise AssertionError(f"forbidden bot attribute: {name}")

    def get_guild(self, gid):
        return self._guild if gid == self._guild.id else None

    def get_user(self, uid):
        return None

    async def fetch_user(self, uid):
        raise LookupError("user not found")


def _delete_source(bot, cid, mid):
    bot.get_guild(GUILD).get_channel(cid).source_store["deleted"].add(mid)


# --- fake native client (snapshot-driven, no trace) ---------------------------


class _FakeClient:
    def __init__(self):
        self.snapshot_calls: list = []
        self.submit_calls: list = []
        self.dm_calls: list = []
        self.snapshot_fn = None
        self.submit_fn = None
        self.dm_sid = SID

    async def get_bot_dm(self, bot_id):
        self.dm_calls.append(bot_id)
        return {
            "native_sid": self.dm_sid,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }

    async def get_session_snapshot(self, sid, expected_title=None):
        self.snapshot_calls.append((sid, expected_title))
        if self.snapshot_fn is not None:
            return await self.snapshot_fn(sid, expected_title)
        return {
            "native_sid": sid,
            "native_id_match": True,
            "title_match": True if expected_title else None,
            "running": False,
            "turn_count": 0,
            "turn_indices": [],
            "pre_max_turn_index": None,
        }

    async def submit_turn(self, sid, text, model=None, effort=None):
        self.submit_calls.append((sid, text, model, effort))
        if self.submit_fn is not None:
            return await self.submit_fn(sid, text, model, effort)
        cleaned = text.strip()
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": 0,
            "turn_count": 1,
            "running": False,
            "submitted_text_len": len(cleaned),
            "submitted_text_sha256": _sha(cleaned),
            "stored_text_match": True,
            "stored_text_len": len(cleaned),
        }


# --- fake Jev adapter (scripted decisions, never the network) ------------------


def _decision_for(snapshot, participation, addressee=None, noul=0.1):
    """Build a ledger-valid JevDecision for the snapshot's real questions."""
    questions = build_questions(snapshot)
    answers = {
        "addressee": {
            "type": "choice",
            "choice": addressee if addressee is not None else "channel",
            "probabilities": {addressee if addressee is not None else "channel": 1.0},
        },
        "explicit_assistant_request": {"type": "noul", "noul": noul},
        "participation": {
            "type": "choice",
            "choice": participation,
            "probabilities": {participation: 1.0},
        },
    }
    assert set(answers) == set(questions)
    return JevDecision(
        answers=answers,
        usage={"input_tokens": 10, "output_tokens": 5},
        model=JEV_MODEL,
        latency_ms=3,
    )


class _FakeAdapter:
    def __init__(self, participation="respond", **decision_kwargs):
        self.script = list(participation) if isinstance(participation, list) else [participation]
        self.decision_kwargs = decision_kwargs
        self.calls: list = []
        self.block_event: asyncio.Event | None = None
        self.raise_exc = None
        self.hook = None  # sync callable run inside decide (race simulation)

    async def decide(self, state):
        self.calls.append(copy.deepcopy(state))
        if self.block_event is not None:
            await self.block_event.wait()
        if self.hook is not None:
            self.hook()
        if self.raise_exc is not None:
            raise self.raise_exc
        part = self.script[min(len(self.calls) - 1, len(self.script) - 1)]
        return _decision_for(state, part, **self.decision_kwargs)


# --- fake canonical collector (scripted snapshots) -----------------------------


class _FakeCollector:
    """Deterministic snapshots bound to the frozen entry plus scripted history."""

    def __init__(self, owner_uid=OWNER, bot_uid=BOT, history=()):
        self.owner_uid = owner_uid
        self.bot_uid = bot_uid
        self.history = list(history)  # (mid, uid, name, text, reply_to|None)
        self.calls: list = []
        # Per-call script: None | "unavailable" | "deleted" | ("edit", text)
        # | ("sender"|"owner"|"assistant", uid).
        self.script: list = []

    async def __call__(self, entry):
        self.calls.append(dict(entry))
        action = self.script.pop(0) if self.script else None
        if action == "unavailable":
            raise ContextUnavailable("fake unavailable")
        if action == "deleted":
            raise SourceDeleted("fake deleted")
        mid = entry["message_id"]
        author = entry["author_uid"]
        content = entry["content"]
        sender = str(author)
        owner = str(self.owner_uid)
        assistant = str(self.bot_uid)
        if isinstance(action, tuple):
            kind, value = action
            if kind == "edit":
                content = value
            elif kind == "sender":
                sender = str(value)
            elif kind == "owner":
                owner = str(value)
            elif kind == "assistant":
                assistant = str(value)
        uids: dict[str, str] = {owner: "Olivia", assistant: "HelperBot"}
        messages: list[dict] = []
        for hmid, huid, hname, htext, hreply in self.history:
            uids.setdefault(str(huid), hname)
            msg: dict = {"id": str(hmid), "sender_uid": str(huid), "text": htext}
            if hreply is not None:
                msg["reply_to"] = str(hreply)
            messages.append(msg)
        uids.setdefault(sender, entry.get("author_name") or f"uid-{author}")
        messages.append({"id": str(mid), "sender_uid": sender, "text": content})
        return {
            "assistant_uid": assistant,
            "owner_uid": owner,
            "participants": [{"uid": uid, "display_name": name} for uid, name in uids.items()],
            "messages": messages,
            "new_message_id": str(mid),
        }


# --- snapshot-driven native turn ----------------------------------------------


def _wire_turn(
    client, settings, *, source_mid, own_index=0, pre_max=None, agent=None, submit_running=False
):
    """Drive FakeClient without trace proof; run the agent sim on POST.

    ``agent`` is ``None`` (no agent action), ``("send", (chunks...))``
    (one store send per chunk, like the MCP tool), or
    ``("declare", reason)`` (terminal no_reply at tool time, like the MCP
    tool). ``submit_running`` returns a still-running POST result so later
    ticks must poll the snapshot; the snapshot reports idle unless
    ``snapshot_running`` scripted otherwise.
    """
    seen: dict = {"texts": [], "turns": [], "declares": []}
    pre_indices = [] if pre_max is None else list(range(pre_max + 1))
    running_flags: list = []

    async def snapshot(sid, expected_title):
        done = [t for t in seen["turns"] if t["done"]]
        indices = sorted({*pre_indices, *(t["index"] for t in done)})
        running = running_flags.pop(0) if running_flags else False
        return {
            "native_sid": sid,
            "native_id_match": True,
            "title_match": True if expected_title else None,
            "running": running,
            "turn_count": len(indices),
            "turn_indices": indices,
            "pre_max_turn_index": (max(indices) if indices else None),
        }

    async def submit(sid, text, model=None, effort=None):
        with Store.open(settings.db_path) as store:
            job = store.get_job(CHANNEL, source_mid)
            ref = job["job_ref"]
        seen["texts"].append(text)
        turn = {"index": own_index + len(seen["texts"]) - 1, "text": text, "done": False}
        seen["turns"].append(turn)
        if agent is not None and agent[0] == "send":
            for i, _chunk in enumerate(agent[1]):
                with Store.open(settings.db_path) as store:
                    store.record_send(
                        CHANNEL, source_mid, job_ref=ref, sent_message_id=SENT_BASE + i
                    )
        elif agent is not None and agent[0] == "declare":
            with Store.open(settings.db_path) as store:
                store.complete_job(CHANNEL, source_mid, outcome="no_reply")
            seen["declares"].append(agent[1])
        turn["done"] = True
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": turn["index"],
            "turn_count": len(seen["turns"]),
            "running": submit_running,
            "submitted_text_len": len(text),
            "submitted_text_sha256": _sha(text),
            "stored_text_match": True,
            "stored_text_len": len(text),
        }

    client.snapshot_fn = snapshot
    client.submit_fn = submit
    return seen, running_flags


def _responder(settings, bot=None, client=None, adapter=None, collector=None, ledger=None):
    bot = bot or _FakeBot()
    client = client or _FakeClient()
    adapter = adapter if adapter is not None else _FakeAdapter("respond")
    collector = collector if collector is not None else _FakeCollector()
    if ledger is None:
        ledger = ParticipationLedger(settings.db_path)
    return Responder(
        settings,
        bot,
        client=client,
        jev_adapter=adapter,
        context_collector=collector,
        participation_ledger=ledger,
    )


def _prompt_json(prompt):
    lines = prompt.split("\n")
    start = next(i for i, line in enumerate(lines) if line.endswith("START"))
    end = next(i for i, line in enumerate(lines) if line.endswith("END"))
    return json.loads("\n".join(lines[start + 1 : end]))


# --- respond route -------------------------------------------------------------


async def test_respond_same_snapshot_completes_and_acks(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("reply-body",)))
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert outcome["acked"] == entry["seq"]
    assert len(client.submit_calls) == 1
    sid, text, _, _ = client.submit_calls[0]
    assert sid == SID
    # SAME canonical snapshot feeds Jev and the prompt.
    assert len(adapter.calls) == 1
    projected = project_conversation_state(adapter.calls[0])
    prompt = _prompt_json(text)
    assert prompt == projected
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "sent")
    assert job["send_count"] == 1
    assert job["sent_message_ids"] == [SENT_BASE]


async def test_respond_prompt_carries_stop_guards(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    responder = _responder(settings, client=client)
    await responder.tick()
    _, text, _, _ = client.submit_calls[0]
    assert "TRUSTED_METADATA" in text
    assert "UNTRUSTED_CONVERSATION" in text
    assert "NEVER read/write" in text
    assert "NEVER disclose" in text
    assert "whoami(sender_uid)" in text


async def test_full_context_preserved_no_truncation(tmp_path):
    settings = _prepare(tmp_path)
    history = tuple((900 + i, OTHER, f"User{OTHER}", f"filler {i}", None) for i in range(5))
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    responder = _responder(settings, client=client, collector=_FakeCollector(history=history))
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    _, text, _, _ = client.submit_calls[0]
    prompt = _prompt_json(text)
    assert len(prompt["messages"]) == 6  # 5 history + source, nothing truncated


async def test_jev_answers_never_identity(tmp_path):
    """Classifier sender/addressee answers never authenticate the sender."""
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    adapter = _FakeAdapter("respond", addressee="channel", noul=0.0)
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert len(client.submit_calls) == 1


async def test_multi_chunk_two_sends_complete(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(
        client, settings, source_mid=SOURCE_MID, agent=("send", ("part-one ", "part-two"))
    )
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "sent")
    assert job["send_count"] == 2
    assert job["sent_message_ids"] == [SENT_BASE, SENT_BASE + 1]


# --- observe / no_reply routes --------------------------------------------------


async def test_observe_zero_post_terminal_ack(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "observed"
    assert outcome["acked"] == entry["seq"]
    assert client.submit_calls == []
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "observed")


async def test_zero_sends_completed_turn_fails_no_outcome(tmp_path):
    """A completed idle turn with no send and no record_no_reply fails."""
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=None)
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "turn_no_outcome"
    job = _job(settings)
    assert job["stage"] == "failed"
    assert job["fail_kind"] == "turn_no_outcome"


async def test_need_context_agent_send_completes(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("ctx",)))
    responder = _responder(settings, client=client, adapter=_FakeAdapter("respond"))
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert _job(settings)["outcome"] == "sent"


async def test_need_context_declare_terminal_at_tool_time(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(
        client, settings, source_mid=SOURCE_MID, agent=("declare", "no_reply_needed")
    )
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "no_reply"
    assert outcome["acked"] == entry["seq"]
    assert _job(settings)["outcome"] == "no_reply"


# --- restart / reuse --------------------------------------------------------------


async def test_409_reuses_cached_no_rebill(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))

    busy_first = True
    real_submit = client.submit_fn

    async def busy_then_ok(sid, text, model=None, effort=None):
        nonlocal busy_first
        if busy_first:
            busy_first = False
            raise ThycaBusyError(status_code=409)
        return await real_submit(sid, text, model, effort)

    client.submit_fn = busy_then_ok
    responder = _responder(settings, client=client, adapter=adapter)
    first = await responder.tick()
    assert first["status"] == "pending"
    assert first["reason"] == "busy_requeued"
    assert len(adapter.calls) == 1
    job = _job(settings)
    assert job["stage"] == "queued"
    assert job["job_ref"] is None
    second = await responder.tick()
    assert second["status"] == "sent"
    assert len(adapter.calls) == 1  # cached decision reused, no rebill
    assert len(seen["texts"]) == 1


async def test_restart_reuses_cache_no_rebill(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID)

    async def busy_once(sid, text, model=None, effort=None):
        raise ThycaBusyError(status_code=409)

    client.submit_fn = busy_once
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["reason"] == "busy_requeued"
    assert len(adapter.calls) == 1
    # Restart: a fresh Responder over the same DB sweeps nothing (queued)
    # and resumes with the cached decision -- zero new provider calls.
    client2 = _FakeClient()
    seen2, _ = _wire_turn(client2, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    responder2 = _responder(settings, client=client2, adapter=adapter)
    outcome2 = await responder2.tick()
    assert outcome2["status"] == "sent"
    assert len(adapter.calls) == 1
    assert len(seen2["texts"]) == 1


async def test_restart_submitted_failed_queued_resumes(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    mid2 = SOURCE_MID + 1
    _append(settings, message_id=mid2)
    client = _FakeClient()
    # First job's POST reports a still-running turn: it stays submitted.
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=None, submit_running=True)
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "turn_running"
    assert _job(settings)["stage"] == "submitted"
    # Restart fails the submitted job; the queued job resumes normally.
    _responder(settings, client=_FakeClient())  # construction runs the sweep
    failed = _job(settings)
    assert failed["stage"] == "failed"
    assert failed["fail_kind"] == "restart"
    assert _job(settings, mid2)["stage"] == "queued"


async def test_running_turn_closes_on_later_idle_tick(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _running = _wire_turn(
        client, settings, source_mid=SOURCE_MID, agent=None, submit_running=True
    )
    responder = _responder(settings, client=client)
    first = await responder.tick()
    # POST reported a still-running turn: the job stays submitted.
    assert first["status"] == "pending"
    assert first["reason"] == "turn_running"
    assert _job(settings)["stage"] == "submitted"
    second = await responder.tick()
    assert second["status"] == "failed"
    assert second["reason"] == "turn_no_outcome"


async def test_cancellation_unknown_blocks_retry(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    adapter = _FakeAdapter("respond")
    adapter.raise_exc = asyncio.CancelledError()
    responder = _responder(settings, adapter=adapter)
    with pytest.raises(asyncio.CancelledError):
        await responder.tick()
    # The cancelled classification persists failed_unknown; the next tick
    # fails the job instead of retrying the provider call.
    adapter.raise_exc = None
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "classification_unknown"
    assert len(adapter.calls) == 1


async def test_no_daily_stop_both_classify_same_day(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    mid2 = SOURCE_MID + 1
    _append(settings, message_id=mid2)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.db_path, day_provider=lambda: DAY)
    responder = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    # No daily spend stop: both sources classify and complete the same day.
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("a",)))
    first = await responder.tick()
    assert first["status"] == "sent"
    _append(settings, message_id=mid2)
    seen2, _ = _wire_turn(client, settings, source_mid=mid2, agent=("send", ("b",)))
    second = await responder.tick()
    assert second["status"] == "sent"
    assert len(adapter.calls) == 2
    with Store.open(settings.db_path) as store:
        assert store.jev_cost_for_day(DAY) == "0.00000084"


async def test_unknown_blocked_preserves_innocent_job(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    adapter = _FakeAdapter("respond")
    adapter.raise_exc = JevError(kind="timeout", unknown_billing=True)
    responder = _responder(settings, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "classification_unknown"
    # A second source stays queued (preserved, never terminalized) while
    # the FIFO visibly halts for operator resolution.
    mid2 = SOURCE_MID + 1
    _append(settings, message_id=mid2)
    responder2 = _responder(settings)
    outcome2 = await responder2.tick()
    assert outcome2["status"] == "pending"
    assert outcome2["reason"] == "unknown_blocked"
    assert _job(settings, mid2)["stage"] == "queued"


async def test_crashed_pending_worker_retries_once_then_decides(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL, SOURCE_MID, entry["seq"])
    # Crash between the Jev pending insert and its resolve.
    from conversation import project_conversation_state as _project

    collector = _FakeCollector()
    snapshot = await collector(_entry_fields())
    projected = _project(snapshot)
    fingerprint = context_fingerprint(projected)
    with Store.open(settings.db_path) as store:
        store.insert_jev_pending(
            CHANNEL,
            SOURCE_MID,
            context_sha256=fingerprint,
            attempt_id="att1",
            author_uid=OTHER,
            attempted_day=DAY,
        )
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert len(adapter.calls) == 1  # exactly the one retry
    with Store.open(settings.db_path) as store:
        assert store.get_jev(CHANNEL, SOURCE_MID)["attempts"] == 2


async def test_crashed_pending_attempts_two_resolves_observe(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL, SOURCE_MID, entry["seq"])
    collector = _FakeCollector()
    snapshot = await collector(_entry_fields())
    projected = project_conversation_state(snapshot)
    fingerprint = context_fingerprint(projected)
    with Store.open(settings.db_path) as store:
        store.insert_jev_pending(
            CHANNEL,
            SOURCE_MID,
            context_sha256=fingerprint,
            attempt_id="att1",
            author_uid=OTHER,
            attempted_day=DAY,
        )
        store.bump_jev_retry(
            CHANNEL,
            SOURCE_MID,
            old_attempt_id="att1",
            new_attempt_id="att2",
            attempted_day=DAY,
        )
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "observed"
    assert len(adapter.calls) == 0  # never a third paid call
    assert client.submit_calls == []  # observe never POSTs


# --- source / snapshot guards ---------------------------------------------------


async def test_source_deleted_before_paid_skips_zero_provider(tmp_path):
    settings = _prepare(tmp_path)
    bot = _FakeBot()
    _append(settings)
    _delete_source(bot, CHANNEL, SOURCE_MID)
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, bot=bot, adapter=adapter)
    outcome = await responder.tick()
    assert outcome == {
        "status": "skipped",
        "reason": "source_deleted",
        "safe_prefix": 1,
        "acked": 1,
        "latest": 1,
    }
    assert len(adapter.calls) == 0
    job = _job(settings)
    assert job["stage"] == "skipped"


async def test_source_deleted_during_paid_skips_sunk_no_post(tmp_path):
    settings = _prepare(tmp_path)
    bot = _FakeBot()
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")

    def _delete_during_decide():
        _delete_source(bot, CHANNEL, SOURCE_MID)

    adapter.hook = _delete_during_decide
    responder = _responder(settings, bot=bot, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    assert client.submit_calls == []  # classification spend sunk, zero native POST


async def test_collector_deletion_during_collect_skips(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    collector = _FakeCollector()
    collector.script = ["deleted"]
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, adapter=adapter, collector=collector)
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert len(adapter.calls) == 0


async def test_collector_unavailable_holds_zero_paid(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    collector = _FakeCollector()
    collector.script = ["unavailable"]
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, adapter=adapter, collector=collector)
    outcome = await responder.tick()
    assert outcome == {
        "status": "pending",
        "reason": "context_unavailable",
        "safe_prefix": 0,
        "acked": 0,
        "latest": 1,
    }
    assert len(adapter.calls) == 0


async def test_edit_fp_mismatch_proven_skips_no_bind_no_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    collector = _FakeCollector()
    collector.script = [None, ("edit", "edited after capture")]
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, adapter=adapter, collector=collector)
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_edited_after_classify"
    assert client.submit_calls == []


@pytest.mark.parametrize("action", ["sender", "owner", "assistant"])
async def test_wrong_uid_snapshot_rejected_all_routes(tmp_path, action):
    settings = _prepare(tmp_path)
    _append(settings)
    collector = _FakeCollector()
    collector.script = [(action, 424242)]
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, adapter=adapter, collector=collector)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "context_mismatch"
    assert len(adapter.calls) == 0


async def test_snapshot_oversize_fails_zero_paid_no_truncate(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, content="x" * 2000)
    collector = _FakeCollector(history=tuple((i, THIRD, "T", "y" * 250, None) for i in range(19)))
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, adapter=adapter, collector=collector)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "snapshot_oversize"
    assert len(adapter.calls) == 0


async def test_fifo_observe_then_respond_ack_prefix(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    mid2 = SOURCE_MID + 1
    _append(settings, message_id=mid2)
    client = _FakeClient()
    adapter = _FakeAdapter(["observe", "respond"])
    seen, _ = _wire_turn(client, settings, source_mid=mid2, agent=("send", ("x",)))
    responder = _responder(settings, client=client, adapter=adapter)
    first = await responder.tick()
    assert first["status"] == "observed"
    assert first["acked"] == 1
    second = await responder.tick()
    assert second["status"] == "sent"
    assert second["acked"] == 2


async def test_midflight_guard_race_respected_no_double_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=None)
    real_submit = client.submit_fn

    async def _complete_during_post(sid, text, model=None, effort=None):
        # The MCP tool completes no_reply during the POST await; the
        # worker respects the terminal instead of closing over it.
        with Store.open(settings.db_path) as store:
            store.complete_job(CHANNEL, SOURCE_MID, outcome="no_reply")
        return await real_submit(sid, text, model, effort)

    client.submit_fn = _complete_during_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "no_reply"
    assert len(client.submit_calls) == 1  # exactly one POST, never a second


async def test_pre_classify_uid_binding(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, author_uid=THIRD, actor_kind="other")
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    with Store.open(settings.db_path) as store:
        record = store.get_jev(CHANNEL, SOURCE_MID)
    assert record is not None
    assert record["author_uid"] == THIRD


async def test_out_of_scope_and_self_filtered_idle(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, channel_id=999999)
    _append(
        settings, message_id=SOURCE_MID + 1, author_uid=BOT, author_name="Bot", actor_kind="self"
    )
    responder = _responder(settings)
    outcome = await responder.tick()
    assert outcome["status"] == "idle"
    assert outcome["acked"] == 2
    with Store.open(settings.db_path) as store:
        assert store.get_job(999999, SOURCE_MID)["skip_reason"] == "out_of_scope"
        assert store.get_job(CHANNEL, SOURCE_MID + 1)["skip_reason"] == "bot_filtered"


async def test_guard_ack_prefix_rejects_beyond_terminal(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    responder = _responder(settings)
    with pytest.raises(ResponderError):
        responder.guard_ack(1)
    responder.guard_ack(0)


async def test_correlated_job_accepts_only_inflight(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    responder = _responder(settings)
    with pytest.raises(ResponderError):
        responder.correlated_job(SOURCE_MID, "ref", CHANNEL)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL, SOURCE_MID, entry["seq"])
        store.mark_submitted(
            CHANNEL, SOURCE_MID, job_ref="ref", native_sid=SID, pre_turn_index=None
        )
    meta = responder.correlated_job(SOURCE_MID, "ref", CHANNEL)
    assert meta["stage"] == "submitted"
    with pytest.raises(ResponderError):
        responder.correlated_job(SOURCE_MID, "stale", CHANNEL)


async def test_close_fails_midflight_keeps_queued(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    mid2 = SOURCE_MID + 1
    entry2 = _append(settings, message_id=mid2)
    # Construct the responder FIRST (constructor sweeps submitted -> failed),
    # then drive the job to submitted, then close.
    responder = _responder(settings)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL, SOURCE_MID, entry["seq"])
        store.enqueue_job(CHANNEL, mid2, entry2["seq"])
        store.mark_submitted(
            CHANNEL, SOURCE_MID, job_ref="ref", native_sid=SID, pre_turn_index=None
        )
    await responder.close()
    assert _job(settings)["stage"] == "failed"
    assert _job(settings)["fail_kind"] == "shutdown"
    assert _job(settings, mid2)["stage"] == "queued"


async def test_target_dm_adoption_single_transaction(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    client.dm_sid = SID2
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("x",)))
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert responder.native_sid == SID2
    with Store.open(settings.db_path) as store:
        assert store.get_native_sid() == SID2
        days = {r[0] for r in store._conn.execute("SELECT day FROM sessions;").fetchall()}
        assert DAY in days


async def test_target_dm_blocked_by_midflight_reconciles(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    seen, _ = _wire_turn(client, settings, source_mid=SOURCE_MID, agent=None, submit_running=True)
    responder = _responder(settings, client=client)
    first = await responder.tick()
    assert first["status"] == "pending"
    assert first["reason"] == "turn_running"
    assert _job(settings)["stage"] == "submitted"
    # DM changes while a job is mid-flight: adoption defers with a
    # reconcile-only pass on the old SID (no submit, no re-adopt).
    client.dm_sid = SID2
    second = await responder.tick()
    assert second["reason"] == "target_dm_blocked"
    assert responder.native_sid == SID  # still on the old SID
