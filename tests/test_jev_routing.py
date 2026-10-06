"""Jev routing integration tests: REAL delivery + REAL ledger, fakes elsewhere.

Every test runs the real ``delivery`` journal and the real
``ParticipationLedger`` (file-backed, tmp paths) with a fake Jev adapter
(scripted ``JevDecision`` objects, never the network), a fake canonical
collector (scripted snapshots), a fake native ``ThycaClient``, and a fake
bot/source surface. No live Discord/Thyca/Jev, no real .env (all settings
use ``_env_file=None``), no paid calls.

Coverage: respond completion with the SAME canonical snapshot feeding Jev
and the prompt; observe as a durable zero-POST terminal with safe ACK;
need_context via agent send and via explicit no-op with exact trace proof
(including two consistent declarations, mixed-reason and missing-result
holds); zero-receipts-never-observe; restart/cache/409 no-rebill;
cancellation/unknown/budget/unknown-blocked holds; pre/post-paid deletion;
edit/context-change holds; FIFO + ACK; nonce/latch races; classifier
privacy (sender/addressee never identity); full-context 4000 budget with
no truncation; pre-classify UID binding for ALL routes.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest

import delivery
from config import Settings
from context import ContextUnavailable, context_fingerprint
from conversation import project_conversation_state
from delivery import (
    enqueue_job,
    get_job,
    initialize_delivery_state,
    mark_turn_submitting,
    mark_uncertain,
    record_send_intent,
    record_send_receipt,
)
from jev import JEV_MODEL, JevDecision, JevError, build_questions
from outbox import append_entry
from participation import ParticipationError, ParticipationLedger
from responder import Responder, ResponderError
from source_messages import SourceDeleted
from thyca import ThycaBusyError, ThycaProtocolError

SID = "2026-01-02T03-04-05_ab12"
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
        "session_id": "discord-main",
        "audit_path": tmp_path / "audit.jsonl",
        "outbox_path": tmp_path / "outbox.jsonl",
        "outbox_state_path": tmp_path / "outbox_state.json",
        "thyca_session_mapping_path": tmp_path / "mapping.json",
        "delivery_state_path": tmp_path / "delivery.json",
        "_env_file": None,
    }
    kwargs.update(overrides)
    settings = Settings(**kwargs)
    assert settings.jev_state_path.parent == tmp_path  # derived beside tmp delivery
    return settings


def _write_mapping(settings):
    settings.thyca_session_mapping_path.write_text(
        json.dumps(
            {
                "native_id": SID,
                "title": settings.session_id,
                "audit_label": settings.session_id,
                "created_at": "2026-01-02T03:04:05+00:00",
                "path": f"/tmp/fake-sessions/{SID}.jsonl",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(settings.thyca_session_mapping_path, 0o600)


def _init_delivery(settings):
    return initialize_delivery_state(
        settings.delivery_state_path,
        native_session_id=SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )


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
    return append_entry(
        settings.outbox_path, settings.outbox_state_path, **_entry_fields(**overrides)
    )


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


# --- fake native client --------------------------------------------------------


class _FakeClient:
    def __init__(self):
        self.snapshot_calls: list = []
        self.submit_calls: list = []
        self.trace_calls: list = []
        self.snapshot_fn = None
        self.submit_fn = None
        self.trace_fn = None

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

    async def get_trace_evidence(
        self, sid, idx, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        self.trace_calls.append((sid, idx, job_ref, source_message_id, channel_id, guild_id))
        if self.trace_fn is not None:
            return await self.trace_fn(sid, idx, job_ref, source_message_id, channel_id, guild_id)
        raise ThycaProtocolError(reason="transport")


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


# --- realistic trace evidence (mirrors the real client output shape) -----------


def _send_item(tool_id, content, sent_mid):
    return {
        "tool_call_id": tool_id,
        "channel_match": True,
        "source_mid_match": True,
        "job_ref_match": True,
        "correlated": True,
        "result_present": True,
        "result_message_id": sent_mid,
        "result_channel_match": True,
        "jump_present": True,
        "jump_ids_match": True,
        "result_ok": True,
        "content_len": len(content),
        "content_sha256": _sha(content),
    }


def _obs_item(tool_id, reason="no_reply_needed", *, present=True, ok=True):
    return {
        "tool_call_id": tool_id,
        "channel_match": True,
        "source_mid_match": True,
        "job_ref_match": True,
        "reason_valid": True,
        "reason": reason,
        "correlated": True,
        "result_present": present,
        "result_ids_match": True,
        "result_reason_match": True,
        "result_status_declared": True,
        "result_ok": ok,
    }


def _trace_evidence(sid, idx, job_ref, source_mid, input_sha, input_len, sends=(), observations=()):
    sends = list(sends)
    observations = list(observations)
    return {
        "native_sid": sid,
        "requested_turn_index": idx,
        "returned_turn_index": idx,
        "trace_session_match": True,
        "turn_index_match": True,
        "status": "completed",
        "status_len": len("completed"),
        "error_present": False,
        "error_kind": "none",
        "request_job_ref_match": True,
        "request_job_ref_len": len(job_ref),
        "request_job_ref_sha256": _sha(job_ref),
        "request_user_count": 1,
        "request_input_sha256": input_sha,
        "request_input_len": input_len,
        "send_call_count": len(sends),
        "correlated_send_count": len(sends),
        "legacy_uncorrelated_count": 0,
        "other_tool_call_count": 0,
        "other_tool_name_lens": [],
        "sends": sends,
        "all_sends_confirmed": bool(sends) and all(s["result_ok"] for s in sends),
        "observation_calls": observations,
        "observation_call_count": len(observations),
        "all_observations_confirmed": bool(observations)
        and all(o["result_ok"] for o in observations),
        "proof": "trace_detail",
    }


def _wire_turn(client, settings, *, source_mid, own_index=0, pre_max=None, agent=None):
    """Drive FakeClient from the submitted text; run the agent sim on POST.

    ``agent`` is ``None`` (no agent action), ``("send", (chunks...))``, or
    ``("declare", reason)`` / ``("declare2", reason)`` (one/two journaled
    declarations are simulated by recording once; the trace carries one or
    two positive calls). Trace observations/sends mirror the agent action.
    """
    seen: dict = {"texts": [], "turns": [], "declares": []}
    pre_indices = [] if pre_max is None else list(range(pre_max + 1))

    async def snapshot(sid, expected_title):
        done = [t for t in seen["turns"] if t["done"]]
        indices = sorted({*pre_indices, *(t["index"] for t in done)})
        return {
            "native_sid": sid,
            "native_id_match": True,
            "title_match": True if expected_title else None,
            "running": False,
            "turn_count": len(indices),
            "turn_indices": indices,
            "pre_max_turn_index": (max(indices) if indices else None),
        }

    async def submit(sid, text, model=None, effort=None):
        job = get_job(settings.delivery_state_path, CHANNEL, source_mid)
        ref = job["job_ref"]
        seen["texts"].append(text)
        turn = {"index": own_index + len(seen["texts"]) - 1, "text": text, "done": False}
        seen["turns"].append(turn)
        sends: list = []
        observations: list = []
        if agent is not None and agent[0] == "send":
            for i, chunk in enumerate(agent[1]):
                send_ref = f"send-{len(seen['texts'])}-{i}"
                record_send_intent(
                    settings.delivery_state_path,
                    CHANNEL,
                    source_mid,
                    send_ref=send_ref,
                    content_len=len(chunk),
                    content_hash=_sha(chunk),
                )
                sent_mid = SENT_BASE + i
                record_send_receipt(
                    settings.delivery_state_path,
                    CHANNEL,
                    source_mid,
                    send_ref=send_ref,
                    sent_message_id=sent_mid,
                    delivered_channel_id=CHANNEL,
                    jump_url=_jump(sent_mid),
                    reply_to_message_id=source_mid,
                )
                sends.append(_send_item(f"call_send_{i}", chunk, sent_mid))
        elif agent is not None and agent[0] in ("declare", "declare2"):
            reason = agent[1]
            delivery.record_no_reply(
                settings.delivery_state_path, CHANNEL, source_mid, job_ref=ref, reason=reason
            )
            seen["declares"].append(reason)
            observations.append(_obs_item("call_obs_0", reason))
            if agent[0] == "declare2":
                observations.append(_obs_item("call_obs_1", reason))
        turn["sends"] = sends
        turn["observations"] = observations
        turn["done"] = True
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": turn["index"],
            "turn_count": len(seen["turns"]),
            "running": False,
            "submitted_text_len": len(text),
            "submitted_text_sha256": _sha(text),
            "stored_text_match": True,
            "stored_text_len": len(text),
        }

    async def trace(sid, idx, job_ref, source_mid_arg, channel_id, guild_id=None):
        rec = next((t for t in seen["turns"] if t["done"] and t["index"] == idx), None)
        if rec is None:
            manual = f"manual-turn-{idx}"
            return _trace_evidence(sid, idx, job_ref, source_mid_arg, _sha(manual), len(manual))
        return _trace_evidence(
            sid,
            idx,
            job_ref,
            source_mid_arg,
            _sha(rec["text"]),
            len(rec["text"]),
            sends=rec.get("sends", ()),
            observations=rec.get("observations", ()),
        )

    client.snapshot_fn = snapshot
    client.submit_fn = submit
    client.trace_fn = trace
    return seen


def _prepare(tmp_path):
    settings = _settings(tmp_path)
    _write_mapping(settings)
    _init_delivery(settings)
    return settings


def _responder(settings, bot=None, client=None, adapter=None, collector=None, ledger=None):
    bot = bot or _FakeBot()
    client = client or _FakeClient()
    adapter = adapter if adapter is not None else _FakeAdapter("respond")
    collector = collector if collector is not None else _FakeCollector()
    if ledger is None:
        ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
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


# --- respond ------------------------------------------------------------------


async def test_respond_same_snapshot_completes_and_acks(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    history = [
        (701, OWNER, "Olivia", "kickoff at noon", None),
        (702, BOT, "HelperBot", "noted", 701),
    ]
    collector = _FakeCollector(history=history)
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, collector=collector, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("On it!",)))
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "completed"
    assert result["acked"] == 1
    assert len(adapter.calls) == 1  # one paid call
    assert len(client.submit_calls) == 1
    # SAME canonical snapshot feeds Jev and the prompt.
    fed = project_conversation_state(adapter.calls[0])
    rendered = _prompt_json(client.submit_calls[0][1])
    assert rendered == fed
    assert [m["id"] for m in rendered["messages"]] == ["701", "702", str(SOURCE_MID)]
    prompt = client.submit_calls[0][1]
    assert "ROUTED respond" in prompt
    assert "send_message" in prompt
    assert len(prompt) <= 4000
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "completed"
    assert job["jev_decision"]["participation"] == "respond"
    assert job["jev_decision"]["context_sha256"] == context_fingerprint(fed)
    record = ledger.get(CHANNEL, SOURCE_MID)
    assert record["status"] == "decided" and record["participation"] == "respond"
    assert re.fullmatch(r"[0-9a-f]{32}", record["attempt_id"])  # 32-hex, not dashed


async def test_respond_prompt_carries_stop_guards(tmp_path):
    """The routed respond prompt includes exact stop/no-plain/threaded guards."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter(["respond", "respond"])
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    mid2 = SOURCE_MID + 1
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("R1",)))
    _append(settings)
    _append(settings, message_id=mid2, content="second question")
    result1 = await inst.tick()
    assert result1["status"] == "completed"
    # Rewire the agent sim for the second source, then tick again.
    _wire_turn(client, settings, source_mid=mid2, agent=("send", ("R2",)))
    result2 = await inst.tick()
    assert result2["status"] == "completed"
    assert len(client.submit_calls) == 2
    for text in (client.submit_calls[0][1], client.submit_calls[1][1]):
        assert "SourceDeleted" in text
        assert "no plain fallback" in text
        assert "threaded" in text
        assert "ROUTED respond" in text


async def test_full_context_preserved_no_truncation(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    history = [
        (600 + i, OTHER if i % 2 else OWNER, "R" if i % 2 else "O", f"history line {i} — ok", None)
        for i in range(6)
    ]
    collector = _FakeCollector(history=history)
    inst = _responder(settings, client=client, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("ack",)))
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "completed"
    prompt = client.submit_calls[0][1]
    body = _prompt_json(prompt)
    assert len(body["messages"]) == 7
    for i in range(6):
        assert f"history line {i} — ok" in prompt  # every line exact, none dropped
    assert len(prompt) <= 4000


async def test_jev_answers_never_identity(tmp_path):
    """A wrong classifier addressee never overrides SDK identity."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond", addressee=str(OWNER))  # Jev misreads the target
    inst = _responder(settings, client=client, adapter=adapter)
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("hi",)))
    _append(settings)  # actual sender is OTHER
    result = await inst.tick()
    assert result["status"] == "completed"
    header = client.submit_calls[0][1].splitlines()[0]
    assert f"sender_uid={OTHER}" in header
    assert "sender_kind=other" in header
    assert "whoami(sender_uid)" in client.submit_calls[0][1]


# --- observe ------------------------------------------------------------------


async def test_observe_zero_post_terminal_ack(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "observed"
    assert result["acked"] == 1
    assert len(adapter.calls) == 1
    assert client.submit_calls == []  # zero POST
    assert client.trace_calls == []  # zero trace reads
    assert len(client.snapshot_calls) == 1  # gate snapshot only, no POST snapshot
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "observed"
    assert job["jev_decision"]["participation"] == "observe"
    assert job["observation"]["origin"] == "jev"
    assert job["send_intents"] == [] and job["send_receipts"] == [] and job["job_ref"] is None
    record = ledger.get(CHANNEL, SOURCE_MID)
    assert record["participation"] == "observe" and record["status"] == "decided"


async def test_zero_receipts_never_observe(tmp_path):
    """An agent no-op WITHOUT a declaration holds uncertain; receipts-absence proves nothing."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    inst = _responder(settings, client=client, adapter=_FakeAdapter("respond"))
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=None)  # agent sends nothing
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "uncertain"
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "uncertain"
    assert result["acked"] == 0


# --- cache / restart / rebill ---------------------------------------------------


async def test_409_reuses_cached_no_rebill(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    inst = _responder(settings, client=client, adapter=adapter)
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("done",)))

    orig_submit = client.submit_fn
    attempts = {"n": 0}

    async def flaky_submit(sid, text, model=None, effort=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            # Definitive 409 BEFORE any agent action: zero evidence.
            raise ThycaBusyError()
        return await orig_submit(sid, text, model, effort)

    client.submit_fn = flaky_submit
    _append(settings)
    result1 = await inst.tick()
    assert result1["status"] == "pending" and result1["reason"] == "busy_requeued"
    assert len(adapter.calls) == 1
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "queued" and job["job_ref"] is None
    assert job["jev_decision"]["participation"] == "respond"  # binding survives 409
    attempt1 = job["jev_decision"]["attempt_id"]
    result2 = await inst.tick()
    assert result2["status"] == "completed"
    assert len(adapter.calls) == 1  # cached: no rebill, no provider recall
    job2 = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job2["jev_decision"]["attempt_id"] == attempt1  # idempotent rebind
    [job["job_ref"], job2["job_ref"]]
    assert len(client.submit_calls) == 2


async def test_restart_reuses_cache_no_rebill(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("r",)))
    snaps = {"n": 0}
    orig_snapshot = client.snapshot_fn

    async def running_once(sid, expected_title):
        snaps["n"] += 1
        snap = await orig_snapshot(sid, expected_title)
        if snaps["n"] == 2:  # POST-time snapshot sees native busy (bound already)
            snap["running"] = True
        return snap

    client.snapshot_fn = running_once
    _append(settings)
    # Observe path never takes the POST snapshot; use respond to reach it.
    adapter.script = ["respond"]
    result1 = await inst.tick()
    assert result1["status"] == "pending" and result1["reason"] == "native_running"
    assert len(adapter.calls) == 1
    # Simulate a process restart: brand-new Responder + ledger, same files.
    ledger2 = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    assert ledger2.get(CHANNEL, SOURCE_MID)["status"] == "decided"  # durable
    inst2 = _responder(settings, client=client, adapter=adapter, ledger=ledger2)
    result2 = await inst2.tick()
    assert result2["status"] == "completed"
    assert len(adapter.calls) == 1  # cache hit across restart: no rebill


async def test_cancellation_unknown_blocks_retry(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    adapter.block_event = asyncio.Event()
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    task = asyncio.create_task(inst.tick())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(adapter.calls) == 1
    record = ledger.get(CHANNEL, SOURCE_MID)
    assert record["status"] == "failed_unknown"  # persistent started/unknown
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "queued"  # responder propagates; job stays queued
    adapter.block_event = None
    result = await inst.tick()
    assert result["status"] == "uncertain"
    assert result["reason"] == "classification_unknown"
    assert len(adapter.calls) == 1  # blocked retry: no provider recall, no rebill


async def test_no_daily_stop_both_classify_same_day(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    mid2 = SOURCE_MID + 1
    ledger = ParticipationLedger(
        settings.jev_state_path,
        native_sid=SID,
        day_provider=lambda: "2026-10-05",
    )
    adapter = _FakeAdapter(["observe", "observe"])
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    _append(settings, message_id=mid2, content="second")
    result1 = await inst.tick()
    assert result1["status"] == "observed"
    assert len(adapter.calls) == 1
    # Same UTC day, cost already accumulated: the second job still classifies.
    _wire_turn(client, settings, source_mid=mid2)
    result2 = await inst.tick()
    assert result2["status"] == "observed"
    assert len(adapter.calls) == 2
    assert get_job(settings.delivery_state_path, CHANNEL, mid2)["stage"] == "observed"


async def test_unknown_blocked_preserves_innocent_job(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    # Pre-seed a failed_unknown record for ANOTHER key via the real ledger.
    other_state = {
        "assistant_uid": str(BOT),
        "owner_uid": str(OWNER),
        "participants": [
            {"uid": str(OWNER), "display_name": "O"},
            {"uid": str(BOT), "display_name": "B"},
            {"uid": str(THIRD), "display_name": "T"},
        ],
        "messages": [{"id": "900", "sender_uid": str(THIRD), "text": "seed"}],
        "new_message_id": "900",
    }
    failing = _FakeAdapter("respond")
    failing.raise_exc = JevError("network", detail="boom", unknown_billing=True)
    with pytest.raises(Exception, match="participation pending"):
        await ledger.classify(
            failing, other_state, channel_id=CHANNEL, message_id=900, author_uid=THIRD
        )
    assert ledger.get(CHANNEL, 900)["status"] == "failed_unknown"
    # The innocent job is held pending (queued, unmarked), never terminalized.
    adapter = _FakeAdapter("observe")
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "pending"
    assert result["reason"] == "unknown_blocked"
    assert len(adapter.calls) == 0
    assert get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)["stage"] == "queued"


# --- guards: deletion, edits, races, FIFO, binding -------------------------------


async def test_uncertain_legacy_never_classifies(tmp_path):
    """A seq-66-like uncertain job (1 intent, 0 receipts) never reclassifies."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    inst = _responder(settings, client=client, adapter=adapter)
    _append(settings)
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=SOURCE_MID,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL,
        SOURCE_MID,
        job_ref="job-66",
        pre_turn_index=0,
        submitted_text_sha256=_sha("old"),
        submitted_text_len=3,
    )
    record_send_intent(
        settings.delivery_state_path,
        CHANNEL,
        SOURCE_MID,
        send_ref="s66",
        content_len=2,
        content_hash=_sha("hi"),
    )
    mark_uncertain(settings.delivery_state_path, CHANNEL, SOURCE_MID, kind="timeout")
    result = await inst.tick()
    assert len(adapter.calls) == 0  # never classifies
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "uncertain"  # never skips, never force-completes
    assert job["error"]["kind"] == "timeout"  # original error preserved
    assert result["acked"] == 0  # never ACKs past it


async def test_source_deleted_before_paid_skips_zero_provider(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)

    async def gone(cid, mid):
        raise SourceDeleted("gone")

    inst = Responder(
        settings,
        _FakeBot(),
        client=client,
        source_checker=gone,
        jev_adapter=adapter,
        context_collector=_FakeCollector(),
        participation_ledger=ledger,
    )
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "skipped" and result["reason"] == "source_deleted"
    assert len(adapter.calls) == 0
    assert ledger.get(CHANNEL, SOURCE_MID) is None  # no ledger record before paid
    assert client.submit_calls == []


async def test_source_deleted_during_paid_skips_sunk_no_post(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    calls = {"n": 0}

    async def gone_second(cid, mid):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise SourceDeleted("gone after paid")
        return None

    inst = Responder(
        settings,
        _FakeBot(),
        client=client,
        source_checker=gone_second,
        jev_adapter=adapter,
        context_collector=_FakeCollector(),
        participation_ledger=ledger,
    )
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "skipped" and result["reason"] == "source_deleted"
    assert len(adapter.calls) == 1  # paid spend is sunk
    assert ledger.get(CHANNEL, SOURCE_MID)["status"] == "decided"
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job.get("jev_decision") is None  # deleted before bind: no bind
    assert client.submit_calls == []  # zero native work


async def test_collector_deletion_during_collect_skips(tmp_path):
    """The source can become deleted DURING collection: exact 404 skips, never generic hold."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    collector = _FakeCollector()
    collector.script = ["deleted"]
    inst = _responder(settings, client=client, adapter=adapter, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "skipped" and result["reason"] == "source_deleted"
    assert len(adapter.calls) == 0


async def test_collector_unavailable_holds_zero_paid(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    collector = _FakeCollector()
    collector.script = ["unavailable"]
    inst = _responder(settings, client=client, adapter=adapter, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "pending" and result["reason"] == "context_unavailable"
    assert len(adapter.calls) == 0
    assert get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)["stage"] == "queued"


async def test_edit_fp_mismatch_holds_no_bind_no_post(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    collector = _FakeCollector()
    collector.script = [None, ("edit", "silently edited text")]
    inst = _responder(settings, client=client, adapter=adapter, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "pending" and result["reason"] == "context_mismatch"
    assert len(adapter.calls) == 1
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job.get("jev_decision") is None  # frozen frame never bound under an edit
    assert client.submit_calls == []


@pytest.mark.parametrize("action", ["sender", "owner", "assistant"])
async def test_wrong_uid_snapshot_rejected_all_routes(tmp_path, action):
    """Pre-classify binding holds for owner/assistant/sender mismatches (all routes)."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")  # observe never renders: binding still enforced
    collector = _FakeCollector()
    bad = THIRD if action == "sender" else (BOT if action == "owner" else OWNER)
    collector.script = [(action, bad)]
    inst = _responder(settings, client=client, adapter=adapter, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "pending" and result["reason"] == "context_mismatch"
    assert len(adapter.calls) == 0  # bad collector cannot trigger paid observe


async def test_snapshot_oversize_holds_zero_paid_no_truncate(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    history = [(600 + i, OTHER, "Ramon", "w" * 150, None) for i in range(19)]
    collector = _FakeCollector(history=history)
    inst = _responder(settings, client=client, adapter=adapter, collector=collector)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "uncertain" and result["reason"] == "snapshot_oversize"
    assert len(adapter.calls) == 0  # fit gate uses the actual renderer pre-paid
    assert client.submit_calls == []
    assert get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)["stage"] == "uncertain"


async def test_fifo_observe_then_respond_ack_prefix(tmp_path):
    settings = _prepare(tmp_path)
    client = _FakeClient()
    mid2 = SOURCE_MID + 1
    adapter = _FakeAdapter(["observe", "respond"])
    inst = _responder(settings, client=client, adapter=adapter)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    _append(settings, message_id=mid2, content="second")
    result1 = await inst.tick()
    assert result1["status"] == "observed"
    assert result1["acked"] == 1
    assert client.submit_calls == []  # observe first: no POST before it
    _wire_turn(client, settings, source_mid=mid2, agent=("send", ("r2",)))
    result2 = await inst.tick()
    assert result2["status"] == "completed"
    assert result2["acked"] == 2
    assert len(client.submit_calls) == 1
    assert len(adapter.calls) == 2


async def test_midflight_gate_race_respected_no_bind_no_post(tmp_path):
    """A gate terminal landing during classification wins: no bind, no POST."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")

    def gate_race():
        mark_uncertain(settings.delivery_state_path, CHANNEL, SOURCE_MID, kind="timeout")

    adapter.hook = gate_race
    inst = _responder(settings, client=client, adapter=adapter)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    result = await inst.tick()
    assert result["status"] == "uncertain"
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job.get("jev_decision") is None
    assert client.submit_calls == []


async def test_ledger_attempt_id_is_32hex_and_binds(tmp_path):
    """Regression: real ledger attempts bind to real delivery (32-hex, resolved conflict)."""
    settings = _prepare(tmp_path)
    adapter = _FakeAdapter("observe")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    collector = _FakeCollector()
    entry = _entry_fields()
    snapshot = await collector(entry)
    record = await ledger.classify(
        adapter, snapshot, channel_id=CHANNEL, message_id=SOURCE_MID, author_uid=OTHER
    )
    assert re.fullmatch(r"[0-9a-f]{32}", record["attempt_id"])
    assert record["model"] == "jev-1.13.0"
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=SOURCE_MID,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    bound = delivery.bind_jev_decision(
        settings.delivery_state_path,
        CHANNEL,
        SOURCE_MID,
        context_sha256=record["fingerprint"],
        attempt_id=record["attempt_id"],
        model=record["model"],
        participation=record["participation"],
    )
    assert bound["jev_decision"]["attempt_id"] == record["attempt_id"]


# --- T3 loss guard --------------------------------------------------------------


def test_loss_guard_proven_journal_missing_file_fails_before_create(tmp_path):
    """A proven journal with a deleted ledger file FAILS construction (no reset/rebill)."""
    import os

    settings = _prepare(tmp_path)
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=SOURCE_MID,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    delivery.bind_jev_decision(
        settings.delivery_state_path,
        CHANNEL,
        SOURCE_MID,
        context_sha256="ab" * 32,
        attempt_id="cd" * 16,
        model="jev-1.13.0",
        participation="respond",
    )
    assert not os.path.lexists(settings.jev_state_path)  # never created
    with pytest.raises(ResponderError, match="refusing auto-create"):
        Responder(
            settings,
            _FakeBot(),
            client=_FakeClient(),
            jev_adapter=_FakeAdapter("respond"),
            context_collector=_FakeCollector(),
            participation_ledger=None,  # real path: guard runs before auto-create
        )
    assert not os.path.lexists(settings.jev_state_path)  # still missing: no reset file


def test_fresh_install_inits_ledger_zero_calls(tmp_path):
    """Fresh installs exclusively init the ledger with zero provider/SDK/POST work."""
    import os
    import stat

    settings = _prepare(tmp_path)
    adapter = _FakeAdapter("respond")
    inst = Responder(
        settings,
        _FakeBot(),
        client=_FakeClient(),
        jev_adapter=adapter,
        context_collector=_FakeCollector(),
        participation_ledger=None,
    )
    assert os.path.lexists(settings.jev_state_path)
    assert stat.S_IMODE(os.stat(settings.jev_state_path).st_mode) == 0o600
    assert len(adapter.calls) == 0
    # The initialized file reopens as the same ledger (durable, exclusive).
    reopened = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    assert reopened.get(CHANNEL, SOURCE_MID) is None
    assert inst._participation_ledger is not None


# --- T4 ledger activation binding (constructor marker + runtime guard) ---------


def test_marker_fresh_bind_durable_zero_calls(tmp_path):
    """Fresh installs bind the ledger marker with zero provider/SDK/POST work."""
    import os
    import stat

    settings = _prepare(tmp_path)
    # Seed an unknown top-level field to prove the bind preserves it.
    raw = json.loads(settings.delivery_state_path.read_text(encoding="utf-8"))
    raw["operator_note"] = "keep-me"
    settings.delivery_state_path.write_text(json.dumps(raw), encoding="utf-8")
    adapter = _FakeAdapter("respond")
    client = _FakeClient()
    inst = Responder(
        settings,
        _FakeBot(),
        client=client,
        jev_adapter=adapter,
        context_collector=_FakeCollector(),
        participation_ledger=None,
    )
    ledger_id = inst._participation_ledger.ledger_id
    assert re.fullmatch(r"[0-9a-f]{32}", ledger_id)
    assert os.path.lexists(settings.jev_state_path)
    assert stat.S_IMODE(os.stat(settings.jev_state_path).st_mode) == 0o600
    state = delivery.load_delivery_state(settings.delivery_state_path)
    assert state["jev_ledger_id"] == ledger_id
    assert state["native_session_id"] == SID
    assert state["baseline_acked"] == 0 and state["jobs"] == {}
    assert state["operator_note"] == "keep-me"
    assert len(adapter.calls) == 0
    assert client.snapshot_calls == [] and client.submit_calls == []
    assert client.trace_calls == []
    # The committed ledger reopens as the same identity (durable order).
    assert ParticipationLedger(settings.jev_state_path, native_sid=SID).ledger_id == ledger_id


def test_marker_bind_order_ledger_committed_first(tmp_path, monkeypatch):
    """The marker bind observes an already-committed readable ledger file."""
    settings = _prepare(tmp_path)
    seen: dict = {}
    orig_bind = delivery.bind_jev_ledger

    def spy_bind(path, *, ledger_id):
        seen["ledger_exists"] = os.path.lexists(settings.jev_state_path)
        seen["file_id"] = ParticipationLedger(settings.jev_state_path, native_sid=SID).ledger_id
        seen["arg"] = ledger_id
        return orig_bind(path, ledger_id=ledger_id)

    monkeypatch.setattr(delivery, "bind_jev_ledger", spy_bind)
    Responder(
        settings,
        _FakeBot(),
        client=_FakeClient(),
        jev_adapter=_FakeAdapter("respond"),
        context_collector=_FakeCollector(),
        participation_ledger=None,
    )
    assert seen["ledger_exists"] is True
    assert seen["file_id"] == seen["arg"]
    assert (
        delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == seen["arg"]
    )


async def test_marker_repeat_restart_no_rewrite_retains_cost(tmp_path):
    """Exact-repeat restarts rebind idempotently: no rewrite, spend retained."""
    settings = _prepare(tmp_path)
    collector = _FakeCollector()
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    # Seed one decided record (known cost) before any Responder exists.
    snapshot = await collector(_entry_fields())
    record = await ledger.classify(
        _FakeAdapter("observe"),
        snapshot,
        channel_id=CHANNEL,
        message_id=SOURCE_MID,
        author_uid=OTHER,
    )
    assert record["status"] == "decided"
    before_summary = ledger.summary()
    _responder(settings, client=_FakeClient(), adapter=_FakeAdapter("observe"), ledger=ledger)
    assert (
        delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"]
        == ledger.ledger_id
    )
    delivery_bytes = settings.delivery_state_path.read_bytes()
    ledger_bytes = settings.jev_state_path.read_bytes()
    # Restart: brand-new Responder + reopened ledger, same files.
    ledger2 = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    _responder(settings, client=_FakeClient(), adapter=_FakeAdapter("observe"), ledger=ledger2)
    assert settings.delivery_state_path.read_bytes() == delivery_bytes  # no rewrite
    assert settings.jev_state_path.read_bytes() == ledger_bytes
    assert ledger2.summary() == before_summary  # spend retained, never reset
    assert ledger2.get(CHANNEL, SOURCE_MID)["attempt_id"] == record["attempt_id"]


def test_loss_guard_marked_missing_fails_before_create(tmp_path):
    """A marked journal with a missing ledger file FAILS before auto-create."""
    settings = _prepare(tmp_path)
    marker = "ab" * 16
    delivery.bind_jev_ledger(settings.delivery_state_path, ledger_id=marker)
    assert not os.path.lexists(settings.jev_state_path)  # never created
    with pytest.raises(ResponderError, match="refusing auto-create"):
        Responder(
            settings,
            _FakeBot(),
            client=_FakeClient(),
            jev_adapter=_FakeAdapter("respond"),
            context_collector=_FakeCollector(),
            participation_ledger=None,  # real path: guard runs before auto-create
        )
    assert not os.path.lexists(settings.jev_state_path)  # still missing: no reset file
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == marker


async def test_same_sid_different_id_fails_no_overwrite_no_rebill(tmp_path):
    """Same SID + different file ID fails with both files preserved, no rebill."""
    settings = _prepare(tmp_path)
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    snapshot = await _FakeCollector()(_entry_fields())
    record = await ledger.classify(
        _FakeAdapter("observe"),
        snapshot,
        channel_id=CHANNEL,
        message_id=SOURCE_MID,
        author_uid=OTHER,
    )
    assert record["status"] == "decided"
    other_id = "cd" * 16
    assert other_id != ledger.ledger_id
    delivery.bind_jev_ledger(settings.delivery_state_path, ledger_id=other_id)
    delivery_bytes = settings.delivery_state_path.read_bytes()
    ledger_bytes = settings.jev_state_path.read_bytes()
    adapter = _FakeAdapter("respond")
    with pytest.raises(delivery.DeliveryError, match="jev ledger conflict"):
        Responder(
            settings,
            _FakeBot(),
            client=_FakeClient(),
            jev_adapter=adapter,
            context_collector=_FakeCollector(),
            participation_ledger=None,
        )
    assert settings.delivery_state_path.read_bytes() == delivery_bytes  # no overwrite
    assert settings.jev_state_path.read_bytes() == ledger_bytes  # committed, no rollback
    assert len(adapter.calls) == 0
    assert ledger.get(CHANNEL, SOURCE_MID)["attempt_id"] == record["attempt_id"]  # no rebill
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == other_id


def test_injected_real_wrong_id_fails(tmp_path):
    """An injected REAL ledger with the wrong ID fails; marker + file preserved."""
    settings = _prepare(tmp_path)
    dir_a = tmp_path / "a"
    dir_a.mkdir()
    dir_b = tmp_path / "b"
    dir_b.mkdir()
    ledger_a = ParticipationLedger(dir_a / "jev.json", native_sid=SID)
    delivery.bind_jev_ledger(settings.delivery_state_path, ledger_id=ledger_a.ledger_id)
    ledger_b = ParticipationLedger(dir_b / "jev.json", native_sid=SID)
    assert ledger_b.ledger_id != ledger_a.ledger_id
    before_delivery = settings.delivery_state_path.read_bytes()
    before_b = (dir_b / "jev.json").read_bytes()
    with pytest.raises(delivery.DeliveryError, match="jev ledger conflict"):
        _responder(settings, ledger=ledger_b)
    assert settings.delivery_state_path.read_bytes() == before_delivery
    assert (dir_b / "jev.json").read_bytes() == before_b
    assert (
        delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"]
        == ledger_a.ledger_id
    )


def test_injected_ledger_id_contract_strict(tmp_path):
    """Injected ledgers must expose strict 32 lower hex; nothing is bound else."""

    class _NoId:
        async def classify(self, *args, **kwargs):
            raise AssertionError("never classify")

    class _BadId:
        def __init__(self, value):
            self._value = value

        @property
        def ledger_id(self):
            return self._value

        async def classify(self, *args, **kwargs):
            raise AssertionError("never classify")

    settings = _prepare(tmp_path)
    with pytest.raises(ResponderError, match="must provide ledger_id"):
        _responder(settings, ledger=_NoId())
    for bad in ("AB" * 16, "ab" * 15 + "AB", "zz" * 16, "abc", "", None, 1234):
        with pytest.raises(ResponderError, match="jev ledger id invalid"):
            _responder(settings, ledger=_BadId(bad))
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] is None


async def test_runtime_ledger_loss_raises_zero_provider(tmp_path):
    """A ledger file lost mid-runtime raises naturally with zero new paid work."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    marker = delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"]
    assert marker == ledger.ledger_id
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("hi",)))
    _append(settings)
    settings.jev_state_path.unlink()  # file lost while the process is alive
    with pytest.raises(ParticipationError, match="missing"):
        await inst.tick()
    assert len(adapter.calls) == 0
    assert client.submit_calls == []
    assert not settings.jev_state_path.exists()  # never auto-recreated
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "queued" and job.get("jev_decision") is None
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == marker


async def test_runtime_ledger_replaced_id_halts_zero_provider(tmp_path):
    """A replaced ledger file (same SID, new ID) halts before classification."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    original_id = ledger.ledger_id
    _wire_turn(client, settings, source_mid=SOURCE_MID, agent=("send", ("hi",)))
    _append(settings)
    other = tmp_path / "replacement.json"
    ParticipationLedger(other, native_sid=SID)
    replacement_id = ParticipationLedger(other, native_sid=SID).ledger_id
    assert replacement_id != original_id
    settings.jev_state_path.write_bytes(other.read_bytes())
    assert ledger.ledger_id == replacement_id  # property reloads: sees the new file ID
    with pytest.raises(ResponderError, match="binding mismatch"):
        await inst.tick()
    assert len(adapter.calls) == 0
    assert client.submit_calls == []
    assert ledger.get(CHANNEL, SOURCE_MID) is None  # guard ran before started persist
    raw = json.loads(settings.jev_state_path.read_text(encoding="utf-8"))
    assert raw["attempts"] == {} and raw["ledger_id"] == replacement_id
    assert (
        delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == original_id
    )


async def test_runtime_delivery_marker_replaced_halts(tmp_path):
    """A replaced journal marker (same SID, new ID) halts before classification."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    raw = json.loads(settings.delivery_state_path.read_text(encoding="utf-8"))
    raw["jev_ledger_id"] = "ff" * 16
    settings.delivery_state_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ResponderError, match="binding mismatch"):
        await inst.tick()
    assert len(adapter.calls) == 0
    assert ledger.get(CHANNEL, SOURCE_MID) is None
    job = get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)
    assert job["stage"] == "queued" and job.get("jev_decision") is None


async def test_runtime_delivery_unmarked_halts_no_rebind(tmp_path):
    """A marker lost mid-runtime fails closed; the runtime path never rebinds."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    adapter = _FakeAdapter("observe")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    raw = json.loads(settings.delivery_state_path.read_text(encoding="utf-8"))
    raw["jev_ledger_id"] = None
    settings.delivery_state_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ResponderError, match="binding missing"):
        await inst.tick()
    assert len(adapter.calls) == 0
    assert ledger.get(CHANNEL, SOURCE_MID) is None
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] is None


async def test_runtime_orphan_started_preserved_no_reset(tmp_path):
    """An orphan unknown halt survives a binding hold; restore keeps blocking."""
    settings = _prepare(tmp_path)
    client = _FakeClient()
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    # Seed an orphan failed_unknown for ANOTHER key (global unknown halt).
    other_state = {
        "assistant_uid": str(BOT),
        "owner_uid": str(OWNER),
        "participants": [
            {"uid": str(OWNER), "display_name": "O"},
            {"uid": str(BOT), "display_name": "B"},
            {"uid": str(THIRD), "display_name": "T"},
        ],
        "messages": [{"id": "900", "sender_uid": str(THIRD), "text": "seed"}],
        "new_message_id": "900",
    }
    failing = _FakeAdapter("respond")
    failing.raise_exc = JevError("network", detail="boom", unknown_billing=True)
    with pytest.raises(Exception, match="participation pending"):
        await ledger.classify(
            failing, other_state, channel_id=CHANNEL, message_id=900, author_uid=THIRD
        )
    assert ledger.get(CHANNEL, 900)["status"] == "failed_unknown"
    adapter = _FakeAdapter("observe")
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings)
    delivery_bytes = settings.delivery_state_path.read_bytes()
    raw = json.loads(delivery_bytes.decode("utf-8"))
    raw["jev_ledger_id"] = "ee" * 16
    settings.delivery_state_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ResponderError, match="binding mismatch"):
        await inst.tick()
    assert len(adapter.calls) == 0
    assert ledger.get(CHANNEL, 900)["status"] == "failed_unknown"  # orphan preserved
    # Restore the journal: the original unknown halt still blocks, no rebill.
    settings.delivery_state_path.write_bytes(delivery_bytes)
    result = await inst.tick()
    assert result["status"] == "pending" and result["reason"] == "unknown_blocked"
    assert len(adapter.calls) == 0
    assert get_job(settings.delivery_state_path, CHANNEL, SOURCE_MID)["stage"] == "queued"


async def test_binding_errors_disclose_nothing(tmp_path):
    """Binding failures carry no source text, IDs, marker values, or keys."""
    settings = _settings(tmp_path, typesafe_api_key="sk-fake-secret-key")
    _write_mapping(settings)
    _init_delivery(settings)
    secret = "SECRET-CONTENT-58291-private"
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    ledger = ParticipationLedger(settings.jev_state_path, native_sid=SID)
    real_id = ledger.ledger_id
    inst = _responder(settings, client=client, adapter=adapter, ledger=ledger)
    _wire_turn(client, settings, source_mid=SOURCE_MID)
    _append(settings, content=secret)
    raw = json.loads(settings.delivery_state_path.read_text(encoding="utf-8"))
    raw["jev_ledger_id"] = "dd" * 16
    settings.delivery_state_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ResponderError, match="binding mismatch") as excinfo:
        await inst.tick()
    text = str(excinfo.value)
    assert secret not in text
    assert real_id not in text and "dd" * 16 not in text
    assert "sk-fake-secret-key" not in text
    assert str(OTHER) not in text and str(SOURCE_MID) not in text
    # The constructor conflict is equally coarse (path + kind only).
    other = tmp_path / "other.json"
    ParticipationLedger(other, native_sid=SID)
    with pytest.raises(delivery.DeliveryError) as excinfo2:
        _responder(settings, ledger=ParticipationLedger(other, native_sid=SID))
    text2 = str(excinfo2.value)
    assert secret not in text2 and "sk-fake-secret-key" not in text2
