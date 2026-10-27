"""Responder core integration: REAL store + REAL ledger, fakes elsewhere.

Every test runs the real SQLite store and the real DB-backed
``ParticipationLedger`` (tmp paths) with a fake Jev adapter (scripted
``JevDecision`` objects, never the network), a fake canonical collector
(scripted snapshots), a fake native ``ThycaClient``, and a fake
bot/source surface. No live Discord/Thyca/Jev, no real .env.

Scope is the responder CORE contract: constructor/sweep, ``guard_ack``,
``correlated_job``, ingest filters, FIFO order, prompt shape, the
snapshot-gated submit path, source deleted/edited convergence, target-DM
adoption, shutdown, busy/cancel/timeout mapping, and privacy. The
respond/observe/need_context end-to-end routes live in
``test_jev_routing.py`` (same kit pattern, no duplication here).

Deleted (reasons): the ``McpSendPath`` intent/receipt simulator and every
intent/receipt assertion, the trace-evidence proof block, the uncertain
stage and all hold/latch/retry suites, the JSON journal/load-validator
and fsync durability suites, nonce races, alias/seq bookkeeping, and
every ``delivery`` import.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest

from config import Settings
from context import ContextUnavailable
from conversation import project_conversation_state
from jev import JEV_MODEL, JevDecision, build_questions
from participation import ParticipationLedger
from responder import Responder, ResponderError
from source_messages import SourceDeleted, SourceUnavailable
from store import Store, StoreError, append_entry, read_entries
from thyca import (
    ThycaBusyError,
    ThycaClientError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
)

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
FAKE_SID = "2026-01-02T03-04-05_ab12"
FAKE_SID2 = "2026-01-03T04-05-06_cd34"
FAKE_DAY = "2026-01-02"
SENT_MID = 9001


def _settings(tmp_path, **overrides):
    kwargs: dict = {
        "discord_token": "fake-token",
        "owner_uid": OWNER_UID,
        "guild_id": GUILD_ID,
        "default_channel_id": CHANNEL_ID,
        "audit_path": tmp_path / "audit.jsonl",
        "db_path": tmp_path / "state.db",
        "_env_file": None,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def _prepare(tmp_path, **overrides):
    settings = _settings(tmp_path, **overrides)
    with Store.open(settings.db_path) as store:
        store.adopt_session(expected_current=None, new_sid=FAKE_SID, day=FAKE_DAY)
    return settings


def _entry_fields(**overrides):
    base = {
        "type": "message",
        "channel_id": CHANNEL_ID,
        "message_id": 701,
        "author_uid": OWNER_UID,
        "author_name": "Owner",
        "actor_kind": "owner",
        "content": "hello thyca",
        "author_is_bot": False,
    }
    base.update(overrides)
    return base


def _append(settings, **overrides):
    return append_entry(settings.db_path, **_entry_fields(**overrides))


def _job(settings, mid=701, channel_id=CHANNEL_ID):
    with Store.open(settings.db_path) as store:
        return store.get_job(channel_id, mid)


def _jobs(settings):
    with Store.open(settings.db_path) as store:
        return store.list_jobs()


# --- fakes ------------------------------------------------------------------


class _FakeMember:
    def __init__(self, uid, is_bot):
        self.id = uid
        self.bot = is_bot


class _FakeGuild:
    def __init__(self, members=None):
        self._members = dict(members or {})

    def get_member(self, uid):
        return self._members.get(uid)

    async def fetch_member(self, uid):
        try:
            return self._members[uid]
        except KeyError:
            raise LookupError(f"member {uid} not found") from None


class _FakeBot:
    """Minimal bot: UID + scriptable member/user bot flags for legacy resolve."""

    def __init__(self, flags=None):
        self.id = BOT_UID
        self.user = SimpleNamespace(id=BOT_UID, bot=True)
        self._flags = dict(flags or {})
        self._guild = _FakeGuild({uid: _FakeMember(uid, flag) for uid, flag in self._flags.items()})

    def get_guild(self, gid):
        assert gid == GUILD_ID
        return self._guild

    def get_user(self, uid):
        if uid in self._flags:
            return _FakeMember(uid, self._flags[uid])
        return None

    async def fetch_user(self, uid):
        if uid in self._flags:
            return _FakeMember(uid, self._flags[uid])
        raise LookupError(f"user {uid} not found")


class _FakeClient:
    def __init__(self, *, pre_max=3, dm_sid=FAKE_SID):
        self.pre_max = pre_max
        self.turn_bump = 0
        self.dm_sid = dm_sid
        self.snapshot_calls: list = []
        self.submit_calls: list = []
        self.snapshot_fn = None
        self.submit_fn = None

    def _snapshot(self, sid):
        # turn_bump simulates native turns completing (reconcile closes
        # against pre_max == job_pre + 1, like the routing _wire_turn).
        eff = (self.pre_max or 0) + self.turn_bump
        indices = list(range(eff + 1)) if self.pre_max is not None else []
        return {
            "native_sid": sid,
            "native_id_match": True,
            "title_match": True,
            "running": False,
            "turn_count": len(indices),
            "turn_indices": indices,
            "pre_max_turn_index": eff if self.pre_max is not None else None,
        }

    async def get_bot_dm(self, bot_id):
        return {
            "native_sid": self.dm_sid,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }

    async def get_session_snapshot(self, sid, expected_title=None):
        self.snapshot_calls.append(sid)
        if self.snapshot_fn is not None:
            return await self.snapshot_fn(sid)
        return self._snapshot(sid)

    async def submit_turn(self, sid, text, model=None, effort=None):
        self.submit_calls.append((sid, text))
        if self.submit_fn is not None:
            return await self.submit_fn(sid, text)
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (self.pre_max or 0) + 1,
            "running": False,
            "stored_text_match": True,
        }


def _decision_for(snapshot, participation):
    questions = build_questions(snapshot)
    answers = {
        "addressee": {"type": "choice", "choice": "channel", "probabilities": {"channel": 1.0}},
        "explicit_assistant_request": {"type": "noul", "noul": 0.1},
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
    def __init__(self, participation="respond"):
        self.script = list(participation) if isinstance(participation, list) else [participation]
        self.calls: list = []

    async def decide(self, state):
        self.calls.append(state)
        part = self.script[min(len(self.calls) - 1, len(self.script) - 1)]
        return _decision_for(state, part)


class _FakeCollector:
    """Canonical snapshots bound to the frozen entry; scriptable per call."""

    def __init__(self, owner_uid=OWNER_UID, bot_uid=BOT_UID):
        self.owner_uid = owner_uid
        self.bot_uid = bot_uid
        self.calls: list = []
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
        sender, owner, assistant = str(author), str(self.owner_uid), str(self.bot_uid)
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
        participants = [
            {"uid": owner, "display_name": "Owner"},
            {"uid": assistant, "display_name": "Bot"},
        ]
        if sender not in (owner, assistant):
            # Spoofed-sender refreshes stay projectable: the binding
            # mismatch (not a projection failure) must hold the tick.
            participants.append({"uid": sender, "display_name": "Rando"})
        return {
            "assistant_uid": assistant,
            "owner_uid": owner,
            "participants": participants,
            "messages": [{"id": str(mid), "sender_uid": sender, "text": content}],
            "new_message_id": str(mid),
        }


def _checker(*, deleted=(), unavailable=False, exc=None):
    async def check(channel_id, message_id):
        if exc is not None:
            raise exc
        if message_id in deleted:
            raise SourceDeleted("fake gone")
        if unavailable:
            raise SourceUnavailable("fake blip")

    return check


def _wire_send(client, settings, mid=701, chunks=("reply-body",)):
    """Agent-send simulation: the POST await records store sends, like the MCP tool."""

    async def submit(sid, text):
        with Store.open(settings.db_path) as store:
            ref = store.get_job(CHANNEL_ID, mid)["job_ref"]
            assert ref and ref in text  # prompt carries the live nonce delimiters
            for i, _chunk in enumerate(chunks):
                store.record_send(CHANNEL_ID, mid, job_ref=ref, sent_message_id=SENT_MID + i)
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": False,
            "stored_text_match": True,
        }

    client.submit_fn = submit


def _responder(settings, **overrides):
    kwargs = {
        "client": _FakeClient(),
        "source_checker": _checker(),
        "jev_adapter": _FakeAdapter("respond"),
        "context_collector": _FakeCollector(),
    }
    kwargs.update(overrides)
    if "participation_ledger" not in kwargs:
        kwargs["participation_ledger"] = ParticipationLedger(settings.db_path)
    return Responder(settings, _FakeBot(), **kwargs)


# --- A: constructor / sweep ---------------------------------------------------


def test_import_has_no_side_effects(tmp_path):
    before = set(__import__("sys").modules)
    import responder  # noqa: F401

    # Package import only loads its own submodules: no stray top-level
    # modules, no file creation (all state lives under the tmp fixture).
    assert set(__import__("sys").modules) - before <= {
        "responder",
        "responder.core",
        "responder.submit",
        "responder.input",
    }


def test_constructor_missing_sid_fails_without_reset(tmp_path):
    settings = _settings(tmp_path)
    assert not settings.db_path.exists()
    with pytest.raises(ResponderError, match="native session not seeded"):
        Responder(
            settings,
            _FakeBot(),
            client=_FakeClient(),
            source_checker=_checker(),
            jev_adapter=_FakeAdapter(),
            context_collector=_FakeCollector(),
            participation_ledger=ParticipationLedger(settings.db_path),
        )
    # Fail loud, never auto-create: the store exists now but holds no SID.
    with Store.open(settings.db_path) as store:
        assert store.get_native_sid() is None


def test_constructor_bad_sid_fails_loud(tmp_path):
    settings = _prepare(tmp_path)
    with Store.open(settings.db_path) as store:
        store._conn.execute("UPDATE state SET native_sid='bogus' WHERE id=1;")
    # Fail loud via StoreError (never auto-create, never reset): the raw
    # get_native_sid check rejects the corrupt SID at open.
    with pytest.raises(StoreError, match="bad native_sid"):
        _responder(settings)


def test_constructor_sweeps_submitted_once_queued_resumes(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    _append(settings, message_id=702)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, 701, 1)
        store.enqueue_job(CHANNEL_ID, 702, 2)
        store.mark_submitted(
            CHANNEL_ID, 701, job_ref="r" * 32, native_sid=FAKE_SID, pre_turn_index=0
        )
    _responder(settings)  # construction runs the restart sweep once
    first = _job(settings, 701)
    assert first["stage"] == "failed" and first["fail_kind"] == "restart"
    assert _job(settings, 702)["stage"] == "queued"  # queued resumes, never swept


def test_constructor_rejects_adapter_without_decide(tmp_path):
    settings = _prepare(tmp_path)
    with pytest.raises(ResponderError, match="jev_adapter must provide async decide"):
        Responder(
            settings,
            _FakeBot(),
            client=_FakeClient(),
            source_checker=_checker(),
            jev_adapter=object(),
            context_collector=_FakeCollector(),
            participation_ledger=ParticipationLedger(settings.db_path),
        )


# --- B: prompt ----------------------------------------------------------------


async def test_prompt_json_is_projected_snapshot_with_stop_guards(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, content="say 'hi' with \\ backslash and ünïcodé")
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    _wire_send(client, settings)
    responder = _responder(settings, client=client, jev_adapter=adapter)
    await responder.tick()
    (sid, text) = client.submit_calls[0]
    assert sid == FAKE_SID
    assert "TRUSTED_METADATA" in text
    assert "UNTRUSTED_CONVERSATION" in text
    assert "NEVER disclose" in text
    lines = text.split("\n")
    start = next(i for i, line in enumerate(lines) if line.endswith("START"))
    end = next(i for i, line in enumerate(lines) if line.endswith("END"))
    import json as _json

    prompt = _json.loads("\n".join(lines[start + 1 : end]))
    # SAME canonical snapshot feeds Jev and the prompt: exact projection.
    assert prompt == project_conversation_state(adapter.calls[0])
    # Quotes/backslashes/nonascii survive the prompt round-trip exactly.
    assert _json.loads(_json.dumps(prompt))["messages"][0]["text"] == (
        "say 'hi' with \\ backslash and ünïcodé"
    )


async def test_prompt_oversize_content_fails_closed(tmp_path):
    # The fit gate fires on the ACTUAL renderer for BOTH participations
    # BEFORE any paid call: necessary-pairs overflow never truncates.
    settings = _prepare(tmp_path)
    _append(settings, content="x" * 9000)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, jev_adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "snapshot_oversize"
    assert adapter.calls == [] and client.submit_calls == []  # zero paid work
    job = _job(settings)
    assert job["stage"] == "failed" and job["fail_kind"] == "transport_error"
    assert job["send_count"] == 0


# --- C: guard_ack ---------------------------------------------------------------


def test_guard_ack_allows_safe_rejects_beyond(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    _append(settings, message_id=702)
    responder = _responder(settings)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, 701, 1, skip_reason="out_of_scope")
    responder.guard_ack(1)  # safe prefix advances over the terminal skip
    with pytest.raises(ResponderError, match="ack beyond safe prefix"):
        responder.guard_ack(2)  # 702 still queued: head-blocked


def test_guard_ack_noop_below_acked_and_rejects_negative(tmp_path):
    settings = _prepare(tmp_path)
    responder = _responder(settings)
    responder.guard_ack(0)  # empty outbox: no-op, never raises
    with pytest.raises(ResponderError, match="guard_ack requires seq"):
        responder.guard_ack(-1)


# --- D: correlated_job ------------------------------------------------------------


async def _submit_one_tick(settings, mid=701):
    """Drive one tick whose POST is still running: the job stays submitted."""
    _append(settings, message_id=mid)
    client = _FakeClient()

    async def running_post(sid, text):
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": True,
            "stored_text_match": True,
        }

    client.submit_fn = running_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending" and outcome["reason"] == "turn_running"
    job = _job(settings, mid)
    assert job["stage"] == "submitted"
    # NOTE: the SAME responder must be reused below -- constructing a new
    # one would run the restart sweep and fail this submitted job.
    return responder, job


async def test_correlated_job_ok_and_validates(tmp_path):
    settings = _prepare(tmp_path)
    responder, job = await _submit_one_tick(settings)
    got = responder.correlated_job(701, job["job_ref"], CHANNEL_ID)
    assert got["job_ref"] == job["job_ref"]
    assert got["stage"] == "submitted" and got["send_count"] == 0
    with pytest.raises(ResponderError, match="unknown job"):
        responder.correlated_job(999, job["job_ref"], CHANNEL_ID)
    with pytest.raises(ResponderError, match="job_ref mismatch"):
        responder.correlated_job(701, "0" * 32, CHANNEL_ID)
    with pytest.raises(ResponderError, match="channel out of scope"):
        responder.correlated_job(701, job["job_ref"], CHANNEL_ID + 1)
    with pytest.raises(ResponderError, match="invalid source_message_id"):
        responder.correlated_job(True, job["job_ref"], CHANNEL_ID)


async def test_correlated_job_rejects_terminal_and_queued(tmp_path):
    settings = _prepare(tmp_path)
    responder, job = await _submit_one_tick(settings)
    ref = job["job_ref"]
    # A queued job carries no ref yet: any presented ref mismatches.
    _append(settings, message_id=702)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, 702, 2)
    with pytest.raises(ResponderError, match="job_ref mismatch"):
        responder.correlated_job(702, ref, CHANNEL_ID)
    # Failed and skipped terminals never re-send, even with the live ref.
    with Store.open(settings.db_path) as store:
        store.mark_failed(CHANNEL_ID, 701, kind="transport_error")
    with pytest.raises(ResponderError, match="stage not eligible"):
        responder.correlated_job(701, ref, CHANNEL_ID)
    # Submit 702 (FIFO next), then skip it: its own live ref is rejected.
    await responder.tick()
    job2 = _job(settings, 702)
    assert job2["stage"] == "submitted"
    with Store.open(settings.db_path) as store:
        store.skip_job(CHANNEL_ID, 702, reason="out_of_scope")
    with pytest.raises(ResponderError, match="stage not eligible"):
        responder.correlated_job(702, job2["job_ref"], CHANNEL_ID)


# --- E: ingest ------------------------------------------------------------------


async def test_tick_filters_self_and_other_bots_no_post(tmp_path):
    settings = _prepare(tmp_path)
    # Self UID wins regardless of the bot flag; other-bot filtered by flag.
    _append(
        settings,
        message_id=701,
        author_uid=BOT_UID,
        actor_kind="self",
        author_name="ThycaBot",
        author_is_bot=False,
    )
    _append(
        settings,
        message_id=702,
        author_uid=OTHER_UID,
        actor_kind="other",
        author_name="OtherBot",
        author_is_bot=True,
    )
    client = _FakeClient()
    responder = _responder(settings, client=client)
    outcome = await responder.tick()  # ingest skips land terminal: idle
    assert outcome["status"] == "idle"
    assert client.submit_calls == []
    assert _job(settings, 701)["skip_reason"] == "bot_filtered"
    assert _job(settings, 702)["skip_reason"] == "bot_filtered"


async def test_self_message_skipped_even_when_flag_says_human(tmp_path):
    # A spoofed author_is_bot=False on the SELF uid must not enqueue.
    settings = _prepare(tmp_path)
    _append(
        settings,
        message_id=701,
        author_uid=BOT_UID,
        actor_kind="self",
        author_name="ThycaBot",
        author_is_bot=False,
    )
    responder = _responder(settings)
    outcome = await responder.tick()
    assert outcome["status"] == "idle"
    assert _job(settings, 701)["skip_reason"] == "bot_filtered"


async def test_out_of_scope_channel_skipped_no_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701, channel_id=9999)
    client = _FakeClient()
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "idle"
    assert client.submit_calls == []
    assert _job(settings, 701, channel_id=9999)["skip_reason"] == "out_of_scope"


async def test_duplicate_mid_ingests_once_no_second_job(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    append_entry(settings.db_path, **_entry_fields(message_id=701))  # poll redelivery
    unread = read_entries(settings.db_path, unread_only=True)
    assert [e["message_id"] for e in unread["entries"]] == [701]  # dedupe: first wins
    responder = _responder(settings)
    await responder.tick()
    assert len(_jobs(settings)) == 1


async def test_tick_corrupt_entry_fails_without_mutation(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    with Store.open(settings.db_path) as store:
        store._conn.execute("UPDATE outbox SET actor_kind='spoofed-owner' WHERE message_id=701;")
    responder = _responder(settings)
    with pytest.raises(ResponderError, match="actor_kind contradicts"):
        await responder.tick()
    assert _jobs(settings) == []  # validation runs before any return/side effect


async def test_legacy_frame_resolves_actual_bot_flag(tmp_path):
    settings = _prepare(tmp_path)
    fields = _entry_fields(message_id=701, author_uid=OTHER_UID, actor_kind="other")
    del fields["author_is_bot"]  # legacy frame: flag unknown, never assumed
    append_entry(settings.db_path, **fields)
    bot = _FakeBot(flags={OTHER_UID: True})  # SDK says: actually a bot
    responder = Responder(
        settings,
        bot,
        client=_FakeClient(),
        source_checker=_checker(),
        jev_adapter=_FakeAdapter(),
        context_collector=_FakeCollector(),
        participation_ledger=ParticipationLedger(settings.db_path),
    )
    outcome = await responder.tick()
    assert outcome["status"] == "idle"
    assert _job(settings, 701)["skip_reason"] == "bot_filtered"


async def test_legacy_frame_unresolvable_defers_at_submit(tmp_path):
    settings = _prepare(tmp_path)
    fields = _entry_fields(message_id=701)
    del fields["author_is_bot"]
    append_entry(settings.db_path, **fields)
    client = _FakeClient()
    responder = _responder(settings, client=client)  # empty flags: unresolvable
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "bot_flag_unresolved"
    assert client.submit_calls == []  # zero paid work for an unproven source


# --- F: FIFO + snapshot gates -----------------------------------------------------


async def test_fifo_first_pending_blocks_second(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    _append(settings, message_id=702)
    client = _FakeClient()
    responder = _responder(settings, client=client)
    outcome = await responder.tick()  # head submits; same tick must not POST 702
    assert outcome["status"] in ("sent", "failed", "pending")
    assert len(client.submit_calls) == 1
    live_ref = _job(settings, 701)["job_ref"]
    assert live_ref and live_ref in client.submit_calls[0][1]
    assert _job(settings, 702)["stage"] == "queued"


async def test_tick_busy_snapshot_holds_queued_no_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def busy(sid):
        raise ThycaBusyError()

    client.snapshot_fn = busy
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "snapshot_busy"
    assert client.submit_calls == []  # never spend a paid call when busy
    assert _job(settings)["stage"] == "queued"


async def test_tick_snapshot_error_bounded_no_post_no_mutation(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def broken(sid):
        raise ThycaProtocolError()

    client.snapshot_fn = broken
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "snapshot_unavailable"
    assert outcome["error_kind"] == "protocol"  # coarse kind only, never raw
    assert client.submit_calls == []
    assert _job(settings)["stage"] == "queued"


async def test_tick_busy_guard_prevents_overlap(tmp_path):
    settings = _prepare(tmp_path)
    responder = _responder(settings)
    responder._busy = True  # simulate a tick already in flight
    assert await responder.tick() == {"status": "busy"}
    responder._busy = False
    assert (await responder.tick())["status"] == "idle"


# --- G: full submit path ------------------------------------------------------------


async def test_owner_message_one_post_completes_sent(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    _wire_send(client, settings)
    responder = _responder(settings, client=client, jev_adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert outcome["acked"] == entry["seq"]
    assert len(client.submit_calls) == 1
    assert len(adapter.calls) == 1  # exactly one paid classification
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "sent")
    assert job["send_count"] == 1
    assert job["sent_message_ids"] == [SENT_MID]


async def test_two_chunks_two_sends_one_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    _wire_send(client, settings, chunks=("part one", "part two"))
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    job = _job(settings)
    assert job["send_count"] == 2
    assert job["sent_message_ids"] == [SENT_MID, SENT_MID + 1]
    assert len(client.submit_calls) == 1  # one POST, multi-chunk sends


# --- H: observe / need_context ------------------------------------------------------


async def test_observe_terminal_zero_post_safe_ack(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()
    responder = _responder(settings, client=client, jev_adapter=_FakeAdapter("observe"))
    outcome = await responder.tick()
    assert outcome["status"] == "observed"
    assert outcome["acked"] == entry["seq"]
    assert client.submit_calls == []  # zero native work on the observe path
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "observed")


async def test_completed_turn_no_outcome_fails_closed(tmp_path):
    # Jev only emits respond/observe: a clean idle turn with NO send and
    # NO no_reply closes failed (both routed prompts obligate one call).
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    responder = _responder(settings, client=client, jev_adapter=_FakeAdapter("respond"))
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "turn_no_outcome"
    job = _job(settings)
    assert job["stage"] == "failed" and job["send_count"] == 0


async def test_terminal_no_reply_during_post_completes(tmp_path):
    settings = _prepare(tmp_path)
    entry = _append(settings)
    client = _FakeClient()

    async def submit(sid, text):
        with Store.open(settings.db_path) as store:
            store.complete_job(CHANNEL_ID, 701, outcome="no_reply")  # tool-time terminal
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": False,
            "stored_text_match": True,
        }

    client.submit_fn = submit
    responder = _responder(settings, client=client, jev_adapter=_FakeAdapter("respond"))
    outcome = await responder.tick()
    assert outcome["status"] == "no_reply"
    assert outcome["acked"] == entry["seq"]
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "no_reply")


# --- I: busy race / running / cancel / timeout ----------------------------------------


async def test_busy_409_zero_sends_requeues_with_fresh_nonce(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def busy_post(sid, text):
        raise ThycaBusyError()

    client.submit_fn = busy_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "busy_requeued"
    job = _job(settings)
    assert job["stage"] == "queued"  # ref cleared; next attempt mints fresh
    assert job["job_ref"] is None
    # Next tick resubmits with a NEW nonce (never the cleared one).
    _wire_send(client, settings)
    outcome2 = await responder.tick()
    assert outcome2["status"] == "sent"
    ref2 = _job(settings)["job_ref"]
    assert ref2 and uuid.UUID(hex=ref2)  # caller-opaque random32 hex


async def test_busy_409_with_send_evidence_fails_never_retried(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def busy_after_send(sid, text):
        with Store.open(settings.db_path) as store:
            ref = store.get_job(CHANNEL_ID, 701)["job_ref"]
            store.record_send(CHANNEL_ID, 701, job_ref=ref, sent_message_id=SENT_MID)
        raise ThycaBusyError()

    client.submit_fn = busy_after_send
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "busy_with_sends"
    job = _job(settings)
    assert job["stage"] == "failed" and job["send_count"] == 1  # send preserved


async def test_agent_send_during_running_post_holds_then_closes_sent(tmp_path):
    # Fact-check A rule: a still-running result holds submitted EVEN when
    # the agent's send already landed -- the send stands, but only the
    # idle poll may complete it. (Previous expectation closed sent at POST
    # return; that contradicts the rule, hence this justified change.)
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def running_post(sid, text):
        # The agent's send lands DURING the POST await (like the MCP tool)
        # while the native turn keeps running: hold, close on idle.
        with Store.open(settings.db_path) as store:
            ref = store.get_job(CHANNEL_ID, 701)["job_ref"]
            store.record_send(CHANNEL_ID, 701, job_ref=ref, sent_message_id=SENT_MID)
        client.turn_bump = 1
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": True,  # still running: hold submitted, do not close
            "stored_text_match": True,
        }

    client.submit_fn = running_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "turn_running"
    job = _job(settings)
    assert job["stage"] == "sent"  # send stands, completion waits for idle
    assert job["send_count"] == 1
    # Later tick: idle snapshot (bumped turn) + recorded send closes sent.
    outcome2 = await responder.tick()
    assert outcome2["status"] == "sent"
    assert _job(settings)["outcome"] == "sent"


async def test_cancellation_fails_job_and_propagates(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def cancelled_post(sid, text):
        raise asyncio.CancelledError()

    client.submit_fn = cancelled_post
    responder = _responder(settings, client=client)
    with pytest.raises(asyncio.CancelledError):
        await responder.tick()
    job = _job(settings)
    assert job["stage"] == "failed"  # ambiguous => failed, never blind retry
    assert job["fail_kind"] == "transport_error"


async def test_post_timeout_uncertain_fails_no_retry(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def timeout_post(sid, text):
        raise ThycaSubmissionUncertainError(reason="timeout")

    client.submit_fn = timeout_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "submission_uncertain"
    assert outcome["error_kind"] == "submission_uncertain"  # coarse, never raw
    job = _job(settings)
    assert job["stage"] == "failed" and job["fail_kind"] == "timeout"
    # No retry means no retry: the next tick reports the terminal and
    # never POSTs again (this second-tick assertion earns the test name).
    outcome2 = await responder.tick()
    assert outcome2["status"] == "idle"
    assert len(client.submit_calls) == 1


@pytest.mark.parametrize("tamper", ["stored_text", "turn_index_high", "turn_index_type"])
async def test_bad_post_result_proof_closes_failed(tmp_path, tamper):
    # A completed idle turn whose result fails proof closes failed
    # (turn_no_outcome) -- never done, never retried.
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def tampered_post(sid, text):
        result = {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": False,
            "stored_text_match": True,
        }
        if tamper == "stored_text":
            result["stored_text_match"] = False
        elif tamper == "turn_index_high":
            result["turn_index"] = 999  # foreign turn interleaved at return
        else:
            result["turn_index"] = "4"  # malformed: never trusted
        return result

    client.submit_fn = tampered_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "turn_no_outcome"
    job = _job(settings)
    assert job["stage"] == "failed" and job["fail_kind"] == "turn_no_outcome"


async def test_poll_interleaved_turn_index_closes_failed(tmp_path):
    # Submitted turn still running at POST return; the next tick polls an
    # idle snapshot whose index EXCEEDS the submitted turn (a manual turn
    # interleaved): close failed(turn_no_outcome) -- never re-POST, never
    # strand pending forever on the stale expectation.
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def running_post(sid, text):
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": True,
            "stored_text_match": True,
        }

    client.submit_fn = running_post
    responder = _responder(settings, client=client)
    assert (await responder.tick())["reason"] == "turn_running"
    assert _job(settings)["stage"] == "submitted"
    client.turn_bump = 2  # foreign turn lands before the next poll
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "turn_no_outcome"
    assert _job(settings)["fail_kind"] == "turn_no_outcome"
    assert len(client.submit_calls) == 1  # poll closes; it never re-POSTs


async def test_post_http_error_maps_coarse_kind(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def http_post(sid, text):
        raise ThycaClientError("http_500", status_code=500)

    client.submit_fn = http_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "post_failed"
    assert outcome["error_kind"] == "http_500"
    job = _job(settings)
    assert job["stage"] == "failed" and job["fail_kind"] == "http_error"


# --- J: SID identity ------------------------------------------------------------------


async def test_gate_snapshot_mismatch_blocks_before_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def mismatch(sid):
        snap = client._snapshot(sid)
        snap["native_id_match"] = False
        return snap

    client.snapshot_fn = mismatch
    adapter = _FakeAdapter("respond")
    responder = _responder(settings, client=client, jev_adapter=adapter)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "snapshot_mismatch"
    assert client.submit_calls == []
    assert adapter.calls == []  # never spend a Jev call when native mismatches
    assert _job(settings)["stage"] == "queued"


async def test_post_sid_mismatch_fails_closed(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def sid_flip(sid, text):
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": False,  # wrong session answered
            "turn_index": 99,
            "running": False,
            "stored_text_match": True,
        }

    client.submit_fn = sid_flip
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "post_sid_mismatch"
    assert _job(settings)["fail_kind"] == "snapshot_mismatch"


async def test_serving_sid_changed_mid_tick_fails_closed(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    responder = _responder(settings)
    with Store.open(settings.db_path) as store:
        store.adopt_session(expected_current=FAKE_SID, new_sid=FAKE_SID2, day=FAKE_DAY)
    with pytest.raises(ResponderError, match="native_session_id mismatch"):
        await responder.tick()


# --- K: source deleted ------------------------------------------------------------


async def test_deleted_source_before_submit_skips_zero_work(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    responder = _responder(
        settings, client=client, jev_adapter=adapter, source_checker=_checker(deleted={701})
    )
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    assert adapter.calls == [] and client.submit_calls == []  # zero paid work
    assert _job(settings)["stage"] == "skipped"


async def test_deleted_after_paid_classify_skips_no_native_work(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    seen = {"first": True}

    async def flaky(channel_id, message_id):
        if seen["first"]:
            seen["first"] = False
            return None
        raise SourceDeleted("gone during classify")

    responder = _responder(settings, client=client, jev_adapter=adapter, source_checker=flaky)
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    assert len(adapter.calls) == 1  # spend is sunk; zero NATIVE work follows
    assert client.submit_calls == []


async def test_inflight_zero_send_deleted_source_converges_skip(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def running_post(sid, text):
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": True,
            "stored_text_match": True,
        }

    client.submit_fn = running_post
    responder = _responder(settings, client=client)
    assert (await responder.tick())["status"] == "pending"
    assert _job(settings)["stage"] == "submitted"
    # Source deleted while submitted, zero sends: reconcile converges the
    # skip. Swap the checker on the SAME responder: constructing a new one
    # would run the restart sweep and fail the submitted job first.
    responder._source_checker = _checker(deleted={701})
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    assert outcome["acked"] == 1  # durable skip advances the safe prefix


async def test_deleted_source_with_send_evidence_fails(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    _wire_send(client, settings)
    responder = _responder(settings, client=client)
    # Agent send lands, THEN the source vanishes: the skip is refused.
    outcome = await responder.tick()
    assert outcome["status"] == "sent"  # recorded send stands; delete is moot
    job = _job(settings)
    assert job["send_count"] == 1
    # A LATER delete check on the terminal job respects the terminal.
    responder2 = _responder(settings, source_checker=_checker(deleted={701}))
    outcome2 = await responder2.tick()
    assert outcome2["status"] == "idle"  # terminal: no reason key, just idle


# --- L: source edited -------------------------------------------------------------------


async def test_durable_skip_wins_over_fingerprint_mismatch_no_post(tmp_path):
    # The MCP pre-send gate records the durable source_deleted skip DURING
    # the post-paid refresh await (after the last routing guard): the
    # submit.py mismatch path must honor the skip -- never pend on
    # context, never POST. Covers busy/timeout-after-skip ordering too:
    # both handlers check the durable skip FIRST (see next test), so this
    # harness (skip landing mid-tick) is the shared race shape.
    settings = _prepare(tmp_path)
    _append(settings, content="original text")
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    inner = _FakeCollector()
    inner.script = [None, ("edit", "changed text")]

    async def skip_then_collect(entry):
        # Plain function wrapper (an instance __call__ assignment would
        # never fire: dunder lookup bypasses the instance dict).
        if len(inner.calls) == 1:  # about to run the post-paid refresh
            with Store.open(settings.db_path) as store:
                store.skip_job(CHANNEL_ID, 701, reason="source_deleted")
        return await inner(entry)

    responder = _responder(
        settings, client=client, jev_adapter=adapter, context_collector=skip_then_collect
    )
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    assert len(adapter.calls) == 1  # spend sunk before the skip landed
    assert client.submit_calls == []  # zero native work after the skip
    assert _job(settings)["stage"] == "skipped"


@pytest.mark.parametrize("kind", ["busy", "timeout"])
async def test_post_race_after_skip_stays_skipped(tmp_path, kind):
    # Skip recorded DURING the POST await: the busy-requeue and the
    # timeout-failure handlers both check the durable skip first and must
    # not clobber it (no requeue, no transport_error failure).
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def racing_post(sid, text):
        with Store.open(settings.db_path) as store:
            store.skip_job(CHANNEL_ID, 701, reason="source_deleted")
        if kind == "busy":
            raise ThycaBusyError()
        raise ThycaSubmissionUncertainError(reason="timeout")

    client.submit_fn = racing_post
    responder = _responder(settings, client=client)
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_deleted"
    job = _job(settings)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"


async def test_proven_edit_after_classify_skips_without_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, content="original text")
    client = _FakeClient()
    collector = _FakeCollector()
    collector.script = [None, ("edit", "changed text")]  # post-paid refresh sees the edit
    adapter = _FakeAdapter("respond")
    responder = _responder(
        settings, client=client, jev_adapter=adapter, context_collector=collector
    )
    outcome = await responder.tick()
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "source_edited_after_classify"
    assert len(adapter.calls) == 1  # spend sunk; answering stale text would be wrong
    assert client.submit_calls == []
    job = _job(settings)
    assert job["stage"] == "skipped"
    assert job["skip_reason"] == "source_edited_after_classify"


async def test_unproven_drift_holds_pending_without_post(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    collector = _FakeCollector()
    # Sender spoof on refresh: not a proven edit, never a skip, never a POST.
    collector.script = [None, ("sender", OTHER_UID)]
    responder = _responder(settings, client=client, context_collector=collector)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "context_mismatch"
    assert client.submit_calls == []
    assert _job(settings)["stage"] == "queued"


async def test_context_unavailable_holds_pending_zero_paid_work(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    adapter = _FakeAdapter("respond")
    collector = _FakeCollector()
    collector.script = ["unavailable"]
    responder = _responder(
        settings, client=client, jev_adapter=adapter, context_collector=collector
    )
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "context_unavailable"
    assert adapter.calls == [] and client.submit_calls == []
    assert _job(settings)["stage"] == "queued"


# --- M: adoption ------------------------------------------------------------------------


async def test_first_tick_converges_empty_store_to_dm(tmp_path, capsys):
    settings = _prepare(tmp_path)
    client = _FakeClient(dm_sid=FAKE_SID2)
    responder = Responder(
        settings,
        _FakeBot(),
        client=client,
        source_checker=_checker(),
        jev_adapter=_FakeAdapter(),
        context_collector=_FakeCollector(),
        participation_ledger=ParticipationLedger(settings.db_path),
        session_day_provider=lambda: FAKE_DAY,
    )
    assert responder.native_sid == FAKE_SID  # constructor does not adopt
    outcome = await responder.tick()
    assert outcome["status"] == "idle"  # converged, then idle on the empty store
    assert responder.native_sid == FAKE_SID2
    with Store.open(settings.db_path) as store:
        assert store.get_native_sid() == FAKE_SID2
    assert "adopted target bot DM session" in capsys.readouterr().err


async def test_adopt_queued_job_submits_under_dm_sid(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient(dm_sid=FAKE_SID2)
    _wire_send(client, settings)
    responder = _responder(settings, client=client, session_day_provider=lambda: FAKE_DAY)
    outcome = await responder.tick()
    assert outcome["status"] == "sent"
    assert client.submit_calls[0][0] == FAKE_SID2  # the POST serves the DM SID
    job = _job(settings)
    assert job["native_sid"] == FAKE_SID2


async def test_adopt_transient_dm_failure_defers_without_mutation(tmp_path, capsys):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def flaky_dm(bot_id):
        raise ThycaProtocolError(reason="http_5xx")  # transient blip: defer

    client.get_bot_dm = flaky_dm
    responder = _responder(settings, client=client, session_day_provider=lambda: FAKE_DAY)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "target_dm_deferred"
    # Deferral fires BEFORE ingest: no job row exists at all (the outbox
    # entry is untouched, retried next tick).
    assert _jobs(settings) == []
    assert "deferred for 2026-01-02" in capsys.readouterr().err
    # One stderr note per day: the second deferral is silent.
    outcome2 = await responder.tick()
    assert outcome2["reason"] == "target_dm_deferred"
    assert capsys.readouterr().err == ""


async def test_adopt_unknown_bot_404_fails_closed(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    from thyca import ThycaPreconditionError

    async def missing_dm(bot_id):
        raise ThycaPreconditionError(status_code=404)  # definitive: bad target

    client.get_bot_dm = missing_dm
    responder = _responder(settings, client=client)
    with pytest.raises(ResponderError, match="target bot DM missing"):
        await responder.tick()
    # Fail closed: adoption raises BEFORE ingest, so no job row exists at
    # all (the outbox entry is untouched, retried next tick).
    assert _jobs(settings) == []


async def test_blocked_adoption_reconciles_midflight_without_submit(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()

    async def running_post(sid, text):
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": (client.pre_max or 0) + 1,
            "running": True,
            "stored_text_match": True,
        }

    client.submit_fn = running_post
    responder = _responder(settings, client=client)
    assert (await responder.tick())["status"] == "pending"
    # Native DM changed while a job is mid-flight: adoption defers and the
    # tick runs a reconcile-only pass on the CURRENT sid (never a submit).
    client.dm_sid = FAKE_SID2
    n_posts = len(client.submit_calls)
    outcome = await responder.tick()
    assert outcome["reason"] == "target_dm_blocked"
    assert len(client.submit_calls) == n_posts  # reconcile only, no POST
    # The reconcile pass polls an idle snapshot whose index trails the
    # submitted turn: per the turn-index decision it closes failed
    # (turn_no_outcome), never re-POSTs, never strands pending.
    job = _job(settings)
    assert job["stage"] == "failed" and job["fail_kind"] == "turn_no_outcome"


async def test_snapshot_gone_holds_then_next_tick_adopts_recreated_dm(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)
    client = _FakeClient()
    from thyca import ThycaPreconditionError

    async def gone(sid):
        raise ThycaPreconditionError(status_code=404)

    client.snapshot_fn = gone
    responder = _responder(settings, client=client, session_day_provider=lambda: FAKE_DAY)
    outcome = await responder.tick()
    assert outcome["status"] == "pending"
    assert outcome["reason"] == "snapshot_gone"
    assert outcome["error_status"] == 404
    assert _job(settings)["stage"] == "queued"  # zero state mutation
    # Native recreated the DM under a new SID: the next tick adopts it.
    client.snapshot_fn = None
    client.dm_sid = FAKE_SID2
    _wire_send(client, settings)
    outcome2 = await responder.tick()
    assert outcome2["status"] == "sent"
    assert responder.native_sid == FAKE_SID2


# --- N: shutdown --------------------------------------------------------------------------


async def test_close_fails_midflight_queued_stays_terminal_kept(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, message_id=701)
    _append(settings, message_id=702)
    _append(settings, message_id=703)
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, 701, 1)
        store.enqueue_job(CHANNEL_ID, 702, 2)
        store.enqueue_job(CHANNEL_ID, 703, 3, skip_reason="out_of_scope")
        store.mark_submitted(
            CHANNEL_ID, 701, job_ref="s" * 32, native_sid=FAKE_SID, pre_turn_index=0
        )
        store.record_send(CHANNEL_ID, 701, job_ref="s" * 32, sent_message_id=SENT_MID)
    responder = _responder(settings)
    await responder.close()
    first = _job(settings, 701)
    assert first["stage"] == "failed" and first["fail_kind"] == "shutdown"
    assert first["send_count"] == 1  # committed sends preserved, never rewound
    assert _job(settings, 702)["stage"] == "queued"
    skipped = _job(settings, 703)
    assert skipped["stage"] == "skipped"  # terminal kept as recorded
    with pytest.raises(ResponderError, match="responder is closed"):
        await responder.tick()


# --- O: privacy -----------------------------------------------------------------------------


async def test_no_raw_content_in_errors_or_job_metadata(tmp_path):
    settings = _prepare(tmp_path)
    canary = "CANARY_RAW_X7Q"
    _append(settings, content=f"secret {canary} payload")
    responder = _responder(settings)
    # Force a bounded failure carrying no raw text: corrupt the frame's
    # actor binding after append (validation error path).
    with Store.open(settings.db_path) as store:
        store._conn.execute("UPDATE outbox SET actor_kind='other' WHERE message_id=701;")
    with pytest.raises(ResponderError) as excinfo:
        await responder.tick()
    assert canary not in str(excinfo.value)
    job = _job(settings, 701)
    assert job is None  # no job row: validation precedes ingest
    raw = settings.db_path.read_bytes()
    assert canary.encode() in raw  # outbox holds source text by design (prompt need)
    # ...but job metadata never does: enqueue, then inspect the jobs table.
    with Store.open(settings.db_path) as store:
        store._conn.execute("UPDATE outbox SET actor_kind='owner' WHERE message_id=701;")
    await responder.tick()
    job = _job(settings, 701)
    assert job is not None
    assert canary.encode() not in str(job).encode()


async def test_audit_free_core_never_logs_raw(tmp_path, capsys):
    settings = _prepare(tmp_path)
    _append(settings, content="CANARY_LOG_Z2W plain hello")
    responder = _responder(settings)
    await responder.tick()
    assert "CANARY_LOG_Z2W" not in capsys.readouterr().err


# --- P: misc -------------------------------------------------------------------------------


async def test_tick_empty_store_idle(tmp_path):
    settings = _prepare(tmp_path)
    responder = _responder(settings)
    outcome = await responder.tick()
    assert outcome["status"] == "idle"
    assert outcome["acked"] == 0


async def test_classification_transport_failure_fails_closed(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings)

    class _BoomLedger:
        async def classify(self, *args, **kwargs):
            raise ConnectionError("provider down")

    responder = _responder(settings, participation_ledger=_BoomLedger())
    outcome = await responder.tick()
    assert outcome["status"] == "failed"
    assert outcome["reason"] == "classification_failed"
    assert _job(settings)["stage"] == "failed"


def test_job_rows_carry_no_content_column(tmp_path):
    settings = _prepare(tmp_path)
    _append(settings, content="metadata only please")
    with Store.open(settings.db_path) as store:
        cols = [r["name"] for r in store._conn.execute("PRAGMA table_info(jobs);")]
    assert "content" not in cols
