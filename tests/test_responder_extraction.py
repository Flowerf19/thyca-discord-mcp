"""T4 extraction ownership: input canonical + recovery delegation (tmp only).

Fake TMP only (``_env_file=None``); no production reads, no network/paid.
Proves: ResponderError/strict/_SAFE_TERMINAL single-owner identity, pure
input equivalence via Root thin wrappers, Recovery direct with injected
fake client, Root->Recovery delegation with fresh callback lookup, and
constructor bind-marker still last after ledger durable with no-IO Recovery
wiring.
"""

from __future__ import annotations

import hashlib
import os
from types import SimpleNamespace

import pytest

import delivery
import responder as responder_mod
from config import Settings
from delivery import (
    enqueue_job,
    initialize_delivery_state,
    mark_inflight,
    mark_turn_submitting,
)
from responder import Responder, ResponderRecovery
from responder import input as input_mod
from responder import recovery as recovery_mod

FAKE_SID = "2026-01-02T03-04-05_ab12"
GUILD = 100000000000000001
CHANNEL = 200000000000000002
OWNER = 300000000000000003
BOT = 400000000000000004
OTHER = 500000000000000005


def _settings(tmp_path, **overrides):
    kwargs: dict = {
        "discord_token": "fake-token",
        "owner_uid": OWNER,
        "guild_id": GUILD,
        "default_channel_id": CHANNEL,
        "delivery_state_path": tmp_path / "delivery.json",
        "outbox_path": tmp_path / "outbox.jsonl",
        "outbox_state_path": tmp_path / "outbox_state.json",
        "_env_file": None,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def _init_delivery(settings):
    return initialize_delivery_state(
        settings.delivery_state_path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )


class _FakeBot:
    def __init__(self, uid=BOT):
        self.user = SimpleNamespace(id=uid)


class _FakeClient:
    def __init__(self):
        self.snapshot_calls: list = []
        self.trace_calls: list = []
        self.dm_calls: list = []
        self.snapshot_fn = None
        self.trace_fn = None

    async def get_bot_dm(self, bot_id):
        self.dm_calls.append(bot_id)
        return {
            "native_sid": FAKE_SID,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }

    async def get_session_snapshot(self, sid, expected_title=None):
        self.snapshot_calls.append((sid, expected_title))
        if self.snapshot_fn is not None:
            return await self.snapshot_fn(sid, expected_title)
        return {
            "native_id_match": True,
            "running": False,
            "turn_indices": [],
            "pre_max_turn_index": None,
        }

    async def get_trace_evidence(
        self, sid, idx, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        self.trace_calls.append((sid, idx, job_ref, source_message_id, channel_id))
        if self.trace_fn is not None:
            return await self.trace_fn(sid, idx, job_ref, source_message_id, channel_id)
        raise AssertionError("trace_fn not wired")


class _FakeAdapter:
    async def decide(self, state):
        raise AssertionError("not used")


async def _fake_collector(entry):
    return {}


class _FakeLedger:
    def __init__(self, ledger_id="ab" * 16):
        self._ledger_id = ledger_id
        self.calls = 0

    @property
    def ledger_id(self) -> str:
        return self._ledger_id

    async def classify(self, adapter, state, *, channel_id, message_id, author_uid):
        self.calls += 1
        raise AssertionError("not used")


def _responder(tmp_path, client=None):
    settings = _settings(tmp_path)
    _init_delivery(settings)
    bot = _FakeBot()
    client = client or _FakeClient()

    async def _source_checker(cid, mid):
        return None

    inst = Responder(
        settings,
        bot,
        client=client,
        source_checker=_source_checker,
        jev_adapter=_FakeAdapter(),
        context_collector=_fake_collector,
        participation_ledger=_FakeLedger(),
    )
    return inst, settings, client


def _valid_entry(**overrides):
    base = {
        "type": "message",
        "channel_id": CHANNEL,
        "message_id": 700000000000000001,
        "author_uid": OTHER,
        "author_name": "Ramon",
        "actor_kind": "other",
        "content": "hello",
        "author_is_bot": False,
        "seq": 1,
    }
    base.update(overrides)
    return base


# --- 1: single-owner identity -------------------------------------------------


def test_canonical_identity_shared():
    assert responder_mod.ResponderError is input_mod.ResponderError
    assert recovery_mod.ResponderError is input_mod.ResponderError
    assert responder_mod._SAFE_TERMINAL is recovery_mod._SAFE_TERMINAL
    assert responder_mod._is_strict_int is input_mod._is_strict_int
    assert responder_mod._is_strict_positive_int is input_mod._is_strict_positive_int
    assert responder_mod._validate_id_string is input_mod._validate_id_string
    assert responder_mod._is_strict_ledger_id is input_mod._is_strict_ledger_id
    assert responder_mod._SAFE_TERMINAL == frozenset({"completed", "skipped", "observed"})
    # No orchestrator import cycle from the owned modules.
    assert not hasattr(input_mod, "Responder")
    assert not hasattr(recovery_mod, "Responder")


def test_strict_helpers_behavior():
    assert input_mod._is_strict_int(3) is True
    assert input_mod._is_strict_int(True) is False
    assert input_mod._is_strict_int(3.0) is False
    assert input_mod._is_strict_positive_int(1) is True
    assert input_mod._is_strict_positive_int(0) is False
    assert input_mod._is_strict_positive_int(False) is False
    assert input_mod._validate_id_string("abc-123") == "abc-123"
    assert input_mod._validate_id_string(" has-space") is None
    assert input_mod._validate_id_string("") is None
    assert input_mod._validate_id_string("a" * 129) is None
    assert input_mod._is_strict_ledger_id("ab" * 16) is True
    assert input_mod._is_strict_ledger_id("AB" * 16) is False
    assert input_mod._is_strict_ledger_id("short") is False


# --- 2: pure input equivalence -------------------------------------------------


def test_validate_entry_root_wrapper_matches_input(tmp_path):
    inst, _, _ = _responder(tmp_path)
    entry = _valid_entry()
    expected = input_mod.validate_entry(entry, bot_uid=BOT, owner_uid=OWNER)
    assert inst._validate_entry(entry, BOT) == expected
    assert expected == {
        "channel_id": CHANNEL,
        "message_id": 700000000000000001,
        "author_uid": OTHER,
        "seq": 1,
        "actor_kind": "other",
    }
    # Bad actor_kind contradicts UID classification in both paths.
    bad = _valid_entry(actor_kind="owner")
    with pytest.raises(input_mod.ResponderError, match="contradicts"):
        input_mod.validate_entry(bad, bot_uid=BOT, owner_uid=OWNER)
    with pytest.raises(responder_mod.ResponderError, match="contradicts"):
        inst._validate_entry(bad, BOT)
    # Bad author_is_bot type.
    bad2 = _valid_entry(author_is_bot="nope")
    with pytest.raises(input_mod.ResponderError, match="author_is_bot"):
        input_mod.validate_entry(bad2, bot_uid=BOT, owner_uid=OWNER)
    with pytest.raises(responder_mod.ResponderError, match="author_is_bot"):
        inst._validate_entry(bad2, BOT)


def test_historical_pairs_root_wrapper_matches_input(tmp_path):
    inst, _, _ = _responder(tmp_path)
    hist = {str(CHANNEL): [11, 12, -3, "x"], "bad": [1], str(CHANNEL + 1): "nope"}
    assert inst._historical_pairs(hist) == input_mod.historical_pairs(hist)
    assert input_mod.historical_pairs(hist) == {(CHANNEL, 11), (CHANNEL, 12)}
    assert inst._historical_pairs(None) == set()
    assert input_mod.historical_pairs(None) == set()


# --- 3: Recovery direct with injected fake client -------------------------------


def test_recovery_evidence_matches_pure():
    settings = SimpleNamespace(delivery_state_path="/tmp/fake.json", guild_id=GUILD)
    rec = ResponderRecovery(
        client=_FakeClient(),
        native_sid=FAKE_SID,
        settings=settings,
        mark_uncertain=lambda *a, **k: None,
        source_deleted_status=lambda *a, **k: None,
    )
    job = {"submitted_text_sha256": "aa", "submitted_text_len": 3}
    good = {
        "request_input_sha256": "aa",
        "request_input_len": 3,
        "status": "completed",
        "error_present": False,
        "trace_session_match": True,
        "turn_index_match": True,
        "request_user_count": 1,
    }
    assert rec.evidence_matches_job(job, good) is True
    bad = dict(good, request_user_count=2)
    assert rec.evidence_matches_job(job, bad) is False
    assert rec.evidence_matches_job({}, good) is False


async def test_recovery_direct_bound_mismatch_calls_callback(tmp_path):
    settings = _settings(tmp_path)
    _init_delivery(settings)
    mid = 710000000000000001
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=mid,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    text = "hello-turn"
    sha = hashlib.sha256(text.encode()).hexdigest()
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL,
        mid,
        job_ref="job-direct-1",
        pre_turn_index=None,
        submitted_text_sha256=sha,
        submitted_text_len=len(text),
    )
    mark_inflight(settings.delivery_state_path, CHANNEL, mid, turn_index=4)
    client = _FakeClient()

    async def _trace(sid, idx, job_ref, source_mid, cid):
        assert sid == FAKE_SID and idx == 4 and job_ref == "job-direct-1"
        return {
            "request_input_sha256": "00" * 32,  # mismatch
            "request_input_len": len(text),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
        }

    client.trace_fn = _trace
    marks: list = []

    def _mark(cid, mid_, *, kind, http_status=None):
        marks.append((cid, mid_, kind, http_status))

    rec = ResponderRecovery(
        client=client,
        native_sid=FAKE_SID,
        settings=settings,
        mark_uncertain=_mark,
        source_deleted_status=lambda cid, mid_: None,
    )
    out = await rec.reconcile_job({"channel_id": CHANNEL, "message_id": mid})
    assert out["status"] == "uncertain" and out["reason"] == "trace_mismatch"
    assert marks == [(CHANNEL, mid, "trace_unverified", None)]
    assert len(client.trace_calls) == 1


async def test_recovery_direct_source_deleted_wins(tmp_path):
    settings = _settings(tmp_path)
    _init_delivery(settings)
    mid = 720000000000000001
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=mid,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    client = _FakeClient()
    marks: list = []
    rec = ResponderRecovery(
        client=client,
        native_sid=FAKE_SID,
        settings=settings,
        mark_uncertain=lambda *a, **k: marks.append((a, k)),
        source_deleted_status=lambda cid, mid_: {"status": "skipped", "reason": "source_deleted"},
    )
    # Safe-terminal path reports the durable skip without trace calls.
    from delivery import skip_job

    skip_job(settings.delivery_state_path, CHANNEL, mid, reason="source_deleted")
    out = await rec.reconcile_job({"channel_id": CHANNEL, "message_id": mid})
    assert out == {"status": "skipped", "reason": "source_deleted"}
    assert marks == [] and client.trace_calls == []


@pytest.mark.parametrize("trace_outcome", ["error", "mismatch", "match", "cancel"])
@pytest.mark.parametrize("late_skip", [True, False])
async def test_bound_reconcile_late_source_deleted_status(
    tmp_path, monkeypatch, trace_outcome, late_skip
):
    import asyncio

    from thyca import ThycaProtocolError

    settings = _settings(tmp_path, typesafe_api_key=None)
    _init_delivery(settings)
    client = _FakeClient()

    async def source_checker(cid, mid):
        return None

    inst = Responder(
        settings,
        _FakeBot(),
        client=client,
        source_checker=source_checker,
        jev_adapter=_FakeAdapter(),
        context_collector=_fake_collector,
        participation_ledger=_FakeLedger(),
    )
    mid = 730000000000000001
    enqueue_job(
        settings.delivery_state_path,
        channel_id=CHANNEL,
        message_id=mid,
        author_uid=OTHER,
        actor_kind="other",
        seq=1,
    )
    text = "fake bound recovery frame"
    sha = hashlib.sha256(text.encode()).hexdigest()
    mark_turn_submitting(
        settings.delivery_state_path,
        CHANNEL,
        mid,
        job_ref="f" * 32,
        pre_turn_index=3,
        submitted_text_sha256=sha,
        submitted_text_len=len(text),
    )
    mark_inflight(settings.delivery_state_path, CHANNEL, mid, turn_index=4)
    marks = []
    original_mark = inst._mark_uncertain

    def mark(cid, mid_, **kwargs):
        marks.append((cid, mid_, kwargs))
        original_mark(cid, mid_, **kwargs)

    monkeypatch.setattr(inst, "_mark_uncertain", mark)
    skipped_bytes = None

    async def trace(sid, idx, job_ref, source_mid, cid):
        nonlocal skipped_bytes
        assert (sid, idx, job_ref, source_mid, cid) == (FAKE_SID, 4, "f" * 32, mid, CHANNEL)
        await asyncio.sleep(0)
        if late_skip and skipped_bytes is None:
            assert inst._skip_source_deleted(cid, source_mid) == {
                "status": "skipped",
                "reason": "source_deleted",
            }
            skipped_bytes = settings.delivery_state_path.read_bytes()
        if trace_outcome == "cancel":
            raise asyncio.CancelledError
        if trace_outcome == "error":
            raise ThycaProtocolError(reason="protocol")
        return {
            "request_input_sha256": sha if trace_outcome == "match" else "00" * 32,
            "request_input_len": len(text),
            "status": "completed",
            "error_present": False,
            "trace_session_match": True,
            "turn_index_match": True,
            "request_user_count": 1,
        }

    client.trace_fn = trace
    if trace_outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await inst._reconcile_job({"channel_id": CHANNEL, "message_id": mid})
    else:
        result = await inst._reconcile_job({"channel_id": CHANNEL, "message_id": mid})
        if late_skip:
            assert result == {"status": "skipped", "reason": "source_deleted"}
        else:
            reasons = {
                "error": "trace_unavailable",
                "mismatch": "trace_mismatch",
                "match": "sends_unconfirmed",
            }
            assert result == {"status": "uncertain", "reason": reasons[trace_outcome]}
    job = delivery.get_job(settings.delivery_state_path, CHANNEL, mid)
    prefix = delivery.processed_seq_prefix(
        settings.delivery_state_path,
        [{"channel_id": CHANNEL, "message_id": mid, "seq": 1}],
        0,
    )
    if late_skip:
        assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"
        assert settings.delivery_state_path.read_bytes() == skipped_bytes
        assert marks == []
        assert len(client.trace_calls) == 1
        assert prefix == 1
    else:
        expected_stage = "inflight" if trace_outcome in ("error", "cancel") else "uncertain"
        assert job["stage"] == expected_stage
        assert prefix == 0
    assert not job["send_intents"] and not job["send_receipts"] and not job["trace_refs"]
    assert job["jev_decision"] is None
    assert client.snapshot_calls == []


# --- 4: Root delegation + fresh lookup ------------------------------------------


async def test_root_forwards_five_to_recovery(tmp_path, monkeypatch):
    inst, _, _ = _responder(tmp_path)
    seen: dict = {}

    async def _fake_after(*a, **k):
        seen["after"] = (a, k)
        return {"status": "sentinel-after"}

    async def _fake_job(*a, **k):
        seen["job"] = (a, k)
        return {"status": "sentinel-job"}

    def _fake_ev(*a, **k):
        seen["ev"] = (a, k)
        return True

    async def _fake_sends(*a, **k):
        seen["sends"] = (a, k)
        return {"status": "sentinel-sends"}

    async def _fake_obs(*a, **k):
        seen["obs"] = (a, k)
        return {"status": "sentinel-obs"}

    monkeypatch.setattr(inst._recovery, "reconcile_after_post", _fake_after)
    monkeypatch.setattr(inst._recovery, "reconcile_job", _fake_job)
    monkeypatch.setattr(inst._recovery, "evidence_matches_job", _fake_ev)
    monkeypatch.setattr(inst._recovery, "verify_sends_and_complete", _fake_sends)
    monkeypatch.setattr(inst._recovery, "verify_observation_and_complete", _fake_obs)

    assert (await inst._reconcile_after_post(1, 2, None, "s", 1))["status"] == "sentinel-after"
    assert seen["after"] == ((1, 2, None, "s", 1), {})
    assert (await inst._reconcile_job({"a": 1}))["status"] == "sentinel-job"
    assert seen["job"] == (({"a": 1},), {})
    assert inst._evidence_matches_job({"j": 1}, {"e": 1}) is True
    assert seen["ev"] == (({"j": 1}, {"e": 1}), {})
    assert (await inst._verify_sends_and_complete(1, 2, 3))["status"] == "sentinel-sends"
    assert seen["sends"] == ((1, 2, 3), {})
    fresh = {"f": 1}
    ev = {"e": 1}
    assert (await inst._verify_observation_and_complete(1, 2, 3, fresh, ev))["status"] == (
        "sentinel-obs"
    )
    assert seen["obs"] == ((1, 2, 3, fresh, ev), {})


async def test_recovery_callbacks_use_fresh_root_lookup(tmp_path, monkeypatch):
    inst, _, _ = _responder(tmp_path)
    mark_seen: list = []
    status_seen: list = []

    def _patched_mark(cid, mid, *, kind, http_status=None):
        mark_seen.append((cid, mid, kind))

    def _patched_status(cid, mid):
        status_seen.append((cid, mid))
        return {"status": "skipped", "reason": "source_deleted"}

    monkeypatch.setattr(inst, "_mark_uncertain", _patched_mark)
    monkeypatch.setattr(inst, "_source_deleted_status", _patched_status)
    # Recovery holds lambdas, not bound copies: late patches dispatch.
    inst._recovery._mark_uncertain(CHANNEL, 999, kind="trace_unverified")
    assert mark_seen == [(CHANNEL, 999, "trace_unverified")]
    assert inst._recovery._source_deleted_status(CHANNEL, 999) == {
        "status": "skipped",
        "reason": "source_deleted",
    }
    assert status_seen == [(CHANNEL, 999)]


# --- 5: constructor bind last + no-IO recovery -----------------------------------


def test_constructor_bind_last_after_ledger_and_recovery_no_io(tmp_path, monkeypatch):
    from participation import ParticipationLedger

    settings = _settings(tmp_path)
    _init_delivery(settings)
    order: list = []
    orig_bind = delivery.bind_jev_ledger
    orig_recovery_init = ResponderRecovery.__init__

    def _spy_bind(path, *, ledger_id):
        order.append("bind")
        return orig_bind(path, ledger_id=ledger_id)

    def _spy_recovery_init(self, **kwargs):
        order.append("recovery")
        # No I/O: tmp dir listing + delivery bytes unchanged across init.
        before_files = sorted(os.listdir(tmp_path))
        before_delivery = settings.delivery_state_path.read_bytes()
        orig_recovery_init(self, **kwargs)
        assert sorted(os.listdir(tmp_path)) == before_files
        assert settings.delivery_state_path.read_bytes() == before_delivery

    monkeypatch.setattr("responder.delivery.bind_jev_ledger", _spy_bind)
    monkeypatch.setattr("responder.ResponderRecovery.__init__", _spy_recovery_init)
    bot = _FakeBot()
    client = _FakeClient()

    async def _source_checker(cid, mid):
        return None

    inst = Responder(
        settings,
        bot,
        client=client,
        source_checker=_source_checker,
        jev_adapter=_FakeAdapter(),
        context_collector=_fake_collector,
        participation_ledger=ParticipationLedger(settings.jev_state_path, native_sid=FAKE_SID),
    )
    assert order == ["recovery", "bind"]
    assert isinstance(inst._recovery, ResponderRecovery)
    assert inst._recovery._client is client
    assert inst._recovery._native_sid == FAKE_SID
    assert inst._recovery._settings is settings
    marker = delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"]
    assert marker == inst._participation_ledger.ledger_id
    # Exact repeat binds idempotently (no rewrite, no conflict).
    before = settings.delivery_state_path.read_bytes()
    inst2 = Responder(
        settings,
        bot,
        client=_FakeClient(),
        source_checker=_source_checker,
        jev_adapter=_FakeAdapter(),
        context_collector=_fake_collector,
        participation_ledger=ParticipationLedger(settings.jev_state_path, native_sid=FAKE_SID),
    )
    assert settings.delivery_state_path.read_bytes() == before
    assert delivery.load_delivery_state(settings.delivery_state_path)["jev_ledger_id"] == marker
    assert inst2._recovery._native_sid == FAKE_SID
