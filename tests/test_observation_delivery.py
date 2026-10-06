"""Observation delivery tests: Jev observe + Thyca no-reply zero-send terminals.

Offline file-state only (tmp paths, fake IDs). No network, env, or native.
Covers bind/observe/declare/complete, foreign nonces, legacy sends,
zero-evidence, duplicates, unknown fields, legacy files, SID, and corrupt
observed tombstones.
"""

import hashlib
import json

import pytest

from delivery import (
    DeliveryError,
    bind_jev_decision,
    bind_jev_ledger,
    complete_job,
    complete_observation,
    enqueue_job,
    get_job,
    initialize_delivery_state,
    load_delivery_state,
    mark_inflight,
    mark_turn_submitting,
    mark_uncertain,
    observe_job,
    processed_seq_prefix,
    record_no_reply,
    record_send_intent,
    record_send_receipt,
    requeue_definitive_busy,
    skip_job,
)

FAKE_SID = "2026-01-01T00-00-00_aaaa"
CHANNEL = 7
AUTHOR = 2
HASH_A = "ab" * 32
HASH_B = "cd" * 32
CTX = "11" * 32
CTX2 = "22" * 32
ATTEMPT = "aa" * 16
ATTEMPT2 = "bb" * 16
MODEL = "jev-1.13.0"
# Synthetic first-user proof for NEW thyca-observed terminal ONLY.
# Real Router POSTs persist the exact prompt.strip SHA-256 + char len;
# legacy completed/skipped/uncertain without a declaration keep None
# compat, and Jev-observed (no POST) must keep None. This NEW terminal
# has no legacy compat need: missing/zero/invalid binding never
# declares, terminals, or ACKs.
FP_TEXT = "synthetic need_context prompt [observation proof]"
FP_SHA = hashlib.sha256(FP_TEXT.encode("utf-8")).hexdigest()
FP_LEN = len(FP_TEXT)


def _init(tmp_path, **kw):
    path = tmp_path / kw.pop("name", "delivery_state.json")
    initialize_delivery_state(
        path,
        native_session_id=kw.pop("sid", FAKE_SID),
        baseline_acked=kw.pop("baseline_acked", 0),
        historical_message_ids=kw.pop("historical", {}),
        authorized_skips=kw.pop("skips", []),
    )
    return path


def _enq(path, mid=100, seq=1, ch=CHANNEL):
    return enqueue_job(
        path, channel_id=ch, message_id=mid, author_uid=AUTHOR, actor_kind="owner", seq=seq
    )


def _bind(path, ch, mid, *, participation="observe", ctx=CTX, attempt=ATTEMPT, model=MODEL):
    return bind_jev_decision(
        path,
        ch,
        mid,
        context_sha256=ctx,
        attempt_id=attempt,
        model=model,
        participation=participation,
    )


def _jump(mid, ch=CHANNEL):
    return f"https://discord.com/channels/1/{ch}/{mid}"


# ---- bind_jev_decision ----


def test_bind_happy_and_idempotent_no_write(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    job = _bind(path, CHANNEL, 100, participation="observe")
    assert job["stage"] == "queued"
    assert job["jev_decision"] == {
        "participation": "observe",
        "context_sha256": CTX,
        "attempt_id": ATTEMPT,
        "model": MODEL,
    }
    assert job["job_ref"] is None
    assert job["observation"] is None
    assert job["no_reply_declaration"] is None
    before = path.read_bytes()
    same = _bind(path, CHANNEL, 100, participation="observe")
    assert same == job
    assert path.read_bytes() == before


def test_bind_conflict_on_any_field_difference(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="jev decision conflict"):
        _bind(path, CHANNEL, 100, participation="respond")
    with pytest.raises(DeliveryError, match="jev decision conflict"):
        _bind(path, CHANNEL, 100, participation="observe", ctx=CTX2)
    with pytest.raises(DeliveryError, match="jev decision conflict"):
        _bind(path, CHANNEL, 100, participation="observe", attempt=ATTEMPT2)
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "kw,match",
    [
        ({"ctx": "ab" * 31}, "invalid context_sha256"),
        ({"ctx": "ab" * 33}, "invalid context_sha256"),
        ({"ctx": "zz" * 32}, "invalid context_sha256"),
        ({"ctx": None}, "invalid context_sha256"),
        ({"ctx": True}, "invalid context_sha256"),
        ({"attempt": "aa" * 15}, "invalid attempt_id"),
        ({"attempt": "aa" * 17}, "invalid attempt_id"),
        ({"attempt": "zz" * 16}, "invalid attempt_id"),
        ({"attempt": "aa-bb-cc"}, "invalid attempt_id"),
        ({"attempt": None}, "invalid attempt_id"),
        ({"model": "jev-9.9.9"}, "invalid jev model"),
        ({"model": "jev-latest"}, "invalid jev model"),
        ({"model": ""}, "invalid jev model"),
        ({"model": None}, "invalid jev model"),
        ({"participation": "OBSERVE"}, "invalid jev participation"),
        ({"participation": "ignore"}, "invalid jev participation"),
        ({"participation": ""}, "invalid jev participation"),
        ({"participation": None}, "invalid jev participation"),
        ({"participation": True}, "invalid jev participation"),
    ],
)
def test_bind_invalid_inputs_fail_closed(tmp_path, kw, match):
    path = _init(tmp_path)
    _enq(path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match=match):
        _bind(path, CHANNEL, 100, **kw)
    assert path.read_bytes() == before
    assert get_job(path, CHANNEL, 100)["jev_decision"] is None


def test_bind_only_fresh_queued(tmp_path):
    path = _init(tmp_path)
    _enq(path, mid=100, seq=1)
    _enq(path, mid=101, seq=2)
    # Non-queued stages refuse.
    mark_turn_submitting(path, CHANNEL, 101, job_ref="jobref-101", pre_turn_index=0)
    with pytest.raises(DeliveryError, match="cannot bind jev"):
        _bind(path, CHANNEL, 101)
    mark_inflight(path, CHANNEL, 101, turn_index=0)
    with pytest.raises(DeliveryError, match="cannot bind jev"):
        _bind(path, CHANNEL, 101)
    record_send_intent(path, CHANNEL, 101, send_ref="s1", content_len=5, content_hash=HASH_A)
    with pytest.raises(DeliveryError, match="cannot bind jev"):
        _bind(path, CHANNEL, 101)
    # Queued stays bindable.
    job = _bind(path, CHANNEL, 100)
    assert job["jev_decision"]["participation"] == "observe"


def test_bind_no_raw_answers_persisted(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    canary = "CANARY_RAW_ANSWER_SECRET"
    # Even if a caller mistakenly passes extra data, only compact fields store.
    # Here we prove the stored decision has exactly the 4 compact keys.
    job = _bind(path, CHANNEL, 100, participation="need_context")
    assert set(job["jev_decision"]) == {"participation", "context_sha256", "attempt_id", "model"}
    assert canary not in path.read_text(encoding="utf-8")


# ---- observe_job (Jev direct) ----


def test_observe_happy_idempotent_and_acks(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    job = observe_job(path, CHANNEL, 100)
    assert job["stage"] == "observed"
    assert job["observation"]["origin"] == "jev"
    assert job["observation"]["ts"]
    assert job["jev_decision"]["participation"] == "observe"
    assert job["send_intents"] == [] and job["send_receipts"] == []
    assert job["trace_refs"] is None and job["error"] is None
    before = path.read_bytes()
    same = observe_job(path, CHANNEL, 100)
    assert same == job
    assert path.read_bytes() == before
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_observe_requires_bound_observe(tmp_path):
    path = _init(tmp_path)
    _enq(path, mid=100, seq=1)
    _enq(path, mid=101, seq=2)
    _enq(path, mid=102, seq=3)
    _bind(path, CHANNEL, 101, participation="respond")
    _bind(path, CHANNEL, 102, participation="need_context")
    for mid in (100, 101, 102):
        with pytest.raises(DeliveryError, match="no jev observe proof"):
            observe_job(path, CHANNEL, mid)
    # No terminal, no ACK.
    for mid in (100, 101, 102):
        assert get_job(path, CHANNEL, mid)["stage"] == "queued"
    assert (
        processed_seq_prefix(
            path,
            [
                {"channel_id": CHANNEL, "message_id": 100, "seq": 1},
                {"channel_id": CHANNEL, "message_id": 101, "seq": 2},
            ],
            0,
        )
        == 0
    )


def test_observe_only_queued_no_retro_uncertain(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    with pytest.raises(DeliveryError, match="cannot observe"):
        observe_job(path, CHANNEL, 100)
    mark_uncertain(path, CHANNEL, 100, kind="timeout")
    with pytest.raises(DeliveryError, match="cannot observe"):
        observe_job(path, CHANNEL, 100)
    assert get_job(path, CHANNEL, 100)["stage"] == "uncertain"
    # Uncertain never ACKs.
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 0
    )


def test_observed_allows_alias_backfill_like_terminals(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    observe_job(path, CHANNEL, 100)
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=9
    )
    assert job["stage"] == "observed"
    assert job["seq_aliases"] == [9]
    assert job["observation"]["origin"] == "jev"


def test_observed_terminal_never_rewinds(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    observe_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="cannot skip"):
        skip_job(path, CHANNEL, 100, reason="operator_approved_drop")
    with pytest.raises(DeliveryError, match="cannot mark uncertain"):
        mark_uncertain(path, CHANNEL, 100, kind="timeout")
    with pytest.raises(DeliveryError, match="is terminal"):
        record_send_intent(path, CHANNEL, 100, send_ref="s1", content_len=5, content_hash=HASH_A)
    with pytest.raises(DeliveryError, match="is terminal"):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref="s1",
            sent_message_id=9101,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(9101),
            reply_to_message_id=100,
        )
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["c1"])
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 100, turn_index=0)


# ---- record_no_reply (midflight declaration, no terminal) ----


def test_declare_happy_immutable_no_ack(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    job = record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert job["stage"] == "turn_submitting"
    assert job["no_reply_declaration"]["job_ref"] == "jobref-001"
    assert job["no_reply_declaration"]["reason"] == "missing_context"
    assert job["no_reply_declaration"]["ts"]
    before = path.read_bytes()
    same = record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert same == job
    assert path.read_bytes() == before
    # Declaration alone never ACKs.
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 0
    )
    # Works from inflight too (same nonce).
    mark_inflight(path, CHANNEL, 100, turn_index=3)
    assert get_job(path, CHANNEL, 100)["no_reply_declaration"]["reason"] == "missing_context"


def test_declare_conflict_and_invalid_reason(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply conflict"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="no_reply_needed")
    with pytest.raises(DeliveryError, match="no_reply conflict"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-002", reason="missing_context")
    assert path.read_bytes() == before
    _enq(path, mid=101, seq=2)
    _bind(path, CHANNEL, 101, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        101,
        job_ref="jobref-101",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    for bad in ("", "observe", "MISSING_CONTEXT", None, True, [], "no_reply_needed "):
        with pytest.raises(DeliveryError, match="invalid no_reply reason"):
            record_no_reply(path, CHANNEL, 101, job_ref="jobref-101", reason=bad)


def test_declare_requires_need_context_exact_nonce_zero_sends(tmp_path):
    path = _init(tmp_path)
    # No decision.
    _enq(path, mid=100, seq=1)
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-100",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    with pytest.raises(DeliveryError, match="no need_context proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="missing_context")
    # Wrong participation.
    _enq(path, mid=101, seq=2)
    _bind(path, CHANNEL, 101, participation="respond")
    mark_turn_submitting(
        path,
        CHANNEL,
        101,
        job_ref="jobref-101",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    with pytest.raises(DeliveryError, match="no need_context proof"):
        record_no_reply(path, CHANNEL, 101, job_ref="jobref-101", reason="missing_context")
    # Foreign nonce.
    _enq(path, mid=102, seq=3)
    _bind(path, CHANNEL, 102, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        102,
        job_ref="jobref-102",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    with pytest.raises(DeliveryError, match="job_ref mismatch"):
        record_no_reply(path, CHANNEL, 102, job_ref="foreign-999", reason="missing_context")
    # With sends.
    _enq(path, mid=103, seq=4)
    _bind(path, CHANNEL, 103, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        103,
        job_ref="jobref-103",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_send_intent(path, CHANNEL, 103, send_ref="s1", content_len=5, content_hash=HASH_A)
    with pytest.raises(DeliveryError, match="cannot declare no_reply"):
        record_no_reply(path, CHANNEL, 103, job_ref="jobref-103", reason="missing_context")


def test_declare_only_midflight(tmp_path):
    path = _init(tmp_path)
    _enq(path, mid=100, seq=1)
    _bind(path, CHANNEL, 100, participation="need_context")
    with pytest.raises(DeliveryError, match="cannot declare no_reply"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="missing_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-100",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="no_reply_needed")
    mark_uncertain(path, CHANNEL, 100, kind="timeout")
    # Uncertain with a declaration keeps it, but a second declare conflicts/terminal.
    with pytest.raises(DeliveryError, match="no_reply conflict|cannot declare"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="missing_context")


def test_send_intent_forbidden_after_declare(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="no_reply_needed")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply proof; cannot send"):
        record_send_intent(path, CHANNEL, 100, send_ref="s1", content_len=5, content_hash=HASH_A)
    assert path.read_bytes() == before


def test_requeue_rejects_with_declaration_preserves_decision(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply proof; cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 100)
    assert path.read_bytes() == before
    # Without a declaration, requeue preserves the durable decision.
    path2 = _init(tmp_path, name="r2.json")
    _enq(path2)
    _bind(path2, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path2,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=1,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    job = requeue_definitive_busy(path2, CHANNEL, 100)
    assert job["stage"] == "queued"
    assert job["job_ref"] is None
    assert job["jev_decision"]["participation"] == "need_context"
    # A new attempt reuses the preserved decision with a fresh nonce.
    mark_turn_submitting(
        path2,
        CHANNEL,
        100,
        job_ref="jobref-002",
        pre_turn_index=1,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    declared = record_no_reply(path2, CHANNEL, 100, job_ref="jobref-002", reason="no_reply_needed")
    assert declared["no_reply_declaration"]["job_ref"] == "jobref-002"


# ---- complete_observation (Thyca traced no-reply) ----


def test_complete_observation_happy_idempotent_and_acks(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=4)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="no_reply_needed")
    job = complete_observation(path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-a", "call-b"])
    assert job["stage"] == "observed"
    assert job["observation"]["origin"] == "thyca"
    assert job["trace_refs"]["tool_call_ids"] == ["call-a", "call-b"]
    assert job["no_reply_declaration"]["job_ref"] == "jobref-001"
    assert job["error"] is None
    before = path.read_bytes()
    same = complete_observation(
        path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-a", "call-b"]
    )
    assert same == job
    assert path.read_bytes() == before
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_complete_observation_mismatch_and_bad_ids(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=4)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="turn_index mismatch"):
        complete_observation(path, CHANNEL, 100, turn_index=5, tool_call_ids=["call-a"])
    for bad_ids in ([], ["dup", "dup"], [" bad"], [""], [True], ["x" * 129], "call-a", None):
        with pytest.raises(DeliveryError, match="invalid tool_call_ids|invalid tool_call_id"):
            complete_observation(path, CHANNEL, 100, turn_index=4, tool_call_ids=bad_ids)
    assert path.read_bytes() == before
    job = complete_observation(path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-a"])
    assert job["stage"] == "observed"
    with pytest.raises(DeliveryError, match="observed payload mismatch"):
        complete_observation(path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-b"])
    with pytest.raises(DeliveryError, match="observed payload mismatch"):
        complete_observation(path, CHANNEL, 100, turn_index=9, tool_call_ids=["call-a"])


def test_complete_observation_requires_declaration_and_zero_evidence(tmp_path):
    path = _init(tmp_path)
    # No declaration.
    _enq(path, mid=100, seq=1)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-100",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=1)
    with pytest.raises(DeliveryError, match="no no_reply proof"):
        complete_observation(path, CHANNEL, 100, turn_index=1, tool_call_ids=["c1"])
    # Wrong participation.
    _enq(path, mid=101, seq=2)
    _bind(path, CHANNEL, 101, participation="respond")
    mark_turn_submitting(
        path,
        CHANNEL,
        101,
        job_ref="jobref-101",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 101, turn_index=1)
    with pytest.raises(DeliveryError, match="no need_context proof"):
        complete_observation(path, CHANNEL, 101, turn_index=1, tool_call_ids=["c1"])
    # turn_submitting (unbound index) cannot complete.
    _enq(path, mid=102, seq=3)
    _bind(path, CHANNEL, 102, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        102,
        job_ref="jobref-102",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 102, job_ref="jobref-102", reason="missing_context")
    with pytest.raises(DeliveryError, match="cannot complete observation"):
        complete_observation(path, CHANNEL, 102, turn_index=0, tool_call_ids=["c1"])


def test_complete_observation_uncertain_reconcile_only_exact_proof(tmp_path):
    path = _init(tmp_path)
    # Zero-evidence uncertain WITH exact declaration reconciles.
    _enq(path, mid=100, seq=1)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-100",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=2)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="missing_context")
    mark_uncertain(path, CHANNEL, 100, kind="timeout")
    job = complete_observation(path, CHANNEL, 100, turn_index=2, tool_call_ids=["call-z"])
    assert job["stage"] == "observed"
    assert job["error"] is None
    assert job["observation"]["origin"] == "thyca"
    # Ambiguous uncertain WITHOUT declaration is never touched.
    _enq(path, mid=101, seq=2)
    mark_turn_submitting(path, CHANNEL, 101, job_ref="jobref-101", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 101, turn_index=2)
    record_send_intent(path, CHANNEL, 101, send_ref="s1", content_len=5, content_hash=HASH_A)
    mark_uncertain(path, CHANNEL, 101, kind="timeout")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no need_context proof|no no_reply proof"):
        complete_observation(path, CHANNEL, 101, turn_index=2, tool_call_ids=["call-z"])
    assert path.read_bytes() == before
    assert get_job(path, CHANNEL, 101)["stage"] == "uncertain"
    # Zero-evidence uncertain without declaration also refuses.
    _enq(path, mid=102, seq=3)
    _bind(path, CHANNEL, 102, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        102,
        job_ref="jobref-102",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 102, turn_index=2)
    mark_uncertain(path, CHANNEL, 102, kind="timeout")
    with pytest.raises(DeliveryError, match="no no_reply proof"):
        complete_observation(path, CHANNEL, 102, turn_index=2, tool_call_ids=["call-z"])


def test_legacy_sends_untouched_by_observation(tmp_path):
    path = _init(tmp_path)
    # Classic send flow without any Jev fields still completes and ACKs.
    _enq(path, mid=100, seq=1)
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    record_send_intent(path, CHANNEL, 100, send_ref="s1", content_len=5, content_hash=HASH_A)
    record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="s1",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    done = complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-1"])
    assert done["stage"] == "completed"
    assert done["jev_decision"] is None
    assert done["observation"] is None
    with pytest.raises(DeliveryError, match="cannot observe|cannot complete observation"):
        observe_job(path, CHANNEL, 100)
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


# ---- skip / prefix compat ----


def test_skip_source_deleted_with_decision_and_declaration(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    job = skip_job(path, CHANNEL, 100, reason="source_deleted")
    assert job["stage"] == "skipped"
    assert job["jev_decision"]["participation"] == "need_context"
    assert job["no_reply_declaration"]["reason"] == "missing_context"
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_prefix_observed_gaps_and_unresolved_stop(tmp_path):
    path = _init(tmp_path)
    _enq(path, mid=100, seq=1)
    _bind(path, CHANNEL, 100, participation="observe")
    observe_job(path, CHANNEL, 100)
    _enq(path, mid=101, seq=5)
    _bind(path, CHANNEL, 101, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        101,
        job_ref="jobref-101",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 101, turn_index=0)
    record_no_reply(path, CHANNEL, 101, job_ref="jobref-101", reason="no_reply_needed")
    complete_observation(path, CHANNEL, 101, turn_index=0, tool_call_ids=["c101"])
    _enq(path, mid=102, seq=6)
    assert (
        processed_seq_prefix(
            path,
            [
                {"channel_id": CHANNEL, "message_id": 100, "seq": 1},
                {"channel_id": CHANNEL, "message_id": 101, "seq": 5},
            ],
            0,
        )
        == 5
    )
    assert (
        processed_seq_prefix(
            path,
            [
                {"channel_id": CHANNEL, "message_id": 100, "seq": 1},
                {"channel_id": CHANNEL, "message_id": 101, "seq": 5},
                {"channel_id": CHANNEL, "message_id": 102, "seq": 6},
            ],
            0,
        )
        == 5
    )


# ---- unknown fields / legacy files / SID ----


def test_unknown_fields_preserved_across_observation(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["_future_top"] = {"keep": 1}
    raw["jobs"]["7:100"]["_future_job"] = "keep-job"
    raw["jobs"]["7:100"]["jev_decision"]["_future_jev"] = "keep-jev"
    path.write_text(json.dumps(raw), encoding="utf-8")
    job = observe_job(path, CHANNEL, 100)
    assert job["_future_job"] == "keep-job"
    assert job["jev_decision"]["_future_jev"] == "keep-jev"
    assert job["observation"]["origin"] == "jev"
    raw2 = json.loads(path.read_text(encoding="utf-8"))
    raw2["jobs"]["7:100"]["observation"]["_future_obs"] = 7
    raw2["jobs"]["7:100"]["no_reply_declaration"] = None
    path.write_text(json.dumps(raw2), encoding="utf-8")
    assert load_delivery_state(path)["jobs"]["7:100"]["observation"]["_future_obs"] == 7
    assert load_delivery_state(path)["_future_top"] == {"keep": 1}


def test_legacy_file_without_observation_keys_loads_as_none(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    raw = json.loads(path.read_text(encoding="utf-8"))
    for key in ("jev_decision", "no_reply_declaration", "observation"):
        del raw["jobs"]["7:100"][key]
    path.write_text(json.dumps(raw), encoding="utf-8")
    job = get_job(path, CHANNEL, 100)
    assert job["jev_decision"] is None
    assert job["no_reply_declaration"] is None
    assert job["observation"] is None
    # Legacy job can still bind fresh (queued only) after requeue.
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    record_send_intent(path, CHANNEL, 100, send_ref="s1", content_len=5, content_hash=HASH_A)
    assert get_job(path, CHANNEL, 100)["stage"] == "delivering"


def test_sid_still_authoritative_with_observation(tmp_path):
    path = _init(tmp_path, sid=FAKE_SID)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    observe_job(path, CHANNEL, 100)
    assert load_delivery_state(path)["native_session_id"] == FAKE_SID
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["native_session_id"] = "bad sid"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="invalid native_session_id"):
        load_delivery_state(path)


# ---- corrupt observed tombstones fail closed ----


def _corrupt_job(tmp_path, name, mutate):
    path = _init(tmp_path, name=name)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    observe_job(path, CHANNEL, 100)
    raw = json.loads(path.read_text(encoding="utf-8"))
    mutate(raw["jobs"]["7:100"])
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


def test_corrupt_observed_with_send_evidence_fails_load(tmp_path):
    def _add_intent(job):
        job["send_intents"] = [
            {"send_ref": "s1", "content_len": 5, "content_hash": HASH_A, "ts": "t"}
        ]

    bad = _corrupt_job(tmp_path, "o1.json", _add_intent)
    with pytest.raises(DeliveryError, match="observed needs zero evidence"):
        load_delivery_state(bad)
    with pytest.raises(DeliveryError, match="observed needs zero evidence"):
        processed_seq_prefix(bad, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)


def test_corrupt_observed_missing_or_wrong_proof_fails_load(tmp_path):
    cases = [
        lambda j: j.update(observation=None),
        lambda j: j.update(jev_decision=None),
        lambda j: j.update(observation={"origin": "thyca", "ts": "t"}),
        lambda j: j["jev_decision"].update(participation="respond"),
        lambda j: j.update(job_ref="jobref-001"),
        lambda j: j.update(trace_refs={"tool_call_ids": ["c1"], "ts": "t"}),
        lambda j: j.update(
            no_reply_declaration={"job_ref": "jobref-001", "reason": "missing_context", "ts": "t"}
        ),
        lambda j: j.update(observation={"origin": "nope", "ts": "t"}),
        lambda j: j.update(observation={"origin": "jev"}),
        lambda j: j.update(jev_decision={"participation": "observe"}),
        lambda j: j["jev_decision"].update(model="jev-9.9.9"),
        lambda j: j["jev_decision"].update(attempt_id="short"),
        lambda j: j["jev_decision"].update(context_sha256="zz" * 32),
    ]
    for i, mutate in enumerate(cases):
        bad = _corrupt_job(tmp_path, f"oc{i}.json", mutate)
        with pytest.raises(DeliveryError, match="corrupt delivery state"):
            load_delivery_state(bad)


def test_corrupt_observed_thyca_needs_unique_trace_and_declaration(tmp_path):
    path = _init(tmp_path, name="thy.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=1)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    complete_observation(path, CHANNEL, 100, turn_index=1, tool_call_ids=["c1", "c2"])
    good = path.read_bytes()

    def _check(mutate):
        raw = json.loads(good)
        mutate(raw["jobs"]["7:100"])
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(DeliveryError, match="corrupt delivery state"):
            load_delivery_state(path)
        path.write_bytes(good)

    _check(lambda j: j.update(trace_refs=None))
    _check(lambda j: j.update(trace_refs={"tool_call_ids": [], "ts": "t"}))
    _check(lambda j: j.update(trace_refs={"tool_call_ids": ["d", "d"], "ts": "t"}))
    _check(lambda j: j.update(no_reply_declaration=None))
    _check(lambda j: j["no_reply_declaration"].update(job_ref="foreign"))
    _check(lambda j: j["jev_decision"].update(participation="observe"))
    _check(lambda j: j.update(turn_index=None))
    _check(lambda j: j.update(job_ref=None))
    _check(lambda j: j.update(observation={"origin": "jev", "ts": "t"}))
    assert load_delivery_state(path)["jobs"]["7:100"]["stage"] == "observed"


def test_corrupt_midflight_declaration_mismatch_fails_load(tmp_path):
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    good = path.read_bytes()
    raw = json.loads(good)
    raw["jobs"]["7:100"]["no_reply_declaration"]["job_ref"] = "foreign"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="no_reply needs need_context"):
        load_delivery_state(path)
    raw = json.loads(good)
    raw["jobs"]["7:100"]["jev_decision"]["participation"] = "respond"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="no_reply needs need_context"):
        load_delivery_state(path)
    path.write_bytes(good)
    assert get_job(path, CHANNEL, 100)["stage"] == "turn_submitting"


# ---- NEW thyca-observed requires stored first-user fingerprint ----


def test_declare_requires_known_fingerprint_no_terminal(tmp_path):
    # Missing binding can never declare, terminalize, or ACK. All real
    # Router POSTs persist the exact prompt.strip SHA-256 + char len.
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="has no submitted proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert path.read_bytes() == before
    assert get_job(path, CHANNEL, 100)["stage"] == "turn_submitting"
    assert get_job(path, CHANNEL, 100)["no_reply_declaration"] is None
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 0
    )
    # Same job with a known fingerprint declares (fresh file for clarity).
    path2 = _init(tmp_path, name="fp2.json")
    _enq(path2)
    _bind(path2, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path2,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    job = record_no_reply(path2, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert job["no_reply_declaration"]["reason"] == "missing_context"
    assert job["submitted_text_sha256"] == FP_SHA
    assert job["submitted_text_len"] == FP_LEN


def test_complete_observation_requires_known_fingerprint(tmp_path):
    # Forged midflight decl without binding (direct file edit to bypass
    # the declare guard) is corrupt load: any non-null declaration is NEW
    # proof and requires the stored fingerprint. Never loads/completes/ACKs.
    path = _init(tmp_path)
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=2)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    # Strip the binding, keep the declaration (forged NEW proof shape).
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:100"]["submitted_text_sha256"] = None
    raw["jobs"]["7:100"]["submitted_text_len"] = None
    path.write_text(json.dumps(raw), encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        complete_observation(path, CHANNEL, 100, turn_index=2, tool_call_ids=["call-z"])
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        get_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    assert path.read_bytes() == before
    # Zero/invalid binding is corrupt load (never ACKs), not a silent None.
    raw2 = json.loads(before)
    raw2["jobs"]["7:100"]["submitted_text_sha256"] = FP_SHA
    raw2["jobs"]["7:100"]["submitted_text_len"] = 0
    path.write_text(json.dumps(raw2), encoding="utf-8")
    with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)


def test_forged_observed_thyca_missing_fingerprint_fails_load_no_ack(tmp_path):
    path = _init(tmp_path, name="forged.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=1)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    complete_observation(path, CHANNEL, 100, turn_index=1, tool_call_ids=["c1", "c2"])
    good = path.read_bytes()
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )
    # Missing binding: forged tombstone never loads/ACKs (decl binding fails first).
    raw = json.loads(good)
    raw["jobs"]["7:100"]["submitted_text_sha256"] = None
    raw["jobs"]["7:100"]["submitted_text_len"] = None
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    # Zero binding: same fail-closed.
    raw = json.loads(good)
    raw["jobs"]["7:100"]["submitted_text_len"] = 0
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)
    # Invalid SHA: corrupt load, never ACKs.
    raw = json.loads(good)
    raw["jobs"]["7:100"]["submitted_text_sha256"] = "zz" * 32
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)
    path.write_bytes(good)
    assert load_delivery_state(path)["jobs"]["7:100"]["stage"] == "observed"
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_valid_fingerprint_allows_thyca_observed_and_acks(tmp_path):
    # Explicit positive: known binding + exact decl + unique trace IDs
    # terminals and ACKs; Jev-observed (no POST) still keeps None pair.
    path = _init(tmp_path, name="valid.json")
    _enq(path, mid=100, seq=1)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-100",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-100", reason="no_reply_needed")
    job = complete_observation(path, CHANNEL, 100, turn_index=0, tool_call_ids=["c-valid"])
    assert job["stage"] == "observed"
    assert job["observation"]["origin"] == "thyca"
    assert job["submitted_text_sha256"] == FP_SHA
    assert job["submitted_text_len"] == FP_LEN
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )
    # Jev-observed keeps None pair (no POST).
    _enq(path, mid=101, seq=2)
    _bind(path, CHANNEL, 101, participation="observe")
    j2 = observe_job(path, CHANNEL, 101)
    assert j2["observation"]["origin"] == "jev"
    assert j2["submitted_text_sha256"] is None
    assert j2["submitted_text_len"] is None


# ---- T3 jev_ledger_id activation marker (top-level, optional) ----

LEDGER_A = "cc" * 16
LEDGER_B = "dd" * 16


def test_ledger_legacy_missing_loads_none_and_flows_work(tmp_path):
    # Old files missing the key load as None; standalone Core flows do
    # NOT require the marker (Router binds globally before classification).
    path = _init(tmp_path, name="legacy-ledger.json")
    assert load_delivery_state(path)["jev_ledger_id"] is None
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["jev_ledger_id"] is None
    del raw["jev_ledger_id"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert load_delivery_state(path)["jev_ledger_id"] is None
    # Core decision APIs still work without a marker.
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    job = observe_job(path, CHANNEL, 100)
    assert job["stage"] == "observed"
    assert load_delivery_state(path)["jev_ledger_id"] is None
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_ledger_bind_happy_idempotent_no_rewrite(tmp_path):
    path = _init(tmp_path, name="ledger.json")
    _enq(path)
    sid_before = load_delivery_state(path)["native_session_id"]
    snap = bind_jev_ledger(path, ledger_id=LEDGER_A)
    assert snap["jev_ledger_id"] == LEDGER_A
    assert snap["native_session_id"] == sid_before
    assert load_delivery_state(path)["jev_ledger_id"] == LEDGER_A
    # Pre/post: file valid, no temp orphans, SID/jobs/ACK untouched.
    assert list(tmp_path.glob("*.tmp.*")) == []
    assert load_delivery_state(path)["native_session_id"] == sid_before
    assert get_job(path, CHANNEL, 100)["stage"] == "queued"
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 0
    )
    before = path.read_bytes()
    same = bind_jev_ledger(path, ledger_id=LEDGER_A)
    assert same == snap
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_ledger_bind_conflict_no_overwrite(tmp_path):
    path = _init(tmp_path, name="conflict.json")
    bind_jev_ledger(path, ledger_id=LEDGER_A)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="ledger conflict"):
        bind_jev_ledger(path, ledger_id=LEDGER_B)
    assert path.read_bytes() == before
    assert load_delivery_state(path)["jev_ledger_id"] == LEDGER_A
    assert list(tmp_path.glob("*.tmp.*")) == []
    # Exact match is case-sensitive: same hex different case conflicts.
    with pytest.raises(DeliveryError, match="ledger conflict"):
        bind_jev_ledger(path, ledger_id=LEDGER_A.upper())
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "bad",
    ["", "cc" * 15, "cc" * 17, "zz" * 16, "cc-dd-ee", None, True, 123, [], {}, " cc" * 8],
)
def test_ledger_bind_invalid_inputs_fail_closed(tmp_path, bad):
    path = _init(tmp_path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="invalid ledger_id"):
        bind_jev_ledger(path, ledger_id=bad)
    assert path.read_bytes() == before
    assert load_delivery_state(path)["jev_ledger_id"] is None


def test_ledger_bind_preserves_unknowns_and_jobs(tmp_path):
    path = _init(tmp_path, name="unknowns.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="observe")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["_future_top"] = {"keep": 1}
    raw["jobs"]["7:100"]["_future_job"] = "keep-job"
    path.write_text(json.dumps(raw), encoding="utf-8")
    snap = bind_jev_ledger(path, ledger_id=LEDGER_A)
    assert snap["jev_ledger_id"] == LEDGER_A
    assert snap["_future_top"] == {"keep": 1}
    assert snap["jobs"]["7:100"]["_future_job"] == "keep-job"
    assert snap["jobs"]["7:100"]["jev_decision"]["participation"] == "observe"
    assert snap["native_session_id"] == FAKE_SID
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_ledger_corrupt_stored_id_fails_load_no_ack(tmp_path):
    for i, bad in enumerate(("not-hex", "cc" * 15, "zz" * 16, 123, True, [], {})):
        base = _init(tmp_path, name=f"bad-ledger-{i}.json")
        _enq(base)
        raw = json.loads(base.read_text(encoding="utf-8"))
        raw["jev_ledger_id"] = bad
        base.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(DeliveryError, match="invalid jev_ledger_id"):
            load_delivery_state(base)
        with pytest.raises(DeliveryError, match="invalid jev_ledger_id"):
            processed_seq_prefix(base, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)


# ---- NEW-proof loader binding: any non-null decl requires stored fp ----


def _forge_null_fp(path):
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:100"]["submitted_text_sha256"] = None
    raw["jobs"]["7:100"]["submitted_text_len"] = None
    path.write_text(json.dumps(raw), encoding="utf-8")


def test_forged_decl_null_fp_turn_submitting_fails_load_no_repeat(tmp_path):
    path = _init(tmp_path, name="forge-turn.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    _forge_null_fp(path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        get_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="no_reply_needed")
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_forged_decl_null_fp_inflight_fails_load_no_skip_ack(tmp_path):
    path = _init(tmp_path, name="forge-inflight.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=3)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    _forge_null_fp(path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        get_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        skip_job(path, CHANNEL, 100, reason="source_deleted")
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        complete_observation(path, CHANNEL, 100, turn_index=3, tool_call_ids=["c1"])
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_forged_decl_null_fp_uncertain_fails_load_no_complete(tmp_path):
    path = _init(tmp_path, name="forge-uncertain.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=2)
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    mark_uncertain(path, CHANNEL, 100, kind="timeout")
    _forge_null_fp(path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        get_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        complete_observation(path, CHANNEL, 100, turn_index=2, tool_call_ids=["c1"])
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_forged_decl_null_fp_skipped_fails_load_no_ack(tmp_path):
    path = _init(tmp_path, name="forge-skipped.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    skip_job(path, CHANNEL, 100, reason="source_deleted")
    _forge_null_fp(path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        get_job(path, CHANNEL, 100)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        load_delivery_state(path)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    with pytest.raises(DeliveryError, match="no_reply needs submitted proof"):
        skip_job(path, CHANNEL, 100, reason="source_deleted")
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_decl_with_bool_invalid_mixed_pair_fails_load(tmp_path):
    bad_pairs = [
        (True, FP_LEN),
        (FP_SHA, True),
        (True, True),
        (None, FP_LEN),
        (FP_SHA, None),
        (FP_SHA, 0),
        (FP_SHA, 4001),
        (FP_SHA, -1),
        ("zz" * 32, FP_LEN),
        ("ab" * 31, FP_LEN),
        ("ab" * 33, FP_LEN),
        ("", FP_LEN),
        (FP_SHA, "12"),
        (123, FP_LEN),
    ]
    for i, (bad_sha, bad_len) in enumerate(bad_pairs):
        path = _init(tmp_path, name=f"badpair-{i}.json")
        _enq(path)
        _bind(path, CHANNEL, 100, participation="need_context")
        mark_turn_submitting(
            path,
            CHANNEL,
            100,
            job_ref="jobref-001",
            pre_turn_index=0,
            submitted_text_sha256=FP_SHA,
            submitted_text_len=FP_LEN,
        )
        record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["jobs"]["7:100"]["submitted_text_sha256"] = bad_sha
        raw["jobs"]["7:100"]["submitted_text_len"] = bad_len
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
            load_delivery_state(path)
        with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
            processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0)


def test_valid_fp_decl_skip_uncertain_late_complete_positives(tmp_path):
    # Valid binding keeps idempotent declare + skip + uncertain reconcile.
    path = _init(tmp_path, name="valid-decl.json")
    _enq(path)
    _bind(path, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    before = path.read_bytes()
    same = record_no_reply(path, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    assert same["no_reply_declaration"]["reason"] == "missing_context"
    assert path.read_bytes() == before
    assert get_job(path, CHANNEL, 100)["submitted_text_sha256"] == FP_SHA
    # Skip with valid decl ACKs.
    path2 = _init(tmp_path, name="valid-skip.json")
    _enq(path2)
    _bind(path2, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path2,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    record_no_reply(path2, CHANNEL, 100, job_ref="jobref-001", reason="no_reply_needed")
    skipped = skip_job(path2, CHANNEL, 100, reason="source_deleted")
    assert skipped["stage"] == "skipped"
    assert skipped["no_reply_declaration"]["job_ref"] == "jobref-001"
    assert (
        processed_seq_prefix(path2, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )
    # Uncertain with valid decl late-completes and ACKs.
    path3 = _init(tmp_path, name="valid-uncertain.json")
    _enq(path3)
    _bind(path3, CHANNEL, 100, participation="need_context")
    mark_turn_submitting(
        path3,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=0,
        submitted_text_sha256=FP_SHA,
        submitted_text_len=FP_LEN,
    )
    mark_inflight(path3, CHANNEL, 100, turn_index=2)
    record_no_reply(path3, CHANNEL, 100, job_ref="jobref-001", reason="missing_context")
    mark_uncertain(path3, CHANNEL, 100, kind="timeout")
    done = complete_observation(path3, CHANNEL, 100, turn_index=2, tool_call_ids=["c-late"])
    assert done["stage"] == "observed"
    assert done["observation"]["origin"] == "thyca"
    assert (
        processed_seq_prefix(path3, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_legacy_no_decl_none_fp_retains_load_ack(tmp_path):
    # Old jobs with None/missing declaration keep None-fp compat.
    path = _init(tmp_path, name="legacy-nodecl.json")
    _enq(path, mid=100, seq=1)
    _enq(path, mid=101, seq=2)
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-100", pre_turn_index=0)
    assert get_job(path, CHANNEL, 100)["no_reply_declaration"] is None
    assert get_job(path, CHANNEL, 100)["submitted_text_sha256"] is None
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    assert get_job(path, CHANNEL, 100)["stage"] == "inflight"
    skipped = skip_job(path, CHANNEL, 101, reason="out_of_scope")
    assert skipped["stage"] == "skipped"
    assert skipped["no_reply_declaration"] is None
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 101, "seq": 2}], 1) == 2
    )
    # Missing keys load as None (older file shape).
    raw = json.loads(path.read_text(encoding="utf-8"))
    for key in ("submitted_text_sha256", "submitted_text_len", "no_reply_declaration"):
        del raw["jobs"]["7:100"][key]
    path.write_text(json.dumps(raw), encoding="utf-8")
    job = get_job(path, CHANNEL, 100)
    assert job["submitted_text_sha256"] is None
    assert job["submitted_text_len"] is None
    assert job["no_reply_declaration"] is None
    # Legacy completed without fp still loads/ACKs.
    path2 = _init(tmp_path, name="legacy-completed.json")
    _enq(path2)
    mark_turn_submitting(path2, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path2, CHANNEL, 100, turn_index=0)
    record_send_intent(path2, CHANNEL, 100, send_ref="s1", content_len=5, content_hash=HASH_A)
    record_send_receipt(
        path2,
        CHANNEL,
        100,
        send_ref="s1",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    done = complete_job(path2, CHANNEL, 100, turn_index=0, tool_call_ids=["c1"])
    assert done["stage"] == "completed"
    assert done["no_reply_declaration"] is None
    assert (
        processed_seq_prefix(path2, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )
    # Legacy uncertain without decl loads but never ACKs.
    path3 = _init(tmp_path, name="legacy-uncertain.json")
    _enq(path3)
    mark_turn_submitting(path3, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_uncertain(path3, CHANNEL, 100, kind="timeout")
    assert get_job(path3, CHANNEL, 100)["stage"] == "uncertain"
    assert (
        processed_seq_prefix(path3, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 0
    )
