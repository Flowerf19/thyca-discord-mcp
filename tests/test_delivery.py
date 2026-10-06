"""Unit tests for delivery state core (file metadata only, no I/O beyond tmp)."""

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from modum import delivery as delivery_mod
from modum.delivery import (
    DeliveryDurabilityUncertain,
    DeliveryError,
    complete_job,
    enqueue_job,
    get_job,
    initialize_delivery_state,
    list_jobs,
    load_delivery_state,
    mark_inflight,
    mark_turn_submitting,
    mark_uncertain,
    processed_seq_prefix,
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
HASH_C = "ef" * 32


def _path(tmp_path: Path) -> Path:
    return tmp_path / "delivery_state.json"


def _init(
    tmp_path: Path,
    *,
    baseline_acked: int = 0,
    historical: dict | None = None,
    skips: list | None = None,
    sid: str = FAKE_SID,
    name: str = "delivery_state.json",
) -> Path:
    path = tmp_path / name
    initialize_delivery_state(
        path,
        native_session_id=sid,
        baseline_acked=baseline_acked,
        historical_message_ids={} if historical is None else historical,
        authorized_skips=[] if skips is None else skips,
    )
    return path


def _jump(mid: int, channel: int = CHANNEL) -> str:
    return f"https://discord.com/channels/1/{channel}/{mid}"


def _to_sent(path: Path, channel: int, mid: int, seq: int, *, turn: int = 0) -> None:
    enqueue_job(
        path, channel_id=channel, message_id=mid, author_uid=AUTHOR, actor_kind="owner", seq=seq
    )
    mark_turn_submitting(path, channel, mid, job_ref="jobref-001", pre_turn_index=turn)
    mark_inflight(path, channel, mid, turn_index=turn)
    record_send_intent(path, channel, mid, send_ref="send-001", content_len=12, content_hash=HASH_A)
    record_send_receipt(
        path,
        channel,
        mid,
        send_ref="send-001",
        sent_message_id=mid + 5000,
        delivered_channel_id=channel,
        jump_url=_jump(mid + 5000, channel),
        reply_to_message_id=mid,
    )


def test_initialize_fresh_and_load_roundtrip(tmp_path):
    path = _init(tmp_path)
    assert path.exists()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    state = load_delivery_state(path)
    assert state["version"] == 1
    assert state["native_session_id"] == FAKE_SID
    assert state["baseline_acked"] == 0
    assert state["historical_message_ids"] == {}
    assert state["authorized_skips"] == []
    assert state["jobs"] == {}
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_initialize_refuses_existing_valid_corrupt_unknown(tmp_path):
    path = _init(tmp_path)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="already exists"):
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    assert path.read_bytes() == before

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("not json{", encoding="utf-8")
    corrupt_bytes = corrupt.read_bytes()
    with pytest.raises(DeliveryError, match="already exists"):
        initialize_delivery_state(
            corrupt,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    assert corrupt.read_bytes() == corrupt_bytes

    unknown = tmp_path / "unknown.json"
    unknown.write_text('{"version": 99}\n', encoding="utf-8")
    unknown_bytes = unknown.read_bytes()
    with pytest.raises(DeliveryError, match="already exists"):
        initialize_delivery_state(
            unknown,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    assert unknown.read_bytes() == unknown_bytes


def test_load_missing_fails_without_reset(tmp_path):
    path = _path(tmp_path)
    with pytest.raises(DeliveryError, match="missing"):
        load_delivery_state(path)
    assert not path.exists()


@pytest.mark.parametrize(
    "raw",
    ["not json{", "[]", "123", '"x"'],
)
def test_load_invalid_json_fails(tmp_path, raw):
    path = _path(tmp_path)
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)


@pytest.mark.parametrize(
    "drop",
    [
        "version",
        "native_session_id",
        "baseline_acked",
        "historical_message_ids",
        "authorized_skips",
        "jobs",
    ],
)
def test_load_missing_top_key_fails(tmp_path, drop):
    path = _init(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw[drop]
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="missing key"):
        load_delivery_state(path)


@pytest.mark.parametrize("bad", [0, 2, "1", True, None, 1.5])
def test_load_bad_version_rejected(tmp_path, bad):
    path = _init(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["version"] = bad
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)


@pytest.mark.parametrize(
    "bad", ["", "  ", 123, True, None, "x" * 129, "has space", "with\nnewline"]
)
def test_load_bad_sid_rejected(tmp_path, bad):
    path = _init(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["native_session_id"] = bad
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)


@pytest.mark.parametrize("bad", [True, -1, "0", None, 1.5])
def test_load_bad_baseline_rejected(tmp_path, bad):
    path = _init(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["baseline_acked"] = bad
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)


def test_load_bad_historical_rejected(tmp_path):
    bad_cases = [
        [],
        {"7": "x"},
        {"7": [0]},
        {"7": [-1]},
        {"7": [True]},
        {"7": ["100"]},
        {"7": [101, 101]},
        {"nope": [101]},
        {"7.5": [101]},
    ]
    for i, bad in enumerate(bad_cases):
        path = tmp_path / f"h{i}.json"
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["historical_message_ids"] = bad
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(DeliveryError, match="corrupt delivery state"):
            load_delivery_state(path)


def test_load_bad_job_key_and_stage_rejected(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["bad-key"] = raw["jobs"].pop("7:100")
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="bad job key"):
        load_delivery_state(path)

    path2 = _init(tmp_path, name="d2.json")
    enqueue_job(
        path2, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    raw2 = json.loads(path2.read_text(encoding="utf-8"))
    raw2["jobs"]["7:101"]["stage"] = "nope"
    path2.write_text(json.dumps(raw2), encoding="utf-8")
    with pytest.raises(DeliveryError, match="invalid stage"):
        load_delivery_state(path2)


def test_load_bool_ids_and_key_mismatch_rejected(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:100"]["channel_id"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="positive int"):
        load_delivery_state(path)

    path2 = _init(tmp_path, name="d2.json")
    enqueue_job(
        path2, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    raw2 = json.loads(path2.read_text(encoding="utf-8"))
    raw2["jobs"]["7:100"]["message_id"] = 999  # key mismatch
    path2.write_text(json.dumps(raw2), encoding="utf-8")
    with pytest.raises(DeliveryError, match="key mismatch"):
        load_delivery_state(path2)


def test_load_duplicate_seq_across_jobs_rejected(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:101"]["seq"] = 1  # duplicate primary seq
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="duplicate seq"):
        load_delivery_state(path)


def test_initialize_validates_strict_types(tmp_path):
    with pytest.raises(DeliveryError, match="baseline_acked"):
        initialize_delivery_state(
            tmp_path / "a.json",
            native_session_id=FAKE_SID,
            baseline_acked=True,
            historical_message_ids={},
            authorized_skips=[],
        )
    with pytest.raises(DeliveryError, match="invalid native_session_id"):
        initialize_delivery_state(
            tmp_path / "b.json",
            native_session_id="",
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        initialize_delivery_state(
            tmp_path / "c.json",
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={7: [True]},
            authorized_skips=[],
        )


def test_permissions_new_dirs_700_existing_parents_untouched(tmp_path):
    before_mode = stat.S_IMODE(tmp_path.stat().st_mode)
    nested = tmp_path / "newdir" / "sub" / "delivery_state.json"
    initialize_delivery_state(
        nested,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    assert stat.S_IMODE(nested.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "newdir").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "newdir" / "sub").stat().st_mode) == 0o700
    assert stat.S_IMODE(tmp_path.stat().st_mode) == before_mode
    enqueue_job(
        nested, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    assert stat.S_IMODE(nested.stat().st_mode) == 0o600


def test_failed_replace_preserves_bytes_and_removes_temp(tmp_path, monkeypatch):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    before = path.read_bytes()

    def _boom(*args, **kwargs):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(os, "replace", _boom)
    with pytest.raises(OSError, match="simulated disk failure"):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
        )
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []
    monkeypatch.undo()
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    assert job["seq"] == 2


def test_corrupt_mutation_fails_before_write_preserving_bytes(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    path.write_text("corrupt{", encoding="utf-8")
    corrupt_bytes = path.read_bytes()
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
        )
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        get_job(path, CHANNEL, 100)
    assert path.read_bytes() == corrupt_bytes
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_enqueue_get_list_and_unknown(tmp_path):
    path = _init(tmp_path)
    assert get_job(path, CHANNEL, 999) is None
    assert list_jobs(path) == []
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    assert job["stage"] == "queued"
    assert job["seq"] == 1
    assert job["seq_aliases"] == []
    assert job["job_ref"] is None
    fetched = get_job(path, CHANNEL, 100)
    assert fetched == job
    assert list_jobs(path) == [job]
    with pytest.raises(DeliveryError, match="invalid channel_id"):
        get_job(path, True, 100)


def test_enqueue_duplicate_same_seq_idempotent_without_write(tmp_path):
    path = _init(tmp_path)
    first = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=3
    )
    before = path.read_bytes()
    second = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=3
    )
    assert second == first
    assert path.read_bytes() == before
    assert len(list_jobs(path)) == 1


def test_enqueue_duplicate_new_seq_adds_sorted_alias(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=5
    )
    assert job["seq"] == 1
    assert job["seq_aliases"] == [5]
    assert job["stage"] == "queued"
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=3
    )
    assert job["seq_aliases"] == [3, 5]
    assert len(list_jobs(path)) == 1


def test_enqueue_conflicts_and_strict_ids(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    with pytest.raises(DeliveryError, match="already used"):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=1
        )
    with pytest.raises(DeliveryError, match="identity mismatch"):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=100, author_uid=999, actor_kind="owner", seq=2
        )
    with pytest.raises(DeliveryError, match="identity mismatch"):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="other", seq=2
        )
    for bad in (0, -1, True, "1", None):
        with pytest.raises(DeliveryError):
            enqueue_job(
                path,
                channel_id=CHANNEL,
                message_id=102,
                author_uid=AUTHOR,
                actor_kind="owner",
                seq=bad,
            )
    with pytest.raises(DeliveryError):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=103, author_uid=AUTHOR, actor_kind="human", seq=9
        )


def test_turn_and_inflight_guards_and_idempotency(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 100, turn_index=0)
    job = mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    assert job["stage"] == "turn_submitting"
    before = path.read_bytes()
    same = mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    assert same == job
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="cannot mark_turn_submitting"):
        mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-002", pre_turn_index=0)
    job = mark_inflight(path, CHANNEL, 100, turn_index=0)
    assert job["stage"] == "inflight"
    same = mark_inflight(path, CHANNEL, 100, turn_index=0)
    assert same == job
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 100, turn_index=1)
    with pytest.raises(DeliveryError, match="cannot mark_turn_submitting"):
        mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)


def test_intent_stage_guards_and_unique_ref(tmp_path):
    # EXPECTATION_CHANGE (v5 accepted native flow, finding 2): a deferred
    # SDK callback during the synchronous POST await records REAL sends
    # while the job is still turn_submitting (turn_index not bound yet).
    # The old refusal would lose that evidence; record_send_intent is now
    # allowed from turn_submitting and lands in delivering with
    # turn_index None (never a guessed future index). Fresh queued jobs
    # still refuse intents, so this widens a stage guard to the accepted
    # flow; it weakens no validation.
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    with pytest.raises(DeliveryError, match="cannot record intent"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A
        )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    job = record_send_intent(
        path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A
    )
    assert job["stage"] == "delivering"
    assert job["turn_index"] is None  # unbound until the own POST/Trace bind
    before = path.read_bytes()
    same = record_send_intent(
        path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A
    )
    assert same == job
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="send_ref conflict"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-001", content_len=6, content_hash=HASH_A
        )
    with pytest.raises(DeliveryError, match="invalid content_len"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-002", content_len=0, content_hash=HASH_A
        )
    with pytest.raises(DeliveryError, match="invalid content_hash"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-002", content_len=5, content_hash="not-hex!!"
        )
    # The later own-Trace bind fills the unbound index (stage preserved).
    job = mark_inflight(path, CHANNEL, 100, turn_index=0)
    assert job["stage"] == "delivering"
    assert job["turn_index"] == 0


def test_receipt_requires_intent_and_channel_match(tmp_path):
    path = _init(tmp_path)
    _to_sent(path, CHANNEL, 100, 1)
    # Receipt without a matching intent fails (fresh job, no intents).
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    mark_turn_submitting(path, CHANNEL, 101, job_ref="jobref-101", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 101, turn_index=0)
    record_send_intent(path, CHANNEL, 101, send_ref="send-101", content_len=4, content_hash=HASH_B)
    with pytest.raises(DeliveryError, match="no intent for receipt"):
        record_send_receipt(
            path,
            CHANNEL,
            101,
            send_ref="send-999",
            sent_message_id=9001,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(9001),
            reply_to_message_id=101,
        )
    with pytest.raises(DeliveryError, match="delivered_channel_id mismatch"):
        record_send_receipt(
            path,
            CHANNEL,
            101,
            send_ref="send-101",
            sent_message_id=9001,
            delivered_channel_id=999,
            jump_url=_jump(9001, 999),
            reply_to_message_id=101,
        )
    with pytest.raises(DeliveryError, match="invalid jump_url"):
        record_send_receipt(
            path,
            CHANNEL,
            101,
            send_ref="send-101",
            sent_message_id=9001,
            delivered_channel_id=CHANNEL,
            jump_url="not-a-url",
            reply_to_message_id=101,
        )


def test_receipt_partial_stays_delivering_and_idempotent(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    record_send_intent(path, CHANNEL, 100, send_ref="send-002", content_len=6, content_hash=HASH_B)
    job = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    assert job["stage"] == "delivering"
    before = path.read_bytes()
    same = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    assert same == job
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="receipt conflict"):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref="send-001",
            sent_message_id=9999,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(9999),
            reply_to_message_id=100,
        )
    job = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-002",
        sent_message_id=9102,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9102),
        reply_to_message_id=100,
    )
    assert job["stage"] == "sent"


def test_complete_requires_all_receipts_two_plus_pending_blocks(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=1)
    for ref, h in (("send-001", HASH_A), ("send-002", HASH_B), ("send-003", HASH_C)):
        record_send_intent(path, CHANNEL, 100, send_ref=ref, content_len=5, content_hash=h)
    for ref, sent in (("send-001", 9201), ("send-002", 9202)):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref=ref,
            sent_message_id=sent,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(sent),
            reply_to_message_id=100,
        )
    assert get_job(path, CHANNEL, 100)["stage"] == "delivering"
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=1, tool_call_ids=["call-001"])
    record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-003",
        sent_message_id=9203,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9203),
        reply_to_message_id=100,
    )
    assert get_job(path, CHANNEL, 100)["stage"] == "sent"
    with pytest.raises(DeliveryError, match="turn_index mismatch"):
        complete_job(path, CHANNEL, 100, turn_index=9, tool_call_ids=["call-001"])
    job = complete_job(path, CHANNEL, 100, turn_index=1, tool_call_ids=["call-001", "call-002"])
    assert job["stage"] == "completed"
    assert job["trace_refs"]["tool_call_ids"] == ["call-001", "call-002"]
    before = path.read_bytes()
    same = complete_job(path, CHANNEL, 100, turn_index=1, tool_call_ids=["call-001", "call-002"])
    assert same == job
    assert path.read_bytes() == before


def test_complete_before_sdk_receipt_fails(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=[])
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=[])


def test_skip_and_uncertain_guards(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    job = skip_job(path, CHANNEL, 100, reason="operator_approved_drop")
    assert job["stage"] == "skipped"
    same = skip_job(path, CHANNEL, 100, reason="operator_approved_drop")
    assert same == job
    with pytest.raises(DeliveryError, match="skip reason mismatch"):
        skip_job(path, CHANNEL, 100, reason="out_of_scope")
    with pytest.raises(DeliveryError, match="cannot mark uncertain"):
        mark_uncertain(path, CHANNEL, 100, kind="timeout")
    with pytest.raises(DeliveryError, match="invalid skip reason"):
        skip_job(path, CHANNEL, 100, reason="because-keyword")

    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="other", seq=2
    )
    job = mark_uncertain(path, CHANNEL, 101, kind="timeout", http_status=504)
    assert job["stage"] == "uncertain"
    assert job["error"]["kind"] == "timeout"
    assert job["error"]["http_status"] == 504
    same = mark_uncertain(path, CHANNEL, 101, kind="timeout", http_status=504)
    assert same == job
    with pytest.raises(DeliveryError, match="cannot skip"):
        skip_job(path, CHANNEL, 101, reason="operator_approved_drop")
    with pytest.raises(DeliveryError, match="invalid uncertain kind"):
        mark_uncertain(path, CHANNEL, 101, kind="boom")
    with pytest.raises(DeliveryError, match="invalid http_status"):
        mark_uncertain(path, 999, 999, kind="timeout", http_status=99)


def test_terminal_cannot_rewind_but_alias_backfill_persists(tmp_path):
    path = _init(tmp_path)
    _to_sent(path, CHANNEL, 100, 1)
    complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    with pytest.raises(DeliveryError, match="cannot skip"):
        skip_job(path, CHANNEL, 100, reason="operator_approved_drop")
    with pytest.raises(DeliveryError, match="cannot mark uncertain"):
        mark_uncertain(path, CHANNEL, 100, kind="timeout")
    with pytest.raises(DeliveryError, match="is terminal"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-002", content_len=5, content_hash=HASH_B
        )
    # Idempotent receipt repeat on a completed job stays a no-op.
    before = path.read_bytes()
    same = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=5100,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(5100),
        reply_to_message_id=100,
    )
    assert same["stage"] == "completed"
    assert path.read_bytes() == before
    # Poll redelivery of the same MID appends an alias without resubmit.
    job = enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=9
    )
    assert job["stage"] == "completed"
    assert job["seq_aliases"] == [9]
    assert job["trace_refs"]["tool_call_ids"] == ["call-001"]


def test_fresh_mutation_preserves_receipt_recorded_during_await(tmp_path):
    # No actual async file writes: a stale snapshot loaded before the
    # receipt must never be saved; later mutations reload fresh.
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    stale = get_job(path, CHANNEL, 100)
    assert stale["send_receipts"] == []
    # Simulated SDK callback during a worker HTTP await (fresh load+save).
    record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    assert stale["send_receipts"] == []  # stale snapshot is untouched
    # A later mutation (second chunk + completion) must preserve receipt 1.
    record_send_intent(path, CHANNEL, 100, send_ref="send-002", content_len=6, content_hash=HASH_B)
    mid_job = get_job(path, CHANNEL, 100)
    assert [r["send_ref"] for r in mid_job["send_receipts"]] == ["send-001"]
    assert mid_job["stage"] == "delivering"
    record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-002",
        sent_message_id=9102,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9102),
        reply_to_message_id=100,
    )
    done = complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    assert [r["sent_message_id"] for r in done["send_receipts"]] == [9101, 9102]
    assert done["stage"] == "completed"


def test_unknown_fields_preserved_across_mutations(tmp_path):
    path = _init(tmp_path)
    _to_sent(path, CHANNEL, 100, 1)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["_future_top"] = {"note": "keep-me"}
    raw["jobs"]["7:100"]["_future_job"] = "keep-job"
    raw["jobs"]["7:100"]["send_intents"][0]["_future_intent"] = 1
    raw["jobs"]["7:100"]["send_receipts"][0]["_future_receipt"] = 2
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert load_delivery_state(path)["_future_top"] == {"note": "keep-me"}
    done = complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    assert done["_future_job"] == "keep-job"
    assert done["send_intents"][0]["_future_intent"] == 1
    assert done["send_receipts"][0]["_future_receipt"] == 2
    fresh = load_delivery_state(path)
    assert fresh["_future_top"] == {"note": "keep-me"}
    # A new job in another channel must not drop existing unknown fields.
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    fresh = load_delivery_state(path)
    assert fresh["_future_top"] == {"note": "keep-me"}
    assert fresh["jobs"]["7:100"]["_future_job"] == "keep-job"


def test_authorized_two_skips_prefix_21_to_23_new_pending_stops(tmp_path):
    channel = 777000000000000007
    historical_mids = list(range(2000, 2021))
    mid22 = 202100000000000001
    mid23 = 202100000000000002
    new_mid = 202100000000000003
    skips = [
        {
            "channel_id": channel,
            "message_id": mid22,
            "seq": 22,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
        {
            "channel_id": channel,
            "message_id": mid23,
            "seq": 23,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
    ]
    path = _init(tmp_path, baseline_acked=21, historical={channel: historical_mids}, skips=skips)
    jobs = list_jobs(path)
    assert [(j["seq"], j["stage"]) for j in jobs] == [(22, "skipped"), (23, "skipped")]
    ordered = [
        {"channel_id": channel, "message_id": mid22, "seq": 22},
        {"channel_id": channel, "message_id": mid23, "seq": 23},
        {"channel_id": channel, "message_id": new_mid, "seq": 24},
    ]
    assert processed_seq_prefix(path, ordered, 21) == 23
    # EXPECTATION_CHANGE (v5 finding 5): a wrong (seq, MID) tuple is now a
    # hard contradiction -- state knows seq 22 as mid22's source while the
    # row supplies new_mid. The old code silently stopped (never advanced,
    # == 21); finding 5 mandates a loud REJECT for known-seq ownership
    # mismatches on ALL supplied rows (also <= current_acked), never a
    # silent stop or a historical-resolution bypass. The "must not
    # advance" invariant is preserved a fortiori.
    wrong = [
        {"channel_id": channel, "message_id": new_mid, "seq": 22},
        {"channel_id": channel, "message_id": mid22, "seq": 23},
    ]
    with pytest.raises(DeliveryError, match="seq ownership mismatch"):
        processed_seq_prefix(path, wrong, 21)
    # Poll redelivery of a skipped MID needs its alias before it can ack.
    dup_unaliased = [
        {"channel_id": channel, "message_id": mid22, "seq": 22},
        {"channel_id": channel, "message_id": mid23, "seq": 23},
        {"channel_id": channel, "message_id": mid22, "seq": 25},
    ]
    assert processed_seq_prefix(path, dup_unaliased, 21) == 23
    enqueue_job(
        path, channel_id=channel, message_id=mid22, author_uid=AUTHOR, actor_kind="owner", seq=25
    )
    assert processed_seq_prefix(path, dup_unaliased, 21) == 25


def test_prefix_gaps_legal_and_first_unresolved_stops(tmp_path):
    path = _init(tmp_path)
    _to_sent(path, CHANNEL, 100, 1)
    complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    _to_sent(path, CHANNEL, 101, 5)
    complete_job(path, CHANNEL, 101, turn_index=0, tool_call_ids=["call-002"])
    enqueue_job(
        path, channel_id=CHANNEL, message_id=102, author_uid=AUTHOR, actor_kind="owner", seq=6
    )
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
    assert processed_seq_prefix(path, [], 5) == 5
    # Uncertain is terminal but never advances the safe prefix.
    mark_uncertain(path, CHANNEL, 102, kind="timeout")
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
    # Historical redelivery with a fresh seq stays resolved without a job.
    hpath = _init(
        tmp_path, name="h.json", baseline_acked=2, historical={CHANNEL: [301, 302]}, skips=[]
    )
    assert (
        processed_seq_prefix(
            hpath,
            [
                {"channel_id": CHANNEL, "message_id": 301, "seq": 9},
            ],
            2,
        )
        == 9
    )
    with pytest.raises(DeliveryError, match="strictly increase"):
        processed_seq_prefix(
            path,
            [
                {"channel_id": CHANNEL, "message_id": 100, "seq": 5},
                {"channel_id": CHANNEL, "message_id": 101, "seq": 5},
            ],
            0,
        )
    with pytest.raises(DeliveryError, match="below baseline"):
        processed_seq_prefix(hpath, [], 1)


def test_canary_body_and_freeform_error_never_persisted_or_echoed(tmp_path):
    body = "CANARY_BODY_SUPER_SECRET_9f8e7d6c5b4a"
    err = "CANARY_FREEFORM_boom_traceback_12345"
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    with pytest.raises(DeliveryError) as exc:
        mark_uncertain(path, CHANNEL, 100, kind=err)
    assert err not in str(exc.value)
    with pytest.raises(DeliveryError) as exc:
        skip_job(path, CHANNEL, 100, reason=body)
    assert body not in str(exc.value)
    with pytest.raises(DeliveryError) as exc:
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind=body, seq=2
        )
    assert body not in str(exc.value)
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    with pytest.raises(DeliveryError) as exc:
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=body
        )
    assert body not in str(exc.value)
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    with pytest.raises(DeliveryError) as exc:
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref="send-001",
            sent_message_id=9101,
            delivered_channel_id=CHANNEL,
            jump_url=body,
            reply_to_message_id=100,
        )
    assert body not in str(exc.value)
    stored = path.read_bytes().decode("utf-8")
    assert body not in stored
    assert err not in stored
    job = mark_uncertain(path, CHANNEL, 100, kind="timeout", http_status=504)
    assert set(job["error"]) == {"kind", "http_status", "ts"}
    assert body not in repr(job) and err not in repr(job)


def test_list_sorted_and_snapshots_are_copies(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=5
    )
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    assert [j["seq"] for j in list_jobs(path)] == [1, 5]
    job = get_job(path, CHANNEL, 100)
    assert job is not None
    job["stage"] = "hacked"
    assert get_job(path, CHANNEL, 100)["stage"] == "queued"
    state = load_delivery_state(path)
    state["baseline_acked"] = 999
    assert load_delivery_state(path)["baseline_acked"] == 0


def test_symlink_state_refused(tmp_path):
    real = _init(tmp_path, name="real.json")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(DeliveryError, match="symlink"):
        load_delivery_state(link)
    with pytest.raises(DeliveryError, match="symlink"):
        get_job(link, CHANNEL, 100)


# ---------------------------------------------------------------------------
# V5 review fixes (7 findings): late receipts, submit/bind/busy requeue,
# removed caps, skip consistency, safe prefix, dir-fsync durability,
# type-before-enum. All paths private TMP with fake IDs only.
# ---------------------------------------------------------------------------


def _fail_dir_fsync(monkeypatch, secret: str):
    """Make only DIRECTORY fsyncs fail; file fsyncs keep working."""
    real_fsync = os.fsync

    def _fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(secret)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", _fsync)


def test_whole_pipeline_late_receipts_recover_and_complete(tmp_path):
    """Callback submit -> 2 intents -> partial -> uncertain -> late receipts.

    Deferred SDK callbacks during the synchronous POST await record intents
    while turn_index is unbound; a partial state goes uncertain; the late
    receipts for EXISTING pending intents keep stage+error; only the explicit
    recovery after binding the ACTUAL own-Trace index with non-empty verified
    tool call ids completes (caller proof; core checks IDs/matching only --
    no caller ACL), clearing the error and enabling the prefix ACK.
    """
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    sha = hashlib.sha256(b"exact prompt.strip text").hexdigest()
    job = mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=None,  # KNOWN empty native session (0 prior user turns)
        submitted_text_sha256=sha,
        submitted_text_len=22,
    )
    assert job["stage"] == "turn_submitting"
    # Real deferred SDK callbacks during the POST await (turn_index unbound).
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    job = record_send_intent(
        path, CHANNEL, 100, send_ref="send-002", content_len=6, content_hash=HASH_B
    )
    assert job["stage"] == "delivering"
    assert job["turn_index"] is None
    job = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9101,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9101),
        reply_to_message_id=100,
    )
    assert job["stage"] == "delivering"
    job = mark_uncertain(path, CHANNEL, 100, kind="delivery_unknown")
    assert job["stage"] == "uncertain"
    assert job["error"]["kind"] == "delivery_unknown"
    # Two confirmed + one pending never completes.
    with pytest.raises(DeliveryError, match="unconfirmed sends"):
        complete_job(path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-a"])
    # A NEW send intent on uncertain is still refused.
    with pytest.raises(DeliveryError, match="is terminal"):
        record_send_intent(
            path, CHANNEL, 100, send_ref="send-003", content_len=7, content_hash=HASH_C
        )
    # Late receipt for the EXISTING pending intent: preserved, no auto-sent.
    job = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-002",
        sent_message_id=9102,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9102),
        reply_to_message_id=100,
    )
    assert job["stage"] == "uncertain"
    assert job["error"]["kind"] == "delivery_unknown"
    assert [r["sent_message_id"] for r in job["send_receipts"]] == [9101, 9102]
    # Duplicate late receipt idempotent; conflicting one fails byte-preserved
    # (MID/channel/actor receipt metadata stays protected).
    before = path.read_bytes()
    same = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-002",
        sent_message_id=9102,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9102),
        reply_to_message_id=100,
    )
    assert same == job
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="receipt conflict"):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref="send-002",
            sent_message_id=9999,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(9999),
            reply_to_message_id=100,
        )
    assert path.read_bytes() == before
    # Non-matching refs can never create a receipt on uncertain either.
    with pytest.raises(DeliveryError, match="no intent for receipt"):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref="send-999",
            sent_message_id=9103,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(9103),
            reply_to_message_id=100,
        )
    # Bind the ACTUAL own-Trace index: stage/receipts/error preserved.
    job = mark_inflight(path, CHANNEL, 100, turn_index=4)
    assert job["stage"] == "uncertain"
    assert job["turn_index"] == 4
    assert job["error"]["kind"] == "delivery_unknown"
    assert [r["sent_message_id"] for r in job["send_receipts"]] == [9101, 9102]
    # Explicit recovery needs the bound matching index + non-empty tool ids.
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=4, tool_call_ids=[])
    with pytest.raises(DeliveryError, match="turn_index mismatch"):
        complete_job(path, CHANNEL, 100, turn_index=5, tool_call_ids=["call-a"])
    job = complete_job(path, CHANNEL, 100, turn_index=4, tool_call_ids=["call-a", "call-b"])
    assert job["stage"] == "completed"
    assert job["error"] is None  # cleared on the explicit complete
    assert job["trace_refs"]["tool_call_ids"] == ["call-a", "call-b"]
    # Prefix ACK only now.
    assert (
        processed_seq_prefix(path, [{"channel_id": CHANNEL, "message_id": 100, "seq": 1}], 0) == 1
    )


def test_two_confirmed_one_pending_never_completes(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    for ref, h in (("send-001", HASH_A), ("send-002", HASH_B), ("send-003", HASH_C)):
        record_send_intent(path, CHANNEL, 100, send_ref=ref, content_len=5, content_hash=h)
    for ref, sent in (("send-001", 9301), ("send-002", 9302)):
        record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref=ref,
            sent_message_id=sent,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(sent),
            reply_to_message_id=100,
        )
    with pytest.raises(DeliveryError, match="cannot complete"):
        complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    mark_uncertain(path, CHANNEL, 100, kind="timeout", http_status=504)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="unconfirmed sends"):
        complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=["call-001"])
    assert path.read_bytes() == before  # failed complete never writes


def test_requeue_definitive_busy_strict_409_only(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    sha = hashlib.sha256(b"native post text!!").hexdigest()
    created = get_job(path, CHANNEL, 100)["created_at"]
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=2,
        submitted_text_sha256=sha,
        submitted_text_len=17,
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:100"]["_future_job"] = "keep-me"
    path.write_text(json.dumps(raw), encoding="utf-8")
    job = requeue_definitive_busy(path, CHANNEL, 100)
    assert job["stage"] == "queued"
    assert job["job_ref"] is None
    assert job["pre_turn_index"] is None
    assert job["turn_index"] is None
    assert job["submitted_text_sha256"] is None
    assert job["submitted_text_len"] is None
    assert job["error"] is None
    assert job["seq"] == 1 and job["seq_aliases"] == []
    assert job["author_uid"] == AUTHOR and job["actor_kind"] == "owner"
    assert job["created_at"] == created  # timestamps preserved per contract
    assert job["updated_at"]
    assert job["_future_job"] == "keep-me"
    # Already queued: explicit fail, never a silent reset of an accepted job.
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 100, http_status=409)
    assert path.read_bytes() == before
    # Non-strict-409 statuses refused: 503/408/None/string/bool/float.
    for bad in (503, 408, 0, -409, None, "409", True, False, 409.0):
        with pytest.raises(DeliveryError, match="invalid http_status"):
            requeue_definitive_busy(path, CHANNEL, 100, http_status=bad)
        assert path.read_bytes() == before
    # A NEW attempt uses a fresh job_ref (new nonce) and works.
    job = mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-002", pre_turn_index=2)
    assert job["stage"] == "turn_submitting"
    assert job["job_ref"] == "jobref-002"
    # inflight with zero evidence also requeues on a definitive 409.
    mark_inflight(path, CHANNEL, 100, turn_index=3)
    job = requeue_definitive_busy(path, CHANNEL, 100, http_status=409)
    assert job["stage"] == "queued"
    # Busy guard hit AFTER a real SDK intent preserves the bytes.
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-003", pre_turn_index=2)
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    before = path.read_bytes()
    with pytest.raises(DeliveryError, match="cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 100)
    assert path.read_bytes() == before
    # Non-rewind stages never requeue: uncertain / skipped (and completed).
    mark_uncertain(path, CHANNEL, 100, kind="busy")
    with pytest.raises(DeliveryError, match="cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 100)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    skip_job(path, CHANNEL, 101, reason="operator_approved_drop")
    with pytest.raises(DeliveryError, match="cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 101)
    # Recover job 100 through the explicit uncertain path, then prove the
    # terminal completed job never requeues either.
    record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9601,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9601),
        reply_to_message_id=100,
    )
    mark_inflight(path, CHANNEL, 100, turn_index=2)
    done = complete_job(path, CHANNEL, 100, turn_index=2, tool_call_ids=["call-001"])
    assert done["stage"] == "completed"
    with pytest.raises(DeliveryError, match="cannot requeue"):
        requeue_definitive_busy(path, CHANNEL, 100)


def test_requeue_definitive_busy_refreshes_updated_at(tmp_path, monkeypatch):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    sha = hashlib.sha256(b"native post text!!").hexdigest()
    mark_turn_submitting(
        path,
        CHANNEL,
        100,
        job_ref="jobref-001",
        pre_turn_index=2,
        submitted_text_sha256=sha,
        submitted_text_len=17,
    )
    before = get_job(path, CHANNEL, 100)
    created = before["created_at"]
    assert before["updated_at"]  # real clock at submit time
    # Both eligible stages share one mutation block (no stage branch), so one
    # stage pins the bug deterministically: freeze the clock AFTER submitting
    # with a fixed future ISO (no sleep, no current-clock race).
    fixed = "2099-12-31T23:59:59+00:00"
    monkeypatch.setattr(delivery_mod, "_now_iso", lambda: fixed)
    job = requeue_definitive_busy(path, CHANNEL, 100)
    assert job["stage"] == "queued"
    assert job["job_ref"] is None
    assert job["pre_turn_index"] is None
    assert job["turn_index"] is None
    assert job["submitted_text_sha256"] is None
    assert job["submitted_text_len"] is None
    assert job["error"] is None
    assert job["created_at"] == created  # created preserved
    assert job["updated_at"] == fixed  # updated advances to the mutation time
    assert job["seq"] == 1 and job["author_uid"] == AUTHOR and job["actor_kind"] == "owner"
    assert get_job(path, CHANNEL, 100) == job  # disk reload equal


def test_bind_actual_index_idempotent_conflict_preserves_receipts(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=1)
    record_send_intent(path, CHANNEL, 100, send_ref="send-001", content_len=5, content_hash=HASH_A)
    job = record_send_receipt(
        path,
        CHANNEL,
        100,
        send_ref="send-001",
        sent_message_id=9401,
        delivered_channel_id=CHANNEL,
        jump_url=_jump(9401),
        reply_to_message_id=100,
    )
    assert job["stage"] == "sent" and job["turn_index"] is None
    job = mark_inflight(path, CHANNEL, 100, turn_index=5)
    assert job["stage"] == "sent"  # bind only: stage preserved
    assert job["turn_index"] == 5
    assert [r["sent_message_id"] for r in job["send_receipts"]] == [9401]
    before = path.read_bytes()
    same = mark_inflight(path, CHANNEL, 100, turn_index=5)  # same index idempotent
    assert same == job
    assert path.read_bytes() == before
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 100, turn_index=6)  # different index conflicts
    assert path.read_bytes() == before
    # queued: no rewind from before the submit.
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 101, turn_index=5)
    # completed never rewinds.
    complete_job(path, CHANNEL, 100, turn_index=5, tool_call_ids=["call-001"])
    with pytest.raises(DeliveryError, match="cannot mark_inflight"):
        mark_inflight(path, CHANNEL, 100, turn_index=5)
    # uncertain bind keeps stage + error.
    enqueue_job(
        path, channel_id=CHANNEL, message_id=102, author_uid=AUTHOR, actor_kind="owner", seq=3
    )
    mark_turn_submitting(path, CHANNEL, 102, job_ref="jobref-102", pre_turn_index=1)
    mark_uncertain(path, CHANNEL, 102, kind="timeout")
    job = mark_inflight(path, CHANNEL, 102, turn_index=7)
    assert job["stage"] == "uncertain"
    assert job["turn_index"] == 7
    assert job["error"]["kind"] == "timeout"


def test_submit_fingerprint_pair_validation_and_immutable(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    # Both-None legacy pair is valid and idempotent.
    job = mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=None)
    assert job["submitted_text_sha256"] is None
    assert job["submitted_text_len"] is None
    before = path.read_bytes()
    same = mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=None)
    assert same == job
    assert path.read_bytes() == before
    # Fingerprint immutable: a different payload on the same attempt conflicts.
    sha = hashlib.sha256(b"exact prompt.strip text").hexdigest()
    with pytest.raises(DeliveryError, match="cannot mark_turn_submitting"):
        mark_turn_submitting(
            path,
            CHANNEL,
            100,
            job_ref="jobref-001",
            pre_turn_index=None,
            submitted_text_sha256=sha,
            submitted_text_len=22,
        )
    # Pair rules: both-or-none, exact 64-hex SHA-256, strict positive len
    # <= 4000 (0 and bools and containers all rejected as coarse errors).
    enqueue_job(
        path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
    )
    for bad_sha, bad_len in [
        (sha, None),
        (None, 22),
        ("ab" * 31, 22),
        ("ab" * 33, 22),
        ("zz" * 32, 22),
        (True, 22),
        ([sha], 22),
        ({"k": sha}, 22),
        (sha, 0),
        (sha, -1),
        (sha, 4001),
        (sha, True),
        (sha, "22"),
        (sha, 1.5),
        (sha, [22]),
    ]:
        with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
            mark_turn_submitting(
                path,
                CHANNEL,
                101,
                job_ref="jobref-101",
                pre_turn_index=0,
                submitted_text_sha256=bad_sha,
                submitted_text_len=bad_len,
            )
    # Boundary lengths 1 and 4000 are valid.
    job = mark_turn_submitting(
        path,
        CHANNEL,
        101,
        job_ref="jobref-101",
        pre_turn_index=0,
        submitted_text_sha256=sha,
        submitted_text_len=4000,
    )
    assert job["submitted_text_sha256"] == sha
    assert job["submitted_text_len"] == 4000
    with pytest.raises(DeliveryError, match="cannot mark_turn_submitting"):
        mark_turn_submitting(
            path,
            CHANNEL,
            101,
            job_ref="jobref-101",
            pre_turn_index=0,
            submitted_text_sha256=HASH_A,
            submitted_text_len=4000,
        )
    # Legacy state files without the fingerprint keys load as None/None.
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["jobs"]["7:100"]["submitted_text_sha256"]
    del raw["jobs"]["7:100"]["submitted_text_len"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    legacy = get_job(path, CHANNEL, 100)
    assert legacy["submitted_text_sha256"] is None
    assert legacy["submitted_text_len"] is None
    # A one-sided stored pair is corrupt.
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["jobs"]["7:101"]["submitted_text_len"] = None
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="invalid submitted fingerprint"):
        load_delivery_state(path)


def test_no_send_caps_51_chunks_101_tool_ids_1001_aliases(tmp_path):
    # EXPECTATION_CHANGE (v5 finding 3): the old MAX_SENDS_PER_JOB /
    # MAX_TOOL_CALL_IDS / MAX_SEQ_ALIASES / MAX_CONTENT_LEN budgets are
    # GONE (defs deleted). Verified native flows have no such business
    # caps; the old code REJECTED these valid inputs. Accepting them
    # widens nothing security-relevant: positive strict ints, unique
    # send_refs/aliases and known hash formats stay enforced.
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    n = 51
    for i in range(n):
        h = hashlib.sha256(f"chunk-{i}".encode()).hexdigest()
        job = record_send_intent(
            path, CHANNEL, 100, send_ref=f"send-{i:03d}", content_len=10**6 + i, content_hash=h
        )
    assert len(job["send_intents"]) == n
    assert job["stage"] == "delivering"
    for i in range(n):
        mid = 9500 + i
        job = record_send_receipt(
            path,
            CHANNEL,
            100,
            send_ref=f"send-{i:03d}",
            sent_message_id=mid,
            delivered_channel_id=CHANNEL,
            jump_url=_jump(mid),
            reply_to_message_id=100,
        )
    assert job["stage"] == "sent"
    tool_ids = [f"call-{i:03d}" for i in range(101)]
    done = complete_job(path, CHANNEL, 100, turn_index=0, tool_call_ids=tool_ids)
    assert done["trace_refs"]["tool_call_ids"] == tool_ids

    # 1001 seq aliases: a load-validated 999-alias fixture plus two REAL
    # enqueue appends (no unvalidated mocks).
    apath = _init(tmp_path, name="aliases.json")
    enqueue_job(
        apath, channel_id=CHANNEL, message_id=200, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    raw = json.loads(apath.read_text(encoding="utf-8"))
    raw["jobs"]["7:200"]["seq_aliases"] = list(range(10, 1009))  # 999 aliases
    apath.write_text(json.dumps(raw), encoding="utf-8")
    assert len(load_delivery_state(apath)["jobs"]["7:200"]["seq_aliases"]) == 999
    for extra in (2000, 2001):
        job = enqueue_job(
            apath,
            channel_id=CHANNEL,
            message_id=200,
            author_uid=AUTHOR,
            actor_kind="owner",
            seq=extra,
        )
    assert len(job["seq_aliases"]) == 1001
    assert job["seq_aliases"] == sorted(set(job["seq_aliases"]))
    assert len(load_delivery_state(apath)["jobs"]["7:200"]["seq_aliases"]) == 1001


def test_content_hash_exact_sha256_and_positive_unbounded_len(tmp_path):
    # EXPECTATION_CHANGE (v5 finding 3): content_hash now requires the
    # KNOWN 64-hex SHA-256 format (same as native Trace) instead of any
    # 1..128-char hex string -- a stricter known-format bound, not a
    # weakening. content_len is a strict positive scalar with no 1M cap.
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    mark_turn_submitting(path, CHANNEL, 100, job_ref="jobref-001", pre_turn_index=0)
    mark_inflight(path, CHANNEL, 100, turn_index=0)
    real_sha = hashlib.sha256(b"chunk").hexdigest()
    job = record_send_intent(
        path, CHANNEL, 100, send_ref="send-001", content_len=10**7, content_hash=real_sha
    )
    assert job["send_intents"][0]["content_len"] == 10**7
    assert job["send_intents"][0]["content_hash"] == real_sha
    for bad in ("ab", "ab" * 31, "ab" * 33, "zz" * 32, "", True, 123, None, ["a"] * 64):
        with pytest.raises(DeliveryError, match="invalid content_hash"):
            record_send_intent(
                path, CHANNEL, 100, send_ref="send-002", content_len=1, content_hash=bad
            )


def test_authorized_skip_job_cross_consistency_on_load(tmp_path):
    channel = 777000000000000007
    mid22 = 202100000000000001
    mid23 = 202100000000000002
    skips = [
        {
            "channel_id": channel,
            "message_id": mid22,
            "seq": 22,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
        {
            "channel_id": channel,
            "message_id": mid23,
            "seq": 23,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
    ]
    path = _init(
        tmp_path,
        baseline_acked=21,
        historical={channel: list(range(2000, 2021))},
        skips=skips,
    )
    state = load_delivery_state(path)
    for key in (f"{channel}:{mid22}", f"{channel}:{mid23}"):
        assert state["jobs"][key]["stage"] == "skipped"
    # Legitimate dedupe: extra seq aliases on a skip job keep the record ok.
    enqueue_job(
        path, channel_id=channel, message_id=mid22, author_uid=AUTHOR, actor_kind="owner", seq=25
    )
    assert load_delivery_state(path)["jobs"][f"{channel}:{mid22}"]["seq_aliases"] == [25]
    # Every cross-field mismatch is corrupt load (bounded error, no repair).
    key22 = f"{channel}:{mid22}"
    mutations = [
        lambda raw: raw["jobs"].pop(key22),
        lambda raw: raw["jobs"][key22].update(seq=24),
        lambda raw: raw["jobs"][key22].update(author_uid=AUTHOR + 1),
        lambda raw: raw["jobs"][key22].update(actor_kind="other"),
        lambda raw: raw["jobs"][key22].update(actor_kind=[]),
        lambda raw: raw["jobs"][key22].update(skip_reason="out_of_scope"),
        lambda raw: raw["jobs"][key22].update(stage="completed"),
        lambda raw: raw["authorized_skips"][0].update(seq=24),
        lambda raw: raw["authorized_skips"][0].update(author_uid=AUTHOR + 1),
        lambda raw: raw["authorized_skips"][0].update(actor_kind="other"),
        lambda raw: raw["authorized_skips"][0].update(actor_kind={}),
        lambda raw: raw["authorized_skips"][0].update(reason="out_of_scope"),
        lambda raw: raw["authorized_skips"][0].update(message_id=999),
    ]
    good = path.read_bytes()
    for mutate in mutations:
        raw = json.loads(good)
        mutate(raw)
        payload = json.dumps(raw)
        path.write_text(payload, encoding="utf-8")
        with pytest.raises(DeliveryError, match="corrupt delivery state"):
            load_delivery_state(path)
        assert path.read_text(encoding="utf-8") == payload  # no corruption repair
        path.write_bytes(good)


def test_prefix_identity_fields_and_seq_ownership_contradictions(tmp_path):
    channel = 777000000000000007
    hist_mid = 3001
    mid22 = 202100000000000001
    mid23 = 202100000000000002
    skips = [
        {
            "channel_id": channel,
            "message_id": mid22,
            "seq": 22,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
        {
            "channel_id": channel,
            "message_id": mid23,
            "seq": 23,
            "author_uid": AUTHOR,
            "actor_kind": "owner",
            "reason": "operator_approved_drop",
        },
    ]
    path = _init(tmp_path, baseline_acked=21, historical={channel: [hist_mid]}, skips=skips)
    e22 = {"channel_id": channel, "message_id": mid22, "seq": 22}
    e23 = {"channel_id": channel, "message_id": mid23, "seq": 23}
    # Optional identity fields that MATCH the known job still ACK.
    assert (
        processed_seq_prefix(
            path,
            [
                {**e22, "author_uid": AUTHOR, "actor_kind": "owner"},
                {**e23, "author_uid": AUTHOR, "actor_kind": "owner"},
            ],
            21,
        )
        == 23
    )
    # Partial optional fields remain compatible (format checks only).
    assert processed_seq_prefix(path, [{**e22, "author_uid": AUTHOR}], 21) == 22
    assert processed_seq_prefix(path, [{**e22, "actor_kind": "owner"}], 21) == 22
    # Historical rows without a job: format checks only, still resolve.
    assert (
        processed_seq_prefix(
            path,
            [{"channel_id": channel, "message_id": hist_mid, "seq": 29, "author_uid": 12345}],
            21,
        )
        == 29
    )
    # Wrong UID / actor raise both at unread and at <= current_acked rows.
    for entry, acked, match in [
        ({**e22, "author_uid": AUTHOR + 1}, 21, "author mismatch"),
        ({**e22, "author_uid": AUTHOR + 1}, 23, "author mismatch"),
        ({**e22, "actor_kind": "other"}, 21, "actor mismatch"),
        ({**e22, "actor_kind": "other"}, 23, "actor mismatch"),
    ]:
        with pytest.raises(DeliveryError, match=match):
            processed_seq_prefix(path, [entry], acked)
    # Non-str actor/UID values are coarse DeliveryErrors, never TypeError.
    for bad_actor in ([], {}, None, 1, ("owner",)):
        with pytest.raises(DeliveryError):
            processed_seq_prefix(path, [{**e22, "actor_kind": bad_actor}], 21)
    for bad_uid in (True, "7", [], None, 1.5):
        with pytest.raises(DeliveryError):
            processed_seq_prefix(path, [{**e22, "author_uid": bad_uid}], 21)
    # Seq ownership contradictions raise both unread and <= current_acked.
    for entry, acked in [
        ({"channel_id": channel, "message_id": mid23, "seq": 22}, 21),
        ({"channel_id": channel, "message_id": mid23, "seq": 22}, 23),
        # Historical row with another job's seq: reject, no bypass.
        ({"channel_id": channel, "message_id": hist_mid, "seq": 22}, 21),
        ({"channel_id": channel, "message_id": hist_mid, "seq": 22}, 23),
    ]:
        with pytest.raises(DeliveryError, match="seq ownership mismatch"):
            processed_seq_prefix(path, [entry], acked)


def test_dir_fsync_failure_after_replace_raises_durability_uncertain(tmp_path, monkeypatch):
    secret = "CANARY_DIR_FSYNC_SECRET_9f8e"
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    _fail_dir_fsync(monkeypatch, secret)
    with pytest.raises(DeliveryDurabilityUncertain) as exc:
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
        )
    err = exc.value
    assert isinstance(err, DeliveryError)
    assert err.kind == "durability_uncertain"
    assert err.committed is True
    assert "directory fsync failed" in str(err)
    assert secret not in str(err)
    assert err.__cause__ is None  # chained raw OSError text is never exposed
    monkeypatch.undo()
    # Committed file is VALID with the NEW state present: no rewind, no
    # delete, and no blind retry is implied (caller must reconcile).
    state = load_delivery_state(path)
    assert "7:101" in state["jobs"]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_precommit_fsync_failure_preserves_original_bytes(tmp_path, monkeypatch):
    secret = "CANARY_PRE_FSYNC_SECRET"
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    before = path.read_bytes()
    real_fsync = os.fsync

    def _fsync(fd):
        if not stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(secret)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", _fsync)
    with pytest.raises(OSError):
        enqueue_job(
            path, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR, actor_kind="owner", seq=2
        )
    monkeypatch.undo()
    assert path.read_bytes() == before  # original bytes identical
    assert list(tmp_path.glob("*.tmp.*")) == []  # own temp cleaned up


def test_initialize_dir_fsync_failure_state_committed_and_exclusive(tmp_path, monkeypatch):
    secret = "CANARY_INIT_FSYNC_SECRET"
    path = _path(tmp_path)
    _fail_dir_fsync(monkeypatch, secret)
    with pytest.raises(DeliveryDurabilityUncertain) as exc:
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    assert exc.value.committed is True
    assert secret not in str(exc.value)
    assert exc.value.__cause__ is None
    monkeypatch.undo()
    # State EXISTS and validates; the tmp link is cleaned; future init
    # still refuses (exclusive bootstrap, no overwrite).
    assert list(tmp_path.glob("*.tmp.*")) == []
    assert load_delivery_state(path)["jobs"] == {}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(DeliveryError, match="already exists"):
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )


def test_type_before_enum_yields_bounded_delivery_error(tmp_path):
    path = _init(tmp_path)
    enqueue_job(
        path, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR, actor_kind="owner", seq=1
    )
    canary = "CANARY_ENUM_VALUE_9f8e7d"
    for bad in ([], {}, None, 1, b"x", ("owner",), canary):
        with pytest.raises(DeliveryError, match="invalid actor_kind"):
            enqueue_job(
                path,
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
                actor_kind=bad,
                seq=2,
            )
        with pytest.raises(DeliveryError, match="invalid skip reason"):
            skip_job(path, CHANNEL, 100, reason=bad)
        with pytest.raises(DeliveryError, match="invalid uncertain kind"):
            mark_uncertain(path, CHANNEL, 100, kind=bad)
    for bad in ([], {}, None, 1, b"x", "has space", "x" * 129):
        with pytest.raises(DeliveryError, match="invalid send_ref"):
            record_send_intent(path, CHANNEL, 100, send_ref=bad, content_len=5, content_hash=HASH_A)
        with pytest.raises(DeliveryError, match="invalid job_ref"):
            mark_turn_submitting(path, CHANNEL, 100, job_ref=bad, pre_turn_index=0)
    # Corrupt stored enum-ish values also load-fail as bounded DeliveryError.
    good = path.read_bytes()
    for field, bad in (
        ("actor_kind", []),
        ("stage", {}),
        ("skip_reason", ["x"]),
    ):
        raw = json.loads(good)
        raw["jobs"]["7:100"][field] = bad
        path.write_text(json.dumps(raw), encoding="utf-8")
        with pytest.raises(DeliveryError, match="corrupt delivery state"):
            load_delivery_state(path)
        path.write_bytes(good)
    raw = json.loads(good)
    raw["jobs"]["7:100"]["error"] = {"kind": [], "http_status": None, "ts": "x"}
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="corrupt delivery state"):
        load_delivery_state(path)
    assert canary.encode() not in path.read_bytes()  # canary never persisted


def test_json_error_message_bounded_no_body(tmp_path):
    canary = "CANARY_FILE_BODY_CONTENT_9f8e"
    path = _path(tmp_path)
    path.write_text(canary + " {{{ broken", encoding="utf-8")
    with pytest.raises(DeliveryError) as exc:
        load_delivery_state(path)
    assert "invalid JSON" in str(exc.value)
    assert canary not in str(exc.value)
    assert exc.value.__cause__ is None
