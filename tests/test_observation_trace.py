"""Observation trace tests: discord__record_no_reply proof in native traces.

Offline only via injected transports. No real native, network, or env.
Covers valid proof, missing/foreign/legacy/no-evidence, duplicates,
unknown fields, SID, invalid responses, and corrupt results. Send outputs
stay bit-for-bit preserved.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from modum.thyca_client import ThycaClient, ThycaProtocolError

VALID_SID = "2026-10-05T12-34-56_ab12"
OTHER_SID = "2026-10-04T01-02-03_cd34"
BASE = "http://127.0.0.1:8765"
JOB = "job_ref_abc123"
FOREIGN_JOB = "job_ref_foreign999"
SOURCE_MID = 111111111111111111
CHANNEL = 222222222222222222
CANARY = "INKCANARYOBS_9f3c7a"

NO_REPLY_TOOL = "discord__record_no_reply"


def _msg(role, content=None, *, tool_calls=None, tool_call_id=None, meta=None):
    out = {"role": role, "content": content, "ts": "2026-10-05T12:00:00Z"}
    if tool_calls is not None:
        out["tool_calls"] = tool_calls
    if tool_call_id is not None:
        out["tool_call_id"] = tool_call_id
    if meta is not None:
        out["meta"] = meta
    return out


def _trace(sid, index, status, messages, *, model=None, error=None):
    return {
        "session_id": sid,
        "turn_index": index,
        "title": "t",
        "started_at": "2026-10-05T12:00:00Z",
        "ended_at": "2026-10-05T12:01:00Z",
        "model": model,
        "status": status,
        "rounds": 1,
        "requests": 1,
        "prompt_tokens": None,
        "cached_tokens": None,
        "completion_tokens": None,
        "total_tokens": None,
        "cost_usd": None,
        "latency_ms": None,
        "error": error,
        "messages": messages,
    }


def _obs_call(call_id, ch, mid, job, reason, extra=None):
    args = {"channel_id": ch, "source_message_id": mid, "job_ref": job, "reason": reason}
    if extra:
        args.update(extra)
    return {"id": call_id, "name": NO_REPLY_TOOL, "arguments": args}


def _obs_result(call_id, ch, mid, job, reason, status="declared", extra=None, meta=None):
    body = {
        "channel_id": ch,
        "source_message_id": mid,
        "job_ref": job,
        "reason": reason,
        "status": status,
    }
    if extra:
        body.update(extra)
    return _msg("tool", json.dumps(body), tool_call_id=call_id, meta=meta)


def _send_call(call_id, ch, mid, job, content="hi"):
    return {
        "id": call_id,
        "name": "discord__send_message",
        "arguments": {
            "channel_id": ch,
            "source_message_id": mid,
            "job_ref": job,
            "content": content,
        },
    }


def _send_result(call_id, mid, ch, jump):
    return _msg(
        "tool",
        json.dumps({"message_id": mid, "channel_id": ch, "jump_url": jump}),
        tool_call_id=call_id,
    )


class FakeTransport:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return 200, json.dumps(self.payload).encode()


async def _evidence(payload, *, job=JOB, sid=VALID_SID, index=2):
    client = ThycaClient(BASE, transport=FakeTransport(payload))
    return await client.get_trace_evidence(
        sid, index, job_ref=job, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )


def _jump(mid):
    return f"https://discord.com/channels/9/{CHANNEL}/{mid}"


# ---- happy path ----


async def test_observation_confirmed_happy():
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"please [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("obs1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("obs1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 1
    assert len(ev["observation_calls"]) == 1
    obs = ev["observation_calls"][0]
    assert obs["tool_call_id"] == "obs1"
    assert obs["channel_match"] is True
    assert obs["source_mid_match"] is True
    assert obs["job_ref_match"] is True
    assert obs["reason_valid"] is True
    assert obs["correlated"] is True
    assert obs["result_present"] is True
    assert obs["result_ids_match"] is True
    assert obs["result_reason_match"] is True
    assert obs["result_status_declared"] is True
    assert obs["result_ok"] is True
    assert ev["all_observations_confirmed"] is True
    # Sends untouched; observation still counts as other for compat.
    assert ev["send_call_count"] == 0
    assert ev["all_sends_confirmed"] is False
    assert ev["other_tool_call_count"] == 1
    assert ev["other_tool_name_lens"] == [len(NO_REPLY_TOOL)]
    assert ev["trace_session_match"] is True
    assert ev["turn_index_match"] is True


async def test_second_reason_confirmed():
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"x [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o2", CHANNEL, SOURCE_MID, JOB, "no_reply_needed")],
            ),
            _obs_result("o2", CHANNEL, SOURCE_MID, JOB, "no_reply_needed"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["all_observations_confirmed"] is True
    assert ev["observation_calls"][0]["result_ok"] is True


# ---- missing / foreign / no evidence ----


async def test_missing_result_is_no_proof():
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("late", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 1
    assert ev["observation_calls"][0]["result_present"] is False
    assert ev["observation_calls"][0]["result_ok"] is False
    assert ev["all_observations_confirmed"] is False


async def test_foreign_nonce_args_and_result_are_no_proof():
    # Foreign job in ARGS.
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("f1", CHANNEL, SOURCE_MID, FOREIGN_JOB, "missing_context")],
            ),
            _obs_result("f1", CHANNEL, SOURCE_MID, FOREIGN_JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["job_ref_match"] is False
    assert ev["observation_calls"][0]["correlated"] is False
    assert ev["observation_calls"][0]["result_ok"] is False
    assert ev["all_observations_confirmed"] is False
    # Foreign job in RESULT (args good, result mismatched).
    payload2 = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("f2", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("f2", CHANNEL, SOURCE_MID, FOREIGN_JOB, "missing_context"),
        ],
    )
    ev2 = await _evidence(payload2)
    assert ev2["observation_calls"][0]["correlated"] is True
    assert ev2["observation_calls"][0]["result_ids_match"] is False
    assert ev2["observation_calls"][0]["result_ok"] is False
    assert ev2["all_observations_confirmed"] is False


async def test_no_evidence_is_not_confirmed():
    payload = _trace(
        VALID_SID, 2, "completed", [_msg("user", f"hi [{JOB}]"), _msg("assistant", "ok")]
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 0
    assert ev["observation_calls"] == []
    assert ev["all_observations_confirmed"] is False


async def test_legacy_sends_preserved_bit_for_bit():
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB, content="reply-body")
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"please [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _send_result("call_1", 444, CHANNEL, _jump(444)),
        ],
    )
    ev = await _evidence(payload)
    assert ev["send_call_count"] == 1
    assert ev["correlated_send_count"] == 1
    assert ev["legacy_uncorrelated_count"] == 0
    assert ev["all_sends_confirmed"] is True
    send = ev["sends"][0]
    assert send["tool_call_id"] == "call_1"
    assert send["result_ok"] is True
    assert send["content_sha256"] == hashlib.sha256(b"reply-body").hexdigest()
    # Observation extension adds empty proof without touching sends/other.
    assert ev["observation_call_count"] == 0
    assert ev["observation_calls"] == []
    assert ev["all_observations_confirmed"] is False
    assert ev["other_tool_call_count"] == 0
    assert ev["other_tool_name_lens"] == []


async def test_mixed_send_and_observation_both_confirmed():
    send = _send_call("s1", CHANNEL, SOURCE_MID, JOB, content="hi")
    obs = _obs_call("o1", CHANNEL, SOURCE_MID, JOB, "no_reply_needed")
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[send, obs]),
            _send_result("s1", 555, CHANNEL, _jump(555)),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "no_reply_needed"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["all_sends_confirmed"] is True
    assert ev["all_observations_confirmed"] is True
    assert ev["send_call_count"] == 1
    assert ev["observation_call_count"] == 1
    assert ev["other_tool_call_count"] == 1


# ---- duplicates require globally unique IDs ----


async def test_duplicate_call_ids_kill_observation_proof_only():
    c1 = _obs_call("dup", CHANNEL, SOURCE_MID, JOB, "missing_context")
    c2 = _obs_call("dup", CHANNEL, SOURCE_MID, JOB, "missing_context")
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[c1, c2]),
            _obs_result("dup", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 2
    assert ev["all_observations_confirmed"] is False
    # Cross-kind duplicate (send + observation share an ID) also kills obs proof.
    send = _send_call("same", CHANNEL, SOURCE_MID, JOB)
    obs = _obs_call("same", CHANNEL, SOURCE_MID, JOB, "missing_context")
    payload2 = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[send, obs]),
            _send_result("same", 666, CHANNEL, _jump(666)),
        ],
    )
    ev2 = await _evidence(payload2)
    assert ev2["all_observations_confirmed"] is False


# ---- corrupt results / args ----


@pytest.mark.parametrize(
    "make_result",
    [
        lambda cid: _msg("tool", json.dumps({"error": "denied"}), tool_call_id=cid),
        lambda cid: _msg("tool", "not json", tool_call_id=cid),
        lambda cid: _msg("tool", "", tool_call_id=cid),
        lambda cid: _msg("tool", json.dumps(["not", "dict"]), tool_call_id=cid),
        lambda cid: _obs_result(cid, CHANNEL, SOURCE_MID, JOB, "missing_context", status="sent"),
        lambda cid: _obs_result(cid, CHANNEL, SOURCE_MID, JOB, "bogus_reason"),
        lambda cid: _obs_result(cid, 999, SOURCE_MID, JOB, "missing_context"),
        lambda cid: _obs_result(cid, CHANNEL, SOURCE_MID, JOB, "no_reply_needed"),
        lambda cid: _msg(
            "tool",
            json.dumps(
                {
                    "channel_id": CHANNEL,
                    "source_message_id": SOURCE_MID,
                    "job_ref": JOB,
                    "reason": "missing_context",
                    "status": "declared",
                    "error": "x",
                }
            ),
            tool_call_id=cid,
        ),
        lambda cid: _obs_result(
            cid, CHANNEL, SOURCE_MID, JOB, "missing_context", meta={"is_error": True}
        ),
        lambda cid: _msg(
            "tool",
            json.dumps(
                {
                    "channel_id": True,
                    "source_message_id": SOURCE_MID,
                    "job_ref": JOB,
                    "reason": "missing_context",
                    "status": "declared",
                }
            ),
            tool_call_id=cid,
        ),
    ],
)
async def test_corrupt_observation_results_are_no_proof(make_result):
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            make_result("o1"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["result_ok"] is False
    assert ev["all_observations_confirmed"] is False


@pytest.mark.parametrize(
    "args_reason",
    ["", "OBSERVE", "observe", "bogus", None, True, 123, ["missing_context"]],
)
async def test_invalid_args_reason_is_uncorrelated(args_reason):
    call = {
        "id": "o1",
        "name": NO_REPLY_TOOL,
        "arguments": {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": args_reason,
        },
    }
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["reason_valid"] is False
    assert ev["observation_calls"][0]["correlated"] is False
    assert ev["all_observations_confirmed"] is False


async def test_bool_ids_in_args_are_no_proof():
    call = {
        "id": "o1",
        "name": NO_REPLY_TOOL,
        "arguments": {
            "channel_id": True,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": "missing_context",
        },
    }
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["channel_match"] is False
    assert ev["all_observations_confirmed"] is False


async def test_exact_tool_name_only():
    # Wrong case / prefix must NOT extract as observation (stays other-only).
    weird = {"id": "w1", "name": "discord__Record_No_Reply", "arguments": {"channel_id": CHANNEL}}
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=[weird])],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 0
    assert ev["other_tool_call_count"] == 1


# ---- unknown fields / SID / whole-turn flags / invalid responses ----


async def test_unknown_fields_ignored_without_leak():
    call = _obs_call(
        "o1",
        CHANNEL,
        SOURCE_MID,
        JOB,
        "missing_context",
        extra={"future_arg": CANARY, "profile": CANARY},
    )
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _obs_result(
                "o1", CHANNEL, SOURCE_MID, JOB, "missing_context", extra={"future_result": CANARY}
            ),
        ],
    )
    payload["_future_top"] = CANARY
    ev = await _evidence(payload)
    assert ev["all_observations_confirmed"] is True
    blob = json.dumps(ev)
    assert CANARY not in blob


async def test_wrong_sid_no_false_proof():
    payload = _trace(
        OTHER_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["trace_session_match"] is False
    assert ev["all_observations_confirmed"] is False


async def test_whole_turn_flags_unchanged_with_observations():
    user_text = f"hello [{JOB}]"
    base = [
        _msg("user", user_text),
        _msg(
            "assistant",
            None,
            tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
        ),
        _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
    ]
    payload = _trace(VALID_SID, 2, "completed", base, model="m1", error=None)
    ev = await _evidence(payload)
    assert ev["status"] == "completed"
    assert ev["error_present"] is False
    assert ev["request_user_count"] == 1
    assert ev["request_job_ref_match"] is True
    assert ev["request_input_len"] == len(user_text)
    assert ev["request_input_sha256"] == hashlib.sha256(user_text.encode()).hexdigest()
    assert ev["all_observations_confirmed"] is True
    # Missing job hint kills proof without touching flags shape.
    payload2 = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", "hello without hint"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev2 = await _evidence(payload2)
    assert ev2["request_job_ref_match"] is False
    assert ev2["all_observations_confirmed"] is False


async def test_invalid_tool_shape_raises_protocol():
    for bad_calls in (
        ["not-a-dict"],
        [{"name": NO_REPLY_TOOL}],
        [{"id": "x"}],
        [{"id": "x", "name": ""}],
    ):
        payload = _trace(
            VALID_SID,
            2,
            "completed",
            [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=bad_calls)],
        )
        with pytest.raises(ThycaProtocolError):
            await _evidence(payload)
    # Non-list tool_calls.
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            {"role": "assistant", "content": None, "tool_calls": {"id": "x"}},
        ],
    )
    with pytest.raises(ThycaProtocolError):
        await _evidence(payload)
    # Non-dict arguments.
    bad = {"id": "o1", "name": NO_REPLY_TOOL, "arguments": ["bad"]}
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=[bad])],
    )
    with pytest.raises(ThycaProtocolError):
        await _evidence(payload)


async def test_two_user_frames_malformed_no_proof():
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("user", "second"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    with pytest.raises(ThycaProtocolError):
        await _evidence(payload)


async def test_raw_result_body_never_returned():
    secret = "SECRET_PROVIDER_BODY_123"
    call = _obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")
    result = _msg(
        "tool",
        json.dumps(
            {
                "channel_id": CHANNEL,
                "source_message_id": SOURCE_MID,
                "job_ref": JOB,
                "reason": "missing_context",
                "status": "declared",
                "raw_body": secret,
            }
        ),
        tool_call_id="o1",
    )
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=[call]), result],
    )
    ev = await _evidence(payload)
    assert ev["all_observations_confirmed"] is True
    assert secret not in json.dumps(ev)


# ---- reason binding for Router vs persisted declaration ----


async def test_observation_reason_field_whitelisted_for_router():
    # Every observation record carries the exact whitelisted enum for
    # Router comparison against persisted job.no_reply_declaration.reason.
    for reason in ("missing_context", "no_reply_needed"):
        payload = _trace(
            VALID_SID,
            2,
            "completed",
            [
                _msg("user", f"hi [{JOB}]"),
                _msg(
                    "assistant",
                    None,
                    tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, reason)],
                ),
                _obs_result("o1", CHANNEL, SOURCE_MID, JOB, reason),
            ],
        )
        ev = await _evidence(payload)
        assert ev["observation_call_count"] == 1
        obs = ev["observation_calls"][0]
        assert "reason" in obs
        assert obs["reason"] == reason
        assert obs["reason_valid"] is True
        assert obs["result_ok"] is True
        assert ev["all_observations_confirmed"] is True


@pytest.mark.parametrize(
    "args_reason",
    ["", "bogus", "OBSERVE", "observe", "no_reply_needed ", None, True, 123, ["x"], {}],
)
async def test_observation_reason_none_for_invalid_args_no_leak(args_reason):
    # Invalid ARG reason never echoes: literal None, never the untrusted
    # string/type/repr. Old flags/IDs/counts stay unchanged.
    call = {
        "id": "o1",
        "name": NO_REPLY_TOOL,
        "arguments": {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": args_reason,
        },
    }
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    obs = ev["observation_calls"][0]
    assert "reason" in obs
    assert obs["reason"] is None
    assert obs["reason_valid"] is False
    assert obs["correlated"] is False
    assert obs["result_ok"] is False
    assert ev["all_observations_confirmed"] is False
    blob = json.dumps(ev)
    if isinstance(args_reason, str) and args_reason:
        assert args_reason not in blob or args_reason.strip() == ""
    # No repr leak for non-string types.
    assert "True" not in blob or obs["reason"] is None


async def test_observation_reason_canary_never_echoed():
    call = _obs_call("o1", CHANNEL, SOURCE_MID, JOB, CANARY)
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    obs = ev["observation_calls"][0]
    assert obs["reason"] is None
    assert obs["reason_valid"] is False
    assert CANARY not in json.dumps(ev)


async def test_observation_reason_present_on_missing_and_corrupt_results():
    # Missing result: reason still bound from ARGS.
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["reason"] == "missing_context"
    assert ev["observation_calls"][0]["result_present"] is False
    assert ev["all_observations_confirmed"] is False
    # Corrupt result: reason still bound, proof still fails closed.
    payload2 = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "no_reply_needed")],
            ),
            _msg("tool", "not json", tool_call_id="o1"),
        ],
    )
    ev2 = await _evidence(payload2)
    assert ev2["observation_calls"][0]["reason"] == "no_reply_needed"
    assert ev2["observation_calls"][0]["result_ok"] is False
    assert ev2["all_observations_confirmed"] is False
    # Invalid ARG + missing result: reason None.
    bad = {
        "id": "o1",
        "name": NO_REPLY_TOOL,
        "arguments": {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": "bogus",
        },
    }
    payload3 = _trace(
        VALID_SID,
        2,
        "completed",
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=[bad])],
    )
    ev3 = await _evidence(payload3)
    assert ev3["observation_calls"][0]["reason"] is None


async def test_multiple_distinct_positive_observations_confirm():
    # >=1 consistent idempotent positives with distinct unique IDs can
    # complete at trace level; Router must still require EACH reason ==
    # persisted declaration reason (trace only binds ARG==RESULT).
    c1 = _obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")
    c2 = _obs_call("o2", CHANNEL, SOURCE_MID, JOB, "missing_context")
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[c1, c2]),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
            _obs_result("o2", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 2
    assert ev["observation_calls"][0]["reason"] == "missing_context"
    assert ev["observation_calls"][1]["reason"] == "missing_context"
    assert all(o["result_ok"] for o in ev["observation_calls"])
    assert ev["all_observations_confirmed"] is True


# ---- duplicate tool-result rows fail closed (observation ONLY) ----


async def test_duplicate_identical_tool_results_fail_closed():
    body = json.dumps(
        {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": "missing_context",
            "status": "declared",
            "future_result": CANARY,
        }
    )
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _msg("tool", body, tool_call_id="o1"),
            _msg("tool", body, tool_call_id="o1"),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_call_count"] == 1
    obs = ev["observation_calls"][0]
    assert obs["reason"] == "missing_context"
    assert obs["result_present"] is True
    assert obs["result_ok"] is False
    assert ev["all_observations_confirmed"] is False
    # Counts/info bounded; no raw payload leak.
    assert ev["send_call_count"] == 0
    assert ev["other_tool_call_count"] == 1
    assert CANARY not in json.dumps(ev)


async def test_contradictory_tool_results_fail_closed():
    good = json.dumps(
        {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": "missing_context",
            "status": "declared",
        }
    )
    # First positive + second meta error.
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _msg("tool", good, tool_call_id="o1"),
            _msg("tool", good, tool_call_id="o1", meta={"is_error": True}),
        ],
    )
    ev = await _evidence(payload)
    assert ev["observation_calls"][0]["result_ok"] is False
    assert ev["all_observations_confirmed"] is False
    # First positive + second wrong reason.
    wrong = json.dumps(
        {
            "channel_id": CHANNEL,
            "source_message_id": SOURCE_MID,
            "job_ref": JOB,
            "reason": "no_reply_needed",
            "status": "declared",
        }
    )
    payload2 = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _msg("tool", good, tool_call_id="o1"),
            _msg("tool", wrong, tool_call_id="o1"),
        ],
    )
    ev2 = await _evidence(payload2)
    assert ev2["observation_calls"][0]["reason"] == "missing_context"
    assert ev2["observation_calls"][0]["result_ok"] is False
    assert ev2["all_observations_confirmed"] is False
    # Counts/info bounded; unrelated SEND proof untouched.
    assert ev2["observation_call_count"] == 1
    assert ev2["send_call_count"] == 0


async def test_send_duplicate_first_wins_preserved():
    # SEND keeps legacy first-wins; only NEW observation proof requires
    # exactly-one tool-result row. Proves existing SEND semantics intact.
    call = _send_call("s1", CHANNEL, SOURCE_MID, JOB, content="hi")
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _send_result("s1", 777, CHANNEL, _jump(777)),
            _send_result("s1", 777, CHANNEL, _jump(777)),
        ],
    )
    ev = await _evidence(payload)
    assert ev["send_call_count"] == 1
    assert ev["sends"][0]["result_ok"] is True
    assert ev["all_sends_confirmed"] is True
    assert ev["observation_call_count"] == 0
    assert ev["all_observations_confirmed"] is False
