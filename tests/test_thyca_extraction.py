"""Thyca single-module tests: canonical surface and pure trace equivalence.

Offline only via injected transports and direct pure calls. No real native,
network, or env. The six former ``thyca_*`` modules are merged into one
``thyca`` module (verbatim bodies); these tests pin its public surface,
constants, and trace equivalence.
"""

from __future__ import annotations

import inspect
import json

import pytest

import thyca
from thyca import ThycaClient, ThycaProtocolError, build_trace_evidence

VALID_SID = "2026-10-05T12-34-56_ab12"
BASE = "http://127.0.0.1:8765"
JOB = "job_ref_abc123"
SOURCE_MID = 111111111111111111
CHANNEL = 222222222222222222


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


def _obs_call(call_id, ch, mid, job, reason):
    return {
        "id": call_id,
        "name": "discord__record_no_reply",
        "arguments": {
            "channel_id": ch,
            "source_message_id": mid,
            "job_ref": job,
            "reason": reason,
        },
    }


def _obs_result(call_id, ch, mid, job, reason, status="declared"):
    return _msg(
        "tool",
        json.dumps(
            {
                "channel_id": ch,
                "source_message_id": mid,
                "job_ref": job,
                "reason": reason,
                "status": status,
            }
        ),
        tool_call_id=call_id,
    )


def _jump(mid):
    return f"https://discord.com/channels/9/{CHANNEL}/{mid}"


class FakeTransport:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return 200, json.dumps(self.payload).encode()


def test_canonical_error_identities() -> None:
    assert thyca.ThycaClientError is thyca.ThycaClientError
    assert thyca.ThycaPreconditionError is thyca.ThycaPreconditionError
    assert thyca.ThycaBusyError is thyca.ThycaBusyError
    assert thyca.ThycaSubmissionUncertainError is thyca.ThycaSubmissionUncertainError
    assert thyca.ThycaProtocolError is thyca.ThycaProtocolError
    assert thyca._TimeoutMarker is thyca._TimeoutMarker
    assert thyca._NetworkMarker is thyca._NetworkMarker


def test_canonical_transport_identities() -> None:
    assert thyca.MAX_RESPONSE_BYTES is thyca.MAX_RESPONSE_BYTES
    assert thyca.DEFAULT_REQUEST_TIMEOUT_SECONDS is thyca.DEFAULT_REQUEST_TIMEOUT_SECONDS
    assert thyca.HttpTransport is thyca.HttpTransport
    assert thyca._declared_body_len is thyca._declared_body_len
    assert thyca._read_bounded is thyca._read_bounded


def test_canonical_validate_helpers_and_pinned_constants() -> None:
    for name in (
        "_normalize_base_url",
        "_validate_sid",
        "_validate_text",
        "_validate_model_effort",
        "_validate_turn_index",
        "_validate_job_ref",
        "_validate_expected_title",
        "_validate_discord_id",
        "_validate_message_roles",
        "_parse_json_dict",
        "_map_http_status",
        "_turn_slice_counts",
        "_sha256_hex",
    ):
        assert getattr(thyca, name) is not None, name
    # Pinned SID grammar and protocol bounds/tool keys (native-exact).
    assert thyca.SESSION_ID_PATTERN == (r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_[0-9a-f]{4}")
    assert thyca.TEXT_MAX == 4000
    assert thyca.SEND_TOOL_NAME == "discord__send_message"
    assert thyca.NO_REPLY_TOOL_NAME == "discord__record_no_reply"
    assert thyca.NO_REPLY_REASONS == frozenset({"no_reply_needed", "missing_context"})
    assert thyca.NO_REPLY_STATUS_DECLARED == "declared"


def test_transport_leaf_has_explicit_timeout() -> None:
    sig = inspect.signature(thyca.sync_http_request)
    params = list(sig.parameters.values())
    assert [p.name for p in params] == ["method", "url", "body", "request_timeout_seconds"]
    assert params[3].default is inspect.Parameter.empty


async def _root_evidence(payload):
    client = ThycaClient(BASE, transport=FakeTransport(payload))
    return await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )


def _leaf_evidence(payload):
    return build_trace_evidence(
        payload,
        native_sid=VALID_SID,
        turn_index=2,
        job_ref=JOB,
        source_message_id=SOURCE_MID,
        channel_id=CHANNEL,
    )


async def test_pure_trace_matches_root_send_happy() -> None:
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"please [{JOB}]"),
            _msg("assistant", None, tool_calls=[_send_call("c1", CHANNEL, SOURCE_MID, JOB)]),
            _send_result("c1", 444, CHANNEL, _jump(444)),
        ],
    )
    assert _leaf_evidence(payload) == await _root_evidence(payload)


async def test_pure_trace_matches_root_observation_happy() -> None:
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [
            _msg("user", f"please [{JOB}]"),
            _msg(
                "assistant",
                None,
                tool_calls=[_obs_call("o1", CHANNEL, SOURCE_MID, JOB, "missing_context")],
            ),
            _obs_result("o1", CHANNEL, SOURCE_MID, JOB, "missing_context"),
        ],
    )
    leaf = _leaf_evidence(payload)
    assert leaf == await _root_evidence(payload)
    assert leaf["all_observations_confirmed"] is True
    assert leaf["observation_calls"][0]["result_ok"] is True


async def test_pure_trace_matches_root_no_proof() -> None:
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
    leaf = _leaf_evidence(payload)
    assert leaf == await _root_evidence(payload)
    assert leaf["observation_calls"][0]["result_present"] is False
    assert leaf["all_observations_confirmed"] is False


async def test_pure_trace_matches_root_malformed() -> None:
    payload = _trace(
        VALID_SID,
        2,
        "completed",
        [_msg("user", f"first [{JOB}]"), _msg("user", f"second [{JOB}]")],
    )
    with pytest.raises(ThycaProtocolError) as leaf_exc:
        _leaf_evidence(payload)
    assert leaf_exc.value.reason == "malformed"
    with pytest.raises(ThycaProtocolError) as root_exc:
        await _root_evidence(payload)
    assert root_exc.value.reason == leaf_exc.value.reason
