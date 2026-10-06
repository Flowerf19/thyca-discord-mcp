"""T4 extraction tests: canonical leaves and pure trace equivalence.

Offline only via injected transports and direct pure calls. No real native,
network, or env. Proves Root re-exports are single-object aliases and the
pure trace leaf matches the Root API on known fixtures.
"""

from __future__ import annotations

import inspect
import json

import pytest

import modum.thyca_client as root
import modum.thyca_errors as errors
import modum.thyca_trace as trace
import modum.thyca_transport as transport
import modum.thyca_validate as validate
from modum.thyca_client import ThycaClient, ThycaProtocolError
from modum.thyca_trace import build_trace_evidence

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
    assert root.ThycaClientError is errors.ThycaClientError
    assert root.ThycaPreconditionError is errors.ThycaPreconditionError
    assert root.ThycaBusyError is errors.ThycaBusyError
    assert root.ThycaSubmissionUncertainError is errors.ThycaSubmissionUncertainError
    assert root.ThycaProtocolError is errors.ThycaProtocolError
    assert root._TimeoutMarker is errors._TimeoutMarker
    assert root._NetworkMarker is errors._NetworkMarker


def test_canonical_transport_identities() -> None:
    assert root.MAX_RESPONSE_BYTES is transport.MAX_RESPONSE_BYTES
    assert root.DEFAULT_REQUEST_TIMEOUT_SECONDS is transport.DEFAULT_REQUEST_TIMEOUT_SECONDS
    assert root.HttpTransport is transport.HttpTransport
    assert root._declared_body_len is transport._declared_body_len
    assert root._read_bounded is transport._read_bounded


def test_canonical_validate_identities_and_pinned_constants() -> None:
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
        assert getattr(root, name) is getattr(validate, name), name
    # Pinned SID grammar and protocol bounds/tool keys (native-exact).
    assert validate.SESSION_ID_PATTERN == (r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_[0-9a-f]{4}")
    assert validate.TEXT_MAX == 4000
    assert validate.SEND_TOOL_NAME == "discord__send_message"
    assert validate.NO_REPLY_TOOL_NAME == "discord__record_no_reply"
    assert validate.NO_REPLY_REASONS == frozenset({"no_reply_needed", "missing_context"})
    assert validate.NO_REPLY_STATUS_DECLARED == "declared"


def test_no_cross_owned_defs() -> None:
    # Single owner: leaves must not define each other's symbols.
    assert not hasattr(transport, "_validate_sid")
    assert not hasattr(transport, "_normalize_base_url")
    assert not hasattr(validate, "_read_bounded")
    assert not hasattr(validate, "_declared_body_len")
    assert not hasattr(validate, "sync_http_request")
    assert not hasattr(validate, "build_trace_evidence")
    assert not hasattr(transport, "build_trace_evidence")
    assert not hasattr(trace, "sync_http_request")
    # Trace is pure: no client/transport imports, no private Root state.
    assert not hasattr(trace, "ThycaClient")
    assert "thyca_client" not in dir(trace)
    assert "thyca_transport" not in dir(trace)


def test_transport_leaf_has_explicit_timeout() -> None:
    sig = inspect.signature(transport.sync_http_request)
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
