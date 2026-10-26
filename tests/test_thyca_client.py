"""Offline tests for the thin native Thyca client (no real 8765)."""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import http.server
import json
import os
import socket
import threading
import urllib.error
import urllib.request
from unittest import mock

import pytest

from thyca import (
    MAX_RESPONSE_BYTES,
    ThycaBusyError,
    ThycaClient,
    ThycaClientError,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
    _declared_body_len,
    _NetworkMarker,
    _read_bounded,
    _TimeoutMarker,
)

VALID_SID = "2026-10-05T12-34-56_ab12"
OTHER_SID = "2026-10-04T01-02-03_cd34"
BASE = "http://127.0.0.1:8765"

CANARY_KEY = "INKCANARYKEY_9f3c"
CANARY_CONTENT = "INKCANARYCONTENT_7a1e"
CANARY_ERROR = "INKCANARYERROR_5b2d"
CANARY_PROFILE = "INKCANARYPROFILE_3c8f"
CANARY_MODEL = "INKCANARYMODEL_1d4a"
CANARY_TOOL = "INKCANARYTOOL_6e9b"
CANARY_TITLE = "INKCANARYTITLE_2f7c"
CANARY_PROXY_USER = "INKCANARYPROXYUSER_a1b2"  # noqa: S105
CANARY_PROXY_PW = "INKCANARYPROXYPW_c3d4"  # noqa: S105

JOB = "job_ref_abc123"
SOURCE_MID = 111111111111111111
CHANNEL = 222222222222222222
GUILD = 333333333333333333
RESULT_MID = 444444444444444444


def _jump(guild: int, channel: int, mid: int) -> str:
    return f"https://discord.com/channels/{guild}/{channel}/{mid}"


def _msg(
    role: str,
    content: str | None = None,
    *,
    tool_calls: list[dict] | None = None,
    tool_call_id: str | None = None,
    meta: dict | None = None,
) -> dict:
    out: dict = {"role": role, "content": content, "ts": "2026-10-05T12:00:00Z"}
    if tool_calls is not None:
        out["tool_calls"] = tool_calls
    if tool_call_id is not None:
        out["tool_call_id"] = tool_call_id
    if meta is not None:
        out["meta"] = meta
    return out


def _session_detail(sid: str, title: str, running: bool, messages: list[dict]) -> dict:
    return {
        "id": sid,
        "title": title,
        "model": "owner-default",
        "messages": messages,
        "ask_remember": False,
        "running": running,
    }


def _trace_detail(
    sid: str,
    index: int,
    status: str,
    model: str | None,
    error: dict | None,
    messages: list[dict],
) -> dict:
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


def _send_call(
    call_id: str,
    channel: int,
    source_mid: int,
    job: str | None,
    content: str = "hello",
    extra_args: dict | None = None,
) -> dict:
    args: dict = {
        "channel_id": channel,
        "source_message_id": source_mid,
        "content": content,
    }
    if job is not None:
        args["job_ref"] = job
    if extra_args:
        args.update(extra_args)
    return {"id": call_id, "name": "discord__send_message", "arguments": args}


def _send_result(call_id: str, mid: int, channel: int, jump: str) -> dict:
    body = json.dumps({"message_id": mid, "channel_id": channel, "jump_url": jump})
    return _msg("tool", body, tool_call_id=call_id)


class FakeTransport:
    def __init__(self, handler) -> None:  # noqa: ANN001
        self.handler = handler
        self.calls: list[tuple[str, str, bytes | None]] = []

    async def __call__(self, method: str, url: str, body: bytes | None) -> tuple[int, bytes]:
        self.calls.append((method, url, body))
        return await self.handler(method, url, body)


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def _assert_no_canary(obj: object, exc_text: str = "") -> None:
    blob = json.dumps(obj, default=str)
    for canary in (
        CANARY_KEY,
        CANARY_CONTENT,
        CANARY_ERROR,
        CANARY_PROFILE,
        CANARY_MODEL,
        CANARY_TOOL,
        CANARY_TITLE,
    ):
        assert canary not in blob, f"canary leaked in metadata: {canary}"
        assert canary not in exc_text, f"canary leaked in error: {canary}"


# ---- base_url + SID validation (zero HTTP on failure) ----


async def test_base_url_rejects_non_loopback_unsafe_shapes() -> None:
    bad = [
        "https://127.0.0.1:8765",
        "http://example.com:8765",
        "http://192.168.1.2:8765",
        "http://user:pass@127.0.0.1:8765",
        "http://127.0.0.1:8765?x=1",
        "http://127.0.0.1:8765#frag",
        "http://127.0.0.1:8765/base",
        "ftp://127.0.0.1:8765",
        "",
        "not-a-url",
        "http://0.0.0.0:8765",
    ]
    for url in bad:
        with pytest.raises(ThycaPreconditionError):
            ThycaClient(url)


async def test_base_url_accepts_loopback_hosts() -> None:
    for url in (
        "http://127.0.0.1:8765",
        "http://127.0.0.1:8765/",
        "http://localhost:8765",
        "http://[::1]:8765",
        "http://127.0.0.2:9999",
    ):
        client = ThycaClient(url)
        assert client.base_url.startswith("http://")


async def test_sid_validation_fails_before_http() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise AssertionError("must not call HTTP")

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    bad_sids = [
        "",
        "not-a-sid",
        "2026-10-05T12-34-56_AB12",
        "2026-10-05 12-34-56_ab12",
        "../etc/passwd",
        "2026-10-05T12-34-56_ab12/turn",
        "123e4567-e89b-12d3-a456-426614174000",
    ]
    for sid in bad_sids:
        with pytest.raises(ThycaPreconditionError):
            await client.get_session_snapshot(sid)
        with pytest.raises(ThycaPreconditionError):
            await client.submit_turn(sid, "hi")
        with pytest.raises(ThycaPreconditionError):
            await client.get_trace_evidence(
                sid, 0, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
            )
    assert transport.calls == []


async def test_exact_post_body_omits_model_effort_by_default() -> None:
    seen: dict = {}

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        assert method == "POST"
        seen["url"] = url
        seen["body"] = json.loads(body.decode("utf-8")) if body else None
        detail = _session_detail(
            VALID_SID, "t", False, [_msg("user", "hello"), _msg("assistant", "ok")]
        )
        return 200, _json_bytes(detail)

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    result = await client.submit_turn(VALID_SID, "  hello  ")
    assert seen["body"] == {"text": "hello"}
    assert seen["url"] == f"{BASE}/api/sessions/{VALID_SID}/turn"
    assert result["completed"] is True
    assert result["stored_text_match"] is True
    assert result["turn_index"] == 0


async def test_exact_post_body_includes_model_effort_when_set() -> None:
    seen: dict = {}

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        seen["body"] = json.loads(body.decode("utf-8")) if body else None
        detail = _session_detail(
            VALID_SID, "t", False, [_msg("user", "hi"), _msg("assistant", "ok")]
        )
        return 200, _json_bytes(detail)

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    await client.submit_turn(VALID_SID, "hi", model="m1", effort="high")
    assert seen["body"] == {"text": "hi", "model": "m1", "effort": "high"}


async def test_text_4000_bound_no_silent_truncation_zero_calls() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise AssertionError("must not call HTTP")

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    ok_text = "x" * 4000

    async def _ok_handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        payload = json.loads(body.decode("utf-8")) if body else {}
        assert payload["text"] == ok_text
        detail = _session_detail(
            VALID_SID, "t", False, [_msg("user", ok_text), _msg("assistant", "ok")]
        )
        return 200, _json_bytes(detail)

    ok_transport = FakeTransport(_ok_handler)
    ok_client = ThycaClient(BASE, transport=ok_transport)
    result = await ok_client.submit_turn(VALID_SID, ok_text)
    assert result["submitted_text_len"] == 4000

    for bad in ["", "   ", "x" * 4001, 123, None]:
        with pytest.raises(ThycaPreconditionError):
            await client.submit_turn(VALID_SID, bad)  # type: ignore[arg-type]
    for bad_model in ["", "  ", "a\nb", "x" * 201, 123]:
        with pytest.raises(ThycaPreconditionError):
            await client.submit_turn(VALID_SID, "hi", model=bad_model)  # type: ignore[arg-type]
    for bad_effort in ["", "  ", 123]:
        with pytest.raises(ThycaPreconditionError):
            await client.submit_turn(VALID_SID, "hi", effort=bad_effort)  # type: ignore[arg-type]
    assert transport.calls == []


async def test_no_create_or_sid_override_urls() -> None:
    urls: list[str] = []

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        urls.append(f"{method} {url}")
        if method == "GET" and url.endswith(f"/api/sessions/{VALID_SID}"):
            return 200, _json_bytes(_session_detail(VALID_SID, "t", False, []))
        if method == "POST":
            detail = _session_detail(
                VALID_SID, "t", False, [_msg("user", "hi"), _msg("assistant", "ok")]
            )
            return 200, _json_bytes(detail)
        if "/api/traces/" in url:
            payload = _trace_detail(VALID_SID, 0, "completed", None, None, [])
            return 200, _json_bytes(payload)
        raise AssertionError(f"unexpected {method} {url}")

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    await client.get_session_snapshot(VALID_SID)
    await client.submit_turn(VALID_SID, "hi")
    await client.get_trace_evidence(
        VALID_SID, 0, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert urls == [
        f"GET {BASE}/api/sessions/{VALID_SID}",
        f"POST {BASE}/api/sessions/{VALID_SID}/turn",
        f"GET {BASE}/api/traces/{VALID_SID}/0",
    ]
    for entry in urls:
        assert "/api/sessions HTTP" not in entry
        assert entry.count(VALID_SID) == 1


# ---- coarse errors, no freeform leakage ----


async def test_busy_precondition_protocol_coarse_no_canary() -> None:
    async def _busy(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 409, _json_bytes({"error": f"session busy {CANARY_ERROR}"})

    client = ThycaClient(BASE, transport=FakeTransport(_busy))
    with pytest.raises(ThycaBusyError) as excinfo:
        await client.submit_turn(VALID_SID, "hi")
    assert CANARY_ERROR not in str(excinfo.value)
    assert CANARY_ERROR not in repr(excinfo.value)

    for code in (400, 404):

        async def _pre(method: str, url: str, body: bytes | None, _c=code):  # noqa: ANN001, ANN202
            return _c, _json_bytes({"error": f"bad {CANARY_CONTENT}"})

        pre_client = ThycaClient(BASE, transport=FakeTransport(_pre))
        with pytest.raises(ThycaPreconditionError) as excinfo2:
            await pre_client.submit_turn(VALID_SID, "hi")
        assert CANARY_CONTENT not in str(excinfo2.value)

    async def _proto(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 503, f"secret {CANARY_KEY}".encode()

    proto_client = ThycaClient(BASE, transport=FakeTransport(_proto))
    with pytest.raises(ThycaProtocolError) as excinfo3:
        await proto_client.get_session_snapshot(VALID_SID)
    assert CANARY_KEY not in str(excinfo3.value)

    async def _badjson(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, b"not json"

    bad_client = ThycaClient(BASE, transport=FakeTransport(_badjson))
    with pytest.raises(ThycaProtocolError):
        await bad_client.get_session_snapshot(VALID_SID)


async def test_redirect_status_is_protocol_not_followed() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 302, b""

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaProtocolError) as excinfo:
        await client.get_session_snapshot(VALID_SID)
    assert excinfo.value.reason == "redirect"


async def test_post_timeout_maps_to_submission_uncertain() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise TimeoutError("slow provider")

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaSubmissionUncertainError) as excinfo:
        await client.submit_turn(VALID_SID, "hi")
    assert "slow provider" not in str(excinfo.value)
    assert excinfo.value.kind == "submission_uncertain"


async def test_get_timeout_maps_to_protocol() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise TimeoutError("slow")

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaProtocolError) as excinfo:
        await client.get_session_snapshot(VALID_SID)
    assert "slow" not in str(excinfo.value)


async def test_cancelled_post_propagates_cancelled() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise asyncio.CancelledError

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(asyncio.CancelledError):
        await client.submit_turn(VALID_SID, "hi")


async def test_native_200_cancelled_is_not_completed() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, _json_bytes({"cancelled": True})

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    result = await client.submit_turn(VALID_SID, "hi")
    assert result["cancelled"] is True
    assert result["completed"] is False
    assert result["turn_index"] is None


async def test_opaque_pass_reply_never_returned() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        detail = _session_detail(
            VALID_SID, "t", False, [_msg("user", "hi"), _msg("assistant", "PASS")]
        )
        detail["reply"] = "PASS"
        return 200, _json_bytes(detail)

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    result = await client.submit_turn(VALID_SID, "hi")
    blob = json.dumps(result)
    assert "PASS" not in blob
    assert result["completed"] is True


# ---- snapshot ----


async def test_snapshot_uses_target_session_not_window_miss() -> None:
    messages: list[dict] = [_msg("system", "[compaction: x]")]
    for i in range(300):
        messages.append(_msg("user", f"q{i}"))
        messages.append(_msg("assistant", f"a{i}"))

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        assert method == "GET"
        assert url == f"{BASE}/api/sessions/{VALID_SID}"
        assert "/api/traces" not in url
        return 200, _json_bytes(_session_detail(VALID_SID, "discord-main", True, messages))

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    snap = await client.get_session_snapshot(VALID_SID, expected_title="discord-main")
    assert snap["turn_count"] == 300
    assert snap["pre_max_turn_index"] == 299
    assert snap["turn_indices"] == list(range(300))
    assert snap["running"] is True
    assert snap["native_id_match"] is True
    assert snap["title_match"] is True
    assert snap["proof"] == "session_detail_turn_slices"
    assert snap["title_len"] == len("discord-main")
    assert "discord-main" not in json.dumps(snap)


async def test_snapshot_title_mismatch_and_id_mismatch() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, _json_bytes(_session_detail(OTHER_SID, "wrong", False, []))

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    snap = await client.get_session_snapshot(VALID_SID, expected_title="discord-main")
    assert snap["native_id_match"] is False
    assert snap["title_match"] is False
    assert snap["turn_count"] == 0
    assert snap["pre_max_turn_index"] is None


async def test_snapshot_malformed_roles_fail_closed() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, _json_bytes(_session_detail(VALID_SID, "t", False, [{"role": "narrator"}]))

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaProtocolError):
        await client.get_session_snapshot(VALID_SID)


# ---- trace evidence ----


def _good_trace_messages(job: str = JOB) -> list[dict]:
    call = _send_call("call_1", CHANNEL, SOURCE_MID, job, content="reply-body")
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    return [
        _msg("user", f"please reply [{job}] now"),
        _msg("assistant", None, tool_calls=[call]),
        result,
    ]


async def _trace_client(payload: dict) -> ThycaClient:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        assert method == "GET"
        assert url == f"{BASE}/api/traces/{VALID_SID}/2"
        return 200, _json_bytes(payload)

    return ThycaClient(BASE, transport=FakeTransport(_handler))


async def test_trace_correlated_send_confirmed() -> None:
    payload = _trace_detail(VALID_SID, 2, "completed", "owner-model", None, _good_trace_messages())
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID,
        2,
        job_ref=JOB,
        source_message_id=SOURCE_MID,
        channel_id=CHANNEL,
        guild_id=GUILD,
    )
    assert evidence["trace_session_match"] is True
    assert evidence["turn_index_match"] is True
    assert evidence["request_job_ref_match"] is True
    assert evidence["send_call_count"] == 1
    assert evidence["correlated_send_count"] == 1
    assert evidence["legacy_uncorrelated_count"] == 0
    assert evidence["all_sends_confirmed"] is True
    send = evidence["sends"][0]
    assert send["tool_call_id"] == "call_1"
    assert send["channel_match"] is True
    assert send["source_mid_match"] is True
    assert send["job_ref_match"] is True
    assert send["result_message_id"] == RESULT_MID
    assert send["result_channel_match"] is True
    assert send["jump_ids_match"] is True
    assert send["result_ok"] is True
    assert send["content_len"] == len("reply-body")
    assert send["content_sha256"] == hashlib.sha256(b"reply-body").hexdigest()
    assert "reply-body" not in json.dumps(evidence)


async def test_trace_wrong_job_ref_source_channel_fails() -> None:
    payload = _trace_detail(
        VALID_SID, 2, "completed", None, None, _good_trace_messages(job="other-job")
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["request_job_ref_match"] is False
    assert evidence["all_sends_confirmed"] is False
    assert evidence["sends"][0]["job_ref_match"] is False
    assert evidence["sends"][0]["result_ok"] is False


async def test_trace_late_tool_missing_result() -> None:
    call = _send_call("call_late", CHANNEL, SOURCE_MID, JOB)
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", None, tool_calls=[call])],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["sends"][0]["result_present"] is False
    assert evidence["sends"][0]["result_ok"] is False
    assert evidence["all_sends_confirmed"] is False


async def test_trace_wrong_native_sid_no_false_pass() -> None:
    payload = _trace_detail(OTHER_SID, 2, "completed", None, None, _good_trace_messages())
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["trace_session_match"] is False
    assert evidence["all_sends_confirmed"] is False


async def test_trace_mismatched_jump_and_channel_no_pass() -> None:
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB)
    bad_jump = _jump(GUILD, CHANNEL, 999999)
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _send_result("call_1", RESULT_MID, CHANNEL, bad_jump),
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["sends"][0]["jump_ids_match"] is False
    assert evidence["sends"][0]["result_ok"] is False
    assert evidence["all_sends_confirmed"] is False

    call_wrong = _send_call("call_2", 999000111222333444, SOURCE_MID, JOB)
    payload2 = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call_wrong]),
            _send_result("call_2", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID)),
        ],
    )
    client2 = await _trace_client(payload2)
    evidence2 = await client2.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence2["sends"][0]["channel_match"] is False
    assert evidence2["sends"][0]["result_ok"] is False


async def test_trace_no_sends_is_not_confirmed() -> None:
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("user", f"hi [{JOB}]"), _msg("assistant", "PASS")],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["send_call_count"] == 0
    assert evidence["all_sends_confirmed"] is False
    assert "PASS" not in json.dumps(evidence)


async def test_trace_tool_error_json_no_false_pass() -> None:
    call = _send_call("call_err", CHANNEL, SOURCE_MID, JOB)
    err_body = json.dumps({"error": "forbidden"})
    payload = _trace_detail(
        VALID_SID,
        2,
        "failed",
        None,
        {"code": "llm_error", "message": "provider down"},
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _msg("tool", err_body, tool_call_id="call_err", meta={"is_error": True}),
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["sends"][0]["result_ok"] is False
    assert evidence["all_sends_confirmed"] is False
    assert evidence["error_present"] is True
    assert evidence["error_kind"] == "llm_error"
    assert "forbidden" not in json.dumps(evidence)
    assert "provider down" not in json.dumps(evidence)


async def test_trace_legacy_uncorrelated_classified_separately() -> None:
    legacy = _send_call("call_legacy", CHANNEL, SOURCE_MID, None)
    correlated = _send_call("call_new", CHANNEL, SOURCE_MID, JOB)
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[legacy, correlated]),
            _send_result(
                "call_legacy",
                555555555555555555,
                CHANNEL,
                _jump(GUILD, CHANNEL, 555555555555555555),
            ),
            _send_result("call_new", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID)),
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["send_call_count"] == 2
    assert evidence["correlated_send_count"] == 1
    assert evidence["legacy_uncorrelated_count"] == 1
    assert evidence["all_sends_confirmed"] is True


async def test_trace_unknown_tool_names_not_leaked() -> None:
    weird = {"id": "call_x", "name": CANARY_TOOL, "arguments": {"a": 1}}
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[weird]),
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["other_tool_call_count"] == 1
    assert evidence["other_tool_name_lens"] == [len(CANARY_TOOL)]
    _assert_no_canary(evidence)


async def test_trace_guild_mismatch_fails_jump() -> None:
    payload = _trace_detail(VALID_SID, 2, "completed", None, None, _good_trace_messages())
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID,
        2,
        job_ref=JOB,
        source_message_id=SOURCE_MID,
        channel_id=CHANNEL,
        guild_id=999888777666555444,
    )
    assert evidence["sends"][0]["jump_ids_match"] is False
    assert evidence["all_sends_confirmed"] is False


async def test_trace_bool_ids_rejected_before_http() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise AssertionError("must not call HTTP")

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    with pytest.raises(ThycaPreconditionError):
        await client.get_trace_evidence(
            VALID_SID,
            0,
            job_ref=JOB,
            source_message_id=True,
            channel_id=CHANNEL,  # type: ignore[arg-type]
        )
    with pytest.raises(ThycaPreconditionError):
        await client.get_trace_evidence(
            VALID_SID,
            0,
            job_ref=JOB,
            source_message_id=SOURCE_MID,
            channel_id=False,  # type: ignore[arg-type]
        )
    with pytest.raises(ThycaPreconditionError):
        await client.get_trace_evidence(
            VALID_SID,
            True,
            job_ref=JOB,
            source_message_id=SOURCE_MID,
            channel_id=CHANNEL,  # type: ignore[arg-type]
        )
    with pytest.raises(ThycaPreconditionError):
        await client.get_trace_evidence(
            VALID_SID, 0, job_ref="", source_message_id=SOURCE_MID, channel_id=CHANNEL
        )
    assert transport.calls == []


async def test_trace_bool_result_mid_is_not_ok() -> None:
    call = _send_call("call_bool", CHANNEL, SOURCE_MID, JOB)
    body = json.dumps(
        {"message_id": True, "channel_id": CHANNEL, "jump_url": _jump(GUILD, CHANNEL, 1)}
    )
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("user", f"hi [{JOB}]"),
            _msg("assistant", None, tool_calls=[call]),
            _msg("tool", body, tool_call_id="call_bool"),
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["sends"][0]["result_message_id"] is None
    assert evidence["sends"][0]["result_ok"] is False


async def test_canary_never_in_any_metadata() -> None:
    user_text = f"hello {CANARY_CONTENT} [{JOB}]"
    call = _send_call(
        "call_c",
        CHANNEL,
        SOURCE_MID,
        JOB,
        content=CANARY_CONTENT,
        extra_args={CANARY_KEY: CANARY_CONTENT, "profile": CANARY_PROFILE},
    )
    result = _send_result("call_c", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    trace_payload = _trace_detail(
        VALID_SID,
        0,
        "completed",
        CANARY_MODEL,
        {"code": CANARY_KEY, "message": CANARY_ERROR, "extra": CANARY_PROFILE},
        [_msg("user", user_text), _msg("assistant", None, tool_calls=[call]), result],
    )

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        if url.endswith(f"/api/sessions/{VALID_SID}") and method == "GET":
            detail = _session_detail(
                VALID_SID,
                CANARY_TITLE,
                False,
                [_msg("user", user_text), _msg("assistant", CANARY_CONTENT)],
            )
            return 200, _json_bytes(detail)
        if method == "POST":
            detail = _session_detail(
                VALID_SID,
                CANARY_TITLE,
                False,
                [_msg("user", user_text), _msg("assistant", CANARY_CONTENT)],
            )
            detail["reply"] = CANARY_CONTENT
            return 200, _json_bytes(detail)
        return 200, _json_bytes(trace_payload)

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    snap = await client.get_session_snapshot(VALID_SID)
    _assert_no_canary(snap)
    posted = await client.submit_turn(VALID_SID, user_text)
    _assert_no_canary(posted)
    evidence = await client.get_trace_evidence(
        VALID_SID, 0, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    _assert_no_canary(evidence)
    assert evidence["error_kind"] == "unknown"
    assert evidence["status"] == "completed"


# ---- real socket behavior with local fake server (default transport) ----


class _SlowHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        import time

        time.sleep(0.3)
        payload = _session_detail(VALID_SID, "t", False, []).copy()
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


class _RedirectHandler(http.server.BaseHTTPRequestHandler):
    hits: list[str] = []

    def do_GET(self) -> None:  # noqa: N802
        type(self).hits.append(self.path)
        if self.path == "/api/sessions/" + VALID_SID:
            self.send_response(302)
            self.send_header("Location", "http://example.com/evil")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def _serve(handler_cls):  # noqa: ANN001, ANN202
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    server.test_thread = thread  # type: ignore[attr-defined]
    return server


def _stop(server) -> None:  # noqa: ANN001
    """Strict bounded teardown: stop, close, and join the serve thread."""
    server.shutdown()
    server.server_close()
    thread = getattr(server, "test_thread", None)
    if isinstance(thread, threading.Thread):
        thread.join(timeout=5)
        assert not thread.is_alive(), "server thread did not stop"


async def test_default_transport_does_not_block_event_loop() -> None:
    server = _serve(_SlowHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        ticks = 0

        async def _ticker() -> None:
            nonlocal ticks
            for _ in range(20):
                await asyncio.sleep(0.02)
                ticks += 1

        ticker = asyncio.create_task(_ticker())
        snapshot = await client.get_session_snapshot(VALID_SID)
        await ticker
        assert snapshot["native_id_match"] is True
        assert ticks >= 5
    finally:
        _stop(server)


async def test_default_transport_blocks_redirect_to_external() -> None:
    _RedirectHandler.hits = []
    server = _serve(_RedirectHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        with pytest.raises(ThycaProtocolError) as excinfo:
            await client.get_session_snapshot(VALID_SID)
        assert excinfo.value.reason == "redirect"
        assert _RedirectHandler.hits == [f"/api/sessions/{VALID_SID}"]
    finally:
        _stop(server)


async def test_exceptions_are_coarse_thyca_errors() -> None:
    assert issubclass(ThycaBusyError, ThycaClientError)
    assert issubclass(ThycaPreconditionError, ThycaClientError)
    assert issubclass(ThycaProtocolError, ThycaClientError)
    assert issubclass(ThycaSubmissionUncertainError, ThycaClientError)


# ---- proxy isolation (default transport ignores environment proxies) ----


class _ProxyTargetHandler(http.server.BaseHTTPRequestHandler):
    hits: list[tuple[str, str, bytes]] = []

    def _send_detail(self, user_text: str) -> None:
        payload = _session_detail(
            VALID_SID, "t", False, [_msg("user", user_text), _msg("assistant", "ok")]
        )
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        type(self).hits.append(("GET", self.path, b""))
        self._send_detail("hi-target")

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b""
        type(self).hits.append(("POST", self.path, raw))
        try:
            posted = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, ValueError):
            posted = {}
        text = posted.get("text") if isinstance(posted, dict) else None
        self._send_detail(text if isinstance(text, str) else "hi-target")

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


class _ProxySpyHandler(http.server.BaseHTTPRequestHandler):
    hits: list[tuple[str, str]] = []

    def _record(self) -> None:
        type(self).hits.append((self.command, self.path))
        body = b"proxy must not receive loopback requests"
        self.send_response(500)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._record()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                self.rfile.read(length)
            except (ConnectionResetError, BrokenPipeError):
                pass
        self._record()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


_PROXY_ENV_KEYS = (
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "no_proxy",
    "NO_PROXY",
    "REQUEST_METHOD",
)


def _install_hostile_proxy_env(proxy_port: int, *, no_proxy_mode: str) -> dict[str, str]:
    saved: dict[str, str] = {}
    for key in _PROXY_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ[key]
        os.environ.pop(key, None)
    proxy_url = f"http://{CANARY_PROXY_USER}:{CANARY_PROXY_PW}@127.0.0.1:{proxy_port}"
    for key in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        os.environ[key] = proxy_url
    if no_proxy_mode == "empty":
        os.environ["NO_PROXY"] = ""
        os.environ["no_proxy"] = ""
    # "removed" mode leaves NO_PROXY/no_proxy/REQUEST_METHOD unset.
    return saved


def _restore_proxy_env(saved: dict[str, str]) -> None:
    for key in _PROXY_ENV_KEYS:
        os.environ.pop(key, None)
    os.environ.update(saved)


async def test_proxy_env_ignored_target_only_no_canary_leak() -> None:
    _ProxyTargetHandler.hits = []
    _ProxySpyHandler.hits = []
    target = _serve(_ProxyTargetHandler)
    proxy = _serve(_ProxySpyHandler)
    try:
        target_port = target.server_address[1]
        proxy_port = proxy.server_address[1]
        for mode in ("removed", "empty"):
            saved = _install_hostile_proxy_env(proxy_port, no_proxy_mode=mode)
            try:
                _ProxyTargetHandler.hits.clear()
                _ProxySpyHandler.hits.clear()
                client = ThycaClient(f"http://127.0.0.1:{target_port}", request_timeout_seconds=5)
                post_text = f"hello-proxy-check-{mode}"
                try:
                    snap = await client.get_session_snapshot(VALID_SID)
                    posted = await client.submit_turn(VALID_SID, post_text)
                except ThycaClientError as exc:
                    blob = f"{exc!s} {exc!r}"
                    assert CANARY_PROXY_USER not in blob
                    assert CANARY_PROXY_PW not in blob
                    raise AssertionError(f"proxy leak caused {exc.kind} in mode={mode}") from exc
                assert snap["native_id_match"] is True
                assert posted["completed"] is True
                assert posted["stored_text_match"] is True
                # Proxy must receive ZERO calls; target receives both + POST text.
                assert _ProxySpyHandler.hits == [], f"mode={mode}"
                assert len(_ProxyTargetHandler.hits) == 2
                methods = [m for m, _p, _b in _ProxyTargetHandler.hits]
                assert methods == ["GET", "POST"]
                post_body = _ProxyTargetHandler.hits[1][2]
                assert post_text.encode() in post_body
            finally:
                _restore_proxy_env(saved)
    finally:
        _stop(target)
        _stop(proxy)


# ---- response-body timeout / reset (default transport) ----


class _StallBodyHandler(http.server.BaseHTTPRequestHandler):
    def _stall(self, payload: dict) -> None:
        import time

        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        # Stall past the client timeout before sending the body.
        time.sleep(0.6)
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._stall(_session_detail(VALID_SID, "t", False, []))

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                self.rfile.read(length)
            except (ConnectionResetError, BrokenPipeError):
                pass
        detail = _session_detail(
            VALID_SID, "t", False, [_msg("user", "hi"), _msg("assistant", "ok")]
        )
        self._stall(detail)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


class _ResetBodyHandler(http.server.BaseHTTPRequestHandler):
    def _reset(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "1000")
        self.end_headers()
        try:
            self.wfile.write(f'{{"id": "partial {CANARY_CONTENT}'.encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._reset()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                self.rfile.read(length)
            except (ConnectionResetError, BrokenPipeError):
                pass
        self._reset()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


async def test_default_transport_post_body_stall_is_submission_uncertain() -> None:
    server = _serve(_StallBodyHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=0.2)
        with pytest.raises(ThycaSubmissionUncertainError) as excinfo:
            await client.submit_turn(VALID_SID, "hi-stall")
        assert excinfo.value.kind == "submission_uncertain"
        assert excinfo.value.reason == "timeout"
        blob = f"{excinfo.value!s} {excinfo.value!r}"
        assert CANARY_CONTENT not in blob
    finally:
        _stop(server)


async def test_default_transport_get_body_stall_is_protocol_timeout() -> None:
    server = _serve(_StallBodyHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=0.2)
        with pytest.raises(ThycaProtocolError) as excinfo:
            await client.get_session_snapshot(VALID_SID)
        assert excinfo.value.reason == "timeout"
    finally:
        _stop(server)


async def test_default_transport_post_reset_midbody_is_submission_uncertain() -> None:
    server = _serve(_ResetBodyHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        with pytest.raises(ThycaSubmissionUncertainError) as excinfo:
            await client.submit_turn(VALID_SID, "hi-reset")
        assert excinfo.value.kind == "submission_uncertain"
        assert excinfo.value.reason == "disconnect"
        blob = f"{excinfo.value!s} {excinfo.value!r}"
        assert CANARY_CONTENT not in blob
    finally:
        _stop(server)


async def test_default_transport_get_reset_midbody_is_protocol_disconnect() -> None:
    server = _serve(_ResetBodyHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        with pytest.raises(ThycaProtocolError) as excinfo:
            await client.get_session_snapshot(VALID_SID)
        assert excinfo.value.reason == "disconnect"
        assert CANARY_CONTENT not in str(excinfo.value)
    finally:
        _stop(server)


async def test_post_invalid_json_remains_protocol_not_uncertain() -> None:
    async def _bad(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, b"not json"

    client = ThycaClient(BASE, transport=FakeTransport(_bad))
    with pytest.raises(ThycaProtocolError) as excinfo:
        await client.submit_turn(VALID_SID, "hi")
    assert excinfo.value.reason == "invalid_json"

    async def _trunc(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, b'{"id": "2026-10-05'

    trunc_client = ThycaClient(BASE, transport=FakeTransport(_trunc))
    try:
        await trunc_client.submit_turn(VALID_SID, "hi")
    except ThycaSubmissionUncertainError:
        pytest.fail("truncated JSON must stay protocol, not uncertain")
    except ThycaProtocolError as exc:
        assert exc.reason == "invalid_json"


class _SlowCancelHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        import time

        payload = _session_detail(VALID_SID, "t", False, [])
        body = json.dumps(payload).encode()
        time.sleep(0.5)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


async def test_default_transport_cancel_propagates_cancelled() -> None:
    server = _serve(_SlowCancelHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        task = asyncio.create_task(client.get_session_snapshot(VALID_SID))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        _stop(server)


# ---- bounded read: declared-length truncation evidence (helper level) ----


class _FakeReader:
    """Synthetic response reader with optional native-style length metadata."""

    def __init__(
        self,
        data: bytes | None,
        *,
        length: object = ...,
        read_exc: BaseException | None = None,
    ) -> None:
        self._data = data
        self._read_exc = read_exc
        if length is not ...:
            self.length = length  # type: ignore[attr-defined]

    def read(self, *args: object, **kwargs: object) -> bytes | None:
        if self._read_exc is not None:
            raise self._read_exc
        return self._data


def test_read_bounded_declared_length_semantics() -> None:
    truncated = b'{"id": "partial " + "x" * 20'
    # Early EOF with declared body remaining -> transport disconnect marker.
    with pytest.raises(_NetworkMarker):
        _read_bounded(_FakeReader(truncated, length=1000))
    with pytest.raises(_NetworkMarker):
        _read_bounded(_FakeReader(b"", length=7))
    # Fulfilled declared length passes bytes through unchanged: malformed
    # JSON must stay protocol/invalid_json, never an inferred disconnect.
    assert _read_bounded(_FakeReader(truncated, length=len(truncated))) == truncated
    assert _read_bounded(_FakeReader(b"", length=0)) == b""
    # Unknown framing (native length None, missing, or non-strict-int such as
    # bool/str/mocks) is never treated as truncation evidence.
    assert _read_bounded(_FakeReader(b"short", length=None)) == b"short"
    assert _read_bounded(_FakeReader(b"short")) == b"short"
    assert _read_bounded(_FakeReader(b"short", length=True)) == b"short"
    assert _read_bounded(_FakeReader(b"short", length="1000")) == b"short"
    assert _read_bounded(_FakeReader(None)) == b""
    mock_fp = mock.MagicMock()
    mock_fp.read.return_value = b"payload"
    assert _declared_body_len(mock_fp) is None
    assert _read_bounded(mock_fp) == b"payload"
    bare_mock = mock.MagicMock()
    with pytest.raises(ThycaProtocolError) as excinfo:
        _read_bounded(bare_mock)
    assert excinfo.value.reason == "invalid_json"


def test_read_bounded_read_errors_map_to_markers() -> None:
    with pytest.raises(_TimeoutMarker):
        _read_bounded(_FakeReader(None, read_exc=TimeoutError("slow")))
    with pytest.raises(_NetworkMarker):
        _read_bounded(_FakeReader(None, read_exc=ConnectionResetError("reset")))
    with pytest.raises(_NetworkMarker):
        _read_bounded(_FakeReader(None, read_exc=http.client.IncompleteRead(b"partial", 100)))
    with pytest.raises(ThycaProtocolError) as excinfo:
        _read_bounded(_FakeReader(None, read_exc=RuntimeError("boom")))
    assert excinfo.value.reason == "transport"


def test_read_bounded_body_too_large_before_truncation() -> None:
    huge = b"x" * (MAX_RESPONSE_BYTES + 1)
    with pytest.raises(ThycaProtocolError) as excinfo:
        _read_bounded(_FakeReader(huge))
    assert excinfo.value.reason == "body_too_large"
    # A far larger declared body must not displace the size bound.
    with pytest.raises(ThycaProtocolError) as excinfo2:
        _read_bounded(_FakeReader(huge, length=2**31))
    assert excinfo2.value.reason == "body_too_large"


# ---- HTTPError branch: known status preserved, error body NEVER read ----


class _SpyErrorBody:
    """Fake HTTPError body: read must never be called; close is best-effort."""

    def __init__(self, payload: bytes = b"", *, close_exc: Exception | None = None) -> None:
        self.payload = payload
        self.read_calls = 0
        self.close_calls = 0
        self._close_exc = close_exc

    def read(self, *args: object, **kwargs: object) -> bytes:
        self.read_calls += 1
        raise AssertionError("error body must never be read")

    def close(self) -> None:
        self.close_calls += 1
        # Fail once like a real transient close error; later best-effort
        # closes (including GC cleanup of the urllib wrapper) stay quiet.
        if self._close_exc is not None and self.close_calls == 1:
            raise self._close_exc


class _SpyHTTPError(urllib.error.HTTPError):
    """HTTPError whose close is observable and optionally failing."""

    def __init__(self, *args: object, close_exc: Exception | None = None, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.close_calls = 0
        self._close_exc = close_exc

    def close(self) -> None:
        self.close_calls += 1
        if self._close_exc is not None and self.close_calls == 1:
            raise self._close_exc
        super().close()


class _RaisingOpener:
    """Fake urllib opener raising one crafted HTTPError per open()."""

    def __init__(self, exc: urllib.error.HTTPError) -> None:
        self._exc = exc
        self.opens = 0

    def open(self, request, timeout=None):  # noqa: ANN001, ANN202
        self.opens += 1
        raise self._exc


def _install_error_opener(monkeypatch, exc):  # noqa: ANN001, ANN202
    opener = _RaisingOpener(exc)
    monkeypatch.setattr(urllib.request, "build_opener", lambda *a, **k: opener)
    return opener


async def test_http_error_body_never_read_known_status_preserved(monkeypatch) -> None:
    expectations = {
        400: (ThycaPreconditionError, "http_400"),
        404: (ThycaPreconditionError, "http_404"),
        409: (ThycaBusyError, "http_409"),
        503: (ThycaProtocolError, "http_5xx"),
    }
    for code, (exc_cls, reason) in expectations.items():
        body = _SpyErrorBody(f"err {CANARY_ERROR} ".encode())
        http_error = _SpyHTTPError(f"{BASE}/api/sessions/{VALID_SID}/turn", code, "err", {}, body)
        opener = _install_error_opener(monkeypatch, http_error)
        client = ThycaClient(BASE)
        with pytest.raises(exc_cls) as excinfo:
            await client.submit_turn(VALID_SID, "hi")
        err = excinfo.value
        assert err.status_code == code
        assert err.reason == reason
        if code == 409:
            assert err.queued is True
            assert err.outcome == "queued"
        assert body.read_calls == 0
        assert body.close_calls >= 1
        assert http_error.close_calls >= 1
        assert opener.opens == 1
        blob = f"{err!s} {err!r}"
        assert CANARY_ERROR not in blob


async def test_http_error_huge_body_never_read_409_stays_busy(monkeypatch) -> None:
    huge = (f"err {CANARY_ERROR} " * (1024 * 64)).encode()  # 1024 * many
    body = _SpyErrorBody(huge)
    http_error = _SpyHTTPError(f"{BASE}/api/sessions/{VALID_SID}/turn", 409, "busy", {}, body)
    _install_error_opener(monkeypatch, http_error)
    client = ThycaClient(BASE)
    with pytest.raises(ThycaBusyError) as excinfo:
        await client.submit_turn(VALID_SID, "hi")
    # Known 409 stays busy: no body read, no body_too_large displacement.
    assert excinfo.value.status_code == 409
    assert excinfo.value.kind == "busy"
    assert body.read_calls == 0
    assert body.close_calls >= 1
    assert CANARY_ERROR not in f"{excinfo.value!s} {excinfo.value!r}"


async def test_http_error_close_failure_keeps_status_no_leak(monkeypatch) -> None:
    for code, exc_cls in ((409, ThycaBusyError), (400, ThycaPreconditionError)):
        body = _SpyErrorBody(close_exc=RuntimeError(f"fp close boom {CANARY_ERROR}"))
        http_error = _SpyHTTPError(
            f"{BASE}/api/sessions/{VALID_SID}/turn",
            code,
            "err",
            {},
            body,
            close_exc=RuntimeError(f"exc close boom {CANARY_PROFILE}"),
        )
        _install_error_opener(monkeypatch, http_error)
        client = ThycaClient(BASE)
        with pytest.raises(exc_cls) as excinfo:
            await client.submit_turn(VALID_SID, "hi")
        # Best-effort close failure must not erase the authoritative status.
        assert excinfo.value.status_code == code
        assert body.read_calls == 0
        assert body.close_calls >= 1
        assert http_error.close_calls >= 1
        blob = f"{excinfo.value!s} {excinfo.value!r}"
        assert CANARY_ERROR not in blob
        assert CANARY_PROFILE not in blob


class _ErrorStatusHandler(http.server.BaseHTTPRequestHandler):
    status_code = 500
    body = b""
    seen_headers: list[list[str]] = []

    def _respond(self) -> None:
        type(self).seen_headers.append(list(self.headers.keys()))
        self.send_response(type(self).status_code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()
        try:
            self.wfile.write(type(self).body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._respond()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                self.rfile.read(length)
            except (ConnectionResetError, BrokenPipeError):
                pass
        self._respond()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def _error_handler(status: int, body: bytes) -> type:
    return type(
        "_ConfiguredErrorStatusHandler",
        (_ErrorStatusHandler,),
        {"status_code": status, "body": body, "seen_headers": []},
    )


async def test_default_transport_native_error_statuses_real_server() -> None:
    expectations = (
        (400, ThycaPreconditionError, "http_400"),
        (404, ThycaPreconditionError, "http_404"),
        (409, ThycaBusyError, "http_409"),
        (503, ThycaProtocolError, "http_5xx"),
    )
    for code, exc_cls, reason in expectations:
        err_body = (f"native error {CANARY_ERROR} {CANARY_KEY} " * 2048).encode()
        handler_cls = _error_handler(code, err_body)
        server = _serve(handler_cls)
        try:
            port = server.server_address[1]
            client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
            with pytest.raises(exc_cls) as post_exc:
                await client.submit_turn(VALID_SID, f"probe-{code}")
            assert post_exc.value.status_code == code
            assert post_exc.value.reason == reason
            with pytest.raises(exc_cls) as get_exc:
                await client.get_session_snapshot(VALID_SID)
            assert get_exc.value.status_code == code
            for err in (post_exc.value, get_exc.value):
                blob = f"{err!s} {err!r}"
                assert CANARY_ERROR not in blob
                assert CANARY_KEY not in blob
            if code == 409:
                assert post_exc.value.queued is True
                assert post_exc.value.outcome == "queued"
            # Web/cookie auth is not available here and nothing is attached.
            assert handler_cls.seen_headers, "server saw no request"
            for header_names in handler_cls.seen_headers:
                lowered = [name.lower() for name in header_names]
                assert "authorization" not in lowered
                assert "cookie" not in lowered
                assert "proxy-authorization" not in lowered
        finally:
            _stop(server)


# ---- FIRST user input fingerprint metadata (Issue 4) ----


async def test_trace_first_user_input_hash_len_exact_unicode() -> None:
    user_text = f"grüße wörld ✓ please reply [{JOB}] now"
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB, content=CANARY_CONTENT)
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("user", user_text), _msg("assistant", None, tool_calls=[call]), result],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    digest = hashlib.sha256(user_text.encode("utf-8")).hexdigest()
    assert evidence["request_input_sha256"] == digest
    assert len(evidence["request_input_sha256"]) == 64  # valid SHA-256 hex
    # Unicode code points (chars), not UTF-8 bytes.
    assert evidence["request_input_len"] == len(user_text)
    assert evidence["request_input_len"] != len(user_text.encode("utf-8"))
    assert evidence["request_user_count"] == 1
    _assert_no_canary(evidence)


async def test_trace_quoted_job_ref_hint_not_full_proof() -> None:
    # A different message merely quoting the job ref gets the SAME hint
    # true, while its input fingerprint differs from the journaled request:
    # the hint alone must never fake the worker's full-proof check.
    quoted_text = f"an unrelated quotation that merely cites {JOB} as text"
    submitted_text = "the actual journaled request body"
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB, content="x")
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("user", quoted_text), _msg("assistant", None, tool_calls=[call]), result],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["request_job_ref_match"] is True  # bounded hint only
    submitted_sha = hashlib.sha256(submitted_text.encode("utf-8")).hexdigest()
    assert evidence["request_input_sha256"] != submitted_sha
    assert evidence["request_input_len"] != len(submitted_text)
    assert quoted_text not in json.dumps(evidence)
    # Even all_sends_confirmed cannot replace the fingerprint comparison.
    assert evidence["all_sends_confirmed"] is True


async def test_trace_zero_user_counts_hash_len_none_no_confirm() -> None:
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB, content="x")
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("system", "sys"), _msg("assistant", None, tool_calls=[call]), result],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["request_user_count"] == 0
    assert evidence["request_input_sha256"] is None
    assert evidence["request_input_len"] is None
    assert evidence["request_job_ref_match"] is False
    assert evidence["all_sends_confirmed"] is False


async def test_trace_multi_user_shape_malformed() -> None:
    # Native turn_slices yields EXACTLY one user frame per trace slice.
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [_msg("user", f"first [{JOB}]"), _msg("user", f"second [{JOB}]")],
    )
    client = await _trace_client(payload)
    with pytest.raises(ThycaProtocolError) as excinfo:
        await client.get_trace_evidence(
            VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
        )
    assert excinfo.value.reason == "malformed"


async def test_trace_extra_messages_cannot_override_first_user_input() -> None:
    user_text = f"the only user frame [{JOB}]"
    call = _send_call("call_1", CHANNEL, SOURCE_MID, JOB, content=CANARY_CONTENT)
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        None,
        None,
        [
            _msg("system", f"system text {CANARY_CONTENT} [{JOB}]"),
            _msg("user", user_text),
            _msg("assistant", f"assistant text {CANARY_CONTENT}"),
            _msg("assistant", None, tool_calls=[call]),
            result,
        ],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    assert evidence["request_user_count"] == 1
    assert evidence["request_input_sha256"] == hashlib.sha256(user_text.encode("utf-8")).hexdigest()
    assert evidence["request_input_len"] == len(user_text)
    _assert_no_canary(evidence)


async def test_trace_nested_unknown_keys_canary_never_raw() -> None:
    user_text = f"request with {CANARY_CONTENT} [{JOB}]"
    call = _send_call(
        "call_1",
        CHANNEL,
        SOURCE_MID,
        JOB,
        content=CANARY_CONTENT,
        extra_args={
            CANARY_KEY: CANARY_CONTENT,
            "profile": CANARY_PROFILE,
            "nested": {"deep": [CANARY_CONTENT, {CANARY_MODEL: CANARY_TOOL}]},
        },
    )
    result = _send_result("call_1", RESULT_MID, CHANNEL, _jump(GUILD, CHANNEL, RESULT_MID))
    result["unknown_key"] = {"deep": CANARY_CONTENT}
    user_msg = _msg("user", user_text)
    user_msg["unknown_key"] = {"deep": CANARY_TOOL}
    payload = _trace_detail(
        VALID_SID,
        2,
        "completed",
        CANARY_MODEL,
        {
            "code": CANARY_KEY,
            "message": CANARY_ERROR,
            "nested": {"deep": CANARY_CONTENT},
            "unknown": [CANARY_PROFILE],
        },
        [user_msg, _msg("assistant", None, tool_calls=[call]), result],
    )
    client = await _trace_client(payload)
    evidence = await client.get_trace_evidence(
        VALID_SID, 2, job_ref=JOB, source_message_id=SOURCE_MID, channel_id=CHANNEL
    )
    _assert_no_canary(evidence)
    # The fingerprint leaves the function; the raw input never does.
    assert evidence["request_input_sha256"] == hashlib.sha256(user_text.encode("utf-8")).hexdigest()
    assert user_text not in json.dumps(evidence)
    assert evidence["error_kind"] == "unknown"


# ---- constructor timeout validation (coarse, no value details) ----


async def test_ctor_timeout_rejects_non_finite_and_bool_coarse() -> None:
    bad_values = (
        float("inf"),
        float("-inf"),
        float("nan"),
        True,
        False,
        0,
        0.0,
        -1,
        -2.5,
        "120",
        None,
    )
    for bad in bad_values:
        with pytest.raises(ThycaPreconditionError) as excinfo:
            ThycaClient(BASE, request_timeout_seconds=bad)  # type: ignore[arg-type]
        err = excinfo.value
        assert err.field == "request_timeout_seconds"
        assert err.reason == "local_validation"
        assert err.status_code is None
        # Coarse fixed message: no per-value detail is ever exposed.
        assert (
            str(err)
            == "precondition reason=local_validation field=request_timeout_seconds status=None"
        )


async def test_ctor_timeout_accepts_positive_finite_numbers() -> None:
    assert ThycaClient(BASE).request_timeout_seconds == 120.0
    assert ThycaClient(BASE, request_timeout_seconds=900).request_timeout_seconds == 900.0
    assert ThycaClient(BASE, request_timeout_seconds=0.5).request_timeout_seconds == 0.5
    assert ThycaClient(BASE, request_timeout_seconds=1).request_timeout_seconds == 1.0


# ---- default transport: cancellation and fulfilled-length invalid JSON ----


async def test_default_transport_post_cancel_propagates_cancelled() -> None:
    server = _serve(_StallBodyHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        task = asyncio.create_task(client.submit_turn(VALID_SID, "hi-cancel"))
        await asyncio.sleep(0.05)
        task.cancel()
        # Cancelling the awaiting coroutine cannot cancel the native turn;
        # it must propagate CancelledError with no outcome claim.
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        _stop(server)


class _InvalidJsonHandler(http.server.BaseHTTPRequestHandler):
    hits: list[str] = []
    body = b'{"id": "2026-10-05'  # truncated JSON, declared length fulfilled

    def _respond(self) -> None:
        type(self).hits.append(self.command)
        body = type(self).body
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        self._respond()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            try:
                self.rfile.read(length)
            except (ConnectionResetError, BrokenPipeError):
                pass
        self._respond()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


async def test_default_transport_fulfilled_length_invalid_json_is_protocol() -> None:
    _InvalidJsonHandler.hits = []
    server = _serve(_InvalidJsonHandler)
    try:
        port = server.server_address[1]
        client = ThycaClient(f"http://127.0.0.1:{port}", request_timeout_seconds=5)
        # Declared body delivered in full but malformed: protocol/invalid_json,
        # never an inferred disconnect and never submission_uncertain.
        with pytest.raises(ThycaProtocolError) as post_exc:
            await client.submit_turn(VALID_SID, "hi-badjson")
        assert post_exc.value.reason == "invalid_json"
        with pytest.raises(ThycaProtocolError) as get_exc:
            await client.get_session_snapshot(VALID_SID)
        assert get_exc.value.reason == "invalid_json"
        assert _InvalidJsonHandler.hits == ["POST", "GET"]
    finally:
        _stop(server)


# ---- get_bot_dm (target-bot DM resolve, DM-only routing) ----


def _dm_detail(bot_id: str = "march7th", sid: str = VALID_SID) -> dict:
    return {
        "id": sid,
        "dm": bot_id,
        "bots": [bot_id],
        "running": False,
        "messages": [_msg("user", "hello")],
    }


async def test_get_bot_dm_resolves_fixed_dm_metadata_only() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        assert method == "GET"
        assert url == f"{BASE}/api/bots/march7th/dm"
        assert body is None
        return 200, _json_bytes(_dm_detail())

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    dm = await client.get_bot_dm("march7th")
    assert dm == {
        "native_sid": VALID_SID,
        "running": False,
        "message_count": 1,
        "proof": "bot_dm",
    }
    _assert_no_canary(dm)


async def test_get_bot_dm_unknown_bot_404_is_precondition() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 404, b""

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaPreconditionError) as exc_info:
        await client.get_bot_dm("nope")
    assert exc_info.value.status_code == 404


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update({"id": "not-a-sid"}),
        lambda p: p.update({"dm": "other-bot"}),
        lambda p: p.update({"bots": ["march7th", "other-bot"]}),
        lambda p: p.update({"bots": []}),
        lambda p: p.update({"running": "yes"}),
        lambda p: p.update({"messages": [{"role": "owner"}]}),
        lambda p: p.update({"messages": "nope"}),
    ],
)
async def test_get_bot_dm_malformed_fails_closed(mutate) -> None:  # noqa: ANN001
    payload = _dm_detail()
    mutate(payload)

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        return 200, _json_bytes(payload)

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    with pytest.raises(ThycaProtocolError):
        await client.get_bot_dm("march7th")


async def test_get_bot_dm_bad_shape_fails_before_http() -> None:
    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        raise AssertionError("must not call HTTP")

    transport = FakeTransport(_handler)
    client = ThycaClient(BASE, transport=transport)
    for bad in ["", "  ", "March7th", "a_b", "-x", "x-", "x y", 5, None]:
        with pytest.raises(ThycaPreconditionError) as exc_info:
            await client.get_bot_dm(bad)  # type: ignore[arg-type]
        assert exc_info.value.field == "bot"
    assert transport.calls == []


async def test_submit_turn_body_has_no_bot_field() -> None:
    """Turns post into the DM session; no routing field is sent."""

    async def _handler(method: str, url: str, body: bytes | None):  # noqa: ANN001, ANN202
        assert method == "POST"
        assert url == f"{BASE}/api/sessions/{VALID_SID}/turn"
        assert body is not None
        assert json.loads(body.decode("utf-8")) == {"text": "hello"}
        return 200, _json_bytes(_session_detail(VALID_SID, "t", False, [_msg("user", "hello")]))

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    result = await client.submit_turn(VALID_SID, "  hello  ")
    assert result["completed"] is True
