"""Thin native Thyca HTTP client (metadata only, loopback only).

OPTION2 worker owns Discord ``send`` via its own MCP tool. This client never
sends Discord messages, never runs an LLM, and never returns reply text, raw
transcripts, tool arguments, or provider errors. It only proves, from the
authoritative native endpoints, what a future Responder/worker needs:

- :meth:`ThycaClient.get_session_snapshot`: pre-turn index + running flag
  from the *target* session GET (never the newest-200 global trace window).
- :meth:`ThycaClient.submit_turn`: synchronous native turn completion status
  metadata only (no reply body trusted).
- :meth:`ThycaClient.get_trace_evidence`: tool-proof correlation for
  ``discord__send_message`` calls in one native turn, via exact
  ``session_id`` + ``turn_index`` + ``job_ref`` + Discord IDs.

Correlation is not authorization: ``job_ref`` / ``source_message_id`` /
``channel_id`` are explicit caller arguments matched against the recorded
trace. They do not authenticate the sender. There is no native idempotency
key, run key, or per-turn tool allowlist, so there is no exactly-once
guarantee.

Network rules: ``http://`` loopback only (127.0.0.1, localhost, ::1, or any
loopback IP), no credentials, query, fragment, or non-root base path. No
redirects are followed. Environment proxies are explicitly disabled so
loopback requests can never be sent to a configured proxy. Non-200 error
bodies are never read (status only). No secrets or custom auth headers. All
HTTP runs in :func:`asyncio.to_thread` so the Discord event loop never blocks.

Error model is coarse metadata only (never ``str(exc)``, tracebacks, or raw
bodies, which may contain credentials):

- :class:`ThycaPreconditionError`: local validation or native 400/404.
- :class:`ThycaBusyError`: native 409 (safe QUEUED; poll snapshot, do not
  submit a new job until idle; this client has no scheduler).
- :class:`ThycaSubmissionUncertainError`: POST timeout/disconnect. Native
  execution continues server-side; do not blind-retry, do not assume failure.
  ``GET running == idle`` alone is not proof of non-acceptance.
- :class:`ThycaProtocolError`: 5xx, redirect, invalid JSON, malformed shape,
  or GET transport failure.

``asyncio.CancelledError`` from the caller propagates unchanged. Cancelling
the awaiting coroutine cannot stop the already-sent native turn.
"""

from __future__ import annotations

import asyncio
import json
import math
import urllib.parse

from .thyca_errors import (
    ThycaBusyError as ThycaBusyError,
)
from .thyca_errors import (
    ThycaClientError,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
    _NetworkMarker,
    _TimeoutMarker,
)
from .thyca_trace import build_trace_evidence
from .thyca_transport import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    MAX_RESPONSE_BYTES,
    HttpTransport,
    sync_http_request,
)
from .thyca_transport import (
    _declared_body_len as _declared_body_len,
)
from .thyca_transport import (
    _read_bounded as _read_bounded,
)
from .thyca_validate import (
    _map_http_status,
    _normalize_base_url,
    _parse_json_dict,
    _sha256_hex,
    _turn_slice_counts,
    _validate_discord_id,
    _validate_expected_title,
    _validate_job_ref,
    _validate_message_roles,
    _validate_model_effort,
    _validate_sid,
    _validate_text,
    _validate_turn_index,
)


class ThycaClient:
    """Thin loopback client for native Thyca session/turn/trace endpoints."""

    def __init__(
        self,
        base_url: str,
        *,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        transport: HttpTransport | None = None,
    ) -> None:
        self._base_url = _normalize_base_url(base_url)
        if (
            not isinstance(request_timeout_seconds, (int, float))
            or isinstance(request_timeout_seconds, bool)
            or not math.isfinite(request_timeout_seconds)
            or not request_timeout_seconds > 0
        ):
            raise ThycaPreconditionError(field="request_timeout_seconds", reason="local_validation")
        self._timeout = float(request_timeout_seconds)
        self._transport = transport

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def request_timeout_seconds(self) -> float:
        return self._timeout

    def _session_url(self, native_sid: str) -> str:
        quoted = urllib.parse.quote(native_sid, safe="")
        return f"{self._base_url}/api/sessions/{quoted}"

    def _turn_url(self, native_sid: str) -> str:
        quoted = urllib.parse.quote(native_sid, safe="")
        return f"{self._base_url}/api/sessions/{quoted}/turn"

    def _trace_url(self, native_sid: str, turn_index: int) -> str:
        quoted = urllib.parse.quote(native_sid, safe="")
        return f"{self._base_url}/api/traces/{quoted}/{turn_index}"

    def _sync_http(self, method: str, url: str, body: bytes | None) -> tuple[int, bytes]:
        return sync_http_request(method, url, body, request_timeout_seconds=self._timeout)

    async def _request(self, method: str, url: str, body: bytes | None) -> tuple[int, bytes]:
        if self._transport is not None:
            try:
                result = await self._transport(method, url, body)
            except (ThycaClientError, asyncio.CancelledError):
                raise
            except (TimeoutError, asyncio.TimeoutError):
                if method == "POST":
                    raise ThycaSubmissionUncertainError(reason="timeout") from None
                raise ThycaProtocolError(reason="timeout") from None
            except Exception:
                if method == "POST":
                    raise ThycaSubmissionUncertainError(reason="disconnect") from None
                raise ThycaProtocolError(reason="disconnect") from None
            if (
                not isinstance(result, tuple)
                or len(result) != 2
                or type(result[0]) is not int
                or not isinstance(result[1], bytes)
            ):
                raise ThycaProtocolError(reason="transport")
            status, payload = result
            if len(payload) > MAX_RESPONSE_BYTES:
                raise ThycaProtocolError(reason="body_too_large")
            return status, payload
        try:
            # to_thread keeps the Discord event loop responsive while the
            # synchronous native turn runs. Cancellation here cannot stop
            # the network thread or the native turn; it only stops waiting.
            return await asyncio.to_thread(self._sync_http, method, url, body)
        except (_TimeoutMarker, TimeoutError, asyncio.TimeoutError):
            if method == "POST":
                raise ThycaSubmissionUncertainError(reason="timeout") from None
            raise ThycaProtocolError(reason="timeout") from None
        except _NetworkMarker:
            if method == "POST":
                raise ThycaSubmissionUncertainError(reason="disconnect") from None
            raise ThycaProtocolError(reason="disconnect") from None

    async def get_session_snapshot(
        self, native_sid: str, *, expected_title: str | None = None
    ) -> dict:
        """Prove pre-turn index and running flag from the target session.

        Uses ``GET /api/sessions/<sid>`` only. The global
        ``/api/traces?limit=200`` window can miss an old SID when other
        sessions are active, so it is never used here.
        """
        _validate_sid(native_sid)
        _validate_expected_title(expected_title)
        status, body = await self._request("GET", self._session_url(native_sid), None)
        _map_http_status(status)
        payload = _parse_json_dict(body)
        sid_ret = payload.get("id")
        title_ret = payload.get("title")
        running_ret = payload.get("running")
        messages_ret = payload.get("messages")
        if not isinstance(sid_ret, str) or not sid_ret:
            raise ThycaProtocolError(reason="malformed")
        if not isinstance(title_ret, str):
            raise ThycaProtocolError(reason="malformed")
        if running_ret is not True and running_ret is not False:
            raise ThycaProtocolError(reason="malformed")
        messages = _validate_message_roles(messages_ret)
        turn_count, turn_indices, pre_max = _turn_slice_counts(messages)
        if expected_title is None:
            title_match: bool | None = None
        else:
            title_match = title_ret == expected_title
        return {
            "native_sid": native_sid,
            "native_id_match": sid_ret == native_sid,
            "title_match": title_match,
            "title_len": len(title_ret),
            "title_sha256": _sha256_hex(title_ret),
            "running": running_ret,
            "turn_count": turn_count,
            "turn_indices": turn_indices,
            "pre_max_turn_index": pre_max,
            "message_count": len(messages),
            "proof": "session_detail_turn_slices",
        }

    async def submit_turn(
        self,
        native_sid: str,
        text: str,
        *,
        model: str | None = None,
        effort: str | None = None,
    ) -> dict:
        """Run one synchronous native turn; return completion metadata only.

        The POST body is exactly ``{"text", "model"?, "effort"?}`` with
        ``None`` values omitted to preserve owner defaults. Reply text, raw
        messages, tool arguments, and provider errors are dropped locally.
        """
        _validate_sid(native_sid)
        cleaned = _validate_text(text)
        _validate_model_effort(model, effort)
        body_dict: dict[str, str] = {"text": cleaned}
        if model is not None:
            body_dict["model"] = model
        if effort is not None:
            body_dict["effort"] = effort
        body_bytes = json.dumps(body_dict, ensure_ascii=False).encode("utf-8")
        status, raw = await self._request("POST", self._turn_url(native_sid), body_bytes)
        _map_http_status(status)
        payload = _parse_json_dict(raw)
        if payload.get("cancelled") is True:
            if "id" in payload:
                raise ThycaProtocolError(reason="malformed")
            return {
                "completed": False,
                "cancelled": True,
                "native_id_match": None,
                "turn_index": None,
                "turn_count": None,
                "running": None,
                "submitted_text_len": len(cleaned),
                "submitted_text_sha256": _sha256_hex(cleaned),
                "stored_text_match": None,
                "stored_text_len": None,
                "proof": "cancelled",
            }
        sid_ret = payload.get("id")
        running_ret = payload.get("running")
        messages_ret = payload.get("messages")
        # "reply" is opaque native output: never trusted, never returned.
        if not isinstance(sid_ret, str) or not sid_ret:
            raise ThycaProtocolError(reason="malformed")
        if running_ret is not True and running_ret is not False:
            raise ThycaProtocolError(reason="malformed")
        messages = _validate_message_roles(messages_ret)
        turn_count, _indices, post_max = _turn_slice_counts(messages)
        if turn_count == 0 or post_max is None:
            raise ThycaProtocolError(reason="malformed")
        stored_content: str | None = None
        for item in reversed(messages):
            if item.get("role") == "user":
                content = item.get("content")
                if isinstance(content, str):
                    stored_content = content
                break
        if stored_content is None:
            stored_match = False
            stored_len: int | None = None
        else:
            stored_match = stored_content == cleaned
            stored_len = len(stored_content)
        # Drop stored_content here; only the match bool/len leave this scope.
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": sid_ret == native_sid,
            "turn_index": post_max,
            "turn_count": turn_count,
            "running": running_ret,
            "submitted_text_len": len(cleaned),
            "submitted_text_sha256": _sha256_hex(cleaned),
            "stored_text_match": stored_match,
            "stored_text_len": stored_len,
            "proof": "post_detail_turn_slices",
        }

    async def get_trace_evidence(
        self,
        native_sid: str,
        turn_index: int,
        *,
        job_ref: str,
        source_message_id: int,
        channel_id: int,
        guild_id: int | None = None,
    ) -> dict:
        """Correlate recorded send and no-reply calls for one turn.

        Uses ``GET /api/traces/<sid>/<index>`` only. Tool arguments and user
        text stay local; only match booleans, counts, lengths, and hashes are
        returned. Send proof (``discord__send_message``) is bit-for-bit
        preserved; ``discord__record_no_reply`` calls are additionally
        extracted as ``observation_calls`` while still counting as other
        tools for compatibility.

        ``request_job_ref_match`` is a bounded supporting hint (substring in
        user text), not exact submitted proof and not native acceptance proof.
        ``request_input_sha256`` / ``request_input_len`` fingerprint the
        authoritative FIRST user message of this turn slice (unicode code
        points for length, SHA-256 hex for content); the future worker MUST
        compare them against the persisted submitted-text fingerprint, require
        ``request_user_count == 1``, ``status == "completed"``,
        ``error_present is False``, and match every journaled send intent and
        SDK receipt MID. This fingerprint does not authenticate the MCP caller.

        ``all_sends_confirmed`` means ONLY the correlated tool-proof subset
        (trace SID/index match, single user frame, job_ref hint present, at
        least one correlated send, every correlated send verified). It is not
        whole-turn completion and MUST NOT alone mark COMPLETED/advance ACK.
        Callers MUST also check ``legacy_uncorrelated_count`` and journaled
        counts.
        """
        _validate_sid(native_sid)
        _validate_turn_index(turn_index)
        _validate_job_ref(job_ref)
        _validate_discord_id(source_message_id, "source_message_id")
        _validate_discord_id(channel_id, "channel_id")
        if guild_id is not None:
            _validate_discord_id(guild_id, "guild_id")
        status, body = await self._request("GET", self._trace_url(native_sid, turn_index), None)
        _map_http_status(status)
        payload = _parse_json_dict(body)
        return build_trace_evidence(
            payload,
            native_sid=native_sid,
            turn_index=turn_index,
            job_ref=job_ref,
            source_message_id=source_message_id,
            channel_id=channel_id,
            guild_id=guild_id,
        )
