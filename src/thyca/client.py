"""Thyca loopback client: session snapshots, DM resolve, turn submit."""

from __future__ import annotations

import asyncio
import json
import math
import urllib.parse

from validate import sha256_hex as _sha256_hex

from .errors import (
    ThycaClientError,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
    _NetworkMarker,
    _TimeoutMarker,
)
from .transport import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    MAX_RESPONSE_BYTES,
    HttpTransport,
    sync_http_request,
)
from .validation import (
    _SID_RE,
    _map_http_status,
    _normalize_base_url,
    _parse_json_dict,
    _turn_slice_counts,
    _validate_bot,
    _validate_expected_title,
    _validate_message_roles,
    _validate_model_effort,
    _validate_sid,
    _validate_text,
)


class ThycaClient:
    """Thin loopback client for native Thyca session/turn endpoints."""

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

    def _bot_dm_url(self, bot_id: str) -> str:
        quoted = urllib.parse.quote(bot_id, safe="")
        return f"{self._base_url}/api/bots/{quoted}/dm"

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

    async def get_bot_dm(self, bot_id: str) -> dict:
        """Resolve the bot's fixed DM session; return adoption metadata only.

        Uses ``GET /api/bots/<bot_id>/dm`` (deterministic find-or-create
        on the native side). The payload is validated strictly: ``id`` a
        strict native SID, ``dm`` exactly ``bot_id``, ``bots`` exactly
        ``[bot_id]``, ``running`` a bool, and ``messages`` a valid role
        list. Returns ``native_sid``, ``running``, ``message_count``,
        and a fixed ``proof`` marker. Titles, raw messages, and bodies
        are dropped locally. HTTP statuses map exactly like the turn POST
        (404 for an unknown/paused bot, never a raw body).
        """
        _validate_bot(bot_id)
        assert isinstance(bot_id, str)
        status, body = await self._request("GET", self._bot_dm_url(bot_id), None)
        _map_http_status(status)
        payload = _parse_json_dict(body)
        sid_ret = payload.get("id")
        dm_ret = payload.get("dm")
        bots_ret = payload.get("bots")
        running_ret = payload.get("running")
        messages_ret = payload.get("messages")
        if not isinstance(sid_ret, str) or _SID_RE.fullmatch(sid_ret) is None:
            raise ThycaProtocolError(reason="malformed")
        if dm_ret != bot_id:
            raise ThycaProtocolError(reason="malformed")
        if bots_ret != [bot_id]:
            raise ThycaProtocolError(reason="malformed")
        if running_ret is not True and running_ret is not False:
            raise ThycaProtocolError(reason="malformed")
        messages = _validate_message_roles(messages_ret)
        return {
            "native_sid": sid_ret,
            "running": running_ret,
            "message_count": len(messages),
            "proof": "bot_dm",
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
        ``None`` values omitted to preserve owner defaults. Turns are posted
        directly into the target bot's DM session, so no routing field is
        needed. Reply text, raw messages, tool arguments, and provider
        errors are dropped locally.
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
