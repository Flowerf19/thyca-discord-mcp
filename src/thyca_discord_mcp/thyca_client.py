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
import hashlib
import http.client
import ipaddress
import json
import math
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable
from typing import Any

# Authoritative SID grammar, composed from the installed native store
# (thyca.sessions.store.SESSION_ID_PATTERN): date + 4 lowercase hex.
SESSION_ID_PATTERN = r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_[0-9a-f]{4}"
_SID_RE = re.compile(rf"^{SESSION_ID_PATTERN}$")

# Native turn text bound (thyca.app.turn_options.TEXT_MAX). Enforced locally
# before any HTTP; never silently truncated.
TEXT_MAX = 4000
MODEL_MAX = 200
EFFORT_MAX = 200
JOB_REF_MAX = 200
TITLE_EXPECTED_MAX = 1000

# Exact native MCP tool name. Prefix/substring matches are rejected.
SEND_TOOL_NAME = "discord__send_message"
ARG_CHANNEL = "channel_id"
ARG_SOURCE_MID = "source_message_id"
ARG_JOB_REF = "job_ref"
ARG_CONTENT = "content"

# Exact Discord jump URL, not a substring search.
_JUMP_RE = re.compile(r"^https://discord\.com/channels/(\d+)/(\d+)/(\d+)$")

_TRACE_STATUSES = frozenset({"completed", "failed", "loop_limit", "context_limit"})
_ERROR_KINDS = frozenset({"llm_error", "chat_unavailable"})
_ROLES = frozenset({"user", "assistant", "tool", "system"})

# Bounded response read so a corrupt/huge body cannot exhaust memory.
MAX_RESPONSE_BYTES = 10 * 1024 * 1024

# Technical per-request socket bound. Not a business budget; the worker owns
# queueing and retry policy (and must not blind-retry an uncertain POST).
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120.0

# Transport injection for offline fixtures:
# async (method, url, body_bytes_or_None) -> (status_code, body_bytes).
HttpTransport = Callable[[str, str, bytes | None], Awaitable[tuple[int, bytes]]]


class ThycaClientError(Exception):
    """Base for coarse, metadata-only client failures (never raw bodies)."""

    def __init__(
        self,
        kind: str,
        *,
        status_code: int | None = None,
        reason: str = "",
        field: str = "",
    ) -> None:
        self.kind = kind
        self.status_code = status_code
        self.reason = reason
        self.field = field
        super().__init__(self._public_message())

    def _public_message(self) -> str:
        # Safe enums/ints only. Never include native bodies or str(exc).
        parts = [self.kind]
        if self.reason:
            parts.append(f"reason={self.reason}")
        if self.field:
            parts.append(f"field={self.field}")
        parts.append(f"status={self.status_code}")
        return " ".join(parts)


class ThycaPreconditionError(ThycaClientError):
    """Local validation failed or native returned 400/404 (no raw body)."""

    def __init__(
        self,
        *,
        status_code: int | None = None,
        reason: str = "local_validation",
        field: str = "",
    ) -> None:
        super().__init__("precondition", status_code=status_code, reason=reason, field=field)


class ThycaBusyError(ThycaClientError):
    """Native 409: session busy. Safe QUEUED; wait for idle, no new submit."""

    def __init__(self, *, status_code: int | None = 409) -> None:
        self.queued = True
        self.outcome = "queued"
        super().__init__("busy", status_code=status_code, reason="http_409")


class ThycaSubmissionUncertainError(ThycaClientError):
    """POST timeout/disconnect: native may still be running. Do not retry."""

    def __init__(self, *, reason: str = "timeout") -> None:
        self.uncertain = True
        self.outcome = "unknown"
        super().__init__("submission_uncertain", status_code=None, reason=reason)


class ThycaProtocolError(ThycaClientError):
    """5xx, redirect, invalid JSON, malformed shape, or GET transport issue."""

    def __init__(self, *, status_code: int | None = None, reason: str = "protocol") -> None:
        super().__init__("protocol", status_code=status_code, reason=reason)


class _TimeoutMarker(Exception):
    pass


class _NetworkMarker(Exception):
    pass


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Block every redirect so urllib can never follow off-loopback."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN202
        raise urllib.error.HTTPError(req.get_full_url(), 310, "redirect blocked", {}, None)


def _is_loopback_host(hostname: str) -> bool:
    if hostname in ("127.0.0.1", "localhost"):
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    try:
        parsed = urllib.parse.urlparse(base_url.strip())
    except ValueError:
        raise ThycaPreconditionError(field="base_url", reason="local_validation") from None
    if parsed.scheme != "http":
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    if parsed.username or parsed.password:
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    if parsed.query or parsed.fragment or parsed.params:
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    if parsed.path not in ("", "/"):
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    hostname = (parsed.hostname or "").lower()
    if not hostname or not _is_loopback_host(hostname):
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    try:
        port = parsed.port
    except ValueError:
        raise ThycaPreconditionError(field="base_url", reason="local_validation") from None
    if port is not None and not 1 <= port <= 65535:
        raise ThycaPreconditionError(field="base_url", reason="local_validation")
    if ":" in hostname and not hostname.startswith("["):
        host_part = f"[{hostname}]"
    else:
        host_part = hostname
    if port is None:
        return f"http://{host_part}"
    return f"http://{host_part}:{port}"


def _validate_sid(native_sid: str) -> str:
    if not isinstance(native_sid, str) or not _SID_RE.fullmatch(native_sid):
        raise ThycaPreconditionError(field="native_sid", reason="local_validation")
    return native_sid


def _validate_text(text: str) -> str:
    if not isinstance(text, str):
        raise ThycaPreconditionError(field="text", reason="local_validation")
    cleaned = text.strip()
    if not cleaned or len(cleaned) > TEXT_MAX:
        raise ThycaPreconditionError(field="text", reason="local_validation")
    return cleaned


def _validate_model_effort(model: str | None, effort: str | None) -> tuple[str | None, str | None]:
    for value, field, bound in (
        (model, "model", MODEL_MAX),
        (effort, "effort", EFFORT_MAX),
    ):
        if value is None:
            continue
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > bound
            or "\n" in value
            or "\r" in value
        ):
            raise ThycaPreconditionError(field=field, reason="local_validation")
    return model, effort


def _validate_turn_index(turn_index: int) -> int:
    if type(turn_index) is not int or turn_index < 0:
        raise ThycaPreconditionError(field="turn_index", reason="local_validation")
    return turn_index


def _validate_job_ref(job_ref: str) -> str:
    if (
        not isinstance(job_ref, str)
        or not job_ref.strip()
        or len(job_ref) > JOB_REF_MAX
        or "\n" in job_ref
        or "\r" in job_ref
    ):
        raise ThycaPreconditionError(field="job_ref", reason="local_validation")
    return job_ref


def _validate_expected_title(expected_title: str | None) -> str | None:
    if expected_title is None:
        return None
    if not isinstance(expected_title, str) or len(expected_title) > TITLE_EXPECTED_MAX:
        raise ThycaPreconditionError(field="expected_title", reason="local_validation")
    return expected_title


def _validate_discord_id(value: int, field: str) -> int:
    # type() is exact: bool is a subclass of int and must be rejected.
    if type(value) is not int or value <= 0:
        raise ThycaPreconditionError(field=field, reason="local_validation")
    return value


def _as_discord_id(value: Any) -> int | None:
    """Coerce native JSON IDs (int or digit-string) to int, else None.

    Used only for data already returned by native; caller-provided IDs use
    the stricter _validate_discord_id. Never raises, never returns raw text.
    """
    if type(value) is int and value > 0:
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            try:
                parsed = int(text)
            except ValueError:
                return None
            if parsed > 0:
                return parsed
    return None


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _turn_slice_counts(messages: list[dict]) -> tuple[int, list[int], int | None]:
    """Authoritative user-turn grouping (mirrors native turn_slices).

    System rows are excluded, rows before the first user are dropped, and
    every user message opens exactly one slice. Roles are already validated.
    """
    slices = 0
    open_slice = False
    for item in messages:
        role = item.get("role")
        if role == "system":
            continue
        if role == "user":
            if open_slice:
                slices += 1
            open_slice = True
        elif open_slice:
            continue
    if open_slice:
        slices += 1
    indices = list(range(slices))
    pre_max = slices - 1 if slices else None
    return slices, indices, pre_max


def _validate_message_roles(messages: Any) -> list[dict]:
    if not isinstance(messages, list):
        raise ThycaProtocolError(reason="malformed")
    for item in messages:
        if not isinstance(item, dict):
            raise ThycaProtocolError(reason="malformed")
        if item.get("role") not in _ROLES:
            raise ThycaProtocolError(reason="malformed")
    return messages


def _parse_json_dict(body: bytes) -> dict:
    if not body:
        raise ThycaProtocolError(reason="invalid_json")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        raise ThycaProtocolError(reason="invalid_json") from None
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        raise ThycaProtocolError(reason="invalid_json") from None
    if not isinstance(payload, dict):
        raise ThycaProtocolError(reason="invalid_json")
    return payload


def _map_http_status(status: int) -> None:
    """Raise the coarse error for a non-200 status (never raw bodies)."""
    if status == 200:
        return
    if status == 409:
        raise ThycaBusyError(status_code=status)
    if status in (400, 404):
        raise ThycaPreconditionError(status_code=status, reason=f"http_{status}")
    if status == 310 or 300 <= status < 400:
        raise ThycaProtocolError(status_code=status, reason="redirect")
    if 500 <= status < 600:
        raise ThycaProtocolError(status_code=status, reason="http_5xx")
    raise ThycaProtocolError(status_code=status, reason="http_status")


def _declared_body_len(fp) -> int | None:  # noqa: ANN001
    """Declared remaining body bytes from the native reader, or None.

    ``http.client.HTTPResponse.length`` is the remaining body count; the
    native layer leaves it ``None`` for chunked, close-delimited, negative,
    or malformed ``Content-Length`` framing. Only a real non-negative
    ``int`` counts as evidence (``bool`` is not evidence); unknown readers
    and mocks yield ``None``, so a truncated body is never inferred without
    transport evidence.
    """
    value = getattr(fp, "length", None)
    if type(value) is int and value >= 0:
        return value
    return None


def _read_bounded(fp) -> bytes:  # noqa: ANN001
    # Captured before reading: HTTPResponse.length counts DOWN as bytes
    # arrive, and native read(amt) on an early EOF returns the short partial
    # body WITHOUT raising (no IncompleteRead for bounded reads).
    declared = _declared_body_len(fp)
    try:
        data = fp.read(MAX_RESPONSE_BYTES + 1)
    except (TimeoutError, asyncio.TimeoutError):
        # Includes socket.timeout (alias of TimeoutError). Never str(exc).
        raise _TimeoutMarker from None
    except (
        OSError,
        http.client.HTTPException,
        urllib.error.URLError,
        ValueError,
        EOFError,
    ):
        # ConnectionResetError, BrokenPipeError, IncompleteRead,
        # RemoteDisconnected, etc. Never propagate str(exc) (may leak creds).
        raise _NetworkMarker from None
    except Exception:
        raise ThycaProtocolError(reason="transport") from None
    if data is None:
        return b""
    if not isinstance(data, bytes):
        raise ThycaProtocolError(reason="invalid_json")
    if len(data) > MAX_RESPONSE_BYTES:
        # Size bound fires before any truncation inference.
        raise ThycaProtocolError(reason="body_too_large")
    if declared is not None and len(data) < declared:
        # The peer closed before delivering the declared body: a transport
        # disconnect, never parseable JSON evidence. Declared lengths that
        # are fulfilled fall through unchanged (malformed JSON stays
        # protocol/invalid_json); unknown framing (declared None) keeps its
        # existing read-error mapping only.
        raise _NetworkMarker from None
    return data


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
        if method == "POST":
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            data: bytes | None = body if body is not None else b"{}"
        else:
            headers = {"Accept": "application/json"}
            data = None
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        # ProxyHandler({}) explicitly disables environment proxies so
        # loopback POST text can never be sent to a configured proxy.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler)
        try:
            with opener.open(request, timeout=self._timeout) as response:
                status = int(response.status)
                return status, _read_bounded(response)
        except urllib.error.HTTPError as exc:
            # Non-200 bodies are never used; preserve only the authoritative
            # status. Never call exc.read() (unbounded). Close best-effort;
            # close failures must not erase the known status.
            try:
                status_code = int(exc.code)
            except (TypeError, ValueError):
                raise ThycaProtocolError(reason="http_status") from None
            fp = getattr(exc, "fp", None)
            if fp is not None:
                try:
                    fp.close()
                except Exception:
                    pass
            try:
                exc.close()
            except Exception:
                pass
            return status_code, b""
        except (_TimeoutMarker, _NetworkMarker):
            raise
        except (TimeoutError, asyncio.TimeoutError):
            raise _TimeoutMarker from None
        except ThycaProtocolError:
            raise
        except (
            OSError,
            urllib.error.URLError,
            ValueError,
            http.client.HTTPException,
            EOFError,
        ):
            # URLError wraps socket timeouts/disconnects; ValueError covers
            # malformed responses; HTTPException covers IncompleteRead,
            # RemoteDisconnected, etc. Never propagate str(exc) (may leak creds).
            raise _NetworkMarker from None

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
        """Correlate recorded ``discord__send_message`` calls for one turn.

        Uses ``GET /api/traces/<sid>/<index>`` only. Tool arguments and user
        text stay local; only match booleans, counts, lengths, and hashes are
        returned.

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
        session_ret = payload.get("session_id")
        index_ret = payload.get("turn_index")
        status_ret = payload.get("status")
        model_ret = payload.get("model")
        error_ret = payload.get("error")
        messages_ret = payload.get("messages")
        if not isinstance(session_ret, str) or not session_ret:
            raise ThycaProtocolError(reason="malformed")
        if type(index_ret) is not int or index_ret < 0:
            raise ThycaProtocolError(reason="malformed")
        if not isinstance(status_ret, str) or not status_ret:
            raise ThycaProtocolError(reason="malformed")
        if model_ret is not None and not isinstance(model_ret, str):
            raise ThycaProtocolError(reason="malformed")
        if error_ret is not None and not isinstance(error_ret, dict):
            raise ThycaProtocolError(reason="malformed")
        messages = _validate_message_roles(messages_ret)

        trace_session_match = session_ret == native_sid
        turn_index_match = index_ret == turn_index
        if status_ret in _TRACE_STATUSES:
            status_out = status_ret
        else:
            status_out = "unknown"
        if model_ret is None:
            model_present = False
            model_len: int | None = None
            model_hash: str | None = None
        else:
            model_present = True
            model_len = len(model_ret)
            model_hash = _sha256_hex(model_ret)
        if error_ret is None:
            error_present = False
            error_kind = "none"
            error_code_len: int | None = None
            error_message_len: int | None = None
            error_keys_len: int | None = None
        else:
            error_present = True
            code_raw = error_ret.get("code")
            msg_raw = error_ret.get("message")
            if isinstance(code_raw, str) and code_raw in _ERROR_KINDS:
                error_kind = code_raw
            else:
                error_kind = "unknown"
            error_code_len = len(code_raw) if isinstance(code_raw, str) else None
            error_message_len = len(msg_raw) if isinstance(msg_raw, str) else None
            error_keys_len = len(error_ret)

        request_job_ref_match = False
        request_user_count = 0
        first_user_content: str | None = None
        first_user_seen = False
        for item in messages:
            if item.get("role") != "user":
                continue
            request_user_count += 1
            content = item.get("content")
            if not first_user_seen:
                first_user_seen = True
                if isinstance(content, str):
                    first_user_content = content
            if isinstance(content, str) and job_ref in content:
                request_job_ref_match = True
        # Native per-turn trace holds exactly one slice, hence at most one
        # user frame when present. More than one user frame is malformed and
        # must never yield tool proof.
        if request_user_count > 1:
            raise ThycaProtocolError(reason="malformed")
        if first_user_content is not None:
            request_input_sha256: str | None = _sha256_hex(first_user_content)
            request_input_len: int | None = len(first_user_content)
        else:
            request_input_sha256 = None
            request_input_len = None
        # first_user_content intentionally goes out of scope here; only the
        # hash/len leave this function.

        tool_results: dict[str, dict] = {}
        for item in messages:
            if item.get("role") != "tool":
                continue
            call_id = item.get("tool_call_id")
            if isinstance(call_id, str) and call_id and call_id not in tool_results:
                tool_results[call_id] = item

        sends: list[dict] = []
        other_tool_count = 0
        other_name_lens: list[int] = []
        for item in messages:
            if item.get("role") != "assistant":
                continue
            calls = item.get("tool_calls")
            if calls is None:
                continue
            if not isinstance(calls, list):
                raise ThycaProtocolError(reason="malformed")
            for call in calls:
                if not isinstance(call, dict):
                    raise ThycaProtocolError(reason="malformed")
                call_id = call.get("id")
                call_name = call.get("name")
                if not isinstance(call_id, str) or not call_id:
                    raise ThycaProtocolError(reason="malformed")
                if not isinstance(call_name, str) or not call_name:
                    raise ThycaProtocolError(reason="malformed")
                if call_name != SEND_TOOL_NAME:
                    other_tool_count += 1
                    other_name_lens.append(len(call_name))
                    continue
                args = call.get("arguments")
                if args is None:
                    args = {}
                if not isinstance(args, dict):
                    raise ThycaProtocolError(reason="malformed")
                channel_match = _as_discord_id(args.get(ARG_CHANNEL)) == channel_id
                source_match = _as_discord_id(args.get(ARG_SOURCE_MID)) == source_message_id
                job_arg = args.get(ARG_JOB_REF)
                job_match = isinstance(job_arg, str) and job_arg == job_ref
                content_arg = args.get(ARG_CONTENT)
                if isinstance(content_arg, str):
                    content_len: int | None = len(content_arg)
                    content_hash: str | None = _sha256_hex(content_arg)
                else:
                    content_len = None
                    content_hash = None
                # args/content_arg intentionally go out of scope here; only
                # match booleans, len, and hash are kept.
                result_item = tool_results.get(call_id)
                if result_item is None:
                    sends.append(
                        {
                            "tool_call_id": call_id,
                            "channel_match": channel_match,
                            "source_mid_match": source_match,
                            "job_ref_match": job_match,
                            "correlated": bool(channel_match and source_match and job_match),
                            "result_present": False,
                            "result_message_id": None,
                            "result_channel_match": False,
                            "jump_present": False,
                            "jump_ids_match": False,
                            "result_ok": False,
                            "content_len": content_len,
                            "content_sha256": content_hash,
                        }
                    )
                    continue
                meta = result_item.get("meta")
                is_error = isinstance(meta, dict) and meta.get("is_error") is True
                result_content = result_item.get("content")
                parsed: dict | None = None
                if isinstance(result_content, str) and result_content.strip():
                    try:
                        decoded = json.loads(result_content)
                    except (json.JSONDecodeError, ValueError):
                        decoded = None
                    if isinstance(decoded, dict):
                        parsed = decoded
                has_error_key = parsed is not None and "error" in parsed
                if parsed is None or has_error_key or is_error:
                    sends.append(
                        {
                            "tool_call_id": call_id,
                            "channel_match": channel_match,
                            "source_mid_match": source_match,
                            "job_ref_match": job_match,
                            "correlated": bool(channel_match and source_match and job_match),
                            "result_present": True,
                            "result_message_id": None,
                            "result_channel_match": False,
                            "jump_present": isinstance(
                                parsed.get("jump_url") if parsed else None, str
                            ),
                            "jump_ids_match": False,
                            "result_ok": False,
                            "content_len": content_len,
                            "content_sha256": content_hash,
                        }
                    )
                    continue
                assert parsed is not None
                mid = _as_discord_id(parsed.get("message_id"))
                channel_parsed = _as_discord_id(parsed.get("channel_id"))
                jump_raw = parsed.get("jump_url")
                result_channel_match = channel_parsed is not None and channel_parsed == channel_id
                jump_present = isinstance(jump_raw, str) and bool(jump_raw)
                jump_match = False
                if isinstance(jump_raw, str):
                    match = _JUMP_RE.fullmatch(jump_raw.strip())
                    if match is not None and mid is not None:
                        try:
                            guild_part = int(match.group(1))
                            channel_part = int(match.group(2))
                            mid_part = int(match.group(3))
                        except ValueError:
                            guild_part = channel_part = mid_part = 0
                        if (
                            guild_part > 0
                            and channel_part == channel_id
                            and mid_part == mid
                            and (guild_id is None or guild_part == guild_id)
                        ):
                            jump_match = True
                result_ok = bool(
                    channel_match
                    and source_match
                    and job_match
                    and mid is not None
                    and result_channel_match
                    and jump_match
                )
                sends.append(
                    {
                        "tool_call_id": call_id,
                        "channel_match": channel_match,
                        "source_mid_match": source_match,
                        "job_ref_match": job_match,
                        "correlated": bool(channel_match and source_match and job_match),
                        "result_present": True,
                        "result_message_id": mid,
                        "result_channel_match": result_channel_match,
                        "jump_present": jump_present,
                        "jump_ids_match": jump_match,
                        "result_ok": result_ok,
                        "content_len": content_len,
                        "content_sha256": content_hash,
                    }
                )

        correlated = [s for s in sends if s["correlated"]]
        all_confirmed = bool(
            trace_session_match
            and turn_index_match
            and request_user_count == 1
            and request_job_ref_match
            and correlated
            and all(s["result_ok"] for s in correlated)
        )
        return {
            "native_sid": native_sid,
            "requested_turn_index": turn_index,
            "returned_turn_index": index_ret,
            "trace_session_match": trace_session_match,
            "turn_index_match": turn_index_match,
            "status": status_out,
            "status_len": len(status_ret),
            "model_present": model_present,
            "model_len": model_len,
            "model_sha256": model_hash,
            "error_present": error_present,
            "error_kind": error_kind,
            "error_code_len": error_code_len,
            "error_message_len": error_message_len,
            "error_keys_len": error_keys_len,
            "request_job_ref_match": request_job_ref_match,
            "request_job_ref_len": len(job_ref),
            "request_job_ref_sha256": _sha256_hex(job_ref),
            "request_user_count": request_user_count,
            "request_input_sha256": request_input_sha256,
            "request_input_len": request_input_len,
            "send_call_count": len(sends),
            "correlated_send_count": len(correlated),
            "legacy_uncorrelated_count": len(sends) - len(correlated),
            "other_tool_call_count": other_tool_count,
            "other_tool_name_lens": other_name_lens,
            "sends": sends,
            "all_sends_confirmed": all_confirmed,
            "proof": "trace_detail",
        }
