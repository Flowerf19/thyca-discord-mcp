"""Pure protocol validation for the thin native Thyca client (no I/O).

Canonical owner of the SID grammar, protocol bounds/grammar constants, and
the pure validation helpers also importable from
:mod:`modum.thyca_client` (same objects, re-exported there).
Depends only on :mod:`modum.thyca_errors`; never on the client.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import urllib.parse
from typing import Any

from .thyca_errors import (
    ThycaBusyError,
    ThycaPreconditionError,
    ThycaProtocolError,
)

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

# Exact native MCP tool names. Prefix/substring matches are rejected.
SEND_TOOL_NAME = "discord__send_message"
NO_REPLY_TOOL_NAME = "discord__record_no_reply"
ARG_CHANNEL = "channel_id"
ARG_SOURCE_MID = "source_message_id"
ARG_JOB_REF = "job_ref"
ARG_CONTENT = "content"
ARG_REASON = "reason"
ARG_STATUS = "status"
NO_REPLY_REASONS = frozenset({"no_reply_needed", "missing_context"})
NO_REPLY_STATUS_DECLARED = "declared"

# Exact Discord jump URL, not a substring search.
_JUMP_RE = re.compile(r"^https://discord\.com/channels/(\d+)/(\d+)/(\d+)$")

_TRACE_STATUSES = frozenset({"completed", "failed", "loop_limit", "context_limit"})
_ERROR_KINDS = frozenset({"llm_error", "chat_unavailable"})
_ROLES = frozenset({"user", "assistant", "tool", "system"})


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
