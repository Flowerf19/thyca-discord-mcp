"""Boundary helpers for the MCP server composition root.

Moved verbatim from server.py: outbox consistency guard, coarse audit error
kind, send metadata args, unvalidated correlation args, and exact jump-URL
segment match. The scoped text-channel resolver lives in
:mod:`source_messages` (shared without a
responder->server_helpers->responder import cycle) and is re-exported here
so existing ``server_helpers._resolve_text_channel`` imports keep working.
No other logic lives here.
"""

from __future__ import annotations

import hashlib
from typing import Any

import discord

from config import Settings
from delivery import DeliveryError
from outbox import OutboxError, read_entries
from responder import ResponderError
from source_messages import _resolve_text_channel

__all__ = [
    "_check_outbox_consistent",
    "_resolve_text_channel",
    "_coarse_error_kind",
    "_send_meta_args",
    "_unvalidated_correlation_args",
    "_jump_url_ids_match",
]


def _check_outbox_consistent(settings: Settings) -> None:
    """Fail loud when outbox state/log disagree (read-only, no writes).

    State-only core APIs (ack, poll cursors) cannot see the log, so every
    server path that advances ack or a poll cursor runs this check first. A
    missing/zeroed state next to a non-empty log, acked > seq, or corrupt
    data raises instead of resetting state or duplicating seqs.
    """
    read_entries(settings.outbox_path, settings.outbox_state_path, limit=1, unread_only=False)


def _coarse_error_kind(exc: BaseException) -> str:
    """Fixed coarse audit kind for v5 auto paths (never raw text or class names)."""
    if isinstance(exc, discord.DiscordException):
        return "discord_error"
    if isinstance(exc, DeliveryError):  # includes DeliveryDurabilityUncertain
        return "delivery_error"
    if isinstance(
        exc, (ResponderError, OutboxError, ValueError, TypeError, LookupError, PermissionError)
    ):
        return "validation_error"
    return "unknown"


def _send_meta_args(
    channel_id: int,
    content: str,
    source_message_id: int | None = None,
    job_ref: str | None = None,
) -> dict[str, Any]:
    """Metadata-only audit args for send_message (never raw content).

    Correlation IDs are kept raw here, so callers must pass only VALIDATED
    identifiers (see _unvalidated_correlation_args for pre-acceptance audits).
    """
    args: dict[str, Any] = {"channel_id": channel_id}
    if isinstance(content, str):
        args["content_len"] = len(content)
        args["content_sha256"] = hashlib.sha256(content.encode("utf-8")).hexdigest()
    if source_message_id is not None:
        args["source_message_id"] = source_message_id
    if job_ref is not None:
        args["job_ref"] = job_ref
    return args


def _unvalidated_correlation_args(source_message_id: Any, job_ref: Any) -> dict[str, Any]:
    """Pre-acceptance correlation audit args: no raw unvalidated IDs.

    A strict-positive-int source MID is inherently metadata-safe and kept raw;
    every job_ref stays length/hash metadata until correlated_job ACCEPTS it,
    so a rejected private-text canary never reaches the audit log verbatim.
    """
    args: dict[str, Any] = {}
    if (
        isinstance(source_message_id, int)
        and not isinstance(source_message_id, bool)
        and source_message_id > 0
    ):
        args["source_message_id"] = source_message_id
    elif isinstance(source_message_id, str):
        args["source_message_id_len"] = len(source_message_id)
        args["source_message_id_sha256"] = hashlib.sha256(
            source_message_id.encode("utf-8")
        ).hexdigest()
    elif source_message_id is not None:
        args["source_message_id_invalid"] = True
    if isinstance(job_ref, str):
        args["job_ref_len"] = len(job_ref)
        args["job_ref_sha256"] = hashlib.sha256(job_ref.encode("utf-8")).hexdigest()
    elif job_ref is not None:
        args["job_ref_invalid"] = True
    return args


def _jump_url_ids_match(jump_url: Any, guild_id: int, channel_id: int, message_id: int) -> bool:
    """Exact native jump-URL proof: discord host + guild/channel/message segments.

    The native SDK builds ``https://discord.com/channels/<guild>/<channel>/
    <message>`` for guild sends (DMs use ``@me`` and never occur on this
    path). Every segment must equal the delivered scope exactly.
    """
    prefix = "https://discord.com/channels/"
    if not isinstance(jump_url, str) or not jump_url.startswith(prefix):
        return False
    return jump_url[len(prefix) :].split("/") == [
        str(guild_id),
        str(channel_id),
        str(message_id),
    ]
