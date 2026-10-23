"""Read-side service: channel history + single message as clean Markdown.

The context contract for the transport surface: callers get ONE Markdown block
(``data.markdown``) plus the structured rows it was rendered from. Message
bodies are whole (never truncated) and escaped just enough that user content
cannot inject headings or code fences into the document. No prompt, memory, or
per-call instructions are ever embedded in the result — the calling bot owns
its own prompt.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from adapters.discord_gateway import GatewayMessage

READ_LIMIT_MAX = 50
READ_LIMIT_MIN = 1

_HEADING_BREAK = re.compile(r"^(\s{0,3})#")
_FENCE_BREAK = re.compile(r"^(\s{0,3})(`{3,}|~{3,})")
_INLINE_UNSAFE = re.compile(r"[\\`*]")


def _escape_body(content: str) -> str:
    """Minimal Markdown escape for untrusted message bodies.

    Escapes a leading ``#`` (fake heading) and a leading `` ``` ``/``~~~``
    run (fake code fence) per line — enough to keep the surrounding document
    structure honest without mangling regular text.
    """
    out_lines: list[str] = []
    for line in content.split("\n"):
        line = _HEADING_BREAK.sub(r"\1\\#", line)
        line = _FENCE_BREAK.sub(lambda m: m.group(1) + "".join(f"\\{c}" for c in m.group(2)), line)
        out_lines.append(line)
    return "\n".join(out_lines)


def _inline(text: str) -> str:
    """Single-line inline text: collapse newlines, escape ``*``/`` ` ``/``\\``."""
    return _INLINE_UNSAFE.sub(r"\\\g<0>", " ".join(text.split()))


def _ts_label(timestamp: str) -> str:
    """``YYYY-MM-DD HH:MM UTC`` when the ISO timestamp parses, else inline text."""
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return _inline(timestamp)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render_markdown(rows: list[dict[str, Any]], heading: str) -> str:
    """Render message rows as one Markdown block, oldest → newest.

    Row keys: ``author_name``, ``author_uid``, ``timestamp``, ``message_id``,
    ``content``; optional ``reply_to_message_id``, ``seq``, ``actor_kind``.
    """
    lines = [heading, ""]
    for row in rows:
        meta = (
            f"**{_inline(str(row['author_name']))}** (`{row['author_uid']}`)"
            f" · {_ts_label(str(row['timestamp']))} · msg `{row['message_id']}`"
        )
        if row.get("reply_to_message_id") is not None:
            meta += f" · reply-to `{row['reply_to_message_id']}`"
        if row.get("seq") is not None:
            meta += f" · seq `{row['seq']}`"
        if row.get("actor_kind"):
            meta += f" · {_inline(str(row['actor_kind']))}"
        lines.append(meta)
        content = row.get("content") or ""
        lines.append(_escape_body(content) if content else "*(empty message)*")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _channel_heading(channel_name: str | None, channel_id: int, count: int) -> str:
    label = f"#{channel_name}" if channel_name else f"channel {channel_id}"
    return f"# {_inline(label)} · {count} message{'s' if count != 1 else ''} (oldest → newest)"


def _require_message_id(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"invalid {name}")
    return value


async def read_messages(
    gateway: Any,
    *,
    channel_id: Any,
    before_message_id: Any = None,
    limit: Any = READ_LIMIT_MAX,
) -> dict[str, Any]:
    """Read up to ``limit`` (1..50) messages of a scoped channel, oldest → newest.

    ``before_message_id`` paginates backwards (exclusive bound, like the SDK).
    Raises ``ValueError`` on invalid args and the gateway's typed errors
    (``scope_denied``/``not_found``/``discord_api``) on Discord failures.
    """
    cid = _require_message_id(channel_id, "channel_id")
    if before_message_id is not None:
        _require_message_id(before_message_id, "before_message_id")
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not READ_LIMIT_MIN <= limit <= READ_LIMIT_MAX
    ):
        raise ValueError(f"limit must be {READ_LIMIT_MIN}..{READ_LIMIT_MAX}")
    channel_name = await gateway.channel_name(cid)
    messages = await gateway.history(cid, before_message_id=before_message_id, limit=limit)
    rows = [m.to_row() for m in messages]
    return {
        "channel_id": cid,
        "channel_name": channel_name,
        "count": len(rows),
        "messages": rows,
        "markdown": render_markdown(
            rows, _channel_heading(channel_name, cid, len(rows))
        ),
    }


async def get_message(
    gateway: Any,
    *,
    channel_id: Any,
    message_id: Any,
) -> dict[str, Any]:
    """Fetch one exact message in a scoped channel (Markdown + structured row)."""
    cid = _require_message_id(channel_id, "channel_id")
    mid = _require_message_id(message_id, "message_id")
    channel_name = await gateway.channel_name(cid)
    message: GatewayMessage = await gateway.fetch_message(cid, mid)
    row = message.to_row()
    heading = f"# {_inline('#' + channel_name) if channel_name else 'message'} · msg `{mid}`"
    return {
        "channel_id": cid,
        "channel_name": channel_name,
        "message": row,
        "markdown": render_markdown([row], heading),
    }


__all__ = [
    "READ_LIMIT_MAX",
    "get_message",
    "read_messages",
    "render_markdown",
]
