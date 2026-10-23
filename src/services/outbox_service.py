"""Outbox service over the durable capture log (read + ack + Markdown view).

Thin wrapper around :mod:`outbox` — the single-writer JSONL outbox. ACK is a
pure ``seq`` cursor: no caller-scoped prefix guard lives here (that policy was
caller-side and moved out with the deprecated auto-reply path).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from outbox import ack_entries, read_entries
from services.context_reader import render_markdown


def _entry_row(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "author_name": entry.get("author_name") or "?",
        "author_uid": entry.get("author_uid"),
        "timestamp": entry.get("ts") or "",
        "message_id": entry.get("message_id"),
        "reply_to_message_id": entry.get("reply_to_message_id"),
        "content": entry.get("content") or "",
        "seq": entry.get("seq"),
        "actor_kind": entry.get("actor_kind"),
    }


def render_entries_markdown(entries: list[dict[str, Any]], *, acked: int, latest: int) -> str:
    """One Markdown block for a pulled page, oldest → newest, with seq labels."""
    heading = f"# outbox · {len(entries)} entries · acked `{acked}` · latest `{latest}`"
    return render_markdown([_entry_row(e) for e in entries], heading)


def read_outbox(
    outbox_path: str | Path,
    outbox_state_path: str | Path,
    *,
    limit: int = 50,
    unread_only: bool = True,
) -> dict[str, Any]:
    """Pull entries oldest-first plus a ``markdown`` rendering of the page."""
    result = read_entries(
        outbox_path, outbox_state_path, limit=limit, unread_only=unread_only
    )
    result["markdown"] = render_entries_markdown(
        result["entries"], acked=result["acked"], latest=result["latest"]
    )
    return result


def ack_outbox(
    outbox_path: str | Path,
    outbox_state_path: str | Path,
    *,
    up_to_seq: Any,
) -> dict[str, Any]:
    """Pure-seq ACK: mark entries up to ``up_to_seq`` read; rejects beyond latest."""
    acked = ack_entries(outbox_state_path, up_to_seq)
    return {"acked": acked}


__all__ = ["ack_outbox", "read_outbox", "render_entries_markdown"]
