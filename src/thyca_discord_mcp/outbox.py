"""Durable outbox: capture Discord messages to disk so the agent can pull them later.

Layout: ``outbox.jsonl`` (one JSON object per line, ``seq`` increasing) plus
``outbox_state.json`` (``{"seq": int, "acked": int, "poll": {channel_id: last_id}}``).

Durability model (single-process / single asyncio loop only):
- All operations are synchronous with no ``await`` inside a mutation, so two
  mutations cannot interleave on one loop. No custom locks or threads.
- ``append_entry`` reserves the ``seq`` in the state file *before* appending the
  log line. A crash between the two leaves a sequence gap, which is acceptable
  (``seq`` must increase, not be contiguous); an allocated ``seq`` is never reused.
- State is saved atomically (write ``.tmp`` + ``flush``/``fsync`` + ``os.replace``);
  log lines are flushed + fsynced after each append.
- A present state file must carry all mandatory keys (``seq``, ``acked``, ``poll``)
  with ``acked <= seq``; there are no silent defaults for a present state file.
- Missing state next to a non-empty log is a loud failure: ``append_entry`` and
  ``read_entries`` raise BEFORE any write (missing-state auto-recovery would
  erase ``acked``/poll cursors). The same guard rejects a ``seq``-0 state next
  to a non-empty log, which can only arise from state loss followed by a
  state-only write — restore the state file from backup instead.
- ``read_entries`` validates the log (required fields, ``seq >= 1`` strictly
  increasing) and rejects a stale state whose ``seq`` is behind the log's
  highest ``seq``. ``append_entry`` only runs O(1) missing/zero-state guards
  (``stat``); it does not scan the log per event.
- ``ack_entries``/``get_poll_cursor``/``set_poll_cursor`` are state-only: they
  cannot see the log, so callers must ensure the state file corresponds to the
  log (the server validates via ``read_entries`` before ack/poll writes).
- Corrupt or malformed data raises :class:`OutboxError` with context (path, line
  number, field name). Nothing is silently reset. No secrets or entry contents
  are ever included in errors.

No stale-state recovery is claimed. Cross-process / multi-writer use is NOT supported.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from thyca_discord_mcp.identity import ActorKind, classify_actor

READ_LIMIT_MIN = 1
READ_LIMIT_MAX = 500

_REQUIRED_ENTRY_FIELDS = (
    "type",
    "channel_id",
    "message_id",
    "author_uid",
    "author_name",
    "actor_kind",
    "content",
)

_REQUIRED_LOG_FIELDS = ("seq", "ts", *_REQUIRED_ENTRY_FIELDS)


class OutboxError(ValueError):
    """Corrupt outbox/state data or an invalid outbox operation."""


def _fresh_state() -> dict[str, Any]:
    return {"seq": 0, "acked": 0, "poll": {}}


def _validate_state(raw: Any, path: Path) -> dict[str, Any]:
    """Validate a present state file. All keys are mandatory; no silent defaults."""
    if not isinstance(raw, dict):
        raise OutboxError(f"corrupt outbox state {path}: top level must be an object")
    for name in ("seq", "acked", "poll"):
        if name not in raw:
            raise OutboxError(f"corrupt outbox state {path}: missing key {name!r}")
    state = dict(raw)  # preserve unknown keys for forward compatibility
    for name in ("seq", "acked"):
        value = raw[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise OutboxError(f"corrupt outbox state {path}: {name!r} must be a non-negative int")
        state[name] = value
    if state["acked"] > state["seq"]:
        raise OutboxError(
            f"corrupt outbox state {path}: 'acked' ({state['acked']}) "
            f"exceeds 'seq' ({state['seq']})"
        )
    poll_raw = raw["poll"]
    if not isinstance(poll_raw, dict):
        raise OutboxError(f"corrupt outbox state {path}: 'poll' must be an object")
    poll: dict[int, int] = {}
    for key, value in poll_raw.items():
        if isinstance(key, bool):
            raise OutboxError(f"corrupt outbox state {path}: bad poll channel {key!r}")
        try:
            channel_id = int(key)
        except (TypeError, ValueError):
            raise OutboxError(f"corrupt outbox state {path}: bad poll channel {key!r}") from None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise OutboxError(f"corrupt outbox state {path}: bad poll cursor for channel {key!r}")
        poll[channel_id] = value
    state["poll"] = poll
    return state


def _load_state(state_path: str | Path) -> dict[str, Any]:
    """Load state; a missing file is a fresh outbox, corrupt data raises."""
    path = Path(state_path)
    if not path.exists():
        return _fresh_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise OutboxError(f"corrupt outbox state {path}: invalid JSON ({e})") from e
    return _validate_state(raw, path)


def _log_nonempty(outbox_path: Path) -> bool:
    """O(1) guard: is there a log file with any bytes? Missing/empty means fresh."""
    try:
        return outbox_path.stat().st_size > 0
    except FileNotFoundError:
        return False


def _save_state(state_path: str | Path, state: dict[str, Any]) -> None:
    """Atomically replace the state file, preserving fields the caller did not touch."""
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _read_log(outbox_path: str | Path) -> list[dict[str, Any]]:
    """Read all log entries; a missing file is empty, malformed lines raise.

    Single pass: every line must be an object with all required fields and a
    ``seq`` that is an int >= 1 strictly greater than the previous line's
    ``seq`` (gaps allowed, duplicates/reorderings rejected).
    """
    path = Path(outbox_path)
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    previous_seq = 0
    with path.open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                raise OutboxError(f"corrupt outbox log {path} line {lineno}: blank line")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                raise OutboxError(
                    f"corrupt outbox log {path} line {lineno}: invalid JSON ({e})"
                ) from e
            if not isinstance(record, dict):
                raise OutboxError(
                    f"corrupt outbox log {path} line {lineno}: entry must be an object"
                )
            for name in _REQUIRED_LOG_FIELDS:
                if name not in record:
                    raise OutboxError(
                        f"corrupt outbox log {path} line {lineno}: missing field {name!r}"
                    )
            seq = record["seq"]
            if isinstance(seq, bool) or not isinstance(seq, int):
                raise OutboxError(f"corrupt outbox log {path} line {lineno}: 'seq' must be an int")
            if seq < 1:
                raise OutboxError(f"corrupt outbox log {path} line {lineno}: 'seq' must be >= 1")
            if seq <= previous_seq:
                raise OutboxError(
                    f"corrupt outbox log {path} line {lineno}: 'seq' not strictly "
                    f"increasing (previous {previous_seq})"
                )
            previous_seq = seq
            entries.append(record)
    return entries


def append_entry(outbox_path: str | Path, state_path: str | Path, **fields: Any) -> dict[str, Any]:
    """Append one entry and return it. Assigns ``seq`` (increasing) and ``ts`` by default.

    The ``seq`` is persisted in the state file before the log line is written, so a
    crash can only leave a gap, never a reused ``seq``. ``acked`` and poll cursors
    in the state file are preserved.

    Fail-loud guards (checked BEFORE any write): a missing state file, or a
    ``seq``-0 state file, next to a non-empty log raises — restore the state
    file from backup instead of appending. Only a missing/empty log starts fresh.
    """
    if "seq" in fields:
        raise OutboxError("append_entry assigns 'seq' itself; do not pass it")
    missing = [name for name in _REQUIRED_ENTRY_FIELDS if name not in fields]
    if missing:
        raise OutboxError(f"append_entry missing required fields: {', '.join(missing)}")
    out_path = Path(outbox_path)
    state_file = Path(state_path)
    if not state_file.exists():
        if _log_nonempty(out_path):
            raise OutboxError(
                f"outbox state {state_file} is missing but log {out_path} is non-empty; "
                "refusing to reset (restore the state file from backup)"
            )
        state = _fresh_state()
    else:
        state = _load_state(state_file)
        if state["seq"] == 0 and _log_nonempty(out_path):
            raise OutboxError(
                f"outbox state {state_file} is at seq 0 but log {out_path} is non-empty; "
                "stale state, refusing to append (restore the state file from backup)"
            )
    state["seq"] += 1
    entry: dict[str, Any] = {
        "seq": state["seq"],
        "ts": fields.pop("ts", None) or datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    _save_state(state_file, state)  # reserve the seq first (see module docstring)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    return entry


def read_entries(
    outbox_path: str | Path,
    state_path: str | Path,
    limit: int = 50,
    unread_only: bool = True,
) -> dict[str, Any]:
    """Read entries in ascending ``seq`` order (oldest first when truncated).

    ``limit`` is clamped to 1..500. ``unread_only`` keeps ``seq > acked``.
    A missing state file next to a non-empty log raises, as does a stale state
    whose ``seq`` is behind the log's highest ``seq`` — ``latest`` always comes
    from the validated state, never from the log alone.
    """
    limit = max(READ_LIMIT_MIN, min(READ_LIMIT_MAX, limit))
    state_file = Path(state_path)
    if not state_file.exists():
        entries = _read_log(outbox_path)
        if entries:
            raise OutboxError(
                f"outbox state {state_file} is missing but the log holds "
                f"{len(entries)} entries; refusing to read against a reset state "
                "(restore the state file from backup)"
            )
        return {"entries": [], "acked": 0, "latest": 0}
    state = _load_state(state_file)
    entries = _read_log(outbox_path)
    entries.sort(key=lambda e: e["seq"])  # stable; the log is already append-ordered
    acked = state["acked"]
    latest = state["seq"]
    if entries and entries[-1]["seq"] > latest:
        raise OutboxError(
            f"stale outbox state {state_file}: seq {latest} is behind the log's "
            f"highest seq {entries[-1]['seq']} (restore the state file from backup)"
        )
    if unread_only:
        entries = [e for e in entries if e["seq"] > acked]
    return {"entries": entries[:limit], "acked": acked, "latest": latest}


def ack_entries(state_path: str | Path, up_to_seq: int) -> int:
    """Mark entries up to ``up_to_seq`` as read. Returns the new ``acked`` value.

    Never moves ``acked`` backward. Rejects ``up_to_seq`` beyond the latest known
    ``seq`` instead of silently clamping it.

    State-only: this helper cannot see the log. Precondition — the state file
    must correspond to the log (the server validates via ``read_entries``
    before ack/poll writes).
    """
    if isinstance(up_to_seq, bool) or not isinstance(up_to_seq, int):
        raise OutboxError(f"ack up_to_seq must be an int, got {type(up_to_seq).__name__}")
    if up_to_seq < 0:
        raise OutboxError(f"ack up_to_seq must be >= 0, got {up_to_seq}")
    state = _load_state(state_path)
    if up_to_seq > state["seq"]:
        raise OutboxError(f"ack up_to_seq {up_to_seq} beyond latest seq {state['seq']}")
    state["acked"] = max(state["acked"], up_to_seq)
    _save_state(state_path, state)
    return state["acked"]


def get_poll_cursor(state_path: str | Path, channel_id: int) -> int | None:
    """Last polled message id for a channel, or None when never polled.

    State-only: cannot see the log; see ``ack_entries`` for the precondition.
    """
    state = _load_state(state_path)
    return state["poll"].get(channel_id)


def set_poll_cursor(state_path: str | Path, channel_id: int, last_id: int) -> int:
    """Persist the poll cursor for a channel (independent of ack).

    State-only: cannot see the log; see ``ack_entries`` for the precondition.
    """
    if isinstance(channel_id, bool) or not isinstance(channel_id, int):
        raise OutboxError(f"poll channel_id must be an int, got {type(channel_id).__name__}")
    if isinstance(last_id, bool) or not isinstance(last_id, int) or last_id < 0:
        raise OutboxError(f"poll cursor must be a non-negative int, got {last_id!r}")
    state = _load_state(state_path)
    state["poll"][channel_id] = last_id
    _save_state(state_path, state)
    return last_id


def build_message_entry(
    scope_guild_id: int | None,
    guild_id: int | None,
    channel_id: int,
    message_id: int,
    author_uid: int,
    author_name: str,
    content: str | None,
    bot_uid: int,
    owner_uid: int,
    watch_channel_ids: list[int],
    author_is_bot: bool | None = None,
    reply_to_message_id: int | None = None,
) -> dict[str, Any] | None:
    """Build a realtime entry's fields, or None when the message must not be captured.

    Pure function (no discord imports): drops out-of-scope guilds (including DMs),
    the bot's own messages, and unwatched channels. Owner/other messages are kept
    even with empty content. Poll entries (which keep ``self`` messages) are built
    separately by the poll tick in T4 using :func:`classify_actor` directly.

    v5 capture metadata (optional, backward compatible): when ``author_is_bot``
    is provided (native ``message.author.bot``), both ``author_is_bot`` and
    ``reply_to_message_id`` (native ``message.reference.message_id`` or None)
    are persisted. Legacy callers omitting ``author_is_bot`` get entries
    without either key; absence means unknown, never assumed human.
    """
    if guild_id != scope_guild_id:
        return None
    actor = classify_actor(author_uid, author_name, bot_uid=bot_uid, owner_uid=owner_uid)
    if actor.kind is ActorKind.SELF:
        return None
    if channel_id not in watch_channel_ids:
        return None
    entry: dict[str, Any] = {
        "type": "message",
        "channel_id": channel_id,
        "message_id": message_id,
        "author_uid": author_uid,
        "author_name": author_name,
        "actor_kind": actor.kind.value,
        "content": content if content else "",
    }
    if author_is_bot is not None:
        entry["author_is_bot"] = author_is_bot
        entry["reply_to_message_id"] = reply_to_message_id
    return entry
