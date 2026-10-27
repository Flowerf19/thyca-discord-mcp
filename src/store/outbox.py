from __future__ import annotations

from typing import Any

from . import queries as sql
from .base import (
    _REQUIRED_ENTRY_FIELDS,
    READ_LIMIT_MAX,
    READ_LIMIT_MIN,
    _now_iso,
    _row_to_entry,
)
from .errors import StoreError


class OutboxMixin:
    """Outbox + poll-cursor API (v1)."""

    def _insert_one(
        self, fields: dict[str, Any], high_water: int
    ) -> tuple[dict[str, Any] | None, int]:
        """INSERT OR IGNORE one entry; returns (entry_or_None_if_dup, new_high)."""
        new_seq = high_water + 1
        ts = fields.get("ts") or _now_iso()
        author_is_bot = fields.get("author_is_bot")
        reply_to = fields.get("reply_to_message_id")
        cur = self._conn.execute(
            sql.OUTBOX_INSERT,
            (
                new_seq,
                fields["type"],
                fields["channel_id"],
                fields["message_id"],
                fields["author_uid"],
                fields["author_name"],
                fields["actor_kind"],
                fields["content"],
                ts,
                None if author_is_bot is None else int(bool(author_is_bot)),
                reply_to,
            ),
        )
        if cur.rowcount == 1:
            self._conn.execute(sql.STATE_BUMP_HIGH_WATER, (new_seq,))
            entry: dict[str, Any] = {
                "seq": new_seq,
                "ts": ts,
                **{k: fields[k] for k in _REQUIRED_ENTRY_FIELDS},
            }
            if author_is_bot is not None:
                entry["author_is_bot"] = fields["author_is_bot"]
                entry["reply_to_message_id"] = reply_to
            return entry, new_seq
        existing = self._conn.execute(
            sql.OUTBOX_GET,
            (fields["channel_id"], fields["message_id"]),
        ).fetchone()
        if existing is None:  # pragma: no cover - defensive; IGNORE implies a row exists
            raise StoreError("outbox insert ignored with no conflicting row")
        return _row_to_entry(existing), high_water

    # -- outbox API (v1, unchanged) --

    def append_entry(self, **fields: Any) -> dict[str, Any]:
        """Append one entry (INSERT OR IGNORE dedupe); returns the row.

        ``seq`` is allocated from ``seq_high_water`` inside the append
        transaction and the high-water advances only on a real insert, so a
        duplicate (same channel+message via realtime+poll) returns the
        existing row without consuming a seq, and deleting the max row never
        reuses its seq.
        """
        if "seq" in fields:
            raise StoreError("append_entry assigns 'seq' itself; do not pass it")
        missing = [name for name in _REQUIRED_ENTRY_FIELDS if name not in fields]
        if missing:
            raise StoreError(f"append_entry missing required fields: {', '.join(missing)}")
        with self._txn():
            _acked, high_water = self._state()
            entry, _new_high = self._insert_one(dict(fields), high_water)
            assert entry is not None
            return entry

    def append_poll_batch(
        self, channel_id: int, items: list[dict[str, Any]], last_message_id: int
    ) -> list[dict[str, Any]]:
        """Append poll entries and advance the poll cursor in ONE transaction.

        ``items`` are full entry field dicts (each with ``message_id`` etc.);
        ``last_message_id`` is the max seen id. The cursor advances only when
        ``items`` is non-empty (mirrors the old tick's ``if collected``).
        Duplicates are ignored without consuming seqs.
        """
        if isinstance(channel_id, bool) or not isinstance(channel_id, int):
            raise StoreError(f"poll channel_id must be an int, got {type(channel_id).__name__}")
        if (
            isinstance(last_message_id, bool)
            or not isinstance(last_message_id, int)
            or last_message_id < 0
        ):
            raise StoreError(f"poll cursor must be a non-negative int, got {last_message_id!r}")
        with self._txn():
            _acked, high_water = self._state()
            out: list[dict[str, Any]] = []
            for fields in items:
                missing = [name for name in _REQUIRED_ENTRY_FIELDS if name not in fields]
                if missing:
                    raise StoreError(f"append_entry missing required fields: {', '.join(missing)}")
                entry, high_water = self._insert_one(dict(fields), high_water)
                assert entry is not None
                out.append(entry)
            if items:
                self._conn.execute(
                    sql.CURSOR_UPSERT,
                    (channel_id, last_message_id),
                )
            return out

    def read_entries(self, limit: int = 50, unread_only: bool = True) -> dict[str, Any]:
        """Read entries ascending by seq (oldest first when truncated).

        ``limit`` is clamped to 1..500. ``unread_only`` keeps ``seq > acked``.
        ``latest`` is the seq high-water (never the log tail).
        """
        # Bool check FIRST: int(True) == 1 would silently accept a bool.
        if isinstance(limit, bool):
            raise StoreError(f"read limit must be an int, got {limit!r}")
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise StoreError(f"read limit must be an int, got {limit!r}") from None
        limit = max(READ_LIMIT_MIN, min(READ_LIMIT_MAX, limit))
        acked, high_water = self._state()
        if unread_only:
            rows = self._conn.execute(
                sql.OUTBOX_READ_UNREAD,
                (acked, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(sql.OUTBOX_READ_ALL, (limit,)).fetchall()
        return {
            "entries": [_row_to_entry(r) for r in rows],
            "acked": acked,
            "latest": high_water,
        }

    def ack_entries(self, up_to_seq: int) -> int:
        """Mark entries up to ``up_to_seq`` as read. Returns the new ``acked``.

        Never moves ``acked`` backward. Rejects ``up_to_seq`` beyond the
        latest high-water instead of silently clamping it.
        """
        if isinstance(up_to_seq, bool) or not isinstance(up_to_seq, int):
            raise StoreError(f"ack up_to_seq must be an int, got {type(up_to_seq).__name__}")
        if up_to_seq < 0:
            raise StoreError(f"ack up_to_seq must be >= 0, got {up_to_seq}")
        with self._txn():
            acked, high_water = self._state()
            if up_to_seq > high_water:
                raise StoreError(f"ack up_to_seq {up_to_seq} beyond latest seq {high_water}")
            new_acked = max(acked, up_to_seq)
            if new_acked != acked:
                self._conn.execute(sql.STATE_SET_ACKED, (new_acked,))
            return new_acked

    def get_poll_cursor(self, channel_id: int) -> int | None:
        """Last polled message id for a channel, or None when never polled."""
        row = self._conn.execute(sql.CURSOR_GET, (channel_id,)).fetchone()
        return None if row is None else row["last_message_id"]

    def set_poll_cursor(self, channel_id: int, last_id: int) -> int:
        """Persist the poll cursor for a channel (independent of ack)."""
        if isinstance(channel_id, bool) or not isinstance(channel_id, int):
            raise StoreError(f"poll channel_id must be an int, got {type(channel_id).__name__}")
        if isinstance(last_id, bool) or not isinstance(last_id, int) or last_id < 0:
            raise StoreError(f"poll cursor must be a non-negative int, got {last_id!r}")
        with self._txn():
            self._conn.execute(
                sql.CURSOR_UPSERT,
                (channel_id, last_id),
            )
            return last_id
