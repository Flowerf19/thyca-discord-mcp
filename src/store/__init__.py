"""SQLite store: outbox + jobs + Jev + sessions on one SQLite DB.

Schema v2:

- ``outbox``: ``seq INTEGER PRIMARY KEY`` plus capture columns;
  ``UNIQUE(channel_id, message_id)`` with ``INSERT OR IGNORE`` ends the
  realtime/poll duplication class (first write wins on edit conflicts).
- ``state``: singleton row ``id=1 CHECK`` holding ``acked`` (highest read
  seq), ``seq_high_water`` (highest allocated seq, never decreases), and
  ``native_sid`` (current serving native session; NULL until seeded).
  ``seq`` is allocated from ``seq_high_water`` inside the append
  transaction: a bare rowid could be reused after deleting the max row,
  and deploy seeding needs a high-water that survives an empty table.
- ``poll_cursors``: ``channel_id PK`` -> ``last_message_id``.
- ``jobs``: ``PRIMARY KEY (channel_id, message_id)``,
  ``seq INTEGER UNIQUE NOT NULL REFERENCES outbox(seq)``,
  ``job_ref`` partial-unique where not null, ``stage``,
  ``outcome`` (only on ``done``), ``skip_reason`` (only on ``skipped``),
  ``fail_kind``/``http_status`` (only on ``failed``), ``native_sid`` set
  at submit time, ``pre_turn_index`` captured before the POST (expected
  index derivation only -- no fingerprint or turn binding is stored).
  Stages: ``queued|submitted|sent|done|skipped|failed`` with
  ``outcome CHECK('sent','no_reply','observed')`` on ``done``. Every
  outbox row gets a job row at ingest (filtered ones inserted directly
  as ``skipped``), so the ACK prefix needs no actor-kind exceptions.
  Index ``(stage, seq)`` for the FIFO scan.
- ``sends``: ``PRIMARY KEY (channel_id, message_id, sent_message_id)``
  FK->jobs. A job legitimately has several correlated sends (multi-chunk
  replies); there is no single ``sent_message_id`` column. The worker
  knows ">=1 send + turn ended", never "all chunks" -- the accepted
  best-effort trade.
- ``jev_decisions``: ``PRIMARY KEY (channel_id, message_id)`` FK->jobs.
  Keyed by message only (never context hash): a second paid call for the
  same message when context changes is forbidden. ``context_sha256`` is a
  checked column on cache hits. Statuses
  ``pending|decided|failed_known|failed_unknown`` plus an ``attempts``
  column implementing the one-retry rule: a crashed ``pending`` with
  ``attempts < 2`` gets exactly one more paid call; an unknown outcome at
  ``attempts == 2`` resolves terminally as ``observe`` -- never a third
  paid call for the same ``(channel_id, message_id)``. A ``decided`` row
  is reused from cache (no re-pay). Any ``pending``/``failed_unknown``
  for another key blocks new paid calls (cached ``decided`` still
  returns). Indexes on ``(status)`` and ``(attempted_day)``.
- ``sessions``: ``day TEXT PK`` -> ``native_sid`` + ``adopted_at``
  (provenance only; the current SID lives in ``state.native_sid``).

Concurrency/durability model (single-process / single asyncio loop only):
- ``isolation_level=None`` (autocommit) + explicit ``BEGIN IMMEDIATE`` per
  mutation. No transaction is ever held across an ``await``: every method
  below is synchronous, and callers fetch Discord history / run the Jev
  provider / POST to Thyca / send via Discord BEFORE/AFTER (never inside)
  store transactions. The Jev paid call, Thyca POST, and Discord send sit
  between transactions by design.
- Pragmas on every open: ``journal_mode=WAL``, ``synchronous=FULL``
  (WAL's default NORMAL can lose the last committed txn on power loss --
  unacceptable for the Jev pending marker),
  ``foreign_keys=ON``, ``busy_timeout=5000``.
- Startup runs ``PRAGMA quick_check`` + a ``user_version`` gate and fails
  loudly (``StoreError``). Never auto-reset, never delete. v1 databases
  migrate to v2 in one transaction inside ``Store.open``; anything newer
  than v2 refuses.
- The DB file is created ``0600`` (like the old state files), and the
  ``-wal``/``-shm`` sidecars are best-effort ``0600`` on every open. One
  connection is used at a time from the single asyncio loop (sqlite calls
  here are sub-millisecond -- this assumption is stated instead of
  building an async DB layer).
- Restart recovery (``sweep_submitted_failed``) is explicit and runs once
  at responder startup, NOT inside ``Store.open``: opens happen per
  operation, and sweeping on every open would fail live ``submitted``
  jobs. ``queued`` jobs resume normally.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import StoreBase
from .errors import StoreError  # noqa: F401 - canonical re-export
from .jev import JevMixin
from .jobs import JobsMixin
from .outbox import OutboxMixin


class Store(OutboxMixin, JobsMixin, JevMixin, StoreBase):
    """One SQLite connection; all mutations are short explicit transactions."""


# Module-level convenience wrappers (open-use-close; each open re-validates).
# Server/responder call sites use these so every operation fail-louds on a
# corrupt DB without holding a connection across awaits.


def append_entry(db_path: str | Path, **fields: Any) -> dict[str, Any]:
    with Store.open(db_path) as store:
        return store.append_entry(**fields)


def append_poll_batch(
    db_path: str | Path, channel_id: int, items: list[dict[str, Any]], last_message_id: int
) -> list[dict[str, Any]]:
    with Store.open(db_path) as store:
        return store.append_poll_batch(channel_id, items, last_message_id)


def read_entries(db_path: str | Path, limit: int = 50, unread_only: bool = True) -> dict[str, Any]:
    with Store.open(db_path) as store:
        return store.read_entries(limit=limit, unread_only=unread_only)


def ack_entries(db_path: str | Path, up_to_seq: int) -> int:
    with Store.open(db_path) as store:
        return store.ack_entries(up_to_seq)


def get_poll_cursor(db_path: str | Path, channel_id: int) -> int | None:
    with Store.open(db_path) as store:
        return store.get_poll_cursor(channel_id)


def set_poll_cursor(db_path: str | Path, channel_id: int, last_id: int) -> int:
    with Store.open(db_path) as store:
        return store.set_poll_cursor(channel_id, last_id)
