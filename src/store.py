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

import os
import re
import sqlite3
from collections.abc import Callable, Collection, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from validate import is_strict_int as _is_strict_int

SCHEMA_VERSION = 2

READ_LIMIT_MIN = 1
READ_LIMIT_MAX = 500

JOB_STAGES = frozenset({"queued", "submitted", "sent", "done", "skipped", "failed"})
JOB_TERMINAL_STAGES = frozenset({"done", "skipped", "failed"})
JOB_OUTCOMES = frozenset({"sent", "no_reply", "observed"})
SKIP_REASONS = frozenset(
    {
        "out_of_scope",
        "bot_filtered",
        "source_deleted",
        "source_edited_after_classify",
    }
)
FAIL_KINDS = frozenset(
    {
        "timeout",
        "transport_error",
        "http_error",
        "busy",
        "post_cancelled",
        "turn_no_outcome",
        "restart",
        "shutdown",
        "snapshot_mismatch",
    }
)
JEV_STATUSES = frozenset({"pending", "decided", "failed_known", "failed_unknown"})
JEV_PARTICIPATIONS = frozenset({"respond", "observe", "need_context"})

# Row-level SID/day grammar (same shape as the native session store).
# Strict validation of values also happens at the Thyca boundary; the DB
# layer enforces the grammar again so a corrupt row fails loudly here.
_SID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_[0-9a-f]{4}$")
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_REQUIRED_ENTRY_FIELDS = (
    "type",
    "channel_id",
    "message_id",
    "author_uid",
    "author_name",
    "actor_kind",
    "content",
)

_SCHEMA_V1_SQL = """
CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY,
    type TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    author_uid INTEGER NOT NULL,
    author_name TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    content TEXT NOT NULL,
    ts TEXT NOT NULL,
    author_is_bot INTEGER,
    reply_to_message_id INTEGER,
    UNIQUE(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    acked INTEGER NOT NULL DEFAULT 0,
    seq_high_water INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS poll_cursors (
    channel_id INTEGER PRIMARY KEY,
    last_message_id INTEGER NOT NULL
);
"""

_SCHEMA_V2_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    seq INTEGER NOT NULL UNIQUE REFERENCES outbox(seq),
    job_ref TEXT,
    stage TEXT NOT NULL CHECK(stage IN
        ('queued','submitted','sent','done','skipped','failed')),
    outcome TEXT CHECK(outcome IN ('sent','no_reply','observed')),
    skip_reason TEXT CHECK(skip_reason IN ('out_of_scope','bot_filtered',
        'source_deleted','source_edited_after_classify')),
    fail_kind TEXT CHECK(fail_kind IN ('timeout','transport_error','http_error',
        'busy','post_cancelled','turn_no_outcome','restart','shutdown','snapshot_mismatch')),
    http_status INTEGER,
    native_sid TEXT,
    pre_turn_index INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_job_ref ON jobs(job_ref) WHERE job_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_stage_seq ON jobs(stage, seq);
CREATE TABLE IF NOT EXISTS sends (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sent_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id, sent_message_id),
    FOREIGN KEY (channel_id, message_id) REFERENCES jobs(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS jev_decisions (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    context_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','decided','failed_known','failed_unknown')),
    attempts INTEGER NOT NULL CHECK(attempts >= 1),
    attempt_id TEXT NOT NULL,
    participation TEXT CHECK(participation IN ('respond','observe','need_context')),
    author_uid INTEGER NOT NULL,
    attempted_day TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    answers_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id),
    FOREIGN KEY (channel_id, message_id) REFERENCES jobs(channel_id, message_id)
);
CREATE INDEX IF NOT EXISTS jev_status ON jev_decisions(status);
CREATE INDEX IF NOT EXISTS jev_day ON jev_decisions(attempted_day);
CREATE TABLE IF NOT EXISTS sessions (
    day TEXT PRIMARY KEY,
    native_sid TEXT NOT NULL,
    adopted_at TEXT NOT NULL
);
"""


class StoreError(ValueError):
    """Corrupt DB, schema mismatch, or an invalid store operation."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_id(name: str, value: Any, path: Path) -> int:
    if not _is_strict_int(value) or value <= 0:
        raise StoreError(f"store {path}: {name} must be a positive int")
    return value


def _check_sid(value: Any, path: Path) -> str:
    if not isinstance(value, str) or _SID_RE.fullmatch(value) is None:
        raise StoreError(f"store {path}: invalid native_sid")
    return value


def _check_day(value: Any, path: Path) -> str:
    if not isinstance(value, str) or _DAY_RE.fullmatch(value) is None:
        raise StoreError(f"store {path}: invalid day")
    from datetime import date as _date

    try:
        parsed = _date.fromisoformat(value)
    except ValueError:
        raise StoreError(f"store {path}: invalid day") from None
    if parsed.isoformat() != value:
        raise StoreError(f"store {path}: invalid day")
    return value


def _row_to_entry(row: sqlite3.Row) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "seq": row["seq"],
        "ts": row["ts"],
        "type": row["type"],
        "channel_id": row["channel_id"],
        "message_id": row["message_id"],
        "author_uid": row["author_uid"],
        "author_name": row["author_name"],
        "actor_kind": row["actor_kind"],
        "content": row["content"],
    }
    if row["author_is_bot"] is not None:
        entry["author_is_bot"] = bool(row["author_is_bot"])
    if row["reply_to_message_id"] is not None:
        entry["reply_to_message_id"] = row["reply_to_message_id"]
    elif "author_is_bot" in entry:
        # v5 rows always carry both keys; a NULL reply with a non-NULL bot
        # flag round-trips as explicit None.
        entry["reply_to_message_id"] = None
    return entry


def _row_to_jev(row: sqlite3.Row) -> dict[str, Any]:
    record: dict[str, Any] = {
        "channel_id": row["channel_id"],
        "message_id": row["message_id"],
        "context_sha256": row["context_sha256"],
        "status": row["status"],
        "attempts": row["attempts"],
        "attempt_id": row["attempt_id"],
        "participation": row["participation"],
        "author_uid": row["author_uid"],
        "attempted_day": row["attempted_day"],
        "input_tokens": row["input_tokens"],
        "output_tokens": row["output_tokens"],
        "answers_json": row["answers_json"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    return record


class Store:
    """One SQLite connection; all mutations are short explicit transactions."""

    def __init__(self, conn: sqlite3.Connection, path: Path) -> None:
        self._conn = conn
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def open(cls, db_path: str | Path) -> Store:
        """Open (creating 0600 if needed), pragma, verify, migrate, check."""
        path = Path(db_path)
        created = not path.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=FULL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA busy_timeout=5000;")
            try:
                quick = conn.execute("PRAGMA quick_check;").fetchall()
            except sqlite3.Error as exc:
                raise StoreError(f"corrupt database {path}: quick_check failed ({exc})") from exc
            if len(quick) != 1 or quick[0][0] != "ok":
                raise StoreError(f"corrupt database {path}: quick_check failed ({quick!r})")
            version_row = conn.execute("PRAGMA user_version;").fetchone()
            version = version_row[0] if version_row is not None else 0
            if version == 0:
                # Fresh DB (no tables yet) -> create v2 schema. A non-empty
                # file with version 0 but pre-existing tables is also
                # initialized idempotently; CREATE TABLE IF NOT EXISTS keeps
                # it safe.
                conn.executescript(_SCHEMA_V1_SQL + _SCHEMA_V2_SQL)
                conn.execute("ALTER TABLE state ADD COLUMN native_sid TEXT;")
                conn.execute(
                    "INSERT OR IGNORE INTO state (id, acked, seq_high_water) VALUES (1, 0, 0);"
                )
                conn.execute(f"PRAGMA user_version={SCHEMA_VERSION};")
            elif version == 1:
                # v1 -> v2 migration in ONE transaction: new tables plus the
                # nullable native_sid column. v1 outbox rows keep no job
                # rows (deploy reseeds past old history; tests seed fresh).
                # NOTE: executescript() commits implicitly, so the migration
                # statements run individually inside one explicit txn.
                conn.execute("BEGIN IMMEDIATE;")
                try:
                    for stmt in _SCHEMA_V2_SQL.split(";"):
                        if stmt.strip():
                            conn.execute(stmt + ";")
                    cols = [r[1] for r in conn.execute("PRAGMA table_info(state);").fetchall()]
                    if "native_sid" not in cols:
                        conn.execute("ALTER TABLE state ADD COLUMN native_sid TEXT;")
                    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION};")
                    conn.execute("COMMIT;")
                except Exception:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    raise
                # Fail loud when the migrated shape is wrong (never proceed
                # half-migrated).
                cls._require_v2_tables(conn, path)
            elif version == SCHEMA_VERSION:
                # v2 DB: ensure expected tables exist (fail loud, never create
                # silently over a half-written file -- schema creation only
                # happens on the version-0 path above).
                cls._require_v2_tables(conn, path)
                conn.execute(
                    "INSERT OR IGNORE INTO state (id, acked, seq_high_water) VALUES (1, 0, 0);"
                )
            else:
                raise StoreError(
                    f"unsupported database version {version} at {path} "
                    f"(expected {SCHEMA_VERSION}); refusing to auto-migrate or reset"
                )
        except StoreError:
            conn.close()
            raise
        except sqlite3.Error as exc:
            conn.close()
            raise StoreError(f"corrupt database {path}: {exc}") from exc
        except Exception:
            conn.close()
            raise
        if created:
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
        # Best-effort 0600 on the WAL sidecars on every open (not just
        # creation): SQLite recreates -wal/-shm with umask-derived modes
        # whenever they are removed (e.g. after a checkpoint restart).
        # Each chmod is independent; OSError never fails the open.
        for sidecar in (path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
            try:
                if sidecar.exists():
                    os.chmod(sidecar, 0o600)
            except OSError:
                pass
        return cls(conn, path)

    @staticmethod
    def _require_v2_tables(conn: sqlite3.Connection, path: Path) -> None:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}
        expected_tables = (
            "outbox",
            "state",
            "poll_cursors",
            "jobs",
            "sends",
            "jev_decisions",
            "sessions",
        )
        for expected in expected_tables:
            if expected not in tables:
                raise StoreError(f"corrupt database {path}: missing table {expected!r}")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- internal helpers (sync, never across await) --

    @contextmanager
    def _txn(self):
        """One BEGIN IMMEDIATE/COMMIT block with best-effort ROLLBACK (internal).

        Same caller-visible semantics as the hand-rolled blocks it replaces:
        ``BEGIN IMMEDIATE`` on entry, ``COMMIT`` on clean exit, best-effort
        ``ROLLBACK`` + re-raise on any exception (including a ``COMMIT``
        failure). One internal difference: snapshot reads that HEAD ran
        after a mid-body COMMIT now run inside the txn, and durability lands
        at the exit COMMIT. Unobservable under the single-instance /
        single-loop connection model; crash outcomes converge via the
        idempotent transitions + the restart sweep. Never held across an
        ``await``.
        """
        self._conn.execute("BEGIN IMMEDIATE;")
        try:
            yield
            self._conn.execute("COMMIT;")
        except Exception:
            try:
                self._conn.execute("ROLLBACK;")
            except Exception:
                pass
            raise

    def _transition_job(
        self,
        channel_id: int,
        message_id: int,
        *,
        allowed: Collection[str],
        stage_error: Callable[[object], str],
        statements: Sequence[tuple[str, tuple[Any, ...]]],
        extra_guard: Callable[[sqlite3.Row], None] | None = None,
        race_error: str | None = None,
        idempotent: Callable[[sqlite3.Row], bool] | None = None,
    ) -> dict[str, Any]:
        """Shared job-transition skeleton: require row -> stage guard -> write -> dict.

        One transaction: require the job row, return the snapshot early when
        ``idempotent`` matches (already in the target state), deny stages
        outside ``allowed`` via ``stage_error(stage)``, run caller-specific
        pre-write checks (``extra_guard``), execute ``statements`` in order,
        re-read and return the job dict. When ``race_error`` is given the
        LAST statement must affect exactly one row. Every denial message is
        built by the caller, so guards stay explicit at each call site.
        """
        with self._txn():
            row = self._require_job_row(channel_id, message_id)
            if idempotent is not None and idempotent(row):
                return self._job_dict(row)
            if row["stage"] not in allowed:
                raise StoreError(stage_error(row["stage"]))
            if extra_guard is not None:
                extra_guard(row)
            cur = None
            for sql, params in statements:
                cur = self._conn.execute(sql, params)
            if race_error is not None and (cur is None or cur.rowcount != 1):
                raise StoreError(race_error)
            updated = self._get_job_row(channel_id, message_id)
            assert updated is not None
            return self._job_dict(updated)

    def _state(self) -> tuple[int, int]:
        row = self._conn.execute("SELECT acked, seq_high_water FROM state WHERE id=1;").fetchone()
        if row is None:
            raise StoreError(f"corrupt database {self._path}: missing singleton state row")
        acked, high = row["acked"], row["seq_high_water"]
        if (
            isinstance(acked, bool)
            or not isinstance(acked, int)
            or isinstance(high, bool)
            or not isinstance(high, int)
            or acked < 0
            or high < 0
        ):
            raise StoreError(f"corrupt database {self._path}: bad state row")
        if acked > high:
            raise StoreError(
                f"corrupt database {self._path}: 'acked' ({acked}) exceeds high water ({high})"
            )
        return acked, high

    def _insert_one(
        self, fields: dict[str, Any], high_water: int
    ) -> tuple[dict[str, Any] | None, int]:
        """INSERT OR IGNORE one entry; returns (entry_or_None_if_dup, new_high)."""
        new_seq = high_water + 1
        ts = fields.get("ts") or _now_iso()
        author_is_bot = fields.get("author_is_bot")
        reply_to = fields.get("reply_to_message_id")
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO outbox "
            "(seq, type, channel_id, message_id, author_uid, author_name, "
            "actor_kind, content, ts, author_is_bot, reply_to_message_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
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
            self._conn.execute("UPDATE state SET seq_high_water=? WHERE id=1;", (new_seq,))
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
            "SELECT * FROM outbox WHERE channel_id=? AND message_id=?;",
            (fields["channel_id"], fields["message_id"]),
        ).fetchone()
        if existing is None:  # pragma: no cover - defensive; IGNORE implies a row exists
            raise StoreError("outbox insert ignored with no conflicting row")
        return _row_to_entry(existing), high_water

    def _get_job_row(self, channel_id: int, message_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM jobs WHERE channel_id=? AND message_id=?;",
            (channel_id, message_id),
        ).fetchone()

    def _sends_for(self, channel_id: int, message_id: int) -> list[int]:
        rows = self._conn.execute(
            "SELECT sent_message_id FROM sends WHERE channel_id=? AND message_id=? ORDER BY rowid;",
            (channel_id, message_id),
        ).fetchall()
        return [r["sent_message_id"] for r in rows]

    def _job_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        sent_ids = self._sends_for(row["channel_id"], row["message_id"])
        jev_row = self._conn.execute(
            "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
            (row["channel_id"], row["message_id"]),
        ).fetchone()
        jev = _row_to_jev(jev_row) if jev_row is not None else None
        return {
            "channel_id": row["channel_id"],
            "message_id": row["message_id"],
            "seq": row["seq"],
            "job_ref": row["job_ref"],
            "stage": row["stage"],
            "outcome": row["outcome"],
            "skip_reason": row["skip_reason"],
            "fail_kind": row["fail_kind"],
            "http_status": row["http_status"],
            "native_sid": row["native_sid"],
            "pre_turn_index": row["pre_turn_index"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "send_count": len(sent_ids),
            "sent_message_ids": sent_ids,
            "jev": jev,
        }

    def _require_job_row(self, channel_id: int, message_id: int) -> sqlite3.Row:
        _require_id("channel_id", channel_id, self._path)
        _require_id("message_id", message_id, self._path)
        row = self._get_job_row(channel_id, message_id)
        if row is None:
            raise StoreError(f"store {self._path}: unknown job ({channel_id}, {message_id})")
        return row

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
                    "INSERT INTO poll_cursors (channel_id, last_message_id)"
                    " VALUES (?, ?)"
                    " ON CONFLICT(channel_id) DO UPDATE"
                    " SET last_message_id=excluded.last_message_id;",
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
                "SELECT * FROM outbox WHERE seq > ? ORDER BY seq ASC LIMIT ?;",
                (acked, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM outbox ORDER BY seq ASC LIMIT ?;", (limit,)
            ).fetchall()
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
                self._conn.execute("UPDATE state SET acked=? WHERE id=1;", (new_acked,))
            return new_acked

    def get_poll_cursor(self, channel_id: int) -> int | None:
        """Last polled message id for a channel, or None when never polled."""
        row = self._conn.execute(
            "SELECT last_message_id FROM poll_cursors WHERE channel_id=?;", (channel_id,)
        ).fetchone()
        return None if row is None else row["last_message_id"]

    def set_poll_cursor(self, channel_id: int, last_id: int) -> int:
        """Persist the poll cursor for a channel (independent of ack)."""
        if isinstance(channel_id, bool) or not isinstance(channel_id, int):
            raise StoreError(f"poll channel_id must be an int, got {type(channel_id).__name__}")
        if isinstance(last_id, bool) or not isinstance(last_id, int) or last_id < 0:
            raise StoreError(f"poll cursor must be a non-negative int, got {last_id!r}")
        with self._txn():
            self._conn.execute(
                "INSERT INTO poll_cursors (channel_id, last_message_id)"
                " VALUES (?, ?)"
                " ON CONFLICT(channel_id) DO UPDATE"
                " SET last_message_id=excluded.last_message_id;",
                (channel_id, last_id),
            )
            return last_id

    # -- jobs API (v2) --

    def enqueue_job(
        self, channel_id: int, message_id: int, seq: int, *, skip_reason: str | None = None
    ) -> dict[str, Any]:
        """Insert the job row for an outbox row (one transaction).

        Fresh jobs land ``queued``; filtered captures pass ``skip_reason``
        and land directly ``skipped`` (ingest never enqueues-then-skips).
        Every outbox row gets exactly one job row, so the ACK prefix needs
        no actor-kind exceptions.
        """
        _require_id("channel_id", channel_id, self._path)
        _require_id("message_id", message_id, self._path)
        if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
            raise StoreError(f"store {self._path}: seq must be a positive int")
        if skip_reason is not None and skip_reason not in SKIP_REASONS:
            raise StoreError(f"store {self._path}: invalid skip_reason {skip_reason!r}")
        stage = "skipped" if skip_reason is not None else "queued"
        now = _now_iso()
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO jobs (channel_id, message_id, seq, stage, skip_reason,"
                    " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    (channel_id, message_id, seq, stage, skip_reason, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"store {self._path}: cannot enqueue job ({exc})") from None
            row = self._get_job_row(channel_id, message_id)
            assert row is not None
            return self._job_dict(row)

    def get_job(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return the job snapshot (with send evidence + Jev row), or None."""
        if isinstance(channel_id, bool) or not isinstance(channel_id, int) or channel_id <= 0:
            raise StoreError(f"store {self._path}: channel_id must be a positive int")
        if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
            raise StoreError(f"store {self._path}: message_id must be a positive int")
        row = self._get_job_row(channel_id, message_id)
        return None if row is None else self._job_dict(row)

    def list_jobs(self, stages: Any = None) -> list[dict[str, Any]]:
        """Job snapshots ordered by seq (FIFO); optionally filtered by stage."""
        if stages is None:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY seq ASC;").fetchall()
        else:
            wanted = set(stages)
            unknown = wanted - JOB_STAGES
            if unknown:
                raise StoreError(f"store {self._path}: invalid stages {sorted(unknown)!r}")
            placeholders = ",".join("?" for _ in wanted)
            rows = self._conn.execute(
                f"SELECT * FROM jobs WHERE stage IN ({placeholders}) ORDER BY seq ASC;",
                tuple(sorted(wanted)),
            ).fetchall()
        return [self._job_dict(r) for r in rows]

    def first_unresolved(self) -> dict[str, Any] | None:
        """Oldest non-terminal job by seq, or None when the queue is drained."""
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE stage NOT IN ('done','skipped','failed')"
            " ORDER BY seq ASC LIMIT 1;"
        ).fetchone()
        return None if row is None else self._job_dict(row)

    def processed_prefix(self, entries: list[dict[str, Any]], acked: int) -> int:
        """Highest seq whose outbox prefix is fully terminal (done/skipped/failed).

        Walks the caller's entries ascending by seq and stops at the first
        entry with a missing job row or a non-terminal stage. Gaps in seq
        are allowed (a terminal entry still advances); unresolved entries
        block everything after them.
        """
        if isinstance(acked, bool) or not isinstance(acked, int) or acked < 0:
            raise StoreError(f"store {self._path}: acked must be a non-negative int")
        rows = self._conn.execute("SELECT seq, stage FROM jobs WHERE seq > ?;", (acked,)).fetchall()
        stages = {r["seq"]: r["stage"] for r in rows}
        safe = acked
        for entry in sorted(entries, key=lambda e: e.get("seq", 0)):
            seq = entry.get("seq")
            if not _is_strict_int(seq) or seq <= acked:
                continue
            if stages.get(seq) in JOB_TERMINAL_STAGES:
                safe = seq
            else:
                break
        return safe

    def mark_submitted(
        self,
        channel_id: int,
        message_id: int,
        *,
        job_ref: str,
        native_sid: str,
        pre_turn_index: int | None,
    ) -> dict[str, Any]:
        """Move ``queued`` -> ``submitted`` BEFORE the Thyca POST (one txn).

        ``pre_turn_index=None`` means a KNOWN empty native session, never an
        unknown/faked index. Only the exact fingerprint-free submit marker:
        no text hash, no turn binding is stored.
        """
        if not isinstance(job_ref, str) or not job_ref:
            raise StoreError(f"store {self._path}: job_ref must be a non-empty string")
        _check_sid(native_sid, self._path)
        if pre_turn_index is not None and (
            isinstance(pre_turn_index, bool)
            or not isinstance(pre_turn_index, int)
            or pre_turn_index < 0
        ):
            raise StoreError(f"store {self._path}: pre_turn_index must be None or seq >= 0")
        return self._transition_job(
            channel_id,
            message_id,
            allowed={"queued"},
            stage_error=lambda stage: f"store {self._path}: cannot submit job in stage {stage!r}",
            statements=[
                (
                    "UPDATE jobs SET stage='submitted', job_ref=?, native_sid=?,"
                    " pre_turn_index=?, updated_at=? WHERE channel_id=? AND message_id=?"
                    " AND stage='queued';",
                    (job_ref, native_sid, pre_turn_index, _now_iso(), channel_id, message_id),
                )
            ],
            race_error=f"store {self._path}: submit lost a race",
        )

    def requeue(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Move ``submitted`` -> ``queued`` after a definitive native 409.

        Only with zero send evidence (a 409 with SDK activity is ambiguous
        and must fail instead). Clears the ref and pre-turn index; the next
        attempt mints a fresh nonce. An already-``queued`` job fails
        explicitly (never a silent reset).
        """

        def _no_sends(row: sqlite3.Row) -> None:
            if self._sends_for(channel_id, message_id):
                raise StoreError(f"store {self._path}: cannot requeue job with sends")

        return self._transition_job(
            channel_id,
            message_id,
            allowed={"submitted"},
            stage_error=lambda stage: f"store {self._path}: cannot requeue job in stage {stage!r}",
            statements=[
                (
                    "UPDATE jobs SET stage='queued', job_ref=NULL, pre_turn_index=NULL,"
                    " updated_at=? WHERE channel_id=? AND message_id=? AND stage='submitted';",
                    (_now_iso(), channel_id, message_id),
                )
            ],
            extra_guard=_no_sends,
        )

    def record_send(
        self, channel_id: int, message_id: int, *, job_ref: str, sent_message_id: int
    ) -> dict[str, Any]:
        """Record one correlated Discord send (conditional, one txn).

        The UPDATE is guarded on ``job_ref`` + stage ``submitted``/``sent``:
        a send for a failed/skipped/done job (or a stale nonce) records
        nothing and raises instead of duplicating. Multi-chunk replies call
        this once per chunk; the ``sends`` PK makes a repeated receipt of
        the same message idempotent.
        """
        if not isinstance(job_ref, str) or not job_ref:
            raise StoreError(f"store {self._path}: job_ref must be a non-empty string")
        if (
            isinstance(sent_message_id, bool)
            or not isinstance(sent_message_id, int)
            or sent_message_id <= 0
        ):
            raise StoreError(f"store {self._path}: sent_message_id must be a positive int")

        def _ref_match(row: sqlite3.Row) -> None:
            if row["job_ref"] != job_ref:
                raise StoreError(f"store {self._path}: send not eligible (stage {row['stage']!r})")

        return self._transition_job(
            channel_id,
            message_id,
            allowed={"submitted", "sent"},
            stage_error=lambda stage: f"store {self._path}: send not eligible (stage {stage!r})",
            statements=[
                (
                    "UPDATE jobs SET stage='sent', updated_at=? WHERE channel_id=?"
                    " AND message_id=? AND job_ref=? AND stage IN ('submitted','sent');",
                    (_now_iso(), channel_id, message_id, job_ref),
                ),
                (
                    "INSERT OR IGNORE INTO sends (channel_id, message_id, sent_message_id,"
                    " created_at) VALUES (?, ?, ?, ?);",
                    (channel_id, message_id, sent_message_id, _now_iso()),
                ),
            ],
            extra_guard=_ref_match,
        )

    def skip_job(self, channel_id: int, message_id: int, *, reason: str) -> dict[str, Any]:
        """Move to ``skipped`` with zero send evidence (one conditional txn).

        Refuses any job with recorded sends (a send landed first -- never
        force the skip) and any terminal stage. An already-``skipped`` job
        with the same reason is idempotent.
        """
        if reason not in SKIP_REASONS:
            raise StoreError(f"store {self._path}: invalid skip_reason {reason!r}")

        def _no_sends(row: sqlite3.Row) -> None:
            if self._sends_for(channel_id, message_id):
                raise StoreError(f"store {self._path}: cannot skip job with sends")

        return self._transition_job(
            channel_id,
            message_id,
            allowed={"queued", "submitted", "sent"},
            stage_error=lambda stage: f"store {self._path}: cannot skip job in stage {stage!r}",
            statements=[
                (
                    "UPDATE jobs SET stage='skipped', skip_reason=?, updated_at=?"
                    " WHERE channel_id=? AND message_id=?"
                    " AND stage IN ('queued','submitted','sent')"
                    " AND NOT EXISTS (SELECT 1 FROM sends s WHERE s.channel_id=jobs.channel_id"
                    " AND s.message_id=jobs.message_id);",
                    (reason, _now_iso(), channel_id, message_id),
                )
            ],
            extra_guard=_no_sends,
            # The original re-read ``updated["stage"] != "skipped"`` check is
            # expressed as a rowcount check: the conditional UPDATE above is
            # the sole in-txn writer, so ``rowcount != 1`` holds exactly when
            # the re-read would not show ``skipped``. Same message.
            race_error=f"store {self._path}: skip lost a race",
            idempotent=lambda row: row["stage"] == "skipped" and row["skip_reason"] == reason,
        )

    def mark_failed(
        self,
        channel_id: int,
        message_id: int,
        *,
        kind: str,
        http_status: int | None = None,
    ) -> dict[str, Any]:
        """Move to ``failed`` with a coarse kind (ambiguous => failed, never retry).

        Never blindly re-POSTs or re-sends after an ambiguous failure: the
        timeout/ambiguous job is failed, logged, and moved on from. Already
        ``failed`` is idempotent (same payload returns as-is); terminal
        ``done``/``skipped`` never rewind.
        """
        if kind not in FAIL_KINDS:
            raise StoreError(f"store {self._path}: invalid fail_kind {kind!r}")
        if http_status is not None and (
            isinstance(http_status, bool) or not isinstance(http_status, int)
        ):
            raise StoreError(f"store {self._path}: http_status must be an int or None")
        return self._transition_job(
            channel_id,
            message_id,
            allowed={"queued", "submitted", "sent"},
            stage_error=lambda stage: f"store {self._path}: cannot fail job in stage {stage!r}",
            statements=[
                (
                    "UPDATE jobs SET stage='failed', fail_kind=?, http_status=?,"
                    " updated_at=? WHERE channel_id=? AND message_id=?"
                    " AND stage IN ('queued','submitted','sent');",
                    (kind, http_status, _now_iso(), channel_id, message_id),
                )
            ],
            # As in skip_job: the original re-read ``!= "failed"`` check is
            # exactly the conditional UPDATE affecting zero rows. Same message.
            race_error=f"store {self._path}: fail lost a race",
            idempotent=lambda row: row["stage"] == "failed",
        )

    def complete_job(self, channel_id: int, message_id: int, *, outcome: str) -> dict[str, Any]:
        """Close a job as ``done`` with a terminal outcome (stage-guarded txn).

        - ``sent``: from ``submitted``/``sent`` with >= 1 recorded send.
        - ``no_reply``: from ``submitted``/``sent`` with zero sends (the
          record_no_reply terminal, applied at tool time).
        - ``observed``: from ``queued`` (Jev observe never passes through
          ``submitted``; crashed-pending observe resolves the same way).
        Already-``done`` with the same outcome is idempotent.
        """
        if outcome not in JOB_OUTCOMES:
            raise StoreError(f"store {self._path}: invalid outcome {outcome!r}")

        def _deny(stage: object) -> str:
            return (
                f"store {self._path}: cannot complete job in stage"
                f" {stage!r} with outcome {outcome!r}"
            )

        def _eligible(row: sqlite3.Row) -> None:
            sends = self._sends_for(channel_id, message_id)
            if outcome == "sent":
                ok = row["stage"] in ("submitted", "sent") and len(sends) >= 1
            elif outcome == "no_reply":
                ok = row["stage"] in ("submitted", "sent") and len(sends) == 0
            else:  # observed
                ok = row["stage"] == "queued"
            if not ok:
                raise StoreError(_deny(row["stage"]))

        return self._transition_job(
            channel_id,
            message_id,
            allowed={"queued", "submitted", "sent"},
            stage_error=_deny,
            statements=[
                (
                    "UPDATE jobs SET stage='done', outcome=?, updated_at=?"
                    " WHERE channel_id=? AND message_id=?;",
                    (outcome, _now_iso(), channel_id, message_id),
                )
            ],
            extra_guard=_eligible,
            idempotent=lambda row: row["stage"] == "done" and row["outcome"] == outcome,
        )

    def sweep_submitted_failed(self) -> int:
        """Restart recovery: fail jobs left ``submitted`` (never resume/re-POST).

        ``queued`` jobs are untouched and resume normally. Explicit: runs
        once at responder startup, never inside ``Store.open`` (opens
        happen per operation; sweeping on open would fail live jobs).
        """
        with self._txn():
            cur = self._conn.execute(
                "UPDATE jobs SET stage='failed', fail_kind='restart', http_status=NULL,"
                " updated_at=? WHERE stage='submitted';",
                (_now_iso(),),
            )
            count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            return count

    # -- sessions / native SID --

    def get_native_sid(self) -> str | None:
        """Current serving native SID, or None before the first adoption."""
        row = self._conn.execute("SELECT native_sid FROM state WHERE id=1;").fetchone()
        if row is None:
            raise StoreError(f"corrupt database {self._path}: missing singleton state row")
        sid = row["native_sid"]
        if sid is None:
            return None
        if not isinstance(sid, str) or _SID_RE.fullmatch(sid) is None:
            raise StoreError(f"corrupt database {self._path}: bad native_sid")
        return sid

    def adopt_session(self, *, expected_current: str | None, new_sid: str, day: str) -> str:
        """Adopt the target bot DM session in ONE transaction.

        Requires the stored SID to equal ``expected_current`` (``None`` only
        when no SID was ever adopted), refuses while any job is mid-flight
        (``submitted``/``sent`` -- the caller drains first and retries next
        tick), then swaps the current SID and upserts the day provenance.
        Old sessions are never deleted: the ``sessions`` history keeps every
        adopted day.
        """
        if expected_current is not None:
            _check_sid(expected_current, self._path)
        _check_sid(new_sid, self._path)
        _check_day(day, self._path)
        with self._txn():
            row = self._conn.execute("SELECT native_sid FROM state WHERE id=1;").fetchone()
            if row is None:
                raise StoreError(f"corrupt database {self._path}: missing singleton state row")
            current = row["native_sid"]
            if current != expected_current:
                raise StoreError(f"store {self._path}: native_session_id mismatch")
            blocked = self._conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE stage IN ('submitted','sent');"
            ).fetchone()[0]
            if blocked:
                raise StoreError(f"store {self._path}: adoption blocked by mid-flight jobs")
            now = _now_iso()
            self._conn.execute("UPDATE state SET native_sid=? WHERE id=1;", (new_sid,))
            self._conn.execute(
                "INSERT INTO sessions (day, native_sid, adopted_at) VALUES (?, ?, ?)"
                " ON CONFLICT(day) DO UPDATE SET native_sid=excluded.native_sid,"
                " adopted_at=excluded.adopted_at;",
                (day, new_sid, now),
            )
            return new_sid

    # -- Jev decisions --

    def get_jev(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Detached Jev record for a job, or None when never classified."""
        if isinstance(channel_id, bool) or not isinstance(channel_id, int) or channel_id <= 0:
            raise StoreError(f"store {self._path}: channel_id must be a positive int")
        if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
            raise StoreError(f"store {self._path}: message_id must be a positive int")
        row = self._conn.execute(
            "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
            (channel_id, message_id),
        ).fetchone()
        return None if row is None else _row_to_jev(row)

    def insert_jev_pending(
        self,
        channel_id: int,
        message_id: int,
        *,
        context_sha256: str,
        attempt_id: str,
        author_uid: int,
        attempted_day: str,
    ) -> dict[str, Any]:
        """Insert the ``pending`` marker BEFORE the paid call (one txn).

        The marker is the spend protocol: a crash between the insert and
        the resolve leaves a ``pending`` row whose outcome -- and whether
        money was spent -- is unknown, handled by the one-retry rule.
        """
        _require_id("channel_id", channel_id, self._path)
        _require_id("message_id", message_id, self._path)
        _require_id("author_uid", author_uid, self._path)
        if not isinstance(context_sha256, str) or len(context_sha256) != 64:
            raise StoreError(f"store {self._path}: invalid context_sha256")
        if not isinstance(attempt_id, str) or not attempt_id:
            raise StoreError(f"store {self._path}: invalid attempt_id")
        _check_day(attempted_day, self._path)
        now = _now_iso()
        with self._txn():
            try:
                self._conn.execute(
                    "INSERT INTO jev_decisions (channel_id, message_id, context_sha256,"
                    " status, attempts, attempt_id, author_uid, attempted_day,"
                    " created_at, updated_at)"
                    " VALUES (?, ?, ?, 'pending', 1, ?, ?, ?, ?, ?);",
                    (
                        channel_id,
                        message_id,
                        context_sha256,
                        attempt_id,
                        author_uid,
                        attempted_day,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"store {self._path}: cannot insert jev pending ({exc})") from None
            row = self._conn.execute(
                "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
                (channel_id, message_id),
            ).fetchone()
            assert row is not None
            return _row_to_jev(row)

    def bump_jev_retry(
        self,
        channel_id: int,
        message_id: int,
        *,
        old_attempt_id: str,
        new_attempt_id: str,
        attempted_day: str,
    ) -> dict[str, Any]:
        """Arm the single retry: ``pending`` attempts 1 -> 2 (conditional txn).

        The conditional ``UPDATE ... WHERE status='pending' AND
        attempt_id=?`` plus the rowcount check is the whole guard: a row
        that already resolved (or a stale attempt id) fails instead of
        paying twice.
        """
        if not isinstance(new_attempt_id, str) or not new_attempt_id:
            raise StoreError(f"store {self._path}: invalid attempt_id")
        _check_day(attempted_day, self._path)
        with self._txn():
            self._require_job_row(channel_id, message_id)
            cur = self._conn.execute(
                "UPDATE jev_decisions SET attempts=2, attempt_id=?, attempted_day=?,"
                " updated_at=? WHERE channel_id=? AND message_id=? AND status='pending'"
                " AND attempt_id=? AND attempts < 2;",
                (
                    new_attempt_id,
                    attempted_day,
                    _now_iso(),
                    channel_id,
                    message_id,
                    old_attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise StoreError(f"store {self._path}: jev retry not eligible")
            row = self._conn.execute(
                "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
                (channel_id, message_id),
            ).fetchone()
            assert row is not None
            return _row_to_jev(row)

    def resolve_jev(
        self,
        channel_id: int,
        message_id: int,
        *,
        attempt_id: str,
        status: str,
        participation: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        answers_json: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a ``pending`` row after the paid call (conditional txn).

        ``decided`` requires the routing participation plus token usage;
        ``failed_known`` requires usage (zero-cost known failures pass
        explicit zeros); ``failed_unknown`` carries optional usage (None =
        pure unknown). Anything else -- including a stale attempt id --
        raises instead of resolving.
        """
        if status not in ("decided", "failed_known", "failed_unknown"):
            raise StoreError(f"store {self._path}: invalid jev status {status!r}")
        if status == "decided":
            if participation not in JEV_PARTICIPATIONS:
                raise StoreError(f"store {self._path}: decided jev needs participation")
            for name, value in (("input_tokens", input_tokens), ("output_tokens", output_tokens)):
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise StoreError(f"store {self._path}: decided jev needs {name}")
        elif status == "failed_known":
            for name, value in (("input_tokens", input_tokens), ("output_tokens", output_tokens)):
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise StoreError(f"store {self._path}: failed_known jev needs {name}")
            participation = None
        else:  # failed_unknown
            for name, value in (("input_tokens", input_tokens), ("output_tokens", output_tokens)):
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                ):
                    raise StoreError(f"store {self._path}: invalid {name}")
            participation = None
        if answers_json is not None and not isinstance(answers_json, str):
            raise StoreError(f"store {self._path}: answers_json must be a string or None")
        with self._txn():
            self._require_job_row(channel_id, message_id)
            cur = self._conn.execute(
                "UPDATE jev_decisions SET status=?, participation=?, input_tokens=?,"
                " output_tokens=?, answers_json=?, updated_at=?"
                " WHERE channel_id=? AND message_id=? AND status='pending' AND attempt_id=?;",
                (
                    status,
                    participation,
                    input_tokens,
                    output_tokens,
                    answers_json,
                    _now_iso(),
                    channel_id,
                    message_id,
                    attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise StoreError(f"store {self._path}: jev resolve not eligible")
            row = self._conn.execute(
                "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
                (channel_id, message_id),
            ).fetchone()
            assert row is not None
            return _row_to_jev(row)

    def mark_jev_observe(
        self,
        channel_id: int,
        message_id: int,
        *,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
    ) -> dict[str, Any]:
        """Terminally resolve an unknown-attempts-2 ``pending`` as ``observe``.

        Owner-ordered synthetic observe: stored as ``decided`` /
        ``observe`` with ``answers_json`` NULL, keeping the retry attempt's
        usage/cost when known (lower-bound accounting survives the unknown
        outcome). One transaction updates the Jev row and closes a
        ``queued`` job as ``done`` / ``observed``: never a third paid call
        for the same ``(channel_id, message_id)``. Already-observed is
        idempotent.
        """
        with self._txn():
            row = self._require_job_row(channel_id, message_id)
            jev_row = self._conn.execute(
                "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
                (channel_id, message_id),
            ).fetchone()
            if jev_row is None:
                raise StoreError(f"store {self._path}: no jev row to observe")
            if (
                jev_row["status"] == "decided"
                and jev_row["participation"] == "observe"
                and row["stage"] == "done"
                and row["outcome"] == "observed"
            ):
                # Idempotent early return: the single exit COMMIT covers it,
                # so no mid-body COMMIT is needed.
                return _row_to_jev(jev_row)
            for name, value in (("input_tokens", input_tokens), ("output_tokens", output_tokens)):
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                ):
                    raise StoreError(f"store {self._path}: invalid {name}")
            cur = self._conn.execute(
                "UPDATE jev_decisions SET status='decided', participation='observe',"
                " input_tokens=?, output_tokens=?, updated_at=?"
                " WHERE channel_id=? AND message_id=? AND status='pending';",
                (input_tokens, output_tokens, _now_iso(), channel_id, message_id),
            )
            if cur.rowcount != 1:
                raise StoreError(f"store {self._path}: jev observe not eligible")
            if row["stage"] == "queued":
                self._conn.execute(
                    "UPDATE jobs SET stage='done', outcome='observed', updated_at=?"
                    " WHERE channel_id=? AND message_id=? AND stage='queued';",
                    (_now_iso(), channel_id, message_id),
                )
            elif not (row["stage"] == "done" and row["outcome"] == "observed"):
                raise StoreError(
                    f"store {self._path}: cannot observe job in stage {row['stage']!r}"
                )
            updated = self._conn.execute(
                "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;",
                (channel_id, message_id),
            ).fetchone()
            assert updated is not None
            return _row_to_jev(updated)

    def jev_cost_for_day(self, day: str) -> str:
        """Canonical cost string for one billing day (decided + known + lower-bound)."""
        _check_day(day, self._path)
        rows = self._conn.execute(
            "SELECT input_tokens FROM jev_decisions WHERE attempted_day=?"
            " AND status IN ('decided','failed_known','failed_unknown')"
            " AND input_tokens IS NOT NULL;",
            (day,),
        ).fetchall()
        total = sum((r["input_tokens"] for r in rows), Decimal("0"))
        # Fixed $0.042/1M input tokens; canonical plain Decimal, "0" for zero.
        cost = total * Decimal("0.000000042")
        if cost == 0:
            return "0"
        return format(cost.normalize(), "f")

    def jev_unknown_blocked(self) -> bool:
        """True when a ``pending``/``failed_unknown`` row blocks new paid calls.

        Any orphan/active ``pending`` means billing is unknown too, so new
        sources halt while cached ``decided`` rows still return. The
        caller's own ``pending`` is handled by the one-retry rule, not by
        an exclusion here: the fresh-key scan covers every row.
        """
        row = self._conn.execute(
            "SELECT COUNT(*) FROM jev_decisions WHERE status IN ('pending','failed_unknown');"
        ).fetchone()
        return bool(row[0])


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
