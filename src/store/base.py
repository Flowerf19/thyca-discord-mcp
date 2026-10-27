from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from validate import is_strict_int as _is_strict_int

from . import queries as sql
from .errors import StoreError
from .schema import _SCHEMA_V1_SQL, _SCHEMA_V2_SQL, SCHEMA_VERSION

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


class StoreBase:
    """One SQLite connection; all mutations are short explicit transactions."""

    def __init__(self, conn: sqlite3.Connection, path: Path) -> None:
        self._conn = conn
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def open(cls, db_path: str | Path) -> StoreBase:
        """Open (creating 0600 if needed), pragma, verify, migrate, check."""
        path = Path(db_path)
        created = not path.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(sql.PRAGMA_JOURNAL_WAL)
            conn.execute(sql.PRAGMA_SYNCHRONOUS_FULL)
            conn.execute(sql.PRAGMA_FOREIGN_KEYS)
            conn.execute(sql.PRAGMA_BUSY_TIMEOUT)
            try:
                quick = conn.execute(sql.PRAGMA_QUICK_CHECK).fetchall()
            except sqlite3.Error as exc:
                raise StoreError(f"corrupt database {path}: quick_check failed ({exc})") from exc
            if len(quick) != 1 or quick[0][0] != "ok":
                raise StoreError(f"corrupt database {path}: quick_check failed ({quick!r})")
            version_row = conn.execute(sql.PRAGMA_USER_VERSION).fetchone()
            version = version_row[0] if version_row is not None else 0
            if version == 0:
                # Fresh DB (no tables yet) -> create v2 schema. A non-empty
                # file with version 0 but pre-existing tables is also
                # initialized idempotently; CREATE TABLE IF NOT EXISTS keeps
                # it safe.
                conn.executescript(_SCHEMA_V1_SQL + _SCHEMA_V2_SQL)
                conn.execute(sql.STATE_ADD_SID_COLUMN)
                conn.execute(sql.STATE_SEED)
                conn.execute(sql.set_user_version(SCHEMA_VERSION))
            elif version == 1:
                # v1 -> v2 migration in ONE transaction: new tables plus the
                # nullable native_sid column. v1 outbox rows keep no job
                # rows (deploy reseeds past old history; tests seed fresh).
                # NOTE: executescript() commits implicitly, so the migration
                # statements run individually inside one explicit txn.
                conn.execute(sql.TXN_BEGIN)
                try:
                    for stmt in _SCHEMA_V2_SQL.split(";"):
                        if stmt.strip():
                            conn.execute(stmt + ";")
                    cols = [r[1] for r in conn.execute(sql.PRAGMA_TABLE_INFO_STATE).fetchall()]
                    if "native_sid" not in cols:
                        conn.execute(sql.STATE_ADD_SID_COLUMN)
                    conn.execute(sql.set_user_version(SCHEMA_VERSION))
                    conn.execute(sql.TXN_COMMIT)
                except Exception:
                    try:
                        conn.execute(sql.TXN_ROLLBACK)
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
                conn.execute(sql.STATE_SEED)
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
        tables = {r[0] for r in conn.execute(sql.TABLES_LIST)}
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

    def __enter__(self) -> StoreBase:
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
        self._conn.execute(sql.TXN_BEGIN)
        try:
            yield
            self._conn.execute(sql.TXN_COMMIT)
        except Exception:
            try:
                self._conn.execute(sql.TXN_ROLLBACK)
            except Exception:
                pass
            raise

    def _state(self) -> tuple[int, int]:
        row = self._conn.execute(sql.STATE_GET).fetchone()
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

    # -- sessions / native SID --

    def get_native_sid(self) -> str | None:
        """Current serving native SID, or None before the first adoption."""
        row = self._conn.execute(sql.STATE_GET_SID).fetchone()
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
            row = self._conn.execute(sql.STATE_GET_SID).fetchone()
            if row is None:
                raise StoreError(f"corrupt database {self._path}: missing singleton state row")
            current = row["native_sid"]
            if current != expected_current:
                raise StoreError(f"store {self._path}: native_session_id mismatch")
            blocked = self._conn.execute(sql.JOB_MID_FLIGHT_COUNT).fetchone()[0]
            if blocked:
                raise StoreError(f"store {self._path}: adoption blocked by mid-flight jobs")
            now = _now_iso()
            self._conn.execute(sql.STATE_SET_SID, (new_sid,))
            self._conn.execute(
                sql.SESSION_UPSERT,
                (day, new_sid, now),
            )
            return new_sid
