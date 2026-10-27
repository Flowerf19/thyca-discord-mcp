from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any

from . import queries as sql
from .base import (
    JEV_PARTICIPATIONS,
    _check_day,
    _now_iso,
    _require_id,
    _row_to_jev,
)
from .errors import StoreError


class JevMixin:
    """Jev decision rows (spend protocol)."""

    # -- Jev decisions --

    def get_jev(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Detached Jev record for a job, or None when never classified."""
        if isinstance(channel_id, bool) or not isinstance(channel_id, int) or channel_id <= 0:
            raise StoreError(f"store {self._path}: channel_id must be a positive int")
        if isinstance(message_id, bool) or not isinstance(message_id, int) or message_id <= 0:
            raise StoreError(f"store {self._path}: message_id must be a positive int")
        row = self._conn.execute(
            sql.JEV_GET,
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
                    sql.JEV_INSERT_PENDING,
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
                sql.JEV_GET,
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
                sql.JEV_BUMP_RETRY,
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
                sql.JEV_GET,
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
                sql.JEV_RESOLVE,
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
                sql.JEV_GET,
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
                sql.JEV_GET,
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
                sql.JEV_OBSERVE,
                (input_tokens, output_tokens, _now_iso(), channel_id, message_id),
            )
            if cur.rowcount != 1:
                raise StoreError(f"store {self._path}: jev observe not eligible")
            if row["stage"] == "queued":
                self._conn.execute(
                    sql.JOB_CLOSE_OBSERVED,
                    (_now_iso(), channel_id, message_id),
                )
            elif not (row["stage"] == "done" and row["outcome"] == "observed"):
                raise StoreError(
                    f"store {self._path}: cannot observe job in stage {row['stage']!r}"
                )
            updated = self._conn.execute(
                sql.JEV_GET,
                (channel_id, message_id),
            ).fetchone()
            assert updated is not None
            return _row_to_jev(updated)

    def jev_cost_for_day(self, day: str) -> str:
        """Canonical cost string for one billing day (decided + known + lower-bound)."""
        _check_day(day, self._path)
        rows = self._conn.execute(sql.JEV_COST_ROWS, (day,)).fetchall()
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
        row = self._conn.execute(sql.JEV_BLOCKED_COUNT).fetchone()
        return bool(row[0])
