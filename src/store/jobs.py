from __future__ import annotations

import sqlite3
from collections.abc import Callable, Collection, Sequence
from typing import Any

from validate import is_strict_int as _is_strict_int

from . import queries as sql
from .base import (
    FAIL_KINDS,
    JOB_OUTCOMES,
    JOB_STAGES,
    JOB_TERMINAL_STAGES,
    SKIP_REASONS,
    _check_sid,
    _now_iso,
    _require_id,
    _row_to_jev,
)
from .errors import StoreError


class JobsMixin:
    """Job lifecycle + send evidence (v2)."""

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
            for stmt, params in statements:
                cur = self._conn.execute(stmt, params)
            if race_error is not None and (cur is None or cur.rowcount != 1):
                raise StoreError(race_error)
            updated = self._get_job_row(channel_id, message_id)
            assert updated is not None
            return self._job_dict(updated)

    def _get_job_row(self, channel_id: int, message_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            sql.JOB_GET,
            (channel_id, message_id),
        ).fetchone()

    def _sends_for(self, channel_id: int, message_id: int) -> list[int]:
        rows = self._conn.execute(
            sql.SENDS_FOR,
            (channel_id, message_id),
        ).fetchall()
        return [r["sent_message_id"] for r in rows]

    def _job_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        sent_ids = self._sends_for(row["channel_id"], row["message_id"])
        jev_row = self._conn.execute(
            sql.JEV_GET,
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
                    sql.JOB_INSERT,
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
            rows = self._conn.execute(sql.JOB_LIST_ALL).fetchall()
        else:
            wanted = set(stages)
            unknown = wanted - JOB_STAGES
            if unknown:
                raise StoreError(f"store {self._path}: invalid stages {sorted(unknown)!r}")
            placeholders = ",".join("?" for _ in wanted)
            rows = self._conn.execute(
                sql.jobs_in_stages(placeholders),
                tuple(sorted(wanted)),
            ).fetchall()
        return [self._job_dict(r) for r in rows]

    def first_unresolved(self) -> dict[str, Any] | None:
        """Oldest non-terminal job by seq, or None when the queue is drained."""
        row = self._conn.execute(sql.JOB_FIRST_UNRESOLVED).fetchone()
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
        rows = self._conn.execute(sql.JOB_SEQ_STAGES_AFTER, (acked,)).fetchall()
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
                    sql.JOB_MARK_SUBMITTED,
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
                    sql.JOB_REQUEUE,
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
                    sql.JOB_RECORD_SEND,
                    (_now_iso(), channel_id, message_id, job_ref),
                ),
                (
                    sql.SEND_INSERT,
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
                    sql.JOB_SKIP,
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
                    sql.JOB_FAIL,
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
                    sql.JOB_COMPLETE,
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
                sql.JOB_SWEEP_SUBMITTED,
                (_now_iso(),),
            )
            count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            return count
