"""Jev participation decisions over the single SQLite store.

One ``jev_decisions`` row per ``(channel_id, message_id)`` (PK, FK to the
job row). The row carries the spend protocol:

- ``pending`` is inserted BEFORE the paid call; the resolve afterwards is
  a conditional ``UPDATE ... WHERE status='pending' AND attempt_id=?``
  with a rowcount check, so a stale attempt never resolves twice.
- A crashed ``pending`` with ``attempts < 2`` gets exactly ONE retry
  (attempts bumped to 2 with a fresh attempt id, one more paid call). An
  unknown outcome at ``attempts == 2`` -- including a crash between the
  second call and its resolve -- resolves terminally as ``observe``.
  Never a third paid call for the same ``(channel_id, message_id)``.
- A ``decided`` row is reused from cache (no re-pay), even while another
  key's ``failed_unknown``/``pending`` globally blocks new paid calls.

Cost math and the billing-day rule live here (the paid-boundary logic the
plan keeps): estimated cost is input-tokens only at exactly ``$0.042/1M``;
per-day cost is reported for observability only -- there is no spend stop.
Jev SDK response validation stays in :mod:`jev` (untrusted paid
boundary); this module only moves validated values into the store.

No ledger files, no activation markers, no binding dance: a single DB
deletes that failure class by construction. Durability-uncertain handling
dies with the two-file journal (a sqlite commit error is an ordinary
:exc:`StoreError`, never a special hold).
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import uuid
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from conversation import ConversationError, project_conversation_state
from jev import (
    JEV_MODEL,
    JevError,
    build_questions,
    validate_answers,
    validate_model_id,
    validate_usage,
)
from store import Store, StoreError
from thyca import validate_day
from validate import is_strict_positive_int as _is_strict_positive_int

from .errors import ParticipationError, ParticipationPending, ParticipationReason

INPUT_USD_PER_TOKEN = Decimal("0.000000042")

_VALID_PARTICIPATIONS = frozenset({"respond", "observe", "need_context"})


def _default_day_provider() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _fingerprint_snapshot(snapshot: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _extract_valid_usage(obj: Any) -> dict[str, int] | None:
    """Extract valid usage FIRST, before model/answers/participation.

    Robust to malformed or raising-getter objects: any unreadable or
    invalid usage returns None (never raises). Valid usage is always
    charged on later shape failure, never replaced with unknown-only.
    """
    try:
        raw = getattr(obj, "usage", None)
    except Exception:
        return None
    try:
        detached = copy.deepcopy(raw)
    except Exception:
        detached = raw
    try:
        return validate_usage(detached)
    except Exception:
        return None


def _extract_unknown_halt(exc: JevError) -> bool:
    """True when billing halts unknown; False only on strict ``False``.

    Fail-closed: unreadable or non-``False`` flags halt. Never raises.
    """
    try:
        flag = getattr(exc, "unknown_billing", True)
    except Exception:
        return True
    return False if flag is False else True


class ParticipationLedger:
    """Serial durable Jev participation ledger for one native session (now DB-backed)."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        day_provider: Callable[[], str] | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        if day_provider is None:
            provider: Callable[[], str] = _default_day_provider
        elif callable(day_provider):
            provider = day_provider
        else:
            raise ParticipationError(f"invalid day_provider for {self._db_path}")
        self._day_provider = provider
        self._lock = asyncio.Lock()
        self._in_progress: set[tuple[int, int]] = set()

    def _today(self) -> str:
        try:
            today = self._day_provider()
        except Exception:
            raise ParticipationError(f"invalid day_provider for {self._db_path}") from None
        if validate_day(today) is None:
            raise ParticipationError(f"invalid day for {self._db_path}")
        return today

    def get(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return a detached Jev record for the job, or None."""
        if not _is_strict_positive_int(channel_id):
            raise ParticipationError(f"invalid channel_id for {self._db_path}")
        if not _is_strict_positive_int(message_id):
            raise ParticipationError(f"invalid message_id for {self._db_path}")
        with Store.open(self._db_path) as store:
            return store.get_jev(channel_id, message_id)

    def summary(self) -> dict[str, Any]:
        """Per-day cost/block flags for today (observability only, no spend stop)."""
        today = self._today()
        with Store.open(self._db_path) as store:
            cost = store.jev_cost_for_day(today)
            blocked = store.jev_unknown_blocked()
        return {
            "today": today,
            "known_cost_today_usd": cost,
            "unknown_blocked": blocked,
        }

    async def classify(
        self,
        adapter: Any,
        state: Mapping[str, Any],
        *,
        channel_id: int,
        message_id: int,
        author_uid: int,
    ) -> dict[str, Any]:
        """Classify one source once; cache ``decided`` only on exact binding.

        Returns a detached ``decided`` record. Raises bounded
        :class:`ParticipationPending` for duplicates, failures, mismatches,
        or unknown-billing blocks. A crashed ``pending`` gets exactly one
        retry; a still-unknown outcome at ``attempts == 2`` resolves
        terminally as ``observe``. Cancellation persists the unknown hold
        (``failed_unknown`` on the first attempt, ``observe`` on the retry)
        then propagates.
        """
        if not _is_strict_positive_int(channel_id):
            raise ParticipationError(f"invalid channel_id for {self._db_path}")
        if not _is_strict_positive_int(message_id):
            raise ParticipationError(f"invalid message_id for {self._db_path}")
        if not _is_strict_positive_int(author_uid):
            raise ParticipationError(f"invalid author_uid for {self._db_path}")
        decide = getattr(adapter, "decide", None)
        if not callable(decide):
            raise ParticipationError(f"invalid adapter for {self._db_path}")
        try:
            snapshot = project_conversation_state(state)
        except ConversationError:
            raise ParticipationError("invalid conversation state") from None
        except Exception:
            raise ParticipationError("invalid conversation state") from None
        # Exact ledger binding BEFORE any provider/write: snapshot must
        # represent this source. No question-diagnostic ownership, no raw
        # body in the bounded reason. Fail closed on missing current.
        if snapshot.get("new_message_id") != str(message_id):
            raise ParticipationPending(ParticipationReason.CONTEXT_MISMATCH)
        current_sender: Any = None
        for msg in snapshot.get("messages", []):
            if isinstance(msg, dict) and msg.get("id") == snapshot["new_message_id"]:
                current_sender = msg.get("sender_uid")
                break
        if current_sender != str(author_uid):
            raise ParticipationPending(ParticipationReason.CONTEXT_MISMATCH)
        fingerprint = _fingerprint_snapshot(snapshot)
        key = (channel_id, message_id)
        async with self._lock:
            if key in self._in_progress:
                raise ParticipationPending(ParticipationReason.IN_PROGRESS)
            self._in_progress.add(key)
        try:
            return await self._classify_guarded(
                decide, snapshot, fingerprint, channel_id, message_id, author_uid
            )
        finally:
            self._in_progress.discard(key)

    async def _classify_guarded(
        self,
        decide: Any,
        snapshot: dict[str, Any],
        fingerprint: str,
        channel_id: int,
        message_id: int,
        author_uid: int,
    ) -> dict[str, Any]:
        try:
            with Store.open(self._db_path) as store:
                existing = store.get_jev(channel_id, message_id)
        except StoreError as exc:
            raise ParticipationError(str(exc)) from None
        if existing is not None:
            status = existing["status"]
            # Crashed second attempt: resolve terminally as observe
            # REGARDLESS of fingerprint/author drift (no third paid call,
            # no re-payment on drift -- the attempt already spent).
            if status == "pending" and existing["attempts"] >= 2:
                try:
                    with Store.open(self._db_path) as store:
                        return store.mark_jev_observe(channel_id, message_id)
                except StoreError as exc:
                    raise ParticipationError(str(exc)) from None
            if existing["author_uid"] != author_uid or existing["context_sha256"] != fingerprint:
                # Mismatch never re-pays: defer with no payment and no
                # resolution (the paid retry below never runs on drift).
                raise ParticipationPending(ParticipationReason.CONTEXT_MISMATCH)
            if status == "decided":
                return copy.deepcopy(existing)
            if status == "failed_known":
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN)
            if status == "failed_unknown":
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN)
            # Crashed first attempt: exactly one retry (fresh attempt id +
            # current billing day in ONE conditional UPDATE committed BEFORE
            # the retry paid call). Retry fires only here, at the FIFO head.
            today = self._today()
            new_attempt_id = uuid.uuid4().hex
            try:
                with Store.open(self._db_path) as store:
                    store.bump_jev_retry(
                        channel_id,
                        message_id,
                        old_attempt_id=existing["attempt_id"],
                        new_attempt_id=new_attempt_id,
                        attempted_day=today,
                    )
            except StoreError as exc:
                raise ParticipationError(str(exc)) from None
            return await self._paid_call(
                decide, snapshot, channel_id, message_id, new_attempt_id, retry=True
            )
        # Fresh source: another key's unknown billing blocks new paid calls;
        # cached decided for the requested key already returned above.
        today = self._today()
        attempt_id = uuid.uuid4().hex
        try:
            with Store.open(self._db_path) as store:
                if store.jev_unknown_blocked():
                    raise ParticipationPending(ParticipationReason.UNKNOWN_BLOCKED)
                store.insert_jev_pending(
                    channel_id,
                    message_id,
                    context_sha256=fingerprint,
                    attempt_id=attempt_id,
                    author_uid=author_uid,
                    attempted_day=today,
                )
        except ParticipationPending:
            raise
        except StoreError as exc:
            raise ParticipationError(str(exc)) from None
        return await self._paid_call(
            decide, snapshot, channel_id, message_id, attempt_id, retry=False
        )

    async def _paid_call(
        self,
        decide: Any,
        snapshot: dict[str, Any],
        channel_id: int,
        message_id: int,
        attempt_id: str,
        *,
        retry: bool,
    ) -> dict[str, Any]:
        """Run the provider call and resolve the pending row (never twice).

        ``retry=True`` means this is the single retry of a crashed pending:
        a still-unknown outcome resolves terminally as ``observe`` instead
        of ``failed_unknown``.
        """
        provider_snapshot = copy.deepcopy(snapshot)
        try:
            decision = await decide(provider_snapshot)
        except asyncio.CancelledError:
            # Persist the unknown hold, then propagate the cancellation
            # itself (never swallow it as a Pending).
            self._persist_unknown(channel_id, message_id, attempt_id, None, retry=retry)
            raise
        except JevError as exc:
            known_usage = _extract_valid_usage(exc)
            unknown_halt = _extract_unknown_halt(exc)
            if known_usage is not None:
                if unknown_halt:
                    return self._resolve_unknown(
                        channel_id, message_id, attempt_id, known_usage, retry=retry
                    )
                self._resolve_known(channel_id, message_id, attempt_id, known_usage)
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
            if not unknown_halt:
                zero = {"input_tokens": 0, "output_tokens": 0}
                self._resolve_known(channel_id, message_id, attempt_id, zero)
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
            return self._resolve_unknown(channel_id, message_id, attempt_id, None, retry=retry)
        except ConversationError:
            zero = {"input_tokens": 0, "output_tokens": 0}
            self._resolve_known(channel_id, message_id, attempt_id, zero)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
        except Exception:
            return self._resolve_unknown(channel_id, message_id, attempt_id, None, retry=retry)
        usage = _extract_valid_usage(decision)
        if usage is None:
            return self._resolve_unknown(channel_id, message_id, attempt_id, None, retry=retry)
        try:
            try:
                raw_model = getattr(decision, "model", None)
            except Exception:
                raise ValueError("bad decision") from None
            model = validate_model_id(raw_model)
            questions = build_questions(snapshot)
            try:
                raw_answers = getattr(decision, "answers", None)
            except Exception:
                raise ValueError("bad decision") from None
            try:
                detached_answers = copy.deepcopy(raw_answers)
            except Exception:
                detached_answers = raw_answers
            answers = validate_answers(questions, detached_answers)
            participation = answers["participation"]["choice"]
            if participation not in _VALID_PARTICIPATIONS or model != JEV_MODEL:
                raise ValueError("bad decision")
        except Exception:
            self._resolve_known(channel_id, message_id, attempt_id, usage)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
        try:
            with Store.open(self._db_path) as store:
                return store.resolve_jev(
                    channel_id,
                    message_id,
                    attempt_id=attempt_id,
                    status="decided",
                    participation=participation,
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    answers_json=json.dumps(answers, ensure_ascii=False, sort_keys=True),
                )
        except StoreError as exc:
            raise ParticipationError(str(exc)) from None

    def _resolve_known(
        self, channel_id: int, message_id: int, attempt_id: str, usage: dict[str, int]
    ) -> None:
        try:
            with Store.open(self._db_path) as store:
                store.resolve_jev(
                    channel_id,
                    message_id,
                    attempt_id=attempt_id,
                    status="failed_known",
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                )
        except StoreError as exc:
            raise ParticipationError(str(exc)) from None

    def _persist_unknown(
        self,
        channel_id: int,
        message_id: int,
        attempt_id: str,
        usage: dict[str, int] | None,
        *,
        retry: bool,
    ) -> None:
        """Write the unknown-billing outcome (retry => terminal observe)."""
        if retry:
            # Second unknown outcome => terminal observe, never a third call.
            try:
                with Store.open(self._db_path) as store:
                    store.mark_jev_observe(channel_id, message_id)
            except StoreError as exc:
                raise ParticipationError(str(exc)) from None
            return
        try:
            with Store.open(self._db_path) as store:
                store.resolve_jev(
                    channel_id,
                    message_id,
                    attempt_id=attempt_id,
                    status="failed_unknown",
                    input_tokens=None if usage is None else usage["input_tokens"],
                    output_tokens=None if usage is None else usage["output_tokens"],
                )
        except StoreError as exc:
            raise ParticipationError(str(exc)) from None

    def _resolve_unknown(
        self,
        channel_id: int,
        message_id: int,
        attempt_id: str,
        usage: dict[str, int] | None,
        *,
        retry: bool,
    ) -> dict[str, Any]:
        """Resolve an unknown-billing outcome; the retry resolves as observe.

        The terminal observe keeps the retry attempt's usage/cost when
        known (lower-bound accounting survives the unknown outcome).
        """
        if retry:
            # Second unknown outcome => terminal observe, never a third call.
            try:
                with Store.open(self._db_path) as store:
                    return store.mark_jev_observe(
                        channel_id,
                        message_id,
                        input_tokens=None if usage is None else usage["input_tokens"],
                        output_tokens=None if usage is None else usage["output_tokens"],
                    )
            except StoreError as exc:
                raise ParticipationError(str(exc)) from None
        self._persist_unknown(channel_id, message_id, attempt_id, usage, retry=False)
        raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN)
