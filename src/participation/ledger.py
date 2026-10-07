"""Durable metadata-only Jev participation ledger.

Independent ledger for Jev classification attempts, keyed by
``"<channel_id>:<message_id>"``. Stores decision metadata only -- never raw
conversation context, prompts, history, message text, profiles, auth, or
freeform provider errors.

File layout (single JSON file, any path)::

    {
      "version": 1,
      "ledger_id": "<UUID4 hex 32 lower, immutable activation identity>",
      "native_session_id": "<native Thyca SID>",
      "attempts": {"<channel>:<mid>": {record}},
      ...unknown top-level keys preserved
    }

Activation identity: ``ledger_id`` is required top-level, generated as
``uuid.uuid4().hex`` ONLY when the file is missing (fresh exclusive
create). It is immutable and never regenerated, migrated, or reset.
Load validates strict canonical 32 lower hex (valid UUID syntax);
missing or non-canonical fails closed without reset. Root owns the
activation proof: it checks the delivery ``jev_ledger_id`` marker
BEFORE constructing, binds fresh ``ledger_id`` into delivery BEFORE any
paid call, and fails on mismatch despite same SID. Standalone fresh
construct is allowed; path change or file loss never reuses an ID.

Record (all IDs strict positive ints, never bool)::

    {
      "native_session_id", "channel_id", "message_id", "author_uid",
      "fingerprint": "<64-hex context SHA256>",
      "model": "jev-1.13.0",
      "attempt_id": "<UUID4 hex 32 lower>",
      "attempted_day": "YYYY-MM-DD", "attempted_at": "<UTC ISO>",
      "status": started|decided|failed_known|failed_unknown,
      "usage": {"input_tokens","output_tokens"}|None,
      "estimated_cost_usd": "<canonical Decimal>"|"unknown"|None,
      "billing": known|unknown|pending,
      "participation": respond|observe|None,
      "answers": {three validated answers}|None,
      ...unknown record keys preserved
    }

Stored answers are the three routing questions (addressee,
explicit_assistant_request, participation). Records written before the
3-question cut carry the legacy five (plus sender_uid and
reply_parent_status); those still validate read-only so a running ledger
keeps its cache and spend history, but new classifications write three.

Binding: fingerprint is SHA256 over the canonical projected snapshot
(``json.dumps(snapshot, ensure_ascii=False, sort_keys=True,
separators=(",", ":"))``). Classify binds the snapshot BEFORE any
provider call or write: ``snapshot.new_message_id`` must equal
``str(message_id)`` and the current message ``sender_uid`` must equal
``str(author_uid)``, else bounded ``CONTEXT_MISMATCH`` with no provider,
no write, no raw body. The Jev ``sender_uid`` answer is diagnostic only
and never confers ownership. Cached ``decided`` returns only on EXACT
channel/mid/author/fingerprint match with no provider call. Any other
existing status (``started``, ``failed_known``, ``failed_unknown``) or any
author/fingerprint mismatch raises :class:`ParticipationPending` -- never a
retry, including after restart or cancellation. Any ``started`` (orphan or
active, billing unknown) or ``failed_unknown`` -- for any other key, any
day -- blocks NEW sources with ``UNKNOWN_BLOCKED`` (cached ``decided``
still returns); never silently reset on rollover or source deletion.

Billing: estimated cost is input-tokens only at exactly
``$0.042/1M`` (``Decimal("0.000000042")`` per token), canonical plain
Decimal strings (no exponent, ``"0"`` for zero). Per-day known cost is
reported in ``summary()`` (``decided`` + ``failed_known`` +
``failed_unknown`` with a retained lower bound) for observability only:
there is NO spend stop -- every new source classifies while the unknown-
billing guards allow. Actual provider/account billing is unknown
(estimate only). Persistence-boundary accounting
validates/extracts returned ``usage`` FIRST, before model/answers/
participation/latency, even for malformed or raising-getter objects;
valid usage is always charged on later shape failure, never replaced
with unknown-only. ``JevError`` valid usage + ``unknown_billing=True``
retains the known lower-bound cost AND the unknown halt (``failed_unknown``
with usage/cost, still globally blocks). Valid usage + ``False`` is
``failed_known`` (no halt); no valid usage + ``False`` is known-zero
``failed_known`` (pre-provider, no halt); no valid usage + unknown is
pure ``"unknown"`` halt. Any ``started`` or ``failed_unknown`` globally
blocks NEW attempts -- never silently reset
on rollover. Known failures block only their source; others proceed only
if FIFO allows. No raw context stored.

Durability (serial single-process / single asyncio loop only):
- Every mutation loads FRESH state from disk and saves atomically (private
  temp inode 0600 + flush/fsync + ``os.replace`` + parent-dir fsync). No
  stale save across the provider ``await``. No retries of provider calls.
  Fresh ``started`` is written synchronously BEFORE the provider ``await``;
  combined with the global ``started`` guard this ensures max 1 active
  paid call across instances in the same loop (sync section has no await,
  so no interleave; cross-process unsupported). Cached ``decided`` for
  another key may still return. Ledger blocks only new paid attempts;
  it never blocks unrelated manual Discord sends.
- In-memory ``asyncio.Lock`` + in-progress set prevents concurrent
  duplicate provider calls for the same key within one instance.
- Missing file is created exclusively (temp + fsync + hard-link commit +
  dir fsync) with a fresh immutable ``ledger_id``; existing
  valid/corrupt/unknown files are never overwritten. Corrupt JSON, wrong
  SID, missing/non-canonical ``ledger_id``, symlink paths/parents, or
  unsafe paths fail closed without reset. Unknown top/record keys are
  preserved.
- Pre-commit failures preserve original bytes and clean only the owned
  temp. Post-commit parent-fsync failures raise
  :class:`ParticipationDurabilityUncertain` (committed=True): the file
  exists and validates; callers MUST stop, never rollback/retry/delete.
- New parent dirs are 0700; existing parents and evidence are never
  chmodded. Ledger file is 0600 via the temp inode; no post-commit chmod.

Public contracts:
- ``ParticipationLedger(path, *, native_sid, day_provider)``
- ``ledger.ledger_id`` -> immutable activation ID, reloaded validated
  from disk on every access; never cached, no raw/env/network.
- ``await classify(adapter, state, *, channel_id, message_id, author_uid)``
  -> detached decided record, else ``ParticipationPending``/``Cancelled``.
- ``get(channel_id, message_id)`` -> detached record or None.
- ``summary()`` -> detached per-day cost/count/block flags + ``ledger_id``.
All snapshots are deep copies. ``ParticipationError`` (``ValueError``) for
missing/corrupt/invalid; ``ParticipationPending`` carries a bounded
``ParticipationReason``; cancellation propagates after persisting
``failed_unknown``. Importing this module performs no work: stdlib plus
local canonical validation only, no env reads, no SDK imports, no network.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import sessions
from conversation import ConversationError, project_conversation_state
from jev import (
    JEV_MODEL,
    PARTICIPATION_OPTIONS,
    JevError,
    build_questions,
    validate_answers,
    validate_model_id,
)

from .errors import ParticipationError, ParticipationPending, ParticipationReason
from .store import (
    _create_ledger,
    _default_day_provider,
    _ensure_parent_dir,
    _load_ledger,
    _now_iso,
    _save_ledger,
)
from .validate import (
    PARTICIPATION_VERSION,
    _canonical_decimal,
    _estimated_cost_str,
    _extract_unknown_halt,
    _extract_valid_usage,
    _fingerprint_snapshot,
    _is_strict_positive_int,
    _is_valid_day,
    _job_key,
    _known_cost_for_today,
    _validate_id_string,
    _validate_ledger,
)


class ParticipationLedger:
    """Serial durable Jev participation ledger for one native session."""

    def __init__(
        self,
        path: str | Path,
        *,
        native_sid: str,
        day_provider: Callable[[], str] | None = _default_day_provider,
    ) -> None:
        file = Path(path)
        if _validate_id_string(native_sid) is None:
            raise ParticipationError(f"invalid native_sid for participation ledger {file}")
        if day_provider is None:
            provider: Callable[[], str] = _default_day_provider
        elif callable(day_provider):
            provider = day_provider
        else:
            raise ParticipationError(f"invalid day_provider for participation ledger {file}")
        self._path = file
        self._native_sid = native_sid
        self._day_provider = provider
        self._lock = asyncio.Lock()
        self._in_progress: set[str] = set()
        if os.path.lexists(file):
            if file.is_symlink():
                raise ParticipationError(f"participation ledger {file} must not be a symlink")
            _load_ledger(file, native_sid)
            return
        _ensure_parent_dir(file)
        if os.path.lexists(file):
            raise ParticipationError(
                f"participation ledger {file} already exists; refusing to overwrite"
            )
        fresh = {
            "version": PARTICIPATION_VERSION,
            "ledger_id": uuid.uuid4().hex,
            "native_session_id": native_sid,
            "native_sessions": {},
            "attempts": {},
        }
        _validate_ledger(json.loads(json.dumps(fresh)), file, native_sid)
        _create_ledger(file, fresh)

    @property
    def ledger_id(self) -> str:
        """Return the immutable activation identity, reloaded from disk.

        Reloads and validates the file on every access; never cached.
        Fails closed if missing/corrupt/wrong SID/symlink/unsafe. No raw
        data, env, or network. Private ``_path`` changes take effect
        immediately; a deleted file never returns a stale cached ID.
        """
        state = _load_ledger(self._path, self._native_sid)
        return state["ledger_id"]

    def get(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return a detached record for ``channel_id:message_id`` or None."""
        if not _is_strict_positive_int(channel_id):
            raise ParticipationError(f"invalid channel_id for participation ledger {self._path}")
        if not _is_strict_positive_int(message_id):
            raise ParticipationError(f"invalid message_id for participation ledger {self._path}")
        state = _load_ledger(self._path, self._native_sid)
        record = state["attempts"].get(_job_key(channel_id, message_id))
        return copy.deepcopy(record) if record is not None else None

    def summary(self) -> dict[str, Any]:
        """Return detached per-day cost/count/block flags for today."""
        state = _load_ledger(self._path, self._native_sid)
        try:
            today = self._day_provider()
        except Exception:
            raise ParticipationError(
                f"invalid day_provider for participation ledger {self._path}"
            ) from None
        if not _is_valid_day(today):
            raise ParticipationError(f"invalid day for participation ledger {self._path}")
        known = _known_cost_for_today(state["attempts"], today)
        counts = {"started": 0, "decided": 0, "failed_known": 0, "failed_unknown": 0}
        for record in state["attempts"].values():
            counts[record["status"]] += 1
        return {
            "native_session_id": self._native_sid,
            "ledger_id": state["ledger_id"],
            "today": today,
            "known_cost_today_usd": _canonical_decimal(known),
            "attempts": len(state["attempts"]),
            "decided": counts["decided"],
            "failed_known": counts["failed_known"],
            "failed_unknown": counts["failed_unknown"],
            "started": counts["started"],
            "unknown_blocked": (counts["failed_unknown"] + counts["started"]) > 0,
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
        budget, or unknown-billing blocks. Cancellation persists
        ``failed_unknown`` then propagates. Post-commit durability failures
        raise :class:`ParticipationDurabilityUncertain`; callers MUST stop.
        """
        if not _is_strict_positive_int(channel_id):
            raise ParticipationError(f"invalid channel_id for participation ledger {self._path}")
        if not _is_strict_positive_int(message_id):
            raise ParticipationError(f"invalid message_id for participation ledger {self._path}")
        if not _is_strict_positive_int(author_uid):
            raise ParticipationError(f"invalid author_uid for participation ledger {self._path}")
        decide = getattr(adapter, "decide", None)
        if not callable(decide):
            raise ParticipationError(f"invalid adapter for participation ledger {self._path}")
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
        key = _job_key(channel_id, message_id)
        async with self._lock:
            if key in self._in_progress:
                raise ParticipationPending(ParticipationReason.IN_PROGRESS)
            self._in_progress.add(key)
        try:
            return await self._classify_guarded(
                decide, snapshot, key, fingerprint, channel_id, message_id, author_uid
            )
        finally:
            self._in_progress.discard(key)

    async def _classify_guarded(
        self,
        decide: Any,
        snapshot: dict[str, Any],
        key: str,
        fingerprint: str,
        channel_id: int,
        message_id: int,
        author_uid: int,
    ) -> dict[str, Any]:
        try:
            today = self._day_provider()
        except Exception:
            raise ParticipationError(
                f"invalid day_provider for participation ledger {self._path}"
            ) from None
        if not _is_valid_day(today):
            raise ParticipationError(f"invalid day for participation ledger {self._path}")
        ledger = _load_ledger(self._path, self._native_sid)
        existing = ledger["attempts"].get(key)
        if existing is not None:
            if existing["author_uid"] != author_uid or existing["fingerprint"] != fingerprint:
                raise ParticipationPending(ParticipationReason.CONTEXT_MISMATCH)
            status = existing["status"]
            if status == "decided":
                return copy.deepcopy(existing)
            if status == "started":
                raise ParticipationPending(ParticipationReason.ALREADY_STARTED)
            if status == "failed_known":
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN)
        for record in ledger["attempts"].values():
            # Orphan/active started means billing unknown too: halt NEW paid.
            # Cached decided for the requested key already returned above.
            if record["status"] in ("failed_unknown", "started"):
                raise ParticipationPending(ParticipationReason.UNKNOWN_BLOCKED)
        attempt_id = uuid.uuid4().hex
        ledger["attempts"][key] = {
            "native_session_id": self._native_sid,
            "channel_id": channel_id,
            "message_id": message_id,
            "author_uid": author_uid,
            "fingerprint": fingerprint,
            "model": JEV_MODEL,
            "attempt_id": attempt_id,
            "attempted_day": today,
            "attempted_at": _now_iso(),
            "status": "started",
            "usage": None,
            "estimated_cost_usd": None,
            "billing": "pending",
            "participation": None,
            "answers": None,
        }
        _save_ledger(self._path, ledger)
        provider_snapshot = copy.deepcopy(snapshot)
        try:
            decision = await decide(provider_snapshot)
        except asyncio.CancelledError:
            self._mark_unknown(key, attempt_id)
            raise
        except JevError as exc:
            known_usage = _extract_valid_usage(exc)
            unknown_halt = _extract_unknown_halt(exc)
            if known_usage is not None:
                if unknown_halt:
                    self._mark_unknown(key, attempt_id, known_usage)
                    raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN) from None
                self._mark_known(key, attempt_id, known_usage)
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
            if not unknown_halt:
                self._mark_known(key, attempt_id, {"input_tokens": 0, "output_tokens": 0})
                raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
            self._mark_unknown(key, attempt_id)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN) from None
        except ConversationError:
            self._mark_known(key, attempt_id, {"input_tokens": 0, "output_tokens": 0})
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
        except Exception:
            self._mark_unknown(key, attempt_id)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN) from None
        usage = _extract_valid_usage(decision)
        if usage is None:
            self._mark_unknown(key, attempt_id)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_UNKNOWN) from None
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
            if participation not in PARTICIPATION_OPTIONS or model != JEV_MODEL:
                raise ValueError("bad decision")
        except Exception:
            self._mark_known(key, attempt_id, usage)
            raise ParticipationPending(ParticipationReason.ALREADY_FAILED_KNOWN) from None
        fresh = _load_ledger(self._path, self._native_sid)
        record = fresh["attempts"].get(key)
        if record is None or record["status"] != "started" or record["attempt_id"] != attempt_id:
            raise ParticipationError("participation ledger state changed during call")
        record["status"] = "decided"
        record["usage"] = dict(usage)
        record["estimated_cost_usd"] = _estimated_cost_str(usage["input_tokens"])
        record["billing"] = "known"
        record["participation"] = participation
        record["answers"] = copy.deepcopy(answers)
        _save_ledger(self._path, fresh)
        return copy.deepcopy(record)

    def _require_started(self, key: str, attempt_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        fresh = _load_ledger(self._path, self._native_sid)
        record = fresh["attempts"].get(key)
        if record is None or record["status"] != "started" or record["attempt_id"] != attempt_id:
            raise ParticipationError("participation ledger state changed during call")
        return fresh, record

    def _mark_known(self, key: str, attempt_id: str, usage: dict[str, int]) -> None:
        fresh, record = self._require_started(key, attempt_id)
        record["status"] = "failed_known"
        record["usage"] = {
            "input_tokens": usage["input_tokens"],
            "output_tokens": usage["output_tokens"],
        }
        record["estimated_cost_usd"] = _estimated_cost_str(usage["input_tokens"])
        record["billing"] = "known"
        record["participation"] = None
        record["answers"] = None
        _save_ledger(self._path, fresh)

    def _mark_unknown(
        self, key: str, attempt_id: str, known_usage: dict[str, int] | None = None
    ) -> None:
        fresh, record = self._require_started(key, attempt_id)
        record["status"] = "failed_unknown"
        if known_usage is None:
            record["usage"] = None
            record["estimated_cost_usd"] = "unknown"
        else:
            record["usage"] = {
                "input_tokens": known_usage["input_tokens"],
                "output_tokens": known_usage["output_tokens"],
            }
            record["estimated_cost_usd"] = _estimated_cost_str(known_usage["input_tokens"])
        record["billing"] = "unknown"
        record["participation"] = None
        record["answers"] = None
        _save_ledger(self._path, fresh)


def rotate_native_session(
    path: str | Path,
    *,
    expected_current: str,
    new_sid: str,
    day: str,
    history: dict[str, str],
) -> dict[str, Any]:
    """Adopt a new daily native session; provenance-preserving.

    Daily-rotation commit for the participation ledger. Loads FRESH
    state and requires, in order:

    1. ``new_sid`` is a strict native SID and ``day`` a real calendar
       day; ``history`` is a valid day -> SID map with ``history[day]
       == new_sid`` and ``expected_current`` among its values (the
       outgoing SID stays in provenance; never silently dropped).
    2. The stored top-level ``native_session_id`` equals
       ``expected_current``; anything else (including an already-rotated
       file) fails loudly -- partial multi-file states never silently
       resume, they fail closed for operator recovery.

    On success the top-level ``native_session_id`` becomes ``new_sid``
    and ``native_sessions`` becomes ``history``; every attempt record
    keeps the SID of its classification day (still valid: records may
    carry any SID in {current} + history). ``ledger_id``, attempts, and
    unknown keys are untouched. The caller owns drain-gating (the
    delivery journal refuses rotation while jobs are unresolved). No
    network, no clock.
    """
    file = Path(path)
    if sessions.validate_native_sid(new_sid) is None:
        raise ParticipationError(f"invalid new_sid for participation ledger {file}")
    if sessions.validate_day(day) is None:
        raise ParticipationError(f"invalid day for participation ledger {file}")
    clean_history = sessions.validate_sessions_history(history)
    if clean_history is None:
        raise ParticipationError(f"invalid history for participation ledger {file}")
    if clean_history.get(day) != new_sid:
        raise ParticipationError(f"participation ledger {file}: history does not bind day")
    if not isinstance(expected_current, str) or not expected_current:
        raise ParticipationError(f"invalid expected_current for participation ledger {file}")
    if expected_current not in clean_history.values():
        raise ParticipationError(f"participation ledger {file}: history drops the current session")
    # Fresh load under the outgoing SID: a wrong-SID file (including
    # an already-rotated one) fails here, never silently resumed.
    state = _load_ledger(file, expected_current)
    state["native_session_id"] = new_sid
    state["native_sessions"] = clean_history
    _save_ledger(file, state)
    return copy.deepcopy(state)
