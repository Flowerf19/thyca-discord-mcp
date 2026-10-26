"""Pre-POST submit pipeline mixin (extracted verbatim from responder).

Owns the seven submit-path methods moved verbatim from
:mod:`responder`: the after-await freshness guard, the routing
snapshot and ledger decision validators, the participation-hold mapper,
the zero-send observe terminal, the queued submit driver, and the
definitive-409 busy requeue. Also canonical owner of
``_VALID_PARTICIPATIONS`` and ``_sha256_hex`` (``responder``
re-exports both so existing imports keep working with identical object
identity).

Import rule: leaf modules only, never ``responder`` (the
orchestrator imports this mixin, not the reverse). ``self`` attributes
(``_settings``, ``_client``, ``_native_sid``, ``_source_checker``,
``_context_collector``, ``_participation_ledger``, ``_bot_uid``,
``_mark_uncertain``, ``_source_deleted_status``, ``_skip_source_deleted``,
``_fresh_source_deleted_skip``, ``_enforce_ledger_binding``,
``_reconcile_after_post``) resolve via inheritance at runtime.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import uuid
from typing import Any

import delivery
from context import (
    FIT_HASH,
    FIT_NONCE,
    ContextUnavailable,
    context_fingerprint,
)
from conversation import ConversationError, project_conversation_state
from participation import (
    ParticipationDurabilityUncertain,
    ParticipationPending,
    ParticipationReason,
)
from source_messages import SourceDeleted, SourceUnavailable
from thyca import (
    CONTENT_MAX,
    PROMPT_MAX,
    ThycaBusyError,
    ThycaClientError,
    ThycaPreconditionError,
    ThycaPromptError,
    ThycaSubmissionUncertainError,
    build_conversation_prompt,
)

from .input import ResponderError, _is_strict_int, _validate_id_string
from .recovery import _SAFE_TERMINAL

# Canonical Jev participation set (observe never POSTs: durable zero-send
# terminal instead). Re-exported by responder.
_VALID_PARTICIPATIONS = frozenset({"respond", "observe", "need_context"})


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_message_of(projected: Any, message_id: int) -> dict[str, Any] | None:
    """New-message row of a projected snapshot, or None when unbound.

    Pure lookup by the snapshot's own ``new_message_id`` (never the
    entry): both edit-proof sites compare classified vs live snapshots
    through this single shape."""
    if not isinstance(projected, dict):
        return None
    new_id = projected.get("new_message_id")
    if new_id != str(message_id):
        return None
    messages = projected.get("messages")
    if not isinstance(messages, list):
        return None
    for msg in messages:
        if isinstance(msg, dict) and msg.get("id") == new_id:
            return msg
    return None


class ResponderSubmitMixin:
    """Submit pipeline: freshness guards, routing validation, native POST."""

    def _fresh_queued_guard(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """After-await freshness guard for the pre-POST routing path.

        Every await in :meth:`_submit_queued` (source/snapshot/collector/
        ledger) may interleave with the MCP pre-send gate. Reloads fresh: a
        durable zero-evidence source_deleted skip wins (``skipped``); other
        safe terminals report ``idle``; a fresh queued job with zero evidence
        returns None (proceed). Mid-flight/uncertain/contradictory states
        return a held status -- never proceed to a paid call or POST on a
        stale frame. Corrupt loads fail closed (propagate);
        durability-uncertain raises :class:`ResponderError`.
        """
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        if fresh is None:
            raise ResponderError("job missing for routing guard")
        stage = fresh.get("stage")
        if stage in _SAFE_TERMINAL:
            return {"status": "idle", "reason": "already_terminal"}
        if stage == "uncertain":
            return {"status": "uncertain", "reason": "already_uncertain"}
        if stage != "queued":
            return {"status": "pending", "reason": "not_queued"}
        if fresh.get("job_ref") is not None:
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "routing_conflict"}
        if fresh.get("send_intents") or fresh.get("send_receipts") or fresh.get("trace_refs"):
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "routing_conflict"}
        return None

    def _validate_routing_snapshot(
        self, channel_id: int, message_id: int, snapshot: Any, entry: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]] | dict[str, Any]:
        """Validate a collected snapshot before any paid call.

        Returns ``(projected, submit_entry)`` on success, else a status
        dict to return (the caller must not proceed). Projection/count/fit
        failures are collector bugs or necessary-pairs overflow:
        ``uncertain`` with zero paid call (never truncate bodies). A
        binding mismatch against the frozen entry is held ``pending``
        (edit/race; not reclassified now) -- except the sole-text
        live-frame rebind below. Classifier sender/addressee answers are
        diagnostic only and are never consulted here; only SDK entry
        metadata authenticates.
        """
        try:
            projected = project_conversation_state(snapshot)
        except ConversationError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "context_invalid"}
        author_uid = entry.get("author_uid")
        content = entry.get("content")
        try:
            new_message = next(
                msg for msg in projected["messages"] if msg["id"] == projected["new_message_id"]
            )
        except StopIteration:
            new_message = None
        bound = (
            new_message is not None
            and projected["new_message_id"] == str(message_id)
            and new_message.get("sender_uid") == str(author_uid)
            and new_message.get("text") == content
            and projected["owner_uid"] == str(self._settings.owner_uid)
            and projected["assistant_uid"] == str(self._bot_uid())
        )
        submit_entry = entry
        if not bound:
            # Live-frame rebind: the source was edited after its latest
            # capture, so the live snapshot text differs from the newest
            # captured frame while every other binding field matches.
            # Proceed on a LOCAL copy carrying the live text (the stored
            # outbox is never mutated). Any other mismatch keeps pending
            # exactly as today -- no silent fallthrough.
            rebound = self._rebind_live_frame(projected, new_message, entry, message_id)
            if rebound is None:
                return {"status": "pending", "reason": "context_mismatch"}
            submit_entry = rebound
        if len(projected["messages"]) > self._settings.conversation_max_messages:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "context_invalid"}
        # Fit gate on the ACTUAL final renderer for BOTH participations
        # (canonical fixed 32-char nonce + 64-char hash placeholders, same
        # lengths as production values -- never a guessed reserve). A
        # snapshot failing either route holds with zero paid call; raw
        # bodies are never shortened to fit.
        bot_uid = self._bot_uid()
        for participation in ("respond", "need_context"):
            try:
                build_conversation_prompt(
                    projected,
                    submit_entry,
                    FIT_NONCE,
                    owner_uid=self._settings.owner_uid,
                    guild_id=self._settings.guild_id,
                    bot_uid=bot_uid,
                    participation=participation,
                    context_sha256=FIT_HASH,
                )
            except (ThycaPromptError, ConversationError):
                skipped = self._source_deleted_status(channel_id, message_id)
                if skipped is not None:
                    return skipped
                self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
                return {"status": "uncertain", "reason": "snapshot_oversize"}
        return projected, submit_entry

    def _rebind_live_frame(
        self,
        projected: dict[str, Any],
        new_message: dict[str, Any] | None,
        entry: dict[str, Any],
        message_id: int,
    ) -> dict[str, Any] | None:
        """Rebind to the live snapshot text when it is the sole mismatch.

        The source was edited after its latest capture: message id,
        sender, owner, and assistant all bind exactly and only the text
        differs from the newest captured frame. Returns a LOCAL copy of
        the entry carrying the live text for this submit (the stored
        outbox is never mutated); returns None when any other field
        mismatches, so those keep pending exactly as today. The live
        text must be a non-empty string within the renderer bound --
        anything else also keeps pending. Downstream proofs need no
        weakening: the ledger fingerprint already binds the exact live
        snapshot, and the prompt renderer re-validates entry-vs-snapshot
        binding on the rebound copy.
        """
        if new_message is None:
            return None
        if projected.get("new_message_id") != str(message_id):
            return None
        if new_message.get("sender_uid") != str(entry.get("author_uid")):
            return None
        if projected.get("owner_uid") != str(self._settings.owner_uid):
            return None
        if projected.get("assistant_uid") != str(self._bot_uid()):
            return None
        live_text = new_message.get("text")
        if (
            not isinstance(live_text, str)
            or not live_text
            or len(live_text) > CONTENT_MAX
        ):
            return None
        rebound = dict(entry)
        rebound["content"] = live_text
        return rebound

    def _proven_edit_after_classify(
        self,
        classified: Any,
        fresh: Any,
        message_id: int,
    ) -> bool:
        """True only when the source text provably changed post-classify.

        Compares the classified snapshot's source message against the
        refreshed snapshot's source message (same message_id + sender
        binding, same owner/assistant binding): a text difference is a
        proven edit after classification started. Any other drift (extra
        window messages, reply changes, sender/owner drift, missing or
        non-string text) returns False -- never skip on suspicion.
        """
        old = _source_message_of(classified, message_id)
        new = _source_message_of(fresh, message_id)
        if old is None or new is None:
            return False
        old_sender = old.get("sender_uid")
        new_sender = new.get("sender_uid")
        if not isinstance(old_sender, str) or not isinstance(new_sender, str):
            return False
        if old_sender != new_sender:
            return False
        if fresh.get("owner_uid") != classified.get("owner_uid"):
            return False
        if fresh.get("assistant_uid") != classified.get("assistant_uid"):
            return False
        old_text = old.get("text")
        new_text = new.get("text")
        if not isinstance(old_text, str) or not isinstance(new_text, str):
            return False
        return old_text != new_text

    def _proven_edit_since_capture(
        self,
        channel_id: int,
        message_id: int,
        snapshot: Any,
        captured_text: Any,
    ) -> bool:
        """True only when the ledger-bound text provably differs live.

        The ledger already holds a started/decided record whose
        fingerprint no longer matches the current snapshot. Rebuild the
        classification-bound candidate by reverting the live source text
        to the newest captured frame text and fingerprint it: equality
        with the record's stored fingerprint proves the record was
        classified on the captured text and the live text is a
        post-classify edit. Anything else -- drift the record never saw,
        a record already bound to the live text (E2 rebind), unparsable
        shapes, a missing record -- keeps pending. In particular a bare
        live-text != frame-text comparison is NOT enough on its own, so
        an E2 rebind followed by pure context drift can never skip here
        (the forbidden silent lost reply). The stored frame is never
        mutated. Sender/owner/assistant binding is inherent in the
        fingerprint equality itself.
        """
        if not isinstance(captured_text, str) or not captured_text:
            return False
        try:
            projected_now = project_conversation_state(snapshot)
        except ConversationError:
            return False
        now = _source_message_of(projected_now, message_id)
        if now is None:
            return False
        now_text = now.get("text")
        if not isinstance(now_text, str) or now_text == captured_text:
            return False
        get_record = getattr(self._participation_ledger, "get", None)
        if not callable(get_record):
            # Ledger without record lookup (test stubs): unprovable.
            return False
        record = get_record(channel_id, message_id)
        if not isinstance(record, dict):
            return False
        stored_fp = record.get("fingerprint")
        if not isinstance(stored_fp, str) or len(stored_fp) != 64:
            return False
        candidate = copy.deepcopy(projected_now)
        for msg in candidate.get("messages", []):
            if isinstance(msg, dict) and msg.get("id") == candidate.get("new_message_id"):
                msg["text"] = captured_text
        try:
            candidate_fp = context_fingerprint(candidate)
        except (ConversationError, ValueError, TypeError):
            return False
        return candidate_fp == stored_fp

    def _validate_decision_record(
        self, channel_id: int, message_id: int, record: Any
    ) -> tuple[str, str, str, str] | dict[str, Any]:
        """Validate a ledger classification record (post-paid, pre-bind).

        Reads the authoritative ``fingerprint`` key (copied to the delivery
        ``context_sha256`` bind param) and returns ``(participation,
        context_sha256, attempt_id, model)`` on success, else a status dict
        to return. Malformed records are ledger bugs with possibly-unknown
        billing: ``uncertain``, never a blind retry or rebind. The ledger
        ``attempt_id`` format is writer-owned (parent standardized 32-hex
        UUID hex to match delivery); this validator accepts the ID shape
        and delivery enforces the exact form at bind.
        """
        fingerprint = record.get("fingerprint") if isinstance(record, dict) else None
        valid = (
            isinstance(record, dict)
            and record.get("status") in ("decided", "cached")
            and record.get("participation") in _VALID_PARTICIPATIONS
            and isinstance(fingerprint, str)
            and len(fingerprint) == 64
            and all(ch in "0123456789abcdefABCDEF" for ch in fingerprint)
            and _validate_id_string(record.get("attempt_id")) is not None
            and _validate_id_string(record.get("model")) is not None
        )
        if not valid:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="transport_error")
            return {"status": "uncertain", "reason": "decision_invalid"}
        assert isinstance(record, dict)
        assert isinstance(fingerprint, str)
        return (
            record["participation"],
            fingerprint,
            record["attempt_id"],
            record["model"],
        )

    def _handle_participation_pending(
        self,
        channel_id: int,
        message_id: int,
        exc: BaseException,
        *,
        snapshot: Any = None,
        captured_text: Any = None,
    ) -> dict[str, Any]:
        """Map a ledger participation hold to a pending/uncertain status.

        Exact ``ParticipationReason`` values (prompt holds stay ``pending``
        with the job queued, unmarked, and reusable from cache with no
        rebill and no immediate reclassify):

        - ``context_mismatch`` / ``in_progress`` ->
          pending (retryable; mismatch/transient).
        - ``unknown_blocked`` (ANOTHER job failed unknown; this job never
          attempted) -> pending ``unknown_blocked``: innocent jobs are
          preserved queued (never terminalized for another job's failure)
          while the FIFO visibly halts for operator resolution.
        - ``already_started`` (durable started record, outcome unknown) /
          ``already_failed_unknown`` -> ``uncertain``
          ``classification_unknown`` (durable unknown, never retry).
        - ``already_failed_known`` -> ``uncertain``
          ``classification_failed`` (durable failure, never retry).
        - A hold whose reason is not a verified ``ParticipationReason``
          member fails closed as ``uncertain`` without echoing the object.
        """
        reason = exc.reason
        if not isinstance(reason, ParticipationReason):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="transport_error")
            return {"status": "uncertain", "reason": "classification_failed"}
        if reason is ParticipationReason.CONTEXT_MISMATCH:
            # Owner policy: a source edited after classification started
            # is skipped, never answered on stale text and never
            # reclassified (no rebill). Proven only by fingerprint: the
            # record's stored fingerprint must equal the live snapshot
            # with its source text reverted to the captured frame text.
            # A bare live-text != frame-text comparison is NOT enough (an
            # E2 rebind binds the live text, so later pure drift would
            # falsely skip); any unproven drift keeps pending as today.
            if self._proven_edit_since_capture(
                channel_id, message_id, snapshot, captured_text
            ):
                return self._skip_source_edited(channel_id, message_id)
            return {"status": "pending", "reason": "context_mismatch"}
        if reason is ParticipationReason.IN_PROGRESS:
            return {"status": "pending", "reason": "classification_started"}
        if reason is ParticipationReason.UNKNOWN_BLOCKED:
            return {"status": "pending", "reason": "unknown_blocked"}
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        self._mark_uncertain(channel_id, message_id, kind="transport_error")
        if reason in (
            ParticipationReason.ALREADY_STARTED,
            ParticipationReason.ALREADY_FAILED_UNKNOWN,
        ):
            return {"status": "uncertain", "reason": "classification_unknown"}
        return {"status": "uncertain", "reason": "classification_failed"}

    def _observe_bound(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Terminalize a bound observe decision with zero native POST.

        Calls the delivery ``observe_job`` transition (fresh queued + bound
        observe + zero evidence -> ``observed``). No snapshot, prompt, or
        Thyca call happens on this path. Terminals are respected; any other
        conflict is a bug/contract mismatch (no await runs between bind and
        observe), so it fails closed instead of forcing the transition.
        """
        try:
            delivery.observe_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        except delivery.DeliveryError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
            except delivery.DeliveryDurabilityUncertain:
                raise ResponderError("delivery durability uncertain") from None
            if fresh is not None and fresh.get("stage") in _SAFE_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("jev observe conflict") from None
        return {"status": "observed"}

    async def _submit_queued(
        self, job: dict[str, Any], entries: list[dict[str, Any]]
    ) -> dict[str, Any]:
        channel_id = job["channel_id"]
        message_id = job["message_id"]
        matches = [
            e
            for e in entries
            if e.get("channel_id") == channel_id and e.get("message_id") == message_id
        ]
        if not matches:
            return {"status": "pending", "reason": "entry_not_found"}
        # Latest frame wins: a message edited after capture redelivers under
        # a new seq (aliased to the same job), while the context snapshot is
        # built from live Discord state. Binding against the stale first
        # frame would pend on context_mismatch forever; the newest captured
        # frame is the frame the live snapshot can match. All guards below
        # stay: the snapshot must still exactly match this frame and live
        # SDK state, and an edit newer than the latest captured frame still
        # pends until its frame arrives.
        entry = max(matches, key=lambda e: e.get("seq", 0))
        # Proven-no-send gate BEFORE any paid work (Jev classification or
        # native POST): a confirmed-deleted source skips with zero work; an
        # unproven source holds queued (retry reads later) with zero work.
        try:
            await self._source_checker(channel_id, message_id)
        except asyncio.CancelledError:
            raise
        except SourceDeleted:
            return self._skip_source_deleted(channel_id, message_id)
        except SourceUnavailable:
            return {"status": "pending", "reason": "source_unavailable"}
        except Exception:
            return {"status": "pending", "reason": "source_unavailable"}
        # Native SID identity + idle gate BEFORE paid classification: never
        # spend a Jev call when native is busy, mismatched, or unavailable.
        # (A second fresh snapshot is taken before the POST itself, since
        # native may move during the classification awaits.)
        try:
            gate_snap = await self._client.get_session_snapshot(self._native_sid)
        except ThycaBusyError as exc:
            return {
                "status": "pending",
                "reason": "snapshot_busy",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        except ThycaClientError as exc:
            # Definitive 404 on the serving SID: the native session is
            # verifiably gone (deleted/pruned). Hold pending with zero
            # state mutation; the next tick re-resolves the target bot DM
            # (recreated natively when deleted) and adopts it. Any other
            # snapshot failure holds pending as before.
            if isinstance(exc, ThycaPreconditionError) and exc.status_code == 404:
                return {
                    "status": "pending",
                    "reason": "snapshot_gone",
                    "error_kind": exc.kind,
                    "error_status": exc.status_code,
                }
            return {
                "status": "pending",
                "reason": "snapshot_unavailable",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        # Session identity is the pinned SID; the native display title may change.
        if not gate_snap.get("native_id_match"):
            return {"status": "pending", "reason": "snapshot_mismatch"}
        if gate_snap.get("running"):
            return {"status": "pending", "reason": "native_running"}
        gate_pre_max = gate_snap.get("pre_max_turn_index")
        if gate_pre_max is not None and not _is_strict_int(gate_pre_max):
            return {"status": "pending", "reason": "snapshot_malformed"}
        if _is_strict_int(gate_pre_max) and gate_pre_max < 0:
            return {"status": "pending", "reason": "snapshot_malformed"}
        guard = self._fresh_queued_guard(channel_id, message_id)
        if guard is not None:
            return guard
        # Canonical snapshot collection (no paid call yet). The
        # authoritative SourceDeleted (also raised by the collector for an
        # exact current 404) skips; ContextUnavailable (including an edited
        # source) holds pending; anything else holds without paid work. The
        # exact current 404 is never treated as generic unavailable.
        try:
            snapshot = await self._context_collector(entry)
        except asyncio.CancelledError:
            raise
        except SourceDeleted:
            return self._skip_source_deleted(channel_id, message_id)
        except ContextUnavailable:
            return {"status": "pending", "reason": "context_unavailable"}
        except Exception:
            return {"status": "pending", "reason": "context_unavailable"}
        stored_text = entry.get("content")
        validated = self._validate_routing_snapshot(channel_id, message_id, snapshot, entry)
        if isinstance(validated, dict):
            return validated
        projected, entry = validated
        guard = self._fresh_queued_guard(channel_id, message_id)
        if guard is not None:
            return guard
        # Activation re-check synchronously BEFORE the paid call: a ledger or
        # journal replaced/deleted while this process is alive must stop here
        # with zero new provider work (no await runs between this guard and
        # the ledger's started persist). Failures propagate; never rebind,
        # reset, or terminalize the job here.
        self._enforce_ledger_binding()
        # Ledger classification (the PAID call): cached same-context never
        # calls the provider. Cancellation propagates after the ledger
        # persists its started/unknown hold (which blocks retry); the job
        # stays queued and the next tick observes the hold.
        try:
            record = await self._participation_ledger.classify(
                self._jev_adapter,
                snapshot,
                channel_id=channel_id,
                message_id=message_id,
                author_uid=entry["author_uid"],
            )
        except asyncio.CancelledError:
            raise
        except ParticipationPending as exc:
            return self._handle_participation_pending(
                channel_id, message_id, exc, snapshot=snapshot, captured_text=stored_text
            )
        except ParticipationDurabilityUncertain:
            raise ResponderError("participation durability uncertain") from None
        except Exception:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="transport_error")
            return {"status": "uncertain", "reason": "classification_failed"}
        decision = self._validate_decision_record(channel_id, message_id, record)
        if isinstance(decision, dict):
            return decision
        participation, context_sha256, attempt_id, model = decision
        # Recheck the ACTUAL source after the paid call, BEFORE observe or
        # native POST. The classification spend is sunk; zero native work
        # follows for a deleted/unproven source (cached decision reused).
        try:
            await self._source_checker(channel_id, message_id)
        except asyncio.CancelledError:
            raise
        except SourceDeleted:
            return self._skip_source_deleted(channel_id, message_id)
        except SourceUnavailable:
            return {"status": "pending", "reason": "source_unavailable"}
        except Exception:
            return {"status": "pending", "reason": "source_unavailable"}
        guard = self._fresh_queued_guard(channel_id, message_id)
        if guard is not None:
            return guard
        # Edit/race guard: re-collect from CURRENT SDK state and require
        # the EXACT frozen ledger fingerprint plus CURRENT SDK-auth binding
        # before any transition, so a source edited silently after
        # classification never POSTs under the frozen frame. SourceDeleted
        # skips (the source can become deleted DURING the collector or the
        # post-paid refresh); mismatch holds pending (not reclassified now;
        # the next tick re-collects and the ledger decides cache-hit vs new
        # call).
        try:
            fresh_snapshot = await self._context_collector(entry)
        except asyncio.CancelledError:
            raise
        except SourceDeleted:
            return self._skip_source_deleted(channel_id, message_id)
        except ContextUnavailable:
            return {"status": "pending", "reason": "context_unavailable"}
        except Exception:
            return {"status": "pending", "reason": "context_unavailable"}
        try:
            fresh_projected = project_conversation_state(fresh_snapshot)
            fresh_fp = context_fingerprint(fresh_snapshot)
        except ConversationError:
            return {"status": "pending", "reason": "context_unavailable"}
        if fresh_fp != context_sha256:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            if self._proven_edit_after_classify(projected, fresh_projected, message_id):
                return self._skip_source_edited(channel_id, message_id)
            return {"status": "pending", "reason": "context_mismatch"}
        # Current SDK-auth binding on the refresh (transitively implied by
        # the fingerprint match, verified explicitly per parent guard): the
        # refreshed frame must still authenticate as the current entry.
        try:
            refresh_new = next(
                msg
                for msg in fresh_projected["messages"]
                if msg["id"] == fresh_projected["new_message_id"]
            )
        except StopIteration:
            refresh_new = None
        if (
            refresh_new is None
            or fresh_projected["new_message_id"] != str(message_id)
            or refresh_new.get("sender_uid") != str(entry.get("author_uid"))
            or refresh_new.get("text") != entry.get("content")
            or fresh_projected["owner_uid"] != str(self._settings.owner_uid)
            or fresh_projected["assistant_uid"] != str(self._bot_uid())
        ):
            return {"status": "pending", "reason": "context_mismatch"}
        guard = self._fresh_queued_guard(channel_id, message_id)
        if guard is not None:
            return guard
        # Atomic bind of the paid decision (delivery owns the transition).
        # Idempotent for the same record (409-requeue retry rebinds the
        # cached decision without rebill). Terminals are respected; any
        # other conflict is a bug/contract mismatch (no await runs between
        # the guard above and this bind), so it fails closed and preserves
        # the queued job instead of terminalizing it.
        try:
            delivery.bind_jev_decision(
                self._settings.delivery_state_path,
                channel_id,
                message_id,
                context_sha256=context_sha256,
                attempt_id=attempt_id,
                model=model,
                participation=participation,
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        except delivery.DeliveryError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
            except delivery.DeliveryDurabilityUncertain:
                raise ResponderError("delivery durability uncertain") from None
            if fresh is not None and fresh.get("stage") in _SAFE_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("jev bind conflict") from None
        if participation == "observe":
            # Durable zero-send terminal: no snapshot, no prompt, no POST.
            return self._observe_bound(channel_id, message_id)
        # respond / need_context: fresh snapshot for the POST pre_max +
        # idle check (native may have moved during classification).
        try:
            snap = await self._client.get_session_snapshot(self._native_sid)
        except ThycaBusyError as exc:
            return {
                "status": "pending",
                "reason": "snapshot_busy",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        except ThycaClientError as exc:
            return {
                "status": "pending",
                "reason": "snapshot_unavailable",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        if not snap.get("native_id_match"):
            return {"status": "pending", "reason": "snapshot_mismatch"}
        if snap.get("running"):
            return {"status": "pending", "reason": "native_running"}
        pre_max = snap.get("pre_max_turn_index")
        if pre_max is not None and not _is_strict_int(pre_max):
            return {"status": "pending", "reason": "snapshot_malformed"}
        if _is_strict_int(pre_max) and pre_max < 0:
            return {"status": "pending", "reason": "snapshot_malformed"}
        guard = self._fresh_queued_guard(channel_id, message_id)
        if guard is not None:
            return guard
        # Fresh bound nonce at NEW submission only (uuid4 hex, no predictable
        # separators). Not a caller auth/permission token: frame + trace
        # correlation only. Never regenerated while inflight/reconciling.
        # The POST renders the FROZEN classified snapshot with the actual
        # job_ref + ledger hash (same lengths as the fit-gate placeholders,
        # so fit is guaranteed); the same canonical bytes feed Jev and Thyca.
        job_ref = uuid.uuid4().hex
        try:
            prompt = build_conversation_prompt(
                snapshot,
                entry,
                job_ref,
                owner_uid=self._settings.owner_uid,
                guild_id=self._settings.guild_id,
                bot_uid=self._bot_uid(),
                participation=participation,
                context_sha256=context_sha256,
            )
        except (ThycaPromptError, ConversationError):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "prompt_oversize"}
        cleaned = prompt.strip()
        if not cleaned or len(cleaned) > PROMPT_MAX:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "prompt_oversize"}
        fingerprint_sha = _sha256_hex(cleaned)
        fingerprint_len = len(cleaned)
        # Durable fingerprint BEFORE the POST: exact strip-clean native text
        # (SHA-256 64-hex + character length, never raw).
        try:
            delivery.mark_turn_submitting(
                self._settings.delivery_state_path,
                channel_id,
                message_id,
                job_ref=job_ref,
                pre_turn_index=pre_max,
                submitted_text_sha256=fingerprint_sha,
                submitted_text_len=fingerprint_len,
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        except delivery.DeliveryError:
            # No await runs between the guard above and this mark, so a
            # conflict means the bind contract moved the stage (writer
            # mismatch) or a terminal landed: respect terminals, else fail
            # closed (never force the mark, never blind retry).
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
            except delivery.DeliveryDurabilityUncertain:
                raise ResponderError("delivery durability uncertain") from None
            if fresh is not None and fresh.get("stage") in _SAFE_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("jev bind conflict") from None
        try:
            result = await self._client.submit_turn(self._native_sid, cleaned)
        except ThycaBusyError as exc:
            return await self._handle_busy_race(channel_id, message_id, exc)
        except ThycaSubmissionUncertainError as exc:
            # Timeout/disconnect: NOT a cancel; native may still run and late
            # SDK callbacks may still land. Durable unknown, no blind retry.
            # A durable source_deleted skip that landed first still wins.
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            kind = "timeout" if exc.reason == "timeout" else "transport_error"
            self._mark_uncertain(channel_id, message_id, kind=kind)
            return {
                "status": "uncertain",
                "reason": "submission_uncertain",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        except asyncio.CancelledError:
            # Cancellation stops only our wait; the HTTP thread / native turn
            # may continue and late receipts may still record against this
            # ref (Core keeps those while UNCERTAIN). Persist the durable
            # unknown (fresh, idempotent) and propagate the cancellation.
            # A durable source_deleted skip is preserved, never clobbered.
            if self._fresh_source_deleted_skip(channel_id, message_id) is None:
                self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            raise
        except ThycaClientError as exc:
            # Coarse kind/status only; never raw bodies or str(exc).
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            if exc.status_code is not None:
                self._mark_uncertain(
                    channel_id, message_id, kind="http_error", http_status=exc.status_code
                )
            else:
                self._mark_uncertain(channel_id, message_id, kind="transport_error")
            return {
                "status": "uncertain",
                "reason": "post_failed",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        if result.get("cancelled") or not result.get("completed"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="delivery_unknown")
            return {"status": "uncertain", "reason": "post_cancelled"}
        if not result.get("native_id_match"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "post_sid_mismatch"}
        return await self._reconcile_after_post(
            channel_id, message_id, pre_max, fingerprint_sha, fingerprint_len
        )

    async def _handle_busy_race(
        self, channel_id: int, message_id: int, exc: ThycaBusyError
    ) -> dict[str, Any]:
        # A durable source_deleted skip (e.g. via the MCP pre-send gate
        # during the POST await) must never be requeued or held uncertain.
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        if fresh is None:
            raise ResponderError("job missing after busy")
        if fresh.get("send_intents") or fresh.get("send_receipts") or fresh.get("trace_refs"):
            self._mark_uncertain(channel_id, message_id, kind="busy", http_status=409)
            return {"status": "uncertain", "reason": "busy_with_intents"}
        if fresh.get("stage") not in ("turn_submitting", "inflight"):
            self._mark_uncertain(channel_id, message_id, kind="busy", http_status=409)
            return {"status": "uncertain", "reason": "busy_wrong_stage"}
        try:
            # Definitive 409 only, zero SDK activity -> queued with ref and
            # fingerprint cleared; the NEXT attempt gets a fresh nonce.
            delivery.requeue_definitive_busy(
                self._settings.delivery_state_path, channel_id, message_id, http_status=409
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        return {
            "status": "pending",
            "reason": "busy_requeued",
            "error_kind": exc.kind,
            "error_status": exc.status_code,
        }
