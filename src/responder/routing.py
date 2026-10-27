"""Routing-validation mixin for the pre-POST submit path.

Owns the routing validation cluster: the canonical
``_VALID_PARTICIPATIONS`` set (``responder`` re-exports it so existing
imports keep working with identical object identity), the snapshot
projector lookup, the routing-snapshot validator, the live-frame
rebinder, the two proven-edit proofs, the ledger decision validator,
and the participation-hold mapper. The queued submit driver
(:meth:`ResponderSubmitMixin._submit_queued`) calls into these helpers
via the composed :class:`Responder`; this module never calls back into
the submit driver.

Import rule: leaf modules only, never ``responder`` (the
orchestrator imports this mixin, not the reverse) and never ``.core``
or ``.submit``. ``self`` attributes (``_settings``, ``_bot_uid``,
``_source_deleted_status``, ``_mark_failed``,
``_participation_ledger``, ``_skip_source_edited``) resolve via
inheritance at runtime.
"""

from __future__ import annotations

import copy
from typing import Any

from context import (
    FIT_HASH,
    FIT_NONCE,
    context_fingerprint,
)
from conversation import ConversationError, project_conversation_state
from participation import (
    ParticipationReason,
)
from thyca import (
    CONTENT_MAX,
    ThycaPromptError,
    build_conversation_prompt,
)

from .input import _validate_id_string

# Canonical Jev participation set (observe never POSTs: durable zero-send
# terminal instead). Re-exported by responder. ``need_context`` never comes
# from the provider (its criteria are respond|observe) but stays valid:
# seeded flows and the agent contract reference the turn shape.
_VALID_PARTICIPATIONS = frozenset({"respond", "observe", "need_context"})


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


class ResponderRoutingMixin:
    """Routing validation: snapshot checks, edit proofs, decision records."""

    def _validate_routing_snapshot(
        self, channel_id: int, message_id: int, snapshot: Any, entry: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]] | dict[str, Any]:
        """Validate a collected snapshot before any paid call.

        Returns ``(projected, submit_entry)`` on success, else a status
        dict to return (the caller must not proceed). Projection/count/fit
        failures are collector bugs or necessary-pairs overflow:
        ``failed`` with zero paid call (never truncate bodies). A binding
        mismatch against the frozen entry is held ``pending``
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
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "context_invalid"}
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
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "context_invalid"}
        # Fit gate on the ACTUAL final renderer for BOTH participations
        # (canonical fixed 32-char nonce + 64-char hash placeholders, same
        # lengths as production values -- never a guessed reserve). A
        # snapshot failing either route fails with zero paid call; raw
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
                self._mark_failed(channel_id, message_id, kind="transport_error")
                return {"status": "failed", "reason": "snapshot_oversize"}
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
        anything else also keeps pending.
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
        if not isinstance(live_text, str) or not live_text or len(live_text) > CONTENT_MAX:
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

        The ledger already holds a decided record whose fingerprint no
        longer matches the current snapshot. Rebuild the classification-
        bound candidate by reverting the live source text to the newest
        captured frame text and fingerprint it: equality with the record's
        stored fingerprint proves the record was classified on the
        captured text and the live text is a post-classify edit. Anything
        else -- drift the record never saw, a record already bound to the
        live text, unparsable shapes, a missing record -- keeps pending.
        The stored frame is never mutated. Sender/owner/assistant binding
        is inherent in the fingerprint equality itself.
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
        record = self._participation_ledger.get(channel_id, message_id)
        if not isinstance(record, dict):
            return False
        stored_fp = record.get("context_sha256")
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
    ) -> tuple[str, str, str] | dict[str, Any]:
        """Validate a ledger classification record (post-paid).

        Reads the authoritative ``context_sha256`` key and returns
        ``(participation, context_sha256, attempt_id)`` on success, else a
        status dict to return. The model pin is enforced inside the ledger
        before the resolve commits, so cache hits are pinned by
        construction. Malformed records fail the job (never a blind retry
        or rebind).
        """
        fingerprint = record.get("context_sha256") if isinstance(record, dict) else None
        valid = (
            isinstance(record, dict)
            and record.get("status") == "decided"
            and record.get("participation") in _VALID_PARTICIPATIONS
            and isinstance(fingerprint, str)
            and len(fingerprint) == 64
            and all(ch in "0123456789abcdefABCDEF" for ch in fingerprint)
            and _validate_id_string(record.get("attempt_id")) is not None
        )
        if not valid:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "decision_invalid"}
        assert isinstance(record, dict)
        assert isinstance(fingerprint, str)
        return (
            record["participation"],
            fingerprint,
            record["attempt_id"],
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
        """Map a ledger participation hold to a pending/failed status.

        Exact ``ParticipationReason`` values (prompt holds stay ``pending``
        with the job queued, reusable from cache with no rebill and no
        immediate reclassify):

        - ``context_mismatch`` / ``in_progress`` ->
          pending (retryable; mismatch/transient).
        - ``unknown_blocked`` (ANOTHER job failed unknown; this job never
          attempted) -> pending ``unknown_blocked``: innocent jobs are
          preserved queued (never terminalized for another job's failure)
          while the FIFO visibly halts for operator resolution.
        - ``already_started`` / ``already_failed_unknown`` ->
          ``failed`` ``classification_unknown`` (durable unknown, never
          retry; the retry-once rule already ran inside the ledger).
        - ``already_failed_known`` -> ``failed``
          ``classification_failed`` (durable failure, never retry).
        - A hold whose reason is not a verified ``ParticipationReason``
          member fails closed without echoing the object.
        """
        reason = exc.reason
        if not isinstance(reason, ParticipationReason):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "classification_failed"}
        if reason is ParticipationReason.CONTEXT_MISMATCH:
            # Owner policy: a source edited after classification started
            # is skipped, never answered on stale text and never
            # reclassified (no rebill). Proven only by fingerprint: the
            # record's stored fingerprint must equal the live snapshot
            # with its source text reverted to the captured frame text.
            # A bare live-text != frame-text comparison is NOT enough (an
            # E2 rebind binds the live text, so later pure drift would
            # falsely skip); any unproven drift keeps pending as today.
            if self._proven_edit_since_capture(channel_id, message_id, snapshot, captured_text):
                return self._skip_source_edited(channel_id, message_id)
            return {"status": "pending", "reason": "context_mismatch"}
        if reason is ParticipationReason.IN_PROGRESS:
            return {"status": "pending", "reason": "classification_started"}
        if reason is ParticipationReason.UNKNOWN_BLOCKED:
            return {"status": "pending", "reason": "unknown_blocked"}
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        self._mark_failed(channel_id, message_id, kind="transport_error")
        if reason in (
            ParticipationReason.ALREADY_STARTED,
            ParticipationReason.ALREADY_FAILED_UNKNOWN,
        ):
            return {"status": "failed", "reason": "classification_unknown"}
        return {"status": "failed", "reason": "classification_failed"}
