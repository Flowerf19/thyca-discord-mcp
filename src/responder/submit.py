"""Pre-POST submit pipeline mixin (Phase 2b+3: store-backed, no trace proof).

Owns the submit-path methods: the after-await freshness guard, the
zero-send observe terminal, the queued submit driver, and the
definitive-409 busy requeue. The routing-validation cluster lives in
:mod:`responder.routing` (``ResponderRoutingMixin``); this mixin's
driver calls into it via the composed :class:`Responder` (never the
reverse). ``_VALID_PARTICIPATIONS`` is canonically owned by
``responder.routing`` (``responder`` re-exports it so existing imports
keep working with identical object identity); ``_sha256_hex`` is
re-exported from :mod:`validate` (the shared stdlib-only home).

The Jev paid decision resolves inside the ledger (conditional
pending->decided/failed over the store); the job side needs no separate
bind write -- the Jev row IS the binding, and a bound ``observe``
terminalizes the queued job directly. The submit marker
(:meth:`Store.mark_submitted`) carries only ``job_ref``, ``native_sid``,
and the pre-turn index (expected-index derivation only): no submitted-
text fingerprint and no turn binding are stored. Completion after the POST
is owned by the orchestrator (``_reconcile_after_post``/``_reconcile_job``)
with the snapshot-poll fallback -- no trace evidence, no recovery module.

Import rule: leaf modules only, never ``responder`` (the
orchestrator imports this mixin, not the reverse) and never
``.routing`` (routing helpers resolve via the composed Responder, not
via import). ``self`` attributes (``_settings``, ``_client``,
``_native_sid``, ``_source_checker``, ``_context_collector``,
``_participation_ledger``, ``_bot_uid``, ``_open_store``,
``_mark_failed``, ``_source_deleted_status``, ``_skip_source_deleted``,
``_fresh_source_deleted_skip``, ``_reconcile_after_post``,
``_validate_routing_snapshot``, ``_validate_decision_record``,
``_handle_participation_pending``, ``_proven_edit_after_classify``)
resolve via inheritance at runtime.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from context import (
    ContextUnavailable,
    context_fingerprint,
)
from conversation import ConversationError, project_conversation_state
from participation import (
    ParticipationPending,
)
from source_messages import SourceDeleted, SourceUnavailable
from store import StoreError
from thyca import (
    PROMPT_MAX,
    ThycaBusyError,
    ThycaClientError,
    ThycaPreconditionError,
    ThycaPromptError,
    ThycaSubmissionUncertainError,
    build_conversation_prompt,
)
from validate import sha256_hex as _sha256_hex  # noqa: F401 - re-exported via responder

from .input import ResponderError, _is_strict_int


class ResponderSubmitMixin:
    """Submit pipeline: freshness guards, native POST."""

    def _fresh_queued_guard(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """After-await freshness guard for the pre-POST routing path.

        Every await in :meth:`_submit_queued` (source/snapshot/collector/
        ledger) may interleave with the MCP pre-send gate. Reloads fresh: a
        durable zero-evidence source_deleted skip wins (``skipped``); other
        terminals report ``idle``; a fresh queued job with zero evidence
        and no submit marker returns None (proceed). Mid-flight jobs are
        reconciled, never submitted here. A queued job already carrying a
        ref or send evidence is a writer bug: fail it (never proceed to a
        paid call or POST on a stale frame). Corrupt loads fail closed.
        """
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            raise ResponderError("job missing for routing guard")
        stage = fresh.get("stage")
        if stage in ("done", "skipped", "failed"):
            return {"status": "idle", "reason": "already_terminal"}
        if stage != "queued":
            return {"status": "pending", "reason": "not_queued"}
        if fresh.get("job_ref") is not None or fresh.get("send_count", 0) > 0:
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "routing_conflict"}
        return None

    def _observe_bound(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Terminalize a bound observe decision with zero native POST.

        Calls the ``observed`` completion (fresh queued only; the Jev row
        IS the binding). No snapshot, prompt, or Thyca call happens on
        this path. Terminals are respected; any other conflict fails
        closed instead of forcing the transition.
        """
        try:
            with self._open_store() as _store:
                _store.complete_job(channel_id, message_id, outcome="observed")
        except StoreError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                with self._open_store() as _store:
                    fresh = _store.get_job(channel_id, message_id)
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            if fresh is not None and fresh.get("stage") in ("done", "skipped", "failed"):
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
        # Latest frame wins: a message edited after capture redelivers
        # under a new seq, while the context snapshot is built from live
        # Discord state. Binding against the stale first frame would pend
        # on context_mismatch forever; the newest captured frame is the
        # frame the live snapshot can match. All guards below stay: the
        # snapshot must still exactly match this frame and live SDK state,
        # and an edit newer than the latest captured frame still pends
        # until its frame arrives.
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
        # Legacy frames without author_is_bot: resolve the ACTUAL bot flag
        # BEFORE the paid call (never assumed False). A bot source skips;
        # an unresolvable UID holds pending with zero paid work (retried
        # next tick); only a proven human proceeds.
        if "author_is_bot" not in entry:
            resolved = await self._resolve_bot_flag(entry["author_uid"])
            if resolved is True:
                return self._skip_bot_legacy(channel_id, message_id)
            if resolved is None:
                return {"status": "pending", "reason": "bot_flag_unresolved"}
            if entry.get("actor_kind") not in ("owner", "other"):
                raise ResponderError("outbox entry actor mismatch")
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
        # Ledger classification (the PAID call): cached decided never calls
        # the provider; a crashed pending gets exactly one retry; a
        # still-unknown outcome at attempts == 2 resolves as observe.
        # Cancellation propagates after the ledger persists its unknown
        # hold; the job stays queued and the next tick observes the hold.
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
        except Exception:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "classification_failed"}
        decision = self._validate_decision_record(channel_id, message_id, record)
        if isinstance(decision, dict):
            return decision
        participation, context_sha256, attempt_id = decision
        if participation == "observe" and record.get("answers_json") is None:
            # Crash-path synthetic observe (D5): no paid call ran this tick
            # and the outcome is unknown, so the ledger already closed the
            # job done/observed in the same transaction. Post-paid guards
            # would misfire on the pre-closed job and on fingerprint drift
            # -- this path is terminal regardless of drift.
            return self._observe_bound(channel_id, message_id)
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
        # The Jev row IS the paid-decision binding (resolved inside the
        # ledger before the POST): no separate bind write. Terminals are
        # respected via the guard above.
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
        # separators). Not a caller auth/permission token: frame correlation
        # only. Never regenerated while submitted/reconciling.
        # The POST renders the FROZEN classified snapshot with the actual
        # job_ref + ledger hash (same lengths as the fit-gate placeholders,
        # so fit is guaranteed).
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
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "prompt_oversize"}
        cleaned = prompt.strip()
        if not cleaned or len(cleaned) > PROMPT_MAX:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "prompt_oversize"}
        # Durable submit marker BEFORE the POST: exact nonce + serving SID
        # + pre-turn index. No text hash, no turn binding is stored.
        # No await runs between the guard above and this mark, so a
        # conflict is a writer bug: terminals are respected, anything else
        # fails closed (never force the mark, never blind retry).
        try:
            with self._open_store() as _store:
                _store.mark_submitted(
                    channel_id,
                    message_id,
                    job_ref=job_ref,
                    native_sid=self._native_sid,
                    pre_turn_index=pre_max,
                )
        except StoreError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                with self._open_store() as _store:
                    fresh = _store.get_job(channel_id, message_id)
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            if fresh is not None and fresh.get("stage") in ("done", "skipped", "failed"):
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("submit conflict") from None
        try:
            result = await self._client.submit_turn(self._native_sid, cleaned)
        except ThycaBusyError as exc:
            return await self._handle_busy_race(channel_id, message_id, exc)
        except ThycaSubmissionUncertainError as exc:
            # Timeout/disconnect: NOT a cancel; native may still run. The
            # outcome is ambiguous: fail (never blind retry). A durable
            # source_deleted skip that landed first still wins.
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            kind = "timeout" if exc.reason == "timeout" else "transport_error"
            self._mark_failed(channel_id, message_id, kind=kind)
            return {
                "status": "failed",
                "reason": "submission_uncertain",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        except asyncio.CancelledError:
            # Cancellation stops only our wait; the native turn may
            # continue. Fail the job (ambiguous, never blind retry) and
            # propagate the cancellation. A durable source_deleted skip is
            # preserved, never clobbered.
            if self._fresh_source_deleted_skip(channel_id, message_id) is None:
                self._mark_failed(channel_id, message_id, kind="transport_error")
            raise
        except ThycaClientError as exc:
            # Coarse kind/status only; never raw bodies or str(exc).
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            if exc.status_code is not None:
                self._mark_failed(
                    channel_id, message_id, kind="http_error", http_status=exc.status_code
                )
            else:
                self._mark_failed(channel_id, message_id, kind="transport_error")
            return {
                "status": "failed",
                "reason": "post_failed",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        if result.get("cancelled") or not result.get("completed"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="post_cancelled")
            return {"status": "failed", "reason": "post_cancelled"}
        if not result.get("native_id_match"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_failed(channel_id, message_id, kind="snapshot_mismatch")
            return {"status": "failed", "reason": "post_sid_mismatch"}
        return await self._reconcile_after_post(channel_id, message_id, pre_max, result)

    def _skip_bot_legacy(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Skip a legacy frame whose bot flag resolved True (proven bot)."""
        try:
            with self._open_store() as _store:
                _store.skip_job(channel_id, message_id, reason="bot_filtered")
        except StoreError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                with self._open_store() as _store:
                    fresh = _store.get_job(channel_id, message_id)
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            if fresh is not None and fresh.get("stage") in ("done", "skipped", "failed"):
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("bot skip conflict") from None
        return {"status": "skipped", "reason": "bot_filtered"}

    async def _handle_busy_race(
        self, channel_id: int, message_id: int, exc: ThycaBusyError
    ) -> dict[str, Any]:
        # A durable source_deleted skip (e.g. via the MCP pre-send gate
        # during the POST await) must never be requeued or failed.
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc2:
            raise ResponderError(f"responder store unavailable: {exc2}") from None
        if fresh is None:
            raise ResponderError("job missing after busy")
        if fresh.get("send_count", 0) > 0:
            self._mark_failed(channel_id, message_id, kind="busy", http_status=409)
            return {"status": "failed", "reason": "busy_with_sends"}
        if fresh.get("stage") != "submitted":
            self._mark_failed(channel_id, message_id, kind="busy", http_status=409)
            return {"status": "failed", "reason": "busy_wrong_stage"}
        try:
            # Definitive 409 only, zero SDK activity -> queued with ref
            # cleared; the NEXT attempt mints a fresh nonce.
            with self._open_store() as _store:
                _store.requeue(channel_id, message_id)
        except StoreError as exc2:
            raise ResponderError(f"responder store unavailable: {exc2}") from None
        return {
            "status": "pending",
            "reason": "busy_requeued",
            "error_kind": exc.kind,
            "error_status": exc.status_code,
        }
