"""Native POST recovery + trace proof (no orchestrator import).

Genuinely owns the five post-POST proof methods moved verbatim from the
orchestrator: candidate coverage with the exact submitted fingerprint,
late durable ``source_deleted`` wins, ``mark_inflight`` binding, 1:1
send intent/receipt/trace matching, and the ``need_context`` observation
proof. Shares one ``client`` / ``native_sid`` / ``settings`` triple plus
two Root policy callbacks (``mark_uncertain`` and ``source_deleted_status``);
it never takes the whole controller, a dummy self protocol, or globals.

Canonical owner of ``_SAFE_TERMINAL``; the orchestrator imports and
re-exports it. Imports :class:`ResponderError` from the canonical input
module and the stable ``delivery`` / ``ThycaClientError`` aliases only.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import delivery
from .config import Settings
from .responder_input import ResponderError, _is_strict_int
from .thyca_client import ThycaClientError

# ``observed`` is the durable zero-send terminal for Jev observe routing
# (positive no-send proof, then safe-prefix ACK). The delivery loader owns
# the stage; recovery only recognizes it as terminal here.
_SAFE_TERMINAL = frozenset({"completed", "skipped", "observed"})


class ResponderRecovery:
    """Cohesive native recovery owner (explicit deps, no I/O on init)."""

    def __init__(
        self,
        *,
        client: Any,
        native_sid: str,
        settings: Settings,
        mark_uncertain: Callable[..., None],
        source_deleted_status: Callable[..., dict[str, Any] | None],
    ) -> None:
        self._client = client
        self._native_sid = native_sid
        self._settings = settings
        if not callable(mark_uncertain):
            raise ResponderError("mark_uncertain must be callable")
        if not callable(source_deleted_status):
            raise ResponderError("source_deleted_status must be callable")
        self._mark_uncertain = mark_uncertain
        self._source_deleted_status = source_deleted_status

    async def reconcile_after_post(
        self,
        channel_id: int,
        message_id: int,
        pre_max: int | None,
        fingerprint_sha: str,
        fingerprint_len: int,
    ) -> dict[str, Any]:
        try:
            snap = await self._client.get_session_snapshot(self._native_sid)
        except ThycaClientError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "snapshot_unavailable"}
        if not snap.get("native_id_match"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "snapshot_mismatch"}
        indices = snap.get("turn_indices") or []
        if pre_max is None:
            candidates = [i for i in indices if _is_strict_int(i) and i >= 0]
        else:
            candidates = [i for i in indices if _is_strict_int(i) and i > pre_max]
        try:
            fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        if fresh is None:
            raise ResponderError("job missing after post")
        job_ref = fresh.get("job_ref")
        if not isinstance(job_ref, str) or not job_ref:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "job_ref_missing"}
        matches: list[int] = []
        attempted = 0
        succeeded = 0
        for idx in candidates:
            attempted += 1
            try:
                evidence = await self._client.get_trace_evidence(
                    self._native_sid,
                    idx,
                    job_ref=job_ref,
                    source_message_id=message_id,
                    channel_id=channel_id,
                    guild_id=self._settings.guild_id,
                )
            except ThycaClientError:
                continue
            succeeded += 1
            if (
                evidence.get("request_input_sha256") == fingerprint_sha
                and evidence.get("request_input_len") == fingerprint_len
                and evidence.get("trace_session_match") is True
                and evidence.get("turn_index_match") is True
            ):
                matches.append(idx)
        # Every await above could have interleaved with the MCP pre-send
        # gate: a durable zero-evidence skip wins over any late outcome.
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        if attempted != succeeded:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {
                "status": "uncertain",
                "reason": "trace_coverage_incomplete",
                "attempted": attempted,
                "succeeded": succeeded,
            }
        if len(matches) != 1:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            reason = "trace_no_match" if not matches else "trace_ambiguous"
            return {"status": "uncertain", "reason": reason, "match_count": len(matches)}
        bound_index = matches[0]
        try:
            delivery.mark_inflight(
                self._settings.delivery_state_path, channel_id, message_id, turn_index=bound_index
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        return await self.verify_sends_and_complete(channel_id, message_id, bound_index)

    async def reconcile_job(self, job: dict[str, Any]) -> dict[str, Any]:
        channel_id = job["channel_id"]
        message_id = job["message_id"]
        try:
            fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        if fresh is None:
            raise ResponderError("job missing for reconcile")
        stage = fresh.get("stage")
        if stage in _SAFE_TERMINAL:
            # A durable zero-evidence source_deleted skip reports itself
            # explicitly; other terminals keep the accepted idle signal.
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            return {"status": "idle", "reason": "already_terminal"}
        job_ref = fresh.get("job_ref")
        if not isinstance(job_ref, str) or not job_ref:
            return {"status": "pending", "reason": "no_job_ref"}
        bound = fresh.get("turn_index")
        if _is_strict_int(bound) and bound >= 0:
            try:
                evidence = await self._client.get_trace_evidence(
                    self._native_sid,
                    bound,
                    job_ref=job_ref,
                    source_message_id=message_id,
                    channel_id=channel_id,
                    guild_id=self._settings.guild_id,
                )
            except ThycaClientError:
                skipped = self._source_deleted_status(channel_id, message_id)
                if skipped is not None:
                    return skipped
                return {"status": "uncertain", "reason": "trace_unavailable"}
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            ok = self.evidence_matches_job(fresh, evidence)
            if not ok:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "trace_mismatch"}
            return await self.verify_sends_and_complete(channel_id, message_id, bound)
        # Unbound (turn_submitting after POST, or uncertain without index):
        # scan strictly-greater candidates for the exact submitted fingerprint.
        expected_sha = fresh.get("submitted_text_sha256")
        expected_len = fresh.get("submitted_text_len")
        if expected_sha is None or expected_len is None:
            return {"status": "pending", "reason": "no_fingerprint"}
        try:
            snap = await self._client.get_session_snapshot(self._native_sid)
        except ThycaClientError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            return {"status": "uncertain", "reason": "snapshot_unavailable"}
        if not snap.get("native_id_match"):
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "snapshot_mismatch"}
        indices = snap.get("turn_indices") or []
        pre_max = fresh.get("pre_turn_index")
        if pre_max is None:
            candidates = [i for i in indices if _is_strict_int(i) and i >= 0]
        elif _is_strict_int(pre_max) and pre_max >= 0:
            candidates = [i for i in indices if _is_strict_int(i) and i > pre_max]
        else:
            return {"status": "pending", "reason": "pre_index_malformed"}
        matches: list[int] = []
        attempted = 0
        succeeded = 0
        for idx in candidates:
            attempted += 1
            try:
                evidence = await self._client.get_trace_evidence(
                    self._native_sid,
                    idx,
                    job_ref=job_ref,
                    source_message_id=message_id,
                    channel_id=channel_id,
                    guild_id=self._settings.guild_id,
                )
            except ThycaClientError:
                continue
            succeeded += 1
            if (
                evidence.get("request_input_sha256") == expected_sha
                and evidence.get("request_input_len") == expected_len
                and evidence.get("trace_session_match") is True
                and evidence.get("turn_index_match") is True
            ):
                matches.append(idx)
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        if attempted != succeeded or len(matches) != 1:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {
                "status": "uncertain",
                "reason": "trace_unresolved",
                "attempted": attempted,
                "succeeded": succeeded,
                "match_count": len(matches),
            }
        bound_index = matches[0]
        try:
            delivery.mark_inflight(
                self._settings.delivery_state_path, channel_id, message_id, turn_index=bound_index
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        return await self.verify_sends_and_complete(channel_id, message_id, bound_index)

    def evidence_matches_job(self, job: dict[str, Any], evidence: dict[str, Any]) -> bool:
        expected_sha = job.get("submitted_text_sha256")
        expected_len = job.get("submitted_text_len")
        if expected_sha is None or expected_len is None:
            return False
        if evidence.get("request_input_sha256") != expected_sha:
            return False
        if evidence.get("request_input_len") != expected_len:
            return False
        if evidence.get("status") != "completed":
            return False
        if evidence.get("error_present") is not False:
            return False
        if evidence.get("trace_session_match") is not True:
            return False
        if evidence.get("turn_index_match") is not True:
            return False
        if evidence.get("request_user_count") != 1:
            return False
        return True

    async def verify_sends_and_complete(
        self, channel_id: int, message_id: int, bound_index: int
    ) -> dict[str, Any]:
        try:
            fresh = delivery.get_job(self._settings.delivery_state_path, channel_id, message_id)
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        if fresh is None:
            raise ResponderError("job missing for completion")
        job_ref = fresh.get("job_ref")
        if not isinstance(job_ref, str) or not job_ref:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "job_ref_missing"}
        try:
            evidence = await self._client.get_trace_evidence(
                self._native_sid,
                bound_index,
                job_ref=job_ref,
                source_message_id=message_id,
                channel_id=channel_id,
                guild_id=self._settings.guild_id,
            )
        except ThycaClientError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "trace_unavailable"}
        # The trace await above is the last yield before completion: a
        # durable zero-evidence skip wins over every late outcome below
        # (no inflight bind, no receipt demand, no completion, no hold).
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        if not self.evidence_matches_job(fresh, evidence):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "trace_mismatch"}
        # need_context no_reply path: when the trace carries observation calls
        # (discord__record_no_reply), commit to the observation proof and never
        # fall through to the send path -- sends plus a no_reply declaration
        # on one turn are contradictory.
        if evidence.get("observation_calls"):
            return await self.verify_observation_and_complete(
                channel_id, message_id, bound_index, fresh, evidence
            )
        # The client's all_sends_confirmed covers ONLY the correlated subset;
        # completion additionally requires journaled intents + receipts matched
        # 1:1 against ALL correlated native calls (see below).
        if evidence.get("all_sends_confirmed") is not True:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "sends_unconfirmed"}
        # Uncorrelated sends (other SID / manual) HOLD uncertain: a foreign
        # manual call with a copied job nonce must never count as complete.
        if (evidence.get("legacy_uncorrelated_count") or 0) != 0:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "uncorrelated_sends"}
        sends = evidence.get("sends") or []
        correlated = [s for s in sends if isinstance(s, dict) and s.get("correlated")]
        intents = fresh.get("send_intents") or []
        receipts = fresh.get("send_receipts") or []
        if not intents or len(correlated) != len(intents) or len(receipts) != len(intents):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "send_count_mismatch"}
        # 1-to-1 content + receipt matching (exact IDs, len, SHA).
        unmatched = list(correlated)
        for intent in intents:
            want_len = intent.get("content_len")
            want_hash = str(intent.get("content_hash") or "").lower()
            found = None
            for cand in unmatched:
                if (
                    cand.get("content_len") == want_len
                    and str(cand.get("content_sha256") or "").lower() == want_hash
                ):
                    found = cand
                    break
            if found is None:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "intent_mismatch"}
            unmatched.remove(found)
        if unmatched:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "extra_correlated_send"}
        for receipt in receipts:
            want_mid = receipt.get("sent_message_id")
            if receipt.get("reply_to_message_id") != message_id:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "reply_ref_mismatch"}
            matched = False
            for cand in correlated:
                if (
                    cand.get("result_message_id") == want_mid
                    and cand.get("result_channel_match") is True
                    and cand.get("jump_ids_match") is True
                    and cand.get("result_ok") is True
                ):
                    matched = True
                    break
            if not matched:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "receipt_mismatch"}
        tool_ids = [
            s.get("tool_call_id") for s in correlated if isinstance(s.get("tool_call_id"), str)
        ]
        if len(tool_ids) != len(correlated):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "tool_id_mismatch"}
        try:
            delivery.complete_job(
                self._settings.delivery_state_path,
                channel_id,
                message_id,
                turn_index=bound_index,
                tool_call_ids=tool_ids,
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        return {"status": "completed", "turn_index": bound_index}

    async def verify_observation_and_complete(
        self,
        channel_id: int,
        message_id: int,
        bound_index: int,
        fresh: dict[str, Any],
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Complete a need_context no_reply declaration via trace proof.

        Requires one or more observation calls (``discord__record_no_reply``)
        that are ALL positive: exact channel/source/job_ref correlation, a
        reason EXACTLY equal to the journaled declaration reason, and a
        declared result with matching IDs. Whole-turn proof is verified by
        the caller. ZERO send evidence is required (trace ``send_call_count``
        plus journaled intents and receipts), with no uncorrelated sends.
        Other tool calls (e.g. ``read_context``) are permitted. Repeated
        identical declarations with distinct call IDs are valid proof (the
        declaration is immutable and idempotent); mismatched/missing
        reasons, missing results, duplicate call IDs, or uncorrelated
        entries stay held uncertain. Zero receipts alone never observe.
        Completion goes through the delivery ``complete_observation``
        transition (exact declaration + zero evidence + nonempty unique
        tool IDs). Expected stage after completion is ``observed`` (safe
        terminal).
        """
        intents = fresh.get("send_intents") or []
        receipts = fresh.get("send_receipts") or []
        if intents or receipts:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_with_sends"}
        if (evidence.get("send_call_count") or 0) != 0:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_with_sends"}
        if (evidence.get("legacy_uncorrelated_count") or 0) != 0:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "uncorrelated_sends"}
        observations = evidence.get("observation_calls") or []
        if not isinstance(observations, list) or not observations:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_unconfirmed"}
        if evidence.get("observation_call_count") != len(observations):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_unconfirmed"}
        if evidence.get("all_observations_confirmed") is not True:
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_unconfirmed"}
        # Exact journal bind: every call's reason must equal the journaled
        # declaration reason (whitelisted enum carried as trace metadata;
        # no raw bodies). A missing/mismatched journal proof stays held.
        decl = fresh.get("no_reply_declaration")
        if (
            not isinstance(decl, dict)
            or decl.get("reason") not in ("no_reply_needed", "missing_context")
            or decl.get("job_ref") != fresh.get("job_ref")
        ):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_unconfirmed"}
        decl_reason = decl["reason"]
        tool_ids: list[str] = []
        for obs in observations:
            if not isinstance(obs, dict):
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "observation_unconfirmed"}
            tool_id = obs.get("tool_call_id")
            if not isinstance(tool_id, str) or not tool_id:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "observation_unconfirmed"}
            if obs.get("correlated") is not True or obs.get("reason_valid") is not True:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "observation_unconfirmed"}
            if obs.get("reason") != decl_reason:
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "observation_unconfirmed"}
            if (
                obs.get("result_present") is not True
                or obs.get("result_ids_match") is not True
                or obs.get("result_reason_match") is not True
                or obs.get("result_status_declared") is not True
                or obs.get("result_ok") is not True
            ):
                self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
                return {"status": "uncertain", "reason": "observation_unconfirmed"}
            tool_ids.append(tool_id)
        if len(set(tool_ids)) != len(tool_ids):
            self._mark_uncertain(channel_id, message_id, kind="trace_unverified")
            return {"status": "uncertain", "reason": "observation_unconfirmed"}
        try:
            delivery.complete_observation(
                self._settings.delivery_state_path,
                channel_id,
                message_id,
                turn_index=bound_index,
                tool_call_ids=tool_ids,
            )
        except delivery.DeliveryDurabilityUncertain:
            raise ResponderError("delivery durability uncertain") from None
        return {"status": "observed", "turn_index": bound_index}
