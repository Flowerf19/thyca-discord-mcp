from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import delivery as _delivery_root
import sessions
from delivery_schema import (
    ACTOR_KINDS,
    JEV_MODEL_PINNED,
    JEV_PARTICIPATIONS,
    MAX_URL_LEN,
    NO_REPLY_REASONS,
    SAFE_TERMINAL_STAGES,
    SKIP_REASONS,
    SUBMITTED_TEXT_MAX,
    TERMINAL_STAGES,
    UNCERTAIN_KINDS,
    DeliveryError,
    _has_whitespace,
    _is_attempt_id,
    _is_sha256_hex,
    _is_strict_int,
    _job_key,
    _submitted_pair_ok,
    _validate_id_string,
)

from .store import _load_state, _new_job, _require_job, _used_seqs


def enqueue_job(
    path: str | Path,
    *,
    channel_id: int,
    message_id: int,
    author_uid: int,
    actor_kind: str,
    seq: int,
) -> dict[str, Any]:
    """Enqueue by MID; duplicates append a seq alias without a new job."""
    file = Path(path)
    if not _is_strict_int(channel_id) or channel_id <= 0:
        raise DeliveryError(f"invalid channel_id for delivery state {file}")
    if not _is_strict_int(message_id) or message_id <= 0:
        raise DeliveryError(f"invalid message_id for delivery state {file}")
    if not _is_strict_int(author_uid) or author_uid <= 0:
        raise DeliveryError(f"invalid author_uid for delivery state {file}")
    if not isinstance(actor_kind, str) or actor_kind not in ACTOR_KINDS:
        raise DeliveryError(f"invalid actor_kind for delivery state {file}")
    if not _is_strict_int(seq) or seq <= 0:
        raise DeliveryError(f"invalid seq for delivery state {file}")
    state = _load_state(file)
    key = _job_key(channel_id, message_id)
    job = state["jobs"].get(key)
    if job is not None:
        if job["author_uid"] != author_uid or job["actor_kind"] != actor_kind:
            raise DeliveryError(f"delivery state {file}: job {key} identity mismatch")
        existing = set([job["seq"], *job["seq_aliases"]])
        if seq in existing:
            return copy.deepcopy(job)
        if seq in _used_seqs(state, exclude_key=key):
            raise DeliveryError(f"delivery state {file}: seq already used by another job")
        # No alias budget: legit dedupe/poll redelivery appends freely;
        # positive-int and global uniqueness checks above remain.
        job["seq_aliases"] = sorted([*job["seq_aliases"], seq])
        job["updated_at"] = _delivery_root._now_iso()
        _delivery_root._save_state(file, state)
        return copy.deepcopy(job)
    if seq in _used_seqs(state):
        raise DeliveryError(f"delivery state {file}: seq already used by another job")
    state["jobs"][key] = _new_job(
        channel_id=channel_id,
        message_id=message_id,
        author_uid=author_uid,
        actor_kind=actor_kind,
        seq=seq,
        stage="queued",
    )
    _delivery_root._save_state(file, state)
    return copy.deepcopy(state["jobs"][key])


def mark_turn_submitting(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    job_ref: str,
    pre_turn_index: int | None,
    submitted_text_sha256: str | None = None,
    submitted_text_len: int | None = None,
) -> dict[str, Any]:
    """Move ``queued`` -> ``turn_submitting`` before the native POST.

    ``job_ref`` is the caller-generated random32-hex job id (bounded ID
    string). ``pre_turn_index=None`` means a KNOWN empty native session
    (zero previous user turns), never an unknown/faked index. The optional
    fingerprint pair is BOTH None (legacy) or an exact 64-hex SHA-256 plus
    a positive length of at most 4000 characters describing the exact
    ``prompt.strip`` native POST text (never the raw text). The stored
    fingerprint is immutable: an idempotent repeat must match job_ref +
    pre_turn_index + fingerprint exactly, anything else conflicts.
    """
    file = Path(path)
    if _validate_id_string(job_ref) is None:
        raise DeliveryError(f"invalid job_ref for delivery state {file}")
    if pre_turn_index is not None and (not _is_strict_int(pre_turn_index) or pre_turn_index < 0):
        raise DeliveryError(f"invalid pre_turn_index for delivery state {file}")
    if not _submitted_pair_ok(submitted_text_sha256, submitted_text_len):
        raise DeliveryError(f"invalid submitted fingerprint for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "queued":
        job["job_ref"] = job_ref
        job["pre_turn_index"] = pre_turn_index
        job["submitted_text_sha256"] = submitted_text_sha256
        job["submitted_text_len"] = submitted_text_len
        job["stage"] = "turn_submitting"
        job["updated_at"] = _delivery_root._now_iso()
        _delivery_root._save_state(file, state)
        return copy.deepcopy(job)
    if job["stage"] == "turn_submitting" and (
        job["job_ref"] == job_ref
        and job["pre_turn_index"] == pre_turn_index
        and job["submitted_text_sha256"] == submitted_text_sha256
        and job["submitted_text_len"] == submitted_text_len
    ):
        return copy.deepcopy(job)
    raise DeliveryError(
        f"delivery state {file}: job {key} stage {job['stage']} cannot mark_turn_submitting"
    )


def mark_inflight(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    turn_index: int,
) -> dict[str, Any]:
    """Bind the ACTUAL native turn index after the confirmed own POST/Trace.

    ``turn_submitting`` -> ``inflight``. In ``delivering``/``sent``/
    ``uncertain`` the stage and ALL receipts/error are preserved and only
    an unbound (None) index is set: the same index is idempotent, a
    different index conflicts. ``queued``/``skipped``/``completed`` never
    rewind; binding needs an existing job_ref. The caller recovers the
    exact index (and input SHA/len from the native Trace, matched against
    the own first USER message) BEFORE binding -- never guess a future
    index before the POST race resolves.
    """
    file = Path(path)
    if not _is_strict_int(turn_index) or turn_index < 0:
        raise DeliveryError(f"invalid turn_index for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "turn_submitting":
        job["turn_index"] = turn_index
        job["stage"] = "inflight"
        job["updated_at"] = _delivery_root._now_iso()
        _delivery_root._save_state(file, state)
        return copy.deepcopy(job)
    if job["stage"] == "inflight":
        if job["turn_index"] == turn_index:
            return copy.deepcopy(job)
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot mark_inflight"
        )
    if job["stage"] in ("delivering", "sent", "uncertain"):
        if job["job_ref"] is None:
            raise DeliveryError(
                f"delivery state {file}: job {key} stage {job['stage']} cannot mark_inflight"
            )
        if job["turn_index"] == turn_index:
            return copy.deepcopy(job)
        if job["turn_index"] is not None:
            raise DeliveryError(
                f"delivery state {file}: job {key} stage {job['stage']} cannot mark_inflight"
            )
        job["turn_index"] = turn_index
        job["updated_at"] = _delivery_root._now_iso()
        _delivery_root._save_state(file, state)
        return copy.deepcopy(job)
    raise DeliveryError(
        f"delivery state {file}: job {key} stage {job['stage']} cannot mark_inflight"
    )


def requeue_definitive_busy(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    http_status: int = 409,
) -> dict[str, Any]:
    """Reset a definitively-busy submit attempt back to ``queued``.

    ONLY for a DEFINITIVE native HTTP 409 on the POST: the native service
    rejected the claim and did NOT accept the turn (a shared-session busy
    is an ordinary manual race, NOT a timeout/unknown -- unknown outcomes
    are never blind-retried here). Caller precondition: the attempt has
    ZERO send intents/receipts/trace (nothing can be in flight; a busy
    guard hit after a real SDK intent must preserve the bytes and refuse).
    Only ``turn_submitting``/``inflight`` can requeue; an already-``queued``
    job fails explicitly (never a silent reset of an accepted job), and
    ``uncertain``/``delivering``/``sent``/``completed``/``skipped``/``observed``
    are refused. Any status other than strict-int 409 (503/408/None/str/bool)
    is refused. A definitive busy can never carry a no_reply_declaration
    (the turn was never accepted); a present declaration fails loudly and
    is never cleared here. Clears job_ref/pre_turn_index/turn_index/
    fingerprint/error; keeps the durable jev_decision, primary source,
    seq aliases, created timestamp and unknown fields, and refreshes the
    updated timestamp. A NEW attempt uses a fresh job_ref. No automatic
    POST is ever triggered.
    """
    file = Path(path)
    if not _is_strict_int(http_status) or http_status != 409:
        raise DeliveryError(f"invalid http_status for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] not in ("turn_submitting", "inflight"):
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} cannot requeue")
    if job["send_intents"] or job["send_receipts"] or job["trace_refs"] is not None:
        raise DeliveryError(f"delivery state {file}: job {key} has send evidence; cannot requeue")
    if job.get("no_reply_declaration") is not None or job.get("observation") is not None:
        raise DeliveryError(f"delivery state {file}: job {key} has no_reply proof; cannot requeue")
    job["job_ref"] = None
    job["pre_turn_index"] = None
    job["turn_index"] = None
    job["submitted_text_sha256"] = None
    job["submitted_text_len"] = None
    job["error"] = None
    job["stage"] = "queued"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def record_send_intent(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    send_ref: str,
    content_len: int,
    content_hash: str,
) -> dict[str, Any]:
    """Record a send intent BEFORE the Discord side effect (-> ``delivering``).

    The caller persists the intent first, then performs the SDK send, then
    records the receipt. Multiple intents per job model multi-chunk replies.
    """
    file = Path(path)
    if _validate_id_string(send_ref) is None:
        raise DeliveryError(f"invalid send_ref for delivery state {file}")
    # Positive scalar length only (the caller owns the Discord send cap of
    # 2000 chars and the <=4000 native turn bound; core stores no budget).
    # content_hash is an exact SHA-256 fingerprint, same as native Trace.
    if not _is_strict_int(content_len) or content_len < 1:
        raise DeliveryError(f"invalid content_len for delivery state {file}")
    if not _is_sha256_hex(content_hash):
        raise DeliveryError(f"invalid content_hash for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    for existing in job["send_intents"]:
        if existing["send_ref"] == send_ref:
            if existing["content_len"] == content_len and existing["content_hash"] == content_hash:
                return copy.deepcopy(job)
            raise DeliveryError(f"delivery state {file}: job {key} send_ref conflict")
    if job["stage"] in TERMINAL_STAGES:
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} is terminal")
    # A declared no-reply forbids any new send: the agent explicitly proved
    # zero-send intent for this nonce; a later send would contradict it.
    if job.get("no_reply_declaration") is not None:
        raise DeliveryError(f"delivery state {file}: job {key} has no_reply proof; cannot send")
    # turn_submitting is allowed: a deferred SDK callback during the
    # synchronous POST await records REAL sends before the bind (stage
    # -> delivering with turn_index still None). No send budget exists.
    if job["stage"] not in ("turn_submitting", "inflight", "delivering", "sent"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot record intent"
        )
    job["send_intents"].append(
        {
            "send_ref": send_ref,
            "content_len": content_len,
            "content_hash": content_hash,
            "ts": _delivery_root._now_iso(),
        }
    )
    job["stage"] = "delivering"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def record_send_receipt(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    send_ref: str,
    sent_message_id: int,
    delivered_channel_id: int,
    jump_url: str,
    reply_to_message_id: int | None,
) -> dict[str, Any]:
    """Record a candidate receipt AFTER SDK success (-> ``sent`` when complete).

    ``channel_id``/``message_id`` are the SOURCE job key;
    ``delivered_channel_id`` is the SDK result and must equal the source
    channel in the auto path. Association by ``send_ref`` is correlation
    only, never authentication.
    """
    file = Path(path)
    if _validate_id_string(send_ref) is None:
        raise DeliveryError(f"invalid send_ref for delivery state {file}")
    if not _is_strict_int(sent_message_id) or sent_message_id <= 0:
        raise DeliveryError(f"invalid sent_message_id for delivery state {file}")
    if not _is_strict_int(delivered_channel_id) or delivered_channel_id <= 0:
        raise DeliveryError(f"invalid delivered_channel_id for delivery state {file}")
    if (
        not isinstance(jump_url, str)
        or not (1 <= len(jump_url) <= MAX_URL_LEN)
        or jump_url.strip() != jump_url
        or _has_whitespace(jump_url)
        or not jump_url.startswith("https://")
    ):
        raise DeliveryError(f"invalid jump_url for delivery state {file}")
    if reply_to_message_id is not None and (
        not _is_strict_int(reply_to_message_id) or reply_to_message_id <= 0
    ):
        raise DeliveryError(f"invalid reply_to_message_id for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if delivered_channel_id != job["channel_id"]:
        raise DeliveryError(f"delivery state {file}: job {key} delivered_channel_id mismatch")
    for existing in job["send_receipts"]:
        if existing["send_ref"] == send_ref:
            if (
                existing["sent_message_id"] == sent_message_id
                and existing["delivered_channel_id"] == delivered_channel_id
                and existing["jump_url"] == jump_url
                and existing["reply_to_message_id"] == reply_to_message_id
            ):
                return copy.deepcopy(job)
            raise DeliveryError(f"delivery state {file}: job {key} receipt conflict")
    if job["stage"] in ("completed", "skipped", "observed"):
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} is terminal")
    intent_refs = {item["send_ref"] for item in job["send_intents"]}
    if send_ref not in intent_refs:
        # A non-matching ref can never create a receipt, on any stage.
        raise DeliveryError(f"delivery state {file}: job {key} has no intent for receipt")
    receipt = {
        "send_ref": send_ref,
        "sent_message_id": sent_message_id,
        "delivered_channel_id": delivered_channel_id,
        "jump_url": jump_url,
        "reply_to_message_id": reply_to_message_id,
        "ts": _delivery_root._now_iso(),
    }
    if job["stage"] == "uncertain":
        # Late SDK receipt for an EXISTING pending intent after UNCERTAIN:
        # keep stage + error untouched (never auto-sent/completed). Not
        # persisting this MID would lose the only send evidence.
        job["send_receipts"].append(receipt)
        job["updated_at"] = _delivery_root._now_iso()
        _delivery_root._save_state(file, state)
        return copy.deepcopy(job)
    if job["stage"] not in ("delivering", "sent"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot record receipt"
        )
    job["send_receipts"].append(receipt)
    receipt_refs = {item["send_ref"] for item in job["send_receipts"]}
    if intent_refs and intent_refs == receipt_refs:
        job["stage"] = "sent"
    else:
        job["stage"] = "delivering"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def complete_job(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    turn_index: int,
    tool_call_ids: list[str],
) -> dict[str, Any]:
    """Complete after the caller verified the native Trace (own SID/turn).

    ``sent`` -> ``completed`` requires ALL send intents receipted and a
    bound matching ``turn_index``. The explicit ``uncertain`` -> completed
    path additionally requires ALL intents receipted (late receipts
    included), a bound matching native ``turn_index``, and NON-EMPTY
    verified native tool_call_ids (the caller's proof precondition; core
    checks ID format and matching only, no caller ACL). Completing from
    ``uncertain`` clears the stored error. Two confirmed + one pending
    intent never completes.
    """
    file = Path(path)
    if not _is_strict_int(turn_index) or turn_index < 0:
        raise DeliveryError(f"invalid turn_index for delivery state {file}")
    if not isinstance(tool_call_ids, list):
        raise DeliveryError(f"invalid tool_call_ids for delivery state {file}")
    for tool_id in tool_call_ids:
        if _validate_id_string(tool_id) is None:
            raise DeliveryError(f"invalid tool_call_id for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "completed":
        existing = job["trace_refs"] or {}
        if job["turn_index"] == turn_index and existing.get("tool_call_ids") == tool_call_ids:
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} completed payload mismatch")
    if job["stage"] not in ("sent", "uncertain"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot complete"
        )
    intent_refs = {item["send_ref"] for item in job["send_intents"]}
    receipt_refs = {item["send_ref"] for item in job["send_receipts"]}
    if not intent_refs or intent_refs != receipt_refs:
        raise DeliveryError(f"delivery state {file}: job {key} has unconfirmed sends")
    if job["turn_index"] != turn_index:
        raise DeliveryError(f"delivery state {file}: job {key} turn_index mismatch")
    if job["stage"] == "uncertain" and not tool_call_ids:
        # Explicit recovery proof: the caller verified non-empty native
        # tool call IDs on the own Trace; core checks IDs/matching only.
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot complete"
        )
    job["trace_refs"] = {"tool_call_ids": list(tool_call_ids), "ts": _delivery_root._now_iso()}
    job["error"] = None
    job["stage"] = "completed"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def skip_job(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    reason: str,
) -> dict[str, Any]:
    """Move any non-terminal job -> ``skipped`` with an explicit reason.

    ``source_deleted`` is the proven-no-send gate: it is refused whenever
    the freshly reloaded job shows ANY send evidence (intents, receipts,
    or trace refs) or an ``uncertain``/``completed`` stage, so a partly
    delivered job can never be terminalized as "no send". The idempotent
    repeat stays valid only with the same reason and zero evidence. All
    other reasons keep their approved behavior. Only stage/skip_reason/
    updated_at change; job_ref, fingerprints, aliases, timestamps, and
    unknown fields are preserved.
    """
    file = Path(path)
    if not isinstance(reason, str) or reason not in SKIP_REASONS:
        raise DeliveryError(f"invalid skip reason for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if reason == "source_deleted" and (
        job["send_intents"] or job["send_receipts"] or job["trace_refs"] is not None
    ):
        raise DeliveryError(f"delivery state {file}: job {key} has send evidence; cannot skip")
    if job["stage"] == "skipped":
        if job["skip_reason"] == reason:
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} skip reason mismatch")
    if job["stage"] in ("completed", "uncertain", "observed"):
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} cannot skip")
    job["skip_reason"] = reason
    job["stage"] = "skipped"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def resolve_uncertain_drop(
    path: str | Path,
    channel_id: int,
    message_id: int,
) -> dict[str, Any]:
    """Operator-drop a stuck ``uncertain`` job with zero send evidence.

    Moves ``uncertain`` -> ``skipped`` with reason ``operator_approved_drop``
    and appends the matching ``authorized_skips`` record (same tuple the
    schema validates: seq/author/actor/reason). This is the ONLY write path
    for an uncertain job that is not ``complete_job``: it exists because a
    responded turn can end with zero verifiable sends (nothing to receipt,
    nothing to declare), leaving reconcile with no forward transition and
    the FIFO head-blocked behind it.

    Preconditions (fresh load, fail closed, never silently repaired): the
    job exists and is ``uncertain``; send intents, receipts, trace refs,
    observation, and no-reply declaration are ALL empty (any evidence
    refuses -- a partly delivered job must never drop as no-send). Only
    stage/skip_reason/error/updated_at change plus one authorized-skips
    append; job_ref, fingerprints, aliases, Jev proof, and unknown fields
    are preserved. Repeat on an already-dropped job returns the stored
    copy only when the authorized record still matches exactly.
    """
    file = Path(path)
    if not _is_strict_int(channel_id) or channel_id <= 0:
        raise DeliveryError(f"invalid channel_id for delivery state {file}")
    if not _is_strict_int(message_id) or message_id <= 0:
        raise DeliveryError(f"invalid message_id for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "skipped" and job["skip_reason"] == "operator_approved_drop":
        skips = state.get("authorized_skips") or []
        if any(
            isinstance(item, dict)
            and item.get("channel_id") == channel_id
            and item.get("message_id") == message_id
            and item.get("seq") == job["seq"]
            and item.get("author_uid") == job["author_uid"]
            and item.get("actor_kind") == job["actor_kind"]
            and item.get("reason") == "operator_approved_drop"
            for item in skips
        ):
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} drop record mismatch")
    if job["stage"] != "uncertain":
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot operator-drop"
        )
    if (
        job["send_intents"]
        or job["send_receipts"]
        or job["trace_refs"] is not None
        or job["observation"] is not None
        or job["no_reply_declaration"] is not None
    ):
        raise DeliveryError(f"delivery state {file}: job {key} has send evidence; cannot drop")
    job["stage"] = "skipped"
    job["skip_reason"] = "operator_approved_drop"
    job["error"] = None
    job["updated_at"] = _delivery_root._now_iso()
    record = {
        "channel_id": channel_id,
        "message_id": message_id,
        "seq": job["seq"],
        "author_uid": job["author_uid"],
        "actor_kind": job["actor_kind"],
        "reason": "operator_approved_drop",
    }
    skips = state.get("authorized_skips")
    if not isinstance(skips, list):
        raise DeliveryError(f"delivery state {file}: authorized_skips must be a list")
    skips.append(record)
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def mark_uncertain(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    kind: str,
    http_status: int | None = None,
) -> dict[str, Any]:
    """Move any non-terminal job -> ``uncertain`` (terminal, human review)."""
    file = Path(path)
    if not isinstance(kind, str) or kind not in UNCERTAIN_KINDS:
        raise DeliveryError(f"invalid uncertain kind for delivery state {file}")
    if http_status is not None and (
        not _is_strict_int(http_status) or not (100 <= http_status <= 599)
    ):
        raise DeliveryError(f"invalid http_status for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "uncertain":
        existing = job["error"] or {}
        if existing.get("kind") == kind and existing.get("http_status") == http_status:
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} uncertain payload mismatch")
    if job["stage"] in ("completed", "skipped", "observed"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot mark uncertain"
        )
    job["error"] = {"kind": kind, "http_status": http_status, "ts": _delivery_root._now_iso()}
    job["stage"] = "uncertain"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def bind_jev_decision(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    context_sha256: str,
    attempt_id: str,
    model: str,
    participation: str,
) -> dict[str, Any]:
    """Bind a compact Jev classification to a fresh ``queued`` job.

    Stores only ``{participation, context_sha256, attempt_id, model}`` --
    never raw answers, probabilities, or message text. ``context_sha256``
    is the exact 64-hex canonical-context fingerprint, ``attempt_id`` an
    exact 32-hex UUID hex, ``model`` exactly ``jev-1.13.0``, and
    ``participation`` one of ``respond``/``observe``/``need_context``.
    Only a fresh queued job with zero evidence (no nonce/index/
    fingerprint/intents/receipts/trace/declaration/observation) can bind;
    an exact repeat is idempotent, any different payload conflicts.
    """
    file = Path(path)
    if not _is_sha256_hex(context_sha256):
        raise DeliveryError(f"invalid context_sha256 for delivery state {file}")
    if not _is_attempt_id(attempt_id):
        raise DeliveryError(f"invalid attempt_id for delivery state {file}")
    if not isinstance(model, str) or model != JEV_MODEL_PINNED:
        raise DeliveryError(f"invalid jev model for delivery state {file}")
    if not isinstance(participation, str) or participation not in JEV_PARTICIPATIONS:
        raise DeliveryError(f"invalid jev participation for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] != "queued":
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot bind jev"
        )
    if (
        job["job_ref"] is not None
        or job["pre_turn_index"] is not None
        or job["turn_index"] is not None
        or job["submitted_text_sha256"] is not None
        or job["submitted_text_len"] is not None
        or job["send_intents"]
        or job["send_receipts"]
        or job["trace_refs"] is not None
        or job.get("no_reply_declaration") is not None
        or job.get("observation") is not None
    ):
        raise DeliveryError(f"delivery state {file}: job {key} is not fresh; cannot bind jev")
    existing = job.get("jev_decision")
    if existing is not None:
        if (
            existing.get("participation") == participation
            and existing.get("context_sha256") == context_sha256
            and existing.get("attempt_id") == attempt_id
            and existing.get("model") == model
        ):
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} jev decision conflict")
    job["jev_decision"] = {
        "participation": participation,
        "context_sha256": context_sha256,
        "attempt_id": attempt_id,
        "model": model,
    }
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def observe_job(
    path: str | Path,
    channel_id: int,
    message_id: int,
) -> dict[str, Any]:
    """Terminalize a Jev-``observe`` queued job as auditable zero-send.

    Only a ``queued`` job with a bound Jev ``observe`` decision and zero
    nonce/index/fingerprint/intents/receipts/trace/declaration can observe.
    Atomically sets stage ``observed`` plus a durable
    ``observation={origin: jev, ts}``. An already-observed Jev job is
    idempotent; any other stage, a non-observe decision, or ANY send
    evidence fails. Never infers from receipt absence: the bound Jev proof
    plus zero evidence is the proof.
    """
    file = Path(path)
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "observed":
        obs = job.get("observation") or {}
        if obs.get("origin") == "jev":
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} observed origin mismatch")
    if job["stage"] != "queued":
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} cannot observe")
    jev = job.get("jev_decision")
    if jev is None or jev.get("participation") != "observe":
        raise DeliveryError(f"delivery state {file}: job {key} has no jev observe proof")
    if (
        job["job_ref"] is not None
        or job["pre_turn_index"] is not None
        or job["turn_index"] is not None
        or job["submitted_text_sha256"] is not None
        or job["submitted_text_len"] is not None
        or job["send_intents"]
        or job["send_receipts"]
        or job["trace_refs"] is not None
        or job.get("no_reply_declaration") is not None
        or job.get("observation") is not None
    ):
        raise DeliveryError(f"delivery state {file}: job {key} has evidence; cannot observe")
    job["observation"] = {"origin": "jev", "ts": _delivery_root._now_iso()}
    job["stage"] = "observed"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def record_no_reply(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    job_ref: str,
    reason: str,
) -> dict[str, Any]:
    """Record a midflight Thyca no-reply declaration (no terminal, no ACK).

    Only a ``turn_submitting``/``inflight`` job with a bound Jev
    ``need_context`` decision, an exact matching nonce, a KNOWN stored
    first-user fingerprint (exact prompt.strip SHA-256 + char len from
    the real Router POST), and zero intents/receipts/trace can declare.
    ``reason`` is ``no_reply_needed``/``missing_context``. The stored
    ``{job_ref, reason, ts}`` is immutable and idempotent; it does NOT
    terminalize or ACK -- only :func:`complete_observation` with traced
    tool proof does. A present declaration forbids any new send intent.
    """
    file = Path(path)
    if _validate_id_string(job_ref) is None:
        raise DeliveryError(f"invalid job_ref for delivery state {file}")
    if not isinstance(reason, str) or reason not in NO_REPLY_REASONS:
        raise DeliveryError(f"invalid no_reply reason for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    existing = job.get("no_reply_declaration")
    if existing is not None:
        if existing.get("job_ref") == job_ref and existing.get("reason") == reason:
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} no_reply conflict")
    if job["stage"] not in ("turn_submitting", "inflight"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot declare no_reply"
        )
    jev = job.get("jev_decision")
    if jev is None or jev.get("participation") != "need_context":
        raise DeliveryError(f"delivery state {file}: job {key} has no need_context proof")
    if job["job_ref"] is None or job["job_ref"] != job_ref:
        raise DeliveryError(f"delivery state {file}: job {key} job_ref mismatch")
    # NEW zero-send proof: the declaring POST must have persisted its
    # first-user fingerprint (all real Router POSTs do). Missing/zero/
    # invalid binding can never declare, terminalize, or ACK.
    _sub_sha = job.get("submitted_text_sha256")
    _sub_len = job.get("submitted_text_len")
    if not (
        _is_sha256_hex(_sub_sha)
        and _is_strict_int(_sub_len)
        and 1 <= _sub_len <= SUBMITTED_TEXT_MAX
    ):
        raise DeliveryError(f"delivery state {file}: job {key} has no submitted proof")
    if job["send_intents"] or job["send_receipts"] or job["trace_refs"] is not None:
        raise DeliveryError(f"delivery state {file}: job {key} has send evidence; cannot declare")
    if job.get("observation") is not None:
        raise DeliveryError(f"delivery state {file}: job {key} already observed")
    job["no_reply_declaration"] = {
        "job_ref": job_ref,
        "reason": reason,
        "ts": _delivery_root._now_iso(),
    }
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def complete_observation(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    turn_index: int,
    tool_call_ids: list[str],
) -> dict[str, Any]:
    """Complete a declared no-reply after traced ``record_no_reply`` proof.

    Only an ``inflight`` job (or a zero-evidence ``uncertain`` job) with a
    bound Jev ``need_context`` decision, an exact matching declaration
    nonce, a KNOWN stored first-user fingerprint (exact prompt.strip
    SHA-256 + char len), zero intents/receipts, and a bound matching
    ``turn_index`` can complete. ``tool_call_ids`` must be nonempty
    unique valid IDs from the caller's verified native Trace (core checks
    format/matching only). Stores ``trace_refs`` plus
    ``observation={origin: thyca}``, sets stage ``observed``, and clears
    an uncertain error. Existing ambiguous uncertain jobs without an
    exact declaration proof are never touched. Unlike legacy completed,
    this NEW terminal has no None-fingerprint compat: missing/zero/
    invalid binding never terminals/ACKs.
    """
    file = Path(path)
    if not _is_strict_int(turn_index) or turn_index < 0:
        raise DeliveryError(f"invalid turn_index for delivery state {file}")
    if not isinstance(tool_call_ids, list) or not tool_call_ids:
        raise DeliveryError(f"invalid tool_call_ids for delivery state {file}")
    for tool_id in tool_call_ids:
        if _validate_id_string(tool_id) is None:
            raise DeliveryError(f"invalid tool_call_id for delivery state {file}")
    if len(set(tool_call_ids)) != len(tool_call_ids):
        raise DeliveryError(f"invalid tool_call_ids for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "observed":
        obs = job.get("observation") or {}
        existing = job.get("trace_refs") or {}
        if (
            obs.get("origin") == "thyca"
            and job["turn_index"] == turn_index
            and existing.get("tool_call_ids") == list(tool_call_ids)
        ):
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} observed payload mismatch")
    if job["stage"] not in ("inflight", "uncertain"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot complete observation"
        )
    jev = job.get("jev_decision")
    if jev is None or jev.get("participation") != "need_context":
        raise DeliveryError(f"delivery state {file}: job {key} has no need_context proof")
    decl = job.get("no_reply_declaration")
    if decl is None or job["job_ref"] is None or decl.get("job_ref") != job["job_ref"]:
        raise DeliveryError(f"delivery state {file}: job {key} has no no_reply proof")
    # NEW terminal proof: stored first-user fingerprint must be present
    # and valid (no legacy None compat). Structurally missing binding
    # never terminals/ACKs even when SID/index/trace were verified.
    _csub_sha = job.get("submitted_text_sha256")
    _csub_len = job.get("submitted_text_len")
    if not (
        _is_sha256_hex(_csub_sha)
        and _is_strict_int(_csub_len)
        and 1 <= _csub_len <= SUBMITTED_TEXT_MAX
    ):
        raise DeliveryError(f"delivery state {file}: job {key} has no submitted proof")
    if job["send_intents"] or job["send_receipts"] or job["trace_refs"] is not None:
        raise DeliveryError(f"delivery state {file}: job {key} has send evidence; cannot observe")
    if job["turn_index"] != turn_index:
        raise DeliveryError(f"delivery state {file}: job {key} turn_index mismatch")
    if job.get("observation") is not None:
        raise DeliveryError(f"delivery state {file}: job {key} already observed")
    job["trace_refs"] = {"tool_call_ids": list(tool_call_ids), "ts": _delivery_root._now_iso()}
    job["observation"] = {"origin": "thyca", "ts": _delivery_root._now_iso()}
    job["error"] = None
    job["stage"] = "observed"
    job["updated_at"] = _delivery_root._now_iso()
    _delivery_root._save_state(file, state)
    return copy.deepcopy(job)


def rotate_native_session(
    path: str | Path,
    *,
    expected_current: str,
    new_sid: str,
    day: str,
    history: dict[str, str],
) -> dict[str, Any]:
    """Adopt a new daily native session; drain-gated, provenance-preserving.

    Daily-rotation commit for the delivery journal. Loads FRESH state and
    requires, in order:

    1. ``new_sid`` is a strict native SID and ``day`` a real calendar day;
       ``history`` is a valid day -> SID map with ``history[day] ==
       new_sid`` and ``expected_current`` among its values (the outgoing
       SID stays in provenance; never silently dropped).
    2. The stored ``native_session_id`` equals ``expected_current``;
       anything else (including an already-rotated file) fails loudly --
       partial multi-file states never silently resume, they fail closed
       for operator recovery.
    3. EVERY job is safe-terminal (completed/skipped/observed): a queued,
       mid-flight, or uncertain job refuses rotation, because its future
       reconcile needs the CURRENT session's native trace.

    On success the top-level ``native_session_id`` becomes ``new_sid``
    and ``native_sessions`` becomes ``history``; jobs are untouched.
    Never touches ACK, markers, or evidence. No network, no clock.
    """
    file = Path(path)
    if sessions.validate_native_sid(new_sid) is None:
        raise DeliveryError(f"invalid new_sid for delivery state {file}")
    if sessions.validate_day(day) is None:
        raise DeliveryError(f"invalid day for delivery state {file}")
    clean_history = sessions.validate_sessions_history(history)
    if clean_history is None:
        raise DeliveryError(f"invalid history for delivery state {file}")
    if clean_history.get(day) != new_sid:
        raise DeliveryError(f"delivery state {file}: history does not bind day to new_sid")
    if not isinstance(expected_current, str) or not expected_current:
        raise DeliveryError(f"invalid expected_current for delivery state {file}")
    if expected_current not in clean_history.values():
        raise DeliveryError(f"delivery state {file}: history drops the current session")
    state = _load_state(file)
    if state.get("native_session_id") != expected_current:
        raise DeliveryError(f"delivery state {file}: native session mismatch")
    for key, job in state["jobs"].items():
        if job.get("stage") not in SAFE_TERMINAL_STAGES:
            raise DeliveryError(
                f"delivery state {file}: job {key} stage {job.get('stage')} blocks rotation"
            )
    state["native_session_id"] = new_sid
    state["native_sessions"] = clean_history
    _delivery_root._save_state(file, state)
    return copy.deepcopy(state)
