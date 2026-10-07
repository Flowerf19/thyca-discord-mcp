"""Delivery schema: constants, errors, and pure state validators.

Cycle-free owner of the delivery file format: public constants, error
types, and side-effect-free validation/parsing helpers. No file I/O and
no import of ``delivery`` or ``delivery_storage`` (errors live here so
the schema never cycles through a facade).

Owns the live format including ``observed``/``SAFE_TERMINAL_STAGES``,
the top-level ``jev_ledger_id`` marker, Jev bindings, no-reply
declarations, and observation proofs. Any non-null new declaration
requires a non-null valid stored first-user fingerprint; legacy files
without the new keys load those fields as None. Unknown top-level and
job keys are preserved.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import sessions

DELIVERY_VERSION = 1

STAGES = frozenset(
    {
        "queued",
        "turn_submitting",
        "inflight",
        "delivering",
        "sent",
        "completed",
        "skipped",
        "uncertain",
        "observed",
    }
)
TERMINAL_STAGES = frozenset({"completed", "skipped", "uncertain", "observed"})
# Safe ACK terminals: completed/skipped/observed advance the processed
# prefix. Uncertain stays terminal but never ACKs (human review).
SAFE_TERMINAL_STAGES = frozenset({"completed", "skipped", "observed"})
ACTOR_KINDS = frozenset({"owner", "other", "self"})
SKIP_REASONS = frozenset(
    {
        "operator_approved_drop",
        "out_of_scope",
        "bot_filtered",
        "duplicate_superseded",
        "source_deleted",
    }
)
UNCERTAIN_KINDS = frozenset(
    {
        "timeout",
        "busy",
        "http_error",
        "transport_error",
        "trace_unverified",
        "delivery_unknown",
    }
)
# Jev classification binding (compact metadata only, never raw answers).
JEV_MODEL_PINNED = "jev-1.13.0"
JEV_PARTICIPATIONS = frozenset({"respond", "observe", "need_context"})
# No-reply declaration reasons (Thyca explicit no-send, midflight only).
NO_REPLY_REASONS = frozenset({"no_reply_needed", "missing_context"})
# Observation proof origins: Jev direct observe vs Thyca traced no-reply.
OBSERVATION_ORIGINS = frozenset({"jev", "thyca"})

MAX_ID_LEN = 128
MAX_URL_LEN = 512
# SHA-256 hex fingerprint length for content_hash and submitted_text_sha256.
SHA256_HEX_LEN = 64
# Native turn text bound (mirrors native TEXT_MAX). Core only validates the
# stored fingerprint length; the caller enforces the Discord/native limit
# before POST and never truncates.
SUBMITTED_TEXT_MAX = 4000


class DeliveryError(ValueError):
    """Corrupt delivery state or an invalid delivery operation."""


class DeliveryDurabilityUncertain(DeliveryError):
    """Post-commit directory fsync failed; namespace durability unknown.

    Raised only AFTER os.replace/os.link succeeded. The new state file
    exists and validates, but the directory entry may not survive power
    loss. committed=True always. Never carries raw OSError text. Callers
    must not blind-retry, rewind, or delete the committed file; operator
    reconciliation is required.
    """

    def __init__(self, path: Path | str) -> None:
        self.kind = "durability_uncertain"
        self.committed = True
        super().__init__(f"delivery state {path}: commit succeeded but directory fsync failed")


def _is_strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _job_key(channel_id: int, message_id: int) -> str:
    return f"{channel_id}:{message_id}"


def _parse_job_key(key: Any) -> tuple[int, int] | None:
    if not isinstance(key, str):
        return None
    parts = key.split(":")
    if len(parts) != 2:
        return None
    try:
        channel_id = int(parts[0])
        message_id = int(parts[1])
    except (TypeError, ValueError):
        return None
    if channel_id <= 0 or message_id <= 0:
        return None
    # Canonical form only: no leading zeros / whitespace.
    if parts[0] != str(channel_id) or parts[1] != str(message_id):
        return None
    return channel_id, message_id


def _is_hex(value: str) -> bool:
    if not value:
        return False
    for ch in value:
        if ch not in "0123456789abcdefABCDEF":
            return False
    return True


def _has_whitespace(value: str) -> bool:
    for ch in value:
        if ch.isspace():
            return True
    return False


def _validate_id_string(value: Any) -> str | None:
    """Non-empty bounded ID without whitespace; None when invalid (no echo)."""
    if not isinstance(value, str):
        return None
    if not (1 <= len(value) <= MAX_ID_LEN):
        return None
    if value.strip() != value or not value:
        return None
    if _has_whitespace(value):
        return None
    return value


def _is_sha256_hex(value: Any) -> bool:
    """Exact 64-char hex (SHA-256 fingerprint); never echoes the value."""
    if not isinstance(value, str):
        return False
    if len(value) != SHA256_HEX_LEN:
        return False
    return _is_hex(value)


def _is_attempt_id(value: Any) -> bool:
    """Exact 32-char UUID hex (no dashes); never echoes the value."""
    if not isinstance(value, str):
        return False
    if len(value) != 32:
        return False
    return _is_hex(value)


def _submitted_pair_ok(sha: Any, length: Any) -> bool:
    """True when the fingerprint pair is both-None or both valid.

    A valid pair is an exact 64-hex SHA-256 with a strict positive length
    of at most SUBMITTED_TEXT_MAX characters (native turn bound).
    """
    if sha is None and length is None:
        return True
    if not _is_sha256_hex(sha):
        return False
    return _is_strict_int(length) and 1 <= length <= SUBMITTED_TEXT_MAX


def _validate_historical(raw: Any, path: Path) -> dict[int, list[int]]:
    if not isinstance(raw, dict):
        raise DeliveryError(
            f"corrupt delivery state {path}: 'historical_message_ids' must be an object"
        )
    out: dict[int, list[int]] = {}
    for key, value in raw.items():
        if isinstance(key, bool):
            raise DeliveryError(f"corrupt delivery state {path}: bad historical channel")
        if isinstance(key, int):
            channel_id = key
        elif isinstance(key, str):
            try:
                channel_id = int(key)
            except (TypeError, ValueError):
                raise DeliveryError(
                    f"corrupt delivery state {path}: bad historical channel"
                ) from None
            if key != str(channel_id):
                raise DeliveryError(f"corrupt delivery state {path}: bad historical channel")
        else:
            raise DeliveryError(f"corrupt delivery state {path}: bad historical channel")
        if not _is_strict_int(channel_id) or channel_id <= 0:
            raise DeliveryError(f"corrupt delivery state {path}: bad historical channel")
        if channel_id in out:
            raise DeliveryError(f"corrupt delivery state {path}: duplicate historical channel")
        if not isinstance(value, list):
            raise DeliveryError(f"corrupt delivery state {path}: bad historical id list")
        mids: list[int] = []
        seen: set[int] = set()
        for item in value:
            if not _is_strict_int(item) or item <= 0:
                raise DeliveryError(f"corrupt delivery state {path}: bad historical message id")
            if item in seen:
                raise DeliveryError(
                    f"corrupt delivery state {path}: duplicate historical message id"
                )
            seen.add(item)
            mids.append(item)
        out[channel_id] = mids
    return out


def _validate_authorized_skip(
    raw: Any, path: Path, baseline_acked: int, index: int
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise DeliveryError(
            f"corrupt delivery state {path}: authorized skip #{index} must be an object"
        )
    for name in (
        "channel_id",
        "message_id",
        "seq",
        "author_uid",
        "actor_kind",
        "reason",
    ):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: authorized skip #{index} missing key {name!r}"
            )
    record = dict(raw)
    for name in ("channel_id", "message_id", "seq", "author_uid"):
        value = raw[name]
        if not _is_strict_int(value) or value <= 0:
            raise DeliveryError(
                f"corrupt delivery state {path}: authorized skip #{index} "
                f"field {name!r} must be a positive int"
            )
        record[name] = value
    if record["seq"] <= baseline_acked:
        raise DeliveryError(
            f"corrupt delivery state {path}: authorized skip #{index} "
            "seq must exceed baseline_acked"
        )
    if not isinstance(raw["actor_kind"], str) or raw["actor_kind"] not in ACTOR_KINDS:
        raise DeliveryError(
            f"corrupt delivery state {path}: authorized skip #{index} has invalid actor_kind"
        )
    record["actor_kind"] = raw["actor_kind"]
    if not isinstance(raw["reason"], str) or raw["reason"] not in SKIP_REASONS:
        raise DeliveryError(
            f"corrupt delivery state {path}: authorized skip #{index} has invalid reason"
        )
    record["reason"] = raw["reason"]
    return record


def _validate_intent(raw: Any, path: Path, key: str, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} intent #{index} must be an object"
        )
    for name in ("send_ref", "content_len", "content_hash", "ts"):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} intent #{index} missing key {name!r}"
            )
    item = dict(raw)
    if _validate_id_string(raw["send_ref"]) is None:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} intent #{index} has invalid send_ref"
        )
    item["send_ref"] = raw["send_ref"]
    content_len = raw["content_len"]
    # Technical scalar: strict positive int only. No business cap; the
    # Discord/native 2000-char send limit is enforced by the caller.
    if not _is_strict_int(content_len) or content_len < 1:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} intent #{index} has invalid content_len"
        )
    item["content_len"] = content_len
    content_hash = raw["content_hash"]
    if not _is_sha256_hex(content_hash):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} intent #{index} has invalid content_hash"
        )
    item["content_hash"] = content_hash
    if not isinstance(raw["ts"], str) or not raw["ts"]:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} intent #{index} has invalid ts"
        )
    item["ts"] = raw["ts"]
    return item


def _validate_receipt(
    raw: Any, path: Path, key: str, index: int, job_channel_id: int
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} must be an object"
        )
    for name in (
        "send_ref",
        "sent_message_id",
        "delivered_channel_id",
        "jump_url",
        "reply_to_message_id",
        "ts",
    ):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} receipt #{index} missing key {name!r}"
            )
    item = dict(raw)
    if _validate_id_string(raw["send_ref"]) is None:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} has invalid send_ref"
        )
    item["send_ref"] = raw["send_ref"]
    sent_mid = raw["sent_message_id"]
    if not _is_strict_int(sent_mid) or sent_mid <= 0:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} has invalid sent_message_id"
        )
    item["sent_message_id"] = sent_mid
    delivered = raw["delivered_channel_id"]
    if not _is_strict_int(delivered) or delivered <= 0:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} "
            "has invalid delivered_channel_id"
        )
    if delivered != job_channel_id:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} "
            "delivered_channel_id mismatch"
        )
    item["delivered_channel_id"] = delivered
    jump_url = raw["jump_url"]
    if (
        not isinstance(jump_url, str)
        or not (1 <= len(jump_url) <= MAX_URL_LEN)
        or jump_url.strip() != jump_url
        or _has_whitespace(jump_url)
        or not jump_url.startswith("https://")
    ):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} has invalid jump_url"
        )
    item["jump_url"] = jump_url
    reply_to = raw["reply_to_message_id"]
    if reply_to is not None and (not _is_strict_int(reply_to) or reply_to <= 0):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} "
            "has invalid reply_to_message_id"
        )
    item["reply_to_message_id"] = reply_to
    if not isinstance(raw["ts"], str) or not raw["ts"]:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} receipt #{index} has invalid ts"
        )
    item["ts"] = raw["ts"]
    return item


def _validate_jev_decision(raw: Any, path: Path, key: str) -> dict[str, Any] | None:
    """Optional Jev binding; None when absent (old files stay valid)."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid jev_decision")
    for name in ("participation", "context_sha256", "attempt_id", "model"):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} jev_decision missing key {name!r}"
            )
    if not isinstance(raw["participation"], str) or raw["participation"] not in JEV_PARTICIPATIONS:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has invalid jev participation"
        )
    if not _is_sha256_hex(raw["context_sha256"]):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid jev context")
    if not _is_attempt_id(raw["attempt_id"]):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid jev attempt")
    if not isinstance(raw["model"], str) or raw["model"] != JEV_MODEL_PINNED:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid jev model")
    return dict(raw)


def _validate_no_reply_declaration(raw: Any, path: Path, key: str) -> dict[str, Any] | None:
    """Optional midflight no-reply marker; None when absent."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has invalid no_reply_declaration"
        )
    for name in ("job_ref", "reason", "ts"):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply missing key {name!r}"
            )
    if _validate_id_string(raw["job_ref"]) is None:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has invalid no_reply job_ref"
        )
    if not isinstance(raw["reason"], str) or raw["reason"] not in NO_REPLY_REASONS:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid no_reply reason")
    if not isinstance(raw["ts"], str) or not raw["ts"]:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid no_reply ts")
    return dict(raw)


def _validate_observation(raw: Any, path: Path, key: str) -> dict[str, Any] | None:
    """Optional observed proof; None when absent (non-observed stages)."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid observation")
    for name in ("origin", "ts"):
        if name not in raw:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} observation missing key {name!r}"
            )
    if not isinstance(raw["origin"], str) or raw["origin"] not in OBSERVATION_ORIGINS:
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has invalid observation origin"
        )
    if not isinstance(raw["ts"], str) or not raw["ts"]:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid observation ts")
    return dict(raw)


def _validate_job(raw: Any, key: str, path: Path) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} must be an object")
    required = (
        "channel_id",
        "message_id",
        "author_uid",
        "actor_kind",
        "seq",
        "seq_aliases",
        "stage",
        "job_ref",
        "pre_turn_index",
        "turn_index",
        "send_intents",
        "send_receipts",
        "trace_refs",
        "skip_reason",
        "error",
        "created_at",
        "updated_at",
    )
    for name in required:
        if name not in raw:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} missing key {name!r}")
    job = dict(raw)
    for name in ("channel_id", "message_id", "author_uid", "seq"):
        value = raw[name]
        if not _is_strict_int(value) or value <= 0:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} field {name!r} must be a positive int"
            )
        job[name] = value
    if key != _job_key(job["channel_id"], job["message_id"]):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} key mismatch")
    if not isinstance(raw["actor_kind"], str) or raw["actor_kind"] not in ACTOR_KINDS:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid actor_kind")
    job["actor_kind"] = raw["actor_kind"]
    aliases = raw["seq_aliases"]
    if not isinstance(aliases, list):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid seq_aliases")
    seen_alias: set[int] = set()
    clean_aliases: list[int] = []
    for item in aliases:
        if not _is_strict_int(item) or item <= 0:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid seq_alias")
        if item == job["seq"] or item in seen_alias:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has duplicate seq_alias")
        seen_alias.add(item)
        clean_aliases.append(item)
    job["seq_aliases"] = clean_aliases
    stage = raw["stage"]
    if not isinstance(stage, str) or stage not in STAGES:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid stage")
    job["stage"] = stage
    job_ref = raw["job_ref"]
    if job_ref is not None and _validate_id_string(job_ref) is None:
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid job_ref")
    job["job_ref"] = job_ref
    for name in ("pre_turn_index", "turn_index"):
        value = raw[name]
        if value is not None and (not _is_strict_int(value) or value < 0):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} field {name!r} "
                "must be a non-negative int or null"
            )
        job[name] = value
    intents_raw = raw["send_intents"]
    if not isinstance(intents_raw, list):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid send_intents")
    intents = [_validate_intent(item, path, key, i) for i, item in enumerate(intents_raw)]
    intent_refs = [item["send_ref"] for item in intents]
    if len(set(intent_refs)) != len(intent_refs):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has duplicate send_ref")
    job["send_intents"] = intents
    receipts_raw = raw["send_receipts"]
    if not isinstance(receipts_raw, list):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid send_receipts")
    receipts = [
        _validate_receipt(item, path, key, i, job["channel_id"])
        for i, item in enumerate(receipts_raw)
    ]
    receipt_refs = [item["send_ref"] for item in receipts]
    if len(set(receipt_refs)) != len(receipt_refs):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has duplicate receipt send_ref"
        )
    for ref in receipt_refs:
        if ref not in set(intent_refs):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} has receipt without intent"
            )
    job["send_receipts"] = receipts
    trace = raw["trace_refs"]
    if trace is not None:
        if not isinstance(trace, dict):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid trace_refs")
        if "tool_call_ids" not in trace or "ts" not in trace:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} trace_refs missing key")
        tool_ids = trace["tool_call_ids"]
        if not isinstance(tool_ids, list):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} has invalid tool_call_ids"
            )
        for tool_id in tool_ids:
            if _validate_id_string(tool_id) is None:
                raise DeliveryError(
                    f"corrupt delivery state {path}: job {key} has invalid tool_call_id"
                )
        if not isinstance(trace["ts"], str) or not trace["ts"]:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid trace ts")
        job["trace_refs"] = dict(trace)
    else:
        job["trace_refs"] = None
    skip_reason = raw["skip_reason"]
    if skip_reason is not None and (
        not isinstance(skip_reason, str) or skip_reason not in SKIP_REASONS
    ):
        raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid skip_reason")
    job["skip_reason"] = skip_reason
    error = raw["error"]
    if error is not None:
        if not isinstance(error, dict):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid error")
        if "kind" not in error or "http_status" not in error or "ts" not in error:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} error missing key")
        if not isinstance(error["kind"], str) or error["kind"] not in UNCERTAIN_KINDS:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid error kind")
        status = error["http_status"]
        if status is not None and (not _is_strict_int(status) or not (100 <= status <= 599)):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid http_status")
        if not isinstance(error["ts"], str) or not error["ts"]:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid error ts")
        job["error"] = dict(error)
    else:
        job["error"] = None
    for name in ("created_at", "updated_at"):
        if not isinstance(raw[name], str) or not raw[name]:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} has invalid {name}")
        job[name] = raw[name]
    # Optional submission fingerprint (exact Native POST text hash/len, never raw).
    # Missing keys mean an older state file: treat as None for compatibility.
    sub_sha = raw.get("submitted_text_sha256")
    sub_len = raw.get("submitted_text_len")
    if not _submitted_pair_ok(sub_sha, sub_len):
        raise DeliveryError(
            f"corrupt delivery state {path}: job {key} has invalid submitted fingerprint"
        )
    job["submitted_text_sha256"] = sub_sha
    job["submitted_text_len"] = sub_len
    # Optional observation fields. Missing keys mean an older state file:
    # treat as None for compatibility. New files must carry valid shapes.
    job["jev_decision"] = _validate_jev_decision(raw.get("jev_decision"), path, key)
    job["no_reply_declaration"] = _validate_no_reply_declaration(
        raw.get("no_reply_declaration"), path, key
    )
    job["observation"] = _validate_observation(raw.get("observation"), path, key)
    jev = job["jev_decision"]
    decl = job["no_reply_declaration"]
    obs = job["observation"]
    # NEW-proof binding: no_reply_declaration never existed in production
    # (HEAD c8b6f94 has no such field/API; source-only new task). Any
    # non-null declaration is NEW proof and requires the already-validated
    # stored first-user fingerprint. None/missing declaration keeps all
    # legacy None-fingerprint compat unchanged. Fail corrupt, never
    # auto-fix or drop the field.
    if decl is not None:
        _d_sha = job.get("submitted_text_sha256")
        _d_len = job.get("submitted_text_len")
        if not (
            _is_sha256_hex(_d_sha) and _is_strict_int(_d_len) and 1 <= _d_len <= SUBMITTED_TEXT_MAX
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply needs submitted proof"
            )
    # Stage invariants (fail corrupt, never silently fix).
    # pre_turn_index None means KNOWN zero prior user turns (empty history)
    # once job_ref is set; queued fresh jobs always have None/None/None.
    # turn_index None in delivering/sent means sends were recorded during a
    # synchronous POST await before the native index was bound.
    all_receipted = len(receipts) == len(intents) and len(intents) > 0
    if stage == "queued":
        if (
            job_ref is not None
            or job["pre_turn_index"] is not None
            or job["turn_index"] is not None
            or intents
            or receipts
            or trace is not None
            or skip_reason is not None
            or error is not None
            or job["submitted_text_sha256"] is not None
            or job["submitted_text_len"] is not None
            or decl is not None
            or obs is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} queued must be fresh")
    elif stage == "turn_submitting":
        if (
            job_ref is None
            or job["turn_index"] is not None
            or intents
            or receipts
            or trace is not None
            or skip_reason is not None
            or error is not None
            or obs is not None
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} turn_submitting invariant"
            )
        if decl is not None and (
            decl["job_ref"] != job_ref or jev is None or jev.get("participation") != "need_context"
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply needs need_context"
            )
    elif stage == "inflight":
        if (
            job_ref is None
            or job["turn_index"] is None
            or intents
            or receipts
            or trace is not None
            or skip_reason is not None
            or error is not None
            or obs is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} inflight invariant")
        if decl is not None and (
            decl["job_ref"] != job_ref or jev is None or jev.get("participation") != "need_context"
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply needs need_context"
            )
    elif stage == "delivering":
        if (
            job_ref is None
            or not intents
            or all_receipted
            or trace is not None
            or skip_reason is not None
            or error is not None
            or decl is not None
            or obs is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} delivering invariant")
    elif stage == "sent":
        if (
            job_ref is None
            or not all_receipted
            or trace is not None
            or skip_reason is not None
            or error is not None
            or decl is not None
            or obs is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} sent invariant")
    elif stage == "completed":
        if (
            job_ref is None
            or job["turn_index"] is None
            or not all_receipted
            or trace is None
            or skip_reason is not None
            or error is not None
            or decl is not None
            or obs is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} completed invariant")
    elif stage == "skipped":
        if skip_reason is None or trace is not None or error is not None or obs is not None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} skipped invariant")
        # Fail-closed load guard for the proven-no-send tombstone ONLY:
        # a source_deleted skip with send evidence contradicts "no send"
        # and must never load (and hence never safe-prefix ACK), even from
        # otherwise-valid JSON. All other legacy skip reasons (e.g.
        # operator out_of_scope with evidence) keep their public behavior.
        if skip_reason == "source_deleted" and (job["send_intents"] or job["send_receipts"]):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} source_deleted skip needs zero evidence"
            )
        if decl is not None and (
            job["send_intents"]
            or job["send_receipts"]
            or job_ref is None
            or decl["job_ref"] != job_ref
            or jev is None
            or jev.get("participation") != "need_context"
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply needs need_context"
            )
    elif stage == "uncertain":
        if error is None or trace is not None or skip_reason is not None or obs is not None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} uncertain invariant")
        if decl is not None and (
            job["send_intents"]
            or job["send_receipts"]
            or job_ref is None
            or decl["job_ref"] != job_ref
            or jev is None
            or jev.get("participation") != "need_context"
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} no_reply needs need_context"
            )
    elif stage == "observed":
        if skip_reason is not None or error is not None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} observed invariant")
        # Fail-closed: ANY send evidence contradicts "observed" zero-send
        # and must never load (and hence never safe-prefix ACK).
        if job["send_intents"] or job["send_receipts"]:
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} observed needs zero evidence"
            )
        if obs is None or jev is None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} observed needs proof")
        origin = obs.get("origin")
        if origin == "jev":
            if (
                job_ref is not None
                or job["pre_turn_index"] is not None
                or job["turn_index"] is not None
                or trace is not None
                or decl is not None
                or job["submitted_text_sha256"] is not None
                or job["submitted_text_len"] is not None
                or jev.get("participation") != "observe"
            ):
                raise DeliveryError(
                    f"corrupt delivery state {path}: job {key} observed jev invariant"
                )
        elif origin == "thyca":
            # NEW terminal: unlike legacy completed, no None-fingerprint
            # compat. The stored first-user proof (exact prompt.strip
            # SHA-256 + char len) must be present and valid; structurally
            # missing binding never loads/ACKs even when the caller
            # verified SID/index/status externally.
            sub_sha = job.get("submitted_text_sha256")
            sub_len = job.get("submitted_text_len")
            sub_ok = (
                _is_sha256_hex(sub_sha)
                and _is_strict_int(sub_len)
                and 1 <= sub_len <= SUBMITTED_TEXT_MAX
            )
            if (
                job_ref is None
                or job["turn_index"] is None
                or trace is None
                or decl is None
                or jev.get("participation") != "need_context"
                or decl.get("job_ref") != job_ref
                or not sub_ok
            ):
                raise DeliveryError(
                    f"corrupt delivery state {path}: job {key} observed thyca invariant"
                )
            else:
                tool_ids = trace.get("tool_call_ids", [])
                if not tool_ids or len(set(tool_ids)) != len(tool_ids):
                    raise DeliveryError(
                        f"corrupt delivery state {path}: job {key} observed needs unique proof"
                    )
        else:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} observed needs proof")
    return job


def _validate_state(raw: Any, path: Path) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise DeliveryError(f"corrupt delivery state {path}: top level must be an object")
    for name in (
        "version",
        "native_session_id",
        "baseline_acked",
        "historical_message_ids",
        "authorized_skips",
        "jobs",
    ):
        if name not in raw:
            raise DeliveryError(f"corrupt delivery state {path}: missing key {name!r}")
    state = dict(raw)
    version = raw["version"]
    if not _is_strict_int(version) or version != DELIVERY_VERSION:
        raise DeliveryError(f"corrupt delivery state {path}: unsupported version")
    state["version"] = DELIVERY_VERSION
    sid = raw["native_session_id"]
    if (
        not isinstance(sid, str)
        or not (1 <= len(sid) <= MAX_ID_LEN)
        or sid.strip() != sid
        or not sid
        or _has_whitespace(sid)
    ):
        raise DeliveryError(f"corrupt delivery state {path}: invalid native_session_id")
    state["native_session_id"] = sid
    baseline = raw["baseline_acked"]
    if not _is_strict_int(baseline) or baseline < 0:
        raise DeliveryError(
            f"corrupt delivery state {path}: 'baseline_acked' must be a non-negative int"
        )
    state["baseline_acked"] = baseline
    historical = _validate_historical(raw["historical_message_ids"], path)
    state["historical_message_ids"] = historical
    skips_raw = raw["authorized_skips"]
    if not isinstance(skips_raw, list):
        raise DeliveryError(f"corrupt delivery state {path}: 'authorized_skips' must be a list")
    skips = [_validate_authorized_skip(item, path, baseline, i) for i, item in enumerate(skips_raw)]
    skip_keys = [(s["channel_id"], s["message_id"]) for s in skips]
    if len(set(skip_keys)) != len(skip_keys):
        raise DeliveryError(f"corrupt delivery state {path}: duplicate authorized skip")
    skip_seqs = [s["seq"] for s in skips]
    if len(set(skip_seqs)) != len(skip_seqs):
        raise DeliveryError(f"corrupt delivery state {path}: duplicate authorized skip seq")
    historical_pairs = {(ch, mid) for ch, mids in historical.items() for mid in mids}
    for i, item in enumerate(skips):
        if (item["channel_id"], item["message_id"]) in historical_pairs:
            raise DeliveryError(
                f"corrupt delivery state {path}: authorized skip #{i} overlaps historical"
            )
    state["authorized_skips"] = skips
    jobs_raw = raw["jobs"]
    if not isinstance(jobs_raw, dict):
        raise DeliveryError(f"corrupt delivery state {path}: 'jobs' must be an object")
    jobs: dict[str, dict[str, Any]] = {}
    for key in jobs_raw:
        if _parse_job_key(key) is None:
            raise DeliveryError(f"corrupt delivery state {path}: bad job key")
    for key, value in jobs_raw.items():
        jobs[key] = _validate_job(value, key, path)
    # Global seq uniqueness across different jobs (aliases may grow per job).
    seq_owner: dict[int, str] = {}
    for key, job in jobs.items():
        for seq in [job["seq"], *job["seq_aliases"]]:
            owner = seq_owner.get(seq)
            if owner is not None and owner != key:
                raise DeliveryError(f"corrupt delivery state {path}: duplicate seq across jobs")
            seq_owner[seq] = key
    # Authorized skips must stay in exact sync with their skipped jobs
    # (approved record tuple is immutable: seq/author/actor/reason).
    # Later legit seq aliases on the job are fine; any mismatch is
    # corrupt load -- bounded error, never a silent repair.
    for i, item in enumerate(skips):
        skip_key = _job_key(item["channel_id"], item["message_id"])
        job = jobs.get(skip_key)
        if (
            job is None
            or job["stage"] != "skipped"
            or job["seq"] != item["seq"]
            or job["author_uid"] != item["author_uid"]
            or job["actor_kind"] != item["actor_kind"]
            or job["skip_reason"] != item["reason"]
        ):
            raise DeliveryError(f"corrupt delivery state {path}: authorized skip #{i} job mismatch")
    state["jobs"] = jobs
    # Optional top-level Jev ledger marker. Missing key means an older
    # state file: treat as None for compatibility. When present, must be
    # None (unbound) or an exact 32hex immutable marker. Ordinary
    # metadata only; never a raw path or chat.
    ledger = raw.get("jev_ledger_id")
    if ledger is None:
        state["jev_ledger_id"] = None
    elif _is_attempt_id(ledger):
        state["jev_ledger_id"] = ledger
    else:
        raise DeliveryError(f"corrupt delivery state {path}: invalid jev_ledger_id")
    # Optional top-level daily-session history (day -> native SID
    # provenance). Missing key means a pre-rotation state file: treat as
    # empty for compatibility. When present, every key must be a real
    # calendar day and every value a strict native SID. Provenance only;
    # no cross-check against native_session_id or jobs at load (the
    # rotate transition maintains the invariant).
    if "native_sessions" not in raw:
        state["native_sessions"] = {}
    else:
        history = sessions.validate_sessions_history(raw.get("native_sessions"))
        if history is None:
            raise DeliveryError(f"corrupt delivery state {path}: invalid native_sessions")
        state["native_sessions"] = history
    return state
