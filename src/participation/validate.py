"""Pure validation/canonicalization helpers for the participation ledger.

Canonical home of the ledger's persistence-boundary validators, key/cost
helpers, and the ``ParticipationError`` base they raise -- all free of
file IO, clocks, and provider calls. ``participation`` re-exports
every name defined here (same objects), so ``participation``
remains the single public import surface. This module imports only
stdlib plus ``conversation``/``jev`` and never imports
``participation`` back (no cycle).
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import sessions
from jev import (
    JEV_MODEL,
    PARTICIPATION_OPTIONS,
    QUESTION_IDS,
    JevError,
    validate_usage,
)

from .errors import ParticipationError

PARTICIPATION_VERSION = 1
PARTICIPATION_STATUSES = frozenset({"started", "decided", "failed_known", "failed_unknown"})
# Pre-3-question stored shape (read-only compat, never written anymore).
_LEGACY_QUESTION_IDS = (
    "sender_uid",
    "addressee",
    "explicit_assistant_request",
    "reply_parent_status",
    "participation",
)
INPUT_USD_PER_TOKEN = Decimal("0.000000042")
MAX_ID_LEN = 128
SHA256_HEX_LEN = 64


def _is_strict_positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _has_whitespace(value: str) -> bool:
    for ch in value:
        if ch.isspace():
            return True
    return False


def _validate_id_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if not (1 <= len(value) <= MAX_ID_LEN):
        return None
    if value.strip() != value or not value:
        return None
    if _has_whitespace(value):
        return None
    return value


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
    if parts[0] != str(channel_id) or parts[1] != str(message_id):
        return None
    return channel_id, message_id


def _is_sha256_hex(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != SHA256_HEX_LEN:
        return False
    for ch in value:
        if ch not in "0123456789abcdefABCDEF":
            return False
    return True


def _canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _cost_for_input_tokens(input_tokens: int) -> Decimal:
    return Decimal(input_tokens) * INPUT_USD_PER_TOKEN


def _estimated_cost_str(input_tokens: int) -> str:
    return _canonical_decimal(_cost_for_input_tokens(input_tokens))


def _extract_valid_usage(obj: Any) -> dict[str, int] | None:
    """Extract valid usage FIRST, before model/answers/participation/latency.

        Robust to malformed or raising-getter objects: any unreadable or
        invalid usage returns None (never raises ``Exception``). Never touches
    other fields. Returned dict is clean and detached.
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


def _known_cost_for_today(attempts: Mapping[str, dict[str, Any]], today: str) -> Decimal:
    """Sum canonical costs for today, including lower-bound unknowns.

    Serial same-process only. Counts any record with a canonical cost
    string (decided, failed_known, lower-bound failed_unknown); skips
    started (None) and pure unknown ("unknown"). No raw context read.
    """
    total = Decimal("0")
    for record in attempts.values():
        if record.get("attempted_day") != today:
            continue
        cost_raw = record.get("estimated_cost_usd")
        if isinstance(cost_raw, str) and cost_raw != "unknown":
            total += Decimal(cost_raw)
    return total


def _is_valid_day(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 10:
        return False
    if value[4] != "-" or value[7] != "-":
        return False
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    return parsed.isoformat() == value


def _fingerprint_snapshot(snapshot: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _validate_stored_usage(raw: Any, path: Path, key: str) -> dict[str, int]:
    if not isinstance(raw, dict) or set(raw) != {"input_tokens", "output_tokens"}:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid usage"
        )
    for name in ("input_tokens", "output_tokens"):
        value = raw[name]
        if type(value) is not int or value < 0:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid usage"
            )
    return {"input_tokens": raw["input_tokens"], "output_tokens": raw["output_tokens"]}


def _check_prob(value: Any, path: Path, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid answers"
        )
    number = float(value)
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid answers"
        )
    return number


def _validate_choice_token(value: Any, path: Path, key: str) -> str:
    if not isinstance(value, str) or not (1 <= len(value) <= MAX_ID_LEN):
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid answers"
        )
    if "\n" in value or "\r" in value:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid answers"
        )
    return value


def _validate_stored_answers(raw: Any, path: Path, key: str) -> dict[str, Any]:
    # New shape is exactly the three routing questions. The legacy five
    # (plus sender_uid + reply_parent_status) still validates read-only so
    # pre-cut ledger files keep their decided cache and spend history; new
    # classifications always write three. No data rewrite, no version bump.
    if isinstance(raw, dict) and set(raw) == set(QUESTION_IDS):
        qids: tuple[str, ...] = QUESTION_IDS
    elif isinstance(raw, dict) and set(raw) == set(_LEGACY_QUESTION_IDS):
        qids = _LEGACY_QUESTION_IDS
    else:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid answers"
        )
    clean: dict[str, Any] = {}
    for qid in qids:
        entry = raw[qid]
        if not isinstance(entry, dict):
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid answers"
            )
        if qid == "explicit_assistant_request":
            if set(entry) != {"type", "noul"} or entry.get("type") != "noul":
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid answers"
                )
            clean[qid] = {"type": "noul", "noul": _check_prob(entry.get("noul"), path, key)}
            continue
        if set(entry) - {"type", "choice", "probabilities", "confidence"}:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid answers"
            )
        if entry.get("type") != "choice":
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid answers"
            )
        choice = _validate_choice_token(entry.get("choice"), path, key)
        probs = entry.get("probabilities")
        if not isinstance(probs, dict) or not probs:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid answers"
            )
        clean_probs: dict[str, float] = {}
        for opt, prob in probs.items():
            clean_probs[_validate_choice_token(opt, path, key)] = _check_prob(prob, path, key)
        item: dict[str, Any] = {
            "type": "choice",
            "choice": choice,
            "probabilities": clean_probs,
        }
        if "confidence" in entry:
            item["confidence"] = _check_prob(entry.get("confidence"), path, key)
        clean[qid] = item
    return clean


def _validate_record(
    raw: Any,
    key: str,
    path: Path,
    expected_sid: str,
    *,
    allowed_sids: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Validate one attempt record; its SID must be the current or historical.

    ``allowed_sids`` is the adoption-aware SID set (current top-level SID
    plus every ``native_sessions`` history value); records keep the SID of
    the day they were classified on, so post-adoption files legitimately
    span SIDs. ``None`` means ``{expected_sid}`` (pre-adoption behavior).
    Cache correctness never depends on the SID: hits still require the
    exact channel/mid/author/fingerprint match.
    """
    if not isinstance(raw, dict):
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} must be an object"
        )
    for name in (
        "native_session_id",
        "channel_id",
        "message_id",
        "author_uid",
        "fingerprint",
        "model",
        "attempt_id",
        "attempted_day",
        "attempted_at",
        "status",
        "usage",
        "estimated_cost_usd",
        "billing",
        "participation",
        "answers",
    ):
        if name not in raw:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} missing key {name!r}"
            )
    record = dict(raw)
    sid = raw["native_session_id"]
    if _validate_id_string(sid) is None:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid session"
        )
    allowed = allowed_sids if allowed_sids is not None else frozenset({expected_sid})
    if sid not in allowed:
        raise ParticipationError(f"participation ledger {path}: job {key} session mismatch")
    record["native_session_id"] = sid
    for name in ("channel_id", "message_id", "author_uid"):
        value = raw[name]
        if type(value) is not int or value <= 0:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid {name}"
            )
        record[name] = value
    if key != _job_key(record["channel_id"], record["message_id"]):
        raise ParticipationError(f"corrupt participation ledger {path}: job {key} key mismatch")
    if not _is_sha256_hex(raw["fingerprint"]):
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid fingerprint"
        )
    record["fingerprint"] = raw["fingerprint"]
    if raw["model"] != JEV_MODEL:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid model"
        )
    record["model"] = JEV_MODEL
    attempt_id = raw["attempt_id"]
    # Canonical uuid4.hex: exactly 32 lower hex, valid UUID syntax.
    # Fresh ledger only; hyphen-36 is corrupt (fail closed, no migration).
    if not isinstance(attempt_id, str) or len(attempt_id) != 32:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid attempt"
        )
    for ch in attempt_id:
        if ch not in "0123456789abcdef":
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid attempt"
            )
    try:
        parsed_attempt = uuid.UUID(attempt_id)
    except ValueError:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid attempt"
        ) from None
    if parsed_attempt.hex != attempt_id:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid attempt"
        )
    record["attempt_id"] = attempt_id
    if not _is_valid_day(raw["attempted_day"]):
        raise ParticipationError(f"corrupt participation ledger {path}: job {key} has invalid day")
    record["attempted_day"] = raw["attempted_day"]
    attempted_at = raw["attempted_at"]
    if (
        not isinstance(attempted_at, str)
        or not (1 <= len(attempted_at) <= 256)
        or "\n" in attempted_at
        or "\r" in attempted_at
    ):
        raise ParticipationError(f"corrupt participation ledger {path}: job {key} has invalid time")
    record["attempted_at"] = attempted_at
    status = raw["status"]
    if status not in PARTICIPATION_STATUSES:
        raise ParticipationError(
            f"corrupt participation ledger {path}: job {key} has invalid status"
        )
    record["status"] = status
    if status in ("decided", "failed_known"):
        usage = _validate_stored_usage(raw["usage"], path, key)
        record["usage"] = usage
        cost_raw = raw["estimated_cost_usd"]
        if not isinstance(cost_raw, str):
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid cost"
            )
        try:
            cost_dec = Decimal(cost_raw)
        except Exception:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid cost"
            ) from None
        if not cost_dec.is_finite() or cost_dec < 0:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid cost"
            )
        if _canonical_decimal(cost_dec) != cost_raw:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid cost"
            )
        if cost_raw != _estimated_cost_str(usage["input_tokens"]):
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid cost"
            )
        record["estimated_cost_usd"] = cost_raw
        if raw["billing"] != "known":
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid billing"
            )
        record["billing"] = "known"
        if status == "decided":
            participation = raw["participation"]
            if participation not in PARTICIPATION_OPTIONS:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid decision"
                )
            record["participation"] = participation
            answers = _validate_stored_answers(raw["answers"], path, key)
            if answers["participation"]["choice"] != participation:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid decision"
                )
            record["answers"] = answers
        else:
            if raw["participation"] is not None or raw["answers"] is not None:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid decision"
                )
            record["participation"] = None
            record["answers"] = None
    elif status == "failed_unknown":
        usage_raw = raw["usage"]
        cost_raw = raw["estimated_cost_usd"]
        if usage_raw is None:
            if cost_raw != "unknown":
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid billing"
                )
            record["usage"] = None
            record["estimated_cost_usd"] = "unknown"
        else:
            usage = _validate_stored_usage(usage_raw, path, key)
            record["usage"] = usage
            if not isinstance(cost_raw, str):
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid cost"
                )
            try:
                cost_dec = Decimal(cost_raw)
            except Exception:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid cost"
                ) from None
            if not cost_dec.is_finite() or cost_dec < 0:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid cost"
                )
            if _canonical_decimal(cost_dec) != cost_raw:
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid cost"
                )
            if cost_raw != _estimated_cost_str(usage["input_tokens"]):
                raise ParticipationError(
                    f"corrupt participation ledger {path}: job {key} has invalid cost"
                )
            record["estimated_cost_usd"] = cost_raw
        if raw["billing"] != "unknown":
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid billing"
            )
        record["billing"] = "unknown"
        if raw["participation"] is not None or raw["answers"] is not None:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid decision"
            )
        record["participation"] = None
        record["answers"] = None
    else:
        if raw["usage"] is not None or raw["estimated_cost_usd"] is not None:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid billing"
            )
        record["usage"] = None
        record["estimated_cost_usd"] = None
        if raw["billing"] != "pending":
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid billing"
            )
        record["billing"] = "pending"
        if raw["participation"] is not None or raw["answers"] is not None:
            raise ParticipationError(
                f"corrupt participation ledger {path}: job {key} has invalid decision"
            )
        record["participation"] = None
        record["answers"] = None
    return record


def _validate_ledger(raw: Any, path: Path, expected_sid: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ParticipationError(
            f"corrupt participation ledger {path}: top level must be an object"
        )
    for name in ("version", "ledger_id", "native_session_id", "attempts"):
        if name not in raw:
            raise ParticipationError(f"corrupt participation ledger {path}: missing key {name!r}")
    state = dict(raw)
    if type(raw["version"]) is not int or raw["version"] != PARTICIPATION_VERSION:
        raise ParticipationError(f"corrupt participation ledger {path}: unsupported version")
    state["version"] = PARTICIPATION_VERSION
    ledger_id = raw["ledger_id"]
    # Immutable activation identity: canonical uuid4.hex, 32 lower hex.
    # Fresh only; missing/non-canonical fails closed, no migration.
    if not isinstance(ledger_id, str) or len(ledger_id) != 32:
        raise ParticipationError(f"corrupt participation ledger {path}: invalid ledger_id")
    for ch in ledger_id:
        if ch not in "0123456789abcdef":
            raise ParticipationError(f"corrupt participation ledger {path}: invalid ledger_id")
    try:
        parsed_ledger = uuid.UUID(ledger_id)
    except ValueError:
        raise ParticipationError(
            f"corrupt participation ledger {path}: invalid ledger_id"
        ) from None
    if parsed_ledger.hex != ledger_id:
        raise ParticipationError(f"corrupt participation ledger {path}: invalid ledger_id")
    state["ledger_id"] = ledger_id
    sid = raw["native_session_id"]
    if _validate_id_string(sid) is None:
        raise ParticipationError(f"corrupt participation ledger {path}: invalid session")
    if sid != expected_sid:
        raise ParticipationError(f"participation ledger {path}: session mismatch")
    state["native_session_id"] = sid
    # Optional top-level daily-session history (day -> native SID
    # provenance). Missing key means a pre-adoption ledger: treat as
    # empty for compatibility. When present, every key must be a real
    # calendar day and every value a strict native SID. Records may
    # carry any SID in {current} + history values (their classification
    # day's session); anything else is a session mismatch.
    if "native_sessions" not in raw:
        history: dict[str, str] = {}
    else:
        validated = sessions.validate_sessions_history(raw.get("native_sessions"))
        if validated is None:
            raise ParticipationError(
                f"corrupt participation ledger {path}: invalid native_sessions"
            )
        history = validated
    state["native_sessions"] = history
    allowed_sids = frozenset({sid, *history.values()})
    # Optional retired-SID list (verifiably-gone sessions old records may
    # still reference). Missing key means a ledger without retired: treat as
    # empty for compatibility. Entries must be unique strict SIDs, and
    # the live top-level SID must never be retired. Records may carry
    # any SID in {current} + history + retired.
    if "retired_sessions" not in raw:
        retired: list[str] = []
    else:
        validated_retired = sessions.validate_retired_sessions(raw.get("retired_sessions"))
        if validated_retired is None:
            raise ParticipationError(
                f"corrupt participation ledger {path}: invalid retired_sessions"
            )
        retired = validated_retired
    if sid in retired:
        raise ParticipationError(f"corrupt participation ledger {path}: current session is retired")
    state["retired_sessions"] = retired
    allowed_sids = frozenset({sid, *history.values(), *retired})
    attempts_raw = raw["attempts"]
    if not isinstance(attempts_raw, dict):
        raise ParticipationError(
            f"corrupt participation ledger {path}: 'attempts' must be an object"
        )
    attempts: dict[str, dict[str, Any]] = {}
    for key in attempts_raw:
        if _parse_job_key(key) is None:
            raise ParticipationError(f"corrupt participation ledger {path}: bad job key")
    for key, value in attempts_raw.items():
        attempts[key] = _validate_record(value, key, path, expected_sid, allowed_sids=allowed_sids)
    state["attempts"] = attempts
    return state
