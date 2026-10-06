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
import hashlib
import json
import math
import os
import tempfile
import uuid
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from .conversation import ConversationError, project_conversation_state
from .jev import (
    JEV_MODEL,
    PARTICIPATION_OPTIONS,
    QUESTION_IDS,
    JevError,
    build_questions,
    validate_answers,
    validate_model_id,
    validate_usage,
)

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

__all__ = [
    "INPUT_USD_PER_TOKEN",
    "JEV_MODEL",
    "PARTICIPATION_STATUSES",
    "PARTICIPATION_VERSION",
    "ParticipationDurabilityUncertain",
    "ParticipationError",
    "ParticipationLedger",
    "ParticipationPending",
    "ParticipationReason",
]


class ParticipationError(ValueError):
    """Corrupt ledger, invalid operation, or failed closed guard."""


class ParticipationReason(str, Enum):
    """Bounded pending reasons; never carries raw payload text."""

    IN_PROGRESS = "in_progress"
    ALREADY_STARTED = "already_started"
    ALREADY_FAILED_KNOWN = "already_failed_known"
    ALREADY_FAILED_UNKNOWN = "already_failed_unknown"
    CONTEXT_MISMATCH = "context_mismatch"
    UNKNOWN_BLOCKED = "unknown_blocked"


class ParticipationPending(ParticipationError):
    """No decision available now; caller must not retry the provider call."""

    def __init__(self, reason: ParticipationReason) -> None:
        if not isinstance(reason, ParticipationReason):
            raise ParticipationError("participation pending needs a bounded reason")
        self.reason = reason
        super().__init__(f"participation pending: {reason.value}")


class ParticipationDurabilityUncertain(ParticipationError):
    """Post-commit dir fsync failed; file committed, durability unknown.

    committed=True always. Never carries raw OSError text. Callers MUST
    stop, never rollback/retry/delete the committed file.
    """

    def __init__(self, path: Path | str) -> None:
        self.kind = "durability_uncertain"
        self.committed = True
        super().__init__(f"participation ledger {path}: commit ok but dir fsync failed")


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


def _default_day_provider() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    if parent.exists():
        if parent.is_symlink() or not parent.is_dir():
            raise ParticipationError(f"participation parent {parent} is not a directory")
        return
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
        if len(missing) > 64:
            raise ParticipationError(f"participation parent {parent} is too deep")
    if cursor.is_symlink() or not cursor.is_dir():
        raise ParticipationError(f"participation parent {cursor} is not a directory")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise ParticipationError(
                    f"participation parent {directory} is not a directory"
                ) from None
            continue
        os.chmod(directory, 0o700)


def _fsync_parent_dir(parent: Path, state_path: Path) -> None:
    fd = None
    try:
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise ParticipationDurabilityUncertain(state_path) from None
    try:
        try:
            os.fsync(fd)
        except OSError:
            raise ParticipationDurabilityUncertain(state_path) from None
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


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


def _validate_record(raw: Any, key: str, path: Path, expected_sid: str) -> dict[str, Any]:
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
    if sid != expected_sid:
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
        attempts[key] = _validate_record(value, key, path, expected_sid)
    state["attempts"] = attempts
    return state


def _load_ledger(path: Path, expected_sid: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ParticipationError(f"participation ledger {path} must not be a symlink")
    if not path.exists():
        raise ParticipationError(f"participation ledger {path} is missing; init explicitly")
    if not path.is_file():
        raise ParticipationError(f"participation ledger {path} is not a regular file")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ParticipationError(
            f"corrupt participation ledger {path}: invalid JSON at line "
            f"{exc.lineno} col {exc.colno}"
        ) from None
    return _validate_ledger(raw, path, expected_sid)


def _save_ledger(path: Path, state: dict[str, Any]) -> None:
    if path.is_symlink():
        raise ParticipationError(f"participation ledger {path} must not be a symlink")
    if not path.exists():
        raise ParticipationError(f"participation ledger {path} is missing; init explicitly")
    if not path.is_file():
        raise ParticipationError(f"participation ledger {path} is not a regular file")
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise ParticipationError(f"participation parent {parent} is not a directory")
    fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=path.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    _fsync_parent_dir(parent, path)


def _create_ledger(path: Path, state: dict[str, Any]) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp_path, path)
        except FileExistsError as exc:
            raise ParticipationError(
                f"participation ledger {path} already exists; refusing to overwrite"
            ) from exc
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass
    _fsync_parent_dir(path.parent, path)


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
