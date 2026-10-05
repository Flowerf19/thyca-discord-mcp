"""Durable delivery state: agent-owned MCP send jobs as file metadata only.

Single JSON file (``delivery_state.json`` by convention, any path allowed)::

    {
      "version": 1,
      "native_session_id": "<native Thyca SID>",
      "baseline_acked": 21,
      "historical_message_ids": {"<channel>": [mid, ...]},
      "authorized_skips": [{"channel_id","message_id","seq","author_uid",
                            "actor_kind","reason"}],
      "jobs": {"<channel>:<mid>": {job}},
      ...unknown top-level keys preserved
    }

Job (all IDs are strict positive ints, never bool)::

    {
      "channel_id","message_id","author_uid","actor_kind","seq",
      "seq_aliases": [extra seqs for same MID, sorted, unique],
      "stage": queued|turn_submitting|inflight|delivering|sent|
               completed|skipped|uncertain,
      "job_ref": str|None, "pre_turn_index": int|None,
      "turn_index": int|None,
      "submitted_text_sha256": str|None, "submitted_text_len": int|None,
      "send_intents": [{"send_ref","content_len","content_hash","ts"}],
      "send_receipts": [{"send_ref","sent_message_id",
                         "delivered_channel_id","jump_url",
                         "reply_to_message_id","ts"}],
      "trace_refs": None|{"tool_call_ids","ts"},
      "skip_reason": None|str, "error": None|{"kind","http_status","ts"},
      "created_at","updated_at", ...unknown job keys preserved
    }

Metadata only: intents store length/hash, receipts store IDs/URL, errors
store coarse ``kind`` + optional HTTP status. No message body, reply text,
profile, or freeform provider error is ever accepted or stored.

Durability model (single-process / single asyncio loop only):
- Pure sync file state. No HTTP, workers, LLM, Discord calls, locks,
  threads, or frameworks. All mutations are sync with no ``await`` inside.
- Every mutation loads FRESH state from disk, patches only its owned job
  fields, and saves atomically. Callers MUST never save a snapshot loaded
  before an ``await`` (a receipt recorded during an HTTP await would be
  lost). Use only the mutation functions here, which reload fresh.
- Save is atomic: private temp inode (0600) + flush/fsync + ``os.replace``
  in the same parent, then a PARENT-DIRECTORY fsync for namespace
  durability. A directory-fsync failure AFTER the replace raises
  DeliveryDurabilityUncertain (committed=True): the new state file exists
  and validates; never rewind it, delete it, or blind-retry. Only owned
  temp files are removed on pre-commit failure; a silent temp-unlink
  failure can leave one known owned orphan (``<name>.tmp.*``) and never
  means the state is uncommitted. Corrupt input fails BEFORE any write;
  original bytes are preserved.
- Init is exclusive (fresh path only, 0600 temp + fsync + hard-link
  commit + directory fsync, no overwrite of valid/corrupt/unknown
  existing; a post-link fsync failure still leaves the state in place and
  later init refuses it). Missing/corrupt load fails loudly; there is no
  automatic fresh reset.
- Cross-process / multi-writer use is NOT supported or claimed.

Permissions / paths (minimal, no framework):
- Delivery file 0600. Temp inode is 0600 BEFORE commit; no post-commit
  chmod. Only missing parent dirs are created (0700); existing parents
  are NEVER chmodded (repo root 0755 or private home 0700 stay as-is).
- Symlink state paths and symlink parents are refused. The state path
  must be a regular file once created.
- Errors carry bounded field/path/key metadata only, never entry content.

Stages / transitions (terminal: completed, skipped, uncertain):
- ``queued`` -> ``turn_submitting`` (mark_turn_submitting; ``pre_turn_index
  = None`` means a KNOWN empty native session, never an unknown/faked
  index), ``turn_submitting`` -> ``inflight`` (mark_inflight binds the
  ACTUAL native turn index after the confirmed own POST/Trace),
  ``turn_submitting``/``inflight``/``delivering``/``sent`` -> ``delivering``
  (record_send_intent, one per send_ref, multi-chunk allowed; a deferred
  SDK callback during the synchronous POST await records real sends while
  ``turn_index`` is still None),
  ``delivering`` -> ``sent`` (record_send_receipt when ALL intents have
  receipts), ``sent`` -> ``completed`` (complete_job, receipts already
  durable, caller verified native Trace externally),
  ``uncertain`` -> ``completed`` (complete_job only when ALL intents are
  receipted, the native ``turn_index`` is bound and matches, and
  non-empty verified tool_call_ids are supplied; the stored error is
  cleared), ``turn_submitting``/``inflight`` -> ``queued``
  (requeue_definitive_busy, ONLY a definitive native POST 409 with zero
  intents/receipts/trace),
  any non-terminal -> ``skipped`` (skip_job, explicit reason),
  any non-terminal -> ``uncertain`` (mark_uncertain, coarse kind).
- Terminal stages never rewind. Idempotent repeats require EXACT same
  payload, otherwise conflict fails. Receipts require a prior intent;
  complete requires ALL intents receipted (2 confirmed + 1 pending
  blocks). Late receipts for EXISTING pending intents are accepted on
  ``uncertain`` jobs and preserve stage + error (never auto-sent/
  completed; dropping the late SDK MID would lose the send evidence); a
  NEW send intent on ``uncertain`` is refused. mark_inflight on
  ``delivering``/``sent``/``uncertain`` only binds the unbound index
  (stage, receipts, error preserved; same index idempotent, different
  index conflicts). Duplicate enqueue for the same MID appends a seq
  alias (sorted, unique, gaps allowed, no alias budget) without a new
  job/ref/resubmit, even for terminal jobs (backfill persists).
  Different MIDs must never share a seq. Channel/author mismatches fail.
- Receipt association by explicit ``send_ref`` key is correlation only,
  never an authentication proof.

Public contracts (stable names for future writers):
- ``initialize_delivery_state(path, *, native_session_id, baseline_acked,
  historical_message_ids, authorized_skips) -> dict`` fresh state snapshot.
- ``load_delivery_state(path) -> dict`` state snapshot, fails if missing.
- ``enqueue_job(path, *, channel_id, message_id, author_uid, actor_kind,
  seq) -> dict`` job snapshot (dedupe by MID, alias by seq).
- ``get_job(path, channel_id, message_id) -> dict|None`` job snapshot.
- ``list_jobs(path) -> list[dict]`` job snapshots sorted by primary seq.
- ``mark_turn_submitting(path, channel_id, message_id, *, job_ref,
  pre_turn_index, submitted_text_sha256=None, submitted_text_len=None)
  -> dict`` (``pre_turn_index=None`` = known-empty native session;
  fingerprint pair both-None or 64-hex SHA-256 + positive len <= 4000
  chars of the exact ``prompt.strip`` POST text, never raw text;
  immutable/idempotent), ``mark_inflight(..., *, turn_index)`` (binds the
  actual native index without rewinding stages or losing receipts).
- ``requeue_definitive_busy(path, channel_id, message_id, *, http_status=
  409) -> dict`` strict-409 definitive-busy reset to ``queued`` for
  ``turn_submitting``/``inflight`` with zero intents/receipts/trace; an
  already-``queued`` job fails explicitly (never a silent reset).
- ``record_send_intent(..., *, send_ref, content_len, content_hash)``.
- ``record_send_receipt(..., *, send_ref, sent_message_id,
  delivered_channel_id, jump_url, reply_to_message_id)`` where the first
  two positionals are the SOURCE job key and ``delivered_channel_id``
  must equal the source channel in the auto path.
- ``complete_job(..., *, turn_index, tool_call_ids)`` (``sent`` or the
  explicit ``uncertain`` path above; ``uncertain`` completion clears the
  stored error).
- ``skip_job(..., *, reason)``, ``mark_uncertain(..., *, kind,
  http_status=None)``.
- ``processed_seq_prefix(path, ordered_entries, current_acked) -> int``
  highest ACTUAL seen seq whose MID is historical or whose job is
  completed/skipped with exact (channel,mid,seq) match; stops at the
  first unresolved entry, gaps allowed. ``ordered_entries`` MUST be the
  FULL unfiltered unread outbox prefix from the caller -- omitted rows
  CANNOT be detected here. Optional ``author_uid``/``actor_kind`` fields
  are type-validated and must MATCH the known job; a seq owned by a
  different known job while the row names a KNOWN (historical or job)
  MID is a contradiction that raises even at/below ``current_acked``.
All snapshots are deep copies. ``DeliveryError`` (a ``ValueError``) is
raised for missing/corrupt data and invalid transitions.
"""

from __future__ import annotations

import copy
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    }
)
TERMINAL_STAGES = frozenset({"completed", "skipped", "uncertain"})
ACTOR_KINDS = frozenset({"owner", "other", "self"})
SKIP_REASONS = frozenset(
    {
        "operator_approved_drop",
        "out_of_scope",
        "bot_filtered",
        "duplicate_superseded",
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _ensure_parent_dir(path: Path) -> None:
    """Create only missing parents as 0700; never touch existing parents."""
    parent = path.parent
    if parent.exists():
        if parent.is_symlink() or not parent.is_dir():
            raise DeliveryError(f"delivery parent {parent} is not a directory")
        return
    # Collect missing chain top-down so only newly created dirs get 0700.
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
        if len(missing) > 64:
            raise DeliveryError(f"delivery parent {parent} is too deep")
    if cursor.is_symlink() or not cursor.is_dir():
        raise DeliveryError(f"delivery parent {cursor} is not a directory")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise DeliveryError(f"delivery parent {directory} is not a directory") from None
            continue
        # mkdir mode is masked by umask; chmod only this newly owned dir.
        os.chmod(directory, 0o700)


def _fsync_parent_dir(parent: Path, state_path: Path) -> None:
    """Fsync the parent directory after a file commit (namespace durability).

    Must be called AFTER os.replace/os.link. On failure raises
    DeliveryDurabilityUncertain (committed=True) without raw OSError text.
    """
    fd = None
    try:
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise DeliveryDurabilityUncertain(state_path) from None
    try:
        try:
            os.fsync(fd)
        except OSError:
            raise DeliveryDurabilityUncertain(state_path) from None
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _is_sha256_hex(value: Any) -> bool:
    """Exact 64-char hex (SHA-256 fingerprint); never echoes the value."""
    if not isinstance(value, str):
        return False
    if len(value) != SHA256_HEX_LEN:
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
        ):
            raise DeliveryError(
                f"corrupt delivery state {path}: job {key} turn_submitting invariant"
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
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} inflight invariant")
    elif stage == "delivering":
        if (
            job_ref is None
            or not intents
            or all_receipted
            or trace is not None
            or skip_reason is not None
            or error is not None
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} delivering invariant")
    elif stage == "sent":
        if (
            job_ref is None
            or not all_receipted
            or trace is not None
            or skip_reason is not None
            or error is not None
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
        ):
            raise DeliveryError(f"corrupt delivery state {path}: job {key} completed invariant")
    elif stage == "skipped":
        if skip_reason is None or trace is not None or error is not None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} skipped invariant")
    elif stage == "uncertain":
        if error is None or trace is not None or skip_reason is not None:
            raise DeliveryError(f"corrupt delivery state {path}: job {key} uncertain invariant")
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
    return state


def _load_state(path: str | Path) -> dict[str, Any]:
    file = Path(path)
    if file.is_symlink():
        raise DeliveryError(f"delivery state {file} must not be a symlink")
    if not file.exists():
        raise DeliveryError(f"delivery state {file} is missing; initialize explicitly")
    if not file.is_file():
        raise DeliveryError(f"delivery state {file} is not a regular file")
    try:
        raw = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        # Bounded position metadata only; never echo the raw file body.
        raise DeliveryError(
            f"corrupt delivery state {file}: invalid JSON at line {e.lineno} col {e.colno}"
        ) from None
    return _validate_state(raw, file)


def _save_state(path: str | Path, state: dict[str, Any]) -> None:
    """Atomically replace an existing state file via a private temp inode."""
    file = Path(path)
    if file.is_symlink():
        raise DeliveryError(f"delivery state {file} must not be a symlink")
    if not file.exists():
        raise DeliveryError(f"delivery state {file} is missing; initialize explicitly")
    if not file.is_file():
        raise DeliveryError(f"delivery state {file} is not a regular file")
    parent = file.parent
    if parent.is_symlink() or not parent.is_dir():
        raise DeliveryError(f"delivery parent {parent} is not a directory")
    fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=file.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, file)
    except BaseException:
        # Pre-commit failure: clean up ONLY our own temp inode. A silent
        # unlink failure can leave one known owned orphan tmp; it never
        # means the state is uncommitted, and the state file itself is
        # untouched (original bytes preserved).
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    # Committed via os.replace (the temp path is already gone). The
    # directory entry needs its own fsync for namespace durability; on
    # failure raise DeliveryDurabilityUncertain WITHOUT rewinding,
    # deleting the committed file, or carrying raw OSError text.
    _fsync_parent_dir(parent, file)


def _new_job(
    *,
    channel_id: int,
    message_id: int,
    author_uid: int,
    actor_kind: str,
    seq: int,
    stage: str,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    now = _now_iso()
    return {
        "channel_id": channel_id,
        "message_id": message_id,
        "author_uid": author_uid,
        "actor_kind": actor_kind,
        "seq": seq,
        "seq_aliases": [],
        "stage": stage,
        "job_ref": None,
        "pre_turn_index": None,
        "turn_index": None,
        "submitted_text_sha256": None,
        "submitted_text_len": None,
        "send_intents": [],
        "send_receipts": [],
        "trace_refs": None,
        "skip_reason": skip_reason,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }


def initialize_delivery_state(
    path: str | Path,
    *,
    native_session_id: str,
    baseline_acked: int,
    historical_message_ids: dict[Any, Any],
    authorized_skips: list[dict[str, Any]],
) -> dict[str, Any]:
    """Create a fresh delivery state file; refuse when the path already exists.

    Returns a snapshot of the new state. ``historical_message_ids`` maps
    channel -> [MIDs]; ``authorized_skips`` entries require ``channel_id``,
    ``message_id``, ``seq``, ``author_uid``, ``actor_kind``, ``reason``.
    Each authorized skip becomes a terminal ``skipped`` job.
    """
    file = Path(path)
    if os.path.lexists(file):
        raise DeliveryError(f"delivery state {file} already exists; refusing to overwrite")
    _ensure_parent_dir(file)
    if os.path.lexists(file):
        raise DeliveryError(f"delivery state {file} already exists; refusing to overwrite")
    if (
        not isinstance(native_session_id, str)
        or not (1 <= len(native_session_id) <= MAX_ID_LEN)
        or native_session_id.strip() != native_session_id
        or not native_session_id
        or _has_whitespace(native_session_id)
    ):
        raise DeliveryError(f"invalid native_session_id for delivery state {file}")
    if not _is_strict_int(baseline_acked) or baseline_acked < 0:
        raise DeliveryError(f"invalid baseline_acked for delivery state {file}")
    historical = _validate_historical(historical_message_ids, file)
    if not isinstance(authorized_skips, list):
        raise DeliveryError(f"invalid authorized_skips for delivery state {file}")
    skips = [
        _validate_authorized_skip(item, file, baseline_acked, i)
        for i, item in enumerate(authorized_skips)
    ]
    skip_keys = [(s["channel_id"], s["message_id"]) for s in skips]
    if len(set(skip_keys)) != len(skip_keys):
        raise DeliveryError(f"duplicate authorized skip for delivery state {file}")
    skip_seqs = [s["seq"] for s in skips]
    if len(set(skip_seqs)) != len(skip_seqs):
        raise DeliveryError(f"duplicate authorized skip seq for delivery state {file}")
    historical_pairs = {(ch, mid) for ch, mids in historical.items() for mid in mids}
    for item in skips:
        if (item["channel_id"], item["message_id"]) in historical_pairs:
            raise DeliveryError(f"authorized skip overlaps historical for delivery state {file}")
    jobs: dict[str, dict[str, Any]] = {}
    for item in skips:
        key = _job_key(item["channel_id"], item["message_id"])
        jobs[key] = _new_job(
            channel_id=item["channel_id"],
            message_id=item["message_id"],
            author_uid=item["author_uid"],
            actor_kind=item["actor_kind"],
            seq=item["seq"],
            stage="skipped",
            skip_reason=item["reason"],
        )
    state: dict[str, Any] = {
        "version": DELIVERY_VERSION,
        "native_session_id": native_session_id,
        "baseline_acked": baseline_acked,
        "historical_message_ids": historical,
        "authorized_skips": skips,
        "jobs": jobs,
    }
    # Validate the assembled state before touching disk.
    state = _validate_state(json.loads(json.dumps(state)), file)
    fd, tmp_name = tempfile.mkstemp(dir=str(file.parent), prefix=file.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp_path, file)
        except FileExistsError as e:
            raise DeliveryError(
                f"delivery state {file} already exists; refusing to overwrite"
            ) from e
    except BaseException:
        # Own temp cleanup only; the state path is never touched here.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    # Exclusive hard-link commit succeeded: drop our temp link (best
    # effort), then fsync the parent for namespace durability. A failing
    # directory fsync raises DeliveryDurabilityUncertain with the state
    # already present; later init still refuses it (exclusive bootstrap).
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass
    _fsync_parent_dir(file.parent, file)
    return copy.deepcopy(state)


def load_delivery_state(path: str | Path) -> dict[str, Any]:
    """Load and validate delivery state; missing/corrupt fails loudly."""
    return copy.deepcopy(_load_state(path))


def _require_job(state: dict[str, Any], path: Path, channel_id: int, message_id: int) -> str:
    if not _is_strict_int(channel_id) or channel_id <= 0:
        raise DeliveryError(f"invalid channel_id for delivery state {path}")
    if not _is_strict_int(message_id) or message_id <= 0:
        raise DeliveryError(f"invalid message_id for delivery state {path}")
    key = _job_key(channel_id, message_id)
    if key not in state["jobs"]:
        raise DeliveryError(f"delivery state {path}: unknown job {key}")
    return key


def _used_seqs(state: dict[str, Any], exclude_key: str | None = None) -> set[int]:
    used: set[int] = set()
    for key, job in state["jobs"].items():
        if exclude_key is not None and key == exclude_key:
            continue
        used.add(job["seq"])
        used.update(job["seq_aliases"])
    return used


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
        job["updated_at"] = _now_iso()
        _save_state(file, state)
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
    _save_state(file, state)
    return copy.deepcopy(state["jobs"][key])


def get_job(path: str | Path, channel_id: int, message_id: int) -> dict[str, Any] | None:
    """Return a job snapshot, or None when the job key is unknown."""
    file = Path(path)
    if not _is_strict_int(channel_id) or channel_id <= 0:
        raise DeliveryError(f"invalid channel_id for delivery state {file}")
    if not _is_strict_int(message_id) or message_id <= 0:
        raise DeliveryError(f"invalid message_id for delivery state {file}")
    state = _load_state(file)
    job = state["jobs"].get(_job_key(channel_id, message_id))
    return copy.deepcopy(job) if job is not None else None


def list_jobs(path: str | Path) -> list[dict[str, Any]]:
    """Return job snapshots sorted by primary seq ascending."""
    state = _load_state(path)
    jobs = sorted(
        state["jobs"].values(), key=lambda j: (j["seq"], j["channel_id"], j["message_id"])
    )
    return copy.deepcopy(jobs)


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
        job["updated_at"] = _now_iso()
        _save_state(file, state)
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
        job["updated_at"] = _now_iso()
        _save_state(file, state)
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
        job["updated_at"] = _now_iso()
        _save_state(file, state)
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
    ``uncertain``/``delivering``/``sent``/``completed``/``skipped`` are
    refused. Any status other than strict-int 409 (503/408/None/str/bool)
    is refused. Clears job_ref/pre_turn_index/turn_index/fingerprint/error;
    keeps primary source, seq aliases, created timestamp and unknown fields,
    and refreshes the updated timestamp. A NEW
    attempt uses a fresh job_ref. No automatic POST is ever triggered.
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
    job["job_ref"] = None
    job["pre_turn_index"] = None
    job["turn_index"] = None
    job["submitted_text_sha256"] = None
    job["submitted_text_len"] = None
    job["error"] = None
    job["stage"] = "queued"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
            "ts": _now_iso(),
        }
    )
    job["stage"] = "delivering"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    if job["stage"] in ("completed", "skipped"):
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
        "ts": _now_iso(),
    }
    if job["stage"] == "uncertain":
        # Late SDK receipt for an EXISTING pending intent after UNCERTAIN:
        # keep stage + error untouched (never auto-sent/completed). Not
        # persisting this MID would lose the only send evidence.
        job["send_receipts"].append(receipt)
        job["updated_at"] = _now_iso()
        _save_state(file, state)
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
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    job["trace_refs"] = {"tool_call_ids": list(tool_call_ids), "ts": _now_iso()}
    job["error"] = None
    job["stage"] = "completed"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
    return copy.deepcopy(job)


def skip_job(
    path: str | Path,
    channel_id: int,
    message_id: int,
    *,
    reason: str,
) -> dict[str, Any]:
    """Move any non-terminal job -> ``skipped`` with an explicit reason."""
    file = Path(path)
    if not isinstance(reason, str) or reason not in SKIP_REASONS:
        raise DeliveryError(f"invalid skip reason for delivery state {file}")
    state = _load_state(file)
    key = _require_job(state, file, channel_id, message_id)
    job = state["jobs"][key]
    if job["stage"] == "skipped":
        if job["skip_reason"] == reason:
            return copy.deepcopy(job)
        raise DeliveryError(f"delivery state {file}: job {key} skip reason mismatch")
    if job["stage"] in ("completed", "uncertain"):
        raise DeliveryError(f"delivery state {file}: job {key} stage {job['stage']} cannot skip")
    job["skip_reason"] = reason
    job["stage"] = "skipped"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    if job["stage"] in ("completed", "skipped"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot mark uncertain"
        )
    job["error"] = {"kind": kind, "http_status": http_status, "ts": _now_iso()}
    job["stage"] = "uncertain"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
    return copy.deepcopy(job)


def processed_seq_prefix(
    path: str | Path,
    ordered_entries: list[dict[str, Any]],
    current_acked: int,
) -> int:
    """Highest ACTUAL seen seq resolved via historical/completed/skipped.

    Caller precondition: ``ordered_entries`` is the AUTHORITATIVE FULL
    UNFILTERED unread outbox prefix (this helper CANNOT detect omitted
    outbox rows; feeding selected rows can over-ack). Entries are strict
    positive ``channel_id``/``message_id``/``seq`` ints ascending by
    ``seq`` (gaps allowed) plus optional ``author_uid``/``actor_kind``
    which are type-validated (actor_kind str before enum) and must MATCH
    the known job before any ack. Every supplied entry is cross-checked
    against fresh state, including seqs at/below ``current_acked``: a seq
    whose KNOWN source (primary/alias of a job) is a different MID than
    the row supplies is a contradiction and raises -- never a silent stop
    and never a bypass of historical resolution. The first entry whose MID
    is not historical and whose job is not ``completed``/``skipped`` with
    an exact (channel, mid, seq) match stops the prefix. Historical MIDs
    without a job get format checks only (deployment-proven baseline).
    Never mutates the file; errors carry field/index metadata only, never
    entry content.
    """
    file = Path(path)
    if not _is_strict_int(current_acked) or current_acked < 0:
        raise DeliveryError(f"invalid current_acked for delivery state {file}")
    if not isinstance(ordered_entries, list):
        raise DeliveryError(f"invalid ordered_entries for delivery state {file}")
    state = _load_state(file)
    baseline = state["baseline_acked"]
    if current_acked < baseline:
        raise DeliveryError(f"delivery state {file}: current_acked below baseline_acked")
    historical_pairs = {
        (ch, mid) for ch, mids in state["historical_message_ids"].items() for mid in mids
    }
    jobs = state["jobs"]
    seq_owner: dict[int, tuple[int, int]] = {}
    for job in jobs.values():
        owner_pair = (job["channel_id"], job["message_id"])
        for owned_seq in [job["seq"], *job["seq_aliases"]]:
            seq_owner[owned_seq] = owner_pair
    previous: int | None = None
    # Pass 1: entry schema (object/keys/strict positive ints/order).
    for index, entry in enumerate(ordered_entries):
        if not isinstance(entry, dict):
            raise DeliveryError(f"delivery state {file}: ordered entry #{index} must be an object")
        for name in ("channel_id", "message_id", "seq"):
            if name not in entry:
                raise DeliveryError(
                    f"delivery state {file}: ordered entry #{index} missing key {name!r}"
                )
            value = entry[name]
            if not _is_strict_int(value) or value <= 0:
                raise DeliveryError(
                    f"delivery state {file}: ordered entry #{index} field {name!r} "
                    "must be a positive int"
                )
        seq = entry["seq"]
        if previous is not None and seq <= previous:
            raise DeliveryError(f"delivery state {file}: ordered entries must strictly increase")
        previous = seq
    # Pass 2: known seq ownership + optional identity, for EVERY entry
    # including already-acked seqs (global cross-ownership validation).
    for index, entry in enumerate(ordered_entries):
        seq = entry["seq"]
        pair = (entry["channel_id"], entry["message_id"])
        job = jobs.get(_job_key(*pair))
        owner = seq_owner.get(seq)
        if owner is not None and owner != pair:
            # Contradiction: the state knows this seq as another source
            # (primary/alias). Reject loudly instead of a silent stop or a
            # historical-resolution bypass.
            raise DeliveryError(
                f"delivery state {file}: ordered entry #{index} seq ownership mismatch"
            )
        if "author_uid" in entry:
            author = entry["author_uid"]
            if not _is_strict_int(author) or author <= 0:
                raise DeliveryError(
                    f"delivery state {file}: ordered entry #{index} field 'author_uid' "
                    "must be a positive int"
                )
            if job is not None and job["author_uid"] != author:
                raise DeliveryError(
                    f"delivery state {file}: ordered entry #{index} author mismatch"
                )
        if "actor_kind" in entry:
            actor = entry["actor_kind"]
            if not isinstance(actor, str) or actor not in ACTOR_KINDS:
                raise DeliveryError(
                    f"delivery state {file}: ordered entry #{index} has invalid actor_kind"
                )
            if job is not None and job["actor_kind"] != actor:
                raise DeliveryError(f"delivery state {file}: ordered entry #{index} actor mismatch")
    result = current_acked
    for entry in ordered_entries:
        seq = entry["seq"]
        if seq <= result:
            continue
        pair = (entry["channel_id"], entry["message_id"])
        if pair in historical_pairs:
            result = seq
            continue
        job = jobs.get(_job_key(*pair))
        if job is None or job["stage"] not in ("completed", "skipped"):
            break
        if seq not in set([job["seq"], *job["seq_aliases"]]):
            break
        result = seq
    return result
