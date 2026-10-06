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
      "jev_ledger_id": None|<32hex ledger marker>,
      ...unknown top-level keys preserved
    }

Top-level ``jev_ledger_id`` is an OPTIONAL activation-storage marker
(None when unbound; old files missing the key load as None). It binds
the delivery file to one immutable private Jev ledger (exact 32hex).
Router persists it AFTER exclusive ledger init+fsync BEFORE any paid
call; a later marker+missing ledger file fails BEFORE auto-create, and
a mismatching ledger file id fails, preventing budget reset/rebill
after loss. Ordinary metadata only; never a raw path or chat.

Job (all IDs are strict positive ints, never bool)::

    {
      "channel_id","message_id","author_uid","actor_kind","seq",
      "seq_aliases": [extra seqs for same MID, sorted, unique],
      "stage": queued|turn_submitting|inflight|delivering|sent|
               completed|skipped|uncertain|observed,
      "job_ref": str|None, "pre_turn_index": int|None,
      "turn_index": int|None,
      "submitted_text_sha256": str|None, "submitted_text_len": int|None,
      "send_intents": [{"send_ref","content_len","content_hash","ts"}],
      "send_receipts": [{"send_ref","sent_message_id",
                         "delivered_channel_id","jump_url",
                         "reply_to_message_id","ts"}],
      "trace_refs": None|{"tool_call_ids","ts"},
      "skip_reason": None|str, "error": None|{"kind","http_status","ts"},
      "jev_decision": None|{"participation","context_sha256",
                         "attempt_id","model"},
      "no_reply_declaration": None|{"job_ref","reason","ts"},
      "observation": None|{"origin","ts"},
      "created_at","updated_at", ...unknown job keys preserved
    }

Metadata only: intents store length/hash, receipts store IDs/URL, errors
store coarse ``kind`` + optional HTTP status, Jev stores compact
participation/context-sha/attempt/model only. No message body, reply text,
Jev answers, profile, or freeform provider error is ever accepted/stored.

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

Stages / transitions (terminal: completed, skipped, uncertain, observed;
 safe ACK: completed, skipped, observed; uncertain never ACKs):
- ``queued`` -> ``turn_submitting`` (mark_turn_submitting; ``pre_turn_index
  = None`` means a KNOWN empty native session, never an unknown/faked
  index), ``turn_submitting`` -> ``inflight`` (mark_inflight binds the
  ACTUAL native turn index after the confirmed own POST/Trace),
  ``turn_submitting``/``inflight``/``delivering``/``sent`` -> ``delivering``
  (record_send_intent, one per send_ref, multi-chunk allowed; a deferred
  SDK callback during the synchronous POST await records real sends while
  ``turn_index`` is still None; refused when a no_reply_declaration exists),
  ``delivering`` -> ``sent`` (record_send_receipt when ALL intents have
  receipts), ``sent`` -> ``completed`` (complete_job, receipts already
  durable, caller verified native Trace externally),
  ``uncertain`` -> ``completed`` (complete_job only when ALL intents are
  receipted, the native ``turn_index`` is bound and matches, and
  non-empty verified tool_call_ids are supplied; the stored error is
  cleared), ``turn_submitting``/``inflight`` -> ``queued``
  (requeue_definitive_busy, ONLY a definitive native POST 409 with zero
  intents/receipts/trace/declaration; a declaration always refuses),
  any non-terminal -> ``skipped`` (skip_job, explicit reason),
  any non-terminal -> ``uncertain`` (mark_uncertain, coarse kind),
  ``queued`` -> ``observed`` (observe_job, ONLY a bound Jev ``observe``
  with zero nonce/index/fingerprint/evidence plus durable Jev proof),
  ``inflight``/zero-evidence-``uncertain`` -> ``observed``
  (complete_observation, ONLY a bound Jev ``need_context`` + exact
  no_reply_declaration + bound matching turn_index + nonempty unique
  traced tool_call_ids; clears an uncertain error).
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
- ``bind_jev_decision(path, ch, mid, *, context_sha256, attempt_id, model,
  participation) -> dict`` (fresh queued only; compact metadata, exact
  idempotent; never raw answers).
- ``bind_jev_ledger(path, *, ledger_id) -> dict`` (top-level activation
  marker; None -> exact 32hex once, exact repeat idempotent, different
  id conflicts; never touches SID/jobs/ACK; old files load None).
- ``observe_job(path, ch, mid) -> dict`` (queued + bound Jev observe +
  zero evidence -> observed with Jev proof).
- ``record_no_reply(path, ch, mid, *, job_ref, reason) -> dict``
  (turn_submitting/inflight + bound need_context + exact nonce + zero
  evidence; immutable; never terminals/ACKs alone).
- ``complete_observation(path, ch, mid, *, turn_index, tool_call_ids)``
  (inflight or zero-evidence uncertain + need_context + exact declaration
  + bound index + nonempty unique traced IDs -> observed with Thyca proof).
- ``skip_job(..., *, reason)``, ``mark_uncertain(..., *, kind,
  http_status=None)``.
- ``processed_seq_prefix(path, ordered_entries, current_acked) -> int``
  highest ACTUAL seen seq whose MID is historical or whose job is
  completed/skipped/observed with exact (channel,mid,seq) match; stops at the
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import delivery as _delivery_root
import delivery_storage as _delivery_storage_mod
from delivery_schema import (  # noqa: F401 - canonical re-exports, same objects
    ACTOR_KINDS,
    DELIVERY_VERSION,
    JEV_MODEL_PINNED,
    JEV_PARTICIPATIONS,
    MAX_ID_LEN,
    MAX_URL_LEN,
    NO_REPLY_REASONS,
    OBSERVATION_ORIGINS,
    SAFE_TERMINAL_STAGES,
    SHA256_HEX_LEN,
    SKIP_REASONS,
    STAGES,
    SUBMITTED_TEXT_MAX,
    TERMINAL_STAGES,
    UNCERTAIN_KINDS,
    DeliveryDurabilityUncertain,
    DeliveryError,
    _has_whitespace,
    _is_attempt_id,
    _is_hex,
    _is_sha256_hex,
    _is_strict_int,
    _job_key,
    _parse_job_key,
    _submitted_pair_ok,
    _validate_authorized_skip,
    _validate_historical,
    _validate_id_string,
    _validate_intent,
    _validate_jev_decision,
    _validate_job,
    _validate_no_reply_declaration,
    _validate_observation,
    _validate_receipt,
    _validate_state,
)
from delivery_storage import (
    _create_state_exclusive as _storage_create_state_exclusive,
)
from delivery_storage import (
    _ensure_parent_dir,
    _load_state,
)
from delivery_storage import (
    _save_state as _storage_save_state,
)

# Same object as storage; exposed for the root re-export. Never used

# bare internally: all call sites use _delivery_root._fsync_parent_dir.

_fsync_parent_dir = _delivery_storage_mod._fsync_parent_dir


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _save_state(path: str | Path, state: dict[str, Any]) -> None:
    """Atomically replace an existing state file (root forwarding wrapper).

    Forwards to storage with the CURRENT root fsync hook explicitly, so
    patching ``delivery._fsync_parent_dir`` faults all mutation commits.
    The hook is looked up freshly at each call (never a bound default).
    """
    # Hook contract (tested): fsync hook supplied explicitly,
    # resolved on _delivery_root at call time.
    _storage_save_state(path, state, fsync_parent_dir=_delivery_root._fsync_parent_dir)


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
    now = _delivery_root._now_iso()
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
        "jev_decision": None,
        "no_reply_declaration": None,
        "observation": None,
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
        "jev_ledger_id": None,
    }
    # Validate the assembled state before touching disk.
    state = _validate_state(json.loads(json.dumps(state)), file)
    # Hook contract (tested): fsync hook supplied explicitly,
    # resolved on _delivery_root at call time.
    _storage_create_state_exclusive(file, state, fsync_parent_dir=_delivery_root._fsync_parent_dir)
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


def bind_jev_ledger(
    path: str | Path,
    *,
    ledger_id: str,
) -> dict[str, Any]:
    """Bind the top-level Jev ledger marker once (activation storage proof).

    ``ledger_id`` is an exact 32hex immutable marker for the private Jev
    ledger file. Only an unbound state (``jev_ledger_id is None``, incl.
    old files missing the key) can bind; the write is atomic (temp
    inode + fsync + replace + parent fsync) with unknown top-level
    fields preserved. An exact repeat is idempotent with no file
    rewrite; a different id conflicts loudly with no overwrite. Never
    touches SID/jobs/ACK. Router persists this AFTER exclusive private
    ledger init+fsync BEFORE any paid call; ordinary metadata only.
    """
    file = Path(path)
    if not _is_attempt_id(ledger_id):
        raise DeliveryError(f"invalid ledger_id for delivery state {file}")
    state = _load_state(file)
    existing = state.get("jev_ledger_id")
    if existing is None:
        state["jev_ledger_id"] = ledger_id
        _delivery_root._save_state(file, state)
        return copy.deepcopy(state)
    if existing == ledger_id:
        return copy.deepcopy(state)
    raise DeliveryError(f"delivery state {file}: jev ledger conflict")
