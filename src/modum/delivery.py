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

from .delivery_schema import (  # noqa: F401 - intentional canonical re-exports, same objects
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
from .delivery_storage import (
    _create_state_exclusive as _storage_create_state_exclusive,
)
from .delivery_storage import (
    _ensure_parent_dir,
    _fsync_parent_dir,
    _load_state,
)
from .delivery_storage import (
    _save_state as _storage_save_state,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _save_state(path: str | Path, state: dict[str, Any]) -> None:
    """Atomically replace an existing state file (root forwarding wrapper).

    Forwards to storage with the CURRENT root fsync hook explicitly, so
    patching ``delivery._fsync_parent_dir`` faults all mutation commits.
    The hook is looked up freshly at each call (never a bound default).
    """
    _storage_save_state(path, state, fsync_parent_dir=_fsync_parent_dir)


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
    _storage_create_state_exclusive(file, state, fsync_parent_dir=_fsync_parent_dir)
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
    if job["stage"] in ("completed", "skipped", "observed"):
        raise DeliveryError(
            f"delivery state {file}: job {key} stage {job['stage']} cannot mark uncertain"
        )
    job["error"] = {"kind": kind, "http_status": http_status, "ts": _now_iso()}
    job["stage"] = "uncertain"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
    return copy.deepcopy(job)


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
        _save_state(file, state)
        return copy.deepcopy(state)
    if existing == ledger_id:
        return copy.deepcopy(state)
    raise DeliveryError(f"delivery state {file}: jev ledger conflict")


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
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    job["observation"] = {"origin": "jev", "ts": _now_iso()}
    job["stage"] = "observed"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    job["no_reply_declaration"] = {"job_ref": job_ref, "reason": reason, "ts": _now_iso()}
    job["updated_at"] = _now_iso()
    _save_state(file, state)
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
    job["trace_refs"] = {"tool_call_ids": list(tool_call_ids), "ts": _now_iso()}
    job["observation"] = {"origin": "thyca", "ts": _now_iso()}
    job["error"] = None
    job["stage"] = "observed"
    job["updated_at"] = _now_iso()
    _save_state(file, state)
    return copy.deepcopy(job)


def processed_seq_prefix(
    path: str | Path,
    ordered_entries: list[dict[str, Any]],
    current_acked: int,
) -> int:
    """Highest ACTUAL seen seq resolved via historical/completed/skipped/observed.

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
    is not historical and whose job is not ``completed``/``skipped``/``observed``
    with an exact (channel, mid, seq) match stops the prefix. Observed jobs
    count only after load validation proved zero send evidence plus a valid
    Jev or Thyca observation proof. Historical MIDs
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
        if job is None or job["stage"] not in ("completed", "skipped", "observed"):
            break
        if seq not in set([job["seq"], *job["seq_aliases"]]):
            break
        result = seq
    return result
