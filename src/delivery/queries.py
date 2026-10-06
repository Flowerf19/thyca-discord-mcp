from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from delivery_schema import (
    ACTOR_KINDS,
    DeliveryError,
    _is_strict_int,
    _job_key,
)

from .store import _load_state


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
