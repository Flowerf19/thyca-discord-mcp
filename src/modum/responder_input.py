"""Pure responder input validation (no I/O, no Root state).

Canonical owner of :class:`ResponderError`, the strict metadata helpers,
``ID_MAX``, and the pure historical/entry frame predicates. The orchestrator
(:mod:`modum.responder`) imports and re-exports these names so
existing ``responder.ResponderError`` / ``responder._is_strict_*`` imports
keep working with identical object identity.

No I/O and no private Root is passed here: callers supply explicit UIDs.
"""

from __future__ import annotations

from typing import Any

from .identity import classify_actor

ID_MAX = 128


class ResponderError(ValueError):
    """Coarse responder failure (never carries raw bodies or provider text)."""


def _is_strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_strict_positive_int(value: Any) -> bool:
    return _is_strict_int(value) and value > 0


def _validate_id_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if not (1 <= len(value) <= ID_MAX):
        return None
    if value.strip() != value or not value:
        return None
    for ch in value:
        if ch.isspace():
            return None
    return value


def _is_strict_ledger_id(value: Any) -> bool:
    """Strict canonical ledger activation identity: exactly 32 lower hex."""
    if not isinstance(value, str) or len(value) != 32:
        return False
    for ch in value:
        if ch not in "0123456789abcdef":
            return False
    return True


def historical_pairs(historical: Any) -> set[tuple[int, int]]:
    """Pure historical (channel_id, message_id) set (no Root fields)."""
    pairs: set[tuple[int, int]] = set()
    if not isinstance(historical, dict):
        return pairs
    for key, mids in historical.items():
        try:
            channel_id = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(mids, list):
            continue
        for mid in mids:
            if _is_strict_positive_int(mid) and _is_strict_positive_int(channel_id):
                pairs.add((channel_id, mid))
    return pairs


def validate_entry(entry: Any, *, bot_uid: int, owner_uid: int) -> dict[str, Any]:
    """Strict source-frame validation BEFORE any return/side effect.

    The outbox only guarantees required-field presence and seq order, so
    every frame is checked here (including historical/skipped MIDs):
    positive IDs are strict non-bool ints, ``type`` is exactly
    ``message``/``poll``, ``author_name``/``content`` are the actual
    source strings (never used for auth), and ``actor_kind`` must MATCH
    the recomputed UID classification (names, content, typed IDs, and
    display names never authenticate). Optional ``author_is_bot`` must be
    a strict bool when present (absent means legacy unknown -> SDK
    resolve, never assumed False); ``reply_to_message_id`` must be None
    or a positive strict int. Guild scope comes from trusted producer /
    settings (no guild_id in the v4 outbox format).
    """
    if not isinstance(entry, dict):
        raise ResponderError("outbox entry must be an object")
    for key in ("type", "author_name", "content"):
        if not isinstance(entry.get(key), str):
            raise ResponderError(f"outbox entry has invalid {key}")
    if entry["type"] not in ("message", "poll"):
        raise ResponderError("outbox entry has invalid type")
    values: dict[str, int] = {}
    for key in ("channel_id", "message_id", "author_uid", "seq"):
        value = entry.get(key)
        if not _is_strict_positive_int(value):
            raise ResponderError(f"outbox entry has invalid {key}")
        values[key] = value
    author_uid = values["author_uid"]
    actor = classify_actor(author_uid, "", bot_uid=bot_uid, owner_uid=owner_uid)
    actor_kind = actor.kind.value
    if entry.get("actor_kind") != actor_kind:
        raise ResponderError("outbox entry actor_kind contradicts UID classification")
    if "author_is_bot" in entry and not isinstance(entry["author_is_bot"], bool):
        raise ResponderError("outbox entry has invalid author_is_bot")
    if "reply_to_message_id" in entry:
        ref = entry["reply_to_message_id"]
        if ref is not None and not _is_strict_positive_int(ref):
            raise ResponderError("outbox entry has invalid reply_to_message_id")
    return {
        "channel_id": values["channel_id"],
        "message_id": values["message_id"],
        "author_uid": author_uid,
        "seq": values["seq"],
        "actor_kind": actor_kind,
    }
