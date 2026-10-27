"""Pure responder input validation (no I/O, no Root state).

Canonical owner of :class:`ResponderError`, the strict metadata helpers,
and the pure historical/entry frame predicates. ``ID_MAX`` and
``_validate_id_string`` are re-exported from :mod:`validate` (the shared
stdlib-only home) so existing ``responder.*`` imports keep working with
identical object identity. The orchestrator (:mod:`responder`) imports
and re-exports these names so existing ``responder.ResponderError`` /
``responder._is_strict_*`` imports keep working with identical object
identity.

No I/O and no private Root is passed here: callers supply explicit UIDs.
"""

from __future__ import annotations

from typing import Any

from identity import classify_actor
from validate import ID_MAX  # noqa: F401 - re-exported via responder
from validate import is_strict_int as _is_strict_int  # noqa: F401 - re-exported via responder
from validate import is_strict_positive_int as _is_strict_positive_int
from validate import (
    validate_id_string as _validate_id_string,  # noqa: F401 - re-exported via responder
)


class ResponderError(ValueError):
    """Coarse responder failure (never carries raw bodies or provider text)."""


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
