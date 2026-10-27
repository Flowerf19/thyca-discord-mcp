"""Pure snapshot assembly for the scoped context collector.

No Discord I/O and no ``discord``/``bot`` imports: participants/messages
builders plus the shared Jev/Thyca fit loop and final projection. The
cached-display-name lookup arrives as a ``fallback_name(uid)`` callable so
this module never touches the bot cache. All bounded failures raise
:class:`ContextUnavailable` with fixed strings (never raw bodies).

Importing this module performs no I/O.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any

from conversation import ConversationError, project_conversation_state
from thyca import ThycaPromptError, build_conversation_prompt

MAX_MESSAGES_LIMIT = 20
FIT_NONCE = "f" * 32
FIT_HASH = "f" * 64


class ContextUnavailable(ValueError):
    """Bounded context failure (never carries raw bodies or provider text)."""


def _placeholder_name(uid: int) -> str:
    return f"uid-{uid}"


def context_fingerprint(state: Mapping[str, Any]) -> str:
    """Return the SHA-256 hex of the canonical projected JSON for ``state``.

    Accepts raw caller state or an already-projected snapshot; the state is
    validated through :func:`conversation.project_conversation_state`
    (unknown/gold keys dropped, :class:`ConversationError` on violations).
    The input is never mutated; the hash covers the detached snapshot only.
    """
    snapshot = project_conversation_state(state)
    canonical = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_participants(
    pool: list[dict[str, Any]],
    *,
    author_uid: int,
    author_name: str,
    owner_uid: int,
    bot_uid: int,
    fallback_name: Callable[[int], str | None],
) -> list[dict[str, str]]:
    """Build the ``participants`` list for ``pool`` plus the source sender.

    Newest display name wins per UID (rename-safe, still exact SDK text);
    names never authenticate. ``fallback_name`` is the sync cache-only
    lookup (guild member cache, then user cache, then the already-available
    ``bot.user.display_name``); when a UID sent no message in the window,
    its result is used before falling back to a ``uid-<uid>`` placeholder
    that never claims owner auth.
    """
    newest_name: dict[int, str] = {}
    # Pool is ascending; later (newer) overwrites older.
    for item in pool:
        newest_name[item["sender_uid"]] = item["name"]
    # Source is newest overall.
    newest_name[author_uid] = author_name
    uids: set[int] = {item["sender_uid"] for item in pool}
    uids.add(author_uid)
    uids.add(owner_uid)
    uids.add(bot_uid)
    parts: list[dict[str, str]] = []
    for uid in sorted(uids):
        if uid in newest_name:
            parts.append({"uid": str(uid), "display_name": newest_name[uid]})
            continue
        cached = fallback_name(uid)
        parts.append({"uid": str(uid), "display_name": cached or _placeholder_name(uid)})
    return parts


def build_messages(
    pool: list[dict[str, Any]],
    *,
    message_id: int,
    author_uid: int,
    content: str,
    reply_to: int | None,
    timestamp: str,
) -> list[dict[str, Any]]:
    """Build the ``messages`` list for ``pool`` plus the current source.

    Pool entries are ordered ascending; the source (newest overall) is
    appended last. All chosen bodies are complete (never truncated).
    """
    ordered = sorted(pool, key=lambda item: item["id"])
    out: list[dict[str, Any]] = []
    for item in ordered:
        msg: dict[str, Any] = {
            "id": str(item["id"]),
            "sender_uid": str(item["sender_uid"]),
            "text": item["text"],
        }
        if item["reply_to"] is not None:
            msg["reply_to"] = str(item["reply_to"])
        msg["timestamp"] = item["timestamp"]
        out.append(msg)
    current: dict[str, Any] = {
        "id": str(message_id),
        "sender_uid": str(author_uid),
        "text": content,
    }
    if reply_to is not None:
        current["reply_to"] = str(reply_to)
    current["timestamp"] = timestamp
    out.append(current)
    return out


def fit_and_project(
    pool: list[dict[str, Any]],
    *,
    mandatory_ids: set[int],
    renderer_entry: dict[str, Any],
    owner_uid: int,
    guild_id: int,
    bot_uid: int,
    author_uid: int,
    author_name: str,
    message_id: int,
    content: str,
    reply_to: int | None,
    timestamp: str,
    fallback_name: Callable[[int], str | None],
) -> dict[str, Any]:
    """Fit the candidate snapshot against both routed prompts, then project.

    Shared Jev/Thyca fit BEFORE any paid call: the full candidate snapshot
    is checked against the ACTUAL final routed prompts for BOTH ``respond``
    and ``need_context``
    (:func:`thyca_prompt.build_conversation_prompt` with paired
    ``participation``/``context_sha256``) with a fixed 32-char fake nonce
    (``"f"*32``) and a fixed 64-char fake hash (``"f"*64``) -- neither
    stored. No guessed reserve: both routed prompts must render ``<=4000``
    via the real renderer. When either cannot fit, the oldest OPTIONAL
    message only is removed (never the source/visible parent, never
    truncates a body) and the check repeats; failure when the mandatory
    pair alone cannot fit either path (a longer ``need_context`` task may
    genuinely fail). Unknown/binding renderer failures fail immediately
    (no drop). Returns the detached canonical snapshot.
    """
    # Fit loop: full newest-first candidate, then drop the oldest
    # OPTIONAL only until BOTH actual routed prompts (respond and
    # need_context) render via the real renderer. Bodies are never
    # truncated; mandatory pair never drops. No guessed reserve.
    pool = list(pool)
    while True:
        candidate = {
            "assistant_uid": str(bot_uid),
            "owner_uid": str(owner_uid),
            "participants": build_participants(
                pool,
                author_uid=author_uid,
                author_name=author_name,
                owner_uid=owner_uid,
                bot_uid=bot_uid,
                fallback_name=fallback_name,
            ),
            "messages": build_messages(
                pool,
                message_id=message_id,
                author_uid=author_uid,
                content=content,
                reply_to=reply_to,
                timestamp=timestamp,
            ),
            "new_message_id": str(message_id),
        }
        try:
            for participation in ("respond", "need_context"):
                build_conversation_prompt(
                    candidate,
                    dict(renderer_entry),
                    FIT_NONCE,
                    owner_uid=owner_uid,
                    guild_id=guild_id,
                    bot_uid=bot_uid,
                    participation=participation,
                    context_sha256=FIT_HASH,
                )
        except ConversationError:
            raise ContextUnavailable("context snapshot invalid") from None
        except ThycaPromptError as exc:
            message = str(exc)
            if "exceeds limit" not in message:
                raise ContextUnavailable("context snapshot invalid") from None
            drop_at = None
            for item in sorted(pool, key=lambda i: i["id"]):
                if item["id"] not in mandatory_ids:
                    # Map back to pool order (pool stays ascending).
                    drop_at = pool.index(item)
                    break
            if drop_at is None:
                raise ContextUnavailable("context mandatory pair oversize") from None
            del pool[drop_at]
            continue
        try:
            return project_conversation_state(candidate)
        except ConversationError:
            raise ContextUnavailable("context snapshot invalid") from None
