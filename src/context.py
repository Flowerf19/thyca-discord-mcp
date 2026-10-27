"""Scoped conversation-context collector for one approved channel.

Builds one canonical snapshot (``conversation``-compatible) for the
exact current source message plus recent same-channel context. A future
parent wiring will call the collector, then feed the SAME snapshot to Jev
and the Thyca renderer; this module performs no routing, budgeting,
inference, sends, or paid calls.

Scope contract (checked on every call, no retries, no guessing):
- Approved guild/channel only: ``guild_id``/``channel_id`` are strict
  positive ints. Resolution reuses the existing scoped helpers
  (``bot.get_scoped_guild`` +
  ``source_messages._resolve_text_channel``): cache-first
  guild/channel lookup with one ``bot.fetch_channel`` fallback, then strict
  ``isinstance(discord.TextChannel)`` plus guild-scope ID checks. Voice and
  stage channels, threads, and DMs are refused independent of class names.
  Every fetched source/parent/history message must carry ``channel.id``/
  ``guild.id`` equal to the approved scope; every reference ``channel_id``/
  ``guild_id`` is verified the same way.
- Source proof is one native ``channel.fetch_message`` read for the EXACT
  entry MID: a strict NATIVE ``discord.NotFound`` with status 404 + code
  10008 (``source_messages.is_unknown_message``) raises the authoritative
  ``source_messages.SourceDeleted`` (re-exported here,
  same object -- the router and the existing checker catch that class).
  Every other fetch failure -- Forbidden, 5xx, timeouts, unknown-channel
  codes, scope mismatch -- raises
  :class:`ContextUnavailable` and is NEVER treated as deletion.
  Cancellation propagates untouched.
- The trusted captured ``entry`` (``channel_id``/``message_id``/
  ``author_uid``/``content``) must agree EXACTLY with the fetched source
  (MID/UID/content byte-identical). An edited source raises
  :class:`ContextUnavailable`; the collector never reinterprets the entry
  from fetched text, names, or typed IDs.
- History is the newest ``<=max_messages-1`` messages BEFORE the source
  via ``channel.history`` with ``before=source`` and ``oldest_first=False``
  (the SDK pages newest-adjacent only in before-strategy; ``True`` would
  page from channel creation via ``after=OLDEST_OBJECT`` and return the
  oldest-ever messages, filtered but wrong). Results are sorted ascending
  locally. Live messages newer than the source are never included. Any
  history message with an ID ``>=`` source, a duplicate ID, or a scope
  mismatch fails :class:`ContextUnavailable` instead of being skipped.
- The current exact reply parent (``message.reference``) is mandatory when
  it is same-channel and visible: it is reused from history when present,
  otherwise taken from the resolved payload (``reference.resolved``) or
  fetched once via ``channel.fetch_message`` when the payload carries no
  resolved message. A strict 404/10008 parent stays explicitly ``missing``
  (``reply_to`` preserved, parent absent); permission/network/unknown-channel
  parent failures raise :class:`ContextUnavailable` (never pretend deleted).
  A cross-channel reference is never fetched from another channel: ``reply_to``
  is preserved as missing. Same-channel parent IDs must precede the child
  (``parent < child``); self/future links fail instead of being guessed.
- Display names are the actual SDK ``author.display_name`` per sender
  (newest message wins on rename); they never authenticate. The snapshot
  uses the sync cache-only helper (``bot.cached_display_name``: guild
  member cache, then user cache, then the already-available
  ``bot.user.display_name``); no member API fetch is ever called. When
  ``owner_uid``/``bot_uid`` sent no message in the window, the helper
  result is used before falling back to an explicit ``uid-<uid>``
  placeholder that never claims owner auth.
- Timestamps are the native ``created_at.isoformat()`` per message,
  preserved exactly (always a datetime on SDK objects). All chosen bodies
  are complete (never truncated).

Selection policy (explicit, not truncating supplied state):
- Mandatory: the source plus the visible exact same-channel parent (when
  one exists). Optional: the newest history messages before the source.
  Total ``<=max_messages`` (default 20). When the mandatory parent sits
  outside the history window, it displaces the oldest optional history
  message; when ``max_messages`` cannot hold the mandatory pair, the call
  fails :class:`ContextUnavailable`.
- Shared Jev/Thyca fit BEFORE any paid call: the full candidate snapshot
  is checked against the ACTUAL final routed prompts for BOTH
  ``respond`` and ``need_context``
  (:func:`thyca_prompt.build_conversation_prompt` with paired
  ``participation``/``context_sha256``) with a fixed 32-char fake nonce
  (``"f"*32``) and a fixed 64-char fake hash (``"f"*64``) -- neither
  stored. No guessed reserve: both routed prompts must render ``<=4000``
  via the real renderer. When either cannot fit, the collector removes
  the oldest OPTIONAL message only (never the source/visible parent,
  never truncates a body) and rechecks; it fails
  :class:`ContextUnavailable` when the mandatory pair alone cannot fit
  either path (a longer ``need_context`` task may genuinely fail).
  Unknown/binding renderer failures fail immediately (no drop).

Fingerprint: :func:`context_fingerprint` projects any raw/snapshot state
through :func:`conversation.project_conversation_state` and returns
the SHA-256 hex of the canonical ``sort_keys`` JSON. It never mutates its
input and carries no raw bodies in errors.

Importing this module performs no I/O: no env reads, no network, no Discord
calls. It imports ``discord`` and the authoritative source helpers
(``source_messages``/``bot`` are cycle-free here and read
nothing on import). All SDK reads happen inside the returned async
collector, exactly once per dependency (no retries).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import discord

from bot import cached_display_name, get_scoped_guild
from conversation import ConversationError, project_conversation_state
from source_messages import SourceDeleted, _resolve_text_channel, is_unknown_message
from thyca import ThycaPromptError, build_conversation_prompt
from validate import is_strict_int as _is_strict_int
from validate import is_strict_positive_int as _is_strict_positive_int

MAX_MESSAGES_LIMIT = 20
FIT_NONCE = "f" * 32
FIT_HASH = "f" * 64

__all__ = [
    "FIT_HASH",
    "FIT_NONCE",
    "MAX_MESSAGES_LIMIT",
    "ContextUnavailable",
    "SourceDeleted",
    "build_context_collector",
    "context_fingerprint",
]


class ContextUnavailable(ValueError):
    """Bounded context failure (never carries raw bodies or provider text)."""


# ``SourceDeleted`` is intentionally NOT redeclared here: the name above is the
# authoritative ``source_messages.SourceDeleted`` object,
# re-exported so the router and the existing checker catch the same class.


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


def _validate_builder_ids(
    guild_id: Any, channel_id: Any, owner_uid: Any, max_messages: Any
) -> None:
    if not _is_strict_positive_int(guild_id):
        raise ValueError("context collector requires a strict positive guild_id")
    if not _is_strict_positive_int(channel_id):
        raise ValueError("context collector requires a strict positive channel_id")
    if not _is_strict_positive_int(owner_uid):
        raise ValueError("context collector requires a strict positive owner_uid")
    if not _is_strict_int(max_messages) or not 1 <= max_messages <= MAX_MESSAGES_LIMIT:
        raise ValueError(f"context collector requires max_messages 1..{MAX_MESSAGES_LIMIT}")


def _validate_entry(entry: Any, channel_id: int) -> tuple[int, int, str]:
    if not isinstance(entry, Mapping):
        raise ContextUnavailable("context entry must be an object")
    for key in ("channel_id", "message_id", "author_uid"):
        if not _is_strict_positive_int(entry.get(key)):
            raise ContextUnavailable(f"context entry has invalid {key}")
    content = entry.get("content")
    if not isinstance(content, str):
        raise ContextUnavailable("context entry has invalid content")
    if len(content) > 2000:
        raise ContextUnavailable("context entry content exceeds limit")
    if entry["channel_id"] != channel_id:
        raise ContextUnavailable("context entry channel out of scope")
    # Return plain ints/str (copies of immutable scalars; entry never mutated).
    return int(entry["message_id"]), int(entry["author_uid"]), content


def _resolve_bot_uid(bot: discord.Client) -> int:
    user = bot.user
    if user is None:
        raise ContextUnavailable("context bot not ready") from None
    return user.id


async def _resolve_channel(
    bot: discord.Client, guild_id: int, channel_id: int
) -> tuple[discord.Guild, discord.TextChannel]:
    """Resolve the approved guild/channel via the existing scoped helpers.

    Uses ``get_scoped_guild`` + ``source_messages._resolve_text_channel``
    (strict ``isinstance(discord.TextChannel)`` plus guild-scope ID checks),
    so voice/stage channels, threads, and DMs are rejected independent of
    class names. All failures (except cancellation) become
    :class:`ContextUnavailable`.
    """
    try:
        guild = get_scoped_guild(bot, guild_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ContextUnavailable("context guild unavailable") from None
    try:
        channel = await _resolve_text_channel(bot, guild, channel_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ContextUnavailable("context channel unavailable") from None
    return guild, channel


def _message_scope(
    message: discord.Message, guild_id: int, channel_id: int
) -> tuple[int, int, str, str]:
    """Check scope; return (mid, author_uid, content, display_name).

    Out-of-scope (wrong channel, missing/different guild) fails; SDK-typed
    fields are read directly (``Message`` always carries int ids, a str
    body, and a display name).
    """
    if message.channel.id != channel_id or message.guild is None or message.guild.id != guild_id:
        raise ContextUnavailable("context message out of scope") from None
    return message.id, message.author.id, message.content, message.author.display_name


def _message_timestamp(message: discord.Message) -> str:
    # ``created_at`` is a snowflake-derived datetime on every SDK message.
    return message.created_at.isoformat()


def _reference_parent(
    message: discord.Message, channel_id: int, guild_id: int
) -> tuple[int | None, bool]:
    """Return (parent_mid or None, is_cross_channel) for ``message.reference``.

    ``None`` means no reply link (absent reference or null message_id, as in
    system messages). Cross-channel (or cross-guild) links are reported as
    ``(mid, True)`` and must never be fetched from another channel.
    """
    ref = message.reference
    if ref is None:
        return None, False
    parent_mid = ref.message_id
    if parent_mid is None:
        return None, False
    if ref.channel_id != channel_id:
        return parent_mid, True
    if ref.guild_id is not None and ref.guild_id != guild_id:
        return parent_mid, True
    return parent_mid, False


async def _fetch_source(channel: discord.TextChannel, mid: int) -> discord.Message:
    try:
        return await channel.fetch_message(mid)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if is_unknown_message(exc):
            raise SourceDeleted("source message deleted") from None
        raise ContextUnavailable("source message unavailable") from None


async def _fetch_parent(channel: discord.TextChannel, parent_mid: int) -> discord.Message | None:
    """Fetch the same-channel parent; strict 404/10008 means missing (None)."""
    try:
        return await channel.fetch_message(parent_mid)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if is_unknown_message(exc):
            return None
        raise ContextUnavailable("context parent unavailable") from None


async def _collect_history(
    channel: discord.TextChannel, source_message: discord.Message, limit: int
) -> list[discord.Message]:
    if limit <= 0:
        return []
    try:
        iterator = channel.history(limit=limit, before=source_message, oldest_first=False)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ContextUnavailable("context history unavailable") from None
    collected: list[discord.Message] = []
    try:
        async for message in iterator:
            collected.append(message)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ContextUnavailable("context history unavailable") from None
    return collected


def build_context_collector(
    bot: discord.Client,
    guild_id: int,
    channel_id: int,
    owner_uid: int,
    *,
    max_messages: int = 20,
) -> Callable[[Mapping[str, Any]], Awaitable[dict[str, Any]]]:
    """Build the scoped collector for one approved guild/channel.

    The constructor validates its scope IDs (strict positive ints) and
    ``max_messages`` (``1..20``) and stores them; it performs no I/O, no
    SDK reads, and no env access. The returned async callable takes the
    trusted captured ``entry`` (``channel_id``/``message_id``/``author_uid``/
    ``content``) and returns the detached canonical snapshot
    (``assistant_uid``/``owner_uid``/``participants``/``messages``/
    ``new_message_id``) selected per the module policy. It raises the
    authoritative ``source_messages.SourceDeleted`` (same
    object, re-exported) only for a strictly deleted source and
    :class:`ContextUnavailable` for every other bounded failure; both
    carry fixed strings (never raw bodies). Cancellation propagates
    untouched. Exactly one history read plus at most two exact
    ``fetch_message`` reads (source + same-channel parent when outside the
    window and absent from the resolved payload); no retries, no member
    fetches, no paid calls.
    """
    _validate_builder_ids(guild_id, channel_id, owner_uid, max_messages)

    async def collect(entry: Mapping[str, Any]) -> dict[str, Any]:
        mid, author_uid, content = _validate_entry(entry, channel_id)
        bot_uid = _resolve_bot_uid(bot)
        if owner_uid == bot_uid:
            raise ContextUnavailable("context scope contradicts owner/bot") from None
        if author_uid == bot_uid:
            raise ContextUnavailable("context source sender is not human") from None
        guild, channel = await _resolve_channel(bot, guild_id, channel_id)

        source_message = await _fetch_source(channel, mid)
        got_mid, got_uid, got_content, got_name = _message_scope(
            source_message, guild_id, channel_id
        )
        if got_mid != mid or got_uid != author_uid or got_content != content:
            # Edited (or redelivered-contradicting) source: unavailable,
            # never reinterpret the trusted entry from fetched text.
            raise ContextUnavailable("context source edited") from None
        source_ts = _message_timestamp(source_message)
        source_parent, source_cross = _reference_parent(source_message, channel_id, guild_id)
        if source_parent is not None and not source_cross:
            if source_parent >= mid:
                raise ContextUnavailable("context parent order invalid") from None

        history_messages = await _collect_history(channel, source_message, max_messages - 1)

        # Verify every history message; reject future/dup/scope.
        seen: dict[int, dict[str, Any]] = {}
        for raw in history_messages:
            hid, huid, htext, hname = _message_scope(raw, guild_id, channel_id)
            if hid >= mid:
                raise ContextUnavailable("context history holds future message") from None
            if hid in seen:
                raise ContextUnavailable("context history holds duplicate") from None
            hts = _message_timestamp(raw)
            hparent, hcross = _reference_parent(raw, channel_id, guild_id)
            if hparent is not None and not hcross and hparent >= hid:
                raise ContextUnavailable("context history parent order invalid") from None
            seen[hid] = {
                "id": hid,
                "sender_uid": huid,
                "text": htext,
                "name": hname,
                "timestamp": hts,
                "reply_to": hparent,
                "cross": hcross,
            }

        parent_extra: dict[str, Any] | None = None
        parent_visible = False
        if source_parent is not None and not source_cross:
            if source_parent in seen:
                parent_visible = True
            else:
                resolved = (
                    source_message.reference.resolved
                    if source_message.reference is not None
                    else None
                )
                if isinstance(resolved, discord.Message):
                    pid, puid, ptext, pname = _message_scope(resolved, guild_id, channel_id)
                    if pid != source_parent:
                        raise ContextUnavailable("context parent id mismatch") from None
                    if pid >= mid:
                        raise ContextUnavailable("context parent order invalid") from None
                    pts = _message_timestamp(resolved)
                    pparent, pcross = _reference_parent(resolved, channel_id, guild_id)
                    if pparent is not None and not pcross and pparent >= pid:
                        raise ContextUnavailable("context parent order invalid") from None
                    parent_extra = {
                        "id": pid,
                        "sender_uid": puid,
                        "text": ptext,
                        "name": pname,
                        "timestamp": pts,
                        "reply_to": pparent,
                        "cross": pcross,
                    }
                    parent_visible = True
                elif resolved is None:
                    fetched = await _fetch_parent(channel, source_parent)
                    if fetched is not None:
                        pid, puid, ptext, pname = _message_scope(fetched, guild_id, channel_id)
                        if pid != source_parent:
                            raise ContextUnavailable("context parent id mismatch") from None
                        if pid >= mid:
                            raise ContextUnavailable("context parent order invalid") from None
                        pts = _message_timestamp(fetched)
                        pparent, pcross = _reference_parent(fetched, channel_id, guild_id)
                        if pparent is not None and not pcross and pparent >= pid:
                            raise ContextUnavailable("context parent order invalid") from None
                        parent_extra = {
                            "id": pid,
                            "sender_uid": puid,
                            "text": ptext,
                            "name": pname,
                            "timestamp": pts,
                            "reply_to": pparent,
                            "cross": pcross,
                        }
                        parent_visible = True
                # else: DeletedReferencedMessage stays missing (reply_to kept).

        # Optional pool: history (ascending) plus the outside-window parent.
        # Mandatory parent displaces the oldest optional when over the cap.
        optional: list[dict[str, Any]] = [seen[k] for k in sorted(seen)]
        if parent_extra is not None:
            optional.append(parent_extra)
            optional.sort(key=lambda item: item["id"])
        # Deduplicate defensively (history already unique; parent was absent).
        deduped: list[dict[str, Any]] = []
        used: set[int] = set()
        for item in optional:
            if item["id"] not in used:
                used.add(item["id"])
                deduped.append(item)
        optional = deduped
        mandatory_ids = {source_parent} if parent_visible and source_parent is not None else set()
        while len(optional) + 1 > max_messages:
            drop_at: int | None = None
            for index, item in enumerate(optional):
                if item["id"] not in mandatory_ids:
                    drop_at = index
                    break
            if drop_at is None:
                raise ContextUnavailable("context mandatory pair exceeds cap") from None
            del optional[drop_at]

        # Newest display name wins per UID (rename-safe, still exact SDK text).
        def _participants_for(pool: list[dict[str, Any]]) -> list[dict[str, str]]:
            newest_name: dict[int, str] = {}
            # Pool is ascending; later (newer) overwrites older.
            for item in pool:
                newest_name[item["sender_uid"]] = item["name"]
            # Source is newest overall.
            newest_name[author_uid] = got_name
            uids: set[int] = {item["sender_uid"] for item in pool}
            uids.add(author_uid)
            uids.add(owner_uid)
            uids.add(bot_uid)
            parts: list[dict[str, str]] = []
            for uid in sorted(uids):
                if uid in newest_name:
                    parts.append({"uid": str(uid), "display_name": newest_name[uid]})
                    continue
                cached = cached_display_name(bot, guild, uid)
                parts.append({"uid": str(uid), "display_name": cached or _placeholder_name(uid)})
            return parts

        def _messages_for(pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
                "id": str(mid),
                "sender_uid": str(author_uid),
                "text": content,
            }
            if source_parent is not None:
                current["reply_to"] = str(source_parent)
            current["timestamp"] = source_ts
            out.append(current)
            return out

        renderer_entry = {
            "channel_id": channel_id,
            "message_id": mid,
            "author_uid": author_uid,
            "content": content,
        }
        # Fit loop: full newest-first candidate, then drop the oldest
        # OPTIONAL only until BOTH actual routed prompts (respond and
        # need_context) render via the real renderer. Bodies are never
        # truncated; mandatory pair never drops. No guessed reserve.
        pool = list(optional)
        while True:
            candidate = {
                "assistant_uid": str(bot_uid),
                "owner_uid": str(owner_uid),
                "participants": _participants_for(pool),
                "messages": _messages_for(pool),
                "new_message_id": str(mid),
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

    return collect
