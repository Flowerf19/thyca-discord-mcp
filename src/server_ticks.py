"""Background tick factories for the MCP server composition root.

Authoritative owner of the three background ticks registered by
:func:`server.create_server`: heartbeat liveness,
capture-only poll, and the v5 auto-reply worker step. Moved verbatim from
server.py (T4 responsibility cut): the composition root keeps the
TaskRegistry, state dict, bot getter, tools, lifespan, SDK send, proof, and
latch logic, and constructs these factories with explicit settings, bot
getters, and a fresh-resolving audit callback so the root
``server.append_audit`` patch target keeps working.

Dependency policy: settings, bot getters, and the audit sink are explicit
constructor arguments (no server imports, no build_bot/Responder capture, no
module/global namespace passing). Leaf helpers (guild scope, channel
resolver, outbox, identity, consistency/coarse-kind) are imported from their
single canonical owners. No IO runs at factory construction: every factory
returns an async TickFn that only works when the native loop calls it.

Failure policy (preserved exactly): heartbeat failures are recorded, not
hidden, and the loop survives; poll errors audit ok:false and leave the
cursor for retry; auto-reply errors propagate to fail the native Loop
(its error handler audits coarse kinds only) and an audit-sink failure
emits a fixed sanitized stderr signal; CancelledError always propagates
untouched.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Coroutine
from contextlib import suppress
from typing import Any, Protocol

import discord

from bot import get_scoped_guild
from config import Settings
from identity import classify_actor
from server_helpers import _coarse_error_kind, _resolve_text_channel
from store import Store
from tasks import TickFn

#: Native Loop error-handler: called once with the failure, must not raise.
ErrorFn = Callable[[BaseException], Coroutine[None, None, None]]

__all__ = [
    "AppendAuditFn",
    "ErrorFn",
    "GetBotFn",
    "GetBotOrNoneFn",
    "TickableResponder",
    "make_heartbeat_tick",
    "make_poll_tick",
    "make_auto_reply_tick",
]

# Signature mirrors audit.append_audit; the root passes a fresh-resolving
# callable so late patches of server.append_audit apply.
AppendAuditFn = Callable[..., dict[str, Any]]
# Poll getter: raises when the bot is not ready (mirrors server._bot).
GetBotFn = Callable[[], discord.Client]
# Heartbeat getter: returns None when no bot yet, never raises.
GetBotOrNoneFn = Callable[[], discord.Client | None]


class TickableResponder(Protocol):
    """Minimal worker surface the auto-reply tick drives (no Responder import)."""

    async def tick(self) -> dict[str, Any]: ...


def make_heartbeat_tick(
    settings: Settings,
    get_bot_or_none: GetBotOrNoneFn,
    append_audit_fn: AppendAuditFn,
) -> TickFn:
    """Build the liveness tick (no IO until the returned TickFn runs).

    Snapshot member count to the audit log every 5 minutes. A missing bot
    returns quietly (no raise, no audit); guild lookup failures are recorded
    with the same error-repr policy as before, and the loop itself survives.
    """

    async def heartbeat() -> None:
        """Liveness signal: snapshot member count to the audit log every 5 minutes."""
        bot = get_bot_or_none()
        if bot is None:
            return
        try:
            guild = get_scoped_guild(bot, settings.guild_id)
            append_audit_fn(
                settings.audit_path,
                session_id=settings.target_bot_id,
                actor="system",
                tool="heartbeat",
                args={"members": guild.member_count},
                ok=True,
            )
        except Exception as exc:
            # Heartbeat failure is recorded, not hidden; the loop itself survives.
            append_audit_fn(
                settings.audit_path,
                session_id=settings.target_bot_id,
                actor="system",
                tool="heartbeat",
                args={},
                ok=False,
                error=repr(exc),
            )

    return heartbeat


def make_poll_tick(
    settings: Settings,
    channel_id: int,
    get_bot: GetBotFn,
    append_audit_fn: AppendAuditFn,
) -> TickFn:
    """Build a capture-only poll tick for one channel (no commands executed).

    First tick (no cursor): fetch the 20 newest messages, reverse, append
    old->new. Later ticks: fetch up to 100 messages after the cursor via
    native paging. The cursor advances to the max seen id only after all
    appends succeed; on error the tick audits ok:false and returns so the
    native loop survives, leaving the cursor for the next tick to retry.
    Unlike realtime capture, poll keeps the bot's own messages (self).
    """

    async def _poll_tick() -> None:
        try:
            bot = get_bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, channel_id)
            assert bot.user is not None
            # History fetch happens BEFORE any store transaction (never hold a
            # txn across an await). The batch append + cursor advance below is
            # one BEGIN IMMEDIATE transaction; Store.open fail-louds on a
            # corrupt DB, so no separate consistency check is needed here.
            with Store.open(settings.db_path) as _store:
                cursor = _store.get_poll_cursor(channel_id)
            if cursor is None:
                # Newest-first page of the 20 newest; reverse to append old->new.
                # Never oldest_first=True here: that would page from channel creation.
                collected = [m async for m in channel.history(limit=20, oldest_first=False)]
                collected.reverse()
            else:
                collected = [
                    m
                    async for m in channel.history(
                        limit=100, after=discord.Object(cursor), oldest_first=True
                    )
                ]
            max_id = cursor or 0
            items: list[dict[str, Any]] = []
            for m in collected:
                actor = classify_actor(
                    m.author.id,
                    m.author.display_name,
                    bot_uid=bot.user.id,
                    owner_uid=settings.owner_uid,
                )
                items.append(
                    {
                        "type": "poll",
                        "channel_id": channel_id,
                        "message_id": m.id,
                        "author_uid": m.author.id,
                        "author_name": m.author.display_name,
                        "actor_kind": actor.kind.value,
                        "content": m.content if m.content else "",
                        "author_is_bot": m.author.bot,
                        "reply_to_message_id": m.reference.message_id
                        if m.reference is not None
                        else None,
                    }
                )
                max_id = max(max_id, m.id)
            if collected:
                # Poll batch + cursor in ONE transaction: either every new
                # message and the cursor land together, or nothing does.
                with Store.open(settings.db_path) as _store:
                    _store.append_poll_batch(channel_id, items, max_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            append_audit_fn(
                settings.audit_path,
                session_id=settings.target_bot_id,
                actor="system",
                tool="poll_tick",
                args={"channel_id": channel_id},
                ok=False,
                error=repr(exc),
            )
            return

    return _poll_tick


def make_auto_reply_tick(
    settings: Settings,
    responder: TickableResponder,
    append_audit_fn: AppendAuditFn,
) -> tuple[TickFn, ErrorFn]:
    """Drive one FIFO worker step; fail the loop closed on any error.

    Returns the tick coroutine plus a native Loop error handler the lifespan
    registers via ``loop.error(...)``. The tick lets exceptions propagate so
    the Loop ends failed (no state reset, no blind repeat POST); the handler
    audits coarse kinds only. ``loop.clear_exception_types()`` in the
    lifespan is REQUIRED: without it the Loop backoff-retries OSError and
    friends instead of ever reaching this handler. The handler must not
    raise (a raising handler replaces the task exception); an audit-sink
    failure emits a fixed sanitized stderr signal instead. Cancellation
    propagates untouched (native Loops skip the handler for CancelledError).
    """

    async def _auto_reply_tick() -> None:
        await responder.tick()

    async def _auto_reply_error(exc: BaseException) -> None:
        kind = _coarse_error_kind(exc)
        try:
            append_audit_fn(
                settings.audit_path,
                session_id=settings.target_bot_id,
                actor="system",
                tool="auto_reply_tick",
                args={"error_kind": kind},
                ok=False,
                error=f"auto_reply_tick failed kind={kind}",
            )
        except Exception:
            # Audit sink failed: fixed sanitized signal only (never the
            # original exception chain). The Loop is already failed here;
            # nothing further is needed to fail closed.
            with suppress(Exception):
                print(
                    "thyca-discord-mcp: auto_reply_tick audit failed",
                    file=sys.stderr,
                )

    return _auto_reply_tick, _auto_reply_error
