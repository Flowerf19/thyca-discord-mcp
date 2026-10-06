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
cursor for retry; auto-reply errors audit coarse kinds only and stop their
own loop unconditionally (an audit-sink failure emits a fixed sanitized
stderr signal and still stops); CancelledError always propagates untouched.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from contextlib import suppress
from typing import Any, Protocol

import discord

from bot import get_scoped_guild
from config import Settings
from identity import classify_actor
from outbox import append_entry, get_poll_cursor, set_poll_cursor
from server_helpers import _check_outbox_consistent, _coarse_error_kind, _resolve_text_channel
from tasks import TickFn

__all__ = [
    "AppendAuditFn",
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
                session_id=settings.session_id,
                actor="system",
                tool="heartbeat",
                args={"members": guild.member_count},
                ok=True,
            )
        except Exception as exc:
            # Heartbeat failure is recorded, not hidden; the loop itself survives.
            append_audit_fn(
                settings.audit_path,
                session_id=settings.session_id,
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
            _check_outbox_consistent(settings)  # before cursor use: no history on state loss
            cursor = get_poll_cursor(settings.outbox_state_path, channel_id)
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
            for m in collected:
                actor = classify_actor(
                    m.author.id,
                    m.author.display_name,
                    bot_uid=bot.user.id,
                    owner_uid=settings.owner_uid,
                )
                append_entry(
                    settings.outbox_path,
                    settings.outbox_state_path,
                    type="poll",
                    channel_id=channel_id,
                    message_id=m.id,
                    author_uid=m.author.id,
                    author_name=m.author.display_name,
                    actor_kind=actor.kind.value,
                    content=m.content if m.content else "",
                    author_is_bot=m.author.bot,
                    reply_to_message_id=m.reference.message_id if m.reference is not None else None,
                )
                max_id = max(max_id, m.id)
            if collected:
                set_poll_cursor(settings.outbox_state_path, channel_id, max_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            append_audit_fn(
                settings.audit_path,
                session_id=settings.session_id,
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
) -> tuple[TickFn, dict[str, Any]]:
    """Drive one FIFO worker step; fail the loop closed on any error.

    Returns the tick coroutine plus a holder the lifespan fills with the
    registered native Loop, so a failing tick can stop its own loop (no
    state reset, no blind repeat POST). Failures are audited with coarse
    kinds only; stopping is UNCONDITIONAL (an audit-sink failure emits a
    fixed sanitized stderr signal and still stops -- a raise here would
    let the native Loop backoff-retry instead). Cancellation propagates
    untouched.
    """
    holder: dict[str, Any] = {}

    async def _auto_reply_tick() -> None:
        try:
            await responder.tick()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            kind = _coarse_error_kind(exc)
            try:
                append_audit_fn(
                    settings.audit_path,
                    session_id=settings.session_id,
                    actor="system",
                    tool="auto_reply_tick",
                    args={"error_kind": kind},
                    ok=False,
                    error=f"auto_reply_tick failed kind={kind}",
                )
            except Exception:
                # Audit sink failed: fixed sanitized signal only (never the
                # original exception chain), then still fail closed below.
                with suppress(Exception):
                    print(
                        "thyca-discord-mcp: auto_reply_tick audit failed",
                        file=sys.stderr,
                    )
            finally:
                loop = holder.get("loop")
                if loop is not None:
                    loop.stop()  # fail close: unconditional, never swallowed
            return

    return _auto_reply_tick, holder
