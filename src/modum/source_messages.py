"""Scoped Discord source-message existence proof (read-only, never a send).

Proves whether a watched source message still exists via the native SDK
``fetch_message`` read for the EXACT message ID: no history scan, no body
retention, no reply/text processing. Only a strict Discord ``NotFound``
with HTTP status 404 and code 10008 (official Unknown Message, verified in
installed discord.py 2.7.1) proves deletion. Every other outcome --
Forbidden, missing permissions, 5xx, timeouts/network resets, unknown
channel codes (e.g. 10003), scope mismatch, malformed reads -- is
``SourceUnavailable`` and must NEVER be treated as deletion.

Callers log metadata only; these errors carry fixed strings (never the
native exception text, raw errors, or message bodies). Cancellation
propagates untouched.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import discord
from discord.ext import commands

from .bot import get_scoped_guild

# Official Discord "Unknown Message" code: the ONLY code that proves the
# exact fetched message is gone (verified discord.py 2.7.1 + official docs).
_UNKNOWN_MESSAGE_CODE = 10008
_NOT_FOUND_STATUS = 404


class SourceDeleted(ValueError):
    """The exact source message is confirmed deleted (strict 404/10008)."""


class SourceUnavailable(ValueError):
    """Existence could not be proven (no send may be inferred from this)."""


async def _resolve_text_channel(
    bot: commands.Bot, guild: discord.Guild, cid: int
) -> discord.TextChannel:
    """Cache-first channel lookup guarded to text channels of the scoped guild.

    Falls back to ``fetch_channel`` on a cache miss. DMs, voice channels,
    threads and other guilds' channels are refused (no widened target support).
    """
    channel = guild.get_channel(cid)
    if channel is None:
        channel = await bot.fetch_channel(cid)
    if not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id:
        raise PermissionError(f"refusing to send outside scoped guild: {cid}")
    return channel


async def check_source_message(channel: Any, source_message_id: int) -> None:
    """Prove the exact source message exists in the resolved channel scope.

    Returns None when the fetched message ID and its channel/guild scope
    match the resolved channel exactly. Raises :class:`SourceDeleted` ONLY
    on a strict ``discord.NotFound`` with int status 404 and code 10008
    from that exact fetch; raises :class:`SourceUnavailable` for every
    other failure or mismatch. Never stores, prints, or returns content.
    """
    if (
        not isinstance(source_message_id, int)
        or isinstance(source_message_id, bool)
        or source_message_id <= 0
    ):
        raise SourceUnavailable("source message unavailable")
    try:
        message = await channel.fetch_message(source_message_id)
    except asyncio.CancelledError:
        raise
    except discord.NotFound as exc:
        if (
            type(exc.status) is int
            and exc.status == _NOT_FOUND_STATUS
            and type(exc.code) is int
            and exc.code == _UNKNOWN_MESSAGE_CODE
        ):
            raise SourceDeleted("source message deleted") from None
        raise SourceUnavailable("source message unavailable") from None
    except Exception:
        # Forbidden, 5xx, timeouts/network resets, permission errors, and
        # any unexpected read failure: existence unproven, never deletion.
        # (CancelledError is BaseException and propagates above.)
        raise SourceUnavailable("source message unavailable") from None
    try:
        got_mid = message.id
        got_cid = message.channel.id
        got_gid = message.guild.id
        want_cid = channel.id
        want_gid = channel.guild.id
    except Exception:
        raise SourceUnavailable("source message unavailable") from None
    if got_mid != source_message_id or got_cid != want_cid or got_gid != want_gid:
        raise SourceUnavailable("source message unavailable") from None


def build_source_checker(bot: Any, guild_id: int) -> Callable[[int, int], Awaitable[None]]:
    """Build the production bot-backed existence checker (live SDK reads).

    Resolves the exact approved guild/channel per call (retaining the scope
    guards of ``get_scoped_guild``/``_resolve_text_channel``) then proves
    the exact source message. Signature
    ``async (channel_id, message_id) -> None`` raising the typed errors
    above. No responder/server/delivery imports (cycle-free by design).
    """
    if not isinstance(guild_id, int) or isinstance(guild_id, bool) or guild_id <= 0:
        raise ValueError("build_source_checker requires a positive guild_id")

    async def check(channel_id: int, message_id: int) -> None:
        guild = get_scoped_guild(bot, guild_id)
        channel = await _resolve_text_channel(bot, guild, channel_id)
        await check_source_message(channel, message_id)

    return check
