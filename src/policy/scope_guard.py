"""Scope guard for the neutral transport surface.

Every Discord read/send on the new tool path resolves through the configured
guild's text channels only: other guilds, DMs, threads, and non-text channels
are refused before any API call. Error types carry the stable ``code`` of the
tool error contract (``scope_denied`` / ``not_found``); unexpected SDK failures
stay the caller's ``discord_api`` mapping responsibility.
"""

from __future__ import annotations

import asyncio

import discord
from discord.ext import commands

from bot import get_scoped_guild
from source_messages import _resolve_text_channel


class ScopeDenied(PermissionError):
    """Target outside the gateway scope (other guild, DM, thread, non-text)."""

    code = "scope_denied"


class ScopedNotFound(LookupError):
    """A named channel/message does not exist inside the scope."""

    code = "not_found"


def scoped_guild(bot: commands.Bot, guild_id: int) -> discord.Guild:
    """Return the configured guild from cache, or ScopedNotFound."""
    try:
        return get_scoped_guild(bot, guild_id)
    except LookupError:
        raise ScopedNotFound(f"scoped guild {guild_id} unavailable") from None


async def scoped_text_channel(
    bot: commands.Bot, guild: discord.Guild, channel_id: int
) -> discord.TextChannel:
    """Resolve a text channel inside the scoped guild.

    Cache-first lookup with a ``fetch_channel`` fallback, same as the shared
    resolver: PermissionError maps to ``scope_denied``, a missing channel to
    ``not_found``, and Discord's ``Forbidden`` (no access) to ``scope_denied``.
    Cancellation propagates untouched.
    """
    try:
        return await _resolve_text_channel(bot, guild, channel_id)
    except asyncio.CancelledError:
        raise
    except PermissionError as exc:
        raise ScopeDenied(str(exc)) from None
    except discord.Forbidden:
        raise ScopeDenied(f"forbidden channel: {channel_id}") from None
    except discord.NotFound:
        raise ScopedNotFound(f"channel not found: {channel_id}") from None


__all__ = ["ScopeDenied", "ScopedNotFound", "scoped_guild", "scoped_text_channel"]
