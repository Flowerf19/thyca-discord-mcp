"""Discord bot construction and guild-scoped helpers."""

from __future__ import annotations

import sys

import discord

from config import Settings


def build_intents(settings: Settings) -> discord.Intents:
    intents = discord.Intents.default()
    intents.message_content = settings.intent_message_content
    intents.members = settings.intent_members
    return intents


def build_bot(settings: Settings) -> discord.Client:
    # Chunking defaults on: the gateway fills the member cache before ready,
    # so guild.members reads are complete. NOTE: intent_members=False with
    # chunking raises ValueError at construction (enforced by discord.py);
    # such a config fails loudly at startup instead of running on an
    # empty member cache.
    bot = discord.Client(
        intents=build_intents(settings),
        chunk_guilds_at_startup=settings.chunk_guilds_at_startup,
    )

    @bot.event
    async def on_ready() -> None:
        assert bot.user is not None
        # stderr: stdout belongs to the MCP stdio protocol.
        print(f"thyca-discord-mcp ready as {bot.user} ({bot.user.id})", file=sys.stderr)

    return bot


def get_scoped_guild(bot: discord.Client, guild_id: int) -> discord.Guild:
    """Return the configured guild from cache; fail loudly if absent."""
    guild = bot.get_guild(guild_id)
    if guild is None:
        raise LookupError(f"guild {guild_id} not in cache (bot not in guild, or not ready)")
    return guild


def cached_display_name(bot: discord.Client, guild: discord.Guild, uid: int) -> str | None:
    """Sync cache-only display name; None when unknown (caller placeholders).

    No API fetch is ever performed: guild member cache, then the user cache,
    then the already-available ``bot.user`` for the bot's own UID. Context
    snapshots must use this (never the async fetch version) so participant
    names stay stable across snapshots.
    """
    member = guild.get_member(uid)
    if member is not None:
        return member.display_name
    user = bot.get_user(uid)
    if user is not None:
        return user.display_name
    if bot.user is not None and uid == bot.user.id:
        return bot.user.display_name
    return None


async def resolve_display_name(bot: discord.Client, guild: discord.Guild, uid: int) -> str:
    """Best-effort display name: guild member first, global user fallback."""
    member = guild.get_member(uid)
    if member is not None:
        return member.display_name
    try:
        return (await guild.fetch_member(uid)).display_name
    except (discord.NotFound, discord.Forbidden):
        pass
    try:
        return (await bot.fetch_user(uid)).display_name
    except (discord.NotFound, discord.Forbidden):
        return "unknown"
