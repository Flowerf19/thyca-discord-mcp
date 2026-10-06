"""Discord bot construction and guild-scoped helpers."""

from __future__ import annotations

import sys

import discord
from discord.ext import commands

from config import Settings


def build_intents(settings: Settings) -> discord.Intents:
    intents = discord.Intents.default()
    intents.message_content = settings.intent_message_content
    intents.members = settings.intent_members
    return intents


def build_bot(settings: Settings) -> commands.Bot:
    bot = commands.Bot(
        command_prefix=settings.command_prefix,
        intents=build_intents(settings),
        chunk_guilds_at_startup=settings.chunk_guilds_at_startup,
    )

    @bot.event
    async def on_ready() -> None:
        assert bot.user is not None
        # stderr: stdout belongs to the MCP stdio protocol.
        print(f"thyca-discord-mcp ready as {bot.user} ({bot.user.id})", file=sys.stderr)

    return bot


def get_scoped_guild(bot: commands.Bot, guild_id: int) -> discord.Guild:
    """Return the configured guild from cache; fail loudly if absent."""
    guild = bot.get_guild(guild_id)
    if guild is None:
        raise LookupError(f"guild {guild_id} not in cache (bot not in guild, or not ready)")
    return guild


async def resolve_display_name(bot: commands.Bot, guild: discord.Guild, uid: int) -> str:
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
