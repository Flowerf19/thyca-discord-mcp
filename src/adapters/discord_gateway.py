"""discord.py adapter for the neutral MCP transport surface.

``DiscordGateway`` is the single discord.py touch point for the new
``tools/`` + ``services/`` path: it resolves scoped channels, reads history and
single messages, and sends under a pinned ``allowed_mentions`` policy. It
returns :class:`GatewayMessage` value objects — never raw SDK types — so
services and tests can run against a fake gateway with no discord import.

Error contract: ``GatewayError.code`` is one of the stable tool error codes
(``scope_denied`` / ``not_found`` / ``discord_api``); ``validation`` belongs to
the service layer. Messages are bounded (ids and fixed text only — never
message bodies or secrets). Cancellation propagates untouched.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import discord
from discord.ext import commands

from config import Settings
from policy.scope_guard import (
    ScopeDenied,
    ScopedNotFound,
    scoped_guild,
    scoped_text_channel,
)

# Pinned mention policy for every outbound send through this gateway: explicit
# <@uid> mentions still resolve (users=True); @everyone/@here, role pings, and
# reply-pings are all off. Never relaxed per call.
GATEWAY_ALLOWED_MENTIONS = discord.AllowedMentions(
    everyone=False, users=True, roles=False, replied_user=False
)


class GatewayError(Exception):
    """Bounded Discord-side failure with a stable error ``code``."""

    def __init__(self, code: str, message: str) -> None:
        if code not in ("scope_denied", "not_found", "discord_api"):
            raise ValueError(f"bad gateway error code: {code!r}")
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class GatewayMessage:
    """Normalized Discord message handed to services (no SDK type leaks)."""

    id: int
    channel_id: int
    guild_id: int
    author_uid: int
    author_name: str
    author_is_bot: bool | None
    content: str
    reply_to_message_id: int | None
    timestamp: str
    jump_url: str | None

    @classmethod
    def from_sdk(cls, message: Any) -> GatewayMessage:
        """Normalize one SDK/fake message; malformed fields are discord_api."""
        try:
            mid = message.id
            channel_id = message.channel.id
            guild_id = message.guild.id
            author_uid = message.author.id
            author_name = message.author.display_name
            author_is_bot = message.author.bot
            content = message.content
            reference = message.reference
            jump_url = message.jump_url
            created = message.created_at
        except Exception:
            raise GatewayError("discord_api", "malformed message from gateway") from None
        reply_to = getattr(reference, "message_id", None) if reference is not None else None
        if not _strict_pos_int(mid) or not _strict_pos_int(channel_id):
            raise GatewayError("discord_api", "malformed message ids from gateway")
        if not _strict_pos_int(guild_id) or not _strict_pos_int(author_uid):
            raise GatewayError("discord_api", "malformed message scope from gateway")
        if not isinstance(author_name, str) or not isinstance(content, str):
            raise GatewayError("discord_api", "malformed message fields from gateway")
        if reply_to is not None and not _strict_pos_int(reply_to):
            raise GatewayError("discord_api", "malformed reply reference from gateway")
        if author_is_bot is not None and not isinstance(author_is_bot, bool):
            raise GatewayError("discord_api", "malformed author flag from gateway")
        if hasattr(created, "isoformat"):
            timestamp = created.isoformat()
        elif isinstance(created, str) and created:
            timestamp = created
        else:
            raise GatewayError("discord_api", "malformed message timestamp from gateway")
        if not isinstance(jump_url, (str, type(None))):
            jump_url = None
        return cls(
            id=mid,
            channel_id=channel_id,
            guild_id=guild_id,
            author_uid=author_uid,
            author_name=author_name,
            author_is_bot=author_is_bot,
            content=content,
            reply_to_message_id=reply_to,
            timestamp=timestamp,
            jump_url=jump_url,
        )

    def to_row(self) -> dict[str, Any]:
        """Structured row returned inside tool payloads (ids + whole body)."""
        return {
            "message_id": self.id,
            "channel_id": self.channel_id,
            "author_uid": self.author_uid,
            "author_name": self.author_name,
            "author_is_bot": self.author_is_bot,
            "content": self.content,
            "reply_to_message_id": self.reply_to_message_id,
            "timestamp": self.timestamp,
            "jump_url": self.jump_url,
        }


def _strict_pos_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


class DiscordGateway:
    """Scoped async facade over the discord.py bot (new tool path only)."""

    def __init__(self, bot: commands.Bot, settings: Settings) -> None:
        self._bot = bot
        self._settings = settings

    def is_ready(self) -> bool:
        return self._bot.is_ready()

    def guild(self) -> discord.Guild:
        """The configured guild; ``not_found`` when the bot cannot see it."""
        try:
            return scoped_guild(self._bot, self._settings.guild_id)
        except ScopedNotFound:
            raise
        except Exception:
            raise GatewayError("discord_api", "scoped guild unavailable") from None

    async def text_channel(self, channel_id: int) -> discord.TextChannel:
        """Scope-checked text channel; map guard errors, pass the rest through."""
        try:
            return await scoped_text_channel(self._bot, self.guild(), channel_id)
        except (ScopeDenied, ScopedNotFound):
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            raise GatewayError("discord_api", "channel resolution failed") from None

    async def channel_name(self, channel_id: int) -> str | None:
        """Scoped channel's display name (None when unnamed or not a nameable channel)."""
        channel = await self.text_channel(channel_id)
        name = getattr(channel, "name", None)
        return name if isinstance(name, str) and name else None

    async def history(
        self,
        channel_id: int,
        *,
        before_message_id: int | None = None,
        limit: int = 50,
    ) -> list[GatewayMessage]:
        """Up to ``limit`` messages before ``before_message_id``, oldest→newest."""
        channel = await self.text_channel(channel_id)
        bound = discord.Object(before_message_id) if before_message_id is not None else None
        try:
            collected = [
                m
                async for m in channel.history(
                    limit=limit, before=bound, oldest_first=False
                )
            ]
        except asyncio.CancelledError:
            raise
        except discord.NotFound:
            raise ScopedNotFound(f"channel not found: {channel_id}") from None
        except discord.Forbidden:
            raise ScopeDenied(f"forbidden channel: {channel_id}") from None
        except Exception:
            raise GatewayError("discord_api", "history read failed") from None
        collected.reverse()  # SDK returns newest-first; service contract is chronological
        return [GatewayMessage.from_sdk(m) for m in collected]

    async def fetch_message(self, channel_id: int, message_id: int) -> GatewayMessage:
        """One exact message in a scoped channel."""
        channel = await self.text_channel(channel_id)
        try:
            message = await channel.fetch_message(message_id)
        except asyncio.CancelledError:
            raise
        except discord.NotFound:
            raise ScopedNotFound(f"message not found: {message_id}") from None
        except discord.Forbidden:
            raise ScopeDenied(f"forbidden channel: {channel_id}") from None
        except Exception:
            raise GatewayError("discord_api", "message fetch failed") from None
        return GatewayMessage.from_sdk(message)

    async def send(
        self,
        channel_id: int,
        content: str,
        *,
        reply_to_message_id: int | None = None,
    ) -> GatewayMessage:
        """Send text (optionally as a Discord reply reference) in scope.

        Replies use ``MessageReference(fail_if_not_exists=True)``: a deleted
        target is an API error, never a silent unthreaded fallback. The pinned
        mention policy applies to every send.
        """
        channel = await self.text_channel(channel_id)
        reference = None
        if reply_to_message_id is not None:
            reference = discord.MessageReference(
                message_id=reply_to_message_id,
                channel_id=channel_id,
                guild_id=self._settings.guild_id,
                fail_if_not_exists=True,
            )
        try:
            message = await channel.send(
                content,
                reference=reference,
                allowed_mentions=GATEWAY_ALLOWED_MENTIONS,
            )
        except asyncio.CancelledError:
            raise
        except discord.NotFound:
            raise ScopedNotFound(f"reply target not found: {reply_to_message_id}") from None
        except discord.Forbidden:
            raise ScopeDenied(f"forbidden channel: {channel_id}") from None
        except Exception:
            raise GatewayError("discord_api", "message send failed") from None
        return GatewayMessage.from_sdk(message)


__all__ = ["DiscordGateway", "GatewayError", "GatewayMessage", "GATEWAY_ALLOWED_MENTIONS"]
