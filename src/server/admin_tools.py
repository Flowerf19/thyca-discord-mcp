"""Scoped-guild admin tools for the MCP server (v2/v3 surface).

Registered by :func:`register_admin_tools`, called from
``server.core.create_server`` at the exact point where the admin block used
to live, so MCP tool registration order is unchanged. Pure move from
``server/core.py``: tool names, signatures, descriptions, audit args, and
error paths are identical. This module must never import ``server.core``
(no cycle): it receives ``settings``, the ``_bot``/``_ok``/``_fail``
closures, and the ``identity_policy`` string as arguments.
"""

from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Any, NoReturn

import discord
from mcp.server.fastmcp import FastMCP
from pydantic import StrictInt

from bot import get_scoped_guild
from config import Settings
from identity import classify_actor
from server_helpers import _resolve_text_channel


def register_admin_tools(
    mcp: FastMCP,
    *,
    settings: Settings,
    bot_fn: Callable[[], discord.Client],
    ok_fn: Callable[..., str],
    fail_fn: Callable[..., NoReturn],
    identity_policy: str,
) -> None:
    """Register the 17 admin tools plus their helpers on ``mcp``.

    Helpers (``_resolve_member``, ``_member_payload``,
    ``_resolve_guild_channel``, ``_require_name``, ``_resolve_category``)
    close over ``settings``/``bot_fn``/``ok_fn``/``fail_fn`` exactly as the
    former ``create_server`` closures did.
    """
    _bot = bot_fn
    _ok = ok_fn
    _fail = fail_fn

    # v2/v3 admin surface (full guild management on the scoped guild only).
    # Every tool here carries the identity policy in its description
    # (mutating tools per the plan; read-only admin reads per the
    # whoami/read_context precedent). Audit args are metadata only (ids,
    # counts, name/reason lengths -- never content or raw reason text).
    # discord.Forbidden/NotFound propagate via _fail.

    async def _resolve_member(bot: discord.Client, guild: discord.Guild, uid: Any) -> Any:
        """Cache-first member lookup, REST fetch fallback; unknown members raise."""
        if not isinstance(uid, int) or isinstance(uid, bool) or uid <= 0:
            raise ValueError(f"invalid uid: {uid!r}")
        member = guild.get_member(uid)
        if member is None:
            member = await guild.fetch_member(uid)
        return member

    def _member_payload(bot: discord.Client, member: Any) -> dict[str, Any]:
        """Public member shape for get/set/role tools."""
        assert bot.user is not None
        actor = classify_actor(
            member.id,
            member.display_name,
            bot_uid=bot.user.id,
            owner_uid=settings.owner_uid,
        )
        joined = member.joined_at
        return {
            "uid": actor.uid,
            "display_name": actor.display_name,
            "nick": member.nick,
            "kind": actor.kind.value,
            "roles": [
                {"id": r.id, "name": r.name, "position": r.position} for r in (member.roles or [])
            ],
            "joined_at": joined.isoformat() if joined is not None else None,
            "bot": bool(member.bot),
        }

    async def _resolve_guild_channel(
        bot: discord.Client, guild: discord.Guild, cid: Any
    ) -> discord.abc.GuildChannel:
        """Text, voice, or category channels of the scoped guild only.

        Unlike _resolve_text_channel (text-only), this also accepts voice
        and category channels for guild management. Forum/stage channels,
        threads, DMs, and other guilds' channels are refused. delete_channel
        is the only caller.
        """
        if not isinstance(cid, int) or isinstance(cid, bool) or cid <= 0:
            raise ValueError(f"invalid channel_id: {cid!r}")
        channel = guild.get_channel(cid)
        if channel is None:
            channel = await bot.fetch_channel(cid)
        if (
            not isinstance(
                channel, (discord.TextChannel, discord.VoiceChannel, discord.CategoryChannel)
            )
            or channel.guild.id != guild.id
        ):
            raise PermissionError(f"refusing to manage outside scoped guild: {cid}")
        return channel

    def _require_name(name: Any, what: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{what} name must be a non-empty string")
        return name

    def _resolve_category(guild: discord.Guild, category_id: Any) -> discord.CategoryChannel | None:
        """Resolve an optional category id to a CategoryChannel in the scoped guild."""
        if category_id is None:
            return None
        if not isinstance(category_id, int) or isinstance(category_id, bool) or category_id <= 0:
            raise ValueError(f"invalid category_id: {category_id!r}")
        category = guild.get_channel(category_id)
        if not isinstance(category, discord.CategoryChannel):
            raise ValueError(f"category {category_id} is not a CategoryChannel in the scoped guild")
        return category

    @mcp.tool(
        description=(
            "Read recent messages newest-first from a scoped-guild text channel; "
            "defaults to the configured channel. Returns up to `limit` (1..100) "
            "entries as {message_id, author_uid, author_name, kind, content, "
            "timestamp, reply_to_message_id}. "
            f"{identity_policy}"
        )
    )
    async def read_history(channel_id: StrictInt | None = None, limit: StrictInt = 20) -> str:
        """Read channel history. (Agent-facing text is the decorator description above.)"""
        cid = channel_id if channel_id is not None else settings.default_channel_id
        limit = max(1, min(limit, 100))
        args: dict[str, Any] = {"channel_id": cid, "limit": limit}
        try:
            bot = _bot()
            assert bot.user is not None
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
            entries = []
            async for m in channel.history(limit=limit, oldest_first=False):
                actor = classify_actor(
                    m.author.id,
                    m.author.display_name,
                    bot_uid=bot.user.id,
                    owner_uid=settings.owner_uid,
                )
                ref = m.reference
                entries.append(
                    {
                        "message_id": m.id,
                        "author_uid": actor.uid,
                        "author_name": actor.display_name,
                        "kind": actor.kind.value,
                        "content": m.content,
                        "timestamp": m.created_at.isoformat(),
                        "reply_to_message_id": ref.message_id if ref is not None else None,
                    }
                )
            return _ok("read_history", args, entries)
        except Exception as exc:
            _fail("read_history", args, exc)

    @mcp.tool(
        description=(
            "Edit a message in a scoped-guild text channel (defaults to the "
            "configured channel). Discord only lets the bot edit its own "
            "messages; editing another user's message fails with a bounded "
            "error. "
            f"{identity_policy}"
        )
    )
    async def edit_message(
        message_id: StrictInt, content: str, channel_id: StrictInt | None = None
    ) -> str:
        """Edit a message. (Agent-facing text is the decorator description above.)"""
        cid = channel_id if channel_id is not None else settings.default_channel_id
        args = {"channel_id": cid, "message_id": message_id}
        if isinstance(content, str):
            args["content_len"] = len(content)
        try:
            if not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0:
                raise ValueError(f"invalid message_id: {message_id!r}")
            if not isinstance(content, str) or not content or len(content) > 2000:
                raise ValueError("edit_message: content must be 1..2000 chars")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
            message = await channel.fetch_message(message_id)
            edited = await message.edit(content=content)
            return _ok("edit_message", args, {"message_id": edited.id, "channel_id": cid})
        except Exception as exc:
            _fail("edit_message", args, exc)

    @mcp.tool(
        description=(
            "Delete a single message from a scoped-guild text channel "
            "(defaults to the configured channel). Requires Manage Messages. "
            f"{identity_policy}"
        )
    )
    async def delete_message(message_id: StrictInt, channel_id: StrictInt | None = None) -> str:
        """Delete a message. (Agent-facing text is the decorator description above.)"""
        cid = channel_id if channel_id is not None else settings.default_channel_id
        args: dict[str, Any] = {"channel_id": cid, "message_id": message_id}
        try:
            if not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0:
                raise ValueError(f"invalid message_id: {message_id!r}")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
            message = await channel.fetch_message(message_id)
            await message.delete()
            return _ok("delete_message", args, {"deleted": message_id, "channel_id": cid})
        except Exception as exc:
            _fail("delete_message", args, exc)

    @mcp.tool(
        description=(
            "Bulk-delete recent messages in the scoped-guild text channel "
            "`channel_id` (required), up to `limit` (1..100). "
            "Returns the deleted COUNT only, never message contents. "
            "Requires Manage Messages. "
            f"{identity_policy}"
        )
    )
    async def purge(channel_id: StrictInt, limit: StrictInt = 20) -> str:
        """Bulk delete. (Agent-facing text is the decorator description above.)"""
        cid = channel_id
        limit = max(1, min(limit, 100))
        args = {"channel_id": cid, "limit": limit}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
            deleted = await channel.purge(limit=limit, bulk=True)
            return _ok("purge", args, {"deleted": len(deleted)})
        except Exception as exc:
            _fail("purge", args, exc)

    @mcp.tool(
        description=(
            "Create a text channel in the scoped guild. `category_id`, when "
            "given, must resolve to a CategoryChannel in the guild. "
            "Requires Manage Channels. "
            f"{identity_policy}"
        )
    )
    async def create_text_channel(
        name: str, category_id: StrictInt | None = None, topic: str | None = None
    ) -> str:
        """Create a text channel. (Agent-facing text is the decorator description.)"""
        args: dict[str, Any] = {"name_len": len(name) if isinstance(name, str) else 0}
        if category_id is not None:
            args["category_id"] = category_id
        if isinstance(topic, str):
            args["topic_len"] = len(topic)
        try:
            _require_name(name, "channel")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            category = _resolve_category(guild, category_id)
            channel = await guild.create_text_channel(name, category=category, topic=topic)
            return _ok(
                "create_text_channel",
                args,
                {
                    "id": channel.id,
                    "name": channel.name,
                    "category_id": channel.category_id,
                    "topic": channel.topic,
                    "position": channel.position,
                },
            )
        except Exception as exc:
            _fail("create_text_channel", args, exc)

    @mcp.tool(
        description=(
            "Create a voice channel in the scoped guild. `category_id`, when "
            "given, must resolve to a CategoryChannel in the guild. "
            "Requires Manage Channels. "
            f"{identity_policy}"
        )
    )
    async def create_voice_channel(name: str, category_id: StrictInt | None = None) -> str:
        """Create a voice channel. (Agent-facing text is the decorator description.)"""
        args = {"name_len": len(name) if isinstance(name, str) else 0}
        if category_id is not None:
            args["category_id"] = category_id
        try:
            _require_name(name, "channel")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            category = _resolve_category(guild, category_id)
            channel = await guild.create_voice_channel(name, category=category)
            return _ok(
                "create_voice_channel",
                args,
                {
                    "id": channel.id,
                    "name": channel.name,
                    "category_id": channel.category_id,
                    "position": channel.position,
                },
            )
        except Exception as exc:
            _fail("create_voice_channel", args, exc)

    @mcp.tool(
        description=(
            "Delete the text, voice, or category channel `channel_id` "
            "(required) in the scoped guild. Destructive: no confirm "
            "step, every call is audited. Requires Manage Channels. "
            f"{identity_policy}"
        )
    )
    async def delete_channel(channel_id: StrictInt) -> str:
        """Delete a guild channel. (Agent-facing text is the decorator description.)"""
        cid = channel_id
        args: dict[str, Any] = {"channel_id": cid}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_guild_channel(bot, guild, cid)
            await channel.delete()
            return _ok("delete_channel", args, {"deleted": cid})
        except Exception as exc:
            _fail("delete_channel", args, exc)

    @mcp.tool(
        description=(
            "Describe one guild member as {uid, display_name, nick, kind, roles, "
            "joined_at, bot}: roles are [{id, name, position}], joined_at is "
            "ISO-8601 or null. "
            f"{identity_policy}"
        )
    )
    async def get_member(uid: StrictInt) -> str:
        """Describe a member. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            return _ok("get_member", args, _member_payload(bot, member))
        except Exception as exc:
            _fail("get_member", args, exc)

    @mcp.tool(
        description=(
            "Set a member's nickname in the scoped guild; an empty nick clears "
            "it. Returns the updated member payload. Requires Manage Nicknames. "
            f"{identity_policy}"
        )
    )
    async def set_nick(uid: StrictInt, nick: str = "") -> str:
        """Set a nickname. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid, "nick_len": len(nick) if isinstance(nick, str) else 0}
        try:
            if not isinstance(nick, str):
                raise ValueError("nick must be a string")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            updated = await member.edit(nick=nick or None)
            return _ok("set_nick", args, _member_payload(bot, updated or member))
        except Exception as exc:
            _fail("set_nick", args, exc)

    @mcp.tool(
        description=(
            "Add a guild role to a member in the scoped guild. The role must "
            "exist and the bot's role must outrank it. Returns the updated "
            "member payload. Requires Manage Roles. "
            f"{identity_policy}"
        )
    )
    async def add_role(uid: StrictInt, role_id: StrictInt) -> str:
        """Add a role. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid, "role_id": role_id}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            if not isinstance(role_id, int) or isinstance(role_id, bool) or role_id <= 0:
                raise ValueError(f"invalid role_id: {role_id!r}")
            role = guild.get_role(role_id)
            if role is None:
                raise LookupError(f"role {role_id} not found in scoped guild")
            await member.add_roles(role)
            fresh = await guild.fetch_member(uid)
            return _ok("add_role", args, _member_payload(bot, fresh))
        except Exception as exc:
            _fail("add_role", args, exc)

    @mcp.tool(
        description=(
            "Remove a guild role from a member in the scoped guild. The role "
            "must exist. Returns the updated member payload. "
            "Requires Manage Roles. "
            f"{identity_policy}"
        )
    )
    async def remove_role(uid: StrictInt, role_id: StrictInt) -> str:
        """Remove a role. (Agent-facing text is the decorator description above.)"""
        args = {"uid": uid, "role_id": role_id}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            if not isinstance(role_id, int) or isinstance(role_id, bool) or role_id <= 0:
                raise ValueError(f"invalid role_id: {role_id!r}")
            role = guild.get_role(role_id)
            if role is None:
                raise LookupError(f"role {role_id} not found in scoped guild")
            await member.remove_roles(role)
            fresh = await guild.fetch_member(uid)
            return _ok("remove_role", args, _member_payload(bot, fresh))
        except Exception as exc:
            _fail("remove_role", args, exc)

    @mcp.tool(
        description=(
            "Create a role in the scoped guild: `permissions` is an int bit "
            "field, `colour` an optional int, `mentionable` a bool. Requires "
            "Manage Roles. "
            f"{identity_policy}"
        )
    )
    async def create_role(
        name: str,
        permissions: StrictInt = 0,
        colour: StrictInt | None = None,
        mentionable: bool = False,
    ) -> str:
        """Create a role. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {
            "name_len": len(name) if isinstance(name, str) else 0,
            "permissions": permissions,
            "mentionable": mentionable,
        }
        if colour is not None:
            args["colour"] = colour
        try:
            _require_name(name, "role")
            if isinstance(permissions, bool) or not isinstance(permissions, int) or permissions < 0:
                raise ValueError("permissions must be a non-negative int")
            if colour is not None and (
                isinstance(colour, bool) or not isinstance(colour, int) or colour < 0
            ):
                raise ValueError("colour must be a non-negative int")
            if not isinstance(mentionable, bool):
                raise ValueError("mentionable must be a bool")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            kwargs: dict[str, Any] = {
                "name": name,
                "permissions": discord.Permissions(permissions),
                "mentionable": mentionable,
            }
            if colour is not None:
                kwargs["colour"] = discord.Colour(value=colour)
            role = await guild.create_role(**kwargs)
            return _ok(
                "create_role",
                args,
                {"id": role.id, "name": role.name, "position": role.position},
            )
        except Exception as exc:
            _fail("create_role", args, exc)

    @mcp.tool(
        description=(
            "List all roles of the scoped guild in position order as "
            "[{id, name, position, member_count, mentionable}], including "
            "@everyone. "
            f"{identity_policy}"
        )
    )
    async def list_roles() -> str:
        """List guild roles. (Agent-facing text is the decorator description above.)"""
        try:
            guild = get_scoped_guild(_bot(), settings.guild_id)
            roles = [
                {
                    "id": r.id,
                    "name": r.name,
                    "position": r.position,
                    "member_count": len(r.members),
                    "mentionable": bool(r.mentionable),
                }
                for r in guild.roles
            ]
            return _ok("list_roles", {}, roles)
        except Exception as exc:
            _fail("list_roles", {}, exc)

    @mcp.tool(
        description=(
            "Time a member out for `minutes` (0 clears the timeout) in the "
            "scoped guild. Requires Moderate Members. "
            f"{identity_policy}"
        )
    )
    async def timeout(uid: StrictInt, minutes: StrictInt, reason: str | None = None) -> str:
        """Time out a member. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid, "minutes": minutes}
        if isinstance(reason, str):
            args["reason_len"] = len(reason)
        try:
            if reason is not None and not isinstance(reason, str):
                raise ValueError("reason must be a string")
            if isinstance(minutes, bool) or not isinstance(minutes, int) or minutes < 0:
                raise ValueError("minutes must be a non-negative int")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            if minutes == 0:
                await member.timeout(None, reason=reason)
            else:
                await member.timeout(datetime.timedelta(minutes=minutes), reason=reason)
            return _ok("timeout", args, {"uid": uid, "minutes": minutes})
        except Exception as exc:
            _fail("timeout", args, exc)

    @mcp.tool(
        description=(
            "Kick a member from the scoped guild. Destructive: no confirm step, "
            "every call is audited. Requires Kick Members. "
            f"{identity_policy}"
        )
    )
    async def kick(uid: StrictInt, reason: str | None = None) -> str:
        """Kick a member. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid}
        if isinstance(reason, str):
            args["reason_len"] = len(reason)
        try:
            if reason is not None and not isinstance(reason, str):
                raise ValueError("reason must be a string")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            member = await _resolve_member(bot, guild, uid)
            await member.kick(reason=reason)
            return _ok("kick", args, {"uid": uid, "kicked": True})
        except Exception as exc:
            _fail("kick", args, exc)

    @mcp.tool(
        description=(
            "Ban a user ID from the scoped guild (works even if the user "
            "already left; no message deletion: delete_message_seconds=0). "
            "Destructive: no confirm step, every "
            "call is audited. Requires Ban Members. "
            f"{identity_policy}"
        )
    )
    async def ban(uid: StrictInt, reason: str | None = None) -> str:
        """Ban a user. (Agent-facing text is the decorator description above.)"""
        args: dict[str, Any] = {"uid": uid}
        if isinstance(reason, str):
            args["reason_len"] = len(reason)
        try:
            if not isinstance(uid, int) or isinstance(uid, bool) or uid <= 0:
                raise ValueError(f"invalid uid: {uid!r}")
            if reason is not None and not isinstance(reason, str):
                raise ValueError("reason must be a string")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            await guild.ban(discord.Object(id=uid), delete_message_seconds=0, reason=reason)
            return _ok("ban", args, {"uid": uid, "banned": True})
        except Exception as exc:
            _fail("ban", args, exc)

    @mcp.tool(
        description=(
            f"Unban a user ID from the scoped guild. Requires Ban Members. {identity_policy}"
        )
    )
    async def unban(uid: StrictInt, reason: str | None = None) -> str:
        """Unban a user. (Agent-facing text is the decorator description above.)"""
        args = {"uid": uid}
        if isinstance(reason, str):
            args["reason_len"] = len(reason)
        try:
            if not isinstance(uid, int) or isinstance(uid, bool) or uid <= 0:
                raise ValueError(f"invalid uid: {uid!r}")
            if reason is not None and not isinstance(reason, str):
                raise ValueError("reason must be a string")
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            await guild.unban(discord.Object(id=uid), reason=reason)
            return _ok("unban", args, {"uid": uid, "unbanned": True})
        except Exception as exc:
            _fail("unban", args, exc)
