"""MCP server (stdio) sharing one asyncio loop with the Discord bot.

FastMCP.run() drives an asyncio loop; the lifespan hook starts the bot in
that same loop, so tools can await discord.py coroutines directly with no
thread bridge. Every tool call is audited; failures are audited then re-raised.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, NoReturn

import discord
from mcp.server.fastmcp import FastMCP

from .audit import append_audit
from .bot import build_bot, get_scoped_guild, resolve_display_name
from .config import Settings
from .identity import classify_actor
from .tasks import TaskRegistry


def create_server(settings: Settings) -> FastMCP:
    registry = TaskRegistry()
    state: dict[str, Any] = {"bot": None}

    def audit(tool: str, args: dict[str, Any], ok: bool = True, error: str | None = None) -> None:
        append_audit(
            settings.audit_path,
            session_id=settings.session_id,
            actor="mcp",
            tool=tool,
            args=args,
            ok=ok,
            error=error,
        )

    def _ok(tool: str, args: dict[str, Any], payload: Any) -> str:
        audit(tool, args, ok=True)
        return json.dumps(payload, ensure_ascii=False)

    def _fail(tool: str, args: dict[str, Any], exc: BaseException) -> NoReturn:
        audit(tool, args, ok=False, error=repr(exc))
        raise exc

    def _bot() -> discord.Client:
        bot = state["bot"]
        if bot is None or not bot.is_ready():
            raise RuntimeError("discord bot not ready")
        return bot

    async def heartbeat() -> None:
        """Liveness signal: snapshot member count to the audit log every 5 minutes."""
        bot = state["bot"]
        if bot is None:
            return
        try:
            guild = get_scoped_guild(bot, settings.guild_id)
            append_audit(
                settings.audit_path,
                session_id=settings.session_id,
                actor="system",
                tool="heartbeat",
                args={"members": guild.member_count},
                ok=True,
            )
        except Exception as exc:
            # Heartbeat failure is recorded, not hidden; the loop itself survives.
            append_audit(
                settings.audit_path,
                session_id=settings.session_id,
                actor="system",
                tool="heartbeat",
                args={},
                ok=False,
                error=repr(exc),
            )

    registry.register("heartbeat", heartbeat, seconds=300.0)

    @asynccontextmanager
    async def lifespan(app: FastMCP):  # noqa: ARG001 - FastMCP passes the app
        bot = build_bot(settings)
        state["bot"] = bot
        # Split start() into login + background connect: wait_until_ready()
        # needs login() to have run first (it raises otherwise).
        await bot.login(settings.discord_token.get_secret_value())
        connect_task = asyncio.create_task(bot.connect(), name="discord-gateway")
        ready_task = asyncio.create_task(bot.wait_until_ready(), name="discord-ready")
        done, _pending = await asyncio.wait(
            {connect_task, ready_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if ready_task not in done:
            # Gateway died before ready (bad token, missing privileged
            # intents, ...): surface the real cause instead of hanging.
            ready_task.cancel()
            await asyncio.gather(ready_task, return_exceptions=True)
            if not bot.is_closed():
                await bot.close()
            await asyncio.gather(connect_task, return_exceptions=True)
            exc = connect_task.exception()
            raise RuntimeError(f"discord gateway failed before ready: {exc!r}") from exc
        await ready_task  # propagate ready-wait errors, if any
        registry.start("heartbeat")
        try:
            yield {"bot": bot, "tasks": registry}
        finally:
            await registry.stop("heartbeat")
            if not bot.is_closed():
                await bot.close()
            await asyncio.gather(connect_task, return_exceptions=True)

    mcp = FastMCP("thyca-discord-mcp", lifespan=lifespan)

    @mcp.tool()
    async def guild_info() -> str:
        """Return id, name, member count and channel counts of the scoped guild."""
        try:
            guild = get_scoped_guild(_bot(), settings.guild_id)
            return _ok(
                "guild_info",
                {},
                {
                    "id": guild.id,
                    "name": guild.name,
                    "member_count": guild.member_count,
                    "text_channels": len(guild.text_channels),
                    "voice_channels": len(guild.voice_channels),
                },
            )
        except Exception as exc:
            _fail("guild_info", {}, exc)

    @mcp.tool()
    async def list_text_channels() -> str:
        """List text channels of the scoped guild as id/name/topic/position."""
        try:
            guild = get_scoped_guild(_bot(), settings.guild_id)
            channels = [
                {"id": c.id, "name": c.name, "topic": c.topic, "position": c.position}
                for c in guild.text_channels
            ]
            return _ok("list_text_channels", {}, channels)
        except Exception as exc:
            _fail("list_text_channels", {}, exc)

    @mcp.tool()
    async def send_message(content: str, channel_id: int | None = None) -> str:
        """Send a message; defaults to the configured channel. Rejects out-of-guild targets."""
        cid = channel_id if channel_id is not None else settings.default_channel_id
        args = {"channel_id": cid, "content": content}
        try:
            bot = _bot()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = guild.get_channel(cid)
            if channel is None:
                channel = await bot.fetch_channel(cid)
            if not isinstance(channel, discord.TextChannel) or channel.guild.id != guild.id:
                raise PermissionError(f"refusing to send outside scoped guild: {cid}")
            message = await channel.send(content)
            return _ok(
                "send_message",
                args,
                {"message_id": message.id, "channel_id": cid, "jump_url": message.jump_url},
            )
        except Exception as exc:
            _fail("send_message", args, exc)

    @mcp.tool()
    async def whoami(uid: int) -> str:
        """Classify a Discord UID as self/owner/other with its current display name."""
        try:
            bot = _bot()
            assert bot.user is not None
            guild = get_scoped_guild(bot, settings.guild_id)
            display_name = await resolve_display_name(bot, guild, uid)
            actor = classify_actor(
                uid, display_name, bot_uid=bot.user.id, owner_uid=settings.owner_uid
            )
            return _ok(
                "whoami",
                {"uid": uid},
                {"uid": actor.uid, "display_name": actor.display_name, "kind": actor.kind.value},
            )
        except Exception as exc:
            _fail("whoami", {"uid": uid}, exc)

    @mcp.tool()
    async def list_members(limit: int = 50) -> str:
        """List guild members as uid/display_name/kind (owner flagged). Max 500."""
        limit = max(1, min(limit, 500))
        try:
            bot = _bot()
            assert bot.user is not None
            guild = get_scoped_guild(bot, settings.guild_id)
            members = []
            async for member in guild.fetch_members(limit=limit):
                actor = classify_actor(
                    member.id,
                    member.display_name,
                    bot_uid=bot.user.id,
                    owner_uid=settings.owner_uid,
                )
                members.append(
                    {"uid": actor.uid, "display_name": actor.display_name, "kind": actor.kind.value}
                )
            return _ok("list_members", {"limit": limit}, members)
        except Exception as exc:
            _fail("list_members", {"limit": limit}, exc)

    @mcp.tool()
    async def list_tasks() -> str:
        """List background tasks with running state and loop counts."""
        try:
            infos = [t.__dict__ for t in registry.list_tasks()]
            return _ok("list_tasks", {}, infos)
        except Exception as exc:
            _fail("list_tasks", {}, exc)

    @mcp.tool()
    async def start_task(name: str) -> str:
        """Start a registered background task by name."""
        try:
            registry.start(name)
            info = registry.status(name)
            return _ok("start_task", {"name": name}, info.__dict__)
        except Exception as exc:
            _fail("start_task", {"name": name}, exc)

    @mcp.tool()
    async def stop_task(name: str) -> str:
        """Stop a running background task by name (no-op if idle)."""
        try:
            await registry.stop(name)
            info = registry.status(name)
            return _ok("stop_task", {"name": name}, info.__dict__)
        except Exception as exc:
            _fail("stop_task", {"name": name}, exc)

    return mcp
