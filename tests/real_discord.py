"""Offline helper: real ``discord.Message`` objects on mock channels (Phase 1).

Proves typed-boundary behavior against real SDK objects instead of
``SimpleNamespace`` spoofs. Offline only: a never-logged-in
``discord.Client`` supplies the ``ConnectionState``; no network, no login.

Usage (option B from the Phase 1 change map): the channel stays a
``MagicMock(spec=discord.TextChannel)`` (``isinstance`` passes, scriptable
``fetch_message``/``history``/``send`` with the established tracking
attributes), while ``store``/``history_messages`` hold REAL
``discord.Message`` objects built from gateway payload dicts. Real messages
only read ``channel.id``/``channel.guild`` off the mock, so the mock's
``guild`` is wired to the real guild registered in the shared state.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import discord

_client = discord.Client(intents=discord.Intents.default())


def real_guild(guild_id: int, channels: dict[int, str] | None = None) -> discord.Guild:
    """Return the shared real guild for ``guild_id`` (created once, cached)."""
    state = _client._connection
    existing = state._get_guild(guild_id)
    if existing is not None:
        return existing
    payloads = [
        {"id": str(cid), "type": 0, "name": name, "position": i}
        for i, (cid, name) in enumerate((channels or {22: "general"}).items())
    ]
    guild = discord.Guild(
        state=state,
        data={
            "id": str(guild_id),
            "name": f"g{guild_id}",
            "member_count": 0,
            "channels": payloads,
            "roles": [],
            "emojis": [],
            "members": [],
        },
    )
    state._add_guild(guild)
    return guild


def real_channel(
    cid: int,
    guild_id: int,
    *,
    channel_name: str = "general",
) -> MagicMock:
    """Mock TextChannel wired to the shared real guild (isinstance passes)."""
    guild = real_guild(guild_id, {cid: channel_name})
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = guild
    channel.store = {}
    channel.history_messages = []
    channel.fetch_calls = []
    channel.history_calls = []
    channel.fetch_exc = None
    channel.history_exc = None
    channel.history_fn = None
    channel.deleted = set()

    async def fake_fetch(mid):
        channel.fetch_calls.append(mid)
        if channel.fetch_exc is not None:
            raise channel.fetch_exc
        if mid in channel.deleted:
            raise _deleted_exc()
        msg = channel.store.get(mid)
        if msg is None:
            raise _deleted_exc()
        return msg

    def fake_history(*, limit=100, before=None, after=None, oldest_first=None):
        channel.history_calls.append(
            {"limit": limit, "before": before, "after": after, "oldest_first": oldest_first}
        )
        if channel.history_exc is not None:
            exc = channel.history_exc

            async def _boom():
                raise exc
                yield  # unreachable, marks async generator

            return _boom()
        if channel.history_fn is not None:
            return channel.history_fn(limit=limit, before=before, oldest_first=oldest_first)

        async def _gen():
            before_id = getattr(before, "id", before)
            if not isinstance(before_id, int):
                before_id = 10**18
            cands = [m for m in channel.history_messages if m.id < before_id]
            cands.sort(key=lambda m: m.id)
            newest = cands[-limit:] if limit else []
            ordered = newest if oldest_first else list(reversed(newest))
            for m in ordered:
                yield m

        return _gen()

    def _send_impl(content, kwargs):
        ref = kwargs.get("reference")
        msg = real_message(
            9000 + len(channel.sent),
            44,
            "HelperBot",
            content,
            channel=channel,
            bot=True,
        )
        # Record the outgoing reference for kwarg assertions.
        channel.sent_refs.append(ref)
        channel.sent.append(msg)
        channel.allowed_mentions_seen.append(kwargs.get("allowed_mentions"))
        return msg

    async def _fake_send(content, **kwargs):
        return _send_impl(content, kwargs)

    channel.send = _fake_send
    channel.fetch_message = fake_fetch
    channel.history = fake_history
    channel.sent = []
    channel.sent_refs = []
    channel.allowed_mentions_seen = []
    channel.source_store = {"deleted": set(), "exc": None, "calls": []}
    return channel


def _deleted_exc() -> discord.NotFound:
    from types import SimpleNamespace

    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def real_message(
    mid: int,
    author_uid: int,
    author_name: str,
    content: str,
    *,
    channel: Any,
    bot: bool = False,
    reference_payload: dict[str, Any] | None = None,
    referenced_message: dict[str, Any] | Any | None = "__absent__",
    member_roles: list[str] | None = None,
) -> discord.Message:
    """Build a real ``discord.Message`` from a gateway payload dict.

    ``referenced_message="__absent__"`` (default) omits the key (``resolved``
    stays ``None`` -> fetch fallback); ``None`` means deleted parent
    (``DeletedReferencedMessage``); a dict embeds the resolved parent.
    """
    state = _client._connection
    # Evict any cached user for this UID first: ConnectionState.store_user
    # returns the FIRST cached object per id, so without this a uid reused
    # across tests (e.g. OTHER=55 as both "Ramon" and an evil name) would
    # keep the stale name. Each built message gets exactly its declared
    # author; previously built messages keep their own User objects.
    state._users.pop(author_uid, None)
    payload: dict[str, Any] = {
        "id": str(mid),
        "type": 0,
        "content": content,
        "author": {
            "id": str(author_uid),
            "username": author_name,
            "discriminator": "0",
            "avatar": None,
            "global_name": author_name,
            "bot": bot,
        },
        "mentions": [],
        "mention_roles": [],
    }
    if member_roles is not None:
        payload["member"] = {
            "roles": member_roles,
            "flags": 0,
            "joined_at": "2020-01-01T00:00:00+00:00",
            "deaf": False,
            "mute": False,
        }
    if reference_payload is not None:
        payload["message_reference"] = reference_payload
        if referenced_message != "__absent__":
            payload["referenced_message"] = referenced_message
    return discord.Message(state=state, channel=channel, data=payload)


def parent_payload(
    mid: int,
    author_uid: int,
    author_name: str,
    content: str,
    *,
    channel_id: int,
    guild_id: int,
) -> dict[str, Any]:
    """Full parent payload dict for embedding as ``referenced_message``."""
    return {
        "id": str(mid),
        "type": 0,
        "content": content,
        "channel_id": str(channel_id),
        "guild_id": str(guild_id),
        "author": {
            "id": str(author_uid),
            "username": author_name,
            "discriminator": "0",
            "avatar": None,
            "global_name": author_name,
        },
        "mentions": [],
        "mention_roles": [],
    }
