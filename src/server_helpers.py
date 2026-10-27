"""Shared server boundary mappings for the composition root.

Coarse audit error kinds stay here so the composition root (server core)
and the tick builders (server_ticks) map failures identically. The scoped
text-channel resolver lives in :mod:`source_messages` (shared without a
responder->server_helpers->responder import cycle) and is re-exported here
so existing ``server_helpers._resolve_text_channel`` imports keep working.
Correlated-tool audit args live in the server core allowlist instead.
"""

from __future__ import annotations

import discord

from responder import ResponderError
from source_messages import _resolve_text_channel
from store import StoreError

__all__ = [
    "_resolve_text_channel",
    "_coarse_error_kind",
]


def _coarse_error_kind(exc: BaseException) -> str:
    """Fixed coarse audit kind (never raw text or class names)."""
    if isinstance(exc, discord.DiscordException):
        return "discord_error"
    if isinstance(exc, StoreError):
        return "delivery_error"
    if isinstance(
        exc, (ResponderError, StoreError, ValueError, TypeError, LookupError, PermissionError)
    ):
        return "validation_error"
    return "unknown"
