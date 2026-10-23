"""Send-side service: one scoped Discord text send under the pinned mention policy.

Pure transport: no job correlation, no journaling, no caller session — the
caller owns all of that. ``reply_to_message_id`` is a plain Discord
``MessageReference`` (fail_if_not_exists), not a workflow correlation.
"""

from __future__ import annotations

from typing import Any

CONTENT_MAX = 2000


async def send_message(
    gateway: Any,
    *,
    channel_id: Any,
    content: Any,
    reply_to_message_id: Any = None,
) -> dict[str, Any]:
    """Send ``content`` to a scoped channel; returns the receipt row.

    Raises ``ValueError`` on invalid args (bad ids, empty/overlong content);
    gateway typed errors (``scope_denied``/``not_found``/``discord_api``)
    cover Discord failures.
    """
    for value, name in ((channel_id, "channel_id"),):
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"invalid {name}")
    if reply_to_message_id is not None and (
        not isinstance(reply_to_message_id, int)
        or isinstance(reply_to_message_id, bool)
        or reply_to_message_id <= 0
    ):
        raise ValueError("invalid reply_to_message_id")
    if not isinstance(content, str) or not content or len(content) > CONTENT_MAX:
        raise ValueError(f"content must be 1..{CONTENT_MAX} chars")
    message = await gateway.send(
        channel_id, content, reply_to_message_id=reply_to_message_id
    )
    return {
        "message_id": message.id,
        "channel_id": message.channel_id,
        "jump_url": message.jump_url,
        "ts": message.timestamp,
        "reply_to_message_id": message.reply_to_message_id,
    }


__all__ = ["CONTENT_MAX", "send_message"]
