"""Canonical conversation context: validation and projection.

Shared, provider-neutral projection of caller-provided conversation state.
Both the Jev adapter (``modum.jev``) and future Thyca wiring consume this
snapshot so speaker identity, reply links, and timestamps stay identical.

Accepted scope notes:
- Input is caller-provided state only; this module collects nothing from
  Discord, the network, the environment, or any session store.
- Display names are readability labels only (duplicates allowed); stable
  UIDs carry identity.
- Message text is untrusted raw content; it is preserved byte-identical and
  never echoed in errors.
- An unknown ``reply_to`` parent is legitimate (explicitly missing); only a
  known parent must precede the child message.
- No window, truncation, recency, threshold, or routing policy lives here;
  those decisions stay open for the parent wiring.
- Importing this module performs no work: stdlib only, no env reads.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict

RESERVED_PARTICIPANT_UIDS = ("channel", "unclear")
REPLY_STATUSES = ("none", "visible", "missing")

__all__ = [
    "RESERVED_PARTICIPANT_UIDS",
    "REPLY_STATUSES",
    "ConversationError",
    "ConversationMessage",
    "ConversationState",
    "Participant",
    "get_new_message",
    "new_message_reply_status",
    "project_conversation_state",
]


class Participant(TypedDict):
    uid: str
    display_name: str


class _MessageBase(TypedDict):
    id: str
    sender_uid: str
    text: str


class ConversationMessage(_MessageBase, total=False):
    reply_to: str | None
    timestamp: str


class ConversationState(TypedDict):
    assistant_uid: str
    owner_uid: str
    participants: list[Participant]
    messages: list[ConversationMessage]
    new_message_id: str


class ConversationError(ValueError):
    """Raised when caller-provided state violates the canonical contract."""


def _is_plain_str(value: object) -> bool:
    return isinstance(value, str) and value != ""


def _short(value: object, limit: int = 80) -> str:
    """Bounded identifier for error messages; never echoes message text."""
    text = value if isinstance(value, str) else type(value).__name__
    return text if len(text) <= limit else text[:limit] + "..."


def project_conversation_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Validate caller state and return a detached snapshot with exact fields.

    The snapshot contains only ``assistant_uid``, ``owner_uid``,
    ``participants`` (``uid``/``display_name``), ``messages`` (``id``,
    ``sender_uid``, ``text``, plus ``reply_to``/``timestamp`` when present),
    and ``new_message_id``. Every other key -- including any gold or
    annotation keys at any nesting level -- is dropped, never forwarded.
    The snapshot shares no mutable structure with the caller.
    """
    if not isinstance(state, Mapping):
        raise ConversationError("state must be an object")
    assistant_uid = state.get("assistant_uid")
    owner_uid = state.get("owner_uid")
    if not _is_plain_str(assistant_uid):
        raise ConversationError("state.assistant_uid must be a non-empty string")
    if not _is_plain_str(owner_uid):
        raise ConversationError("state.owner_uid must be a non-empty string")

    participants = state.get("participants")
    if not isinstance(participants, list) or not participants:
        raise ConversationError("state.participants must be a non-empty list")
    uids: list[str] = []
    clean_participants: list[dict[str, str]] = []
    for index, part in enumerate(participants):
        where = f"state.participants[{index}]"
        if not isinstance(part, Mapping):
            raise ConversationError(f"{where} must be an object")
        uid = part.get("uid")
        name = part.get("display_name")
        if not _is_plain_str(uid) or not _is_plain_str(name):
            raise ConversationError(f"{where} needs uid and display_name as non-empty strings")
        assert isinstance(uid, str) and isinstance(name, str)
        if uid in RESERVED_PARTICIPANT_UIDS:
            raise ConversationError(f"{where}.uid {_short(uid)!r} is reserved")
        if uid in uids:
            raise ConversationError(f"duplicate participant uid {_short(uid)!r}")
        uids.append(uid)
        clean_participants.append({"uid": uid, "display_name": name})
    if assistant_uid not in uids:
        raise ConversationError("state.assistant_uid must be a participant")
    if owner_uid not in uids:
        raise ConversationError("state.owner_uid must be a participant")

    messages = state.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ConversationError("state.messages must be a non-empty list")
    all_ids = [m.get("id") if isinstance(m, Mapping) else None for m in messages]
    seen: dict[str, int] = {}
    clean_messages: list[dict[str, Any]] = []
    for index, msg in enumerate(messages):
        where = f"state.messages[{index}]"
        if not isinstance(msg, Mapping):
            raise ConversationError(f"{where} must be an object")
        mid = msg.get("id")
        if not _is_plain_str(mid):
            raise ConversationError(f"{where}.id must be a non-empty string")
        assert isinstance(mid, str)
        if mid in seen:
            raise ConversationError(f"duplicate message id {_short(mid)!r}")
        sender = msg.get("sender_uid")
        if sender not in uids:
            raise ConversationError(f"{where}.sender_uid {_short(sender)!r} is not a participant")
        if not isinstance(msg.get("text"), str):
            raise ConversationError(f"{where}.text must be a string")
        if "reply_to" in msg:
            reply_to = msg.get("reply_to")
            if reply_to is not None:
                if not isinstance(reply_to, str) or reply_to == "":
                    raise ConversationError(
                        f"{where}.reply_to must be null or a non-empty string id"
                    )
                if reply_to in seen:
                    pass  # known parent precedes the child: fine
                elif reply_to in all_ids:
                    # Self reference or future reference: not a valid visible link.
                    raise ConversationError(
                        f"{where}.reply_to {_short(reply_to)!r} must precede the child message"
                    )
                # else: unknown parent link is allowed (explicitly missing)
        timestamp = msg.get("timestamp", None)
        if timestamp is not None and not isinstance(timestamp, str):
            raise ConversationError(f"{where}.timestamp must be a string when present")
        seen[mid] = index
        entry: dict[str, Any] = {"id": mid, "sender_uid": sender, "text": msg["text"]}
        if "reply_to" in msg:
            entry["reply_to"] = msg["reply_to"]
        if "timestamp" in msg:
            entry["timestamp"] = msg["timestamp"]
        clean_messages.append(entry)

    new_id = state.get("new_message_id")
    if not _is_plain_str(new_id) or new_id not in seen:
        raise ConversationError("state.new_message_id must reference a message")

    return {
        "assistant_uid": assistant_uid,
        "owner_uid": owner_uid,
        "participants": clean_participants,
        "messages": clean_messages,
        "new_message_id": new_id,
    }


def get_new_message(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return the new message from validated state (raw state or snapshot)."""
    snapshot = project_conversation_state(state)
    for msg in snapshot["messages"]:
        if msg["id"] == snapshot["new_message_id"]:
            return msg
    raise ConversationError("state.new_message_id must reference a message")


def new_message_reply_status(state: Mapping[str, Any]) -> str:
    """Classify the new message reply link: none, visible, or missing."""
    snapshot = project_conversation_state(state)
    new_id = snapshot["new_message_id"]
    ids_before: set[str] = set()
    for msg in snapshot["messages"]:
        if msg["id"] == new_id:
            reply_to = msg.get("reply_to")
            if reply_to is None:
                return "none"
            return "visible" if reply_to in ids_before else "missing"
        ids_before.add(msg["id"])
    raise ConversationError("state.new_message_id must reference a message")
