"""Native-session helpers: SID grammar, day keys, history validation.

Pure stdlib module (no project imports, no file IO, no clock reads):
ownes the verified native SID grammar (same shape as the native store)
plus the day -> SID history map shared by the delivery state and the
participation ledger.

The Jev billing day (UTC, owned by the participation store) is a
different concern and is intentionally untouched here.

Value validators return ``None`` on invalid input (caller raises the
domain error).
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

# Verified native SID grammar (mirrors thyca.sessions.store +
# thyca_client): date + 4 lowercase hex. Canonical copy; the responder
# and the native client validate against this shape.
NATIVE_SID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_[0-9a-f]{4}$")

# Calendar day key: strict YYYY-MM-DD shape; real-date checked separately.
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_native_sid(value: Any) -> str | None:
    """Return the native SID when it matches the strict grammar, else None."""
    if not isinstance(value, str):
        return None
    if not NATIVE_SID_RE.fullmatch(value):
        return None
    return value


def validate_day(value: Any) -> str | None:
    """Return the day key when it is a real YYYY-MM-DD calendar date, else None."""
    if not isinstance(value, str):
        return None
    if not DAY_RE.fullmatch(value):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    if parsed.isoformat() != value:
        return None
    return value


def validate_sessions_history(value: Any) -> dict[str, str] | None:
    """Validate a day -> native-SID history map; detached copy or None.

    Every key must be a real calendar day and every value a strict
    native SID. Unbounded by design (one small entry per adoption day,
    same growth shape as the delivery job journal).
    """
    if not isinstance(value, dict):
        return None
    cleaned: dict[str, str] = {}
    for day, sid in value.items():
        if validate_day(day) is None:
            return None
        if validate_native_sid(sid) is None:
            return None
        cleaned[day] = sid
    return cleaned


def validate_retired_sessions(value: Any) -> list[str] | None:
    """Validate a retired-SID list; detached copy or None.

    Every entry must be a strict native SID, in order, with no
    duplicates. Retired SIDs are old sessions that old ledger records may
    still reference: dropping one would orphan those records, so the list
    is preserved as loaded. Unbounded by design (one small entry per
    adoption at most).
    """
    if not isinstance(value, list):
        return None
    cleaned: list[str] = []
    for sid in value:
        if validate_native_sid(sid) is None:
            return None
        if sid in cleaned:
            return None
        cleaned.append(sid)
    return cleaned


__all__ = [
    "DAY_RE",
    "NATIVE_SID_RE",
    "validate_day",
    "validate_native_sid",
    "validate_retired_sessions",
    "validate_sessions_history",
]
