"""Daily native-session helpers: SID grammar, logical days, history validation.

Pure stdlib module (no project imports, no file IO, no clock reads except
inside the provider closure returned by
:func:`default_session_day_provider`): the rotation subsystem's owner of
the verified native SID grammar (same shape as the native store), the
rotation "logical day", and the day -> SID history map shared by the
mapping registry, the delivery state, and the participation ledger.

A logical day runs from ``rollover_hour`` (inclusive) to the next
``rollover_hour`` (exclusive) in the explicit ``session_day_timezone``
(default ``Asia/Ho_Chi_Minh``, rollover 04:00). The 04:00 boundary keeps
a late-night conversation (23:30-00:30) inside ONE native session so its
Thyca context is never split across midnight. The timezone is explicit
(IANA name) so rotation never shifts when the host clock zone changes.

The Jev billing day (UTC, owned by the participation store) is a
different concern and is intentionally untouched here.

Value validators return ``None`` on invalid input (caller raises the
domain error); ``logical_day`` raises ``ValueError`` for bad clock
config (timezone/rollover/naive ``now``).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
    native SID. Unbounded by design (one small entry per rotation day,
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


def parse_tz_name(tz_name: Any) -> ZoneInfo | None:
    """Return the ZoneInfo for an IANA name, or None when invalid."""
    if not isinstance(tz_name, str) or not tz_name:
        return None
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def logical_day(
    now: datetime,
    *,
    tz_name: str,
    rollover_hour: int,
) -> str:
    """Return the YYYY-MM-DD logical day for an aware ``now``.

    The logical day flips at ``rollover_hour`` (0-23) in ``tz_name``:
    wall times before the rollover hour still belong to the previous
    calendar day. Raises ``ValueError`` for a naive ``now``, an unknown
    timezone, or a non 0-23 rollover hour (bools rejected).
    """
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be a timezone-aware datetime")
    if type(rollover_hour) is not int or not 0 <= rollover_hour <= 23:
        raise ValueError("rollover_hour must be an int 0..23")
    tz = parse_tz_name(tz_name)
    if tz is None:
        raise ValueError("tz_name must be a valid IANA timezone")
    local = now.astimezone(tz)
    day = local.date()
    if local.hour < rollover_hour:
        day = day - timedelta(days=1)
    return day.isoformat()


def default_session_day_provider(
    tz_name: str,
    rollover_hour: int,
) -> Callable[[], str]:
    """Build the production logical-day provider (real UTC clock).

    Validates the timezone and rollover hour EAGERLY (raises
    ``ValueError``) so a bad clock config fails at construction, never
    mid-tick. The returned closure reads ``datetime.now(timezone.utc)``
    on every call.
    """
    if parse_tz_name(tz_name) is None:
        raise ValueError("tz_name must be a valid IANA timezone")
    if type(rollover_hour) is not int or not 0 <= rollover_hour <= 23:
        raise ValueError("rollover_hour must be an int 0..23")

    def _provider() -> str:
        return logical_day(
            datetime.now(timezone.utc),
            tz_name=tz_name,
            rollover_hour=rollover_hour,
        )

    return _provider


def sid_day_guess(native_sid: Any) -> str | None:
    """Guess the creation day of a native SID from its timestamp prefix.

    Native SIDs embed local wall time (``YYYY-MM-DDTHH-...``); the
    leading 10 chars are the creation calendar day. Used ONLY to seed
    provenance history when adopting a legacy (dateless) mapping --
    never for rotation gating. Returns None for non-SID input.
    """
    if validate_native_sid(native_sid) is None:
        return None
    return validate_day(native_sid[:10])


__all__ = [
    "DAY_RE",
    "NATIVE_SID_RE",
    "default_session_day_provider",
    "logical_day",
    "parse_tz_name",
    "sid_day_guess",
    "validate_day",
    "validate_native_sid",
    "validate_sessions_history",
]
