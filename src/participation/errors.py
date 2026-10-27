"""Bounded participation error types (no IO, no clocks, no provider calls)."""

from __future__ import annotations

from enum import Enum


class ParticipationError(ValueError):
    """Corrupt store, invalid operation, or failed closed guard."""


class ParticipationReason(str, Enum):
    """Bounded pending reasons; never carries raw payload text."""

    IN_PROGRESS = "in_progress"
    ALREADY_STARTED = "already_started"
    ALREADY_FAILED_KNOWN = "already_failed_known"
    ALREADY_FAILED_UNKNOWN = "already_failed_unknown"
    CONTEXT_MISMATCH = "context_mismatch"
    UNKNOWN_BLOCKED = "unknown_blocked"


class ParticipationPending(ParticipationError):
    """No decision available now; caller must not retry the provider call."""

    def __init__(self, reason: ParticipationReason) -> None:
        if not isinstance(reason, ParticipationReason):
            raise ParticipationError("participation pending needs a bounded reason")
        self.reason = reason
        super().__init__(f"participation pending: {reason.value}")
