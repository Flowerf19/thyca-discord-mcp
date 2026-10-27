"""Thyca error taxonomy (single-defined; re-exported via :mod:`thyca`)."""

from __future__ import annotations


class ThycaClientError(Exception):
    """Base for coarse, metadata-only client failures (never raw bodies)."""

    def __init__(
        self,
        kind: str,
        *,
        status_code: int | None = None,
        reason: str = "",
        field: str = "",
    ) -> None:
        self.kind = kind
        self.status_code = status_code
        self.reason = reason
        self.field = field
        super().__init__(self._public_message())

    def _public_message(self) -> str:
        # Safe enums/ints only. Never include native bodies or str(exc).
        parts = [self.kind]
        if self.reason:
            parts.append(f"reason={self.reason}")
        if self.field:
            parts.append(f"field={self.field}")
        parts.append(f"status={self.status_code}")
        return " ".join(parts)


class ThycaPreconditionError(ThycaClientError):
    """Local validation failed or native returned 400/404 (no raw body)."""

    def __init__(
        self,
        *,
        status_code: int | None = None,
        reason: str = "local_validation",
        field: str = "",
    ) -> None:
        super().__init__("precondition", status_code=status_code, reason=reason, field=field)


class ThycaBusyError(ThycaClientError):
    """Native 409: session busy. Safe QUEUED; wait for idle, no new submit."""

    def __init__(self, *, status_code: int | None = 409) -> None:
        self.queued = True
        self.outcome = "queued"
        super().__init__("busy", status_code=status_code, reason="http_409")


class ThycaSubmissionUncertainError(ThycaClientError):
    """POST timeout/disconnect: native may still be running. Do not retry."""

    def __init__(self, *, reason: str = "timeout") -> None:
        self.uncertain = True
        self.outcome = "unknown"
        super().__init__("submission_uncertain", status_code=None, reason=reason)


class ThycaProtocolError(ThycaClientError):
    """5xx, redirect, invalid JSON, malformed shape, or GET transport issue."""

    def __init__(self, *, status_code: int | None = None, reason: str = "protocol") -> None:
        super().__init__("protocol", status_code=status_code, reason=reason)


class _TimeoutMarker(Exception):
    pass


class _NetworkMarker(Exception):
    pass
