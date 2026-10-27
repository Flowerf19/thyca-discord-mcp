"""Jev shared types: constants, errors, and result/decision shapes.

Bottom of the Jev dependency graph: no imports from sibling Jev modules,
so :mod:`jev.contract`, :mod:`jev.provider`, and :mod:`jev.adapter` can all
depend on this module without cycles.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

JEV_MODEL = "jev-1.13.0"
JEV_BASE_URL = "https://api.typesafe.ai"
JEV_TIMEOUT = 10.0

QUESTION_IDS = (
    "addressee",
    "explicit_assistant_request",
    "participation",
)
PARTICIPATION_OPTIONS = ("respond", "observe")


class JevDataError(ValueError):
    """Raised when answers, usage, or a model id violate the Jev contract."""


@dataclass
class JevError(Exception):
    """Bounded, key-free Jev failure metadata."""

    kind: str  # auth | network | timeout | validation | unknown
    status: int | None = None
    detail: str = ""
    unknown_billing: bool = True
    usage: dict | None = None
    model: str | None = None

    def __str__(self) -> str:
        status = f" status={self.status}" if self.status is not None else ""
        detail = f" detail={self.detail[:120]}" if self.detail else ""
        return f"{self.kind}{status}{detail}"


@dataclass
class JevProviderResult:
    """Raw-normalized single-call result from any provider (SDK or fake)."""

    answers: dict
    usage: dict
    model: str
    latency_ms: int


@dataclass(frozen=True)
class JevDecision:
    """Validated single-call Jev outcome.

    ``sender_uid`` is trusted SDK metadata upstream (never a model vote);
    ``participation`` is the routing signal, cross-checked against
    ``addressee``/``explicit_assistant_request`` by the caller.
    """

    answers: dict[str, Any]
    usage: dict[str, int]
    model: str
    latency_ms: int

    @property
    def addressee(self) -> str:
        return self.answers["addressee"]["choice"]

    @property
    def explicit_assistant_request(self) -> float:
        return self.answers["explicit_assistant_request"]["noul"]

    @property
    def participation(self) -> str:
        return self.answers["participation"]["choice"]


ProviderCallable = Callable[[dict[str, Any], dict[str, Any]], Awaitable[JevProviderResult]]
