"""JevAdapter: one projected call, usage-first validation, typed decision."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from conversation import project_conversation_state

from .contract import (
    _safe_model_or_none,
    _validated_carried_usage,
    build_questions,
    validate_answers,
    validate_model_id,
    validate_usage,
)
from .provider import make_sdk_provider
from .types import JevDataError, JevDecision, JevError, ProviderCallable

# ---------------------------------------------------------------------------
# Adapter: one projected call, usage-first validation, typed decision
# ---------------------------------------------------------------------------


class JevAdapter:
    """Single-call Jev facade over canonical conversation state.

    The constructor stores its arguments only (no SDK import, no env read,
    no inference). :meth:`decide` projects the state, builds the questions,
    calls the provider exactly once with no retry, and returns a validated
    :class:`JevDecision`. Pass ``provider`` to inject a fake in tests; pass
    ``api_key`` (from the composition root) for the live SDK path.
    """

    def __init__(
        self, *, api_key: str | None = None, provider: ProviderCallable | None = None
    ) -> None:
        self._api_key = api_key
        self._provider = provider

    def _require_api_key(self) -> str:
        key = self._api_key
        if key is None or (isinstance(key, str) and not key.strip()):
            raise JevError("auth", detail="no api key provided", unknown_billing=False)
        if not isinstance(key, str):
            raise JevError("auth", detail="invalid api key", unknown_billing=False)
        return key

    async def decide(self, state: Mapping[str, Any]) -> JevDecision:
        """Run one Jev call for ``state`` and return the typed decision.

        Raises :class:`ConversationError` before any provider contact when
        the state violates the canonical contract, and :class:`JevError`
        (bounded, key-free, usage-carrying) for provider-phase failures.
        Cancellation propagates untouched; it is never a decision.
        """
        snapshot = project_conversation_state(state)
        questions = build_questions(snapshot)
        provider = self._provider
        if provider is None:
            provider = make_sdk_provider(self._require_api_key())
        # Validate against a frozen copy so a custom provider cannot weaken
        # validation by mutating the questions object it was handed.
        frozen_questions = copy.deepcopy(questions)
        try:
            result = await provider(snapshot, questions)
        except JevError as exc:
            carried = _validated_carried_usage(exc.usage)
            if carried is None and exc.usage is not None:
                exc.usage = None
                exc.unknown_billing = True
            exc.model = _safe_model_or_none(exc.model)
            raise
        except Exception as exc:  # unexpected provider failure; billing unknown
            raise JevError("unknown", detail=type(exc).__name__, unknown_billing=True) from None
        # Usage FIRST: a billable reply with valid usage keeps it even if
        # answers/model later prove malformed.
        try:
            usage = validate_usage(result.usage)
        except Exception:  # garbage shape reads as unknown billing too
            raise JevError("validation", detail="bad usage", unknown_billing=True) from None
        # Post-usage boundary: every shape/validation failure below retains
        # valid known usage. BaseException propagates untouched.
        safe_model: str | None = None
        try:
            try:
                raw_model_hint = result.model
            except Exception:
                raw_model_hint = None
            try:
                safe_model = _safe_model_or_none(raw_model_hint)
            except Exception:
                safe_model = None
            answers = validate_answers(frozen_questions, result.answers)
            model = validate_model_id(result.model)
            # latency_ms passes through unwitnessed (verified runner behavior);
            # it is informational and never affects billing, but a missing or
            # raising getter must not lose known usage.
            latency_ms = result.latency_ms
        except JevDataError as exc:
            raise JevError(
                "validation",
                detail=str(exc)[:120],
                unknown_billing=False,
                usage=usage,
                model=safe_model,
            ) from None
        except Exception as exc:  # garbage result shape; type-name only, usage kept
            raise JevError(
                "validation",
                detail=type(exc).__name__,
                unknown_billing=False,
                usage=usage,
                model=safe_model,
            ) from None
        return JevDecision(answers=answers, usage=usage, model=model, latency_ms=latency_ms)
