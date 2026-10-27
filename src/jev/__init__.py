"""Minimal TypeSafe Jev adapter over a shared conversation snapshot.

Single-call inference only: project the state, build the three
routing questions, call the provider exactly once, and validate
usage before answers so a billable reply keeps its reported usage even when
answers or the model id prove malformed.

Boundaries (accepted scope):
- No retries, requeue, polling, scheduling, budget policy, metrics, gold
  scoring, or report writing. No Discord, journal, ACK, or send side effects.
- ``participation`` (respond/observe) is the only routing signal; the other
  two answers exist so a deterministic override can cross-check the vote
  (address the assistant / explicit request, yet voted observe).
- ``sender_uid``/``reply_parent_status`` are NOT asked: sender identity is
  trusted SDK metadata upstream and reply-link state is structural context,
  neither needs a model vote.
- Importing this module or constructing the adapter performs no work: the
  SDK is imported lazily only when a live call is actually built, the API
  key comes from the composition root (never env/dotenv here), and the key
  is never repr'd or logged.

Internal layout: :mod:`jev.types` (constants/errors/shapes),
:mod:`jev.contract` (questions + validation), :mod:`jev.provider` (live SDK
transport), :mod:`jev.adapter` (:class:`JevAdapter`). Import from here,
e.g. ``from jev import JevAdapter, build_questions``.
"""

from .adapter import JevAdapter  # noqa: F401 - canonical re-export
from .contract import (  # noqa: F401 - canonical re-exports
    build_questions,
    validate_answers,
    validate_model_id,
    validate_usage,
)
from .provider import (
    _classify_sdk_error,  # noqa: F401 - test-imported shim
    make_sdk_provider,  # noqa: F401 - canonical re-export
)
from .types import (  # noqa: F401 - canonical re-exports
    JEV_BASE_URL,
    JEV_MODEL,
    JEV_TIMEOUT,
    PARTICIPATION_OPTIONS,
    QUESTION_IDS,
    JevDataError,
    JevDecision,
    JevError,
    JevProviderResult,
)

__all__ = [
    "JEV_BASE_URL",
    "JEV_MODEL",
    "JEV_TIMEOUT",
    "PARTICIPATION_OPTIONS",
    "QUESTION_IDS",
    "JevAdapter",
    "JevDataError",
    "JevDecision",
    "JevError",
    "JevProviderResult",
    "build_questions",
    "make_sdk_provider",
    "validate_answers",
    "validate_model_id",
    "validate_usage",
]

# ``_classify_sdk_error`` is intentionally NOT redeclared here: the name above
# is the authoritative ``jev.provider._classify_sdk_error`` object,
# re-exported so the existing ``from jev import _classify_sdk_error`` test
# import keeps working without touching the single definition.
