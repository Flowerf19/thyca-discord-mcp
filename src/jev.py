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
"""

from __future__ import annotations

import copy
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from conversation import project_conversation_state

JEV_MODEL = "jev-1.13.0"
JEV_BASE_URL = "https://api.typesafe.ai"
JEV_TIMEOUT = 10.0

QUESTION_IDS = (
    "addressee",
    "explicit_assistant_request",
    "participation",
)
PARTICIPATION_OPTIONS = ("respond", "observe")

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


# ---------------------------------------------------------------------------
# Questions (assistant_uid perspective; routing set)
# ---------------------------------------------------------------------------


def build_questions(state: Mapping[str, Any]) -> dict[str, Any]:
    """Build the three typed questions for a conversation state.

    Accepts raw caller state or a projected snapshot; the state is projected
    first so only canonical fields shape the questions. Criteria name
    participant UIDs only (display names are readability labels).
    """
    snapshot = project_conversation_state(state)
    names = {p["uid"]: p["display_name"] for p in snapshot["participants"]}
    assistant_uid = snapshot["assistant_uid"]
    return {
        "addressee": {
            "type": "choice",
            "instructions": (
                "Who is the actual conversational target of the new message, "
                "including reply links and surrounding context? A message that "
                "names, mentions, or quotes the assistant to request action "
                "addresses the assistant, even inside multi-party chatter. "
                "Talking ABOUT the assistant in the third person, without asking "
                "it to act, is not a direct address: use the actual human "
                "addressee, channel, or unclear instead. Display names are "
                "shown for readability only; answer with the stable UID."
            ),
            "criteria": {
                **dict(names),
                "channel": "the message addresses the group generally",
                "unclear": "not enough context to determine the addressee",
            },
        },
        "explicit_assistant_request": {
            "type": "noul",
            "instructions": (
                "The new message explicitly asks the assistant for a response or "
                "action, by name, mention, or clear request language. A mere "
                "mention of the assistant in passing, or a question addressed to "
                "someone else, does not count."
            ),
        },
        "participation": {
            "type": "choice",
            "instructions": (
                "Decide from the perspective of the assistant identified by "
                f"assistant_uid {assistant_uid!r}. Respond when the new message "
                "addresses the assistant directly, by name, mention, or explicit "
                "request, or continues a thread the assistant is actively part of. "
                "Keep listening (observe) for chit-chat between humans, reactions, "
                "emoji-only messages, and messages addressed to another human or "
                "the channel generally, even if the assistant is mentioned in "
                "passing. When the addressee is the assistant or the message "
                "explicitly requests the assistant, the answer is almost always "
                "respond."
            ),
            "criteria": {
                "respond": "the assistant responds to the new message",
                "observe": "the assistant keeps listening and stays silent",
            },
        },
    }


# ---------------------------------------------------------------------------
# Answer / usage / model validation
# ---------------------------------------------------------------------------


def _check_finite_prob(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise JevDataError(f"{where} must be a number, got {type(value).__name__}")
    number = float(value)
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        # number is a plain float: safe repr, never an untrusted object repr.
        raise JevDataError(f"{where} must be finite within 0..1, got {number!r}")
    return number


_JEV_VERSION_ID_RE = re.compile(r"jev-[0-9]+\.[0-9]+\.[0-9]+")


def _safe_model_or_none(value: object) -> str | None:
    """Return the model id when it is a verified Jev version id, else None.

    Only anchored ``jev-MAJOR.MINOR.PATCH`` ids are retained (the pinned
    ``jev-1.13.0`` plus other resolved version diagnostics such as
    ``jev-9.9.9``). Every other raw value -- aliases, generic text,
    secret-shaped strings -- is omitted so untrusted payloads never reach
    error metadata. Pinned equality is still enforced by
    :func:`validate_model_id`.
    """
    if not isinstance(value, str) or value == "":
        return None
    if len(value) > 64:
        return None
    if _JEV_VERSION_ID_RE.fullmatch(value) is None:
        return None
    return value


def validate_model_id(value: object) -> str:
    """Validate that the resolved model id is exactly the pinned Jev model."""
    safe = _safe_model_or_none(value)
    if safe is None:
        raise JevDataError("model must be a safe non-empty id")
    if safe != JEV_MODEL:
        raise JevDataError("model mismatch: response is not the pinned model")
    return safe


def validate_answers(questions: Mapping[str, Any], answers: object) -> dict[str, Any]:
    """Validate provider answers against the requested questions."""
    if not isinstance(answers, dict):
        raise JevDataError("answers must be an object")
    missing = [qid for qid in questions if qid not in answers]
    extra = [qid for qid in answers if qid not in questions]
    if missing or extra:
        # Generic only: never echo untrusted answer keys.
        raise JevDataError("answer keys mismatch")
    clean: dict[str, Any] = {}
    for qid, spec in questions.items():
        ans = answers[qid]
        if not isinstance(ans, dict):
            raise JevDataError(f"{qid}: answer must be an object")
        qtype = spec.get("type")
        if ans.get("type") != qtype:
            raise JevDataError(f"{qid}: expected type {qtype!r}")
        if qtype == "choice":
            choice = ans.get("choice")
            # Bound unhashable/nonsensical choices without raw repr crashes.
            if not isinstance(choice, str) or choice not in spec["criteria"]:
                if isinstance(choice, str):
                    # qid is a known question id; the untrusted choice is omitted.
                    raise JevDataError(f"{qid}: choice is not an allowed option")
                raise JevDataError(
                    f"{qid}: choice must be a string option, got {type(choice).__name__}"
                )
            probs = ans.get("probabilities")
            if not isinstance(probs, dict) or not probs:
                raise JevDataError(f"{qid}: probabilities must be a non-empty object")
            criteria = spec["criteria"]
            for opt, prob in probs.items():
                if not isinstance(opt, str) or opt not in criteria:
                    raise JevDataError(f"{qid}: probability key is not a criterion")
                _check_finite_prob(prob, f"{qid}.probabilities")
            # Recorded as reported; sums are approximately 1 per docs and are
            # not enforced here (no unsupported epsilon normalization). Sparse
            # subsets are allowed; junk keys are rejected above.
            entry: dict[str, Any] = {
                "type": "choice",
                "choice": choice,
                "probabilities": dict(probs),
            }
            # Choice answers carry confidence 0..1 (docs); preserve when
            # present, validate strictly, never invent for Noul.
            if "confidence" in ans:
                entry["confidence"] = _check_finite_prob(ans.get("confidence"), f"{qid}.confidence")
            clean[qid] = entry
        elif qtype == "noul":
            prob = _check_finite_prob(ans.get("noul"), f"{qid}.noul")
            clean[qid] = {"type": "noul", "noul": prob}
        else:
            raise JevDataError(f"{qid}: unsupported question type {qtype!r}")
    return clean


def validate_usage(usage: object) -> dict[str, int]:
    """Validate reported token usage: strict ints >= 0 (bools rejected)."""
    if not isinstance(usage, dict):
        raise JevDataError("usage must be an object")
    clean: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens"):
        value = usage.get(key)
        if type(value) is not int or value < 0:
            raise JevDataError(f"usage.{key} must be a strict int >= 0")
        clean[key] = value
    return clean


def _validated_carried_usage(usage: object) -> dict[str, int] | None:
    """Return carried usage when a provider attached valid known usage."""
    if usage is None:
        return None
    try:
        return validate_usage(usage)
    except JevDataError:
        return None


# ---------------------------------------------------------------------------
# Live SDK provider (lazy import; exactly zero retries; one call per client)
# ---------------------------------------------------------------------------


def _plain_sdk_answer(ans: object) -> dict:
    if isinstance(ans, dict):
        return dict(ans)
    kind = getattr(ans, "type", None)
    if kind == "choice":
        entry: dict[str, Any] = {
            "type": "choice",
            "choice": getattr(ans, "choice", None),
            "probabilities": dict(getattr(ans, "probabilities", {}) or {}),
        }
        if hasattr(ans, "confidence"):
            entry["confidence"] = getattr(ans, "confidence", None)
        return entry
    if kind == "noul":
        return {"type": "noul", "noul": getattr(ans, "noul", None)}
    raise JevError("validation", detail="unsupported answer type")


def _plain_sdk_usage(usage: object) -> dict:
    if isinstance(usage, dict):
        return usage
    return {
        "input_tokens": getattr(usage, "input_tokens", None),
        "output_tokens": getattr(usage, "output_tokens", None),
    }


def _classify_sdk_error(exc: BaseException) -> JevError:
    if not isinstance(exc, Exception):
        raise exc  # never convert cancellation/interrupt into a provider error
    name = type(exc).__name__
    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if isinstance(status, bool):
        status = None
    if not isinstance(status, int):
        status = None
    lowered = name.lower()
    if "auth" in lowered or "credential" in lowered or status == 401:
        return JevError("auth", status=status, detail=name)
    if "timeout" in lowered:
        return JevError("timeout", status=status, detail=name)
    if status == 422:
        return JevError("validation", status=status, detail=name)
    if "rate" in lowered or "limit" in lowered or status == 429:
        return JevError("network", status=status, detail=name)
    return JevError("network", status=status, detail=name)


_SDK_LOG_PREFIXES = ("typesafe", "typesafe_sdk", "httpx2", "httpcore2")
_KNOWN_SDK_LOGGERS = (
    "typesafe",
    "typesafe_sdk",
    "httpx2",
    "httpcore2",
    "httpcore2.connection",
    "httpcore2.http11",
    "httpcore2.http2",
    "httpcore2.proxy",
    "httpcore2.socks",
)
_SDK_LOG_OFF = logging.CRITICAL + 1  # matches SDK "off"; silences WARNING too


def _silence_unsolicited_sdk_logging() -> None:
    """Silence unsolicited SDK/transport diagnostics (factory preflight only).

    Pinned typesafe-sdk 0.7.2 emits raw provider bodies via ``typesafe_sdk``
    WARNING (``_prepare_system_one_response`` drops unknown answer types) and
    DEBUG/INFO wire logs when ``TYPESAFE_LOG_LEVEL`` is set; ``httpx2`` and
    ``httpcore2.*`` emit transport diagnostics. A root-only WARNING guard
    still leaks WARNINGs, and preconfigured child loggers with direct
    handlers bypass it. Silence known roots plus currently registered
    descendants with level/disabled/propagate guards so neither direct
    handlers nor root propagation can leak. Handlers/filters are left in
    place; unrelated loggers are untouched. No SDK import, env, or key
    access; no billing impact. Covers pinned 0.7.2 known loggers only.
    """
    names: set[str] = set(_KNOWN_SDK_LOGGERS)
    try:
        registered = list(logging.Logger.manager.loggerDict)
    except Exception:
        registered = []
    for name in registered:
        for prefix in _SDK_LOG_PREFIXES:
            if name == prefix or name.startswith(prefix + "."):
                names.add(name)
                break
    for name in names:
        try:
            logger = logging.getLogger(name)
        except Exception:
            continue
        try:
            logger.setLevel(_SDK_LOG_OFF)
        except Exception:
            pass
        try:
            logger.disabled = True
        except Exception:
            pass
        try:
            logger.propagate = False
        except Exception:
            pass


def make_sdk_provider(api_key: str) -> ProviderCallable:
    """Build the live provider. Imports the SDK lazily; exactly zero retries.

    Exact pinned interface only (typesafe-sdk 0.7.2, verified research):
    ``AsyncTypeSafeClient(api_key, base_url, model, retry, timeout)`` with
    ``RetryPolicy(max_retries=0)`` and ``timeout=10.0``, async context, and
    ``system_one(state, questions)``. Any missing/unsupported piece fails
    closed BEFORE any inference; never falls back to retry=None/defaults.
    """
    import importlib

    _silence_unsolicited_sdk_logging()
    try:
        sdk = importlib.import_module("typesafe_sdk")
    except ImportError:
        # No module means no request could have been sent: billing is known.
        raise JevError("unknown", detail="TypeSafeSDKMissing", unknown_billing=False) from None
    # Override TYPESAFE_LOG_LEVEL applied at SDK import; silence WARNINGs and
    # transport diagnostics that can carry raw provider bodies.
    _silence_unsolicited_sdk_logging()

    retry_cls = getattr(sdk, "RetryPolicy", None)
    client_cls = getattr(sdk, "AsyncTypeSafeClient", None)
    choice_cls = getattr(sdk, "Choice", None)
    noul_cls = getattr(sdk, "Noul", None)
    if retry_cls is None or client_cls is None:
        raise JevError("unknown", detail="UnsupportedSDKInterface", unknown_billing=False) from None
    if choice_cls is None or noul_cls is None:
        raise JevError("unknown", detail="UnsupportedSDKInterface", unknown_billing=False) from None
    try:
        retry_policy = retry_cls(max_retries=0)
    except Exception as exc:  # fail closed, no fallback
        raise JevError("unknown", detail=type(exc).__name__, unknown_billing=False) from None

    def _to_sdk_question(spec: dict) -> Any:
        try:
            if spec.get("type") == "noul":
                return noul_cls(instructions=spec.get("instructions"))
            return choice_cls(instructions=spec.get("instructions"), criteria=spec.get("criteria"))
        except Exception as exc:  # preflight, no request sent
            raise JevError("unknown", detail=type(exc).__name__, unknown_billing=False) from None

    async def call(state: dict, questions: dict) -> JevProviderResult:
        started = time.monotonic()
        try:
            client = client_cls(
                api_key=api_key,
                base_url=JEV_BASE_URL,
                model=JEV_MODEL,
                retry=retry_policy,
                timeout=JEV_TIMEOUT,
            )
        except Exception as exc:  # preflight, no request sent
            raise JevError("auth", detail=type(exc).__name__, unknown_billing=False) from None
        try:
            sdk_questions = {qid: _to_sdk_question(spec) for qid, spec in questions.items()}
        except JevError:
            aclose = getattr(client, "aclose", None)
            if callable(aclose):
                try:
                    await aclose()
                except Exception:
                    pass
            raise
        if not hasattr(client, "__aenter__"):
            raise JevError(
                "unknown", detail="UnsupportedSDKClient", unknown_billing=False
            ) from None
        response = None
        try:
            async with client as live:
                response = await live.system_one(state=state, questions=sdk_questions)
        except Exception as exc:  # mapped to bounded metadata
            error = _classify_sdk_error(exc)
            if response is not None:
                try:
                    known = validate_usage(_plain_sdk_usage(getattr(response, "usage", None)))
                except Exception:
                    known = None
                if known is not None:
                    error.usage = known
                    try:
                        error.model = _safe_model_or_none(getattr(response, "model", ""))
                    except Exception:
                        error.model = None
                    error.unknown_billing = False
            raise error from None
        latency_ms = int((time.monotonic() - started) * 1000)
        # Read USAGE first so a normalization failure after a billable reply
        # keeps known spend; answers/model validation stays in decide().
        try:
            usage = _plain_sdk_usage(getattr(response, "usage", None))
        except Exception as exc:  # unreadable usage
            raise JevError("validation", detail=type(exc).__name__, unknown_billing=True) from None
        carried_usage: dict | None
        try:
            carried_usage = validate_usage(usage)
        except JevDataError:
            carried_usage = None
        try:
            safe_model = _safe_model_or_none(getattr(response, "model", ""))
        except Exception:
            safe_model = None
        try:
            raw_answers = getattr(response, "answers", None) or {}
            answers = {qid: _plain_sdk_answer(a) for qid, a in dict(raw_answers).items()}
            model = str(getattr(response, "model", "") or "")
        except JevError as exc:
            exc.usage = carried_usage
            exc.model = safe_model
            exc.unknown_billing = carried_usage is None
            raise
        except Exception as exc:  # malformed response shape
            raise JevError(
                "validation",
                detail=type(exc).__name__,
                unknown_billing=carried_usage is None,
                usage=carried_usage,
                model=safe_model,
            ) from None
        # No answer/model validation here; decide() owns usage-first accounting.
        return JevProviderResult(answers=answers, usage=usage, model=model, latency_ms=latency_ms)

    return call


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
