"""Live TypeSafe SDK provider (lazy import; exactly zero retries)."""

from __future__ import annotations

import logging
import time
from typing import Any

from .contract import _safe_model_or_none, validate_usage
from .types import (
    JEV_BASE_URL,
    JEV_MODEL,
    JEV_TIMEOUT,
    JevDataError,
    JevError,
    JevProviderResult,
    ProviderCallable,
)

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
