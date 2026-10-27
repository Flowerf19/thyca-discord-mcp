"""Jev contract: question building plus answer/usage/model validation."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any

from conversation import project_conversation_state

from .types import JEV_MODEL, JevDataError

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
