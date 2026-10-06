"""Isolated Jev conversation-evaluation runner.

Offline by default: validates a fixture, prints case counts/categories, the
planned question count, and per-case state sizes. The default path performs
NO API calls and reads NO keys and NO environment variables.

Paid calls happen only with explicit ``--run`` plus mandatory
``--max-calls`` and ``--max-estimated-usd``. Budget is estimated from
REPORTED usage only (input tokens * $0.042 / 1M); usage is charged BEFORE
answer/model validation, and ``estimated_usd_spent`` means KNOWN reported
usage only (see ``billing_unknown`` when transport, timeout, bad usage, or a
pending interrupt leaves extra spend unknowable). The exact token amount of
a future request cannot be proven in advance, so the runner additionally
enforces a strict max-calls cap and stops before the next call once known
spent reaches the limit. No retries, no repeats, no concurrency. Progress is
persisted incrementally to a private 0600 report (owned fd, no path reopen)
after every case/failure and before every call.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import stat
import sys
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA_VERSION = 1
PINNED_MODEL = "jev-1.13.0"
SDK_BASE_URL = "https://api.typesafe.ai"
SDK_TIMEOUT = 10.0
USD_PER_MTOK_INPUT = Decimal("0.042")
TOKENS_PER_UNIT = Decimal(1_000_000)

QUESTION_IDS = (
    "addressee",
    "explicit_assistant_request",
    "participation",
)
CHOICE_QUESTIONS = ("addressee", "participation")
RESERVED_NAMES = ("channel", "unclear")
PARTICIPATION_OPTIONS = ("respond", "observe")
CASE_ID_RE = r"^[a-z0-9_]+$"

API_KEY_ENV = "TYPESAFE_API_KEY"
DOTENV_NAME = ".env"


class FixtureError(ValueError):
    """Raised when a fixture violates the data contract."""


class AnswerError(ValueError):
    """Raised when a provider answer violates the response contract."""


@dataclass
class ProviderResult:
    """Normalized single-case result from any provider (SDK or fake)."""

    answers: dict
    usage: dict
    model: str
    latency_ms: int


@dataclass
class ProviderError(Exception):
    """Bounded, key-free provider failure metadata."""

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


def script_dir() -> Path:
    return Path(__file__).resolve().parent


def project_root() -> Path:
    return script_dir().parent.parent


def default_cases_path() -> Path:
    return script_dir() / "cases.json"


# ---------------------------------------------------------------------------
# Fixture validation
# ---------------------------------------------------------------------------


def _is_plain_str(value: object) -> bool:
    return isinstance(value, str) and value != ""


def _check_bool(value: object, where: str) -> None:
    if type(value) is not bool:
        raise FixtureError(f"{where} must be a boolean, got {type(value).__name__}")


def _validate_case_id(case_id: object, seen: set[str]) -> str:
    if not isinstance(case_id, str) or re.match(CASE_ID_RE, case_id) is None:
        raise FixtureError(f"case id must be unique snake_case, got {case_id!r}")
    if case_id in seen:
        raise FixtureError(f"duplicate case id {case_id!r}")
    seen.add(case_id)
    return case_id


def validate_fixture(data: object, source_name: str = "fixture") -> list[dict]:
    """Validate the fixture data contract; return the case list."""
    if not isinstance(data, dict):
        raise FixtureError(f"{source_name}: root must be an object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise FixtureError(f"{source_name}: schema_version must be {SCHEMA_VERSION}")
    if not _is_plain_str(data.get("description")):
        raise FixtureError(f"{source_name}: description must be a non-empty string")
    cases = data.get("cases")
    if not isinstance(cases, list):
        raise FixtureError(f"{source_name}: cases must be a list")

    seen_ids: set[str] = set()
    for index, case in enumerate(cases):
        where = f"{source_name}.cases[{index}]"
        if not isinstance(case, dict):
            raise FixtureError(f"{where} must be an object")
        case_id = _validate_case_id(case.get("id"), seen_ids)
        where = f"{source_name}.{case_id}"
        for key in ("category", "description", "rationale"):
            if not _is_plain_str(case.get(key)):
                raise FixtureError(f"{where}.{key} must be a non-empty string")
        if case.get("participation_label") is not None:
            raise FixtureError(f"{where}.participation_label must be null (unscored)")
        if not isinstance(case.get("participation_notes"), str):
            raise FixtureError(f"{where}.participation_notes must be a string")

        state = case.get("state")
        if not isinstance(state, dict):
            raise FixtureError(f"{where}.state must be an object")
        for key in ("assistant_uid", "owner_uid"):
            if not _is_plain_str(state.get(key)):
                raise FixtureError(f"{where}.state.{key} must be a non-empty string")

        participants = state.get("participants")
        if not isinstance(participants, list) or not participants:
            raise FixtureError(f"{where}.state.participants must be a non-empty list")
        uids: list[str] = []
        names: dict[str, str] = {}
        for part in participants:
            if not isinstance(part, dict):
                raise FixtureError(f"{where}.state.participants entries must be objects")
            uid = part.get("uid")
            name = part.get("display_name")
            if not _is_plain_str(uid) or not _is_plain_str(name):
                raise FixtureError(f"{where}.state.participants need uid/display_name")
            assert isinstance(uid, str) and isinstance(name, str)
            if uid in RESERVED_NAMES:
                raise FixtureError(f"{where}: participant uid {uid!r} is reserved")
            if uid in uids:
                raise FixtureError(f"{where}: duplicate participant uid {uid!r}")
            uids.append(uid)
            names[uid] = name
        for role in ("assistant_uid", "owner_uid"):
            if state[role] not in uids:
                raise FixtureError(f"{where}.state.{role} must be a participant")

        messages = state.get("messages")
        if not isinstance(messages, list) or not messages:
            raise FixtureError(f"{where}.state.messages must be a non-empty list")
        seen_mids: dict[str, int] = {}
        for mindex, msg in enumerate(messages):
            mwhere = f"{where}.state.messages[{mindex}]"
            if not isinstance(msg, dict):
                raise FixtureError(f"{mwhere} must be an object")
            mid = msg.get("id")
            sender = msg.get("sender_uid")
            if not _is_plain_str(mid):
                raise FixtureError(f"{mwhere}.id must be a non-empty string")
            assert isinstance(mid, str)
            if mid in seen_mids:
                raise FixtureError(f"{mwhere}: duplicate message id {mid!r}")
            if sender not in uids:
                raise FixtureError(f"{mwhere}.sender_uid {sender!r} is not a participant")
            if not isinstance(msg.get("text"), str):
                raise FixtureError(f"{mwhere}.text must be a string")
            reply_to = msg.get("reply_to")
            if reply_to is not None:
                if not isinstance(reply_to, str) or reply_to == "":
                    raise FixtureError(f"{mwhere}.reply_to must be null or a string id")
                if reply_to in seen_mids:
                    pass  # known parent precedes child: fine
                elif reply_to in [m.get("id") for m in messages]:
                    raise FixtureError(
                        f"{mwhere}.reply_to {reply_to!r} must precede the child message"
                    )
                # else: unknown parent link is allowed (legitimate missing parent)
            timestamp = msg.get("timestamp", None)
            if timestamp is not None and not isinstance(timestamp, str):
                raise FixtureError(f"{mwhere}.timestamp must be a string when present")
            seen_mids[mid] = mindex

        new_id = state.get("new_message_id")
        if new_id not in seen_mids:
            raise FixtureError(f"{where}.state.new_message_id must reference a message")

        expected = case.get("expected")
        if not isinstance(expected, dict):
            raise FixtureError(f"{where}.expected must be an object")
        addressee = expected.get("addressee")
        if addressee not in uids and addressee not in RESERVED_NAMES:
            raise FixtureError(f"{where}.expected.addressee {addressee!r} out of bounds")
        _check_bool(expected.get("explicit_assistant_request"), f"{where}.expected")
    return cases


def load_cases(path: Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FixtureError(f"cases file not found: {path}") from None
    except json.JSONDecodeError as exc:
        raise FixtureError(f"cases file is not valid JSON: {exc}") from exc
    return validate_fixture(data, source_name=str(path))


# ---------------------------------------------------------------------------
# Model payloads (source state only; never gold)
# ---------------------------------------------------------------------------


def build_model_state(case: dict) -> dict:
    """Return the model-visible state. Gold fields are never included."""
    state = case["state"]
    return {
        "assistant_uid": state["assistant_uid"],
        "owner_uid": state["owner_uid"],
        "participants": [
            {"uid": p["uid"], "display_name": p["display_name"]} for p in state["participants"]
        ],
        "messages": [
            {k: m[k] for k in ("id", "sender_uid", "text", "reply_to") if k in m}
            | ({"timestamp": m["timestamp"]} if "timestamp" in m else {})
            for m in state["messages"]
        ],
        "new_message_id": state["new_message_id"],
    }


def build_questions(case: dict) -> dict:
    """Same question set for every case; Choice criteria name participants only.

    Mirrors ``modum.jev.build_questions`` (3 routing questions); the
    consistency test pins them equal. Sender identity is trusted metadata
    and reply-link state is structural context, so neither is asked here.
    """
    names = {p["uid"]: p["display_name"] for p in case["state"]["participants"]}
    assistant_uid = case["state"]["assistant_uid"]
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
# Answer / usage validation
# ---------------------------------------------------------------------------


def _check_finite_prob(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AnswerError(f"{where} must be a number, got {type(value).__name__}")
    number = float(value)
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        raise AnswerError(f"{where} must be finite within 0..1, got {value!r}")
    return number


def _safe_model_or_none(value: object) -> str | None:
    """Return the model id when it is a safe non-empty id, else None."""
    if not isinstance(value, str) or value == "":
        return None
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value) or value != value.strip():
        return None
    return value


def _check_model_id(value: object) -> str:
    safe = _safe_model_or_none(value)
    if safe is None:
        raise AnswerError("model must be a safe non-empty id")
    if safe != PINNED_MODEL:
        raise AnswerError("model mismatch: response is not the pinned model")
    return safe


def validate_answers(questions: dict, answers: object) -> dict:
    """Validate provider answers against requested questions; halt on problems."""
    if not isinstance(answers, dict):
        raise AnswerError("answers must be an object")
    missing = [qid for qid in questions if qid not in answers]
    extra = [qid for qid in answers if qid not in questions]
    if missing or extra:
        raise AnswerError(f"answer keys mismatch: missing={missing} extra={extra}")
    clean: dict = {}
    for qid, spec in questions.items():
        ans = answers[qid]
        if not isinstance(ans, dict):
            raise AnswerError(f"{qid}: answer must be an object")
        qtype = spec.get("type")
        if ans.get("type") != qtype:
            raise AnswerError(f"{qid}: expected type {qtype!r}")
        if qtype == "choice":
            choice = ans.get("choice")
            # Bound unhashable/nonsensical choices without raw repr crashes.
            if not isinstance(choice, str) or choice not in spec["criteria"]:
                if isinstance(choice, str):
                    raise AnswerError(f"{qid}: choice {choice[:60]!r} is not allowed")
                raise AnswerError(
                    f"{qid}: choice must be a string option, got {type(choice).__name__}"
                )
            probs = ans.get("probabilities")
            if not isinstance(probs, dict) or not probs:
                raise AnswerError(f"{qid}: probabilities must be a non-empty object")
            criteria = spec["criteria"]
            for opt, prob in probs.items():
                if not isinstance(opt, str) or opt not in criteria:
                    raise AnswerError(f"{qid}: probability key is not a criterion")
                _check_finite_prob(prob, f"{qid}.probabilities")
            # Recorded as reported; sums are approximately 1 per docs and are
            # not enforced here (no unsupported epsilon normalization). Sparse
            # subsets are allowed; junk keys are rejected above.
            entry: dict = {
                "type": "choice",
                "choice": choice,
                "probabilities": dict(probs),
            }
            # SDK Choice answers carry confidence 0..1 (docs); preserve when
            # present, validate strictly, never invent for Noul.
            if "confidence" in ans:
                entry["confidence"] = _check_finite_prob(ans.get("confidence"), f"{qid}.confidence")
            clean[qid] = entry
        elif qtype == "noul":
            prob = _check_finite_prob(ans.get("noul"), f"{qid}.noul")
            clean[qid] = {"type": "noul", "noul": prob}
        else:
            raise AnswerError(f"{qid}: unsupported question type {qtype!r}")
    return clean


def validate_usage(usage: object) -> dict:
    if not isinstance(usage, dict):
        raise AnswerError("usage must be an object")
    clean = {}
    for key in ("input_tokens", "output_tokens"):
        value = usage.get(key)
        if type(value) is not int or value < 0:
            raise AnswerError(f"usage.{key} must be a strict int >= 0")
        clean[key] = value
    return clean


def estimate_usd(usage: dict) -> Decimal:
    """Estimated spend from REPORTED input tokens only (output is free)."""
    return Decimal(usage["input_tokens"]) * USD_PER_MTOK_INPUT / TOKENS_PER_UNIT


def brier_score(probs: list[float], outcomes: list[bool]) -> float:
    return sum((p - (1.0 if y else 0.0)) ** 2 for p, y in zip(probs, outcomes, strict=True)) / len(
        probs
    )


# ---------------------------------------------------------------------------
# Evaluation loop (fake-provider injectable; no SDK/env/network here)
# ---------------------------------------------------------------------------


@dataclass
class CaseRecord:
    case_id: str
    model: str
    latency_ms: int
    answers: dict
    usage: dict
    estimated_usd: str


@dataclass
class EvalReport:
    selected: int
    total_cases: int
    selection_reason: str | None
    attempted: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    stopped_reason: str | None = None
    estimated_usd_spent: str = "0"
    billing_unknown: bool = False
    billing_note: str | None = None
    pending_case: str | None = None
    per_question: dict = field(default_factory=dict)
    explicit_brier: dict = field(default_factory=dict)
    by_category: dict = field(default_factory=dict)
    participation_distribution: dict = field(default_factory=dict)
    cases: list[dict] = field(default_factory=list)
    failed_cases: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "model_pin": PINNED_MODEL,
            "selected": self.selected,
            "total_cases": self.total_cases,
            "selection_reason": self.selection_reason,
            "attempted": self.attempted,
            "completed": self.completed,
            "failed": self.failed,
            "stopped_reason": self.stopped_reason,
            "estimated_usd_spent": self.estimated_usd_spent,
            "billing_unknown": self.billing_unknown,
            "billing_note": self.billing_note,
            "pending_case": self.pending_case,
            "per_question": self.per_question,
            "explicit_brier": self.explicit_brier,
            "by_category": self.by_category,
            "participation_distribution": self.participation_distribution,
            "cases": self.cases,
            "failed_cases": self.failed_cases,
        }


async def evaluate(
    cases: list[dict],
    provider,
    max_calls: int,
    budget_limit: Decimal,
    on_progress=None,
) -> EvalReport:
    """Run selected cases in order; stop on budget, billing-unknown, or schema halt.

    Usage is charged BEFORE answer/model validation. Never retries or repeats
    a request. Cancellation (BaseException) propagates to the caller.
    ``on_progress`` is called with the report initially, before every call
    (pending durably tracked), and after every case/failure before the next
    call; persistence failure stops at once with no further inference.
    """
    total = len(cases)
    selected_cases = cases[:max_calls]
    reason = None
    if max_calls < total:
        reason = f"limited to first {len(selected_cases)} of {total} by --max-calls"
    report = EvalReport(selected=len(selected_cases), total_cases=total, selection_reason=reason)
    spent = Decimal(0)
    records: dict[str, CaseRecord] = {}
    had_unknown = False

    def _note_unknown(note: str) -> None:
        nonlocal had_unknown
        had_unknown = True
        report.billing_unknown = True
        report.billing_note = note

    def _sync_known() -> None:
        report.pending_case = None
        report.billing_unknown = had_unknown
        if not had_unknown:
            report.billing_note = None
        report.estimated_usd_spent = str(spent)

    def _emit() -> None:
        _score(report, cases, records)
        if on_progress is not None:
            on_progress(report)

    _emit()  # initial progress before any call

    for case in selected_cases:
        if spent >= budget_limit:
            report.stopped_reason = (
                f"known estimated spend {spent} USD >= limit {budget_limit} USD; "
                "stopping before the next call"
            )
            _sync_known()
            _emit()
            break
        case_id = case["id"]
        state = build_model_state(case)
        questions = build_questions(case)
        report.attempted.append(case_id)
        report.pending_case = case_id
        # Pending outcome is unknowable until the reply lands; attest UNKNOWN.
        report.billing_unknown = True
        report.billing_note = (
            f"pending {case_id}: paid outcome unknown until the reply lands; "
            "known spent so far is reported separately"
        )
        report.estimated_usd_spent = str(spent)
        _emit()  # durable pending BEFORE the SDK call
        try:
            result = await provider(state, questions)
        except ProviderError as exc:
            carried = _validated_carried_usage(exc)
            if carried is not None:
                cost = estimate_usd(carried)
                spent += cost
                report.failed.append(case_id)
                report.failed_cases.append(
                    {
                        "case_id": case_id,
                        "failure_kind": "provider_error",
                        "error": str(exc)[:160],
                        "usage": carried,
                        "estimated_usd": str(cost),
                        "model": exc.model if _safe_model_or_none(exc.model) is not None else None,
                    }
                )
                report.stopped_reason = f"provider {exc}; stopping with no auto-retry"
                _sync_known()
                _emit()
                break
            _note_unknown(
                f"provider {exc.kind} on {case_id}: no valid usage reported; "
                "unknown extra amount may have been billed"
            )
            report.failed.append(case_id)
            report.failed_cases.append(
                {"case_id": case_id, "failure_kind": "provider_error", "error": str(exc)[:160]}
            )
            report.stopped_reason = f"provider {exc}; stopping with no auto-retry"
            report.pending_case = None
            report.estimated_usd_spent = str(spent)
            _emit()
            break
        except Exception as exc:  # noqa: BLE001 - bounded, then halt
            _note_unknown(
                f"unexpected {type(exc).__name__} on {case_id}: "
                "unknown extra amount may have been billed"
            )
            report.failed.append(case_id)
            report.failed_cases.append(
                {"case_id": case_id, "failure_kind": "unexpected", "error": type(exc).__name__[:80]}
            )
            report.stopped_reason = f"unexpected {type(exc).__name__} on {case_id}; halting"
            report.pending_case = None
            report.estimated_usd_spent = str(spent)
            _emit()
            break
        # Usage FIRST: any billable reply with valid usage is charged even if
        # answers/model later prove malformed.
        try:
            usage = validate_usage(result.usage)
        except Exception:
            _note_unknown(
                f"bad/missing usage on {case_id}: unknown extra amount may have been billed"
            )
            report.failed.append(case_id)
            report.failed_cases.append(
                {"case_id": case_id, "failure_kind": "validation", "error": "bad usage"}
            )
            report.stopped_reason = f"schema problem on {case_id}: bad usage; halting"
            report.pending_case = None
            report.estimated_usd_spent = str(spent)
            _emit()
            break
        cost = estimate_usd(usage)
        spent += cost
        safe_model = _safe_model_or_none(result.model)
        try:
            answers = validate_answers(questions, result.answers)
            model = _check_model_id(result.model)
        except Exception as exc:  # noqa: BLE001 - bounded halt, usage kept
            report.failed.append(case_id)
            entry: dict = {
                "case_id": case_id,
                "failure_kind": "validation",
                "error": str(exc)[:160],
                "usage": usage,
                "estimated_usd": str(cost),
            }
            if safe_model is not None:
                entry["model"] = safe_model
            report.failed_cases.append(entry)
            report.stopped_reason = f"schema problem on {case_id}: {exc}; halting"
            _sync_known()
            _emit()
            break
        records[case_id] = CaseRecord(
            case_id=case_id,
            model=model,
            latency_ms=result.latency_ms,
            answers=answers,
            usage=usage,
            estimated_usd=str(cost),
        )
        report.completed.append(case_id)
        _sync_known()
        _emit()  # durable progress BEFORE the next call

    report.estimated_usd_spent = str(spent)
    _emit()  # final scored report
    return report


def _validated_carried_usage(exc: ProviderError) -> dict | None:
    """Return carried usage when the provider attached valid known usage."""
    if exc.usage is None:
        return None
    try:
        return validate_usage(exc.usage)
    except AnswerError:
        return None


def _score(report: EvalReport, cases: list[dict], records: dict[str, CaseRecord]) -> None:
    # Derived snapshot rebuilt from records on every emit: interrupts keep full
    # completed-case results. Idempotent; never touches attempted/completed/
    # failed/pending/billing/failed_cases.
    report.per_question = {}
    report.explicit_brier = {}
    report.by_category = {}
    report.participation_distribution = {}
    report.cases = []
    by_id = {c["id"]: c for c in cases}
    for qid in CHOICE_QUESTIONS:
        matches = 0
        denominator = 0
        for case_id in report.completed:
            case = by_id[case_id]
            gold = _gold_answer(case, qid)
            if gold is None:  # no gold (participation): fail closed, exclude
                continue
            denominator += 1
            if records[case_id].answers[qid].get("choice") == gold:
                matches += 1
        report.per_question[qid] = {"matches": matches, "denominator": denominator}

    probs: list[float] = []
    outcomes: list[bool] = []
    for case_id in report.completed:
        gold = by_id[case_id]["expected"]["explicit_assistant_request"]
        if type(gold) is bool:
            probs.append(records[case_id].answers["explicit_assistant_request"]["noul"])
            outcomes.append(gold)
    if probs:
        report.explicit_brier = {
            "brier": brier_score(probs, outcomes),
            "n": len(probs),
            "note": "probabilities reported as-is; no threshold applied",
        }
    else:
        report.explicit_brier = {"brier": None, "n": 0, "note": "no scored cases"}

    for case_id in report.completed:
        case = by_id[case_id]
        cat = case["category"]
        entry = report.by_category.setdefault(
            cat, {"completed": 0, "matches": {}, "denominator": {}}
        )
        entry["completed"] += 1
        for qid in CHOICE_QUESTIONS:
            gold = _gold_answer(case, qid)
            if gold is None:
                continue
            entry["denominator"][qid] = entry["denominator"].get(qid, 0) + 1
            if records[case_id].answers[qid].get("choice") == gold:
                entry["matches"][qid] = entry["matches"].get(qid, 0) + 1

    dist: dict[str, int] = {}
    for case_id in report.completed:
        choice = records[case_id].answers["participation"].get("choice")
        dist[str(choice)] = dist.get(str(choice), 0) + 1
    report.participation_distribution = {
        "counts": dist,
        "unscored": True,
        "note": "exploratory distribution only; no correctness score",
    }

    for case_id in report.completed:
        rec = records[case_id]
        report.cases.append(
            {
                "case_id": rec.case_id,
                "model": rec.model,
                "latency_ms": rec.latency_ms,
                "answers": rec.answers,
                "usage": rec.usage,
                "estimated_usd": rec.estimated_usd,
            }
        )


def _gold_answer(case: dict, qid: str) -> str | None:
    expected = case["expected"]
    if qid == "addressee":
        return expected[qid]
    return None  # participation has no gold and is never scored


# ---------------------------------------------------------------------------
# Paid provider (lazy SDK import; only constructed for --run)
# ---------------------------------------------------------------------------


def load_api_key(root: Path) -> str:
    """Read the key from the environment, else from the project .env file only."""
    key = os.environ.get(API_KEY_ENV)
    if key is not None:
        key = key.strip()
        if key:
            return key
        raise ProviderError("auth", detail="empty key in environment")
    try:
        dotenv = __import__("importlib").import_module("dotenv")
    except ImportError:
        raise ProviderError("auth", detail="no env key and dotenv unavailable") from None
    found = dotenv.get_key(str(root / DOTENV_NAME), API_KEY_ENV)
    if not found or not found.strip():
        raise ProviderError("auth", detail="no key in environment or project .env")
    return found.strip()


def _plain_sdk_answer(ans: object) -> dict:
    if isinstance(ans, dict):
        out = dict(ans)
        # Preserve documented Choice confidence when present; never invent.
        return out
    kind = getattr(ans, "type", None)
    if kind == "choice":
        entry: dict = {
            "type": "choice",
            "choice": getattr(ans, "choice", None),
            "probabilities": dict(getattr(ans, "probabilities", {}) or {}),
        }
        if hasattr(ans, "confidence"):
            entry["confidence"] = getattr(ans, "confidence", None)
        return entry
    if kind == "noul":
        return {"type": "noul", "noul": getattr(ans, "noul", None)}
    raise ProviderError("validation", detail="unsupported answer type")


def _plain_sdk_usage(usage: object) -> dict:
    if isinstance(usage, dict):
        return usage
    return {
        "input_tokens": getattr(usage, "input_tokens", None),
        "output_tokens": getattr(usage, "output_tokens", None),
    }


def make_sdk_provider(api_key: str):
    """Build the real provider. Imports the SDK lazily; exactly zero retries.

    Exact pinned interface only (typesafe-sdk 0.7.2, verified research):
    ``AsyncTypeSafeClient(api_key, base_url, model, retry, timeout)`` with
    ``RetryPolicy(max_retries=0)`` and ``timeout=10.0``, async context, and
    ``system_one(state, questions)``. Any missing/unsupported piece fails
    closed BEFORE any inference; never falls back to retry=None/defaults.
    """
    import importlib

    try:
        sdk = importlib.import_module("typesafe_sdk")
    except ImportError:
        raise ProviderError("unknown", detail="TypeSafeSDKMissing") from None
    # Never allow TYPESAFE_LOG_LEVEL=DEBUG/INFO to enable request/auth logs.
    logging.getLogger("typesafe").setLevel(logging.WARNING)
    logging.getLogger("typesafe_sdk").setLevel(logging.WARNING)

    retry_cls = getattr(sdk, "RetryPolicy", None)
    client_cls = getattr(sdk, "AsyncTypeSafeClient", None)
    choice_cls = getattr(sdk, "Choice", None)
    noul_cls = getattr(sdk, "Noul", None)
    if retry_cls is None or client_cls is None:
        raise ProviderError("unknown", detail="UnsupportedSDKInterface") from None
    if choice_cls is None or noul_cls is None:
        raise ProviderError("unknown", detail="UnsupportedSDKInterface") from None
    try:
        retry_policy = retry_cls(max_retries=0)
    except Exception as exc:  # noqa: BLE001 - fail closed, no fallback
        raise ProviderError("unknown", detail=type(exc).__name__, unknown_billing=False) from None

    def _to_sdk_question(spec: dict):
        try:
            if spec.get("type") == "noul":
                return noul_cls(instructions=spec.get("instructions"))
            return choice_cls(instructions=spec.get("instructions"), criteria=spec.get("criteria"))
        except Exception as exc:  # noqa: BLE001 - preflight, no request sent
            raise ProviderError(
                "unknown", detail=type(exc).__name__, unknown_billing=False
            ) from None

    async def call(state: dict, questions: dict) -> ProviderResult:
        started = time.monotonic()
        try:
            client = client_cls(
                api_key=api_key,
                base_url=SDK_BASE_URL,
                model=PINNED_MODEL,
                retry=retry_policy,
                timeout=SDK_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 - preflight, no request sent
            raise ProviderError("auth", detail=type(exc).__name__, unknown_billing=False) from None
        try:
            sdk_questions = {qid: _to_sdk_question(spec) for qid, spec in questions.items()}
        except ProviderError:
            aclose = getattr(client, "aclose", None)
            if callable(aclose):
                try:
                    await aclose()
                except Exception:
                    pass
            raise
        if not hasattr(client, "__aenter__"):
            raise ProviderError(
                "unknown", detail="UnsupportedSDKClient", unknown_billing=False
            ) from None
        response = None
        try:
            async with client as live:
                response = await live.system_one(state=state, questions=sdk_questions)
        except Exception as exc:  # noqa: BLE001 - mapped to bounded metadata
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
        # keeps known spend; answers/model validation stays central in evaluate.
        try:
            usage = _plain_sdk_usage(getattr(response, "usage", None))
        except Exception as exc:  # noqa: BLE001 - unreadable usage
            raise ProviderError(
                "validation", detail=type(exc).__name__, unknown_billing=True
            ) from None
        carried_usage: dict | None
        try:
            carried_usage = validate_usage(usage)
        except AnswerError:
            carried_usage = None
        safe_model = _safe_model_or_none(getattr(response, "model", ""))
        try:
            raw_answers = getattr(response, "answers", None) or {}
            answers = {qid: _plain_sdk_answer(a) for qid, a in dict(raw_answers).items()}
            model = str(getattr(response, "model", "") or "")
        except ProviderError as exc:
            exc.usage = carried_usage
            exc.model = safe_model
            exc.unknown_billing = carried_usage is None
            raise
        except Exception as exc:  # noqa: BLE001 - malformed response shape
            raise ProviderError(
                "validation",
                detail=type(exc).__name__,
                unknown_billing=carried_usage is None,
                usage=carried_usage,
                model=safe_model,
            ) from None
        # No answer/model validation here; evaluate owns accounting.
        return ProviderResult(answers=answers, usage=usage, model=model, latency_ms=latency_ms)

    return call


def _classify_sdk_error(exc: BaseException) -> ProviderError:
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
        return ProviderError("auth", status=status, detail=name)
    if "timeout" in lowered:
        return ProviderError("timeout", status=status, detail=name)
    if status == 422:
        return ProviderError("validation", status=status, detail=name)
    if "rate" in lowered or "limit" in lowered or status == 429:
        return ProviderError("network", status=status, detail=name)
    return ProviderError("network", status=status, detail=name)


# ---------------------------------------------------------------------------
# Reporting (exclusive, mode 0600, prepared before any API/key read)
#
# Durability claim: the paid path holds an owned fd from O_EXCL creation and
# rewrites the same inode via that fd after every case/failure and before
# every call (flush+fsync). A crash/interrupt therefore keeps all previously
# persisted cases plus a pending-case attestation with billing UNKNOWN.
# Limitations: no cross-process resume, no directory fsync, and a host crash
# may lose the last fsync window per OS guarantees.
# ---------------------------------------------------------------------------


class ReportWriter:
    """Owns a private 0600 report leaf via an open fd; never reopens by path."""

    def __init__(self, path: Path, fd: int) -> None:
        self.path = path
        self.fd = fd
        st = os.fstat(fd)
        self._dev = st.st_dev
        self._ino = st.st_ino
        self.closed = False

    def _verify_identity(self) -> None:
        try:
            cur = os.lstat(self.path)
        except FileNotFoundError:
            raise FixtureError(f"report target vanished: {self.path}") from None
        if stat.S_ISLNK(cur.st_mode) or cur.st_dev != self._dev or cur.st_ino != self._ino:
            raise FixtureError(f"report target swapped, refusing to write: {self.path}")

    def write_report(self, report: EvalReport) -> None:
        if self.closed:
            raise FixtureError("report writer is closed")
        self._verify_identity()
        data = json.dumps(report.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")
        os.lseek(self.fd, 0, os.SEEK_SET)
        os.ftruncate(self.fd, 0)
        view = memoryview(data)
        while view:
            written = os.write(self.fd, view)
            view = view[written:]
        os.fsync(self.fd)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            try:
                os.close(self.fd)
            except OSError:
                pass


def prepare_report_writer(path: Path) -> ReportWriter:
    """Create a new 0600 leaf exclusively and retain the owned fd."""
    parent = path.parent
    if not parent.is_dir():
        raise FixtureError(f"--report parent must already exist: {parent}")
    if os.path.lexists(path):
        raise FixtureError(f"--report target already exists (no overwrite): {path}")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except OSError:
        pass
    return ReportWriter(path, fd)


def default_report_writer() -> ReportWriter:
    """New owned temp dir 0700 plus a new 0600 leaf; retains the owned fd."""
    owned_dir = Path(tempfile.mkdtemp(prefix="thyca-jev-runner-"))
    try:
        os.chmod(owned_dir, 0o700)
    except OSError:
        pass
    leaf = owned_dir / "report.json"
    fd = os.open(leaf, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except OSError:
        pass
    return ReportWriter(leaf, fd)


def prepare_report_file(path: Path) -> Path:
    """Test helper: exclusive 0600 creation; paid path must use the writer."""
    writer = prepare_report_writer(path)
    out = writer.path
    writer.close()
    return out


def default_report_file() -> Path:
    writer = default_report_writer()
    out = writer.path
    writer.close()
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Isolated Jev conversation eval runner")
    parser.add_argument("--cases", type=str, default=str(default_cases_path()))
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--max-calls", type=str, default=None)
    parser.add_argument("--max-estimated-usd", type=str, default=None)
    parser.add_argument("--report", type=str, default=None)
    return parser.parse_args(argv)


def parse_paid_limits(args: argparse.Namespace) -> tuple[int, Decimal]:
    if args.max_calls is None or args.max_estimated_usd is None:
        raise FixtureError("--run requires --max-calls and --max-estimated-usd")
    try:
        max_calls = int(args.max_calls)
    except (TypeError, ValueError):
        raise FixtureError("--max-calls must be a positive integer") from None
    if max_calls <= 0:
        raise FixtureError("--max-calls must be a positive integer")
    try:
        budget = Decimal(str(args.max_estimated_usd))
    except (InvalidOperation, ValueError):
        raise FixtureError("--max-estimated-usd must be a positive decimal") from None
    if not budget.is_finite() or budget <= 0:
        raise FixtureError("--max-estimated-usd must be positive and finite")
    return max_calls, budget


def describe_offline(cases: list[dict]) -> str:
    categories: dict[str, int] = {}
    lines = []
    for case in cases:
        categories[case["category"]] = categories.get(case["category"], 0) + 1
        state_json = json.dumps(build_model_state(case), ensure_ascii=False)
        lines.append(
            f"  {case['id']}: "
            f"state_chars={len(state_json)} "
            f"messages={len(case['state']['messages'])} "
            f"participants={len(case['state']['participants'])}"
        )
    header = [
        f"cases={len(cases)} categories={categories}",
        f"planned_questions_per_case={len(QUESTION_IDS)}"
        f" total_planned={len(cases) * len(QUESTION_IDS)}",
        "mode=offline (no API calls, no key reads)",
        "note: exact token amounts cannot be proven in advance; "
        "no token or cost estimate is claimed here",
    ]
    return "\n".join(header + lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        cases = load_cases(Path(args.cases))
    except FixtureError as exc:
        print(f"fixture error: {exc}", file=sys.stderr)
        return 2

    if not args.run:
        print(describe_offline(cases))
        return 0

    if not cases:
        print("limit error: paid run needs at least one case", file=sys.stderr)
        return 2

    try:
        max_calls, budget = parse_paid_limits(args)
    except FixtureError as exc:
        print(f"limit error: {exc}", file=sys.stderr)
        return 2

    # Validate the report target BEFORE any key read or API call.
    try:
        writer = (
            prepare_report_writer(Path(args.report)) if args.report else default_report_writer()
        )
    except FixtureError as exc:
        print(f"report error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"report error: {type(exc).__name__}", file=sys.stderr)
        return 2
    report_path = writer.path

    def _emit(report: EvalReport) -> None:
        writer.write_report(report)

    # Key read happens only on the paid path, after fixture + report validation.
    try:
        api_key = load_api_key(project_root())
    except ProviderError as exc:
        writer.close()
        print(f"credential error: {exc}", file=sys.stderr)
        return 2

    try:
        provider = make_sdk_provider(api_key)
    except ProviderError as exc:
        writer.close()
        print(f"provider error: {exc}", file=sys.stderr)
        return 2

    try:
        report = asyncio.run(evaluate(cases, provider, max_calls, budget, on_progress=_emit))
    except (KeyboardInterrupt, asyncio.CancelledError):
        # All on_progress writes so far are already durable (owned fd,
        # fsync); the last write before the in-flight call carries
        # pending_case plus billing_unknown=true. Do not overwrite it here.
        writer.close()
        print(
            f"interrupted; partial results persisted report={report_path} "
            "pending_billing_unknown=true",
            file=sys.stderr,
        )
        return 130
    except (OSError, FixtureError) as exc:
        writer.close()
        print(f"persistence error: {type(exc).__name__}", file=sys.stderr)
        return 1
    writer.close()
    print(
        f"selected={report.selected}/{report.total_cases} "
        f"attempted={len(report.attempted)} completed={len(report.completed)} "
        f"failed={len(report.failed)} spent_usd={report.estimated_usd_spent} "
        f"billing_unknown={report.billing_unknown} "
        f"stopped={report.stopped_reason} report={report_path}"
    )
    # Budget-limited success (no failures) is exit 0; provider/schema or
    # persistence stops must not look like a passing automation run.
    if report.failed:
        return 1
    if report.stopped_reason and "persistence" in report.stopped_reason.lower():
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
