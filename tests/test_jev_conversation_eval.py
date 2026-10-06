"""Offline tests for the isolated Jev conversation-eval runner.

No SDK, no network, no real .env, no paid calls. The runner script is loaded
directly via importlib so these tests need no package dependencies.
"""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import logging
import os
import shutil
import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

RUNNER_PATH = (
    Path(__file__).resolve().parent.parent / "experiments" / "jev_conversation" / "run_eval.py"
)


def _load_runner():
    name = "jev_run_eval_under_test"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, RUNNER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


rx = _load_runner()


# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------


def _participants():
    return [
        {"uid": "alice_demo", "display_name": "Alice"},
        {"uid": "bot_demo", "display_name": "HelperBot"},
        {"uid": "carol_demo", "display_name": "Carol"},
    ]


def _messages(tag=""):
    return [
        {"id": f"{tag}m1", "sender_uid": "alice_demo", "text": "hi all", "reply_to": None},
        {
            "id": f"{tag}m2",
            "sender_uid": "bot_demo",
            "text": "hello Alice",
            "reply_to": f"{tag}m1",
        },
        {
            "id": f"{tag}m3",
            "sender_uid": "carol_demo",
            "text": "HelperBot, please summarize this thread",
            "reply_to": f"{tag}m2",
        },
    ]


def make_case(
    case_id="basic_thread",
    *,
    category="request",
    new_id=None,
    messages=None,
    addressee="bot_demo",
    explicit=True,
    notes="synthetic",
):
    tag = f"{case_id}__"
    default_messages = _messages(tag)
    resolved_messages = copy.deepcopy(messages) if messages is not None else default_messages
    resolved_new_id = new_id if new_id is not None else f"{tag}m3"
    return {
        "id": case_id,
        "category": category,
        "description": f"synthetic {case_id}",
        "state": {
            "assistant_uid": "bot_demo",
            "owner_uid": "alice_demo",
            "participants": _participants(),
            "messages": resolved_messages,
            "new_message_id": resolved_new_id,
        },
        "expected": {
            "addressee": addressee,
            "explicit_assistant_request": explicit,
        },
        "rationale": f"synthetic rationale for {case_id}",
        "participation_label": None,
        "participation_notes": notes,
    }


def make_fixture(cases):
    return {"schema_version": 1, "description": "synthetic offline fixture", "cases": cases}


def perfect_answers(case, *, participation="respond", explicit_prob=None):
    explicit = (
        explicit_prob
        if explicit_prob is not None
        else (1.0 if case["expected"]["explicit_assistant_request"] else 0.0)
    )
    return {
        "addressee": {
            "type": "choice",
            "choice": case["expected"]["addressee"],
            "probabilities": {"channel": 0.05, "unclear": 0.05},
        },
        "explicit_assistant_request": {"type": "noul", "noul": explicit},
        "participation": {
            "type": "choice",
            "choice": participation,
            "probabilities": {"respond": 0.8, "observe": 0.2},
        },
    }


class FakeProvider:
    """Injectable async provider; makes no SDK/env/network calls."""

    def __init__(self, answer_fn, usage=None, model="jev-1.13.0", fail_on=()):
        self.answer_fn = answer_fn
        self.usage = usage or {"input_tokens": 1000, "output_tokens": 50}
        self.model = model
        self.calls = []
        self.fail_on = set(fail_on)

    async def __call__(self, state, questions):
        self.calls.append((state, questions))
        case_id = _case_id_from_state(state)
        if case_id in self.fail_on:
            raise rx.ProviderError("timeout", detail="TypeSafeAPITimeoutError")
        return rx.ProviderResult(
            answers=self.answer_fn(state, questions),
            usage=dict(self.usage),
            model=self.model,
            latency_ms=7,
        )


def _case_id_from_state(state):
    return state.get("_test_case_id", "unknown")


def _provider_for(cases, **kwargs):
    by_new = {c["state"]["new_message_id"]: c for c in cases}
    default_fn = lambda s, q: perfect_answers(by_new[s["new_message_id"]])  # noqa: E731
    answer_fn = kwargs.pop("answer_fn", None) or default_fn
    provider = FakeProvider(answer_fn, **kwargs)

    async def call(state, questions):
        tagged = dict(state)
        tagged["_test_case_id"] = state["new_message_id"]
        return await provider(tagged, questions)

    call.inner = provider
    return call


# ---------------------------------------------------------------------------
# Fixture validation
# ---------------------------------------------------------------------------


def test_fixture_validation_accepts_minimal():
    cases = rx.validate_fixture(make_fixture([make_case()]), source_name="t")
    assert [c["id"] for c in cases] == ["basic_thread"]


def test_fixture_validation_rejects_non_boolean_explicit():
    case = make_case()
    case["expected"]["explicit_assistant_request"] = 1
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case]))


def test_fixture_validation_rejects_reserved_participant_uid():
    case = make_case()
    case["state"]["participants"].append({"uid": "channel", "display_name": "Fake"})
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case]))


def test_fixture_validation_rejects_future_reply_reference():
    msgs = [
        {"id": "m1", "sender_uid": "alice_demo", "text": "a", "reply_to": "m2"},
        {"id": "m2", "sender_uid": "bot_demo", "text": "b", "reply_to": None},
    ]
    case = make_case(messages=msgs, new_id="m2", addressee="channel")
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case]))


def test_fixture_validation_rejects_bad_addressee_and_missing_new_id():
    case = make_case()
    case["expected"]["addressee"] = "ghost_demo"  # not a participant
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case]))
    case2 = make_case()
    case2["state"]["new_message_id"] = "nope"
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case2]))


def test_fixture_validation_rejects_owner_not_participant_and_bad_id():
    case = make_case()
    case["state"]["owner_uid"] = "ghost"
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([case]))
    bad = make_case(case_id="Not-Snake!")
    with pytest.raises(rx.FixtureError):
        rx.validate_fixture(make_fixture([bad]))


def test_fixture_allows_unknown_parent_link_as_missing():
    msgs = [
        {"id": "m1", "sender_uid": "alice_demo", "text": "a", "reply_to": None},
        {"id": "m2", "sender_uid": "alice_demo", "text": "b", "reply_to": "gone"},
    ]
    case = make_case(messages=msgs, new_id="m2", addressee="channel")
    assert rx.validate_fixture(make_fixture([case]))[0]["id"] == "basic_thread"


# ---------------------------------------------------------------------------
# No gold leak into model payloads
# ---------------------------------------------------------------------------


def test_model_state_and_questions_contain_no_gold():
    case = make_case(category="direct_ask_zz")
    state = rx.build_model_state(case)
    questions = rx.build_questions(case)
    assert set(state) == {
        "assistant_uid",
        "owner_uid",
        "participants",
        "messages",
        "new_message_id",
    }
    blob = json.dumps({"state": state, "questions": questions})
    for leaked in (case["rationale"], case["category"], case["description"]):
        assert json.dumps(leaked) not in blob
    assert "expected" not in state


# ---------------------------------------------------------------------------
# Scoring with fakes
# ---------------------------------------------------------------------------


def test_perfect_fake_scores_exact_and_zero_brier():
    cases = [make_case(), make_case(case_id="second_thread", category="chit_chat", explicit=False)]
    provider = _provider_for(cases)
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.completed == ["basic_thread", "second_thread"]
    assert report.failed == []
    assert report.per_question["addressee"] == {"matches": 2, "denominator": 2}
    assert report.explicit_brier["brier"] == pytest.approx(0.0)
    assert report.explicit_brier["n"] == 2


def test_wrong_addressee_counted_with_denominator():
    cases = [make_case()]

    def answer_fn(state, questions):
        ans = perfect_answers(cases[0])
        ans["addressee"] = {
            "type": "choice",
            "choice": "channel",
            "probabilities": {"channel": 0.9, "unclear": 0.1},
        }
        return ans

    provider = _provider_for(cases, answer_fn=answer_fn)
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.per_question["addressee"] == {"matches": 0, "denominator": 1}


def test_brier_uses_standard_math():
    cases = [
        make_case(explicit=True),
        make_case(case_id="neg_case", explicit=False),
    ]
    probs = iter([0.7, 0.3])

    def answer_fn(state, questions):
        case = next(c for c in cases if c["state"]["new_message_id"] == state["new_message_id"])
        ans = perfect_answers(case, explicit_prob=next(probs))
        return ans

    provider = _provider_for(cases, answer_fn=answer_fn)
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.explicit_brier["brier"] == pytest.approx(((0.7 - 1) ** 2 + (0.3 - 0) ** 2) / 2)
    assert "accuracy" not in json.dumps(report.explicit_brier).lower()


def test_participation_is_recorded_but_never_scored():
    cases = [make_case(), make_case(case_id="p2")]
    provider = _provider_for(cases)
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.per_question["participation"] == {"matches": 0, "denominator": 0}
    assert report.participation_distribution["counts"] == {"respond": 2}
    assert report.participation_distribution["unscored"] is True


# ---------------------------------------------------------------------------
# Malformed provider output halts (fail closed)
# ---------------------------------------------------------------------------


def _halt_report(cases, answers, usage=None, model="jev-1.13.0"):
    async def provider(state, questions):
        fallback = {"input_tokens": 10, "output_tokens": 0}
        return rx.ProviderResult(
            answers=copy.deepcopy(answers),
            usage=dict(usage or fallback),
            model=model,
            latency_ms=1,
        )

    return asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))


def test_unknown_question_key_halts():
    cases = [make_case()]
    ans = perfect_answers(cases[0])
    ans["invented_question"] = {"type": "noul", "noul": 0.5}
    report = _halt_report(cases, ans)
    assert report.completed == [] and report.failed == ["basic_thread"]
    assert "halt" in (report.stopped_reason or "")


def test_nonfinite_noul_halts():
    cases = [make_case()]
    ans = perfect_answers(cases[0])
    ans["explicit_assistant_request"] = {"type": "noul", "noul": float("nan")}
    report = _halt_report(cases, ans)
    assert report.completed == [] and "halt" in (report.stopped_reason or "")


def test_bool_usage_halts_and_bad_model_halts():
    cases = [make_case()]
    ans = perfect_answers(cases[0])
    report = _halt_report(cases, ans, usage={"input_tokens": True, "output_tokens": 0})
    assert report.completed == []
    report2 = _halt_report(cases, ans, model="  ")
    assert report2.completed == []


# ---------------------------------------------------------------------------
# Limits, budget, selection
# ---------------------------------------------------------------------------


def test_run_requires_explicit_limits(tmp_path):
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps(make_fixture([make_case()])), encoding="utf-8")
    assert rx.main(["--cases", str(fixture), "--run"]) == 2
    assert (
        rx.main(["--cases", str(fixture), "--run", "--max-calls", "0", "--max-estimated-usd", "1"])
        == 2
    )
    assert (
        rx.main(
            ["--cases", str(fixture), "--run", "--max-calls", "2", "--max-estimated-usd", "NaN"]
        )
        == 2
    )
    assert (
        rx.main(
            ["--cases", str(fixture), "--run", "--max-calls", "two", "--max-estimated-usd", "1"]
        )
        == 2
    )


def test_max_calls_selects_first_n_deterministically():
    cases = [make_case(case_id=f"c{i}") for i in range(5)]
    provider = _provider_for(cases)
    report = asyncio.run(rx.evaluate(cases, provider, 2, Decimal("5")))
    assert report.selected == 2 and report.total_cases == 5
    assert report.selection_reason is not None and "2" in report.selection_reason
    assert report.completed == ["c0", "c1"]
    assert len(provider.inner.calls) == 2


def test_budget_stops_before_next_call_with_partial_report():
    cases = [make_case(case_id=f"b{i}") for i in range(4)]
    # 1000 input tokens * 0.042 / 1M = 0.000042 USD per call.
    provider = _provider_for(cases, usage={"input_tokens": 1000, "output_tokens": 0})
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("0.00005")))
    assert len(provider.inner.calls) == 2  # 1st + 2nd cross the limit; 3rd never starts
    assert report.attempted == ["b0", "b1"]
    assert report.completed == ["b0", "b1"]
    assert report.failed == []
    assert report.stopped_reason is not None and "limit" in report.stopped_reason
    assert Decimal(report.estimated_usd_spent) == Decimal("0.000084")


def test_full_selection_reports_no_reason():
    cases = [make_case()]
    provider = _provider_for(cases)
    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.selection_reason is None


# ---------------------------------------------------------------------------
# Errors stop without retry; secrets never logged
# ---------------------------------------------------------------------------


def test_timeout_error_stops_with_no_auto_retry(capsys):
    cases = [make_case(), make_case(case_id="t2")]
    calls = []

    async def call(state, questions):
        calls.append(state["new_message_id"])
        if len(calls) == 2:
            raise rx.ProviderError("timeout", detail="TypeSafeAPITimeoutError")
        case = cases[len(calls) - 1]
        return rx.ProviderResult(
            answers=perfect_answers(case),
            usage={"input_tokens": 5, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=1,
        )

    report = asyncio.run(rx.evaluate(cases, call, 10, Decimal("5")))
    assert len(calls) == 2  # second call raised; no third call, no repeat
    assert report.completed == ["basic_thread"]
    assert report.failed == ["t2"]
    assert "retry" in (report.stopped_reason or "").lower()
    out = capsys.readouterr().out
    assert "TYPESAFE_API_KEY" not in out


def test_offline_default_reads_no_key_and_imports_no_sdk(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-KEY-should-never-be-read")
    for mod in ("typesafe_sdk", "dotenv"):
        monkeypatch.delitem(sys.modules, mod, raising=False)
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps(make_fixture([make_case()])), encoding="utf-8")
    assert rx.main(["--cases", str(fixture)]) == 0
    out = capsys.readouterr().out
    assert "SENTINEL" not in out
    assert "typesafe_sdk" not in sys.modules
    assert "mode=offline" in out
    assert "cannot be proven" in out


def test_offline_output_makes_no_cost_claim(tmp_path, capsys):
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps(make_fixture([make_case()])), encoding="utf-8")
    assert rx.main(["--cases", str(fixture)]) == 0
    out = capsys.readouterr().out
    assert "$" not in out and "USD" not in out


# ---------------------------------------------------------------------------
# Report file safety (validated before any call)
# ---------------------------------------------------------------------------


def test_existing_report_target_rejected_before_calls(tmp_path):
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps(make_fixture([make_case()])), encoding="utf-8")
    existing = tmp_path / "report.json"
    existing.write_text("{}", encoding="utf-8")
    with pytest.raises(rx.FixtureError):
        rx.prepare_report_file(existing)


def test_symlink_report_target_rejected(tmp_path):
    real = tmp_path / "real.json"
    real.write_text("{}", encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(rx.FixtureError):
        rx.prepare_report_file(link)


def test_report_parent_must_exist(tmp_path):
    with pytest.raises(rx.FixtureError):
        rx.prepare_report_file(tmp_path / "no-such-dir" / "report.json")


def test_report_file_created_mode_0600(tmp_path):
    target = tmp_path / "fresh.json"
    rx.prepare_report_file(target)
    assert target.exists()
    assert oct(os.stat(target).st_mode & 0o777) == "0o600"


# ---------------------------------------------------------------------------
# Cancellation and error accounting
# ---------------------------------------------------------------------------


def test_cancellation_propagates():
    cases = [make_case()]

    async def scenario():
        async def provider(state, questions):
            await asyncio.sleep(10)
            raise AssertionError("unreachable")

        task = asyncio.ensure_future(rx.evaluate(cases, provider, 10, Decimal("5")))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_failed_cases_listed_not_silently_passed():
    cases = [make_case(), make_case(case_id="f2")]

    async def provider(state, questions):
        raise rx.ProviderError("auth", status=401, detail="TypeSafeAPIError")

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert report.completed == []
    assert report.failed == ["basic_thread"]
    assert report.per_question["addressee"]["denominator"] == 0


def test_credential_error_paths_do_not_format_key_or_env(monkeypatch, capsys):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delitem(sys.modules, "dotenv", raising=False)
    err = None
    try:
        rx.load_api_key(Path("/nonexistent-root-xyz"))
    except rx.ProviderError as exc:
        err = str(exc)
    assert err is not None
    assert "SUPERSECRET" not in err
    assert "sk-live" not in err
    # Pin the exact safe message: no key material, no env dump possible.
    assert err == "auth detail=no key in environment or project .env"


# ---------------------------------------------------------------------------
# Usage-first accounting: valid usage + malformed answers still bills, then halts
# ---------------------------------------------------------------------------

COST_1000 = Decimal("0.000042")  # 1000 input tokens * 0.042 / 1M


def test_valid_usage_bad_answers_charges_and_halts_no_next_call():
    cases = [make_case(), make_case(case_id="second_thread")]
    bad = perfect_answers(cases[0])
    bad["invented_question"] = {"type": "noul", "noul": 0.5}
    calls = []

    async def provider(state, questions):
        calls.append(state["new_message_id"])
        return rx.ProviderResult(
            answers=copy.deepcopy(bad),
            usage={"input_tokens": 1000, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=3,
        )

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert len(calls) == 1  # halted; second case never attempted
    assert report.attempted == ["basic_thread"]
    assert report.completed == []
    assert report.failed == ["basic_thread"]
    assert Decimal(report.estimated_usd_spent) == COST_1000
    assert report.billing_unknown is False
    assert len(report.failed_cases) == 1
    failed = report.failed_cases[0]
    assert failed["failure_kind"] == "validation"
    assert failed["usage"] == {"input_tokens": 1000, "output_tokens": 0}
    assert Decimal(failed["estimated_usd"]) == COST_1000
    assert "answers" not in failed  # raw malformed answers never retained
    assert set(failed) <= {"case_id", "failure_kind", "error", "usage", "estimated_usd", "model"}


def test_provider_error_carrying_known_usage_charges_once():
    cases = [make_case(), make_case(case_id="c2")]
    usage = {"input_tokens": 2000, "output_tokens": 10}
    calls = []

    async def provider(state, questions):
        calls.append(1)
        raise rx.ProviderError(
            "validation",
            detail="UnreadableChunk",
            usage=dict(usage),
            model="jev-1.13.0",
            unknown_billing=False,
        )

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert len(calls) == 1
    assert report.failed == ["basic_thread"]
    assert Decimal(report.estimated_usd_spent) == Decimal("0.000084")
    assert report.billing_unknown is False
    assert report.failed_cases[0]["failure_kind"] == "provider_error"
    assert report.failed_cases[0]["usage"] == usage
    assert report.failed_cases[0]["model"] == "jev-1.13.0"


@pytest.mark.parametrize("model, retained", [("jev-9.9.9", "jev-9.9.9"), ("  ", None)])
def test_wrong_model_billed_then_halts(model, retained):
    cases = [make_case(), make_case(case_id="c2")]
    calls = []

    async def provider(state, questions):
        calls.append(1)
        return rx.ProviderResult(
            answers=perfect_answers(cases[0]),
            usage={"input_tokens": 1000, "output_tokens": 0},
            model=model,
            latency_ms=2,
        )

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert len(calls) == 1
    assert report.completed == [] and report.failed == ["basic_thread"]
    assert Decimal(report.estimated_usd_spent) == COST_1000
    assert report.billing_unknown is False
    assert report.failed_cases[0]["usage"] == {"input_tokens": 1000, "output_tokens": 0}
    assert report.failed_cases[0].get("model") == retained


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": True, "output_tokens": 0},
        {"input_tokens": -5, "output_tokens": 0},
        {"input_tokens": 10},
        "not-an-object",
    ],
)
def test_bad_usage_flags_billing_unknown_no_charge(usage):
    cases = [make_case(), make_case(case_id="c2")]
    calls = []

    async def provider(state, questions):
        calls.append(1)
        return rx.ProviderResult(
            answers=perfect_answers(cases[0]),
            usage=usage,
            model="jev-1.13.0",
            latency_ms=1,
        )

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5")))
    assert len(calls) == 1
    assert report.completed == [] and report.failed == ["basic_thread"]
    assert report.estimated_usd_spent == "0"
    assert report.billing_unknown is True
    assert "unknown" in (report.billing_note or "").lower()
    assert "usage" not in report.failed_cases[0]


# ---------------------------------------------------------------------------
# Exact pinned SDK interface (fake typesafe_sdk module; no network)
# ---------------------------------------------------------------------------


class _FakeSDK:
    """Record exact ctor/question/call args of the pinned typesafe-sdk 0.7.2 shape."""

    def __init__(
        self, *, response_fn=None, omit=(), retry_init=None, client_init=None, exit_error=None
    ):
        self.client_kwargs = None
        self.retry_kwargs = None
        self.choice_calls = []
        self.noul_calls = []
        self.system_one_calls = []
        self.response_fn = response_fn
        outer = self

        class RetryPolicy:
            def __init__(self, *args, **kwargs):
                outer.retry_kwargs = {"args": args, "kwargs": dict(kwargs)}
                if retry_init is not None:
                    retry_init(*args, **kwargs)
                elif args or kwargs != {"max_retries": 0}:
                    raise TypeError("RetryPolicy must be (max_retries=0)")

        class Choice:
            def __init__(self, *args, **kwargs):
                outer.choice_calls.append({"args": args, "kwargs": dict(kwargs)})
                self.instructions = kwargs.get("instructions")
                self.criteria = kwargs.get("criteria")

        class Noul:
            def __init__(self, *args, **kwargs):
                outer.noul_calls.append({"args": args, "kwargs": dict(kwargs)})
                if "criteria" in kwargs:
                    raise TypeError("Noul takes no criteria")
                self.instructions = kwargs.get("instructions")

        module = types.ModuleType("typesafe_sdk")
        module.RetryPolicy = RetryPolicy
        module.Choice = Choice
        module.Noul = Noul

        class _Live:
            async def system_one(self, *args, **kwargs):
                outer.system_one_calls.append({"args": args, "kwargs": dict(kwargs)})
                assert outer.response_fn is not None
                return outer.response_fn()

        class AsyncTypeSafeClient:
            def __init__(self, *args, **kwargs):
                if args:
                    raise TypeError("kwargs only")
                outer.client_kwargs = dict(kwargs)
                if client_init is not None:
                    client_init(kwargs)

            async def __aenter__(self):
                return _Live()

            async def __aexit__(self, *exc):
                if exit_error is not None:
                    raise exit_error
                return False

            async def aclose(self):
                pass

        module.AsyncTypeSafeClient = AsyncTypeSafeClient
        for name in omit:
            delattr(module, name)
        self.module = module

    def install(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "typesafe_sdk", self.module)


def _sdk_response(*, case, usage=None, model="jev-1.13.0", answers=None):
    return types.SimpleNamespace(
        answers=answers if answers is not None else perfect_answers(case),
        usage=dict(usage or {"input_tokens": 1000, "output_tokens": 5}),
        model=model,
    )


def test_sdk_exact_ctor_kwargs_and_questions(monkeypatch):
    case = make_case()
    questions = rx.build_questions(case)
    state = rx.build_model_state(case)
    fake = _FakeSDK(response_fn=lambda: _sdk_response(case=case))
    fake.install(monkeypatch)
    provider = rx.make_sdk_provider("FAKE-KEY-123")
    result = asyncio.run(provider(state, questions))
    assert fake.client_kwargs is not None
    assert set(fake.client_kwargs) == {"api_key", "base_url", "model", "retry", "timeout"}
    assert fake.client_kwargs["api_key"] == "FAKE-KEY-123"
    assert fake.client_kwargs["base_url"] == "https://api.typesafe.ai"
    assert fake.client_kwargs["model"] == "jev-1.13.0"
    assert fake.client_kwargs["timeout"] == 10.0 == rx.SDK_TIMEOUT
    assert isinstance(fake.client_kwargs["retry"], fake.module.RetryPolicy)
    assert fake.retry_kwargs == {"args": (), "kwargs": {"max_retries": 0}}
    assert len(fake.system_one_calls) == 1
    call_kwargs = fake.system_one_calls[0]["kwargs"]
    assert set(call_kwargs) == {"state", "questions"}  # no per-call model override
    assert call_kwargs["state"] == state
    assert len(fake.noul_calls) == 1
    assert set(fake.noul_calls[0]["kwargs"]) == {"instructions"}  # no None criteria
    assert len(fake.choice_calls) == 2
    assert all(set(c["kwargs"]) == {"instructions", "criteria"} for c in fake.choice_calls)
    assert any("channel" in c["kwargs"]["criteria"] for c in fake.choice_calls)
    assert result.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert result.model == "jev-1.13.0"


def test_sdk_missing_retry_failclosed_no_system_one(monkeypatch):
    fake = _FakeSDK(response_fn=lambda: None, omit=("RetryPolicy",))
    fake.install(monkeypatch)
    with pytest.raises(rx.ProviderError):
        rx.make_sdk_provider("FAKE-KEY")
    assert fake.system_one_calls == []


def test_sdk_invalid_retry_failclosed_no_fallback(monkeypatch):
    def _boom(*args, **kwargs):
        raise TypeError("old signature without max_retries")

    fake = _FakeSDK(response_fn=lambda: None, retry_init=_boom)
    fake.install(monkeypatch)
    with pytest.raises(rx.ProviderError) as excinfo:
        rx.make_sdk_provider("FAKE-KEY")
    assert excinfo.value.unknown_billing is False
    assert fake.system_one_calls == []


def test_sdk_unsupported_client_kwargs_failclosed(monkeypatch):
    case = make_case()

    def _old_client(kwargs):
        if "model" in kwargs:
            raise TypeError("unexpected keyword 'model'")

    fake = _FakeSDK(response_fn=lambda: _sdk_response(case=case), client_init=_old_client)
    fake.install(monkeypatch)
    provider = rx.make_sdk_provider("FAKE-KEY")
    with pytest.raises(rx.ProviderError) as excinfo:
        asyncio.run(provider(rx.build_model_state(case), rx.build_questions(case)))
    assert excinfo.value.unknown_billing is False  # preflight: no request sent
    assert fake.system_one_calls == []


def test_sdk_normalization_failure_carries_known_usage(monkeypatch):
    case = make_case()
    weird = types.SimpleNamespace(type="mystery", payload="CANARY-RAW-BODY-123")

    def _resp():
        return types.SimpleNamespace(
            answers={"sender_uid": weird},
            usage={"input_tokens": 500, "output_tokens": 1},
            model="jev-1.13.0",
        )

    fake = _FakeSDK(response_fn=_resp)
    fake.install(monkeypatch)
    provider = rx.make_sdk_provider("FAKE-KEY")
    with pytest.raises(rx.ProviderError) as excinfo:
        asyncio.run(provider(rx.build_model_state(case), rx.build_questions(case)))
    err = excinfo.value
    assert err.usage == {"input_tokens": 500, "output_tokens": 1}
    assert err.unknown_billing is False
    assert "CANARY" not in str(err) and "CANARY" not in err.detail

    async def boom(state, questions):
        raise err

    report = asyncio.run(rx.evaluate([case], boom, 10, Decimal("5")))
    assert report.failed == [case["id"]]
    assert Decimal(report.estimated_usd_spent) == rx.estimate_usd(
        {"input_tokens": 500, "output_tokens": 1}
    )


# ---------------------------------------------------------------------------
# Answer validation: prob-key subset, confidence, bounded choices
# ---------------------------------------------------------------------------


def test_unknown_probability_key_rejected_bounded():
    case = make_case()
    questions = rx.build_questions(case)
    ans = perfect_answers(case)
    ans["addressee"]["probabilities"]["CANARY-JUNK-KEY"] = 0.5
    with pytest.raises(rx.AnswerError) as excinfo:
        rx.validate_answers(questions, ans)
    assert "CANARY" not in str(excinfo.value)
    # Sparse subsets stay valid (no sum/normalization rule is invented):
    sparse = perfect_answers(case)
    sparse["addressee"]["probabilities"] = {"channel": 0.9}
    out = rx.validate_answers(questions, sparse)
    assert out["addressee"]["probabilities"] == {"channel": 0.9}


def test_choice_confidence_preserved_and_range_checked():
    case = make_case()
    questions = rx.build_questions(case)
    ans = perfect_answers(case)
    ans["addressee"]["confidence"] = 0.8
    out = rx.validate_answers(questions, ans)
    assert out["addressee"]["confidence"] == pytest.approx(0.8)
    assert "confidence" not in out["explicit_assistant_request"]  # never for Noul
    bad = perfect_answers(case)
    bad["addressee"]["confidence"] = 1.5
    with pytest.raises(rx.AnswerError):
        rx.validate_answers(questions, bad)
    nan = perfect_answers(case)
    nan["addressee"]["confidence"] = float("nan")
    with pytest.raises(rx.AnswerError):
        rx.validate_answers(questions, nan)

    async def provider(state, questions):
        return rx.ProviderResult(
            answers=copy.deepcopy(ans),
            usage={"input_tokens": 100, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=1,
        )

    report = asyncio.run(rx.evaluate([case], provider, 10, Decimal("5")))
    assert report.completed == [case["id"]]
    assert report.cases[0]["answers"]["addressee"]["confidence"] == pytest.approx(0.8)


def test_unhashable_choice_bounded_no_crash():
    case = make_case()
    questions = rx.build_questions(case)
    ans = perfect_answers(case)
    ans["addressee"]["choice"] = ["bot_demo"]  # unhashable, nonsensical
    with pytest.raises(rx.AnswerError) as excinfo:
        rx.validate_answers(questions, ans)
    assert "list" in str(excinfo.value)  # type name, not a raw-repr crash


def test_participation_assistant_perspective_no_gold_or_threshold():
    case = make_case()
    questions = rx.build_questions(case)
    instructions = questions["participation"]["instructions"]
    assert "bot_demo" in instructions  # explicit assistant_uid perspective
    assert "assistant" in instructions.lower()
    assert "UNSCORED" not in instructions
    assert "almost always" in instructions
    assert "helpful conversational participant" not in instructions
    blob = json.dumps({"state": rx.build_model_state(case), "questions": questions})
    for leaked in (case["rationale"], case["category"], case["description"]):
        assert json.dumps(leaked) not in blob
    assert "threshold" not in instructions.lower()


# ---------------------------------------------------------------------------
# Durable private incremental persistence
# ---------------------------------------------------------------------------


def test_progress_persisted_before_call_and_after_each_case():
    cases = [make_case(case_id="d0"), make_case(case_id="d1")]
    snapshots = []
    seen_at_call = []
    base = _provider_for(cases, usage={"input_tokens": 1000, "output_tokens": 0})

    async def provider(state, questions):
        seen_at_call.append(len(snapshots))
        return await base(state, questions)

    def on_progress(report):
        snapshots.append(copy.deepcopy(report.to_dict()))

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=on_progress))
    assert report.completed == ["d0", "d1"]
    assert len(snapshots) == 6  # initial + (pending, after) x2 + final
    assert snapshots[0]["attempted"] == [] and snapshots[0]["pending_case"] is None
    assert snapshots[1]["pending_case"] == "d0" and snapshots[1]["billing_unknown"] is True
    assert snapshots[2]["completed"] == ["d0"] and snapshots[2]["pending_case"] is None
    assert snapshots[2]["billing_unknown"] is False
    assert snapshots[3]["pending_case"] == "d1"
    assert snapshots[4]["completed"] == ["d0", "d1"]
    assert snapshots[5]["per_question"]  # final scored emit
    assert seen_at_call == [2, 4]  # each call observed its durable pending emit first


@pytest.mark.parametrize("fail_at, expected_calls", [(1, 0), (3, 1)])
def test_persistence_failure_halts_no_further_calls(fail_at, expected_calls):
    cases = [make_case(case_id="p0"), make_case(case_id="p1")]
    provider = _provider_for(cases)
    emits = {"n": 0}

    def on_progress(report):
        if emits["n"] == fail_at:
            raise OSError("injected persistence failure")
        emits["n"] += 1

    with pytest.raises(OSError):
        asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=on_progress))
    assert len(provider.inner.calls) == expected_calls


def test_interrupt_second_request_keeps_first_and_pending_unknown(tmp_path):
    cases = [make_case(case_id="i0"), make_case(case_id="i1")]
    by_new = {c["state"]["new_message_id"]: c for c in cases}
    calls = []

    async def provider(state, questions):
        calls.append(state["new_message_id"])
        if len(calls) == 2:
            raise KeyboardInterrupt()
        case = by_new[state["new_message_id"]]
        return rx.ProviderResult(
            answers=perfect_answers(case),
            usage={"input_tokens": 1000, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=2,
        )

    target = tmp_path / "interrupt.json"
    writer = rx.prepare_report_writer(target)
    try:
        with pytest.raises(KeyboardInterrupt):
            asyncio.run(
                rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=writer.write_report)
            )
    finally:
        writer.close()
    persisted = json.loads(target.read_text(encoding="utf-8"))
    assert persisted["attempted"] == ["i0", "i1"]
    assert persisted["completed"] == ["i0"]
    assert persisted["pending_case"] == "i1"
    assert persisted["billing_unknown"] is True
    assert Decimal(persisted["estimated_usd_spent"]) == COST_1000


def test_swapped_report_target_refuses_write_and_chmod(tmp_path):
    target = tmp_path / "report.json"
    writer = rx.prepare_report_writer(target)
    first = rx.EvalReport(selected=1, total_cases=1, selection_reason=None)
    first.attempted.append("a0")
    writer.write_report(first)
    moved = tmp_path / "moved.json"
    os.rename(target, moved)
    target.write_text("VICTIM", encoding="utf-8")
    os.chmod(target, 0o644)
    second = rx.EvalReport(selected=1, total_cases=1, selection_reason=None)
    with pytest.raises(rx.FixtureError):
        writer.write_report(second)
    try:
        assert target.read_text(encoding="utf-8") == "VICTIM"
        assert oct(os.stat(target).st_mode & 0o777) == "0o644"
        kept = json.loads(moved.read_text(encoding="utf-8"))
        assert kept["attempted"] == ["a0"]
    finally:
        writer.close()
    # Symlink-swap variant: the link target must stay untouched too.
    target2 = tmp_path / "report2.json"
    writer2 = rx.prepare_report_writer(target2)
    writer2.write_report(first)
    os.rename(target2, tmp_path / "moved2.json")
    (tmp_path / "evil.json").write_text("EVIL", encoding="utf-8")
    target2.symlink_to(tmp_path / "evil.json")
    with pytest.raises(rx.FixtureError):
        writer2.write_report(second)
    try:
        assert (tmp_path / "evil.json").read_text(encoding="utf-8") == "EVIL"
    finally:
        writer2.close()


def test_default_writer_owned_dir_0700_leaf_0600_exclusive():
    writer = rx.default_report_writer()
    try:
        assert oct(os.stat(writer.path.parent).st_mode & 0o777) == "0o700"
        assert oct(os.stat(writer.path).st_mode & 0o777) == "0o600"
        report = rx.EvalReport(selected=0, total_cases=0, selection_reason=None)
        writer.write_report(report)
        assert json.loads(writer.path.read_text(encoding="utf-8"))["model_pin"] == rx.PINNED_MODEL
    finally:
        parent = writer.path.parent
        writer.close()
        shutil.rmtree(parent, ignore_errors=True)


def test_report_collision_before_key_read(tmp_path, monkeypatch, capsys):
    fixture = tmp_path / "cases.json"
    fixture.write_text(json.dumps(make_fixture([make_case()])), encoding="utf-8")
    existing = tmp_path / "report.json"
    existing.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-KEY-collision-before-read")
    rc = rx.main(
        [
            "--cases",
            str(fixture),
            "--run",
            "--max-calls",
            "1",
            "--max-estimated-usd",
            "1",
            "--report",
            str(existing),
        ]
    )
    assert rc == 2
    captured = capsys.readouterr()
    assert "SENTINEL" not in captured.out and "SENTINEL" not in captured.err


# ---------------------------------------------------------------------------
# Secrets, logging, CLI exit honesty (fake providers only, never a real key)
# ---------------------------------------------------------------------------


def test_sdk_logging_forced_warning_despite_env(monkeypatch):
    fake = _FakeSDK(response_fn=lambda: None)
    fake.install(monkeypatch)
    monkeypatch.setenv("TYPESAFE_LOG_LEVEL", "DEBUG")
    loggers = [logging.getLogger("typesafe"), logging.getLogger("typesafe_sdk")]
    saved = [lg.level for lg in loggers]
    try:
        for lg in loggers:
            lg.setLevel(logging.DEBUG)
        rx.make_sdk_provider("FAKE-KEY-no-read")
        assert all(lg.level == logging.WARNING for lg in loggers)
    finally:
        for lg, level in zip(loggers, saved, strict=True):
            lg.setLevel(level)


def test_error_bounds_no_secret_canary():
    class Boom(Exception):
        pass

    exc = Boom("sk-live-CANARY-123 body=TOP-SECRET-BODY")
    exc.status_code = 500
    err = rx._classify_sdk_error(exc)
    assert "CANARY" not in str(err) and "TOP-SECRET" not in str(err)
    assert err.status == 500

    async def provider(state, questions):
        raise ValueError("CANARY-IN-UNEXPECTED-MESSAGE")

    report = asyncio.run(rx.evaluate([make_case()], provider, 10, Decimal("5")))
    assert report.failed_cases[0]["error"] == "ValueError"
    assert "CANARY" not in (report.stopped_reason or "")
    assert report.billing_unknown is True


def _write_fixture(tmp_path, name, cases):
    path = tmp_path / name
    path.write_text(json.dumps(make_fixture(cases)), encoding="utf-8")
    return path


def test_cli_budget_stop_zero_vs_failure_nonzero(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rx, "load_api_key", lambda root: "FAKE-KEY")
    cases = [make_case(case_id=f"g{i}") for i in range(3)]
    fixture = _write_fixture(tmp_path, "cases.json", cases)
    budget_provider = _provider_for(cases, usage={"input_tokens": 1000, "output_tokens": 0})
    monkeypatch.setattr(rx, "make_sdk_provider", lambda key: budget_provider)
    report_path = tmp_path / "budget.json"
    rc = rx.main(
        [
            "--cases",
            str(fixture),
            "--run",
            "--max-calls",
            "10",
            "--max-estimated-usd",
            "0.00005",
            "--report",
            str(report_path),
        ]
    )
    assert rc == 0
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["failed"] == [] and data["completed"] == ["g0", "g1"]
    assert "limit" in (data["stopped_reason"] or "")
    # Schema failure is billed and exits nonzero with an honest failed list:
    bad_cases = [make_case(case_id="s0"), make_case(case_id="s1")]
    bad_fixture = _write_fixture(tmp_path, "bad.json", bad_cases)
    bad_answers = perfect_answers(bad_cases[0])
    bad_answers["addressee"]["choice"] = "not-a-uid"

    async def bad_provider(state, questions):
        return rx.ProviderResult(
            answers=copy.deepcopy(bad_answers),
            usage={"input_tokens": 1000, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=1,
        )

    monkeypatch.setattr(rx, "make_sdk_provider", lambda key: bad_provider)
    bad_report = tmp_path / "bad-report.json"
    capsys.readouterr()
    rc = rx.main(
        [
            "--cases",
            str(bad_fixture),
            "--run",
            "--max-calls",
            "10",
            "--max-estimated-usd",
            "5",
            "--report",
            str(bad_report),
        ]
    )
    assert rc == 1
    data = json.loads(bad_report.read_text(encoding="utf-8"))
    assert data["completed"] == [] and data["failed"] == ["s0"]
    assert Decimal(data["estimated_usd_spent"]) == COST_1000
    assert "failed=1" in capsys.readouterr().out

    # Provider outage exits nonzero with billing unknown:
    async def down_provider(state, questions):
        raise rx.ProviderError("timeout", detail="TypeSafeAPITimeoutError")

    monkeypatch.setattr(rx, "make_sdk_provider", lambda key: down_provider)
    down_report = tmp_path / "down-report.json"
    rc = rx.main(
        [
            "--cases",
            str(fixture),
            "--run",
            "--max-calls",
            "3",
            "--max-estimated-usd",
            "5",
            "--report",
            str(down_report),
        ]
    )
    assert rc == 1
    data = json.loads(down_report.read_text(encoding="utf-8"))
    assert data["billing_unknown"] is True and data["failed"] == ["g0"]


def test_cli_selection_reports_unrun_cases(tmp_path, monkeypatch):
    monkeypatch.setattr(rx, "load_api_key", lambda root: "FAKE-KEY")
    cases = [make_case(case_id=f"u{i}") for i in range(3)]
    fixture = _write_fixture(tmp_path, "sel.json", cases)
    provider = _provider_for(cases)
    monkeypatch.setattr(rx, "make_sdk_provider", lambda key: provider)
    report_path = tmp_path / "sel-report.json"
    rc = rx.main(
        [
            "--cases",
            str(fixture),
            "--run",
            "--max-calls",
            "1",
            "--max-estimated-usd",
            "5",
            "--report",
            str(report_path),
        ]
    )
    assert rc == 0
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["selected"] == 1 and data["total_cases"] == 3
    assert "1" in (data["selection_reason"] or "")
    assert "3" in (data["selection_reason"] or "")
    assert data["completed"] == ["u0"]


def test_empty_paid_run_rejected_before_key(tmp_path, monkeypatch, capsys):
    fixture = _write_fixture(tmp_path, "empty.json", [])
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-EMPTY-NEVER-READ")
    called = []
    monkeypatch.setattr(rx, "make_sdk_provider", lambda key: called.append(key))
    rc = rx.main(["--cases", str(fixture), "--run", "--max-calls", "1", "--max-estimated-usd", "1"])
    assert rc == 2
    assert called == []
    captured = capsys.readouterr()
    assert "SENTINEL" not in captured.out and "SENTINEL" not in captured.err


# ---------------------------------------------------------------------------
# Follow-up: full completed results in every persisted snapshot; close-failure
# usage recovery (fake providers/SDK only)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc_class", [KeyboardInterrupt, asyncio.CancelledError])
def test_interrupt_keeps_full_completed_results_and_metrics(tmp_path, exc_class):
    cases = [make_case(case_id="k0"), make_case(case_id="k1")]
    by_new = {c["state"]["new_message_id"]: c for c in cases}
    calls = []

    async def provider(state, questions):
        calls.append(state["new_message_id"])
        if len(calls) == 2:
            raise exc_class()
        case = by_new[state["new_message_id"]]
        return rx.ProviderResult(
            answers=perfect_answers(case),
            usage={"input_tokens": 1000, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=9,
        )

    target = tmp_path / "interrupt-full.json"
    writer = rx.prepare_report_writer(target)
    try:
        with pytest.raises(exc_class):
            asyncio.run(
                rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=writer.write_report)
            )
    finally:
        writer.close()
    persisted = json.loads(target.read_text(encoding="utf-8"))
    assert persisted["attempted"] == ["k0", "k1"]
    assert persisted["completed"] == ["k0"]
    assert persisted["pending_case"] == "k1"
    assert persisted["billing_unknown"] is True
    assert Decimal(persisted["estimated_usd_spent"]) == COST_1000
    # Full paid result for k0, not just its id:
    assert len(persisted["cases"]) == 1
    saved = persisted["cases"][0]
    assert saved["case_id"] == "k0"
    assert saved["model"] == "jev-1.13.0"
    assert saved["latency_ms"] == 9
    assert saved["usage"] == {"input_tokens": 1000, "output_tokens": 0}
    assert Decimal(saved["estimated_usd"]) == COST_1000
    assert set(saved["answers"]) == set(rx.QUESTION_IDS)
    assert saved["answers"]["addressee"]["choice"] == "bot_demo"
    # Metrics already cover the one valid completed case:
    assert persisted["per_question"]["addressee"] == {"matches": 1, "denominator": 1}
    assert persisted["explicit_brier"]["n"] == 1
    assert persisted["explicit_brier"]["brier"] == pytest.approx(0.0)
    assert persisted["by_category"]["request"]["completed"] == 1
    assert persisted["participation_distribution"]["counts"] == {"respond": 1}


def test_every_emit_recomputes_without_duplicates():
    cases = [make_case(case_id=f"e{i}") for i in range(3)]
    snapshots = []
    provider = _provider_for(cases, usage={"input_tokens": 100, "output_tokens": 0})

    def on_progress(report):
        snapshots.append(copy.deepcopy(report.to_dict()))

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=on_progress))
    assert report.completed == ["e0", "e1", "e2"]
    assert len(snapshots) == 8  # initial + (pending, after) x3 + final
    for snap in snapshots:
        n = len(snap["completed"])
        assert len(snap["cases"]) == n
        assert snap["per_question"]["addressee"]["denominator"] == n
        assert snap["explicit_brier"]["n"] == n
        assert sum(c["completed"] for c in snap["by_category"].values()) == n
        assert sum(snap["participation_distribution"]["counts"].values()) == n
        assert snap["participation_distribution"]["unscored"] is True
    assert len(report.cases) == 3  # rebuilt, never double-appended


def test_first_success_preserved_when_second_case_fails():
    cases = [make_case(case_id="f0"), make_case(case_id="f1")]
    bad = perfect_answers(cases[1])
    bad["addressee"]["choice"] = "not-a-uid"
    by_new = {c["state"]["new_message_id"]: c for c in cases}
    snapshots = []

    async def provider(state, questions):
        case = by_new[state["new_message_id"]]
        answers = copy.deepcopy(bad) if case["id"] == "f1" else perfect_answers(case)
        return rx.ProviderResult(
            answers=answers,
            usage={"input_tokens": 1000, "output_tokens": 0},
            model="jev-1.13.0",
            latency_ms=4,
        )

    def on_progress(report):
        snapshots.append(copy.deepcopy(report.to_dict()))

    report = asyncio.run(rx.evaluate(cases, provider, 10, Decimal("5"), on_progress=on_progress))
    assert report.completed == ["f0"] and report.failed == ["f1"]
    assert len(report.cases) == 1 and report.cases[0]["case_id"] == "f0"
    assert set(report.cases[0]["answers"]) == set(rx.QUESTION_IDS)
    assert report.cases[0]["usage"] == {"input_tokens": 1000, "output_tokens": 0}
    assert report.failed_cases[0]["failure_kind"] == "validation"
    assert report.failed_cases[0]["usage"] == {"input_tokens": 1000, "output_tokens": 0}
    assert Decimal(report.estimated_usd_spent) == COST_1000 * 2  # success + failed billed once
    # Failed case never counts toward valid-completed metrics:
    assert report.per_question["addressee"] == {"matches": 1, "denominator": 1}
    assert report.explicit_brier["n"] == 1
    # The post-failure persisted snapshot already held the full first result:
    assert snapshots[-2]["failed"] == ["f1"]
    assert len(snapshots[-2]["cases"]) == 1
    assert snapshots[-2]["cases"][0]["case_id"] == "f0"


def test_sdk_close_failure_after_response_keeps_known_usage(monkeypatch):
    case = make_case()

    class CanaryCloseError(Exception):
        pass

    fake = _FakeSDK(
        response_fn=lambda: _sdk_response(
            case=case, usage={"input_tokens": 1000, "output_tokens": 1}
        ),
        exit_error=CanaryCloseError("SECRET-CLOSE-BODY-sk-live-999"),
    )
    fake.install(monkeypatch)
    provider = rx.make_sdk_provider("FAKE-KEY")
    with pytest.raises(rx.ProviderError) as excinfo:
        asyncio.run(provider(rx.build_model_state(case), rx.build_questions(case)))
    err = excinfo.value
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.model == "jev-1.13.0"
    assert err.unknown_billing is False
    assert "SECRET" not in str(err) and "SECRET" not in err.detail
    assert len(fake.system_one_calls) == 1  # exactly one query, no retry/recreate

    calls = []

    async def boom(state, questions):
        calls.append(1)
        raise err

    report = asyncio.run(rx.evaluate([case, make_case(case_id="z2")], boom, 10, Decimal("5")))
    assert len(calls) == 1  # stopped; next case never called
    assert report.completed == [] and report.failed == [case["id"]]
    assert Decimal(report.estimated_usd_spent) == COST_1000
    assert report.billing_unknown is False
    assert report.failed_cases[0]["usage"] == {"input_tokens": 1000, "output_tokens": 1}


@pytest.mark.parametrize("kind", ["bad_usage", "no_response"])
def test_sdk_close_failure_unknown_controls(monkeypatch, kind):
    case = make_case()

    class CanaryCloseError(Exception):
        pass

    if kind == "bad_usage":
        fake = _FakeSDK(
            response_fn=lambda: _sdk_response(
                case=case, usage={"input_tokens": True, "output_tokens": 0}
            ),
            exit_error=CanaryCloseError("SECRET-BAD-USAGE"),
        )
    else:

        def _no_resp():
            raise ValueError("CANARY-NO-RESPONSE-BODY")

        fake = _FakeSDK(response_fn=_no_resp)
    fake.install(monkeypatch)
    provider = rx.make_sdk_provider("FAKE-KEY")
    with pytest.raises(rx.ProviderError) as excinfo:
        asyncio.run(provider(rx.build_model_state(case), rx.build_questions(case)))
    err = excinfo.value
    assert err.usage is None
    assert err.unknown_billing is True
    assert "SECRET" not in str(err) and "CANARY" not in str(err)
