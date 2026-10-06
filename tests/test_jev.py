"""Offline tests for the minimal Jev adapter (jev).

Fake/offline only: synthetic states, the 30 benchmark fixture states (state
fields only, never gold in requests), and an injected fake typesafe_sdk
module. No network, no real keys, no paid calls.
"""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import io
import json
import logging
import sys
import types
from pathlib import Path

import pytest

from conversation import ConversationError, project_conversation_state
from jev import (
    JEV_BASE_URL,
    JEV_MODEL,
    JEV_TIMEOUT,
    QUESTION_IDS,
    JevAdapter,
    JevDataError,
    JevError,
    JevProviderResult,
    build_questions,
    make_sdk_provider,
    validate_answers,
    validate_model_id,
    validate_usage,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent / "experiments" / "jev_conversation" / "cases.json"
)
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


def _fixture_cases():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["cases"]


def make_state(tag="", **overrides):
    state = {
        "assistant_uid": "bot_demo",
        "owner_uid": "alice_demo",
        "participants": [
            {"uid": "alice_demo", "display_name": "Alice"},
            {"uid": "bot_demo", "display_name": "HelperBot"},
            {"uid": "carol_demo", "display_name": "Carol"},
        ],
        "messages": [
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
                "text": "HelperBot, please summarize",
                "reply_to": f"{tag}m2",
            },
        ],
        "new_message_id": f"{tag}m3",
    }
    state.update(overrides)
    return state


def perfect_answers(state=None, *, participation="respond", explicit_prob=1.0):
    state = state or make_state()
    return {
        "addressee": {
            "type": "choice",
            "choice": "bot_demo",
            "probabilities": {"bot_demo": 0.8, "channel": 0.1, "unclear": 0.1},
        },
        "explicit_assistant_request": {"type": "noul", "noul": explicit_prob},
        "participation": {
            "type": "choice",
            "choice": participation,
            "probabilities": {"respond": 0.8, "observe": 0.2},
        },
    }


def make_result(state=None, **overrides):
    result = JevProviderResult(
        answers=perfect_answers(state),
        usage={"input_tokens": 1000, "output_tokens": 5},
        model=JEV_MODEL,
        latency_ms=7,
    )
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


# ---------------------------------------------------------------------------
# Question constants: benchmark-verified, assistant perspective, no gold
# ---------------------------------------------------------------------------


def test_question_ids_exact():
    assert QUESTION_IDS == (
        "addressee",
        "explicit_assistant_request",
        "participation",
    )
    assert JEV_MODEL == "jev-1.13.0"
    assert JEV_BASE_URL == "https://api.typesafe.ai"
    assert JEV_TIMEOUT == 10.0


def test_build_questions_matches_runner_all_thirty():
    rx = _load_runner()
    for case in _fixture_cases():
        assert build_questions(case["state"]) == rx.build_questions(case)


def test_question_shapes_and_vocab():
    questions = build_questions(make_state())
    assert set(questions) == set(QUESTION_IDS)
    assert questions["addressee"]["criteria"] == {
        "alice_demo": "Alice",
        "bot_demo": "HelperBot",
        "carol_demo": "Carol",
        "channel": "the message addresses the group generally",
        "unclear": "not enough context to determine the addressee",
    }
    assert set(questions["participation"]["criteria"]) == {"respond", "observe"}
    assert set(questions["explicit_assistant_request"]) == {"type", "instructions"}
    assert questions["explicit_assistant_request"]["type"] == "noul"
    instructions = questions["participation"]["instructions"]
    assert "bot_demo" in instructions
    assert "assistant" in instructions.lower()
    assert "UNSCORED" not in instructions
    assert "almost always" in instructions
    assert "threshold" not in instructions.lower()


def test_questions_and_state_contain_no_gold_all_thirty():
    for case in _fixture_cases():
        blob = json.dumps(
            {
                "state": project_conversation_state(case["state"]),
                "questions": build_questions(case["state"]),
            }
        )
        for leaked in (
            case["rationale"],
            case["category"],
            case["description"],
            case["participation_notes"],
            json.dumps(case["expected"]),
        ):
            assert json.dumps(leaked) not in blob, case["id"]


# ---------------------------------------------------------------------------
# Answer / usage / model validation
# ---------------------------------------------------------------------------


def test_validate_answers_accepts_perfect_and_preserves_confidence():
    questions = build_questions(make_state())
    answers = perfect_answers()
    answers["addressee"]["confidence"] = 0.8
    out = validate_answers(questions, answers)
    assert out["addressee"]["confidence"] == pytest.approx(0.8)
    assert out["addressee"]["probabilities"]["bot_demo"] == pytest.approx(0.8)
    assert "confidence" not in out["explicit_assistant_request"]
    sparse = perfect_answers()
    sparse["addressee"]["probabilities"] = {"channel": 0.9}
    assert validate_answers(questions, sparse)["addressee"]["probabilities"] == {"channel": 0.9}


def test_unknown_probability_key_rejected_bounded():
    questions = build_questions(make_state())
    answers = perfect_answers()
    answers["addressee"]["probabilities"]["CANARY-JUNK-KEY"] = 0.5
    with pytest.raises(JevDataError) as excinfo:
        validate_answers(questions, answers)
    assert "CANARY" not in str(excinfo.value)


def test_bad_confidence_and_noul_rejected():
    questions = build_questions(make_state())
    for bad_conf in (1.5, -0.1, float("nan"), float("inf"), True, "high"):
        bad = perfect_answers()
        bad["addressee"]["confidence"] = bad_conf
        with pytest.raises(JevDataError):
            validate_answers(questions, bad)
    for bad_noul in (float("nan"), float("inf"), 1.5, -0.1, True, "yes", None):
        bad = perfect_answers()
        bad["explicit_assistant_request"] = {"type": "noul", "noul": bad_noul}
        with pytest.raises(JevDataError):
            validate_answers(questions, bad)


def test_answer_key_and_choice_bounds():
    # Security wording: generic mismatch/option messages never echo untrusted
    # keys or choices. Classification (JevDataError) and failure preserved.
    questions = build_questions(make_state())
    missing = perfect_answers()
    del missing["participation"]
    with pytest.raises(JevDataError, match="answer keys mismatch"):
        validate_answers(questions, missing)
    extra = perfect_answers()
    extra["invented"] = {"type": "noul", "noul": 0.5}
    with pytest.raises(JevDataError, match="answer keys mismatch"):
        validate_answers(questions, extra)
    wrong_choice = perfect_answers()
    wrong_choice["addressee"]["choice"] = "ghost_demo"
    with pytest.raises(JevDataError, match="not an allowed option"):
        validate_answers(questions, wrong_choice)
    unhashable = perfect_answers()
    unhashable["addressee"]["choice"] = ["bot_demo"]
    with pytest.raises(JevDataError) as excinfo:
        validate_answers(questions, unhashable)
    assert "list" in str(excinfo.value)
    mistyped = perfect_answers()
    mistyped["addressee"]["type"] = "noul"
    with pytest.raises(JevDataError, match="expected type"):
        validate_answers(questions, mistyped)
    with pytest.raises(JevDataError, match="must be an object"):
        validate_answers(questions, ["not", "an", "object"])


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": True, "output_tokens": 0},
        {"input_tokens": 0, "output_tokens": False},
        {"input_tokens": -1, "output_tokens": 0},
        {"input_tokens": 1.5, "output_tokens": 0},
        {"input_tokens": 10},
        {"input_tokens": 10, "output_tokens": None},
        "not-an-object",
        None,
    ],
)
def test_validate_usage_strict(usage):
    with pytest.raises(JevDataError):
        validate_usage(usage)
    assert validate_usage({"input_tokens": 0, "output_tokens": 0}) == {
        "input_tokens": 0,
        "output_tokens": 0,
    }


def test_validate_model_id_exact_pin():
    assert validate_model_id("jev-1.13.0") == "jev-1.13.0"
    for bad in ("jev-9.9.9", "jev-latest", "", "  ", " jev-1.13.0", "jev-1.13.0\n", None, 42):
        with pytest.raises(JevDataError):
            validate_model_id(bad)


# ---------------------------------------------------------------------------
# Exact pinned SDK interface (fake typesafe_sdk module; no network)
# ---------------------------------------------------------------------------


class _FakeSDK:
    """Record exact ctor/question/call args of the pinned typesafe-sdk shape."""

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


def _sdk_response(*, state=None, usage=None, model=JEV_MODEL, answers=None):
    return types.SimpleNamespace(
        answers=copy.deepcopy(answers if answers is not None else perfect_answers(state)),
        usage=dict(usage or {"input_tokens": 1000, "output_tokens": 5}),
        model=model,
    )


def test_sdk_exact_ctor_kwargs_and_questions(monkeypatch):
    state = project_conversation_state(make_state())
    questions = build_questions(state)
    fake = _FakeSDK(response_fn=lambda: _sdk_response(state=state))
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY-123")
    result = asyncio.run(provider(state, questions))
    assert fake.client_kwargs is not None
    assert set(fake.client_kwargs) == {"api_key", "base_url", "model", "retry", "timeout"}
    assert fake.client_kwargs["api_key"] == "FAKE-KEY-123"
    assert fake.client_kwargs["base_url"] == "https://api.typesafe.ai"
    assert fake.client_kwargs["model"] == "jev-1.13.0"
    assert fake.client_kwargs["timeout"] == 10.0 == JEV_TIMEOUT
    assert isinstance(fake.client_kwargs["retry"], fake.module.RetryPolicy)
    assert fake.retry_kwargs == {"args": (), "kwargs": {"max_retries": 0}}
    assert len(fake.system_one_calls) == 1
    call_kwargs = fake.system_one_calls[0]["kwargs"]
    assert set(call_kwargs) == {"state", "questions"}
    assert call_kwargs["state"] == state
    assert len(fake.noul_calls) == 1
    assert set(fake.noul_calls[0]["kwargs"]) == {"instructions"}
    assert len(fake.choice_calls) == 2
    assert all(set(c["kwargs"]) == {"instructions", "criteria"} for c in fake.choice_calls)
    assert any("channel" in c["kwargs"]["criteria"] for c in fake.choice_calls)
    assert result.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert result.model == "jev-1.13.0"


@pytest.mark.parametrize("omit", ["RetryPolicy", "AsyncTypeSafeClient", "Choice", "Noul"])
def test_sdk_missing_interface_failclosed_no_rpc(monkeypatch, omit):
    fake = _FakeSDK(response_fn=lambda: None, omit=(omit,))
    fake.install(monkeypatch)
    with pytest.raises(JevError) as excinfo:
        make_sdk_provider("FAKE-KEY")
    assert excinfo.value.unknown_billing is False
    assert fake.system_one_calls == []


def test_sdk_missing_module_failclosed_no_rpc(monkeypatch):
    monkeypatch.setitem(sys.modules, "typesafe_sdk", None)
    with pytest.raises(JevError) as excinfo:
        make_sdk_provider("FAKE-KEY")
    assert excinfo.value.unknown_billing is False
    assert "Unsupported" in excinfo.value.detail or "Missing" in excinfo.value.detail


def test_sdk_invalid_retry_failclosed_no_fallback(monkeypatch):
    def _boom(*args, **kwargs):
        raise TypeError("old signature without max_retries")

    fake = _FakeSDK(response_fn=lambda: None, retry_init=_boom)
    fake.install(monkeypatch)
    with pytest.raises(JevError) as excinfo:
        make_sdk_provider("FAKE-KEY")
    assert excinfo.value.unknown_billing is False
    assert fake.system_one_calls == []


def test_sdk_unsupported_client_kwargs_failclosed(monkeypatch):
    def _old_client(kwargs):
        if "model" in kwargs:
            raise TypeError("unexpected keyword 'model'")

    fake = _FakeSDK(response_fn=lambda: _sdk_response(), client_init=_old_client)
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY")
    state = project_conversation_state(make_state())
    with pytest.raises(JevError) as excinfo:
        asyncio.run(provider(state, build_questions(state)))
    assert excinfo.value.unknown_billing is False
    assert fake.system_one_calls == []


def test_sdk_close_failure_after_response_keeps_known_usage(monkeypatch):
    state = project_conversation_state(make_state())

    class CanaryCloseError(Exception):
        pass

    fake = _FakeSDK(
        response_fn=lambda: _sdk_response(
            state=state, usage={"input_tokens": 1000, "output_tokens": 1}
        ),
        exit_error=CanaryCloseError("SECRET-CLOSE-BODY-sk-live-999"),
    )
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY")
    with pytest.raises(JevError) as excinfo:
        asyncio.run(provider(state, build_questions(state)))
    err = excinfo.value
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.model == "jev-1.13.0"
    assert err.unknown_billing is False
    assert "SECRET" not in str(err) and "SECRET" not in err.detail
    assert len(fake.system_one_calls) == 1


@pytest.mark.parametrize("kind", ["bad_usage", "no_response"])
def test_sdk_close_failure_unknown_controls(monkeypatch, kind):
    state = project_conversation_state(make_state())

    class CanaryCloseError(Exception):
        pass

    if kind == "bad_usage":
        fake = _FakeSDK(
            response_fn=lambda: _sdk_response(
                state=state, usage={"input_tokens": True, "output_tokens": 0}
            ),
            exit_error=CanaryCloseError("SECRET-BAD-USAGE"),
        )
    else:

        def _no_resp():
            raise ValueError("CANARY-NO-RESPONSE-BODY")

        fake = _FakeSDK(response_fn=_no_resp)
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY")
    with pytest.raises(JevError) as excinfo:
        asyncio.run(provider(state, build_questions(state)))
    err = excinfo.value
    assert err.usage is None
    assert err.unknown_billing is True
    assert "SECRET" not in str(err) and "CANARY" not in str(err)


def test_sdk_error_classification_bounded():
    from jev import _classify_sdk_error

    class TypeSafeAPITimeoutError(Exception):
        pass

    class TypeSafeRateLimitError(Exception):
        status_code = 429

    class Boom(Exception):
        pass

    err = _classify_sdk_error(TypeSafeAPITimeoutError("sk-live-CANARY body"))
    assert (err.kind, err.status) == ("timeout", None)
    assert "CANARY" not in str(err)
    err = _classify_sdk_error(TypeSafeRateLimitError("CANARY"))
    assert (err.kind, err.status) == ("network", 429)
    assert "CANARY" not in str(err)
    boom = Boom("CANARY")
    boom.status_code = True
    err = _classify_sdk_error(boom)
    assert err.status is None
    assert err.detail == "Boom"


def test_sdk_cancellation_propagates_not_converted(monkeypatch):
    state = project_conversation_state(make_state())

    def _cancel():
        raise asyncio.CancelledError()

    fake = _FakeSDK(response_fn=_cancel)
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY")
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(provider(state, build_questions(state)))


def test_sdk_logging_forced_warning_despite_env(monkeypatch, caplog, capsys):
    # Security hardening (pinned SDK 0.7.2): the prior WARNING guard still
    # leaked raw provider bodies via typesafe_sdk WARNING
    # (_prepare_system_one_response drops unknown types). Stricter silence
    # at ALL levels is the expected secure behavior; provider failures
    # still raise bounded JevError with usage (no weakened failure).
    fake = _FakeSDK(response_fn=lambda: None)
    fake.install(monkeypatch)
    monkeypatch.setenv("TYPESAFE_LOG_LEVEL", "DEBUG")
    known = [
        "typesafe",
        "typesafe_sdk",
        "httpx2",
        "httpcore2",
        "httpcore2.connection",
        "httpcore2.http11",
        "httpcore2.http2",
        "httpcore2.proxy",
        "httpcore2.socks",
    ]
    unrelated = ["discord.test_guard", "audit.test_guard", "urllib3.test_guard"]
    saved: dict[str, tuple[int, bool, bool]] = {}
    for name in known + unrelated:
        lg = logging.getLogger(name)
        saved[name] = (lg.level, lg.disabled, lg.propagate)
    orig_handlers = {n: list(logging.getLogger(n).handlers) for n in known}
    try:
        for name in known + unrelated:
            lg = logging.getLogger(name)
            lg.setLevel(logging.DEBUG)
            lg.disabled = False
            lg.propagate = True
        sdk_stream = io.StringIO()
        sdk_handler = logging.StreamHandler(sdk_stream)
        sdk_handler.setLevel(logging.DEBUG)
        logging.getLogger("typesafe_sdk").addHandler(sdk_handler)
        audit_stream = io.StringIO()
        audit_handler = logging.StreamHandler(audit_stream)
        audit_handler.setLevel(logging.DEBUG)
        logging.getLogger("audit.test_guard").addHandler(audit_handler)
        try:
            with caplog.at_level(logging.DEBUG):
                make_sdk_provider("FAKE-KEY-no-read")
                off = logging.CRITICAL + 1
                for name in known:
                    lg = logging.getLogger(name)
                    assert lg.level == off, name
                    assert lg.disabled is True, name
                    assert lg.propagate is False, name
                for name in known:
                    for h in orig_handlers[name]:
                        assert h in logging.getLogger(name).handlers, name
                assert sdk_handler in logging.getLogger("typesafe_sdk").handlers
                for name in unrelated:
                    lg = logging.getLogger(name)
                    assert lg.level == logging.DEBUG, name
                    assert lg.disabled is False, name
                    assert lg.propagate is True, name
                canary = "FAKE_SDK_SILENCE_CANARY"
                sdk_lg = logging.getLogger("typesafe_sdk")
                sdk_lg.debug("debug %s", canary)
                sdk_lg.info("info %s", canary)
                sdk_lg.warning("warn %s", canary)
                sdk_lg.error("error %s", canary)
                sdk_lg.critical("crit %s", canary)
                logging.getLogger("httpx2").warning("transport %s", canary)
                logging.getLogger("httpcore2.connection").debug("t %s", canary)
                assert canary not in caplog.text
                assert canary not in sdk_stream.getvalue()
                assert canary not in capsys.readouterr().out
                audit_canary = "FAKE_AUDIT_SCOPE_CANARY"
                logging.getLogger("audit.test_guard").warning("a %s", audit_canary)
                assert audit_canary in audit_stream.getvalue()
        finally:
            logging.getLogger("typesafe_sdk").removeHandler(sdk_handler)
            logging.getLogger("audit.test_guard").removeHandler(audit_handler)
    finally:
        for name, (level, disabled, propagate) in saved.items():
            lg = logging.getLogger(name)
            lg.setLevel(level)
            lg.disabled = disabled
            lg.propagate = propagate


def test_sdk_preconfigured_child_handlers_silenced(monkeypatch, caplog, capsys):
    # Preconfigured DEBUG/ERROR child loggers with direct handlers must not
    # leak fake auth/raw-body/SDK-error canaries after factory preflight.
    fake = _FakeSDK(response_fn=lambda: None)
    fake.install(monkeypatch)
    monkeypatch.setenv("TYPESAFE_LOG_LEVEL", "DEBUG")
    child_names = [
        "typesafe_sdk.preconf_debug",
        "typesafe.preconf_debug",
        "httpx2.preconf_debug",
        "httpcore2.connection.preconf_debug",
        "httpcore2.http11.preconf_debug",
    ]
    parent_names = ["typesafe_sdk", "typesafe", "httpx2", "httpcore2.connection"]
    saved: dict[str, tuple[int, bool, bool]] = {}
    for name in child_names + parent_names:
        lg = logging.getLogger(name)
        saved[name] = (lg.level, lg.disabled, lg.propagate)
    streams: dict[str, io.StringIO] = {}
    handlers: dict[str, logging.Handler] = {}
    try:
        for name in child_names:
            lg = logging.getLogger(name)
            lg.setLevel(logging.DEBUG)
            lg.disabled = False
            lg.propagate = True
            stream = io.StringIO()
            handler = logging.StreamHandler(stream)
            handler.setLevel(logging.DEBUG)
            lg.addHandler(handler)
            streams[name] = stream
            handlers[name] = handler
        with caplog.at_level(logging.DEBUG):
            make_sdk_provider("FAKE-KEY-no-read")
            off = logging.CRITICAL + 1
            for name in child_names + parent_names:
                lg = logging.getLogger(name)
                assert lg.level == off, name
                assert lg.disabled is True, name
                assert lg.propagate is False, name
            auth_canary = "FAKE_AUTH_CANARY"
            body_canary = "FAKE_RAW_BODY_CANARY"
            err_canary = "FAKE_SDK_ERROR_CANARY"
            for name in child_names:
                lg = logging.getLogger(name)
                lg.debug("auth %s", auth_canary)
                lg.warning("raw %s", body_canary)
                lg.error("sdk %s", err_canary)
            for name in parent_names:
                lg = logging.getLogger(name)
                lg.warning("parent raw %s", body_canary)
                lg.error("parent sdk %s", err_canary)
            for canary in (auth_canary, body_canary, err_canary):
                assert canary not in caplog.text
                assert canary not in capsys.readouterr().out
                for stream in streams.values():
                    assert canary not in stream.getvalue()
    finally:
        for name, handler in handlers.items():
            try:
                logging.getLogger(name).removeHandler(handler)
            except Exception:
                pass
        for name, (level, disabled, propagate) in saved.items():
            lg = logging.getLogger(name)
            lg.setLevel(level)
            lg.disabled = disabled
            lg.propagate = propagate


def test_sdk_unrecognized_answer_warning_silenced_real_sdk(monkeypatch, caplog, capsys):
    # Pinned SDK 0.7.2 CPU-only regression (no live client/API/network):
    # _prepare_system_one_response drops unknown answer types with a WARNING
    # containing raw provider body. Factory must silence it; the adapter
    # must still fail validation with usage known (no silent success).
    monkeypatch.setenv("TYPESAFE_LOG_LEVEL", "DEBUG")
    monkeypatch.delitem(sys.modules, "typesafe_sdk", raising=False)
    known = [
        "typesafe",
        "typesafe_sdk",
        "httpx2",
        "httpcore2",
        "httpcore2.connection",
        "httpcore2.http11",
        "httpcore2.http2",
        "httpcore2.proxy",
        "httpcore2.socks",
    ]
    saved: dict[str, tuple[int, bool, bool]] = {}
    for name in known:
        lg = logging.getLogger(name)
        saved[name] = (lg.level, lg.disabled, lg.propagate)
    sdk_stream = io.StringIO()
    sdk_handler = logging.StreamHandler(sdk_stream)
    sdk_handler.setLevel(logging.DEBUG)
    logging.getLogger("typesafe_sdk").addHandler(sdk_handler)
    try:
        with caplog.at_level(logging.DEBUG):
            # Factory preflight only (imports real SDK, no client call,
            # no network, no bill); guard must silence the SDK WARNING.
            make_sdk_provider("FAKE-KEY-no-read")
            off = logging.CRITICAL + 1
            for name in known:
                lg = logging.getLogger(name)
                assert lg.level == off, name
                assert lg.disabled is True, name
                assert lg.propagate is False, name
            assert sdk_handler in logging.getLogger("typesafe_sdk").handlers
            # Lazy installed-SDK imports inside the test only (never at
            # module import); CPU-only fake response, no network/client.
            import importlib as _importlib

            _real_sdk = sys.modules.get("typesafe_sdk")
            assert _real_sdk is not None
            assert hasattr(_real_sdk, "AsyncTypeSafeClient")
            rt = _importlib.import_module("typesafe_sdk._core.response_types")
            httpx2 = _importlib.import_module("httpx2")
            canary = "FAKE_PROVIDER_BODY_CANARY"
            payload = json.dumps({"answers": {"probe": {"type": canary}}})
            fake_resp = httpx2.Response(200, content=payload.encode("utf-8"))
            decoded = rt._prepare_system_one_response(fake_resp, set())
            assert decoded == {"answers": {}}
            assert canary not in caplog.text
            assert canary not in sdk_stream.getvalue()
            assert canary not in capsys.readouterr().out
            # SDK dropping an unsupported answer still fails adapter
            # validation (no silent success) with valid usage known.
            dropped = perfect_answers()
            del dropped["participation"]

            async def _dropped_provider(sent_state, sent_questions):
                return make_result(answers=copy.deepcopy(dropped))

            with caplog.at_level(logging.DEBUG):
                with pytest.raises(JevError) as excinfo:
                    asyncio.run(JevAdapter(provider=_dropped_provider).decide(make_state()))
            err = excinfo.value
            assert err.kind == "validation"
            assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
            assert err.unknown_billing is False
            assert err.model == "jev-1.13.0"
            assert canary not in caplog.text
            assert canary not in sdk_stream.getvalue()
            assert canary not in capsys.readouterr().out
    finally:
        logging.getLogger("typesafe_sdk").removeHandler(sdk_handler)
        for name, (level, disabled, propagate) in saved.items():
            lg = logging.getLogger(name)
            lg.setLevel(level)
            lg.disabled = disabled
            lg.propagate = propagate


# ---------------------------------------------------------------------------
# Adapter: one projected call, usage-first, typed decision
# ---------------------------------------------------------------------------


def test_decide_success_returns_typed_decision():
    state = make_state()
    calls = []

    async def provider(sent_state, sent_questions):
        calls.append((copy.deepcopy(sent_state), copy.deepcopy(sent_questions)))
        answers = perfect_answers(state)
        answers["addressee"]["confidence"] = 0.8
        return JevProviderResult(
            answers=answers,
            usage={"input_tokens": 1000, "output_tokens": 5},
            model=JEV_MODEL,
            latency_ms=9,
        )

    decision = asyncio.run(JevAdapter(provider=provider).decide(state))
    assert len(calls) == 1
    assert calls[0][0] == project_conversation_state(state)
    assert calls[0][1] == build_questions(state)
    assert decision.addressee == "bot_demo"
    assert decision.explicit_assistant_request == pytest.approx(1.0)
    assert decision.participation == "respond"
    assert decision.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert decision.model == "jev-1.13.0"
    assert decision.latency_ms == 9
    assert decision.answers["addressee"]["confidence"] == pytest.approx(0.8)
    assert set(decision.answers) == set(QUESTION_IDS)


def test_decide_validates_state_before_provider():
    calls = []

    async def provider(sent_state, sent_questions):
        calls.append(1)
        raise AssertionError("unreachable")

    with pytest.raises(ConversationError):
        asyncio.run(JevAdapter(provider=provider).decide(make_state(new_message_id="nope")))
    assert calls == []


@pytest.mark.parametrize("bad_id", [["m3"], {"m3": 1}, ("m3",), None, 123, ""])
def test_decide_invalid_new_message_id_types_no_provider(bad_id):
    calls = []

    async def provider(sent_state, sent_questions):
        calls.append(1)
        raise AssertionError("unreachable")

    with pytest.raises(ConversationError, match="new_message_id"):
        asyncio.run(JevAdapter(provider=provider).decide(make_state(new_message_id=bad_id)))
    assert calls == []


def test_decide_hands_provider_a_detached_snapshot():
    state = make_state()
    seen = []

    async def provider(sent_state, sent_questions):
        seen.append(copy.deepcopy(sent_state))
        return make_result(state)

    asyncio.run(JevAdapter(provider=provider).decide(state))
    state["messages"][0]["text"] = "mutated after the call"
    state["participants"].append({"uid": "mallory_demo", "display_name": "Mallory"})
    assert seen[0] == project_conversation_state(make_state())
    assert set(seen[0]) == {
        "assistant_uid",
        "owner_uid",
        "participants",
        "messages",
        "new_message_id",
    }


def test_decide_no_key_no_provider_auth_closed(monkeypatch):
    monkeypatch.delitem(sys.modules, "typesafe_sdk", raising=False)
    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter().decide(make_state()))
    assert excinfo.value.kind == "auth"
    assert excinfo.value.unknown_billing is False
    assert "typesafe_sdk" not in sys.modules


@pytest.mark.parametrize("key", ["", "   ", 12345, b"bytes-key"])
def test_decide_blank_or_nonstring_key_closed_no_rpc(monkeypatch, key):
    monkeypatch.delitem(sys.modules, "typesafe_sdk", raising=False)
    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(api_key=key).decide(make_state()))
    assert excinfo.value.kind == "auth"
    assert excinfo.value.unknown_billing is False
    assert "typesafe_sdk" not in sys.modules


def test_decide_valid_usage_bad_answers_carries_usage():
    bad = perfect_answers()
    bad["invented"] = {"type": "noul", "noul": 0.5}

    async def provider(sent_state, sent_questions):
        return make_result(answers=copy.deepcopy(bad))

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.model == "jev-1.13.0"
    assert err.unknown_billing is False


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": True, "output_tokens": 0},
        {"input_tokens": -5, "output_tokens": 0},
        {"input_tokens": 10},
        "not-an-object",
    ],
)
def test_decide_bad_usage_flags_billing_unknown(usage):
    async def provider(sent_state, sent_questions):
        return make_result(usage=usage)

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage is None
    assert err.unknown_billing is True


@pytest.mark.parametrize("model, retained", [("jev-9.9.9", "jev-9.9.9"), ("  ", None)])
def test_decide_wrong_model_carries_usage(model, retained):
    async def provider(sent_state, sent_questions):
        return make_result(model=model)

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False
    assert err.model == retained


def test_decide_unexpected_provider_exception_bounded_unknown():
    async def provider(sent_state, sent_questions):
        raise ValueError("CANARY-IN-UNEXPECTED-MESSAGE-sk-live-1")

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "unknown"
    assert err.detail == "ValueError"
    assert err.unknown_billing is True
    assert "CANARY" not in str(err)


def test_decide_sanitizes_carried_usage_and_model():
    async def provider(sent_state, sent_questions):
        raise JevError(
            "timeout",
            detail="TypeSafeAPITimeoutError",
            unknown_billing=False,
            usage={"input_tokens": True, "output_tokens": 0},
            model="  ",
        )

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.usage is None
    assert err.model is None
    assert err.unknown_billing is True


def test_decide_missing_model_carries_valid_usage():
    async def provider(sent_state, sent_questions):
        return types.SimpleNamespace(
            answers=copy.deepcopy(perfect_answers()),
            usage={"input_tokens": 1000, "output_tokens": 1},
            latency_ms=1,
        )

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.unknown_billing is False
    assert err.model is None
    assert err.detail == "AttributeError"


def test_decide_raising_model_getter_carries_usage_no_leak():
    canary = "CANARY-MODEL-GETTER-SECRET"

    class RaisingModel:
        def __init__(self):
            self.answers = copy.deepcopy(perfect_answers())
            self.usage = {"input_tokens": 1000, "output_tokens": 1}

        @property
        def model(self):
            raise ValueError(canary)

        @property
        def latency_ms(self):
            return 1

    async def provider(sent_state, sent_questions):
        return RaisingModel()

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.unknown_billing is False
    assert err.detail == "ValueError"
    assert canary not in str(err)
    assert canary not in repr(err)
    assert canary not in err.detail


def test_decide_missing_latency_carries_valid_usage():
    async def provider(sent_state, sent_questions):
        return types.SimpleNamespace(
            answers=copy.deepcopy(perfect_answers()),
            usage={"input_tokens": 1000, "output_tokens": 1},
            model=JEV_MODEL,
        )

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.unknown_billing is False
    assert err.model == JEV_MODEL
    assert err.detail == "AttributeError"


def test_decide_raising_latency_getter_carries_usage_no_leak():
    canary = "CANARY-LATENCY-SECRET"

    class RaisingLatency:
        def __init__(self):
            self.answers = copy.deepcopy(perfect_answers())
            self.usage = {"input_tokens": 1000, "output_tokens": 1}
            self.model = JEV_MODEL

        @property
        def latency_ms(self):
            raise RuntimeError(canary)

    async def provider(sent_state, sent_questions):
        return RaisingLatency()

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.unknown_billing is False
    assert err.model == JEV_MODEL
    assert err.detail == "RuntimeError"
    assert canary not in str(err)
    assert canary not in repr(err)


def test_sdk_model_getter_raises_preserves_usage(monkeypatch):
    canary = "CANARY-SDK-MODEL-SECRET"
    state = project_conversation_state(make_state())

    class BadModelResponse:
        def __init__(self):
            self.answers = copy.deepcopy(perfect_answers())
            self.usage = {"input_tokens": 1000, "output_tokens": 1}

        @property
        def model(self):
            raise ValueError(canary)

    fake = _FakeSDK(response_fn=lambda: BadModelResponse())
    fake.install(monkeypatch)
    provider = make_sdk_provider("FAKE-KEY")
    with pytest.raises(JevError) as excinfo:
        asyncio.run(provider(state, build_questions(state)))
    err = excinfo.value
    assert err.usage == {"input_tokens": 1000, "output_tokens": 1}
    assert err.unknown_billing is False
    assert err.detail == "ValueError"
    assert canary not in str(err)
    assert canary not in repr(err)


def test_extra_answer_key_canary_absent_preserves_failure(caplog, capsys):
    canary = "FAKE_SECRET_CANARY"
    bad = perfect_answers()
    bad[canary] = {"type": "noul", "noul": 0.5}

    async def provider(sent_state, sent_questions):
        return make_result(answers=copy.deepcopy(bad))

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(JevError) as excinfo:
            asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False
    assert canary not in str(err)
    assert canary not in repr(err)
    assert canary not in err.detail
    assert canary not in caplog.text
    assert canary not in capsys.readouterr().out


def test_invalid_choice_canary_absent_preserves_failure(caplog, capsys):
    canary = "FAKE_SECRET_CANARY"
    bad = perfect_answers()
    bad["addressee"]["choice"] = canary

    async def provider(sent_state, sent_questions):
        return make_result(answers=copy.deepcopy(bad))

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(JevError) as excinfo:
            asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False
    assert "addressee" in err.detail
    assert canary not in str(err)
    assert canary not in repr(err)
    assert canary not in err.detail
    assert canary not in caplog.text
    assert canary not in capsys.readouterr().out


def test_suspect_model_canary_omitted_preserves_failure(caplog, capsys):
    canary = "FAKE_SECRET_CANARY"

    async def provider(sent_state, sent_questions):
        return make_result(model=canary)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(JevError) as excinfo:
            asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False
    assert err.model is None
    assert canary not in str(err)
    assert canary not in repr(err)
    assert canary not in err.detail
    assert canary not in caplog.text
    assert canary not in capsys.readouterr().out


@pytest.mark.parametrize(
    "canary",
    [
        "sk-live-synthetic-canary",
        "jev-1.13.0-canary.evil",
        "fake.model-id",
    ],
)
def test_lowercase_model_canary_omitted_preserves_failure(canary, caplog, capsys):
    # Lowercase hyphen/dot canaries fit a generic text grammar; only anchored
    # jev-MAJOR.MINOR.PATCH ids may be retained in error metadata.
    async def provider(sent_state, sent_questions):
        return make_result(model=canary)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(JevError) as excinfo:
            asyncio.run(JevAdapter(provider=provider).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False
    assert err.model is None
    assert canary not in str(err)
    assert canary not in repr(err)
    assert canary not in err.detail
    assert canary not in caplog.text
    assert canary not in capsys.readouterr().out


def test_decide_single_call_no_retry_no_reuse_state():
    calls = []

    async def provider(sent_state, sent_questions):
        calls.append(1)
        raise JevError("timeout", detail="TypeSafeAPITimeoutError")

    adapter = JevAdapter(provider=provider)
    with pytest.raises(JevError):
        asyncio.run(adapter.decide(make_state()))
    with pytest.raises(JevError):
        asyncio.run(adapter.decide(make_state()))
    assert len(calls) == 2  # exactly one provider call per decide(), never retried


def test_provider_cannot_weaken_validation_by_mutating_questions():
    async def sneaky(sent_state, sent_questions):
        sent_questions["invented"] = {"type": "noul", "instructions": "smuggled"}
        answers = perfect_answers()
        answers["invented"] = {"type": "noul", "noul": 0.5}
        return make_result(answers=answers)

    with pytest.raises(JevError) as excinfo:
        asyncio.run(JevAdapter(provider=sneaky).decide(make_state()))
    err = excinfo.value
    assert err.kind == "validation"
    assert err.usage == {"input_tokens": 1000, "output_tokens": 5}
    assert err.unknown_billing is False


def test_decide_cancellation_propagates():
    async def scenario():
        async def provider(sent_state, sent_questions):
            await asyncio.sleep(10)
            raise AssertionError("unreachable")

        task = asyncio.ensure_future(JevAdapter(provider=provider).decide(make_state()))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Constructor laziness and secret handling
# ---------------------------------------------------------------------------


def test_constructor_defers_all_work(monkeypatch):
    monkeypatch.delitem(sys.modules, "typesafe_sdk", raising=False)
    calls = []

    async def provider(sent_state, sent_questions):
        calls.append(1)
        return make_result()

    JevAdapter(api_key="FAKE-KEY", provider=provider)
    JevAdapter(api_key="FAKE-KEY")
    JevAdapter()
    assert calls == []
    assert "typesafe_sdk" not in sys.modules


def test_api_key_never_in_errors_or_logs(monkeypatch, caplog):
    canary = "SECRET-KEY-CANARY-sk-live-999"
    fake = _FakeSDK(response_fn=lambda: None, omit=("RetryPolicy",))
    fake.install(monkeypatch)
    with pytest.raises(JevError) as excinfo:
        make_sdk_provider(canary)
    assert canary not in str(excinfo.value)
    assert canary not in excinfo.value.detail

    async def provider(sent_state, sent_questions):
        return make_result()

    with caplog.at_level(logging.DEBUG):
        asyncio.run(JevAdapter(api_key=canary, provider=provider).decide(make_state()))
    assert canary not in caplog.text


def test_offline_default_reads_no_key_and_imports_no_sdk(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-KEY-should-never-be-read")
    for mod in ("typesafe_sdk", "dotenv"):
        monkeypatch.delitem(sys.modules, mod, raising=False)
    import conversation  # noqa: F401
    import jev  # noqa: F401

    adapter = JevAdapter(provider=lambda s, q: make_result())
    assert adapter is not None
    assert "typesafe_sdk" not in sys.modules
