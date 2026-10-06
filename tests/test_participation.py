"""Offline tests for the durable Jev participation ledger.

Fake/offline only: synthetic conversation states, fake adapters, TMP
ledger files, fake SIDs and UTC days. No network, no real keys, no paid
calls, no native/outbox/journal access.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import stat
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from conversation import project_conversation_state
from jev import JEV_MODEL, JevDecision, JevError
from participation import (
    ParticipationDurabilityUncertain,
    ParticipationError,
    ParticipationLedger,
    ParticipationPending,
    ParticipationReason,
)

FAKE_SID = "2026-01-01T00-00-00_aaaa"
OTHER_SID = "2026-01-02T00-00-00_bbbb"
CHANNEL = 7
AUTHOR = 2


def make_state(tag="", *, mid=100, author=2, **overrides):
    # Binding-compliant: snapshot.new_message_id == str(mid) and current
    # sender == str(author), so the state actually represents that source.
    # Base trio preserved for Jev criteria; author added when distinct.
    author_str = str(author)
    mid_str = str(mid)
    participants = [
        {"uid": "alice_demo", "display_name": "Alice"},
        {"uid": "bot_demo", "display_name": "HelperBot"},
        {"uid": "carol_demo", "display_name": "Carol"},
    ]
    if author_str not in {p["uid"] for p in participants}:
        participants.append({"uid": author_str, "display_name": f"User{author_str}"})
    state = {
        "assistant_uid": "bot_demo",
        "owner_uid": "alice_demo",
        "participants": participants,
        "messages": [
            {"id": f"{tag}m1", "sender_uid": "alice_demo", "text": "hi all", "reply_to": None},
            {
                "id": f"{tag}m2",
                "sender_uid": "bot_demo",
                "text": "hello Alice",
                "reply_to": f"{tag}m1",
            },
            {
                "id": mid_str,
                "sender_uid": author_str,
                "text": "HelperBot, please summarize",
                "reply_to": f"{tag}m2",
            },
        ],
        "new_message_id": mid_str,
    }
    state.update(overrides)
    return state


def perfect_answers(state=None, *, participation="respond"):
    return {
        "addressee": {
            "type": "choice",
            "choice": "bot_demo",
            "probabilities": {"bot_demo": 0.8, "channel": 0.1, "unclear": 0.1},
        },
        "explicit_assistant_request": {"type": "noul", "noul": 1.0},
        "participation": {
            "type": "choice",
            "choice": participation,
            "probabilities": {"respond": 0.8, "observe": 0.2},
        },
    }


def legacy_five_answers(state=None, *, participation="respond"):
    """Pre-3-question stored shape (sender_uid + reply_parent_status included)."""
    state = state or make_state()
    uids = [p["uid"] for p in state["participants"]]
    answers = perfect_answers(state, participation=participation)
    answers["sender_uid"] = {
        "type": "choice",
        "choice": "carol_demo",
        "probabilities": dict.fromkeys(uids, 0.1),
    }
    answers["reply_parent_status"] = {
        "type": "choice",
        "choice": "visible",
        "probabilities": {"none": 0.1, "visible": 0.8, "missing": 0.1},
    }
    return answers


def fingerprint_for(state):
    snapshot = project_conversation_state(state)
    canonical = json.dumps(
        snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class FakeAdapter:
    def __init__(self, fn):
        self._fn = fn
        self.calls = 0

    async def decide(self, state):
        self.calls += 1
        return await self._fn(state)


def success_fn(state=None, *, usage=None, participation="respond", model=JEV_MODEL):
    async def _fn(seen):
        base = state if state is not None else make_state()
        return JevDecision(
            answers=copy.deepcopy(perfect_answers(base, participation=participation)),
            usage=dict(usage or {"input_tokens": 1000, "output_tokens": 5}),
            model=model,
            latency_ms=7,
        )

    return _fn


class DayCtl:
    def __init__(self, day):
        self.day = day

    def __call__(self):
        return self.day


def ledger_path(tmp_path: Path, name="participation.json") -> Path:
    # Ledger path derives from configured delivery parent
    # (sibling of delivery_state.json), never repo default.
    delivery_path = tmp_path / "delivery_state.json"
    return delivery_path.parent / name


def make_ledger(tmp_path, *, sid=FAKE_SID, day="2026-10-05", name=None):
    path = ledger_path(tmp_path, name or "participation.json")
    ledger = ParticipationLedger(path, native_sid=sid, day_provider=DayCtl(day))
    return ledger, path


# ---------------------------------------------------------------------------
# Import purity: no key, no env, no SDK
# ---------------------------------------------------------------------------


def test_import_pure_no_sdk_no_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-should-never-be-read")
    for mod in ("typesafe_sdk", "dotenv"):
        monkeypatch.delitem(sys.modules, mod, raising=False)
    import participation as part

    assert part.PARTICIPATION_VERSION == 1
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["status"] == "decided"
    assert "typesafe_sdk" not in sys.modules
    assert "dotenv" not in sys.modules
    blob = path.read_bytes().decode("utf-8")
    assert "SENTINEL" not in blob


# ---------------------------------------------------------------------------
# Success record binds SID/channel/MID/UID/fingerprint/model/attempt/day
# ---------------------------------------------------------------------------


def test_success_record_binds_and_proof_fields(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state, usage={"input_tokens": 1000, "output_tokens": 5}))
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["status"] == "decided"
    assert record["native_session_id"] == FAKE_SID
    assert record["channel_id"] == CHANNEL
    assert record["message_id"] == 100
    assert record["author_uid"] == AUTHOR
    assert record["fingerprint"] == fingerprint_for(state)
    assert len(record["fingerprint"]) == 64
    assert record["model"] == "jev-1.13.0" == JEV_MODEL
    assert record["attempted_day"] == "2026-10-05"
    assert record["attempted_at"]
    # Canonical uuid4.hex: 32 lower hex, no hyphens, valid UUID syntax.
    assert len(record["attempt_id"]) == 32
    assert "-" not in record["attempt_id"]
    assert all(c in "0123456789abcdef" for c in record["attempt_id"])
    assert uuid.UUID(record["attempt_id"]).hex == record["attempt_id"]
    assert record["usage"] == {"input_tokens": 1000, "output_tokens": 5}
    assert record["estimated_cost_usd"] == "0.000042"
    assert record["billing"] == "known"
    assert record["participation"] == "respond"
    assert set(record["answers"]) == {
        "addressee",
        "explicit_assistant_request",
        "participation",
    }
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert list(tmp_path.glob("*.tmp.*")) == []
    # No raw text in the durable file.
    blob = path.read_bytes().decode("utf-8")
    assert "HelperBot, please summarize" not in blob
    assert "hi all" not in blob


def test_fingerprint_stable_and_sensitive(tmp_path):
    first = fingerprint_for(make_state())
    second = fingerprint_for(make_state())
    assert first == second
    other = make_state()
    other["messages"][0]["text"] = "different words"
    assert fingerprint_for(other) != first
    assert len(first) == 64 and all(c in "0123456789abcdef" for c in first)


def test_legacy_five_answers_still_load_and_cache_hit(tmp_path):
    """Pre-3-question decided records validate read-only and serve cache hits."""
    state = make_state()
    attempt_id = uuid.uuid4().hex
    ledger_id = uuid.uuid4().hex
    path = tmp_path / "jev.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "ledger_id": ledger_id,
                "native_session_id": FAKE_SID,
                "attempts": {
                    f"{CHANNEL}:100": {
                        "native_session_id": FAKE_SID,
                        "channel_id": CHANNEL,
                        "message_id": 100,
                        "author_uid": AUTHOR,
                        "fingerprint": fingerprint_for(state),
                        "model": JEV_MODEL,
                        "attempt_id": attempt_id,
                        "attempted_day": "2026-10-05",
                        "attempted_at": "2026-10-05T00:00:00+00:00",
                        "status": "decided",
                        "usage": {"input_tokens": 1000, "output_tokens": 5},
                        "estimated_cost_usd": "0.000042",
                        "billing": "known",
                        "participation": "respond",
                        "answers": legacy_five_answers(state),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "decided"
    assert set(stored["answers"]) == {
        "sender_uid",
        "addressee",
        "explicit_assistant_request",
        "reply_parent_status",
        "participation",
    }
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"

    async def _boom(seen):
        raise AssertionError("legacy cache hit must not call the provider")

    record = asyncio.run(
        ledger.classify(
            FakeAdapter(_boom), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided" and record["attempt_id"] == attempt_id


# ---------------------------------------------------------------------------
# Duplicate: cached decided only on exact binding, else pending, no provider
# ---------------------------------------------------------------------------


def test_duplicate_same_returns_cached_without_provider_or_write(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    first = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert adapter.calls == 1
    before = path.read_bytes()
    second_adapter = FakeAdapter(success_fn(state))
    second = asyncio.run(
        ledger.classify(
            second_adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert second == first
    assert second_adapter.calls == 0
    assert path.read_bytes() == before
    assert second is not first
    assert second["attempt_id"] == first["attempt_id"]


def test_duplicate_context_mismatch_pending_no_provider(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert adapter.calls == 1
    before = path.read_bytes()
    other = make_state()
    other["messages"][0]["text"] = "changed context words"
    other_adapter = FakeAdapter(success_fn(other))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                other_adapter, other, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert other_adapter.calls == 0
    assert path.read_bytes() == before


def test_duplicate_author_mismatch_pending(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    other_adapter = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                other_adapter, state, channel_id=CHANNEL, message_id=100, author_uid=999
            )
        )
    assert excinfo.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert other_adapter.calls == 0


# ---------------------------------------------------------------------------
# Restart every status
# ---------------------------------------------------------------------------


def test_restart_decided_cached(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    first = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.get(CHANNEL, 100) == first
    fresh_adapter = FakeAdapter(success_fn(state))
    second = asyncio.run(
        restarted.classify(
            fresh_adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert second == first
    assert fresh_adapter.calls == 0


def _inject_started(path, state, *, channel=CHANNEL, mid=100, author=AUTHOR, day="2026-10-05"):
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["attempts"][f"{channel}:{mid}"] = {
        "native_session_id": FAKE_SID,
        "channel_id": channel,
        "message_id": mid,
        "author_uid": author,
        "fingerprint": fingerprint_for(state),
        "model": JEV_MODEL,
        "attempt_id": uuid.uuid4().hex,
        "attempted_day": day,
        "attempted_at": "2026-10-05T00:00:00+00:00",
        "status": "started",
        "usage": None,
        "estimated_cost_usd": None,
        "billing": "pending",
        "participation": None,
        "answers": None,
    }
    path.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")


def test_restart_started_blocks_same_and_other_unknown_halt(tmp_path):
    # Changed from allows_other: orphan started after restart means billing
    # unknown too (no usage/cost, may have been billed). Accepted policy is
    # unknown-halt for NEW paid (user-delegated needContext/20-window/$0.01
    # UTC-day, no-new-paid incl restart/no-rebill) -- not a new threshold.
    # Cached decided still returns; NEW sources halt across rollover.
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    _inject_started(path, state)
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.get(CHANNEL, 100)["status"] == "started"
    assert restarted.summary()["unknown_blocked"] is True
    adapter = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_STARTED
    assert adapter.calls == 0
    # NEW source halted: orphan billing ambiguity, 0 provider calls.
    other_state = make_state(tag="o", mid=101)
    other_adapter = FakeAdapter(success_fn(other_state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                other_adapter, other_state, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert other_adapter.calls == 0
    assert restarted.get(CHANNEL, 101) is None


def test_restart_failed_known_blocks_same_allows_other(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    async def _fail(_seen):
        raise JevError(
            "validation",
            detail="JevDataError",
            unknown_billing=False,
            usage={"input_tokens": 1000, "output_tokens": 1},
            model=JEV_MODEL,
        )

    adapter = FakeAdapter(_fail)
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_known"
    assert stored["usage"] == {"input_tokens": 1000, "output_tokens": 1}
    assert stored["estimated_cost_usd"] == "0.000042"
    assert stored["billing"] == "known"
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    retry = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(retry, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    assert retry.calls == 0
    other_state = make_state(tag="o", mid=101)
    other_adapter = FakeAdapter(success_fn(other_state))
    record = asyncio.run(
        restarted.classify(
            other_adapter, other_state, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"


def test_restart_failed_unknown_blocks_globally_even_after_rollover(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    async def _fail(_seen):
        raise JevError("timeout", detail="TypeSafeAPITimeoutError", unknown_billing=True)

    adapter = FakeAdapter(_fail)
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_unknown"
    assert stored["usage"] is None
    assert stored["estimated_cost_usd"] == "unknown"
    assert stored["billing"] == "unknown"
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    retry = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(retry, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    new_adapter = FakeAdapter(success_fn(make_state(tag="n", mid=101)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                new_adapter,
                make_state(tag="n", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert new_adapter.calls == 0
    # Day rollover never silently clears the unknown block.
    rolled = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-06"))
    rolled_adapter = FakeAdapter(success_fn(make_state(tag="r", mid=102)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            rolled.classify(
                rolled_adapter,
                make_state(tag="r", mid=102),
                channel_id=CHANNEL,
                message_id=102,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert rolled_adapter.calls == 0


# ---------------------------------------------------------------------------
# UTC rollover, exact Decimal, cost reporting (no spend stop)
# ---------------------------------------------------------------------------


def test_utc_rollover_resets_known_cost(tmp_path):
    day = DayCtl("2026-10-05")
    path = ledger_path(tmp_path)
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=day)
    state = make_state()
    adapter = FakeAdapter(success_fn(state, usage={"input_tokens": 1000, "output_tokens": 1}))
    asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"
    day.day = "2026-10-06"
    assert ledger.summary()["known_cost_today_usd"] == "0"
    assert ledger.summary()["today"] == "2026-10-06"
    other_state = make_state(tag="n", mid=101)
    other_adapter = FakeAdapter(success_fn(other_state))
    record = asyncio.run(
        ledger.classify(
            other_adapter, other_state, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
        )
    )
    assert record["attempted_day"] == "2026-10-06"
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"


@pytest.mark.parametrize(
    ("tokens", "expected"),
    [(0, "0"), (1, "0.000000042"), (1000, "0.000042"), (1000000, "0.042"), (1000000000, "42")],
)
def test_exact_decimal_cost_strings(tmp_path, tokens, expected):
    ledger, _ = make_ledger(tmp_path)
    state = make_state(tag=f"t{tokens}", mid=100 + tokens % 100000)
    adapter = FakeAdapter(success_fn(state, usage={"input_tokens": tokens, "output_tokens": 0}))
    record = asyncio.run(
        ledger.classify(
            adapter, state, channel_id=CHANNEL, message_id=100 + tokens % 100000, author_uid=AUTHOR
        )
    )
    assert record["estimated_cost_usd"] == expected
    assert Decimal(record["estimated_cost_usd"]) == Decimal(tokens) * Decimal("0.000000042")
    assert "E" not in record["estimated_cost_usd"] and "e" not in record["estimated_cost_usd"]


def test_no_daily_stop_second_call_proceeds(tmp_path):
    path = ledger_path(tmp_path)
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    state = make_state()
    adapter = FakeAdapter(success_fn(state, usage={"input_tokens": 1000, "output_tokens": 1}))
    asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    before = path.read_bytes()
    other_state = make_state(tag="o", mid=101)
    other_adapter = FakeAdapter(success_fn(other_state))
    record = asyncio.run(
        ledger.classify(
            other_adapter, other_state, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"
    assert other_adapter.calls == 1
    assert path.read_bytes() != before
    assert ledger.get(CHANNEL, 101) is not None
    assert ledger.summary()["known_cost_today_usd"] == "0.000084"


def test_known_failure_cost_reported_without_stop(tmp_path):
    path = ledger_path(tmp_path)
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    state = make_state()

    async def _fail(_seen):
        raise JevError(
            "validation",
            detail="JevDataError",
            unknown_billing=False,
            usage={"input_tokens": 1000, "output_tokens": 0},
            model=JEV_MODEL,
        )

    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fail), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"
    other_adapter = FakeAdapter(success_fn(make_state(tag="o", mid=101)))
    record = asyncio.run(
        ledger.classify(
            other_adapter,
            make_state(tag="o", mid=101),
            channel_id=CHANNEL,
            message_id=101,
            author_uid=AUTHOR,
        )
    )
    assert record["status"] == "decided"
    assert other_adapter.calls == 1
    assert ledger.summary()["known_cost_today_usd"] == "0.000084"


# ---------------------------------------------------------------------------
# Persistence-boundary revalidation: usage charged even on model/answer fail
# ---------------------------------------------------------------------------


def test_decision_wrong_model_charged_known(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(
        success_fn(state, usage={"input_tokens": 1000, "output_tokens": 1}, model="jev-9.9.9")
    )
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_known"
    assert stored["usage"] == {"input_tokens": 1000, "output_tokens": 1}
    assert stored["estimated_cost_usd"] == "0.000042"
    assert "jev-9.9.9" not in path.read_bytes().decode("utf-8")


def test_decision_bad_answers_charged_known(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()
    bad = perfect_answers(state)
    bad["addressee"]["choice"] = "ghost_demo"

    async def _fn(_seen):
        return JevDecision(
            answers=copy.deepcopy(bad),
            usage={"input_tokens": 500, "output_tokens": 2},
            model=JEV_MODEL,
            latency_ms=3,
        )

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_known"
    assert stored["usage"] == {"input_tokens": 500, "output_tokens": 2}


def test_decision_bad_usage_leaves_unknown_and_blocks(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()

    async def _fn(_seen):
        return JevDecision(
            answers=copy.deepcopy(perfect_answers(state)),
            usage={"input_tokens": True, "output_tokens": 0},
            model=JEV_MODEL,
            latency_ms=3,
        )

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert ledger.get(CHANNEL, 100)["billing"] == "unknown"
    other_adapter = FakeAdapter(success_fn(make_state(tag="o", mid=101)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                other_adapter,
                make_state(tag="o", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED


def test_unexpected_exception_leaves_unknown_no_leak(tmp_path):
    canary = "CANARY_UNEXPECTED_SECRET_9f8e7d"
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    async def _boom(_seen):
        raise ValueError(canary)

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_boom), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert canary not in str(excinfo.value)
    assert canary not in path.read_bytes().decode("utf-8")
    assert ledger.get(CHANNEL, 100)["estimated_cost_usd"] == "unknown"


# ---------------------------------------------------------------------------
# Fake canaries: no raw payload/freeform errors/text/profiles/auth
# ---------------------------------------------------------------------------


def test_canaries_never_persisted_or_echoed(tmp_path):
    body_canary = "CANARY_BODY_SUPER_SECRET_aaa111"
    err_canary = "CANARY_FREEFORM_boom_trace_xyz789"
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    state["messages"][0]["text"] = body_canary
    state["messages"][1]["text"] = "profile USER.md auth sk-live should never persist"
    adapter = FakeAdapter(success_fn(state))
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["status"] == "decided"
    blob = path.read_bytes().decode("utf-8")
    assert body_canary not in blob
    assert "USER.md" not in blob
    assert "sk-live" not in blob

    async def _fail(_seen):
        raise JevError("network", detail=err_canary, unknown_billing=True)

    other_state = make_state(tag="e", mid=200)
    # Unknown from a fresh ledger would block; use a separate file for the error canary.
    ledger2, path2 = make_ledger(tmp_path, name="participation2.json")
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger2.classify(
                FakeAdapter(_fail),
                other_state,
                channel_id=CHANNEL,
                message_id=200,
                author_uid=AUTHOR,
            )
        )
    assert err_canary not in str(excinfo.value)
    assert err_canary not in path2.read_bytes().decode("utf-8")

    bad_model_canary = "CANARY_MODEL_EVIL_123"
    ledger3, path3 = make_ledger(tmp_path, name="participation3.json")
    other_state3 = make_state(tag="e", mid=300)
    bad_adapter = FakeAdapter(success_fn(other_state3, model=bad_model_canary))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger3.classify(
                bad_adapter, other_state3, channel_id=CHANNEL, message_id=300, author_uid=AUTHOR
            )
        )
    assert bad_model_canary not in str(excinfo.value)
    assert bad_model_canary not in path3.read_bytes().decode("utf-8")


# ---------------------------------------------------------------------------
# Cancellation persists failed_unknown then propagates; in-progress guard
# ---------------------------------------------------------------------------


def test_cancellation_persists_unknown_then_propagates(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    entered = asyncio.Event()

    async def _hang(_seen):
        entered.set()
        await asyncio.sleep(30)

    async def _scenario():
        task = asyncio.ensure_future(
            ledger.classify(
                FakeAdapter(_hang), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
        await entered.wait()
        # Started snapshot is already durable before the provider await.
        assert (
            json.loads(path.read_text(encoding="utf-8"))["attempts"]["7:100"]["status"] == "started"
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_scenario())
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_unknown"
    assert stored["billing"] == "unknown"
    assert stored["estimated_cost_usd"] == "unknown"
    assert stored["usage"] is None
    retry = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(retry, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    new_adapter = FakeAdapter(success_fn(make_state(tag="n", mid=101)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                new_adapter,
                make_state(tag="n", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED


def test_concurrent_duplicate_in_progress_no_second_call(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()
    release = asyncio.Event()
    adapter = FakeAdapter(success_fn(state))

    async def _blocking(seen):
        await release.wait()
        return await adapter.decide(seen)

    blocking = FakeAdapter(_blocking)

    async def _scenario():
        first = asyncio.ensure_future(
            ledger.classify(blocking, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert blocking.calls == 1
        with pytest.raises(ParticipationPending) as excinfo:
            await ledger.classify(
                FakeAdapter(success_fn(state)),
                state,
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        assert excinfo.value.reason is ParticipationReason.IN_PROGRESS
        release.set()
        record = await first
        assert record["status"] == "decided"
        assert adapter.calls == 1

    asyncio.run(_scenario())


# ---------------------------------------------------------------------------
# fsync pre/post commit
# ---------------------------------------------------------------------------


def test_failed_replace_preserves_bytes_and_removes_temp(tmp_path, monkeypatch):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    before = path.read_bytes()

    def _boom(*args, **kwargs):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(os, "replace", _boom)
    other_adapter = FakeAdapter(success_fn(make_state(tag="o", mid=101)))
    with pytest.raises(OSError, match="simulated disk failure"):
        asyncio.run(
            ledger.classify(
                other_adapter,
                make_state(tag="o", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert other_adapter.calls == 0
    assert path.read_bytes() == before
    assert list(tmp_path.glob("*.tmp.*")) == []
    monkeypatch.undo()
    record = asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(make_state(tag="o", mid=101))),
            make_state(tag="o", mid=101),
            channel_id=CHANNEL,
            message_id=101,
            author_uid=AUTHOR,
        )
    )
    assert record["status"] == "decided"


def _fail_dir_fsync(monkeypatch, secret):
    real_fsync = os.fsync

    def _fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(secret)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", _fsync)


def test_post_replace_dir_fsync_started_is_uncertain_must_stop(tmp_path, monkeypatch):
    secret = "CANARY_DIR_FSYNC_SECRET_zzz"
    ledger, path = make_ledger(tmp_path)
    _fail_dir_fsync(monkeypatch, secret)
    state = make_state()
    adapter = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationDurabilityUncertain) as excinfo:
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert excinfo.value.committed is True
    assert secret not in str(excinfo.value)
    assert adapter.calls == 0
    # Committed started exists and validates; never rolled back.
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["attempts"]["7:100"]["status"] == "started"
    assert list(tmp_path.glob("*.tmp.*")) == []
    monkeypatch.undo()
    # Restart sees the committed started and refuses a retry.
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                FakeAdapter(success_fn(state)),
                state,
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_STARTED


def test_post_replace_dir_fsync_final_is_uncertain_no_return(tmp_path, monkeypatch):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    release = asyncio.Event()
    entered = asyncio.Event()
    inner = FakeAdapter(success_fn(state))

    async def _blocking(seen):
        entered.set()
        await release.wait()
        return await inner.decide(seen)

    async def _scenario():
        task = asyncio.ensure_future(
            ledger.classify(
                FakeAdapter(_blocking), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
        await entered.wait()
        _fail_dir_fsync(monkeypatch, "CANARY_FINAL_DIR_SYNC")
        release.set()
        with pytest.raises(ParticipationDurabilityUncertain) as excinfo:
            await task
        assert excinfo.value.committed is True
        assert "CANARY" not in str(excinfo.value)

    asyncio.run(_scenario())
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["attempts"]["7:100"]["status"] == "decided"
    monkeypatch.undo()
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.get(CHANNEL, 100)["status"] == "decided"


# ---------------------------------------------------------------------------
# Symlink / corrupt / wrong SID retention, unsafe paths
# ---------------------------------------------------------------------------


def test_symlink_state_refused(tmp_path):
    _, path = make_ledger(tmp_path)
    link = tmp_path / "link.json"
    try:
        link.symlink_to(path)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(ParticipationError, match="symlink"):
        ParticipationLedger(link, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    before = path.read_bytes()
    # Direct path-object symlink checks fail closed on every API.
    ledger_sym = ParticipationLedger.__new__(ParticipationLedger)
    ledger_sym._path = link
    ledger_sym._native_sid = FAKE_SID
    ledger_sym._day_provider = DayCtl("2026-10-05")
    with pytest.raises(ParticipationError, match="symlink"):
        ledger_sym.get(CHANNEL, 100)
    with pytest.raises(ParticipationError, match="symlink"):
        ledger_sym.summary()
    assert path.read_bytes() == before
    assert ledger.get(CHANNEL, 999) is None


def test_corrupt_retention_never_reset(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    before_valid = path.read_bytes()
    path.write_text("not json{", encoding="utf-8")
    corrupt_bytes = path.read_bytes()
    with pytest.raises(ParticipationError, match="corrupt"):
        ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationError, match="corrupt"):
        ledger.get(CHANNEL, 100)
    with pytest.raises(ParticipationError, match="corrupt"):
        ledger.summary()
    with pytest.raises(ParticipationError, match="corrupt"):
        asyncio.run(
            ledger.classify(
                FakeAdapter(success_fn(make_state(tag="o", mid=101))),
                make_state(tag="o", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert path.read_bytes() == corrupt_bytes
    assert list(tmp_path.glob("*.tmp.*")) == []
    assert before_valid != corrupt_bytes


def test_wrong_sid_fails_closed(tmp_path):
    _, path = make_ledger(tmp_path, sid=FAKE_SID)
    before = path.read_bytes()
    with pytest.raises(ParticipationError, match="session"):
        ParticipationLedger(path, native_sid=OTHER_SID, day_provider=DayCtl("2026-10-05"))
    assert path.read_bytes() == before


def test_unsafe_paths_fail_closed(tmp_path):
    with pytest.raises(ParticipationError):
        ParticipationLedger(tmp_path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(ParticipationError):
        ParticipationLedger(
            blocker / "ledger.json", native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05")
        )


def test_unknown_top_and_record_keys_preserved(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["_future_top"] = {"note": "keep-me"}
    raw["attempts"]["7:100"]["_future_job"] = "keep-job"
    path.write_text(json.dumps(raw), encoding="utf-8")
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.get(CHANNEL, 100)["_future_job"] == "keep-job"
    assert restarted.summary()["attempts"] == 1
    other_state = make_state(tag="o", mid=101)
    asyncio.run(
        restarted.classify(
            FakeAdapter(success_fn(other_state)),
            other_state,
            channel_id=CHANNEL,
            message_id=101,
            author_uid=AUTHOR,
        )
    )
    fresh = json.loads(path.read_text(encoding="utf-8"))
    assert fresh["_future_top"] == {"note": "keep-me"}
    assert fresh["attempts"]["7:100"]["_future_job"] == "keep-job"


def test_permissions_new_dirs_700_existing_untouched(tmp_path):
    before_mode = stat.S_IMODE(tmp_path.stat().st_mode)
    nested = tmp_path / "newdir" / "sub" / "participation.json"
    ledger = ParticipationLedger(nested, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert stat.S_IMODE(nested.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "newdir").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "newdir" / "sub").stat().st_mode) == 0o700
    assert stat.S_IMODE(tmp_path.stat().st_mode) == before_mode
    state = make_state()
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    assert stat.S_IMODE(nested.stat().st_mode) == 0o600
    assert stat.S_IMODE(tmp_path.stat().st_mode) == before_mode


# ---------------------------------------------------------------------------
# Validation, summary, detachment
# ---------------------------------------------------------------------------


def test_invalid_ids_raise_error_not_pending(tmp_path):
    ledger, path = make_ledger(tmp_path)
    before = path.read_bytes()
    for bad in (0, -1, True, "7", None, 7.0):
        with pytest.raises(ParticipationError):
            ledger.get(bad, 100)
        with pytest.raises(ParticipationError):
            asyncio.run(
                ledger.classify(
                    FakeAdapter(success_fn(make_state())),
                    make_state(),
                    channel_id=bad,
                    message_id=100,
                    author_uid=AUTHOR,
                )
            )
    assert path.read_bytes() == before


def test_invalid_state_no_write_no_provider(tmp_path):
    ledger, path = make_ledger(tmp_path)
    before = path.read_bytes()
    bad = make_state(new_message_id="nope")
    adapter = FakeAdapter(success_fn(make_state()))
    with pytest.raises(ParticipationError, match="invalid conversation"):
        asyncio.run(
            ledger.classify(adapter, bad, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert adapter.calls == 0
    assert path.read_bytes() == before


def test_get_unknown_returns_none_and_summary_counts(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    assert ledger.get(CHANNEL, 999) is None
    summary = ledger.summary()
    assert summary["attempts"] == 0
    assert summary["today"] == "2026-10-05"
    assert summary["known_cost_today_usd"] == "0"
    assert summary["unknown_blocked"] is False
    state = make_state()
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    summary = ledger.summary()
    assert summary["attempts"] == 1
    assert summary["decided"] == 1
    assert summary["known_cost_today_usd"] == "0.000042"


def test_all_snapshots_detached(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    record = asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    record["status"] = "hacked"
    record["answers"]["participation"]["choice"] = "hacked"
    record["usage"]["input_tokens"] = 999999
    fetched = ledger.get(CHANNEL, 100)
    assert fetched is not None and fetched["status"] == "decided"
    assert fetched["participation"] == "respond"
    assert fetched["usage"] == {"input_tokens": 1000, "output_tokens": 5}
    fetched["status"] = "hacked2"
    assert ledger.get(CHANNEL, 100)["status"] == "decided"
    summary = ledger.summary()
    summary["known_cost_today_usd"] = "hacked"
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"
    # Caller mutation of the input state after the call cannot affect storage.
    state["messages"][0]["text"] = "mutated after call"
    assert ledger.get(CHANNEL, 100)["fingerprint"] == fingerprint_for(make_state())
    assert "mutated after call" not in path.read_bytes().decode("utf-8")


def test_provider_mutation_cannot_inject_stored_decision(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()
    leaked: dict = {}

    async def _fn(_seen):
        decision = JevDecision(
            answers=copy.deepcopy(perfect_answers(state)),
            usage={"input_tokens": 100, "output_tokens": 1},
            model=JEV_MODEL,
            latency_ms=1,
        )
        leaked["answers"] = decision.answers
        leaked["usage"] = decision.usage
        return decision

    record = asyncio.run(
        ledger.classify(
            FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"
    leaked["answers"]["participation"]["choice"] = "observe"
    leaked["answers"]["invented"] = {"type": "noul", "noul": 0.5}
    leaked["usage"]["input_tokens"] = 999999
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["participation"] == "respond"
    assert stored["usage"] == {"input_tokens": 100, "output_tokens": 1}
    assert "invented" not in stored["answers"]


# ---------------------------------------------------------------------------
# Usage-first boundary: valid usage charged, lower-bound + halt
# ---------------------------------------------------------------------------


def test_jev_valid_usage_unknown_flag_retains_cost_and_halts(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    async def _fail(_seen):
        raise JevError(
            "timeout",
            detail="TypeSafeAPITimeoutError",
            unknown_billing=True,
            usage={"input_tokens": 1000, "output_tokens": 2},
            model=JEV_MODEL,
        )

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fail), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_unknown"
    assert stored["usage"] == {"input_tokens": 1000, "output_tokens": 2}
    assert stored["estimated_cost_usd"] == "0.000042"
    assert stored["billing"] == "unknown"
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"
    assert ledger.summary()["unknown_blocked"] is True
    blob = path.read_bytes().decode("utf-8")
    assert "TypeSafeAPITimeoutError" not in blob
    new_adapter = FakeAdapter(success_fn(make_state(tag="n", mid=101)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                new_adapter,
                make_state(tag="n", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert new_adapter.calls == 0


def test_jev_no_usage_known_zero_no_halt(tmp_path):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()

    async def _fail(_seen):
        raise JevError("auth", detail="no api key", unknown_billing=False, usage=None)

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fail), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_known"
    assert stored["usage"] == {"input_tokens": 0, "output_tokens": 0}
    assert stored["estimated_cost_usd"] == "0"
    assert stored["billing"] == "known"
    assert ledger.summary()["unknown_blocked"] is False
    assert ledger.summary()["known_cost_today_usd"] == "0"
    other = FakeAdapter(success_fn(make_state(tag="o", mid=101)))
    record = asyncio.run(
        ledger.classify(
            other,
            make_state(tag="o", mid=101),
            channel_id=CHANNEL,
            message_id=101,
            author_uid=AUTHOR,
        )
    )
    assert record["status"] == "decided"


def test_usage_first_raising_model_charged_known_no_leak(tmp_path):
    canary = "CANARY_MODEL_GETTER_SECRET"
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    class RaisingModel:
        def __init__(self):
            self.usage = {"input_tokens": 1000, "output_tokens": 1}
            self.answers = copy.deepcopy(perfect_answers(state))

        @property
        def model(self):
            raise ValueError(canary)

    async def _fn(_seen):
        return RaisingModel()

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    assert canary not in str(excinfo.value)
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_known"
    assert stored["usage"] == {"input_tokens": 1000, "output_tokens": 1}
    assert stored["estimated_cost_usd"] == "0.000042"
    assert canary not in path.read_bytes().decode("utf-8")


def test_usage_first_raising_answers_charged_known(tmp_path):
    canary = "CANARY_ANSWERS_GETTER_SECRET"
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    class RaisingAnswers:
        def __init__(self):
            self.usage = {"input_tokens": 500, "output_tokens": 1}
            self.model = JEV_MODEL

        @property
        def answers(self):
            raise ValueError(canary)

    async def _fn(_seen):
        return RaisingAnswers()

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["usage"] == {"input_tokens": 500, "output_tokens": 1}
    assert canary not in path.read_bytes().decode("utf-8")


def test_usage_first_raising_usage_pure_unknown_halts(tmp_path):
    canary = "CANARY_USAGE_GETTER_SECRET"
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    class RaisingUsage:
        @property
        def usage(self):
            raise ValueError(canary)

        @property
        def model(self):
            return JEV_MODEL

        @property
        def answers(self):
            return copy.deepcopy(perfect_answers(state))

    async def _fn(_seen):
        return RaisingUsage()

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert canary not in str(excinfo.value)
    stored = ledger.get(CHANNEL, 100)
    assert stored is not None and stored["status"] == "failed_unknown"
    assert stored["usage"] is None
    assert stored["estimated_cost_usd"] == "unknown"
    assert canary not in path.read_bytes().decode("utf-8")
    assert ledger.summary()["unknown_blocked"] is True


@pytest.mark.parametrize("bad", [None, 123, "x", [], {}])
def test_malformed_decision_object_pure_unknown(tmp_path, bad):
    ledger, _ = make_ledger(tmp_path)
    state = make_state()

    async def _fn(_seen):
        return bad

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fn), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert ledger.get(CHANNEL, 100)["billing"] == "unknown"


def test_jev_flaky_getters_fail_closed_unknown_no_leak(tmp_path):
    canary = "CANARY_FLAKY_JEV_SECRET"
    ledger, path = make_ledger(tmp_path)
    state = make_state()

    class FlakyJevError(JevError):
        def __getattribute__(self, name):
            if name in ("usage", "unknown_billing"):
                raise ValueError(canary)
            return super().__getattribute__(name)

    async def _fail(_seen):
        raise FlakyJevError("timeout", detail="t", unknown_billing=True, usage=None)

    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fail), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert canary not in str(excinfo.value)
    assert canary not in path.read_bytes().decode("utf-8")


def test_lower_bound_counts_and_unknown_blocks_without_stop(tmp_path):
    path = ledger_path(tmp_path)
    ledger = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    state = make_state()

    async def _fail(_seen):
        raise JevError(
            "timeout",
            detail="t",
            unknown_billing=True,
            usage={"input_tokens": 1000, "output_tokens": 0},
        )

    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(_fail), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"
    assert ledger.summary()["unknown_blocked"] is True
    other = FakeAdapter(success_fn(make_state(tag="o", mid=101)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                other,
                make_state(tag="o", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED


def test_ledger_path_derives_delivery_parent_canonical_days(tmp_path):
    delivery_path = tmp_path / "delivery_state.json"
    ledger_file = delivery_path.parent / "participation.json"
    assert ledger_file.parent == delivery_path.parent
    assert ledger_file.parent != Path.cwd()
    ledger = ParticipationLedger(
        ledger_file, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05")
    )
    assert ledger.summary()["today"] == "2026-10-05"
    assert ledger.get(CHANNEL, 999) is None
    for bad_day in ("2026-1-5", "not-a-day", "", None, 20261005):
        bad = ParticipationLedger.__new__(ParticipationLedger)
        bad._path = ledger_file
        bad._native_sid = FAKE_SID
        bad._day_provider = DayCtl(bad_day)  # type: ignore[arg-type]
        with pytest.raises(ParticipationError):
            bad.summary()


# ---------------------------------------------------------------------------
# Fencing fixes: orphan started halt, single active paid, binding, attempt hex
# RED prepatch: orphan allowed (1 provider, unknown_blocked False), concurrent
# different keys both paid (2 calls), binding mismatch allowed (decided),
# attempt hyphen-36. See /tmp/red-proof.py output.
# ---------------------------------------------------------------------------


def test_orphan_started_blocks_new_zero_provider_across_rollover(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state100 = make_state()
    _inject_started(path, state100, mid=100)
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.summary()["started"] == 1
    assert restarted.summary()["unknown_blocked"] is True
    # NEW unrelated source (current previous deleted does not erase ambiguity).
    other = make_state(tag="o", mid=101)
    adapter = FakeAdapter(success_fn(other))
    before = path.read_bytes()
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                adapter, other, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert adapter.calls == 0
    assert path.read_bytes() == before
    assert restarted.get(CHANNEL, 101) is None
    # UTC rollover never clears the orphan halt.
    rolled = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-06"))
    assert rolled.summary()["unknown_blocked"] is True
    adapter2 = FakeAdapter(success_fn(make_state(tag="r", mid=102)))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            rolled.classify(
                adapter2,
                make_state(tag="r", mid=102),
                channel_id=CHANNEL,
                message_id=102,
                author_uid=AUTHOR,
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert adapter2.calls == 0


def test_cache_decided_allowed_despite_orphan_started(tmp_path):
    ledger, path = make_ledger(tmp_path)
    s100 = make_state(mid=100)
    first = asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(s100)),
            s100,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    assert first["status"] == "decided"
    s101 = make_state(tag="o", mid=101)
    _inject_started(path, s101, mid=101)
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.summary()["unknown_blocked"] is True
    cached_adapter = FakeAdapter(success_fn(s100))
    cached = asyncio.run(
        restarted.classify(
            cached_adapter, s100, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert cached == first
    assert cached_adapter.calls == 0
    s102 = make_state(tag="n", mid=102)
    new_adapter = FakeAdapter(success_fn(s102))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            restarted.classify(
                new_adapter, s102, channel_id=CHANNEL, message_id=102, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert new_adapter.calls == 0


def test_concurrent_different_keys_same_instance_single_paid(tmp_path):
    ledger, path = make_ledger(tmp_path)
    s1 = make_state(mid=100)
    s2 = make_state(tag="o", mid=101)
    release = asyncio.Event()
    entered = asyncio.Event()
    inner = FakeAdapter(success_fn(s1, usage={"input_tokens": 100, "output_tokens": 1}))

    async def _blocking(seen):
        entered.set()
        await release.wait()
        return await inner.decide(seen)

    async def _scenario():
        first_outer = FakeAdapter(_blocking)
        t1 = asyncio.ensure_future(
            ledger.classify(first_outer, s1, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
        await entered.wait()
        stored = json.loads(path.read_text(encoding="utf-8"))["attempts"]["7:100"]
        assert stored["status"] == "started"
        second = FakeAdapter(success_fn(s2, usage={"input_tokens": 100, "output_tokens": 1}))
        with pytest.raises(ParticipationPending) as excinfo:
            await ledger.classify(second, s2, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR)
        assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
        assert second.calls == 0
        assert first_outer.calls == 1
        assert inner.calls == 0
        release.set()
        r1 = await t1
        assert r1["status"] == "decided"
        assert inner.calls == 1
        retry = FakeAdapter(success_fn(s2, usage={"input_tokens": 100, "output_tokens": 1}))
        r2 = await ledger.classify(retry, s2, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR)
        assert r2["status"] == "decided"
        assert retry.calls == 1
        assert ledger.summary()["unknown_blocked"] is False

    asyncio.run(_scenario())


def test_concurrent_different_keys_separate_instances_single_paid(tmp_path):
    path = ledger_path(tmp_path)
    ledger1 = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    ledger2 = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    s1 = make_state(mid=100)
    s2 = make_state(tag="o", mid=101)
    release = asyncio.Event()
    entered = asyncio.Event()
    inner = FakeAdapter(success_fn(s1, usage={"input_tokens": 100, "output_tokens": 1}))

    async def _blocking(seen):
        entered.set()
        await release.wait()
        return await inner.decide(seen)

    async def _scenario():
        first_outer = FakeAdapter(_blocking)
        t1 = asyncio.ensure_future(
            ledger1.classify(first_outer, s1, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
        await entered.wait()
        second = FakeAdapter(success_fn(s2, usage={"input_tokens": 100, "output_tokens": 1}))
        with pytest.raises(ParticipationPending) as excinfo:
            await ledger2.classify(
                second, s2, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
            )
        assert excinfo.value.reason is ParticipationReason.UNKNOWN_BLOCKED
        assert second.calls == 0
        assert first_outer.calls == 1
        assert inner.calls == 0
        release.set()
        r1 = await t1
        assert r1["status"] == "decided"
        assert inner.calls == 1
        retry = FakeAdapter(success_fn(s2, usage={"input_tokens": 100, "output_tokens": 1}))
        r2 = await ledger2.classify(
            retry, s2, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
        )
        assert r2["status"] == "decided"
        assert retry.calls == 1

    asyncio.run(_scenario())


def test_binding_mismatch_mid_no_provider_no_write(tmp_path):
    canary = "CANARY_BODY_MID_MISMATCH_zzz111"
    ledger, path = make_ledger(tmp_path)
    state100 = make_state(mid=100)
    state100["messages"][2]["text"] = canary
    before = path.read_bytes()
    adapter = FakeAdapter(success_fn(state100))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(
                adapter, state100, channel_id=CHANNEL, message_id=101, author_uid=AUTHOR
            )
        )
    assert excinfo.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert adapter.calls == 0
    assert path.read_bytes() == before
    assert ledger.get(CHANNEL, 101) is None
    assert ledger.get(CHANNEL, 100) is None
    assert canary not in str(excinfo.value)
    assert canary not in path.read_bytes().decode("utf-8")


def test_binding_mismatch_author_no_provider_no_write(tmp_path):
    canary = "CANARY_BODY_AUTHOR_MISMATCH_xxx222"
    ledger, path = make_ledger(tmp_path)
    state = make_state(mid=100, author=2)
    state["messages"][2]["text"] = canary
    before = path.read_bytes()
    adapter = FakeAdapter(success_fn(state))
    with pytest.raises(ParticipationPending) as excinfo:
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=3)
        )
    assert excinfo.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert adapter.calls == 0
    assert path.read_bytes() == before
    assert ledger.get(CHANNEL, 100) is None
    assert canary not in str(excinfo.value)
    assert canary not in path.read_bytes().decode("utf-8")


def test_attempt_id_canonical_32hex_and_hyphen_rejected(tmp_path):
    ledger, path = make_ledger(tmp_path)
    state = make_state()
    record = asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    aid = record["attempt_id"]
    assert len(aid) == 32
    assert "-" not in aid
    assert all(c in "0123456789abcdef" for c in aid)
    assert uuid.UUID(aid).hex == aid
    assert "fingerprint" in record and len(record["fingerprint"]) == 64
    assert "context_sha256" not in record
    assert aid in path.read_text(encoding="utf-8")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["attempts"]["7:100"]["attempt_id"] = str(uuid.uuid4())
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ParticipationError, match="invalid attempt"):
        ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationError):
        ledger.get(CHANNEL, 100)


# ---------------------------------------------------------------------------
# Activation identity: immutable ledger_id (fresh-only uuid4.hex, no reset)
# RED prepatch: file had no ledger_id, no .ledger_id property; deleted
# already-paid file autocreated empty with cost 0 and allowed rebill.
# See /tmp/red-ledger-id-proof.py output. Whole integration pending Root +
# delivery jev_ledger_id marker (owner 789, router e523).
# ---------------------------------------------------------------------------


def test_ledger_id_fresh_canonical_unique_and_summary(tmp_path):
    ledger, path = make_ledger(tmp_path)
    lid = ledger.ledger_id
    assert isinstance(lid, str)
    assert len(lid) == 32
    assert "-" not in lid
    assert all(c in "0123456789abcdef" for c in lid)
    assert uuid.UUID(lid).hex == lid
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["ledger_id"] == lid
    assert ledger.summary()["ledger_id"] == lid
    ledger2, path2 = make_ledger(tmp_path, name="participation2.json")
    lid2 = ledger2.ledger_id
    assert uuid.UUID(lid2).hex == lid2
    assert lid2 != lid
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.ledger_id == lid
    assert restarted.summary()["ledger_id"] == lid
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_ledger_id_missing_fails_closed_no_migration(tmp_path):
    ledger, path = make_ledger(tmp_path)
    assert len(ledger.ledger_id) == 32
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["ledger_id"]
    path.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")
    bad_bytes = path.read_bytes()
    with pytest.raises(ParticipationError, match="ledger_id"):
        ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationError):
        ledger.get(CHANNEL, 100)
    with pytest.raises(ParticipationError):
        ledger.summary()
    with pytest.raises(ParticipationError):
        _ = ledger.ledger_id
    with pytest.raises(ParticipationError):
        asyncio.run(
            ledger.classify(
                FakeAdapter(success_fn(make_state(tag="o", mid=101))),
                make_state(tag="o", mid=101),
                channel_id=CHANNEL,
                message_id=101,
                author_uid=AUTHOR,
            )
        )
    assert path.read_bytes() == bad_bytes
    assert "ledger_id" not in json.loads(path.read_text(encoding="utf-8"))
    assert list(tmp_path.glob("*.tmp.*")) == []


@pytest.mark.parametrize(
    "bad",
    [
        None,
        123,
        "",
        "short",
        "a" * 31,
        "a" * 33,
        "A" * 32,
        "ABCDEF0123456789ABCDEF0123456789",
        str(uuid.uuid4()),
        "z" * 32,
        "g" * 32,
        [],
        {},
    ],
)
def test_ledger_id_strict_canonical_rejected(tmp_path, bad):
    ledger, path = make_ledger(tmp_path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["ledger_id"] = bad
    path.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")
    bad_bytes = path.read_bytes()
    with pytest.raises(ParticipationError, match="ledger_id"):
        ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationError):
        _ = ledger.ledger_id
    with pytest.raises(ParticipationError):
        ledger.get(CHANNEL, 100)
    assert path.read_bytes() == bad_bytes
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_ledger_id_property_reloads_no_cache_deleted_and_path_switch(tmp_path):
    ledger, path = make_ledger(tmp_path)
    lid = ledger.ledger_id
    ledger2, path2 = make_ledger(tmp_path, name="participation2.json")
    lid2 = ledger2.ledger_id
    assert lid != lid2
    path.unlink()
    with pytest.raises(ParticipationError, match="missing"):
        _ = ledger.ledger_id
    with pytest.raises(ParticipationError):
        ledger.get(CHANNEL, 100)
    # Private path switch reloads new authority immediately, never stale cache.
    ledger._path = path2
    assert ledger.ledger_id == lid2
    assert ledger.summary()["ledger_id"] == lid2
    ledger._path = path
    with pytest.raises(ParticipationError):
        _ = ledger.ledger_id
    # Standalone fresh allowed with NEW ID; Root must check delivery marker
    # before this autocreate (router e523 contract, delivery owner 789).
    fresh = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert fresh.ledger_id != lid
    assert fresh.summary()["attempts"] == 0
    assert fresh.summary()["ledger_id"] == fresh.ledger_id


def test_ledger_id_immutable_across_classify_and_unknown_keys(tmp_path):
    ledger, path = make_ledger(tmp_path)
    lid = ledger.ledger_id
    state = make_state()
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(state)),
            state,
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    assert ledger.ledger_id == lid
    assert json.loads(path.read_text(encoding="utf-8"))["ledger_id"] == lid
    # TMP helper preserves the legitimate activation identity.
    other = make_state(tag="o", mid=101)
    _inject_started(path, other, mid=101)
    assert json.loads(path.read_text(encoding="utf-8"))["ledger_id"] == lid
    restarted = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted.ledger_id == lid
    assert restarted.summary()["unknown_blocked"] is True
    # Unknown keys preserved alongside the immutable identity.
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["_future_top"] = {"note": "keep-me"}
    raw["attempts"]["7:100"]["_future_job"] = "keep-job"
    path.write_text(json.dumps(raw), encoding="utf-8")
    restarted2 = ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    assert restarted2.ledger_id == lid
    assert restarted2.get(CHANNEL, 100)["_future_job"] == "keep-job"
    assert json.loads(path.read_text(encoding="utf-8"))["_future_top"] == {"note": "keep-me"}


def test_ledger_id_wrong_sid_corrupt_symlink_no_reset(tmp_path):
    ledger, path = make_ledger(tmp_path, sid=FAKE_SID)
    lid = ledger.ledger_id
    before = path.read_bytes()
    with pytest.raises(ParticipationError, match="session"):
        ParticipationLedger(path, native_sid=OTHER_SID, day_provider=DayCtl("2026-10-05"))
    wrong = ParticipationLedger.__new__(ParticipationLedger)
    wrong._path = path
    wrong._native_sid = OTHER_SID
    wrong._day_provider = DayCtl("2026-10-05")
    with pytest.raises(ParticipationError):
        _ = wrong.ledger_id
    assert path.read_bytes() == before
    assert json.loads(before.decode("utf-8"))["ledger_id"] == lid
    link = tmp_path / "link.json"
    try:
        link.symlink_to(path)
    except OSError:
        pytest.skip("symlinks unavailable")
    linked = ParticipationLedger.__new__(ParticipationLedger)
    linked._path = link
    linked._native_sid = FAKE_SID
    linked._day_provider = DayCtl("2026-10-05")
    with pytest.raises(ParticipationError, match="symlink"):
        _ = linked.ledger_id
    assert path.read_bytes() == before
    path.write_text("not json{", encoding="utf-8")
    corrupt_bytes = path.read_bytes()
    with pytest.raises(ParticipationError, match="corrupt"):
        ParticipationLedger(path, native_sid=FAKE_SID, day_provider=DayCtl("2026-10-05"))
    with pytest.raises(ParticipationError):
        _ = ledger.ledger_id
    assert path.read_bytes() == corrupt_bytes
    assert list(tmp_path.glob("*.tmp.*")) == []


def test_ledger_id_readonly_property_no_setter(tmp_path):
    ledger, path = make_ledger(tmp_path)
    prop = type(ledger).ledger_id
    assert isinstance(prop, property)
    assert prop.fset is None
    before = path.read_bytes()
    with pytest.raises(AttributeError):
        ledger.ledger_id = "00" * 16  # type: ignore[misc]
    assert len(ledger.ledger_id) == 32
    assert path.read_bytes() == before
    assert "SENTINEL" not in ledger.ledger_id
