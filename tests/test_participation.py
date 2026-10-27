"""Offline tests for the DB-backed Jev participation ledger.

Fake/offline only: synthetic conversation states, fake adapters, TMP
database files, fake days. No network, no real keys, no paid calls, no
native/Discord access.

Coverage: decided-cache reuse (dup/restart, no second provider call),
context/author binding mismatch (zero provider calls), known/unknown
failure mapping, the crash-pending one-retry rule (attempts < 2 gets
exactly one more paid call; an unknown outcome at attempts == 2 resolves
terminally as observe -- never a third paid call), the global
failed_unknown/pending block with cached-decided exemption, exact decimal
cost reporting with no daily stop, and UTC-day cost separation.

Deleted (reasons): the ledger_id activation matrix, bind/marker/loss-guard
block, fsync/post-replace durability faults, symlink/permission guards,
unknown-key preservation, file-shape validators, separate-instances
concurrency, per-day spend stops, and adopt/retired-session bookkeeping --
the single-DB design deletes those concepts by construction.
"""

from __future__ import annotations

import asyncio
import copy

import pytest

from conversation import project_conversation_state
from jev import JEV_MODEL, JevDecision, JevError
from participation import (
    ParticipationError,
    ParticipationLedger,
    ParticipationPending,
    ParticipationReason,
)
from store import Store

CHANNEL = 7
AUTHOR = 2
DAY = "2026-10-05"
DAY2 = "2026-10-06"


def make_state(tag="", *, mid=100, author=2, **overrides):
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


def perfect_answers(*, participation="respond"):
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


class FakeAdapter:
    def __init__(self, fn):
        self._fn = fn
        self.calls = 0

    async def decide(self, state):
        self.calls += 1
        return await self._fn(state)


def success_fn(*, usage=None, participation="respond", model=JEV_MODEL):
    async def _fn(seen):
        return JevDecision(
            answers=copy.deepcopy(perfect_answers(participation=participation)),
            usage=dict(usage or {"input_tokens": 1000, "output_tokens": 5}),
            model=model,
            latency_ms=7,
        )

    return _fn


def raising_fn(exc):
    async def _fn(seen):
        raise exc

    return _fn


class DayCtl:
    def __init__(self, day):
        self.day = day

    def __call__(self):
        return self.day


def seed_db(db, channel=CHANNEL, mid=100, author=AUTHOR):
    """Append an outbox row + queued job so Jev rows have a parent job."""
    from server.core import build_message_entry

    fields = build_message_entry(
        scope_guild_id=9,
        guild_id=9,
        channel_id=channel,
        message_id=mid,
        author_uid=author,
        author_name=f"User{author}",
        content="HelperBot, please summarize",
        bot_uid=1,
        owner_uid=99,
        watch_channel_ids=[channel],
    )
    assert fields is not None
    with Store.open(db) as store:
        entry = store.append_entry(**fields)
        store.enqueue_job(channel, mid, entry["seq"])


def make_ledger(db, *, day=DAY):
    return ParticipationLedger(db, day_provider=DayCtl(day))


# ---------------------------------------------------------------------------
# Decided cache: no second provider call (dup, restart)
# ---------------------------------------------------------------------------


def test_decided_cache_no_second_provider_call(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    adapter = FakeAdapter(success_fn())
    state = make_state()
    first = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert first["status"] == "decided"
    assert first["participation"] == "respond"
    assert adapter.calls == 1
    second = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert second == first
    assert adapter.calls == 1


def test_decided_cache_survives_restart(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    adapter = FakeAdapter(success_fn())
    state = make_state()
    asyncio.run(
        make_ledger(db).classify(
            adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert adapter.calls == 1
    # Fresh ledger instance over the same DB: cache hit, zero new calls.
    adapter2 = FakeAdapter(success_fn())
    record = asyncio.run(
        make_ledger(db).classify(
            adapter2, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"
    assert adapter2.calls == 0


def test_binding_mismatch_never_pays(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    adapter = FakeAdapter(success_fn())
    asyncio.run(
        ledger.classify(
            adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert adapter.calls == 1
    # Different author for the same message: no provider call, ever.
    adapter2 = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter2,
                make_state(author=3),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=3,
            )
        )
    assert exc_info.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert adapter2.calls == 0
    # Edited snapshot (different fingerprint): same, no re-pay.
    edited = make_state()
    edited["messages"][-1] = dict(edited["messages"][-1], text="changed text")
    adapter3 = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(adapter3, edited, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert exc_info.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert adapter3.calls == 0


# ---------------------------------------------------------------------------
# Crash-pending one-retry rule (§5.2)
# ---------------------------------------------------------------------------


def test_crashed_pending_gets_exactly_one_retry(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    state = make_state()
    snapshot = project_conversation_state(state)
    import hashlib as _hashlib
    import json as _json

    fingerprint = _hashlib.sha256(
        _json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    # Simulate the crash: pending marker committed, resolve never ran.
    with Store.open(db) as store:
        store.insert_jev_pending(
            CHANNEL,
            100,
            context_sha256=fingerprint,
            attempt_id="att1",
            author_uid=AUTHOR,
            attempted_day=DAY,
        )
    ledger = make_ledger(db)
    adapter = FakeAdapter(success_fn())
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["status"] == "decided"
    assert record["participation"] == "respond"
    assert record["attempts"] == 2
    assert adapter.calls == 1
    # And now it is cached: no further calls.
    adapter2 = FakeAdapter(success_fn())
    asyncio.run(
        ledger.classify(adapter2, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert adapter2.calls == 0


def test_generic_adapter_crash_resolves_failed_unknown_job_stays_queued(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    state = make_state()
    ledger = make_ledger(db)
    # First attempt crashes generically (unknown outcome).
    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(RuntimeError("boom"))),
                state,
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    with Store.open(db) as store:
        assert store.get_jev(CHANNEL, 100)["status"] == "failed_unknown"
    # Hmm: a generic first-attempt failure resolves failed_unknown directly
    # (the attempt ran to completion inside this process). The retry path
    # applies to a *crashed pending* (attempt interrupted before resolve).
    with Store.open(db) as store:
        job = store.get_job(CHANNEL, 100)
        assert job["stage"] == "queued"  # failed_unknown never terminalizes the job


def test_retry_path_second_unknown_is_observe(tmp_path):
    """Crashed pending -> 1 retry -> unknown outcome => terminal observe."""
    db = tmp_path / "state.db"
    seed_db(db)
    state = make_state()
    snapshot = project_conversation_state(state)
    import hashlib as _hashlib
    import json as _json

    fingerprint = _hashlib.sha256(
        _json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with Store.open(db) as store:
        store.insert_jev_pending(
            CHANNEL,
            100,
            context_sha256=fingerprint,
            attempt_id="att1",
            author_uid=AUTHOR,
            attempted_day=DAY,
        )
    ledger = make_ledger(db)
    adapter = FakeAdapter(raising_fn(RuntimeError("boom during retry")))
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert adapter.calls == 1  # exactly the one retry
    assert record["status"] == "decided"
    assert record["participation"] == "observe"
    with Store.open(db) as store:
        job = store.get_job(CHANNEL, 100)
        assert (job["stage"], job["outcome"]) == ("done", "observed")
    # Terminal: a third classify reuses the decided observe row, zero calls.
    adapter2 = FakeAdapter(success_fn())
    again = asyncio.run(
        ledger.classify(adapter2, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert again["participation"] == "observe"
    assert adapter2.calls == 0


def test_attempts_two_pending_never_pays_again(tmp_path):
    """Crash between the 2nd call and its resolve: observe, zero new calls."""
    db = tmp_path / "state.db"
    seed_db(db)
    state = make_state()
    ledger = make_ledger(db)
    adapter = FakeAdapter(raising_fn(RuntimeError("first crash")))
    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    # Simulate: the retry ran and crashed before resolving (attempts == 2,
    # still pending) -- rewrite the row to that shape via the store.
    with Store.open(db) as store:
        row = store.get_jev(CHANNEL, 100)
        assert row["status"] == "failed_unknown"  # first crash resolved unknown
    # Fresh crash-pending at attempts == 2: craft directly.
    seed_db(db, mid=200)
    state2 = make_state(mid=200)
    snapshot = project_conversation_state(state2)
    import hashlib as _hashlib
    import json as _json

    fingerprint = _hashlib.sha256(
        _json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with Store.open(db) as store:
        store.insert_jev_pending(
            CHANNEL,
            200,
            context_sha256=fingerprint,
            attempt_id="att1",
            author_uid=AUTHOR,
            attempted_day=DAY,
        )
        store.bump_jev_retry(
            CHANNEL,
            200,
            old_attempt_id="att1",
            new_attempt_id="att2",
            attempted_day=DAY,
        )
    adapter2 = FakeAdapter(success_fn())
    record = asyncio.run(
        make_ledger(db).classify(
            adapter2, state2, channel_id=CHANNEL, message_id=200, author_uid=AUTHOR
        )
    )
    assert record["participation"] == "observe"
    assert adapter2.calls == 0  # never a third paid call
    with Store.open(db) as store:
        job = store.get_job(CHANNEL, 200)
        assert (job["stage"], job["outcome"]) == ("done", "observed")


# ---------------------------------------------------------------------------
# Failure mapping + global block
# ---------------------------------------------------------------------------


def test_known_failure_never_retries_but_never_blocks_others(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    ledger = make_ledger(db)
    known = JevError(kind="validation", unknown_billing=False)
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(known)),
                make_state(),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    # Same source retries raise without a new provider call.
    adapter = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    assert adapter.calls == 0
    # A known failure never blocks a NEW source.
    adapter2 = FakeAdapter(success_fn())
    record = asyncio.run(
        ledger.classify(
            adapter2,
            make_state(mid=200),
            channel_id=CHANNEL,
            message_id=200,
            author_uid=AUTHOR,
        )
    )
    assert record["status"] == "decided"
    assert adapter2.calls == 1


def test_unknown_failure_blocks_new_but_cache_still_returns(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    ledger = make_ledger(db)
    unknown = JevError(kind="timeout", unknown_billing=True)
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(unknown)),
                make_state(),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    # New source: globally blocked, zero provider calls.
    adapter = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter,
                make_state(mid=200),
                channel_id=CHANNEL,
                message_id=200,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert adapter.calls == 0
    # A third source is blocked the same way -- nothing paid while halted.
    # (Cached decided rows still return; covered by the next test.)
    seed_db(db, mid=300)
    adapter_ok = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter_ok,
                make_state(mid=300),
                channel_id=CHANNEL,
                message_id=300,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert adapter_ok.calls == 0


def test_decided_cache_returns_despite_global_block(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    ledger = make_ledger(db)
    state = make_state()
    first_adapter = FakeAdapter(success_fn())
    asyncio.run(
        ledger.classify(first_adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert first_adapter.calls == 1
    # Another key fails unknown -> global block ...
    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(JevError(kind="timeout", unknown_billing=True))),
                make_state(mid=200),
                channel_id=CHANNEL,
                message_id=200,
                author_uid=AUTHOR,
            )
        )
    # ... but the decided row still returns with zero provider calls.
    adapter = FakeAdapter(success_fn(participation="observe"))
    record = asyncio.run(
        ledger.classify(adapter, state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["participation"] == "respond"  # cached, not the new fake
    assert adapter.calls == 0


def test_unknown_with_usage_keeps_lower_bound_cost(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    exc = JevError(
        kind="timeout", unknown_billing=True, usage={"input_tokens": 1_000_000, "output_tokens": 0}
    )
    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(exc)),
                make_state(),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert ledger.summary()["known_cost_today_usd"] == "0.042"


def test_known_zero_cost_failure(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    exc = JevError(kind="auth", unknown_billing=False)
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(exc)),
                make_state(),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    assert ledger.summary()["known_cost_today_usd"] == "0"


def test_malformed_decision_is_known_failure(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    adapter = FakeAdapter(success_fn(model="jev-9.9.9"))
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_KNOWN
    assert adapter.calls == 1  # the call happened; the shape failed


def test_cancellation_first_attempt_fails_unknown_and_propagates(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)

    async def _cancel(seen):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            ledger.classify(
                FakeAdapter(_cancel),
                make_state(),
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    with Store.open(db) as store:
        assert store.get_jev(CHANNEL, 100)["status"] == "failed_unknown"
    adapter = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
            )
        )
    assert exc_info.value.reason is ParticipationReason.ALREADY_FAILED_UNKNOWN
    assert adapter.calls == 0


# ---------------------------------------------------------------------------
# Cost reporting: exact decimals, no stop, UTC-day separation
# ---------------------------------------------------------------------------


def test_exact_decimal_cost_and_no_daily_stop(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    ledger = make_ledger(db)
    usage = {"input_tokens": 1_000_000, "output_tokens": 0}
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(usage=usage)),
            make_state(),
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn(usage=usage)),
            make_state(mid=200),
            channel_id=CHANNEL,
            message_id=200,
            author_uid=AUTHOR,
        )
    )
    assert ledger.summary() == {
        "today": DAY,
        "known_cost_today_usd": "0.084",
        "unknown_blocked": False,
    }


def test_cost_separated_by_day(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    usage = {"input_tokens": 1_000_000, "output_tokens": 0}
    asyncio.run(
        make_ledger(db, day=DAY).classify(
            FakeAdapter(success_fn(usage=usage)),
            make_state(),
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    assert make_ledger(db, day=DAY).summary()["known_cost_today_usd"] == "0.042"
    assert make_ledger(db, day=DAY2).summary()["known_cost_today_usd"] == "0"
    asyncio.run(
        make_ledger(db, day=DAY2).classify(
            FakeAdapter(success_fn(usage=usage)),
            make_state(mid=200),
            channel_id=CHANNEL,
            message_id=200,
            author_uid=AUTHOR,
        )
    )
    assert make_ledger(db, day=DAY2).summary()["known_cost_today_usd"] == "0.042"
    assert make_ledger(db, day=DAY).summary()["known_cost_today_usd"] == "0.042"


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def test_classify_rejects_bad_ids_adapter_snapshot_day(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    state = make_state()
    adapter = FakeAdapter(success_fn())
    for bad in (0, -1, True, "100", None):
        with pytest.raises(ParticipationError):
            asyncio.run(
                ledger.classify(adapter, state, channel_id=bad, message_id=100, author_uid=AUTHOR)
            )
        with pytest.raises(ParticipationError):
            asyncio.run(
                ledger.classify(
                    adapter, state, channel_id=CHANNEL, message_id=bad, author_uid=AUTHOR
                )
            )
    with pytest.raises(ParticipationError):
        asyncio.run(
            ledger.classify(object(), state, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    with pytest.raises(ParticipationError):
        asyncio.run(
            ledger.classify(
                adapter,
                {"bogus": True},
                channel_id=CHANNEL,
                message_id=100,
                author_uid=AUTHOR,
            )
        )
    assert adapter.calls == 0
    bad_day = ParticipationLedger(db, day_provider=lambda: "not-a-day")
    with pytest.raises(ParticipationError):
        bad_day.summary()


def test_get_returns_detached_record(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    ledger = make_ledger(db)
    assert ledger.get(CHANNEL, 100) is None
    asyncio.run(
        ledger.classify(
            FakeAdapter(success_fn()),
            make_state(),
            channel_id=CHANNEL,
            message_id=100,
            author_uid=AUTHOR,
        )
    )
    record = ledger.get(CHANNEL, 100)
    assert record is not None
    assert record["status"] == "decided"
    assert record["context_sha256"] is not None
    record["status"] = "mutated"
    assert ledger.get(CHANNEL, 100)["status"] == "decided"


# --- Lead decisions D1/D3/D4/D5 ----------------------------------------------


def _pending_row(db, ch, mid, *, author=AUTHOR, day=DAY, fp=None, attempt="att1"):
    if fp is None:
        import hashlib as _hashlib
        import json as _json

        snapshot = project_conversation_state(make_state(mid=mid, author=author))
        fp = _hashlib.sha256(
            _json.dumps(
                snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
    with Store.open(db) as store:
        return store.insert_jev_pending(
            ch,
            mid,
            context_sha256=fp,
            attempt_id=attempt,
            author_uid=author,
            attempted_day=day,
        )


def test_d1_retry_fires_despite_unrelated_failed_unknown(tmp_path):
    """Same-key pending retry runs BEFORE the cross-key block scan."""
    db = tmp_path / "state.db"
    seed_db(db)
    seed_db(db, mid=200)
    seed_db(db, mid=300)
    # Unrelated key fails unknown -> global block is live ...
    ledger = make_ledger(db)
    with pytest.raises(ParticipationPending):
        asyncio.run(
            ledger.classify(
                FakeAdapter(raising_fn(JevError(kind="timeout", unknown_billing=True))),
                make_state(mid=200),
                channel_id=CHANNEL,
                message_id=200,
                author_uid=AUTHOR,
            )
        )
    # ... yet the crashed-pending key still gets its one retry ...
    _pending_row(db, CHANNEL, 100)
    adapter = FakeAdapter(success_fn())
    record = asyncio.run(
        ledger.classify(
            adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"
    assert record["attempts"] == 2
    assert adapter.calls == 1
    # ... while a FRESH key in the same DB stays blocked.
    adapter2 = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(
                adapter2,
                make_state(mid=300),
                channel_id=CHANNEL,
                message_id=300,
                author_uid=AUTHOR,
            )
        )
    assert exc_info.value.reason is ParticipationReason.UNKNOWN_BLOCKED
    assert adapter2.calls == 0


def test_d3_terminal_observe_keeps_retry_usage_and_cost(tmp_path):
    """Synthetic observe: decided/observe, answers NULL, usage/cost kept."""
    db = tmp_path / "state.db"
    seed_db(db)
    _pending_row(db, CHANNEL, 100)
    ledger = make_ledger(db)
    usage = {"input_tokens": 1_000_000, "output_tokens": 7}
    exc = JevError(kind="timeout", unknown_billing=True, usage=dict(usage))
    adapter = FakeAdapter(raising_fn(exc))
    record = asyncio.run(
        ledger.classify(
            adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert adapter.calls == 1
    assert record["status"] == "decided"
    assert record["participation"] == "observe"
    assert record["answers_json"] is None
    assert record["input_tokens"] == 1_000_000
    assert record["output_tokens"] == 7
    # Lower-bound cost survives the unknown outcome.
    assert ledger.summary()["known_cost_today_usd"] == "0.042"
    # Decided cache: no further calls, ever.
    adapter2 = FakeAdapter(success_fn())
    again = asyncio.run(
        ledger.classify(
            adapter2, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert again["participation"] == "observe"
    assert adapter2.calls == 0


def test_d4_retry_bump_refreshes_billing_day(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    _pending_row(db, CHANNEL, 100, day=DAY)
    other_day = "2026-10-06"
    ledger = ParticipationLedger(db, day_provider=DayCtl(other_day))
    adapter = FakeAdapter(success_fn())
    record = asyncio.run(
        ledger.classify(
            adapter, make_state(), channel_id=CHANNEL, message_id=100, author_uid=AUTHOR
        )
    )
    assert record["status"] == "decided"
    assert record["attempted_day"] == other_day
    assert ledger.summary()["known_cost_today_usd"] == "0.000042"  # 1000 tokens
    assert make_ledger(db, day=DAY).summary()["known_cost_today_usd"] == "0"


def test_d5_attempts_two_observes_despite_fingerprint_drift(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    _pending_row(db, CHANNEL, 100)
    with Store.open(db) as store:
        store.bump_jev_retry(
            CHANNEL,
            100,
            old_attempt_id="att1",
            new_attempt_id="att2",
            attempted_day=DAY,
        )
    ledger = make_ledger(db)
    drifted = make_state()
    drifted["messages"][-1] = dict(drifted["messages"][-1], text="edited after crash")
    adapter = FakeAdapter(success_fn())
    record = asyncio.run(
        ledger.classify(adapter, drifted, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
    )
    assert record["participation"] == "observe"
    assert adapter.calls == 0  # never a third paid call, even on drift


def test_d5_first_retry_never_pays_on_drift(tmp_path):
    db = tmp_path / "state.db"
    seed_db(db)
    _pending_row(db, CHANNEL, 100)
    ledger = make_ledger(db)
    drifted = make_state()
    drifted["messages"][-1] = dict(drifted["messages"][-1], text="edited after crash")
    adapter = FakeAdapter(success_fn())
    with pytest.raises(ParticipationPending) as exc_info:
        asyncio.run(
            ledger.classify(adapter, drifted, channel_id=CHANNEL, message_id=100, author_uid=AUTHOR)
        )
    assert exc_info.value.reason is ParticipationReason.CONTEXT_MISMATCH
    assert adapter.calls == 0  # deferred: no payment, no resolution
    with Store.open(db) as store:
        row = store.get_jev(CHANNEL, 100)
        assert row["status"] == "pending"
        assert row["attempts"] == 1
