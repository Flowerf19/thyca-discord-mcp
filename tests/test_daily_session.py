"""Offline tests for daily native-session rotation (tmp files only).

Covers the rotation subsystem end to end with no live Discord/Thyca:
``sessions`` day/SID helpers, ``ThycaClient.create_session`` (fake
transport), the delivery + participation ``rotate_native_session``
transitions, the mapping registry load/save, ``rotate_to_today``
orchestration, and the responder tick gate (adopt/drain-gated/lazy).
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import delivery
import participation
import sessions
from config import Settings
from delivery import DeliveryError, initialize_delivery_state
from outbox import append_entry
from participation import ParticipationError, ParticipationLedger
from responder import Responder, ResponderError
from responder.rotation import (
    RotationDeferred,
    adopt_session_day,
    build_rotation_history,
    derive_session_path,
    load_session_registry,
    rotate_to_today,
    save_session_registry,
)
from thyca import (
    ThycaBusyError,
    ThycaClient,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
)

OLD_SID = "2026-10-05T01-59-55_0f25"
NEW_SID = "2026-10-07T04-00-01_a1b2"
REUSE_SID = "2026-10-07T05-30-00_c3d4"
OTHER_SID = "2026-10-06T02-03-04_d5e6"
DAY_OLD = "2026-10-05"
DAY_NEW = "2026-10-07"
BASE = "http://127.0.0.1:8765"

FAKE_GUILD = 100000000000000001
FAKE_CHANNEL = 200000000000000002
FAKE_OWNER = 300000000000000003
FAKE_BOT = 400000000000000004
FAKE_OTHER = 500000000000000005


# -- sessions.py: SID/day/history validation --------------------------------


def test_validate_native_sid_accepts_strict_grammar_only():
    assert sessions.validate_native_sid(OLD_SID) == OLD_SID
    for bad in (
        "not-a-sid",
        "",
        "2026-10-05T01-59-55_0F25",  # uppercase hex rejected
        "2026-10-05T01-59-55_0f2",  # short suffix
        "2026-10-05T01-59-55_0f256",  # long suffix
        "2026-10-05 01-59-55_0f25",  # space, not T
        " 2026-10-05T01-59-55_0f25",
        None,
        123,
        b"2026-10-05T01-59-55_0f25",
    ):
        assert sessions.validate_native_sid(bad) is None


def test_validate_day_requires_real_calendar_date():
    assert sessions.validate_day("2026-10-07") == "2026-10-07"
    assert sessions.validate_day("2024-02-29") == "2024-02-29"  # leap day ok
    for bad in (
        "2026-02-30",  # shape ok, not a real date
        "2026-13-01",
        "2026-10-7",
        "26-10-07",
        "2026/10/07",
        "",
        None,
        20261007,
    ):
        assert sessions.validate_day(bad) is None


def test_validate_sessions_history_returns_detached_copy():
    raw = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    clean = sessions.validate_sessions_history(raw)
    assert clean == raw
    clean[DAY_NEW] = OTHER_SID
    assert raw[DAY_NEW] == NEW_SID  # detached, original untouched
    assert sessions.validate_sessions_history({}) == {}
    for bad in (
        {"2026-02-30": OLD_SID},
        {DAY_OLD: "not-a-sid"},
        {123: OLD_SID},
        [(DAY_OLD, OLD_SID)],
        None,
        "x",
    ):
        assert sessions.validate_sessions_history(bad) is None


def test_parse_tz_name_strict():
    assert sessions.parse_tz_name("Asia/Ho_Chi_Minh") == ZoneInfo("Asia/Ho_Chi_Minh")
    assert sessions.parse_tz_name("UTC") == ZoneInfo("UTC")
    for bad in ("Mars/Olympus", "", None, 123):
        assert sessions.parse_tz_name(bad) is None


def test_logical_day_rollover_boundary_vietnam():
    tz = "Asia/Ho_Chi_Minh"
    # 03:59 local still belongs to the previous logical day.
    assert (
        sessions.logical_day(
            datetime(2026, 10, 7, 3, 59, 59, tzinfo=ZoneInfo(tz)),
            tz_name=tz,
            rollover_hour=4,
        )
        == "2026-10-06"
    )
    # 04:00 local flips to the new logical day.
    assert (
        sessions.logical_day(
            datetime(2026, 10, 7, 4, 0, 0, tzinfo=ZoneInfo(tz)),
            tz_name=tz,
            rollover_hour=4,
        )
        == "2026-10-07"
    )


def test_logical_day_converts_utc_clock_to_configured_zone():
    # 2026-10-06T20:59:59Z == 2026-10-07T03:59:59+07 -> previous day.
    assert (
        sessions.logical_day(
            datetime(2026, 10, 6, 20, 59, 59, tzinfo=timezone.utc),
            tz_name="Asia/Ho_Chi_Minh",
            rollover_hour=4,
        )
        == "2026-10-06"
    )
    # 2026-10-06T21:00:00Z == 2026-10-07T04:00:00+07 -> new day.
    assert (
        sessions.logical_day(
            datetime(2026, 10, 6, 21, 0, 0, tzinfo=timezone.utc),
            tz_name="Asia/Ho_Chi_Minh",
            rollover_hour=4,
        )
        == "2026-10-07"
    )


def test_logical_day_custom_zone_and_midnight_rollover():
    assert (
        sessions.logical_day(
            datetime(2026, 10, 7, 0, 0, 0, tzinfo=timezone.utc),
            tz_name="UTC",
            rollover_hour=0,
        )
        == "2026-10-07"
    )
    assert (
        sessions.logical_day(
            datetime(2026, 10, 6, 23, 59, 59, tzinfo=timezone.utc),
            tz_name="UTC",
            rollover_hour=0,
        )
        == "2026-10-06"
    )


def test_logical_day_rejects_bad_clock_config():
    now = datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone-aware"):
        sessions.logical_day(datetime(2026, 10, 7, 12, 0, 0), tz_name="UTC", rollover_hour=0)
    with pytest.raises(ValueError, match="IANA"):
        sessions.logical_day(now, tz_name="Mars/Olympus", rollover_hour=0)
    for bad_hour in (-1, 24, True, False, 4.0, "4", None):
        with pytest.raises(ValueError, match="rollover_hour"):
            sessions.logical_day(now, tz_name="UTC", rollover_hour=bad_hour)


def test_default_session_day_provider_validates_eagerly_and_returns_day():
    with pytest.raises(ValueError, match="IANA"):
        sessions.default_session_day_provider("Mars/Olympus", 4)
    with pytest.raises(ValueError, match="rollover_hour"):
        sessions.default_session_day_provider("UTC", 24)
    provider = sessions.default_session_day_provider("Asia/Ho_Chi_Minh", 4)
    assert sessions.validate_day(provider()) is not None


def test_sid_day_guess_uses_timestamp_prefix_only():
    assert sessions.sid_day_guess(OLD_SID) == "2026-10-05"
    assert sessions.sid_day_guess("not-a-sid") is None
    assert sessions.sid_day_guess(None) is None


# -- Settings: rotation clock config -----------------------------------------


def _settings(tmp_path, **overrides):
    kwargs: dict = {
        "discord_token": "fake-token",
        "owner_uid": FAKE_OWNER,
        "guild_id": FAKE_GUILD,
        "default_channel_id": FAKE_CHANNEL,
        "session_id": "discord-main",
        "thyca_session_mapping_path": tmp_path / "mapping.json",
        "delivery_state_path": tmp_path / "delivery.json",
        "outbox_path": tmp_path / "outbox.jsonl",
        "outbox_state_path": tmp_path / "outbox_state.json",
        "jev_state_path": tmp_path / "jev_state.json",
        "_env_file": None,
    }
    kwargs.update(overrides)
    return Settings(**kwargs)


def test_rotation_settings_defaults(tmp_path):
    settings = _settings(tmp_path)
    assert settings.daily_session_rotation is True
    assert settings.session_day_timezone == "Asia/Ho_Chi_Minh"
    assert settings.session_day_rollover_hour == 4


def test_rotation_settings_accept_explicit_values(tmp_path):
    settings = _settings(
        tmp_path,
        daily_session_rotation=False,
        session_day_timezone="UTC",
        session_day_rollover_hour=0,
    )
    assert settings.daily_session_rotation is False
    assert settings.session_day_timezone == "UTC"
    assert settings.session_day_rollover_hour == 0


def test_rotation_settings_reject_bad_clock(tmp_path):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _settings(tmp_path, session_day_timezone="Mars/Olympus")
    for bad_hour in (24, -1, True, 4.0):
        with pytest.raises(ValidationError):
            _settings(tmp_path, session_day_rollover_hour=bad_hour)


def test_rotation_settings_env_coercion(tmp_path, monkeypatch):
    monkeypatch.setenv("SESSION_DAY_ROLLOVER_HOUR", "6")
    monkeypatch.setenv("DAILY_SESSION_ROTATION", "false")
    settings = _settings(tmp_path)
    assert settings.session_day_rollover_hour == 6
    assert settings.daily_session_rotation is False


# -- ThycaClient.create_session ------------------------------------------------


class FakeTransport:
    def __init__(self, handler):
        self.handler = handler
        self.calls: list = []

    async def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return await self.handler(method, url, body)


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def _blank_detail(sid: str) -> dict:
    return {
        "id": sid,
        "title": f"session {sid}",
        "model": "owner-default",
        "messages": [],
        "running": False,
    }


async def test_create_session_posts_empty_json_and_returns_metadata_only():
    async def _handler(method, url, body):
        assert method == "POST"
        assert url == f"{BASE}/api/sessions"
        assert body == b"{}"
        return 200, _json_bytes(_blank_detail(NEW_SID))

    client = ThycaClient(BASE, transport=FakeTransport(_handler))
    out = await client.create_session()
    assert out == {"native_sid": NEW_SID, "message_count": 0, "proof": "session_create_blank"}
    assert f"session {NEW_SID}" not in json.dumps(out)  # native title dropped locally


async def test_create_session_rejects_nonblank_or_malformed_detail():
    async def _handler_for(payload):
        async def _handler(method, url, body):
            return 200, _json_bytes(payload)

        return _handler

    bad_messages = _blank_detail(NEW_SID)
    bad_messages["messages"] = [{"role": "user", "content": "hi"}]
    running = _blank_detail(NEW_SID)
    running["running"] = True
    bad_id = _blank_detail("not-a-sid")
    cases = [bad_messages, running, bad_id, {"id": NEW_SID}, [NEW_SID]]
    for payload in cases:
        client = ThycaClient(BASE, transport=FakeTransport(await _handler_for(payload)))
        with pytest.raises(ThycaProtocolError):
            await client.create_session()


async def test_create_session_status_mapping_mirrors_turn_post():
    async def _status_handler(status):
        async def _handler(method, url, body):
            return status, b""

        return _handler

    cases = [
        (409, ThycaBusyError),
        (400, ThycaPreconditionError),
        (404, ThycaPreconditionError),
        (500, ThycaProtocolError),
        (302, ThycaProtocolError),
    ]
    for status, exc in cases:
        client = ThycaClient(BASE, transport=FakeTransport(await _status_handler(status)))
        with pytest.raises(exc):
            await client.create_session()


async def test_create_session_transport_outcome_is_uncertain():
    async def _timeout(method, url, body):
        raise TimeoutError("slow native")

    async def _disconnect(method, url, body):
        raise OSError("gone")

    for handler in (_timeout, _disconnect):
        client = ThycaClient(BASE, transport=FakeTransport(handler))
        with pytest.raises(ThycaSubmissionUncertainError):
            await client.create_session()


# -- delivery.rotate_native_session --------------------------------------------


def _init_delivery(path, sid=OLD_SID):
    return initialize_delivery_state(
        path,
        native_session_id=sid,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )


def test_delivery_rotate_empty_journal_adopts_new_sid(tmp_path):
    path = tmp_path / "delivery.json"
    _init_delivery(path)
    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    snap = delivery.rotate_native_session(
        path, expected_current=OLD_SID, new_sid=NEW_SID, day=DAY_NEW, history=history
    )
    assert snap["native_session_id"] == NEW_SID
    assert snap["native_sessions"] == history
    reloaded = delivery.load_delivery_state(path)
    assert reloaded["native_session_id"] == NEW_SID
    assert reloaded["native_sessions"] == history


def test_delivery_rotate_allows_safe_terminal_jobs(tmp_path):
    path = tmp_path / "delivery.json"
    _init_delivery(path)
    delivery.enqueue_job(
        path,
        channel_id=FAKE_CHANNEL,
        message_id=11,
        author_uid=FAKE_OWNER,
        actor_kind="owner",
        seq=1,
    )
    delivery.skip_job(path, FAKE_CHANNEL, 11, reason="bot_filtered")
    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    snap = delivery.rotate_native_session(
        path, expected_current=OLD_SID, new_sid=NEW_SID, day=DAY_NEW, history=history
    )
    assert snap["native_session_id"] == NEW_SID
    assert delivery.get_job(path, FAKE_CHANNEL, 11)["stage"] == "skipped"


def test_delivery_rotate_blocked_by_queued_and_uncertain(tmp_path):
    for stage_setup in ("queued", "uncertain"):
        path = tmp_path / f"delivery_{stage_setup}.json"
        _init_delivery(path)
        delivery.enqueue_job(
            path,
            channel_id=FAKE_CHANNEL,
            message_id=21,
            author_uid=FAKE_OWNER,
            actor_kind="owner",
            seq=1,
        )
        if stage_setup == "uncertain":
            delivery.mark_uncertain(path, FAKE_CHANNEL, 21, kind="timeout")
        history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
        with pytest.raises(DeliveryError, match="blocks rotation"):
            delivery.rotate_native_session(
                path, expected_current=OLD_SID, new_sid=NEW_SID, day=DAY_NEW, history=history
            )
        assert delivery.load_delivery_state(path)["native_session_id"] == OLD_SID


def test_delivery_rotate_rejects_mismatch_and_bad_args(tmp_path):
    path = tmp_path / "delivery.json"
    _init_delivery(path)
    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    mismatch_history = {DAY_OLD: OLD_SID, "2026-10-06": OTHER_SID, DAY_NEW: NEW_SID}
    with pytest.raises(DeliveryError, match="native session mismatch"):
        delivery.rotate_native_session(
            path,
            expected_current=OTHER_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history=mismatch_history,
        )
    with pytest.raises(DeliveryError, match="invalid new_sid"):
        delivery.rotate_native_session(
            path, expected_current=OLD_SID, new_sid="bad", day=DAY_NEW, history=history
        )
    with pytest.raises(DeliveryError, match="invalid day"):
        delivery.rotate_native_session(
            path, expected_current=OLD_SID, new_sid=NEW_SID, day="2026-02-30", history=history
        )
    # History must bind the new day to the new SID.
    with pytest.raises(DeliveryError, match="does not bind day"):
        delivery.rotate_native_session(
            path,
            expected_current=OLD_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history={DAY_OLD: OLD_SID, DAY_NEW: OTHER_SID},
        )
    # History must keep the outgoing SID in provenance.
    with pytest.raises(DeliveryError, match="drops the current"):
        delivery.rotate_native_session(
            path,
            expected_current=OLD_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history={DAY_NEW: NEW_SID},
        )
    assert delivery.load_delivery_state(path)["native_session_id"] == OLD_SID


def test_delivery_state_history_field_backward_compatible(tmp_path):
    path = tmp_path / "delivery.json"
    _init_delivery(path)
    # Fresh files carry an empty history.
    assert delivery.load_delivery_state(path)["native_sessions"] == {}
    # Pre-rotation files without the key load as empty.
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["native_sessions"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert delivery.load_delivery_state(path)["native_sessions"] == {}
    # A corrupt history fails closed.
    raw["native_sessions"] = {DAY_NEW: "bad-sid"}
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DeliveryError, match="invalid native_sessions"):
        delivery.load_delivery_state(path)


# -- participation.rotate_native_session ---------------------------------------


def _failed_known_record(sid: str, channel: int, mid: int, author: int) -> dict:
    from jev import JEV_MODEL

    return {
        "native_session_id": sid,
        "channel_id": channel,
        "message_id": mid,
        "author_uid": author,
        "fingerprint": "ab" * 32,
        "model": JEV_MODEL,
        "attempt_id": uuid.uuid4().hex,
        "attempted_day": DAY_OLD,
        "attempted_at": "2026-10-05T01:00:00+00:00",
        "status": "failed_known",
        "usage": {"input_tokens": 0, "output_tokens": 0},
        "estimated_cost_usd": "0",
        "billing": "known",
        "participation": None,
        "answers": None,
    }


def _init_ledger(path, sid=OLD_SID):
    return ParticipationLedger(path, native_sid=sid)


def test_ledger_rotate_preserves_records_and_identity(tmp_path):
    path = tmp_path / "jev_state.json"
    ledger = _init_ledger(path)
    ledger_id = ledger.ledger_id
    raw = json.loads(path.read_text(encoding="utf-8"))
    key = f"{FAKE_CHANNEL}:{FAKE_OWNER}"
    raw["attempts"][key] = _failed_known_record(OLD_SID, FAKE_CHANNEL, FAKE_OWNER, FAKE_OWNER)
    path.write_text(json.dumps(raw), encoding="utf-8")

    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    snap = participation.rotate_native_session(
        path, expected_current=OLD_SID, new_sid=NEW_SID, day=DAY_NEW, history=history
    )
    assert snap["native_session_id"] == NEW_SID
    assert snap["native_sessions"] == history
    assert snap["ledger_id"] == ledger_id
    # The old record keeps its classification-day SID and still validates.
    assert snap["attempts"][key]["native_session_id"] == OLD_SID
    reopened = ParticipationLedger(path, native_sid=NEW_SID)
    assert reopened.ledger_id == ledger_id
    assert reopened.get(FAKE_CHANNEL, FAKE_OWNER)["status"] == "failed_known"


def test_ledger_rotate_rejects_unknown_record_sid_and_mismatch(tmp_path):
    path = tmp_path / "jev_state.json"
    _init_ledger(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    key = f"{FAKE_CHANNEL}:{FAKE_OWNER}"
    raw["attempts"][key] = _failed_known_record(OTHER_SID, FAKE_CHANNEL, FAKE_OWNER, FAKE_OWNER)
    path.write_text(json.dumps(raw), encoding="utf-8")
    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    # A record stamped with a SID outside {current} + history fails closed.
    with pytest.raises(ParticipationError, match="session mismatch"):
        participation.rotate_native_session(
            path, expected_current=OLD_SID, new_sid=NEW_SID, day=DAY_NEW, history=history
        )
    with pytest.raises(ParticipationError, match="session mismatch"):
        participation.rotate_native_session(
            path,
            expected_current=OTHER_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history={DAY_OLD: OLD_SID, "2026-10-06": OTHER_SID, DAY_NEW: NEW_SID},
        )
    # The file is untouched (top-level still seals the outgoing SID).
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["native_session_id"] == OLD_SID


def test_ledger_rotate_rejects_bad_args(tmp_path):
    path = tmp_path / "jev_state.json"
    _init_ledger(path)
    history = {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    with pytest.raises(ParticipationError, match="invalid new_sid"):
        participation.rotate_native_session(
            path, expected_current=OLD_SID, new_sid="bad", day=DAY_NEW, history=history
        )
    with pytest.raises(ParticipationError, match="does not bind day"):
        participation.rotate_native_session(
            path,
            expected_current=OLD_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history={DAY_OLD: OLD_SID, DAY_NEW: OTHER_SID},
        )
    with pytest.raises(ParticipationError, match="drops the current"):
        participation.rotate_native_session(
            path,
            expected_current=OLD_SID,
            new_sid=NEW_SID,
            day=DAY_NEW,
            history={DAY_NEW: NEW_SID},
        )


def test_ledger_history_field_backward_compatible(tmp_path):
    path = tmp_path / "jev_state.json"
    _init_ledger(path)
    assert ParticipationLedger(path, native_sid=OLD_SID).summary()["native_session_id"] == OLD_SID
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["native_sessions"] == {}
    del raw["native_sessions"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert ParticipationLedger(path, native_sid=OLD_SID).ledger_id is not None
    raw["native_sessions"] = {DAY_NEW: "bad-sid"}
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ParticipationError, match="invalid native_sessions"):
        ParticipationLedger(path, native_sid=OLD_SID)


# -- mapping registry ------------------------------------------------------------


def _write_mapping(path, sid=OLD_SID, extra=None, mode=0o600):
    data = {
        "native_id": sid,
        "title": "discord-main",
        "audit_label": "discord-main",
        "created_at": "2026-10-05T01:59:55+00:00",
        "path": f"/tmp/fake-sessions/{sid}.jsonl",
    }
    if extra:
        data.update(extra)
    path.write_text(json.dumps(data), encoding="utf-8")
    os.chmod(path, mode)
    return data


def test_load_registry_legacy_file_has_no_active_day(tmp_path):
    mapping = tmp_path / "mapping.json"
    _write_mapping(mapping)
    registry = load_session_registry(mapping, "discord-main")
    assert registry["native_id"] == OLD_SID
    assert registry["active_date"] is None
    assert registry["sessions"] == {}


def test_load_registry_sealed_file_returns_history(tmp_path):
    mapping = tmp_path / "mapping.json"
    history = {DAY_OLD: OLD_SID}
    _write_mapping(mapping, extra={"active_date": DAY_OLD, "sessions": history})
    registry = load_session_registry(mapping, "discord-main")
    assert registry["active_date"] == DAY_OLD
    assert registry["sessions"] == history


def test_load_registry_rejects_inconsistent_rotation_state(tmp_path):
    mapping = tmp_path / "mapping.json"
    # Date without history.
    _write_mapping(mapping, extra={"active_date": DAY_OLD})
    with pytest.raises(ResponderError, match="inconsistent rotation"):
        load_session_registry(mapping, "discord-main")
    # History without date.
    _write_mapping(mapping, extra={"sessions": {DAY_OLD: OLD_SID}})
    with pytest.raises(ResponderError, match="inconsistent rotation"):
        load_session_registry(mapping, "discord-main")
    # Active day binding a different SID.
    _write_mapping(mapping, extra={"active_date": DAY_OLD, "sessions": {DAY_OLD: OTHER_SID}})
    with pytest.raises(ResponderError, match="inconsistent rotation"):
        load_session_registry(mapping, "discord-main")
    # Malformed rotation values.
    _write_mapping(mapping, extra={"active_date": "2026-02-30", "sessions": {}})
    with pytest.raises(ResponderError, match="invalid active_date"):
        load_session_registry(mapping, "discord-main")


def test_adopt_session_day_seals_legacy_mapping(tmp_path):
    mapping = tmp_path / "mapping.json"
    _write_mapping(mapping, extra={"operator_note": "keep-me"})
    registry = load_session_registry(mapping, "discord-main")
    outcome = adopt_session_day(
        mapping_path=mapping,
        expected_session_id="discord-main",
        today=DAY_OLD,
        registry=registry,
    )
    assert outcome == {"native_sid": OLD_SID, "day": DAY_OLD, "adopted": True}
    raw = json.loads(mapping.read_text(encoding="utf-8"))
    assert raw["active_date"] == DAY_OLD
    assert raw["sessions"] == {DAY_OLD: OLD_SID}
    assert raw["native_id"] == OLD_SID
    assert raw["operator_note"] == "keep-me"  # unknown keys preserved
    assert stat.S_IMODE(mapping.stat().st_mode) == 0o600
    # Re-adopt is a no-op sync (no rewrite, same outcome shape).
    before = mapping.read_text(encoding="utf-8")
    registry = load_session_registry(mapping, "discord-main")
    outcome = adopt_session_day(
        mapping_path=mapping,
        expected_session_id="discord-main",
        today=DAY_OLD,
        registry=registry,
    )
    assert outcome["adopted"] is False
    assert mapping.read_text(encoding="utf-8") == before


def test_save_registry_refuses_concurrent_change(tmp_path):
    mapping = tmp_path / "mapping.json"
    _write_mapping(mapping)
    registry = load_session_registry(mapping, "discord-main")
    tampered = dict(registry)
    tampered["native_id"] = OTHER_SID
    new_registry = {
        "native_id": OTHER_SID,
        "title": "discord-main",
        "audit_label": "discord-main",
        "created_at": "2026-10-06T00:00:00+00:00",
        "path": f"/tmp/fake-sessions/{OTHER_SID}.jsonl",
        "active_date": "2026-10-06",
        "sessions": {"2026-10-06": OTHER_SID},
    }
    with pytest.raises(ResponderError, match="mapping changed during rotation"):
        save_session_registry(mapping, tampered, new_registry, expected_session_id="discord-main")
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == OLD_SID


def test_build_rotation_history_merges_and_guards():
    history = build_rotation_history(
        {DAY_OLD: OLD_SID},
        old_day=DAY_OLD,
        old_sid=OLD_SID,
        new_day=DAY_NEW,
        new_sid=NEW_SID,
    )
    assert history == {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    with pytest.raises(ResponderError, match="must advance"):
        build_rotation_history(
            {}, old_day=DAY_NEW, old_sid=OLD_SID, new_day=DAY_NEW, new_sid=NEW_SID
        )
    with pytest.raises(ResponderError, match="conflicts with recorded"):
        build_rotation_history(
            {DAY_NEW: REUSE_SID},
            old_day=DAY_OLD,
            old_sid=OLD_SID,
            new_day=DAY_NEW,
            new_sid=NEW_SID,
        )
    with pytest.raises(ResponderError, match="invalid previous day"):
        build_rotation_history({}, old_day="bad", old_sid=OLD_SID, new_day=DAY_NEW, new_sid=NEW_SID)


def test_derive_session_path_sits_beside_mapping(tmp_path):
    mapping = tmp_path / "mapping.json"
    assert derive_session_path(mapping, NEW_SID) == str(tmp_path / "sessions" / f"{NEW_SID}.jsonl")


# -- rotate_to_today orchestration -------------------------------------------------


class FakeRotClient:
    """Rotation-capable fake: create_session + get_session_snapshot only."""

    def __init__(self, created_sid=NEW_SID):
        self.create_calls: list = []
        self.snapshot_calls: list = []
        self.submit_calls: list = []
        self.created_sid = created_sid
        self.create_fn = None
        self.snapshot_fn = None

    async def create_session(self):
        self.create_calls.append(True)
        if self.create_fn is not None:
            return await self.create_fn()
        return {
            "native_sid": self.created_sid,
            "message_count": 0,
            "proof": "session_create_blank",
        }

    async def get_session_snapshot(self, sid, expected_title=None):
        self.snapshot_calls.append(sid)
        if self.snapshot_fn is not None:
            return await self.snapshot_fn(sid)
        return {
            "native_sid": sid,
            "native_id_match": True,
            "running": False,
            "turn_count": 0,
            "turn_indices": [],
            "pre_max_turn_index": None,
        }

    async def submit_turn(self, sid, text, model=None, effort=None):
        self.submit_calls.append((sid, text, model, effort))
        cleaned = text.strip()
        return {
            "completed": True,
            "cancelled": False,
            "native_id_match": True,
            "turn_index": 0,
            "turn_count": 1,
            "running": False,
            "submitted_text_len": len(cleaned),
            "submitted_text_sha256": hashlib.sha256(cleaned.encode("utf-8")).hexdigest(),
            "stored_text_match": True,
            "stored_text_len": len(cleaned),
        }

    async def get_trace_evidence(
        self, sid, idx, *, job_ref, source_message_id, channel_id, guild_id=None
    ):
        raise ThycaProtocolError(reason="transport")


def _sealed_mapping(path, sid=OLD_SID, day=DAY_OLD, history=None):
    history = {day: sid} if history is None else history
    return _write_mapping(path, sid=sid, extra={"active_date": day, "sessions": history})


def _rotation_fixture(tmp_path, *, day=DAY_OLD, history=None):
    mapping = tmp_path / "mapping.json"
    delivery_path = tmp_path / "delivery.json"
    ledger_path = tmp_path / "jev_state.json"
    _sealed_mapping(mapping, day=day, history=history)
    _init_delivery(delivery_path)
    _init_ledger(ledger_path)
    return mapping, delivery_path, ledger_path


async def test_rotate_to_today_creates_and_converges_all_stores(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()
    outcome = await rotate_to_today(
        client=client,
        mapping_path=mapping,
        delivery_path=delivery_path,
        ledger_path=ledger_path,
        expected_session_id="discord-main",
        today=DAY_NEW,
        registry=registry,
    )
    assert outcome["native_sid"] == NEW_SID
    assert outcome["day"] == DAY_NEW
    assert outcome["reused"] is False
    assert outcome["history"] == {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    assert client.create_calls != [] and client.snapshot_calls == []
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == NEW_SID
    assert ParticipationLedger(ledger_path, native_sid=NEW_SID).ledger_id is not None
    raw = json.loads(mapping.read_text(encoding="utf-8"))
    assert raw["native_id"] == NEW_SID and raw["active_date"] == DAY_NEW


async def test_rotate_to_today_reuses_recorded_session_without_create(tmp_path):
    history = {DAY_OLD: OLD_SID, DAY_NEW: REUSE_SID}
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path, history=history)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()
    outcome = await rotate_to_today(
        client=client,
        mapping_path=mapping,
        delivery_path=delivery_path,
        ledger_path=ledger_path,
        expected_session_id="discord-main",
        today=DAY_NEW,
        registry=registry,
    )
    assert outcome["native_sid"] == REUSE_SID and outcome["reused"] is True
    assert client.create_calls == [] and client.snapshot_calls == [REUSE_SID]
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == REUSE_SID


async def test_rotate_to_today_recreates_after_verified_404(tmp_path):
    history = {DAY_OLD: OLD_SID, DAY_NEW: REUSE_SID}
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path, history=history)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()

    async def _gone(sid):
        raise ThycaPreconditionError(status_code=404, reason="http_404")

    client.snapshot_fn = _gone
    outcome = await rotate_to_today(
        client=client,
        mapping_path=mapping,
        delivery_path=delivery_path,
        ledger_path=ledger_path,
        expected_session_id="discord-main",
        today=DAY_NEW,
        registry=registry,
    )
    assert outcome["native_sid"] == NEW_SID and outcome["reused"] is False
    assert client.create_calls != []
    assert outcome["history"] == {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}


async def test_rotate_to_today_verify_mismatch_fails_with_zero_writes(tmp_path):
    history = {DAY_OLD: OLD_SID, DAY_NEW: REUSE_SID}
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path, history=history)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()

    async def _mismatch(sid):
        return {"native_id_match": False}

    client.snapshot_fn = _mismatch
    with pytest.raises(ResponderError, match="native session mismatch"):
        await rotate_to_today(
            client=client,
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == OLD_SID


async def test_rotate_to_today_create_failure_defers_with_zero_writes(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()

    async def _boom():
        raise ThycaProtocolError(reason="http_5xx")

    client.create_fn = _boom
    with pytest.raises(RotationDeferred) as caught:
        await rotate_to_today(
            client=client,
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    assert caught.value.kind == "http_5xx"
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == OLD_SID


async def test_rotate_to_today_transient_matrix_defers(tmp_path):
    from thyca import ThycaSubmissionUncertainError

    transient: list = [
        ThycaSubmissionUncertainError(reason="timeout"),
        ThycaSubmissionUncertainError(reason="disconnect"),
        ThycaBusyError(),
        ThycaProtocolError(reason="http_5xx"),
    ]
    expected_kinds = ["timeout", "disconnect", "http_409", "http_5xx"]
    for index, error in enumerate(transient):
        case_dir = tmp_path / f"t{index}"
        case_dir.mkdir(parents=True, exist_ok=True)
        mapping, delivery_path, ledger_path = _rotation_fixture(case_dir)
        registry = load_session_registry(mapping, "discord-main")
        client = FakeRotClient()

        async def _fail(error=error):
            raise error

        client.create_fn = _fail
        with pytest.raises(RotationDeferred) as caught:
            await rotate_to_today(
                client=client,
                mapping_path=mapping,
                delivery_path=delivery_path,
                ledger_path=ledger_path,
                expected_session_id="discord-main",
                today=DAY_NEW,
                registry=registry,
            )
        assert caught.value.kind == expected_kinds[index]
        assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID


async def test_rotate_to_today_verify_timeout_defers(tmp_path):
    history = {DAY_OLD: OLD_SID, DAY_NEW: REUSE_SID}
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path, history=history)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()

    async def _timeout(sid):
        raise ThycaProtocolError(reason="timeout")

    client.snapshot_fn = _timeout
    with pytest.raises(RotationDeferred) as caught:
        await rotate_to_today(
            client=client,
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    assert caught.value.kind == "timeout"
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID


async def test_rotate_to_today_definitive_failures_still_halt(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    registry = load_session_registry(mapping, "discord-main")
    client = FakeRotClient()

    async def _bad_request():
        raise ThycaPreconditionError(status_code=400, reason="http_400")

    client.create_fn = _bad_request
    with pytest.raises(ResponderError) as caught:
        await rotate_to_today(
            client=client,
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    assert type(caught.value) is ResponderError
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID


async def test_rotate_to_today_rejects_malformed_create_result(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    registry = load_session_registry(mapping, "discord-main")
    for bad in ({"native_sid": "bad", "message_count": 0}, {"native_sid": NEW_SID}, None):
        client = FakeRotClient()

        async def _bad(bad=bad):
            return bad

        client.create_fn = _bad
        with pytest.raises(ResponderError, match="create failed"):
            await rotate_to_today(
                client=client,
                mapping_path=mapping,
                delivery_path=delivery_path,
                ledger_path=ledger_path,
                expected_session_id="discord-main",
                today=DAY_NEW,
                registry=registry,
            )
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == OLD_SID


async def test_rotate_to_today_drain_lost_refuses_before_mapping_write(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    delivery.enqueue_job(
        delivery_path,
        channel_id=FAKE_CHANNEL,
        message_id=31,
        author_uid=FAKE_OWNER,
        actor_kind="owner",
        seq=1,
    )
    registry = load_session_registry(mapping, "discord-main")
    with pytest.raises(ResponderError, match="delivery refused"):
        await rotate_to_today(
            client=FakeRotClient(),
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == OLD_SID


async def test_rotate_to_today_ledger_refusal_half_commits_loudly(tmp_path):
    mapping, delivery_path, ledger_path = _rotation_fixture(tmp_path)
    # Corrupt the ledger SID binding so its converge refuses AFTER delivery.
    raw = json.loads(ledger_path.read_text(encoding="utf-8"))
    raw["native_session_id"] = OTHER_SID
    ledger_path.write_text(json.dumps(raw), encoding="utf-8")
    registry = load_session_registry(mapping, "discord-main")
    with pytest.raises(ResponderError, match="ledger refused"):
        await rotate_to_today(
            client=FakeRotClient(),
            mapping_path=mapping,
            delivery_path=delivery_path,
            ledger_path=ledger_path,
            expected_session_id="discord-main",
            today=DAY_NEW,
            registry=registry,
        )
    # Fail-closed half-commit: delivery advanced, mapping still seals the old
    # day, so the next startup mismatch names both SIDs for the operator.
    assert delivery.load_delivery_state(delivery_path)["native_session_id"] == NEW_SID
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == OLD_SID


async def test_rotate_to_today_skips_unowned_ledger(tmp_path):
    mapping, delivery_path, _ledger_path = _rotation_fixture(tmp_path)
    registry = load_session_registry(mapping, "discord-main")
    outcome = await rotate_to_today(
        client=FakeRotClient(),
        mapping_path=mapping,
        delivery_path=delivery_path,
        ledger_path=None,
        expected_session_id="discord-main",
        today=DAY_NEW,
        registry=registry,
    )
    assert outcome["native_sid"] == NEW_SID
    assert json.loads(mapping.read_text(encoding="utf-8"))["native_id"] == NEW_SID


# -- responder gate ---------------------------------------------------------------


class _JevAdapterStub:
    async def decide(self, state):
        raise AssertionError("must not classify in gate tests")


class _LedgerStub:
    def __init__(self):
        self.calls = 0

    @property
    def ledger_id(self):
        return "1e" * 16

    async def classify(self, adapter, state, *, channel_id, message_id, author_uid):
        self.calls += 1
        from context import context_fingerprint

        return {
            "status": "decided",
            "fingerprint": context_fingerprint(state),
            "attempt_id": hashlib.sha256(f"{channel_id}:{message_id}".encode()).hexdigest()[:32],
            "model": "jev-1.13.0",
            "participation": "respond",
        }


def _collector(owner_uid, bot_uid):
    async def collect(entry):
        mid = entry["message_id"]
        author = entry["author_uid"]
        return {
            "assistant_uid": str(bot_uid),
            "owner_uid": str(owner_uid),
            "participants": [
                {"uid": str(owner_uid), "display_name": "Owner"},
                {"uid": str(bot_uid), "display_name": "Bot"},
            ],
            "messages": [{"id": str(mid), "sender_uid": str(author), "text": entry["content"]}],
            "new_message_id": str(mid),
        }

    return collect


async def _source_ok(channel_id, message_id):
    return None


def _bot():
    return SimpleNamespace(user=SimpleNamespace(id=FAKE_BOT))


def _responder(tmp_path, *, day_provider, client=None, mapping_extra=None, use_owned_ledger=False):
    tmp_path.mkdir(parents=True, exist_ok=True)
    settings = _settings(tmp_path)
    mapping = settings.thyca_session_mapping_path
    _write_mapping(mapping, extra=mapping_extra)
    _init_delivery(settings.delivery_state_path)
    client = client or FakeRotClient()
    kwargs: dict = {
        "client": client,
        "source_checker": _source_ok,
        "jev_adapter": _JevAdapterStub(),
        "context_collector": _collector(FAKE_OWNER, FAKE_BOT),
        "session_day_provider": day_provider,
    }
    if not use_owned_ledger:
        kwargs["participation_ledger"] = _LedgerStub()
    return Responder(settings, _bot(), **kwargs), settings, client


def _bot_entry(settings, message_id, **overrides):
    fields = {
        "type": "poll",
        "channel_id": FAKE_CHANNEL,
        "message_id": message_id,
        "author_uid": 600000000000000006,
        "author_name": "SomeBot",
        "actor_kind": "other",
        "content": "beep",
        "author_is_bot": True,
    }
    fields.update(overrides)
    append_entry(settings.outbox_path, settings.outbox_state_path, **fields)


async def test_gate_adopts_legacy_mapping_without_native_calls(tmp_path):
    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_OLD)
    assert inst.native_sid == OLD_SID
    out = await inst.tick()
    assert out["status"] == "idle"
    assert client.create_calls == [] and client.snapshot_calls == []
    raw = json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))
    assert raw["active_date"] == DAY_OLD
    assert raw["sessions"] == {DAY_OLD: OLD_SID}
    # Second tick is a steady-state no-op.
    out = await inst.tick()
    assert out["status"] == "idle"


async def test_gate_disabled_leaves_legacy_mapping_untouched(tmp_path, monkeypatch):
    monkeypatch.setenv("DAILY_SESSION_ROTATION", "false")
    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_OLD)
    out = await inst.tick()
    assert out["status"] == "idle"
    raw = json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))
    assert "active_date" not in raw and "sessions" not in raw


async def test_gate_idle_day_does_not_create_empty_session(tmp_path):
    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra)
    out = await inst.tick()
    assert out["status"] == "idle"
    assert client.create_calls == []  # lazy: no unread work, no session
    assert inst.native_sid == OLD_SID
    raw = json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))
    assert raw["active_date"] == DAY_OLD


async def test_gate_blocked_day_rotates_as_soon_as_drained(tmp_path):
    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra)
    _bot_entry(settings, 700000000000000001)
    # A queued job with no outbox entry blocks rotation; its submit short-
    # circuits on entry_not_found with zero native calls.
    delivery.enqueue_job(
        settings.delivery_state_path,
        channel_id=FAKE_CHANNEL,
        message_id=700000000000000009,
        author_uid=FAKE_OWNER,
        actor_kind="owner",
        seq=99,
    )
    out = await inst.tick()
    assert out["status"] == "pending"
    assert client.create_calls == []
    assert inst.native_sid == OLD_SID
    raw = json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))
    assert raw["active_date"] == DAY_OLD
    # Once drained, the next tick with unread work rotates immediately.
    delivery.skip_job(
        settings.delivery_state_path,
        FAKE_CHANNEL,
        700000000000000009,
        reason="operator_approved_drop",
    )
    _bot_entry(settings, 700000000000000002)
    out = await inst.tick()
    assert client.create_calls != []
    assert inst.native_sid == NEW_SID
    assert out["status"] == "idle"


async def test_gate_rotates_then_serves_new_day(tmp_path):
    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    inst, settings, client = _responder(
        tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra, use_owned_ledger=True
    )
    _bot_entry(settings, 700000000000000001)  # bot-filtered: no submit, no Jev
    out = await inst.tick()
    assert client.create_calls != []
    assert inst.native_sid == NEW_SID
    dstate = delivery.load_delivery_state(settings.delivery_state_path)
    assert dstate["native_session_id"] == NEW_SID
    assert ParticipationLedger(settings.jev_state_path, native_sid=NEW_SID).ledger_id is not None
    raw = json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))
    assert raw["native_id"] == NEW_SID
    assert raw["active_date"] == DAY_NEW
    assert raw["sessions"] == {DAY_OLD: OLD_SID, DAY_NEW: NEW_SID}
    assert Path(raw["path"]).stem == NEW_SID
    assert out["status"] == "idle"


async def test_submit_after_rotation_targets_new_session(tmp_path):
    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra)
    append_entry(
        settings.outbox_path,
        settings.outbox_state_path,
        type="message",
        channel_id=FAKE_CHANNEL,
        message_id=700000000000000001,
        author_uid=FAKE_OWNER,
        author_name="Owner",
        actor_kind="owner",
        content="hello",
        author_is_bot=False,
    )
    out = await inst.tick()
    assert inst.native_sid == NEW_SID
    assert client.submit_calls != []
    assert client.submit_calls[0][0] == NEW_SID
    # Trace is unavailable in this fake, so the job holds uncertain; the
    # rotation itself already converged before the submit.
    assert out["status"] == "uncertain"
    dstate = delivery.load_delivery_state(settings.delivery_state_path)
    assert dstate["native_session_id"] == NEW_SID


async def test_gate_defers_transient_warns_once_then_rotates_on_recovery(tmp_path, capsys):
    from thyca import ThycaSubmissionUncertainError

    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    client = FakeRotClient()
    failures = {"left": 2}

    async def _flaky():
        if failures["left"] > 0:
            failures["left"] -= 1
            raise ThycaSubmissionUncertainError(reason="timeout")
        return {
            "native_sid": NEW_SID,
            "message_count": 0,
            "proof": "session_create_blank",
        }

    client.create_fn = _flaky
    inst, settings, _ = _responder(
        tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra, client=client
    )
    _bot_entry(settings, 700000000000000001)
    # Tick 1: deferred, no raise, old session keeps serving.
    out = await inst.tick()
    assert out["status"] == "idle"
    assert inst.native_sid == OLD_SID
    # Tick 2 (new unread work): still deferred, still no second warning.
    _bot_entry(settings, 700000000000000002)
    out = await inst.tick()
    assert out["status"] == "idle"
    assert inst.native_sid == OLD_SID
    # Tick 3: native recovered, rotation proceeds.
    _bot_entry(settings, 700000000000000003)
    out = await inst.tick()
    assert inst.native_sid == NEW_SID
    assert out["status"] == "idle"
    err = capsys.readouterr().err
    assert err.count("rotation deferred for 2026-10-07 (timeout)") == 1
    assert "rotated daily session for 2026-10-07 (created)" in err


async def test_gate_adopt_already_sealed_syncs_full_history(tmp_path):
    from responder.rotation import save_session_registry as _save

    inst, settings, client = _responder(tmp_path, day_provider=lambda: DAY_NEW)
    assert inst._active_date is None
    # A concurrent writer seals the mapping (multi-day history) before tick 1.
    mapping_p = settings.thyca_session_mapping_path
    legacy = load_session_registry(mapping_p, "discord-main")
    sealed = {
        "native_id": OLD_SID,
        "title": "discord-main",
        "audit_label": "discord-main",
        "created_at": legacy["created_at"],
        "path": legacy["path"],
        "active_date": DAY_NEW,
        "sessions": {DAY_OLD: OLD_SID, DAY_NEW: OLD_SID},
    }
    _save(mapping_p, legacy, sealed, expected_session_id="discord-main")
    frozen = mapping_p.read_text(encoding="utf-8")
    out = await inst.tick()
    assert out["status"] == "idle"
    assert client.create_calls == []
    # No rewrite (already sealed), full history synced in memory.
    assert mapping_p.read_text(encoding="utf-8") == frozen
    assert inst._registry_sessions == {DAY_OLD: OLD_SID, DAY_NEW: OLD_SID}
    # The next rollover must rotate, not trip the concurrent-change guard.
    inst._session_day_provider = lambda: "2026-10-08"
    _bot_entry(settings, 700000000000000001)
    out = await inst.tick()
    assert inst.native_sid == NEW_SID
    assert out["status"] == "idle"
    raw = json.loads(mapping_p.read_text(encoding="utf-8"))
    assert raw["sessions"] == {DAY_OLD: OLD_SID, DAY_NEW: OLD_SID, "2026-10-08": NEW_SID}


async def test_gate_rotation_definitive_failure_fails_tick_with_zero_writes(tmp_path):
    extra = {"active_date": DAY_OLD, "sessions": {DAY_OLD: OLD_SID}}
    client = FakeRotClient()

    async def _boom():
        raise ThycaPreconditionError(status_code=400, reason="http_400")

    client.create_fn = _boom
    inst, settings, _ = _responder(
        tmp_path, day_provider=lambda: DAY_NEW, mapping_extra=extra, client=client
    )
    _bot_entry(settings, 700000000000000001)
    with pytest.raises(ResponderError, match="create failed"):
        await inst.tick()
    assert inst.native_sid == OLD_SID
    dstate = delivery.load_delivery_state(settings.delivery_state_path)
    assert dstate["native_session_id"] == OLD_SID
    assert (
        json.loads(settings.thyca_session_mapping_path.read_text(encoding="utf-8"))["native_id"]
        == OLD_SID
    )


async def test_gate_rejects_bad_day_provider(tmp_path):
    inst, _settings, _client = _responder(tmp_path / "a", day_provider=lambda: "not-a-day")
    with pytest.raises(ResponderError, match="invalid day"):
        await inst.tick()

    def _boom():
        raise RuntimeError("clock gone")

    inst, _, _ = _responder(tmp_path / "b", day_provider=_boom)
    with pytest.raises(ResponderError, match="provider failed"):
        await inst.tick()


def test_constructor_rejects_non_callable_day_provider(tmp_path):
    settings = _settings(tmp_path)
    _write_mapping(settings.thyca_session_mapping_path)
    _init_delivery(settings.delivery_state_path)
    with pytest.raises(ResponderError, match="session_day_provider"):
        Responder(settings, _bot(), client=FakeRotClient(), session_day_provider="x")


def test_recovery_rotate_native_sid_rebinds_strict(tmp_path):
    from responder.recovery import ResponderRecovery

    recovery = ResponderRecovery(
        client=FakeRotClient(),
        native_sid=OLD_SID,
        settings=_settings(tmp_path),
        mark_uncertain=lambda *a, **k: None,
        source_deleted_status=lambda *a, **k: None,
    )
    recovery.rotate_native_sid(NEW_SID)
    assert recovery._native_sid == NEW_SID
    with pytest.raises(ResponderError, match="invalid session"):
        recovery.rotate_native_sid("bad-sid")
