"""Offline tests for the canonical conversation projection.

Fake/offline only: synthetic states plus the 30 benchmark fixture states
(state fields only -- gold is never passed into the projection, only used
for absence assertions). No SDK, no network, no env, no paid calls.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

from conversation import (
    ConversationError,
    get_new_message,
    new_message_reply_status,
    project_conversation_state,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "jev_conversation_cases.json"
)

FORBIDDEN_KEYS = frozenset(
    {
        "expected",
        "rationale",
        "category",
        "description",
        "participation_notes",
        "participation_label",
        "gold_label",
    }
)


def _fixture_cases():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["cases"]


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
            "text": "HelperBot, please summarize",
            "reply_to": f"{tag}m2",
        },
    ]


def make_state(tag="", **overrides):
    state = {
        "assistant_uid": "bot_demo",
        "owner_uid": "alice_demo",
        "participants": _participants(),
        "messages": _messages(tag),
        "new_message_id": f"{tag}m3",
    }
    state.update(overrides)
    return state


def _assert_no_forbidden(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            assert key not in FORBIDDEN_KEYS, key
            _assert_no_forbidden(value)
    elif isinstance(obj, list):
        for item in obj:
            _assert_no_forbidden(item)


# ---------------------------------------------------------------------------
# Canonical contract over all 30 fixture states
# ---------------------------------------------------------------------------


def test_all_thirty_fixture_states_project_exact_fields():
    cases = _fixture_cases()
    assert len(cases) == 30
    for case in cases:
        snapshot = project_conversation_state(case["state"])
        assert set(snapshot) == {
            "assistant_uid",
            "owner_uid",
            "participants",
            "messages",
            "new_message_id",
        }, case["id"]
        for part in snapshot["participants"]:
            assert set(part) == {"uid", "display_name"}, case["id"]
        for msg in snapshot["messages"]:
            assert {"id", "sender_uid", "text"} <= set(msg), case["id"]
            assert set(msg) <= {"id", "sender_uid", "text", "reply_to", "timestamp"}, case["id"]
        assert snapshot["new_message_id"] in {m["id"] for m in snapshot["messages"]}



def test_no_gold_leak_in_projection_all_thirty():
    for case in _fixture_cases():
        blob = json.dumps(project_conversation_state(case["state"]))
        for leaked in (
            case["rationale"],
            case["category"],
            case["description"],
            case["participation_notes"],
            json.dumps(case["expected"]),
        ):
            assert json.dumps(leaked) not in blob, case["id"]
        _assert_no_forbidden(json.loads(blob))


def test_all_messages_preserved_byte_identical_no_window():
    for case in _fixture_cases():
        raw = case["state"]["messages"]
        snapshot = project_conversation_state(case["state"])
        assert len(snapshot["messages"]) == len(raw), case["id"]
        for sent, original in zip(snapshot["messages"], raw, strict=True):
            assert sent["id"] == original["id"]
            assert sent["sender_uid"] == original["sender_uid"]
            assert sent["text"] == original["text"]
            for key in ("reply_to", "timestamp"):
                if key in original:
                    assert sent[key] == original[key]
                else:
                    assert key not in sent


# ---------------------------------------------------------------------------
# Snapshot detachment and extra-key filtering
# ---------------------------------------------------------------------------


def test_snapshot_detached_from_caller_mutation():
    state = make_state()
    before = copy.deepcopy(state)
    snapshot = project_conversation_state(state)
    assert snapshot == project_conversation_state(before)
    state["assistant_uid"] = "mallory_demo"
    state["participants"].append({"uid": "mallory_demo", "display_name": "Mallory"})
    state["participants"][0]["display_name"] = "Mutated"
    state["messages"][0]["text"] = "mutated text"
    state["messages"].append({"id": "mx", "sender_uid": "alice_demo", "text": "x"})
    state["new_message_id"] = "mx"
    assert snapshot == project_conversation_state(before)
    snapshot["messages"][0]["text"] = "snapshot edit"
    snapshot["participants"].append({"uid": "zz", "display_name": "ZZ"})
    assert project_conversation_state(before)["messages"][0]["text"] == "hi all"


def test_extra_keys_dropped_at_all_levels():
    state = make_state()
    state["expected"] = {"sender_uid": "carol_demo"}
    state["rationale"] = "GOLD-RATIONALE"
    state["category"] = "gold_category"
    state["description"] = "gold description"
    state["participation_notes"] = "gold notes"
    state["participants"][0]["role"] = "admin"
    state["participants"][0]["expected"] = True
    state["messages"][0]["gold_label"] = "respond"
    state["messages"][0]["rationale"] = {"nested": "gold"}
    snapshot = project_conversation_state(state)
    assert set(snapshot) == {
        "assistant_uid",
        "owner_uid",
        "participants",
        "messages",
        "new_message_id",
    }
    blob = json.dumps(snapshot)
    assert "GOLD-RATIONALE" not in blob
    assert "gold_category" not in blob
    assert "gold notes" not in blob
    _assert_no_forbidden(snapshot)


def test_message_text_never_echoed_in_errors():
    state = make_state()
    state["messages"][0]["text"] = "SECRET-TEXT-CANARY-123"
    state["messages"][1] = {"id": "m2", "sender_uid": "ghost_demo", "text": "other"}
    with pytest.raises(ConversationError) as excinfo:
        project_conversation_state(state)
    assert "CANARY" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# Identity and membership validation
# ---------------------------------------------------------------------------


def test_duplicate_display_names_allowed():
    state = make_state()
    state["participants"][1]["display_name"] = "Alice"
    state["participants"][2]["display_name"] = "Alice"
    snapshot = project_conversation_state(state)
    assert [p["display_name"] for p in snapshot["participants"]] == ["Alice", "Alice", "Alice"]


def test_duplicate_message_id_rejected():
    state = make_state()
    state["messages"][2]["id"] = "m1"
    with pytest.raises(ConversationError, match="duplicate message id"):
        project_conversation_state(state)


def test_duplicate_participant_uid_rejected():
    state = make_state()
    state["participants"][2]["uid"] = "alice_demo"
    with pytest.raises(ConversationError, match="duplicate participant uid"):
        project_conversation_state(state)


def test_unknown_sender_rejected():
    state = make_state()
    state["messages"][2]["sender_uid"] = "ghost_demo"
    with pytest.raises(ConversationError, match="not a participant"):
        project_conversation_state(state)


def test_unknown_new_message_id_rejected():
    state = make_state(new_message_id="nope")
    with pytest.raises(ConversationError, match="new_message_id"):
        project_conversation_state(state)


@pytest.mark.parametrize("bad_id", [["m3"], {"m3": 1}, ("m3",), None, 123, "", "  no-trim-match  "])
def test_new_message_id_invalid_types_rejected(bad_id):
    # Unhashable list/dict must raise ConversationError, not TypeError; all
    # invalid shapes share the uniform contract. No provider exists here.
    with pytest.raises(ConversationError, match="new_message_id"):
        project_conversation_state(make_state(new_message_id=bad_id))


@pytest.mark.parametrize("role", ["assistant_uid", "owner_uid"])
def test_owner_and_assistant_must_be_participants(role):
    state = make_state(**{role: "ghost_demo"})
    with pytest.raises(ConversationError, match="must be a participant"):
        project_conversation_state(state)


@pytest.mark.parametrize("uid", ["channel", "unclear"])
def test_reserved_participant_uid_rejected(uid):
    state = make_state()
    state["participants"].append({"uid": uid, "display_name": "Reserved"})
    with pytest.raises(ConversationError, match="reserved"):
        project_conversation_state(state)


def test_empty_and_nonstring_ids_rejected():
    bad_states = [
        make_state(assistant_uid=""),
        make_state(owner_uid=None),
        make_state(new_message_id=""),
    ]
    for bad in bad_states:
        with pytest.raises(ConversationError):
            project_conversation_state(bad)
    bad_participant = make_state()
    bad_participant["participants"][0]["uid"] = 123
    with pytest.raises(ConversationError):
        project_conversation_state(bad_participant)
    bad_name = make_state()
    bad_name["participants"][0]["display_name"] = ""
    with pytest.raises(ConversationError):
        project_conversation_state(bad_name)
    bad_mid = make_state()
    bad_mid["messages"][0]["id"] = ""
    with pytest.raises(ConversationError):
        project_conversation_state(bad_mid)


def test_non_object_inputs_rejected():
    for bad in (None, [], "state", 42):
        with pytest.raises(ConversationError, match="must be an object"):
            project_conversation_state(bad)
    for field in ("participants", "messages"):
        bad = make_state(**{field: []})
        with pytest.raises(ConversationError, match="non-empty list"):
            project_conversation_state(bad)
        bad = make_state(**{field: {}})
        with pytest.raises(ConversationError, match="non-empty list"):
            project_conversation_state(bad)
    bad = make_state()
    bad["participants"][0] = "alice_demo"
    with pytest.raises(ConversationError, match="must be an object"):
        project_conversation_state(bad)
    bad = make_state()
    bad["messages"][0] = "m1"
    with pytest.raises(ConversationError, match="must be an object"):
        project_conversation_state(bad)


# ---------------------------------------------------------------------------
# Text, reply links, timestamps
# ---------------------------------------------------------------------------


def test_text_must_be_string_empty_allowed():
    state = make_state()
    state["messages"][0]["text"] = ""
    assert project_conversation_state(state)["messages"][0]["text"] == ""
    for bad_text in (None, 123, ["x"]):
        bad = make_state()
        bad["messages"][0]["text"] = bad_text
        with pytest.raises(ConversationError, match="text must be a string"):
            project_conversation_state(bad)


def test_visible_and_missing_parents_accepted():
    visible = project_conversation_state(make_state())
    assert visible["messages"][2]["reply_to"] == "m2"
    missing = make_state()
    missing["messages"][2]["reply_to"] = "gone"
    assert project_conversation_state(missing)["messages"][2]["reply_to"] == "gone"


def test_future_and_self_reply_rejected():
    future = make_state()
    future["messages"][0]["reply_to"] = "m3"
    with pytest.raises(ConversationError, match="must precede the child"):
        project_conversation_state(future)
    self_ref = make_state()
    self_ref["messages"][1]["reply_to"] = "m2"
    with pytest.raises(ConversationError, match="must precede the child"):
        project_conversation_state(self_ref)


def test_bad_reply_shapes_rejected_missing_key_ok():
    for bad_reply in ("", 123, ["m1"]):
        bad = make_state()
        bad["messages"][1]["reply_to"] = bad_reply
        with pytest.raises(ConversationError, match="reply_to"):
            project_conversation_state(bad)
    bare = make_state()
    del bare["messages"][1]["reply_to"]
    snapshot = project_conversation_state(bare)
    assert "reply_to" not in snapshot["messages"][1]
    assert new_message_reply_status(make_state(tag="", new_message_id="m1")) == "none"


def test_timestamp_optional_and_preserved():
    assert "timestamp" not in project_conversation_state(make_state())["messages"][0]
    stamped = make_state()
    stamped["messages"][0]["timestamp"] = "2026-09-01T09:00:00Z"
    assert project_conversation_state(stamped)["messages"][0]["timestamp"] == "2026-09-01T09:00:00Z"
    bad = make_state()
    bad["messages"][0]["timestamp"] = 123
    with pytest.raises(ConversationError, match="timestamp"):
        project_conversation_state(bad)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def test_get_new_message_returns_new_message():
    msg = get_new_message(make_state())
    assert msg["id"] == "m3"
    assert msg["sender_uid"] == "carol_demo"
    with pytest.raises(ConversationError):
        get_new_message(make_state(new_message_id="nope"))


@pytest.mark.parametrize(
    ("reply_to", "new_id", "expected"),
    [(None, "m1", "none"), ("m1", "m2", "visible"), ("gone", "m2", "missing")],
)
def test_new_message_reply_status(reply_to, new_id, expected):
    state = make_state(new_message_id=new_id)
    state["messages"][1]["reply_to"] = reply_to
    assert new_message_reply_status(state) == expected


# ---------------------------------------------------------------------------
# Import purity: no env reads, no SDK
# ---------------------------------------------------------------------------


def test_projection_needs_no_env_key_or_sdk(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    for mod in ("typesafe_sdk", "dotenv"):
        monkeypatch.delitem(sys.modules, mod, raising=False)
    snapshot = project_conversation_state(make_state())
    assert snapshot["new_message_id"] == "m3"
    assert "typesafe_sdk" not in sys.modules
    assert "dotenv" not in sys.modules


def test_package_import_reads_no_env(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "SENTINEL-should-never-be-read")
    import conversation  # noqa: F401

    blob = json.dumps(project_conversation_state(make_state()))
    assert "SENTINEL" not in blob
