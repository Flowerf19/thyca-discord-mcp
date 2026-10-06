import json
from pathlib import Path

import pytest

from outbox import (
    OutboxError,
    ack_entries,
    append_entry,
    build_message_entry,
    get_poll_cursor,
    read_entries,
    set_poll_cursor,
)


def _paths(tmp_path):
    return tmp_path / "outbox.jsonl", tmp_path / "outbox_state.json"


def _fields(**overrides):
    base = {
        "type": "message",
        "channel_id": 7,
        "message_id": 100,
        "author_uid": 2,
        "author_name": "Owner",
        "actor_kind": "owner",
        "content": "hi",
    }
    base.update(overrides)
    return base


def test_append_read_ack_flow(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    for mid in (101, 102, 103):
        append_entry(outbox_path, state_path, **_fields(message_id=mid))

    unread = read_entries(outbox_path, state_path)
    assert [e["seq"] for e in unread["entries"]] == [1, 2, 3]
    assert unread["acked"] == 0
    assert unread["latest"] == 3

    assert ack_entries(state_path, 2) == 2
    unread = read_entries(outbox_path, state_path)
    assert [e["seq"] for e in unread["entries"]] == [3]
    assert unread["acked"] == 2

    all_entries = read_entries(outbox_path, state_path, unread_only=False)
    assert [e["seq"] for e in all_entries["entries"]] == [1, 2, 3]


def test_append_assigns_seq_and_ts(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    entry = append_entry(outbox_path, state_path, **_fields())
    assert entry["seq"] == 1
    assert entry["ts"]  # defaulted to now
    entry2 = append_entry(outbox_path, state_path, **_fields(ts="2026-01-01T00:00:00+00:00"))
    assert entry2["seq"] == 2
    assert entry2["ts"] == "2026-01-01T00:00:00+00:00"


def test_append_validates_fields(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    fields = _fields()
    del fields["content"]
    with pytest.raises(OutboxError, match="missing required fields"):
        append_entry(outbox_path, state_path, **fields)
    with pytest.raises(OutboxError, match="'seq'"):
        append_entry(outbox_path, state_path, **_fields(seq=99))


def test_ack_never_moves_backward(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields())
    append_entry(outbox_path, state_path, **_fields())
    assert ack_entries(state_path, 2) == 2
    assert ack_entries(state_path, 1) == 2
    assert read_entries(outbox_path, state_path)["acked"] == 2


def test_ack_beyond_latest_rejected(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields())
    with pytest.raises(OutboxError, match="beyond latest seq 1"):
        ack_entries(state_path, 99)
    assert read_entries(outbox_path, state_path)["acked"] == 0


def test_ack_invalid_values_rejected(tmp_path):
    _, state_path = _paths(tmp_path)
    for bad in (-1, "2", True, None):
        with pytest.raises(OutboxError):
            ack_entries(state_path, bad)


def test_read_limit_clamped_oldest_first(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    for mid in (101, 102, 103):
        append_entry(outbox_path, state_path, **_fields(message_id=mid))
    assert len(read_entries(outbox_path, state_path, limit=0)["entries"]) == 1
    assert len(read_entries(outbox_path, state_path, limit=-5)["entries"]) == 1
    got = read_entries(outbox_path, state_path, limit=2, unread_only=False)["entries"]
    assert [e["seq"] for e in got] == [1, 2]  # oldest first, not newest
    assert len(read_entries(outbox_path, state_path, limit=999)["entries"]) == 3


def test_read_missing_files_is_empty(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    got = read_entries(outbox_path, state_path)
    assert got == {"entries": [], "acked": 0, "latest": 0}


def test_missing_state_with_nonempty_log_fails_loud_on_append(tmp_path):
    # Requirement correction (review 2026-10-04): missing-state auto-recovery
    # would erase acked/poll cursors, so append must refuse BEFORE any write.
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    append_entry(outbox_path, state_path, **_fields(message_id=102))
    log_bytes = outbox_path.read_bytes()
    state_path.unlink()  # simulate state loss
    with pytest.raises(OutboxError, match="missing but log .* is non-empty"):
        append_entry(outbox_path, state_path, **_fields(message_id=103))
    assert outbox_path.read_bytes() == log_bytes  # no append happened
    assert not state_path.exists()  # no state rewritten either


def test_missing_state_with_nonempty_log_fails_loud_on_read(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    log_bytes = outbox_path.read_bytes()
    state_path.unlink()
    with pytest.raises(OutboxError, match="missing but the log holds 1"):
        read_entries(outbox_path, state_path)
    assert outbox_path.read_bytes() == log_bytes
    assert not state_path.exists()


def test_seq_zero_state_with_nonempty_log_fails_loud_on_append(tmp_path):
    # set_poll_cursor / ack(0) after state loss create a seq-0 state; a later
    # append must refuse rather than duplicate seq 1.
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    append_entry(outbox_path, state_path, **_fields(message_id=102))
    log_bytes = outbox_path.read_bytes()
    state_path.unlink()
    set_poll_cursor(state_path, 7, 999)
    state_bytes = state_path.read_bytes()
    with pytest.raises(OutboxError, match="seq 0 but log .* is non-empty"):
        append_entry(outbox_path, state_path, **_fields(message_id=103))
    assert outbox_path.read_bytes() == log_bytes
    assert state_path.read_bytes() == state_bytes


def test_ack_zero_after_state_loss_then_append_fails_loud(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    log_bytes = outbox_path.read_bytes()
    state_path.unlink()
    assert ack_entries(state_path, 0) == 0  # state-only helper, see precondition
    with pytest.raises(OutboxError, match="seq 0 but log .* is non-empty"):
        append_entry(outbox_path, state_path, **_fields(message_id=102))
    assert outbox_path.read_bytes() == log_bytes


def test_stale_state_behind_log_fails_loud_on_read(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    append_entry(outbox_path, state_path, **_fields(message_id=102))
    state_path.write_text(
        json.dumps({"seq": 1, "acked": 0, "poll": {}}) + "\n", encoding="utf-8"
    )  # simulate a stale backup restored over the state file
    with pytest.raises(OutboxError, match="stale outbox state"):
        read_entries(outbox_path, state_path)


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        '{"seq": 1}',
        '{"seq": 1, "acked": 0}',
        '{"seq": 1, "poll": {}}',
        '{"acked": 0, "poll": {}}',
    ],
)
def test_state_missing_keys_rejected_without_defaults(tmp_path, raw):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(raw, encoding="utf-8")
    with pytest.raises(OutboxError, match="missing key"):
        read_entries(outbox_path, state_path)
    with pytest.raises(OutboxError, match="missing key"):
        append_entry(outbox_path, state_path, **_fields())


def test_state_acked_above_seq_rejected(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 1, "acked": 2, "poll": {}}) + "\n", encoding="utf-8")
    with pytest.raises(OutboxError, match="exceeds 'seq'"):
        read_entries(outbox_path, state_path)


def _write_log_lines(outbox_path, lines):
    with outbox_path.open("w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def _log_record(seq, **overrides):
    record = {
        "seq": seq,
        "ts": "2026-01-01T00:00:00+00:00",
        "type": "message",
        "channel_id": 7,
        "message_id": 100,
        "author_uid": 2,
        "author_name": "Owner",
        "actor_kind": "owner",
        "content": "hi",
    }
    record.update(overrides)
    return json.dumps(record, ensure_ascii=False)


def test_failed_log_write_leaves_gap_never_reuse(tmp_path, monkeypatch):
    outbox_path, state_path = _paths(tmp_path)
    real_open = Path.open

    def failing_open(self, *args, **kwargs):
        if self.name == "outbox.jsonl" and args and args[0] == "a":
            raise OSError("simulated disk failure")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)
    with pytest.raises(OSError, match="simulated disk failure"):
        append_entry(outbox_path, state_path, **_fields(message_id=101))
    monkeypatch.undo()  # disk works again

    # seq 1 was reserved in state before the failed write: the next append
    # takes seq 2 (gap), it must never reuse seq 1.
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["seq"] == 1
    entry = append_entry(outbox_path, state_path, **_fields(message_id=102))
    assert entry["seq"] == 2
    got = read_entries(outbox_path, state_path, unread_only=False)
    assert [e["seq"] for e in got["entries"]] == [2]


def test_append_preserves_acked_and_poll_cursors(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    ack_entries(state_path, 1)
    set_poll_cursor(state_path, 7, 555)
    append_entry(outbox_path, state_path, **_fields(message_id=102))
    got = read_entries(outbox_path, state_path)
    assert got["acked"] == 1
    assert got["latest"] == 2
    assert get_poll_cursor(state_path, 7) == 555


def test_no_tmp_files_left_behind(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields())
    ack_entries(state_path, 1)
    set_poll_cursor(state_path, 7, 1)
    assert list(tmp_path.glob("*.tmp")) == []


def test_corrupt_state_raises_and_keeps_file(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text("not json{", encoding="utf-8")
    with pytest.raises(OutboxError, match="corrupt outbox state"):
        read_entries(outbox_path, state_path)
    with pytest.raises(OutboxError, match="corrupt outbox state"):
        append_entry(outbox_path, state_path, **_fields())
    with pytest.raises(OutboxError, match="corrupt outbox state"):
        ack_entries(state_path, 0)
    assert state_path.read_text(encoding="utf-8") == "not json{"  # never silently reset


@pytest.mark.parametrize(
    "raw",
    [
        "[]",
        '{"seq": "x", "acked": 0, "poll": {}}',
        '{"seq": -1, "acked": 0, "poll": {}}',
        '{"seq": 0, "acked": true, "poll": {}}',
        '{"seq": 0, "acked": 0, "poll": []}',
        '{"seq": 0, "acked": 0, "poll": {"7": "x"}}',
        '{"seq": 0, "acked": 0, "poll": {"nope": 1}}',
    ],
)
def test_malformed_state_rejected(tmp_path, raw):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(raw, encoding="utf-8")
    with pytest.raises(OutboxError, match="corrupt outbox state"):
        read_entries(outbox_path, state_path)


def test_malformed_log_line_raises_with_line_number(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    with outbox_path.open("a", encoding="utf-8") as f:
        f.write("oops\n")
    with pytest.raises(OutboxError, match="line 2"):
        read_entries(outbox_path, state_path)


def test_blank_log_line_raises(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    append_entry(outbox_path, state_path, **_fields(message_id=101))
    with outbox_path.open("a", encoding="utf-8") as f:
        f.write("\n")
    with pytest.raises(OutboxError, match="line 2"):
        read_entries(outbox_path, state_path)


@pytest.mark.parametrize("bad_seq", [0, -1, "1", True, 1.5, None])
def test_log_seq_must_be_int_at_least_1(tmp_path, bad_seq):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 1, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    _write_log_lines(outbox_path, [_log_record(bad_seq)])
    with pytest.raises(OutboxError, match="line 1"):
        read_entries(outbox_path, state_path)


def test_log_duplicate_seq_rejected(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 2, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    _write_log_lines(outbox_path, [_log_record(1), _log_record(1)])
    with pytest.raises(OutboxError, match="not strictly increasing"):
        read_entries(outbox_path, state_path)


def test_log_non_monotonic_seq_rejected(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 3, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    _write_log_lines(outbox_path, [_log_record(1), _log_record(3), _log_record(2)])
    with pytest.raises(OutboxError, match="line 3"):
        read_entries(outbox_path, state_path)


def test_log_seq_gaps_accepted(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 5, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    _write_log_lines(outbox_path, [_log_record(1), _log_record(5)])
    got = read_entries(outbox_path, state_path, unread_only=False)
    assert [e["seq"] for e in got["entries"]] == [1, 5]
    assert got["latest"] == 5


def test_log_bare_seq_record_rejected(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 1, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    _write_log_lines(outbox_path, ['{"seq": 1}'])
    with pytest.raises(OutboxError, match="missing field"):
        read_entries(outbox_path, state_path)


@pytest.mark.parametrize(
    "drop",
    [
        "ts",
        "type",
        "channel_id",
        "message_id",
        "author_uid",
        "author_name",
        "actor_kind",
        "content",
    ],
)
def test_log_missing_required_field_rejected(tmp_path, drop):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 1, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    record = json.loads(_log_record(1))
    del record[drop]
    _write_log_lines(outbox_path, [json.dumps(record, ensure_ascii=False)])
    with pytest.raises(OutboxError, match=f"missing field '{drop}'"):
        read_entries(outbox_path, state_path)


def test_log_error_mentions_field_without_contents(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    state_path.write_text(json.dumps({"seq": 1, "acked": 0, "poll": {}}) + "\n", encoding="utf-8")
    record = json.loads(_log_record(1, content="super-secret-content"))
    del record["ts"]
    _write_log_lines(outbox_path, [json.dumps(record, ensure_ascii=False)])
    with pytest.raises(OutboxError) as exc_info:
        read_entries(outbox_path, state_path)
    assert "super-secret-content" not in str(exc_info.value)
    assert "missing field 'ts'" in str(exc_info.value)


def test_poll_cursor_roundtrip(tmp_path):
    _, state_path = _paths(tmp_path)
    assert get_poll_cursor(state_path, 7) is None
    assert set_poll_cursor(state_path, 7, 111) == 111
    assert get_poll_cursor(state_path, 7) == 111
    assert get_poll_cursor(state_path, 8) is None
    set_poll_cursor(state_path, 8, 222)
    assert get_poll_cursor(state_path, 7) == 111
    assert get_poll_cursor(state_path, 8) == 222


def test_poll_cursor_invalid_rejected(tmp_path):
    _, state_path = _paths(tmp_path)
    for bad in (-1, "111", True):
        with pytest.raises(OutboxError):
            set_poll_cursor(state_path, 7, bad)


def test_build_message_entry_filters():
    kw = {
        "scope_guild_id": 100,
        "channel_id": 7,
        "message_id": 55,
        "author_uid": 2,
        "author_name": "Owner",
        "content": "hello",
        "bot_uid": 1,
        "owner_uid": 2,
        "watch_channel_ids": [7],
    }
    assert build_message_entry(**kw, guild_id=999) is None  # wrong guild
    assert build_message_entry(**kw, guild_id=None) is None  # DM
    assert build_message_entry(**{**kw, "guild_id": 100, "author_uid": 1}) is None  # self
    assert build_message_entry(**{**kw, "guild_id": 100, "channel_id": 8}) is None  # unwatched


def test_build_message_entry_owner_other_and_empty_content():
    base = {
        "scope_guild_id": 100,
        "guild_id": 100,
        "channel_id": 7,
        "message_id": 55,
        "bot_uid": 1,
        "owner_uid": 2,
        "watch_channel_ids": [7],
    }
    owner = build_message_entry(**base, author_uid=2, author_name="Owner", content="do it")
    assert owner is not None
    assert owner["actor_kind"] == "owner"
    assert owner["type"] == "message"
    assert owner["content"] == "do it"

    other = build_message_entry(**base, author_uid=3, author_name="Rando", content="hey")
    assert other is not None
    assert other["actor_kind"] == "other"

    empty = build_message_entry(**base, author_uid=3, author_name="Rando", content="")
    assert empty is not None  # embed/attachment-only messages are still captured
    assert empty["content"] == ""

    none_content = build_message_entry(**base, author_uid=3, author_name="Rando", content=None)
    assert none_content is not None
    assert none_content["content"] == ""


def test_build_message_entry_self_wins_on_misconfiguration():
    entry = build_message_entry(
        scope_guild_id=100,
        guild_id=100,
        channel_id=7,
        message_id=55,
        author_uid=1,
        author_name="Bot",
        content="x",
        bot_uid=1,
        owner_uid=1,  # misconfigured: bot == owner -> still dropped as self
        watch_channel_ids=[7],
    )
    assert entry is None


def test_build_message_entry_legacy_call_omits_optional_fields():
    # v5 capture metadata: legacy callers omit both keys; absence means
    # unknown, never assumed human by a future worker.
    entry = build_message_entry(
        scope_guild_id=100,
        guild_id=100,
        channel_id=7,
        message_id=55,
        author_uid=3,
        author_name="Rando",
        content="hey",
        bot_uid=1,
        owner_uid=2,
        watch_channel_ids=[7],
    )
    assert entry is not None
    assert "author_is_bot" not in entry
    assert "reply_to_message_id" not in entry


def test_build_message_entry_human_and_bot_exact_bool():
    base = {
        "scope_guild_id": 100,
        "guild_id": 100,
        "channel_id": 7,
        "message_id": 55,
        "bot_uid": 1,
        "owner_uid": 2,
        "watch_channel_ids": [7],
    }
    human = build_message_entry(
        **base,
        author_uid=2,
        author_name="Owner",
        content="hi",
        author_is_bot=False,
        reply_to_message_id=None,
    )
    assert human is not None
    assert human["author_is_bot"] is False
    assert human["reply_to_message_id"] is None
    assert human["actor_kind"] == "owner"

    other_bot = build_message_entry(
        **base,
        author_uid=9,
        author_name="OtherBot",
        content="beep",
        author_is_bot=True,
        reply_to_message_id=None,
    )
    assert other_bot is not None
    assert other_bot["author_is_bot"] is True
    assert other_bot["reply_to_message_id"] is None
    # UID-based actor classification is unchanged: a non-self bot is "other".
    assert other_bot["actor_kind"] == "other"


def test_build_message_entry_reply_reference_recorded():
    base = {
        "scope_guild_id": 100,
        "guild_id": 100,
        "channel_id": 7,
        "message_id": 56,
        "bot_uid": 1,
        "owner_uid": 2,
        "watch_channel_ids": [7],
    }
    reply = build_message_entry(
        **base,
        author_uid=3,
        author_name="Rando",
        content="re",
        author_is_bot=False,
        reply_to_message_id=55,
    )
    assert reply is not None
    assert reply["reply_to_message_id"] == 55

    no_ref = build_message_entry(
        **base,
        author_uid=3,
        author_name="Rando",
        content="top",
        author_is_bot=False,
        reply_to_message_id=None,
    )
    assert no_ref is not None
    assert no_ref["reply_to_message_id"] is None


def test_build_message_entry_self_excluded_regardless_of_bot_flag():
    base = {
        "scope_guild_id": 100,
        "guild_id": 100,
        "channel_id": 7,
        "message_id": 55,
        "author_uid": 1,
        "author_name": "ThycaBot",
        "content": "self",
        "bot_uid": 1,
        "owner_uid": 2,
        "watch_channel_ids": [7],
    }
    assert build_message_entry(**base, author_is_bot=True) is None
    assert build_message_entry(**base, author_is_bot=False) is None
    assert build_message_entry(**base) is None


def test_append_read_mixed_legacy_and_v5_entries(tmp_path):
    outbox_path, state_path = _paths(tmp_path)
    legacy = append_entry(outbox_path, state_path, **_fields(message_id=101))
    assert "author_is_bot" not in legacy
    assert "reply_to_message_id" not in legacy
    v5 = append_entry(
        outbox_path,
        state_path,
        **_fields(message_id=102, author_uid=9, author_name="OtherBot"),
        author_is_bot=True,
        reply_to_message_id=101,
    )
    assert v5["author_is_bot"] is True
    assert v5["reply_to_message_id"] == 101
    got = read_entries(outbox_path, state_path, unread_only=False)
    assert [e["message_id"] for e in got["entries"]] == [101, 102]
    assert "author_is_bot" not in got["entries"][0]
    assert got["entries"][1]["author_is_bot"] is True
    assert got["entries"][1]["reply_to_message_id"] == 101
