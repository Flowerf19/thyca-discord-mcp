"""SQLite outbox store tests (Phase 2a gate).

Seeds: entry-building kernels from tests/test_outbox.py (capture semantics
preserved verbatim); storage kernels rewritten for the single-DB design.

Deleted two-file JSON consistency class (reasons):
- missing/zeroed-state-next-to-nonempty-log, stale-state-behind-log,
  seq-gap/reuse-via-state-reserve, log seq validation (blank lines, bad seq,
  duplicates, gaps, missing fields), state missing-key/acked-above-seq,
  malformed-state, tmp-file checks: the two-file design no longer exists.
  The singleton state row + UNIQUE(channel,message) + high-water inside one
  BEGIN IMMEDIATE transaction make the whole class impossible.
- Replaced by: corrupt-DB-loud on open (quick_check + user_version gate),
  INSERT OR IGNORE dedupe, high-water seq non-reuse, cursor resume.
"""

import os
import sqlite3
import stat

import pytest

from server.core import build_message_entry
from store import (
    Store,
    StoreError,
    ack_entries,
    append_entry,
    append_poll_batch,
    get_poll_cursor,
    read_entries,
    set_poll_cursor,
)


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
    db = tmp_path / "state.db"
    for mid in (101, 102, 103):
        append_entry(db, **_fields(message_id=mid))

    unread = read_entries(db)
    assert [e["seq"] for e in unread["entries"]] == [1, 2, 3]
    assert unread["acked"] == 0
    assert unread["latest"] == 3

    assert ack_entries(db, 2) == 2
    unread = read_entries(db)
    assert [e["seq"] for e in unread["entries"]] == [3]
    assert unread["acked"] == 2

    all_entries = read_entries(db, unread_only=False)
    assert [e["seq"] for e in all_entries["entries"]] == [1, 2, 3]


def test_append_assigns_seq_and_ts(tmp_path):
    db = tmp_path / "state.db"
    entry = append_entry(db, **_fields())
    assert entry["seq"] == 1
    assert entry["ts"]
    entry2 = append_entry(db, **_fields(ts="2026-01-01T00:00:00+00:00"))
    # Same (channel,message) dedupes: second append returns the existing row.
    assert entry2["seq"] == 1
    assert entry2["ts"] == entry["ts"]


def test_append_validates_fields(tmp_path):
    db = tmp_path / "state.db"
    fields = _fields()
    del fields["content"]
    with pytest.raises(StoreError, match="missing required fields"):
        append_entry(db, **fields)
    with pytest.raises(StoreError, match="'seq'"):
        append_entry(db, **_fields(seq=99))


def test_ack_never_moves_backward(tmp_path):
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    append_entry(db, **_fields(message_id=102))
    assert ack_entries(db, 2) == 2
    assert ack_entries(db, 1) == 2
    assert read_entries(db)["acked"] == 2


def test_ack_beyond_latest_rejected(tmp_path):
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    with pytest.raises(StoreError, match="beyond latest seq 1"):
        ack_entries(db, 99)
    assert read_entries(db)["acked"] == 0


def test_ack_invalid_values_rejected(tmp_path):
    db = tmp_path / "state.db"
    for bad in (-1, "2", True, None):
        with pytest.raises(StoreError):
            ack_entries(db, bad)


def test_read_limit_clamped_oldest_first(tmp_path):
    db = tmp_path / "state.db"
    for mid in (101, 102, 103):
        append_entry(db, **_fields(message_id=mid))
    assert len(read_entries(db, limit=0)["entries"]) == 1
    assert len(read_entries(db, limit=-5)["entries"]) == 1
    got = read_entries(db, limit=2, unread_only=False)["entries"]
    assert [e["seq"] for e in got] == [1, 2]
    assert len(read_entries(db, limit=999)["entries"]) == 3


def test_read_bool_limit_rejected(tmp_path):
    db = tmp_path / "state.db"
    for mid in (101, 102):
        append_entry(db, **_fields(message_id=mid))
    # int(True) == 1 would silently pass without the pre-conversion guard.
    for bad in (True, False):
        with pytest.raises(StoreError, match="must be an int"):
            read_entries(db, limit=bad)


def test_read_empty_db_is_empty(tmp_path):
    db = tmp_path / "state.db"
    got = read_entries(db)
    assert got == {"entries": [], "acked": 0, "latest": 0}


def test_realtime_poll_dedupe_single_row(tmp_path):
    """Gate: same message via realtime + poll => exactly one row."""
    db = tmp_path / "state.db"
    realtime = append_entry(db, **_fields(message_id=101))
    assert realtime["seq"] == 1
    # Poll tick for the same message goes through the batch path.
    rows = append_poll_batch(
        db,
        7,
        [
            _fields(
                type="poll",
                message_id=101,
                author_is_bot=False,
                reply_to_message_id=None,
            )
        ],
        101,
    )
    assert len(rows) == 1
    assert rows[0]["seq"] == 1  # no new seq consumed
    got = read_entries(db, unread_only=False)
    assert len(got["entries"]) == 1
    assert got["entries"][0]["seq"] == 1
    assert got["latest"] == 1
    assert get_poll_cursor(db, 7) == 101


def test_poll_batch_cursor_atomic(tmp_path):
    """Gate: poll batch + cursor land in ONE transaction."""
    db = tmp_path / "state.db"
    items = [_fields(type="poll", message_id=mid) for mid in (201, 202, 203)]
    rows = append_poll_batch(db, 7, items, 203)
    assert [r["seq"] for r in rows] == [1, 2, 3]
    assert get_poll_cursor(db, 7) == 203
    got = read_entries(db, unread_only=False)
    assert [e["message_id"] for e in got["entries"]] == [201, 202, 203]


def test_poll_cursor_resume_across_reopen(tmp_path):
    """Gate: cursor persists; reopen resumes after it."""
    db = tmp_path / "state.db"
    assert get_poll_cursor(db, 7) is None
    append_poll_batch(db, 7, [_fields(type="poll", message_id=50)], 50)
    with Store.open(db) as reopened:
        assert reopened.get_poll_cursor(7) == 50
        assert reopened.read_entries()["latest"] == 1
    # New batch after reopen continues seqs (no reset).
    append_poll_batch(db, 7, [_fields(type="poll", message_id=51)], 51)
    got = read_entries(db, unread_only=False)
    assert [e["seq"] for e in got["entries"]] == [1, 2]
    assert get_poll_cursor(db, 7) == 51


def test_seq_never_reused_via_high_water(tmp_path):
    """Gate: deleting the max row never reuses its seq (high-water survives)."""
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    append_entry(db, **_fields(message_id=102))
    conn = sqlite3.connect(str(db))
    conn.execute("DELETE FROM outbox WHERE seq = 2;")
    conn.commit()
    conn.close()
    entry = append_entry(db, **_fields(message_id=103))
    assert entry["seq"] == 3  # gap (2 deleted), never reused
    got = read_entries(db, unread_only=False)
    assert [e["seq"] for e in got["entries"]] == [1, 3]
    assert got["latest"] == 3


def test_corrupt_db_fails_loudly_on_open(tmp_path):
    """Gate: garbage bytes fail loudly; never auto-reset or delete."""
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    before = db.read_bytes()
    with db.open("r+b") as f:
        f.seek(32)
        f.write(b"\x00\xff\x00\xff-corrupt-corrupt-corrupt")
    with pytest.raises(Exception, match="(?i)corrupt|malform|header|database|format|file"):
        Store.open(db)
    with pytest.raises(Exception, match="(?i)corrupt|malform|header|database|format|file"):
        read_entries(db)
    # The file is never reset or deleted by the failed open: it is still
    # present, still corrupt (a repeat open still fails), and byte-identical.
    assert db.exists()
    assert db.read_bytes() != before  # corruption landed
    after = db.read_bytes()
    with pytest.raises(Exception, match="(?i)corrupt|malform|header|database|format|file"):
        Store.open(db)
    assert db.read_bytes() == after


def test_unsupported_version_fails_loudly(tmp_path):
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA user_version=999;")
    conn.close()
    with pytest.raises(StoreError, match="unsupported database version"):
        Store.open(db)
    with pytest.raises(StoreError, match="unsupported database version"):
        read_entries(db)


def test_db_file_created_0600(tmp_path):
    db = tmp_path / "sub" / "state.db"
    append_entry(db, **_fields())
    assert stat.S_IMODE(db.stat().st_mode) == 0o600


def test_wal_sidecars_chmod_0600_best_effort(tmp_path, monkeypatch):
    db = tmp_path / "state.db"
    append_entry(db, **_fields())
    wal = tmp_path / "state.db-wal"
    shm = tmp_path / "state.db-shm"
    # Sidecars vanish on clean close, so hold a live writer to keep them.
    holder = sqlite3.connect(str(db), isolation_level=None)
    try:
        holder.execute("PRAGMA journal_mode=WAL;")
        holder.execute(
            "INSERT INTO outbox (channel_id, message_id, type, author_uid, author_name,"
            " actor_kind, content, ts) VALUES (7, 999, 'message', 2, 'O', 'owner', 'hold', 't');"
        )
        assert wal.exists() and shm.exists()
        # Loosen the modes, then reopen: open must restore 0600 best-effort.
        os.chmod(wal, 0o644)
        os.chmod(shm, 0o644)
        Store.open(db).close()
        assert stat.S_IMODE(wal.stat().st_mode) == 0o600
        assert stat.S_IMODE(shm.stat().st_mode) == 0o600
        # OSError during chmod never fails the open.

        def _boom(path, mode):
            raise OSError("denied")

        monkeypatch.setattr(os, "chmod", _boom)
        Store.open(db).close()
    finally:
        holder.close()


def test_poll_cursor_roundtrip(tmp_path):
    db = tmp_path / "state.db"
    assert get_poll_cursor(db, 7) is None
    assert set_poll_cursor(db, 7, 111) == 111
    assert get_poll_cursor(db, 7) == 111
    assert get_poll_cursor(db, 8) is None
    set_poll_cursor(db, 8, 222)
    assert get_poll_cursor(db, 7) == 111
    assert get_poll_cursor(db, 8) == 222


def test_poll_cursor_invalid_rejected(tmp_path):
    db = tmp_path / "state.db"
    for bad in (-1, "111", True):
        with pytest.raises(StoreError):
            set_poll_cursor(db, 7, bad)


def test_append_preserves_acked_and_poll_cursors(tmp_path):
    db = tmp_path / "state.db"
    append_entry(db, **_fields(message_id=101))
    ack_entries(db, 1)
    set_poll_cursor(db, 7, 555)
    append_entry(db, **_fields(message_id=102))
    got = read_entries(db)
    assert got["acked"] == 1
    assert got["latest"] == 2
    assert get_poll_cursor(db, 7) == 555


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
    assert build_message_entry(**kw, guild_id=999) is None
    assert build_message_entry(**kw, guild_id=None) is None
    assert build_message_entry(**{**kw, "guild_id": 100, "author_uid": 1}) is None
    assert build_message_entry(**{**kw, "guild_id": 100, "channel_id": 8}) is None


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
    assert empty is not None
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
        owner_uid=1,
        watch_channel_ids=[7],
    )
    assert entry is None


def test_build_message_entry_legacy_call_omits_optional_fields():
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
    db = tmp_path / "state.db"
    legacy = append_entry(db, **_fields(message_id=101))
    assert "author_is_bot" not in legacy
    v5 = append_entry(
        db,
        **_fields(message_id=102, author_uid=9, author_name="OtherBot"),
        author_is_bot=True,
        reply_to_message_id=101,
    )
    assert v5["author_is_bot"] is True
    assert v5["reply_to_message_id"] == 101
    got = read_entries(db, unread_only=False)
    assert [e["message_id"] for e in got["entries"]] == [101, 102]
    assert "author_is_bot" not in got["entries"][0]
    assert got["entries"][1]["author_is_bot"] is True
    assert got["entries"][1]["reply_to_message_id"] == 101


# -- Phase 2b+3 (sub-step 1): schema v2 migration, jobs, Jev, sessions --

SID_A = "2026-10-27T10-00-00_ab12"
SID_B = "2026-10-27T11-00-00_cd34"
DAY_A = "2026-10-27"


def _seed_job(db, message_id=101, seq=None, channel_id=7):
    entry = append_entry(db, **_fields(message_id=message_id, channel_id=channel_id))
    use_seq = seq if seq is not None else entry["seq"]
    with Store.open(db) as store:
        return store.enqueue_job(channel_id, message_id, use_seq)


def _make_v1_db(path):
    import sqlite3 as _sqlite3

    conn = _sqlite3.connect(str(path), isolation_level=None)
    conn.executescript(
        """
        CREATE TABLE outbox (
            seq INTEGER PRIMARY KEY, type TEXT NOT NULL, channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL, author_uid INTEGER NOT NULL, author_name TEXT NOT NULL,
            actor_kind TEXT NOT NULL, content TEXT NOT NULL, ts TEXT NOT NULL,
            author_is_bot INTEGER, reply_to_message_id INTEGER,
            UNIQUE(channel_id, message_id));
        CREATE TABLE state (id INTEGER PRIMARY KEY CHECK(id = 1),
            acked INTEGER NOT NULL DEFAULT 0, seq_high_water INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE poll_cursors (channel_id INTEGER PRIMARY KEY,
            last_message_id INTEGER NOT NULL);
        """
    )
    conn.execute(
        "INSERT INTO outbox (seq, type, channel_id, message_id, author_uid, author_name,"
        " actor_kind, content, ts) VALUES (1, 'message', 7, 101, 2, 'Owner', 'owner', 'hi', 't');"
    )
    conn.execute("INSERT INTO state (id, acked, seq_high_water) VALUES (1, 0, 1);")
    conn.execute("PRAGMA user_version=1;")
    conn.close()


def test_v1_to_v2_migration_preserves_outbox(tmp_path):
    db = tmp_path / "state.db"
    _make_v1_db(db)
    with Store.open(db) as store:
        version = store._conn.execute("PRAGMA user_version;").fetchone()[0]
        assert version == 2
        assert store.get_native_sid() is None
        data = store.read_entries(unread_only=False)
    assert [e["message_id"] for e in data["entries"]] == [101]
    assert data["acked"] == 0
    assert data["latest"] == 1
    with Store.open(db) as store:
        tables = {
            r[0] for r in store._conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        }
    assert {"jobs", "sends", "jev_decisions", "sessions"} <= tables


def test_v1_db_without_job_rows_stops_prefix(tmp_path):
    db = tmp_path / "state.db"
    _make_v1_db(db)
    with Store.open(db) as store:
        data = store.read_entries(unread_only=False)
        assert store.processed_prefix(data["entries"], 0) == 0


def test_version_above_v2_refuses(tmp_path):
    import sqlite3 as _sqlite3

    db = tmp_path / "state.db"
    conn = _sqlite3.connect(str(db))
    conn.execute("PRAGMA user_version=3;")
    conn.close()
    with pytest.raises(StoreError):
        Store.open(db)


def test_job_lifecycle_submit_send_complete(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        job = store.get_job(7, 101)
        assert job["stage"] == "queued"
        assert job["send_count"] == 0
        assert job["jev"] is None
        sub = store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=None)
        assert sub["stage"] == "submitted"
        assert sub["job_ref"] == "r1"
        assert sub["native_sid"] == SID_A
        assert sub["pre_turn_index"] is None
        sent = store.record_send(7, 101, job_ref="r1", sent_message_id=9001)
        assert sent["stage"] == "sent"
        assert sent["send_count"] == 1
        assert sent["sent_message_ids"] == [9001]
        done = store.complete_job(7, 101, outcome="sent")
        assert done["stage"] == "done"
        assert done["outcome"] == "sent"


def test_job_enqueue_filtered_lands_skipped(tmp_path):
    db = tmp_path / "state.db"
    entry = append_entry(db, **_fields(message_id=101))
    with Store.open(db) as store:
        job = store.enqueue_job(7, 101, entry["seq"], skip_reason="out_of_scope")
    assert job["stage"] == "skipped"
    assert job["skip_reason"] == "out_of_scope"


def test_job_enqueue_duplicate_seq_rejected(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db, message_id=101)
    entry = append_entry(db, **_fields(message_id=102))
    with Store.open(db) as store:
        with pytest.raises(StoreError):
            store.enqueue_job(7, 102, 1)  # seq 1 belongs to the first job
        assert entry["seq"] == 2


def test_record_send_guards_job_ref_and_stage(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        with pytest.raises(StoreError):
            store.record_send(7, 101, job_ref="wrong", sent_message_id=9001)
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=5)
        with pytest.raises(StoreError):
            store.record_send(7, 101, job_ref="stale", sent_message_id=9001)
        ok = store.record_send(7, 101, job_ref="r1", sent_message_id=9001)
        assert ok["send_count"] == 1


def test_multi_chunk_two_sends_recorded(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        store.record_send(7, 101, job_ref="r1", sent_message_id=9001)
        job = store.record_send(7, 101, job_ref="r1", sent_message_id=9002)
        assert job["stage"] == "sent"
        assert job["send_count"] == 2
        assert job["sent_message_ids"] == [9001, 9002]


def test_skip_refused_with_send_evidence(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        store.record_send(7, 101, job_ref="r1", sent_message_id=9001)
        with pytest.raises(StoreError):
            store.skip_job(7, 101, reason="source_deleted")


def test_skip_source_deleted_zero_sends(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        skipped = store.skip_job(7, 101, reason="source_deleted")
        assert skipped["stage"] == "skipped"
        assert skipped["skip_reason"] == "source_deleted"
        # Idempotent repeat with the same reason.
        assert store.skip_job(7, 101, reason="source_deleted")["stage"] == "skipped"


def test_mark_failed_and_terminal_never_rewinds(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        failed = store.mark_failed(7, 101, kind="timeout")
        assert failed["stage"] == "failed"
        assert failed["fail_kind"] == "timeout"
        assert failed["http_status"] is None
        # Idempotent repeat.
        assert store.mark_failed(7, 101, kind="timeout")["stage"] == "failed"
        with pytest.raises(StoreError):
            store.skip_job(7, 101, reason="source_deleted")
        with pytest.raises(StoreError):
            store.complete_job(7, 101, outcome="sent")


def test_complete_guards_per_outcome(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db, message_id=101)
    _seed_job(db, message_id=102)
    _seed_job(db, message_id=103)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        store.mark_submitted(7, 102, job_ref="r2", native_sid=SID_A, pre_turn_index=0)
        # no_reply needs zero sends; sent needs >= 1.
        with pytest.raises(StoreError):
            store.complete_job(7, 101, outcome="sent")
        no_reply = store.complete_job(7, 101, outcome="no_reply")
        assert (no_reply["stage"], no_reply["outcome"]) == ("done", "no_reply")
        store.record_send(7, 102, job_ref="r2", sent_message_id=9001)
        with pytest.raises(StoreError):
            store.complete_job(7, 102, outcome="no_reply")
        # observed closes straight from queued.
        observed = store.complete_job(7, 103, outcome="observed")
        assert (observed["stage"], observed["outcome"]) == ("done", "observed")
        with pytest.raises(StoreError):
            store.complete_job(7, 102, outcome="observed")


def test_requeue_strict_409_zero_sends(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        with pytest.raises(StoreError):
            store.requeue(7, 101)  # already queued: never a silent reset
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        queued = store.requeue(7, 101)
        assert queued["stage"] == "queued"
        assert queued["job_ref"] is None


def test_sweep_submitted_failed_queued_untouched_and_not_on_open(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db, message_id=101)
    _seed_job(db, message_id=102)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
    # Ordinary opens never sweep: the submitted job survives reopen.
    with Store.open(db):
        pass
    with Store.open(db) as store:
        assert store.get_job(7, 101)["stage"] == "submitted"
        assert store.sweep_submitted_failed() == 1
        assert store.sweep_submitted_failed() == 0
        failed = store.get_job(7, 101)
        assert failed["stage"] == "failed"
        assert failed["fail_kind"] == "restart"
        assert store.get_job(7, 102)["stage"] == "queued"


def test_processed_prefix_mixed_terminals(tmp_path):
    db = tmp_path / "state.db"
    for mid in (101, 102, 103, 104):
        _seed_job(db, message_id=mid)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        store.complete_job(7, 101, outcome="no_reply")
        store.skip_job(7, 102, reason="bot_filtered")
        store.mark_submitted(7, 103, job_ref="r3", native_sid=SID_A, pre_turn_index=0)
        store.mark_failed(7, 103, kind="timeout")
        data = store.read_entries(unread_only=False)
        assert store.processed_prefix(data["entries"], 0) == 3
        # Entry 104 still queued blocks the prefix.
        assert store.processed_prefix(data["entries"], 2) == 3


def test_adoption_single_transaction(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        assert store.get_native_sid() is None
        with pytest.raises(StoreError):
            store.adopt_session(expected_current=SID_A, new_sid=SID_B, day=DAY_A)
        assert store.adopt_session(expected_current=None, new_sid=SID_A, day=DAY_A) == SID_A
        assert store.get_native_sid() == SID_A
        with pytest.raises(StoreError):
            store.adopt_session(expected_current=None, new_sid=SID_B, day=DAY_A)
        # Mid-flight job blocks adoption; drain then adopt.
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        with pytest.raises(StoreError):
            store.adopt_session(expected_current=SID_A, new_sid=SID_B, day=DAY_A)
        store.mark_failed(7, 101, kind="timeout")
        assert store.adopt_session(expected_current=SID_A, new_sid=SID_B, day=DAY_A) == SID_B
        days = {
            r[0]: r[1]
            for r in store._conn.execute("SELECT day, native_sid FROM sessions;").fetchall()
        }
        assert days == {DAY_A: SID_B}
        assert store.get_native_sid() == SID_B


def test_jev_pending_resolve_conditional(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        pending = store.insert_jev_pending(
            7,
            101,
            context_sha256="a" * 64,
            attempt_id="att1",
            author_uid=2,
            attempted_day=DAY_A,
        )
        assert pending["status"] == "pending"
        assert pending["attempts"] == 1
        with pytest.raises(StoreError):
            store.resolve_jev(
                7,
                101,
                attempt_id="stale",
                status="decided",
                participation="respond",
                input_tokens=10,
                output_tokens=1,
            )
        decided = store.resolve_jev(
            7,
            101,
            attempt_id="att1",
            status="decided",
            participation="respond",
            input_tokens=1000,
            output_tokens=5,
            answers_json="{}",
        )
        assert decided["status"] == "decided"
        assert decided["participation"] == "respond"
        assert store.jev_unknown_blocked() is False


def test_jev_retry_once_then_observe(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db)
    with Store.open(db) as store:
        store.insert_jev_pending(
            7,
            101,
            context_sha256="a" * 64,
            attempt_id="att1",
            author_uid=2,
            attempted_day=DAY_A,
        )
        # Stale attempt id cannot arm the retry.
        with pytest.raises(StoreError):
            store.bump_jev_retry(
                7,
                101,
                old_attempt_id="nope",
                new_attempt_id="att2",
                attempted_day=DAY_A,
            )
        bumped = store.bump_jev_retry(
            7,
            101,
            old_attempt_id="att1",
            new_attempt_id="att2",
            attempted_day=DAY_A,
        )
        assert bumped["attempts"] == 2
        assert bumped["attempt_id"] == "att2"
        # Second attempt cannot arm another retry.
        with pytest.raises(StoreError):
            store.bump_jev_retry(
                7,
                101,
                old_attempt_id="att2",
                new_attempt_id="att3",
                attempted_day=DAY_A,
            )
        # Unknown outcome at attempts == 2 resolves terminally as observe.
        observed = store.mark_jev_observe(7, 101)
        assert observed["status"] == "decided"
        assert observed["participation"] == "observe"
        job = store.get_job(7, 101)
        assert (job["stage"], job["outcome"]) == ("done", "observed")
        # Idempotent repeat.
        assert store.mark_jev_observe(7, 101)["participation"] == "observe"


def test_jev_failed_unknown_blocks_new(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db, message_id=101)
    _seed_job(db, message_id=102)
    with Store.open(db) as store:
        store.insert_jev_pending(
            7,
            101,
            context_sha256="a" * 64,
            attempt_id="att1",
            author_uid=2,
            attempted_day=DAY_A,
        )
        assert store.jev_unknown_blocked() is True
        store.resolve_jev(7, 101, attempt_id="att1", status="failed_unknown")
        assert store.jev_unknown_blocked() is True


def test_jev_cost_for_day_counts_known_and_lower_bound(tmp_path):
    db = tmp_path / "state.db"
    _seed_job(db, message_id=101)
    _seed_job(db, message_id=102)
    _seed_job(db, message_id=103)
    with Store.open(db) as store:
        assert store.jev_cost_for_day(DAY_A) == "0"
        store.insert_jev_pending(
            7,
            101,
            context_sha256="a" * 64,
            attempt_id="a1",
            author_uid=2,
            attempted_day=DAY_A,
        )
        store.resolve_jev(
            7,
            101,
            attempt_id="a1",
            status="decided",
            participation="respond",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert store.jev_cost_for_day(DAY_A) == "0.042"
        store.insert_jev_pending(
            7,
            102,
            context_sha256="b" * 64,
            attempt_id="a2",
            author_uid=2,
            attempted_day=DAY_A,
        )
        store.resolve_jev(
            7,
            102,
            attempt_id="a2",
            status="failed_unknown",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert store.jev_cost_for_day(DAY_A) == "0.084"
        # Other days do not leak in.
        store.insert_jev_pending(
            7,
            103,
            context_sha256="c" * 64,
            attempt_id="a3",
            author_uid=2,
            attempted_day="2026-10-28",
        )
        store.resolve_jev(
            7,
            103,
            attempt_id="a3",
            status="decided",
            participation="observe",
            input_tokens=1_000_000,
            output_tokens=0,
        )
        assert store.jev_cost_for_day(DAY_A) == "0.084"


def test_ack_advances_over_mixed_done_skipped_failed(tmp_path):
    db = tmp_path / "state.db"
    for mid in (101, 102, 103):
        _seed_job(db, message_id=mid)
    with Store.open(db) as store:
        store.mark_submitted(7, 101, job_ref="r1", native_sid=SID_A, pre_turn_index=0)
        store.complete_job(7, 101, outcome="no_reply")
        store.skip_job(7, 102, reason="bot_filtered")
        store.mark_failed(7, 103, kind="timeout")
        data = store.read_entries(unread_only=False)
        assert store.processed_prefix(data["entries"], 0) == 3
        assert store.ack_entries(3) == 3
