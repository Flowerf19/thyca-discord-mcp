"""Runtime SQL for the store (named constants + narrow builders; no code).

All ``SELECT``/``INSERT``/``UPDATE`` statements, ``PRAGMA``s, and
transaction control (``BEGIN``/``COMMIT``/``ROLLBACK``) executed by
:mod:`store.base`, :mod:`store.outbox`, :mod:`store.jobs`, and
:mod:`store.jev` live here, separated from validation and execution
logic. DDL stays in :mod:`store.schema`. The two pre-existing dynamic
statements (the ``user_version`` pragma and the variable stage
``IN``-list) get narrowly scoped builders that preserve the original
SQL text and binding exactly -- no generic query loader or framework.
"""

from __future__ import annotations

# -- transaction control (single connection, never across an await) --

TXN_BEGIN = "BEGIN IMMEDIATE;"
TXN_COMMIT = "COMMIT;"
TXN_ROLLBACK = "ROLLBACK;"

# -- open: pragmas, version gate, migration --

PRAGMA_JOURNAL_WAL = "PRAGMA journal_mode=WAL;"
PRAGMA_SYNCHRONOUS_FULL = "PRAGMA synchronous=FULL;"
PRAGMA_FOREIGN_KEYS = "PRAGMA foreign_keys=ON;"
PRAGMA_BUSY_TIMEOUT = "PRAGMA busy_timeout=5000;"
PRAGMA_QUICK_CHECK = "PRAGMA quick_check;"
PRAGMA_USER_VERSION = "PRAGMA user_version;"
PRAGMA_TABLE_INFO_STATE = "PRAGMA table_info(state);"
TABLES_LIST = "SELECT name FROM sqlite_master WHERE type='table';"
STATE_ADD_SID_COLUMN = "ALTER TABLE state ADD COLUMN native_sid TEXT;"
STATE_SEED = "INSERT OR IGNORE INTO state (id, acked, seq_high_water) VALUES (1, 0, 0);"


def set_user_version(version: int) -> str:
    """Version-stamping pragma (dynamic only in the version number)."""
    return f"PRAGMA user_version={version};"


# -- state singleton --

STATE_GET = "SELECT acked, seq_high_water FROM state WHERE id=1;"
STATE_BUMP_HIGH_WATER = "UPDATE state SET seq_high_water=? WHERE id=1;"
STATE_SET_ACKED = "UPDATE state SET acked=? WHERE id=1;"
STATE_GET_SID = "SELECT native_sid FROM state WHERE id=1;"
STATE_SET_SID = "UPDATE state SET native_sid=? WHERE id=1;"

# -- outbox --

OUTBOX_INSERT = (
    "INSERT OR IGNORE INTO outbox "
    "(seq, type, channel_id, message_id, author_uid, author_name, "
    "actor_kind, content, ts, author_is_bot, reply_to_message_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
)
OUTBOX_GET = "SELECT * FROM outbox WHERE channel_id=? AND message_id=?;"
OUTBOX_READ_UNREAD = "SELECT * FROM outbox WHERE seq > ? ORDER BY seq ASC LIMIT ?;"
OUTBOX_READ_ALL = "SELECT * FROM outbox ORDER BY seq ASC LIMIT ?;"

# -- poll cursors --

CURSOR_GET = "SELECT last_message_id FROM poll_cursors WHERE channel_id=?;"
CURSOR_UPSERT = (
    "INSERT INTO poll_cursors (channel_id, last_message_id)"
    " VALUES (?, ?)"
    " ON CONFLICT(channel_id) DO UPDATE"
    " SET last_message_id=excluded.last_message_id;"
)

# -- jobs --

JOB_GET = "SELECT * FROM jobs WHERE channel_id=? AND message_id=?;"
JOB_INSERT = (
    "INSERT INTO jobs (channel_id, message_id, seq, stage, skip_reason,"
    " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?);"
)
JOB_LIST_ALL = "SELECT * FROM jobs ORDER BY seq ASC;"


def jobs_in_stages(placeholders: str) -> str:
    """Stage-filtered FIFO scan (placeholders built from the wanted stages)."""
    return f"SELECT * FROM jobs WHERE stage IN ({placeholders}) ORDER BY seq ASC;"


JOB_FIRST_UNRESOLVED = (
    "SELECT * FROM jobs WHERE stage NOT IN ('done','skipped','failed') ORDER BY seq ASC LIMIT 1;"
)
JOB_SEQ_STAGES_AFTER = "SELECT seq, stage FROM jobs WHERE seq > ?;"
JOB_MID_FLIGHT_COUNT = "SELECT COUNT(*) FROM jobs WHERE stage IN ('submitted','sent');"
JOB_MARK_SUBMITTED = (
    "UPDATE jobs SET stage='submitted', job_ref=?, native_sid=?,"
    " pre_turn_index=?, updated_at=? WHERE channel_id=? AND message_id=?"
    " AND stage='queued';"
)
JOB_REQUEUE = (
    "UPDATE jobs SET stage='queued', job_ref=NULL, pre_turn_index=NULL,"
    " updated_at=? WHERE channel_id=? AND message_id=? AND stage='submitted';"
)
JOB_RECORD_SEND = (
    "UPDATE jobs SET stage='sent', updated_at=? WHERE channel_id=?"
    " AND message_id=? AND job_ref=? AND stage IN ('submitted','sent');"
)
SENDS_FOR = "SELECT sent_message_id FROM sends WHERE channel_id=? AND message_id=? ORDER BY rowid;"
SEND_INSERT = (
    "INSERT OR IGNORE INTO sends (channel_id, message_id, sent_message_id,"
    " created_at) VALUES (?, ?, ?, ?);"
)
JOB_SKIP = (
    "UPDATE jobs SET stage='skipped', skip_reason=?, updated_at=?"
    " WHERE channel_id=? AND message_id=?"
    " AND stage IN ('queued','submitted','sent')"
    " AND NOT EXISTS (SELECT 1 FROM sends s WHERE s.channel_id=jobs.channel_id"
    " AND s.message_id=jobs.message_id);"
)
JOB_FAIL = (
    "UPDATE jobs SET stage='failed', fail_kind=?, http_status=?,"
    " updated_at=? WHERE channel_id=? AND message_id=?"
    " AND stage IN ('queued','submitted','sent');"
)
JOB_COMPLETE = (
    "UPDATE jobs SET stage='done', outcome=?, updated_at=? WHERE channel_id=? AND message_id=?;"
)
JOB_CLOSE_OBSERVED = (
    "UPDATE jobs SET stage='done', outcome='observed', updated_at=?"
    " WHERE channel_id=? AND message_id=? AND stage='queued';"
)
JOB_SWEEP_SUBMITTED = (
    "UPDATE jobs SET stage='failed', fail_kind='restart', http_status=NULL,"
    " updated_at=? WHERE stage='submitted';"
)

# -- sessions --

SESSION_UPSERT = (
    "INSERT INTO sessions (day, native_sid, adopted_at) VALUES (?, ?, ?)"
    " ON CONFLICT(day) DO UPDATE SET native_sid=excluded.native_sid,"
    " adopted_at=excluded.adopted_at;"
)

# -- jev decisions --

JEV_GET = "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;"
JEV_INSERT_PENDING = (
    "INSERT INTO jev_decisions (channel_id, message_id, context_sha256,"
    " status, attempts, attempt_id, author_uid, attempted_day,"
    " created_at, updated_at)"
    " VALUES (?, ?, ?, 'pending', 1, ?, ?, ?, ?, ?);"
)
JEV_BUMP_RETRY = (
    "UPDATE jev_decisions SET attempts=2, attempt_id=?, attempted_day=?,"
    " updated_at=? WHERE channel_id=? AND message_id=? AND status='pending'"
    " AND attempt_id=? AND attempts < 2;"
)
JEV_RESOLVE = (
    "UPDATE jev_decisions SET status=?, participation=?, input_tokens=?,"
    " output_tokens=?, answers_json=?, updated_at=?"
    " WHERE channel_id=? AND message_id=? AND status='pending' AND attempt_id=?;"
)
JEV_OBSERVE = (
    "UPDATE jev_decisions SET status='decided', participation='observe',"
    " input_tokens=?, output_tokens=?, updated_at=?"
    " WHERE channel_id=? AND message_id=? AND status='pending';"
)
JEV_COST_ROWS = (
    "SELECT input_tokens FROM jev_decisions WHERE attempted_day=?"
    " AND status IN ('decided','failed_known','failed_unknown')"
    " AND input_tokens IS NOT NULL;"
)
JEV_BLOCKED_COUNT = (
    "SELECT COUNT(*) FROM jev_decisions WHERE status IN ('pending','failed_unknown');"
)
