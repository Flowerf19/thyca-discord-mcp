from __future__ import annotations

"""SQLite DDL for the store (schema text only; no code)."""

SCHEMA_VERSION = 2

_SCHEMA_V1_SQL = """
CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY,
    type TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    author_uid INTEGER NOT NULL,
    author_name TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    content TEXT NOT NULL,
    ts TEXT NOT NULL,
    author_is_bot INTEGER,
    reply_to_message_id INTEGER,
    UNIQUE(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    acked INTEGER NOT NULL DEFAULT 0,
    seq_high_water INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS poll_cursors (
    channel_id INTEGER PRIMARY KEY,
    last_message_id INTEGER NOT NULL
);
"""

_SCHEMA_V2_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    seq INTEGER NOT NULL UNIQUE REFERENCES outbox(seq),
    job_ref TEXT,
    stage TEXT NOT NULL CHECK(stage IN
        ('queued','submitted','sent','done','skipped','failed')),
    outcome TEXT CHECK(outcome IN ('sent','no_reply','observed')),
    skip_reason TEXT CHECK(skip_reason IN ('out_of_scope','bot_filtered',
        'source_deleted','source_edited_after_classify')),
    fail_kind TEXT CHECK(fail_kind IN ('timeout','transport_error','http_error',
        'busy','post_cancelled','turn_no_outcome','restart','shutdown','snapshot_mismatch')),
    http_status INTEGER,
    native_sid TEXT,
    pre_turn_index INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_job_ref ON jobs(job_ref) WHERE job_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_stage_seq ON jobs(stage, seq);
CREATE TABLE IF NOT EXISTS sends (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sent_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id, sent_message_id),
    FOREIGN KEY (channel_id, message_id) REFERENCES jobs(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS jev_decisions (
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    context_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','decided','failed_known','failed_unknown')),
    attempts INTEGER NOT NULL CHECK(attempts >= 1),
    attempt_id TEXT NOT NULL,
    participation TEXT CHECK(participation IN ('respond','observe','need_context')),
    author_uid INTEGER NOT NULL,
    attempted_day TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    answers_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_id),
    FOREIGN KEY (channel_id, message_id) REFERENCES jobs(channel_id, message_id)
);
CREATE INDEX IF NOT EXISTS jev_status ON jev_decisions(status);
CREATE INDEX IF NOT EXISTS jev_day ON jev_decisions(attempted_day);
CREATE TABLE IF NOT EXISTS sessions (
    day TEXT PRIMARY KEY,
    native_sid TEXT NOT NULL,
    adopted_at TEXT NOT NULL
);
"""
