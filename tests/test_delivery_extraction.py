"""Extraction tests: delivery responsibility refactor (schema/storage vs root).

Portable regression checks only (no baseline fixtures shipped). Verifies
the parent-chosen T4 cuts without weakening existing tests:
- Canonical single-owner identities (schema errors/constants/validators,
  storage FS primitives, root mutation ownership).
- Root runtime patch hooks preserved (``_now_iso`` timestamp, root
  ``_fsync_parent_dir`` faults for init + replace with committed files).
- Structural ownership (moved helpers no longer defined in root, still
  same-object aliases via current source only).
Snapshot AST acceptance (unchanged-mutation and verbatim-schema/storage
comparison against the pre-refactor baseline) lives in a private
harness outside the repo (TMP allowed there, never a permanent test
dependency), not shipped for clean-clone CI.

Offline file-state only (tmp paths, fake IDs). No network, env, native,
or production reads.
"""

import ast
import inspect
from pathlib import Path

import pytest

from modum import delivery as delivery_mod
from modum import delivery_schema as schema_mod
from modum import delivery_storage as storage_mod
from modum.delivery import (
    DeliveryDurabilityUncertain,
    DeliveryError,
    enqueue_job,
    initialize_delivery_state,
    load_delivery_state,
)

FAKE_SID = "2026-01-01T00-00-00_aaaa"

SCHEMA_CONSTS = [
    "ACTOR_KINDS",
    "DELIVERY_VERSION",
    "JEV_MODEL_PINNED",
    "JEV_PARTICIPATIONS",
    "MAX_ID_LEN",
    "MAX_URL_LEN",
    "NO_REPLY_REASONS",
    "OBSERVATION_ORIGINS",
    "SAFE_TERMINAL_STAGES",
    "SHA256_HEX_LEN",
    "SKIP_REASONS",
    "STAGES",
    "SUBMITTED_TEXT_MAX",
    "TERMINAL_STAGES",
    "UNCERTAIN_KINDS",
]

SCHEMA_FUNCS = [
    "_has_whitespace",
    "_is_attempt_id",
    "_is_hex",
    "_is_sha256_hex",
    "_is_strict_int",
    "_job_key",
    "_parse_job_key",
    "_submitted_pair_ok",
    "_validate_authorized_skip",
    "_validate_historical",
    "_validate_id_string",
    "_validate_intent",
    "_validate_jev_decision",
    "_validate_job",
    "_validate_no_reply_declaration",
    "_validate_observation",
    "_validate_receipt",
    "_validate_state",
]


def test_canonical_error_identities_single_leaf():
    assert delivery_mod.DeliveryError is schema_mod.DeliveryError
    assert delivery_mod.DeliveryDurabilityUncertain is schema_mod.DeliveryDurabilityUncertain
    assert storage_mod.DeliveryError is schema_mod.DeliveryError
    assert storage_mod.DeliveryDurabilityUncertain is schema_mod.DeliveryDurabilityUncertain
    assert DeliveryError is schema_mod.DeliveryError
    assert DeliveryDurabilityUncertain is schema_mod.DeliveryDurabilityUncertain
    assert issubclass(DeliveryDurabilityUncertain, DeliveryError)
    assert issubclass(DeliveryError, ValueError)


def test_canonical_const_validator_aliases_same_objects():
    for name in SCHEMA_CONSTS:
        assert hasattr(delivery_mod, name), f"delivery missing {name}"
        assert hasattr(schema_mod, name), f"schema missing {name}"
        assert getattr(delivery_mod, name) is getattr(schema_mod, name), name
    for name in SCHEMA_FUNCS:
        assert hasattr(delivery_mod, name), f"delivery missing {name}"
        assert hasattr(schema_mod, name), f"schema missing {name}"
        assert getattr(delivery_mod, name) is getattr(schema_mod, name), name
        # Single owner: canonical __module__ is schema (import may alter it, fine).
        assert getattr(schema_mod, name).__module__ == "modum.delivery_schema"


def test_storage_aliases_and_root_ownership():
    # Direct aliases: same objects.
    assert delivery_mod._ensure_parent_dir is storage_mod._ensure_parent_dir
    assert delivery_mod._fsync_parent_dir is storage_mod._fsync_parent_dir
    assert delivery_mod._load_state is storage_mod._load_state
    # Root-owned: defined in delivery, not aliases.
    assert delivery_mod._now_iso.__module__ == "modum.delivery"
    assert delivery_mod._save_state.__module__ == "modum.delivery"
    assert delivery_mod._save_state is not storage_mod._save_state
    assert delivery_mod._new_job.__module__ == "modum.delivery"
    assert not hasattr(schema_mod, "_now_iso")
    assert not hasattr(storage_mod, "_now_iso")
    assert not hasattr(schema_mod, "_save_state")
    # No thin all-public facade: root still defines all public mutations.
    for name in [
        "initialize_delivery_state",
        "load_delivery_state",
        "enqueue_job",
        "get_job",
        "list_jobs",
        "mark_turn_submitting",
        "mark_inflight",
        "requeue_definitive_busy",
        "record_send_intent",
        "record_send_receipt",
        "complete_job",
        "skip_job",
        "mark_uncertain",
        "bind_jev_ledger",
        "bind_jev_decision",
        "observe_job",
        "record_no_reply",
        "complete_observation",
        "processed_seq_prefix",
    ]:
        assert getattr(delivery_mod, name).__module__ == "modum.delivery", name


def test_storage_write_requires_explicit_typed_fsync_callback():
    for fname in ("_save_state", "_create_state_exclusive"):
        fn = getattr(storage_mod, fname)
        sig = inspect.signature(fn)
        assert "fsync_parent_dir" in sig.parameters, fname
        param = sig.parameters["fsync_parent_dir"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY, fname
        assert param.default is inspect.Parameter.empty, f"{fname} must have no default"
        assert param.annotation is not inspect.Parameter.empty, f"{fname} must be typed"
    # Root wrapper takes only (path, state) and supplies the hook freshly.
    sig = inspect.signature(delivery_mod._save_state)
    assert list(sig.parameters) == ["path", "state"]
    src = inspect.getsource(delivery_mod._save_state)
    assert "fsync_parent_dir=_fsync_parent_dir" in src
    assert "_storage_save_state" in src
    init_src = inspect.getsource(delivery_mod.initialize_delivery_state)
    assert "_storage_create_state_exclusive" in init_src
    assert "fsync_parent_dir=_fsync_parent_dir" in init_src


def test_timestamp_root_patch_propagated(tmp_path, monkeypatch):
    fixed = "2026-10-05T00:00:00+00:00"
    monkeypatch.setattr(delivery_mod, "_now_iso", lambda: fixed)
    path = tmp_path / "delivery_state.json"
    initialize_delivery_state(
        path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    job = enqueue_job(path, channel_id=7, message_id=100, author_uid=2, actor_kind="owner", seq=1)
    assert job["created_at"] == fixed
    assert job["updated_at"] == fixed
    # Second mutation with a new fixed time updates only updated_at.
    fixed2 = "2026-10-05T00:00:01+00:00"
    monkeypatch.setattr(delivery_mod, "_now_iso", lambda: fixed2)
    job2 = enqueue_job(path, channel_id=7, message_id=100, author_uid=2, actor_kind="owner", seq=2)
    assert job2["created_at"] == fixed
    assert job2["updated_at"] == fixed2
    assert job2["seq_aliases"] == [2]


def test_root_fsync_fault_on_init_commits_file_and_stays_exclusive(tmp_path, monkeypatch):
    path = tmp_path / "delivery_state.json"

    def _boom(parent, state_path):
        raise DeliveryDurabilityUncertain(state_path)

    monkeypatch.setattr(delivery_mod, "_fsync_parent_dir", _boom)
    with pytest.raises(DeliveryDurabilityUncertain) as exc:
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )
    err = exc.value
    assert isinstance(err, DeliveryError)
    assert err.committed is True
    assert "No space" not in str(err)
    assert "Traceback" not in str(err)
    # Committed file exists and validates; later init still refuses it.
    monkeypatch.undo()
    state = load_delivery_state(path)
    assert state["native_session_id"] == FAKE_SID
    assert state["jobs"] == {}
    with pytest.raises(DeliveryError, match="already exists"):
        initialize_delivery_state(
            path,
            native_session_id=FAKE_SID,
            baseline_acked=0,
            historical_message_ids={},
            authorized_skips=[],
        )


def test_root_fsync_fault_on_replace_commits_job_and_raises_uncertain(tmp_path, monkeypatch):
    path = tmp_path / "delivery_state.json"
    initialize_delivery_state(
        path,
        native_session_id=FAKE_SID,
        baseline_acked=0,
        historical_message_ids={},
        authorized_skips=[],
    )
    real = delivery_mod._fsync_parent_dir
    calls = {"n": 0}

    def _fault_once(parent, state_path):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DeliveryDurabilityUncertain(state_path)
        return real(parent, state_path)

    monkeypatch.setattr(delivery_mod, "_fsync_parent_dir", _fault_once)
    with pytest.raises(DeliveryDurabilityUncertain) as exc:
        enqueue_job(path, channel_id=7, message_id=101, author_uid=2, actor_kind="owner", seq=1)
    assert exc.value.committed is True
    monkeypatch.undo()
    # Committed job preserved (replace succeeded, only namespace fsync faulted).
    job = load_delivery_state(path)["jobs"]["7:101"]
    assert job["stage"] == "queued"
    assert job["seq"] == 1


def test_moved_helpers_no_longer_defined_in_root():
    curr_tree = ast.parse(Path(delivery_mod.__file__).read_text(encoding="utf-8"))
    defined = {
        node.name for node in curr_tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
    }
    for name in SCHEMA_FUNCS + ["DeliveryError", "DeliveryDurabilityUncertain"]:
        assert name not in defined, f"{name} must live in schema, not root"
    for name in ["_ensure_parent_dir", "_fsync_parent_dir", "_load_state"]:
        assert name not in defined, f"{name} must live in storage, not root"
    # But still accessible as same-object aliases.
    for name in SCHEMA_FUNCS:
        assert getattr(delivery_mod, name) is getattr(schema_mod, name)
    assert delivery_mod._ensure_parent_dir is storage_mod._ensure_parent_dir
    assert delivery_mod._load_state is storage_mod._load_state
