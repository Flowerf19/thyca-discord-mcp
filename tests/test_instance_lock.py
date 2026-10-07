"""Offline tests for the single-instance lock (tmp files only).

Covers acquire/release, immediate contention failure with holder
diagnostics, kernel release on holder death (no stale locks),
path guards, lock-path derivation, and the ``main()`` startup gate
(exit codes, server never reached on contention).
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from instance_lock import (
    InstanceLockBusy,
    InstanceLockError,
    acquire_service_lock,
    derive_lock_path,
)


def test_acquire_release_roundtrip_writes_diagnostics_0600(tmp_path):
    lock_p = tmp_path / ".discord_mcp.lock"
    lock = acquire_service_lock(lock_p)
    try:
        assert stat.S_IMODE(lock_p.stat().st_mode) == 0o600
        data = json.loads(lock_p.read_text(encoding="utf-8"))
        assert data["pid"] == os.getpid()
        assert isinstance(data["started_at"], str) and data["started_at"]
        assert lock.path == lock_p
    finally:
        lock.release()
        lock.release()  # idempotent


def test_second_acquire_fails_immediately_with_holder_pid(tmp_path):
    lock_p = tmp_path / ".discord_mcp.lock"
    first = acquire_service_lock(lock_p)
    try:
        with pytest.raises(InstanceLockBusy) as caught:
            acquire_service_lock(lock_p)
        assert f"pid {os.getpid()}" in str(caught.value)
        assert "single-writer" in str(caught.value)
        assert caught.value.lock_path == lock_p
    finally:
        first.release()
    # After release the same path acquires cleanly again.
    second = acquire_service_lock(lock_p)
    second.release()


def test_holder_death_releases_without_stale_state(tmp_path):
    lock_p = tmp_path / ".discord_mcp.lock"
    repo_root = Path(__file__).resolve().parent.parent
    script = (
        "import os, sys; sys.path.insert(0, 'src');"
        "from instance_lock import acquire_service_lock;"
        f"acquire_service_lock({str(lock_p)!r});"
        "print(os.getpid(), flush=True);"
    )
    # The child acquires and exits WITHOUT releasing: the kernel must
    # release the flock on process death (exit code 0, no cleanup call).
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    stale = json.loads(lock_p.read_text(encoding="utf-8"))
    assert stale["pid"] == int(proc.stdout.strip())
    # A new holder acquires despite the stale PID content (diagnostic only).
    lock = acquire_service_lock(lock_p)
    try:
        fresh = json.loads(lock_p.read_text(encoding="utf-8"))
        assert fresh["pid"] == os.getpid()
    finally:
        lock.release()


def test_refuses_symlink_and_missing_parent(tmp_path):
    target = tmp_path / "real.lock"
    link = tmp_path / "link.lock"
    link.symlink_to(target)
    with pytest.raises(InstanceLockError, match="must not be a symlink"):
        acquire_service_lock(link)
    with pytest.raises(InstanceLockError, match="is not a directory"):
        acquire_service_lock(tmp_path / "no-such-dir" / ".discord_mcp.lock")


def test_derive_lock_path_sits_beside_delivery_state(tmp_path):
    delivery = tmp_path / "sub" / "delivery.json"
    assert derive_lock_path(delivery) == tmp_path / "sub" / ".discord_mcp.lock"


def _env_for_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_TOKEN", "fake-token")
    monkeypatch.setenv("OWNER_UID", "300000000000000003")
    monkeypatch.setenv("GUILD_ID", "100000000000000001")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "200000000000000002")
    monkeypatch.setenv("DELIVERY_STATE_PATH", str(tmp_path / "delivery.json"))


def test_main_refuses_second_instance_without_starting_server(tmp_path, monkeypatch, capsys):
    import main as main_mod

    _env_for_settings(monkeypatch, tmp_path)
    (tmp_path / "delivery.json").write_text("{}", encoding="utf-8")
    first = acquire_service_lock(derive_lock_path(tmp_path / "delivery.json"))
    try:

        def _must_not_run(settings):
            raise AssertionError("server must not start on contention")

        monkeypatch.setattr(main_mod, "create_server", _must_not_run)
        with pytest.raises(SystemExit) as caught:
            main_mod.main()
        assert caught.value.code == 2
        err = capsys.readouterr().err
        assert "another instance holds" in err
        assert "single-writer" in err
    finally:
        first.release()


def test_main_exits_3_on_broken_lock_path(tmp_path, monkeypatch, capsys):
    import main as main_mod

    _env_for_settings(monkeypatch, tmp_path)
    monkeypatch.setenv("DELIVERY_STATE_PATH", str(tmp_path / "no-such-dir" / "d.json"))

    def _must_not_run(settings):
        raise AssertionError("server must not start on lock error")

    monkeypatch.setattr(main_mod, "create_server", _must_not_run)
    with pytest.raises(SystemExit) as caught:
        main_mod.main()
    assert caught.value.code == 3
    assert "service lock" in capsys.readouterr().err


def test_main_releases_lock_on_clean_shutdown(tmp_path, monkeypatch):
    import main as main_mod

    _env_for_settings(monkeypatch, tmp_path)
    (tmp_path / "delivery.json").write_text("{}", encoding="utf-8")

    class _Server:
        def run(self, transport):
            assert transport == "stdio"

    monkeypatch.setattr(main_mod, "create_server", lambda settings: _Server())
    main_mod.main()  # returns normally; finally releases
    # Re-acquirable immediately afterwards (no lingering hold).
    lock = acquire_service_lock(derive_lock_path(tmp_path / "delivery.json"))
    lock.release()
    assert isinstance(Path(str(lock.path)), Path)
