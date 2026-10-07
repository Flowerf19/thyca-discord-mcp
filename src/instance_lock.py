"""Single-instance guard: one writer per delivery state directory.

The MCP server owns its journal files (delivery/outbox/ledger) under
single-process / single-loop discipline. A second OS process (detached
daemon + MCP-client auto-spawn + manual terminal run) would corrupt that
discipline: Discord double-login flapping on one token, journal
read-modify-write races, duplicate native POSTs, crossed receipts. This
module enforces one live holder with an advisory ``fcntl.flock``
exclusive lock held for the whole process lifetime:

- ``acquire_service_lock`` opens (creating 0600) a lock file beside the
  delivery state and takes ``LOCK_EX | LOCK_NB``. A second holder fails
  IMMEDIATELY with :class:`InstanceLockBusy` (never waits, never runs
  degraded) carrying best-effort holder diagnostics (pid + start time).
- Stale-safe by construction: the kernel releases ``flock`` on ANY
  process death (crash, SIGKILL, power loss), so a dead holder never
  blocks the next start. The PID recorded in the file is DIAGNOSTIC
  ONLY -- lock decisions never consult it.
- No work happens on import. No network, no clock reads except the
  holder's own start timestamp. Unix-only (``fcntl``), matching the
  existing ``O_DIRECTORY`` store code.
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOCK_BASENAME = ".discord_mcp.lock"
_CMD_MAX = 200


class InstanceLockError(OSError):
    """Base for lock-file failures (not lock contention)."""


class InstanceLockBusy(InstanceLockError):
    """Another live process holds the service lock (refuse to start)."""

    def __init__(self, lock_path: Path, holder: str) -> None:
        self.lock_path = lock_path
        self.holder = holder
        super().__init__(
            f"another instance holds {lock_path} ({holder}); refusing to start (single-writer)"
        )


def derive_lock_path(delivery_state_path: str | Path) -> Path:
    """Return the lock file beside the delivery state (fixed basename)."""
    return Path(delivery_state_path).parent / LOCK_BASENAME


def _describe_holder(raw: bytes) -> str:
    """Best-effort holder diagnostics; never raises, never dumps secrets."""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return "holder info unreadable"
    if not isinstance(data, dict):
        return "holder info unreadable"
    pid = data.get("pid")
    started = data.get("started_at")
    if not isinstance(pid, int) or pid <= 0:
        return "holder info unreadable"
    if not isinstance(started, str) or not started or len(started) > 64:
        return f"pid {pid}"
    return f"pid {pid} since {started}"


def _current_cmd() -> str:
    """Bounded command description for holder diagnostics (no secrets read)."""
    import sys

    try:
        parts = [str(a) for a in sys.argv[:4]]
    except Exception:
        return ""
    return " ".join(parts)[:_CMD_MAX]


class ServiceLock:
    """A held exclusive service lock; closing releases it (idempotent)."""

    def __init__(self, fd: int, path: Path) -> None:
        self._fd = fd
        self._path = path
        self._closed = False

    @property
    def path(self) -> Path:
        return self._path

    def release(self) -> None:
        """Release the lock (idempotent; also runs via __exit__/__del__)."""
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._fd)
        except OSError:
            pass

    def __enter__(self) -> ServiceLock:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()

    def __del__(self) -> None:  # pragma: no cover - interpreter teardown
        try:
            self.release()
        except Exception:
            pass


def acquire_service_lock(lock_path: str | Path) -> ServiceLock:
    """Take the exclusive service lock or raise :class:`InstanceLockBusy`.

    Refuses symlink lock paths and missing parents (no directory
    creation). Creates a missing lock file 0600. On contention reads
    the holder's diagnostics best-effort for the error message. The
    returned handle MUST stay referenced (and unreleased) for the whole
    process lifetime; dropping it releases the lock.
    """
    path = Path(lock_path)
    if path.is_symlink():
        raise InstanceLockError(f"service lock {path} must not be a symlink")
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise InstanceLockError(f"service lock parent {parent} is not a directory")
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as exc:
        raise InstanceLockError(f"service lock {path} open failed") from exc
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, PermissionError):
        holder = "holder info unreadable"
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            holder = _describe_holder(os.read(fd, 4096))
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass
        raise InstanceLockBusy(path, holder) from None
    except OSError as exc:
        try:
            os.close(fd)
        except OSError:
            pass
        raise InstanceLockError(f"service lock {path} failed") from exc
    try:
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "cmd": _current_cmd(),
            }
        ).encode("utf-8")
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    except OSError as exc:
        try:
            os.close(fd)
        except OSError:
            pass
        raise InstanceLockError(f"service lock {path} write failed") from exc
    return ServiceLock(fd, path)


__all__ = [
    "LOCK_BASENAME",
    "InstanceLockBusy",
    "InstanceLockError",
    "ServiceLock",
    "acquire_service_lock",
    "derive_lock_path",
]
