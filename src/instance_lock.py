"""Single-instance guard: one live holder via a flock lock file.

One OS process holds an advisory exclusive lock beside the SQLite store
for its whole lifetime; a second starter fails immediately with
:class:`InstanceLockBusy` (never waits). Stale-safe: the kernel releases
flock on ANY process death. The file payload (PID + start time) is
diagnostic only; busy messages carry path + PID. Unix-only (fcntl).
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOCK_BASENAME = ".discord_mcp.lock"


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


def derive_lock_path(db_path: str | Path) -> Path:
    """Lock file beside the SQLite store (fixed basename)."""
    return Path(db_path).parent / LOCK_BASENAME


def _describe_holder(raw: bytes) -> str:
    """Best-effort holder PID; never raises, never dumps secrets."""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return "holder info unreadable"
    pid = data.get("pid") if isinstance(data, dict) else None
    if not isinstance(pid, int) or pid <= 0:
        return "holder info unreadable"
    return f"pid {pid}"


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
        """Release the lock (idempotent; also runs via __exit__)."""
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._fd)
        except OSError:
            pass

    def __enter__(self) -> ServiceLock:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def __del__(self) -> None:  # pragma: no cover - interpreter teardown
        try:
            self.release()
        except Exception:
            pass


def acquire_service_lock(lock_path: str | Path) -> ServiceLock:
    """Take the exclusive lock or raise :class:`InstanceLockBusy`."""
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
            {"pid": os.getpid(), "started_at": datetime.now(timezone.utc).isoformat()}
        ).encode()
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
