"""Delivery storage: file commit/load primitives for delivery state.

Cycle-free file-I/O owner: parent-dir setup, directory fsyncs, atomic
replace saves, fresh loads, and exclusive fresh-file creation. Imports
only schema errors/validators; never imports ``delivery``.

Write primitives take a REQUIRED explicit ``fsync_parent_dir`` callback
(no default) so the mutation owner (root ``delivery``) supplies its
current fsync hook freshly at each call. This preserves the existing
root patch hook (``delivery._fsync_parent_dir``) for durability-fault
tests without a different owner silently bypassing it.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .delivery_schema import DeliveryDurabilityUncertain, DeliveryError, _validate_state


def _ensure_parent_dir(path: Path) -> None:
    """Create only missing parents as 0700; never touch existing parents."""
    parent = path.parent
    if parent.exists():
        if parent.is_symlink() or not parent.is_dir():
            raise DeliveryError(f"delivery parent {parent} is not a directory")
        return
    # Collect missing chain top-down so only newly created dirs get 0700.
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
        if len(missing) > 64:
            raise DeliveryError(f"delivery parent {parent} is too deep")
    if cursor.is_symlink() or not cursor.is_dir():
        raise DeliveryError(f"delivery parent {cursor} is not a directory")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise DeliveryError(f"delivery parent {directory} is not a directory") from None
            continue
        # mkdir mode is masked by umask; chmod only this newly owned dir.
        os.chmod(directory, 0o700)


def _fsync_parent_dir(parent: Path, state_path: Path) -> None:
    """Fsync the parent directory after a file commit (namespace durability).

    Must be called AFTER os.replace/os.link. On failure raises
    DeliveryDurabilityUncertain (committed=True) without raw OSError text.
    """
    fd = None
    try:
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise DeliveryDurabilityUncertain(state_path) from None
    try:
        try:
            os.fsync(fd)
        except OSError:
            raise DeliveryDurabilityUncertain(state_path) from None
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _load_state(path: str | Path) -> dict[str, Any]:
    file = Path(path)
    if file.is_symlink():
        raise DeliveryError(f"delivery state {file} must not be a symlink")
    if not file.exists():
        raise DeliveryError(f"delivery state {file} is missing; initialize explicitly")
    if not file.is_file():
        raise DeliveryError(f"delivery state {file} is not a regular file")
    try:
        raw = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        # Bounded position metadata only; never echo the raw file body.
        raise DeliveryError(
            f"corrupt delivery state {file}: invalid JSON at line {e.lineno} col {e.colno}"
        ) from None
    return _validate_state(raw, file)


def _save_state(
    path: str | Path,
    state: dict[str, Any],
    *,
    fsync_parent_dir: Callable[[Path, Path], None],
) -> None:
    """Atomically replace an existing state file via a private temp inode.

    ``fsync_parent_dir`` is a REQUIRED explicit callback (no default) for
    post-commit namespace durability. The mutation owner passes its current
    fsync hook freshly at each call; storage never falls back to its own
    global, so a different owner cannot be silently bypassed.
    """
    file = Path(path)
    if file.is_symlink():
        raise DeliveryError(f"delivery state {file} must not be a symlink")
    if not file.exists():
        raise DeliveryError(f"delivery state {file} is missing; initialize explicitly")
    if not file.is_file():
        raise DeliveryError(f"delivery state {file} is not a regular file")
    parent = file.parent
    if parent.is_symlink() or not parent.is_dir():
        raise DeliveryError(f"delivery parent {parent} is not a directory")
    fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=file.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, file)
    except BaseException:
        # Pre-commit failure: clean up ONLY our own temp inode. A silent
        # unlink failure can leave one known owned orphan tmp; it never
        # means the state is uncommitted, and the state file itself is
        # untouched (original bytes preserved).
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    # Committed via os.replace (the temp path is already gone). The
    # directory entry needs its own fsync for namespace durability; on
    # failure raise DeliveryDurabilityUncertain WITHOUT rewinding,
    # deleting the committed file, or carrying raw OSError text.
    fsync_parent_dir(parent, file)


def _create_state_exclusive(
    file: Path,
    state: dict[str, Any],
    *,
    fsync_parent_dir: Callable[[Path, Path], None],
) -> None:
    """Commit a validated fresh state via exclusive hard-link.

    Post-assembled block only: private 0600 temp inode + flush/fsync +
    exclusive ``os.link`` commit, best-effort temp unlink, then the REQUIRED
    explicit ``fsync_parent_dir`` callback for namespace durability. Caller
    owns pre-disk validation, early existing-path refusal, and parent-dir
    creation; this primitive never validates, refuses, or creates parents.
    A post-link fsync failure raises DeliveryDurabilityUncertain with the
    state already present (later init still refuses it).
    """
    fd, tmp_name = tempfile.mkstemp(dir=str(file.parent), prefix=file.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp_path, file)
        except FileExistsError as e:
            raise DeliveryError(
                f"delivery state {file} already exists; refusing to overwrite"
            ) from e
    except BaseException:
        # Own temp cleanup only; the state path is never touched here.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    # Exclusive hard-link commit succeeded: drop our temp link (best
    # effort), then fsync the parent for namespace durability. A failing
    # directory fsync raises DeliveryDurabilityUncertain with the state
    # already present; later init still refuses it (exclusive bootstrap).
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass
    fsync_parent_dir(file.parent, file)
