"""Durable atomic file IO for the participation ledger (no provider calls)."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import ParticipationDurabilityUncertain, ParticipationError
from .validate import _validate_ledger


def _default_day_provider() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    if parent.exists():
        if parent.is_symlink() or not parent.is_dir():
            raise ParticipationError(f"participation parent {parent} is not a directory")
        return
    missing: list[Path] = []
    cursor = parent
    while not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
        if len(missing) > 64:
            raise ParticipationError(f"participation parent {parent} is too deep")
    if cursor.is_symlink() or not cursor.is_dir():
        raise ParticipationError(f"participation parent {cursor} is not a directory")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise ParticipationError(
                    f"participation parent {directory} is not a directory"
                ) from None
            continue
        os.chmod(directory, 0o700)


def _fsync_parent_dir(parent: Path, state_path: Path) -> None:
    fd = None
    try:
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise ParticipationDurabilityUncertain(state_path) from None
    try:
        try:
            os.fsync(fd)
        except OSError:
            raise ParticipationDurabilityUncertain(state_path) from None
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _load_ledger(path: Path, expected_sid: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ParticipationError(f"participation ledger {path} must not be a symlink")
    if not path.exists():
        raise ParticipationError(f"participation ledger {path} is missing; init explicitly")
    if not path.is_file():
        raise ParticipationError(f"participation ledger {path} is not a regular file")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ParticipationError(
            f"corrupt participation ledger {path}: invalid JSON at line "
            f"{exc.lineno} col {exc.colno}"
        ) from None
    return _validate_ledger(raw, path, expected_sid)


def _save_ledger(path: Path, state: dict[str, Any]) -> None:
    if path.is_symlink():
        raise ParticipationError(f"participation ledger {path} must not be a symlink")
    if not path.exists():
        raise ParticipationError(f"participation ledger {path} is missing; init explicitly")
    if not path.is_file():
        raise ParticipationError(f"participation ledger {path} is not a regular file")
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise ParticipationError(f"participation parent {parent} is not a directory")
    fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=path.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    _fsync_parent_dir(parent, path)


def _create_ledger(path: Path, state: dict[str, Any]) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp_path, path)
        except FileExistsError as exc:
            raise ParticipationError(
                f"participation ledger {path} already exists; refusing to overwrite"
            ) from exc
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        pass
    _fsync_parent_dir(path.parent, path)
