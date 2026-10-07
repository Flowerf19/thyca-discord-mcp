"""Daily native-session rotation: mapping registry + drain-gated orchestration.

Canonical owner of the ``discord_session.json`` mapping file (moved
verbatim from the orchestrator): the strict 5-key load plus the optional
rotation keys (``active_date`` + ``sessions`` day -> SID history), the
atomic 0600-preserving registry save, and the async
:func:`rotate_to_today` orchestration (recorded-session reuse or fresh
native create -> GET-verify -> delivery converge -> ledger converge ->
mapping activate).

Write order is delivery -> ledger -> mapping (the registry is the
commit record). Every store refuses partial states loudly; there is no
silent multi-file resume. A crash between the sync store writes fails
closed at the next startup mismatch with both SIDs named for operator
recovery.

No work happens on import. No clock reads here: the caller supplies
``today`` (the orchestrator owns the day provider).
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import delivery
import participation
import sessions
from thyca import (
    ThycaBusyError,
    ThycaClientError,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
)

from .input import ResponderError

_MAPPING_STRICT_KEYS = ("native_id", "title", "audit_label", "created_at", "path")


class RotationDeferred(ResponderError):
    """Transient native failure during rotation: defer, do not halt.

    A single blip (timeout/disconnect/busy/5xx) during the one rotation
    tick of the day must not stop the auto-reply loop: the previous
    day's session is fully valid, so the gate catches this, keeps
    serving it, and retries next tick. Definitive failures (malformed
    native shapes, 400s, verify mismatch, store refusals) still raise
    plain :class:`ResponderError` and halt. ``kind`` is the coarse
    native reason (``timeout``/``disconnect``/``http_409``/``http_5xx``).
    """

    def __init__(self, kind: str) -> None:
        self.kind = kind if isinstance(kind, str) and kind else "transient"
        super().__init__(f"daily rotation deferred: {self.kind}")


def _is_transient_native_error(exc: ThycaClientError) -> bool:
    """True for maybe-later native failures (timeout/disconnect/busy/5xx)."""
    if isinstance(exc, (ThycaSubmissionUncertainError, ThycaBusyError)):
        return True
    return isinstance(exc, ThycaProtocolError) and exc.reason in (
        "timeout",
        "disconnect",
        "http_5xx",
    )


def _defer_or_halt(exc: ThycaClientError, *, op: str) -> RotationDeferred | ResponderError:
    """Map a native rotation-call failure to defer (transient) or halt."""
    if _is_transient_native_error(exc):
        reason = getattr(exc, "reason", "")
        return RotationDeferred(reason if isinstance(reason, str) and reason else "transient")
    return ResponderError(f"daily rotation {op} failed")


def _load_mapping(path: Path, expected_session_id: str) -> dict[str, str]:
    """Load the existing native SID mapping (0600, 5 strict keys, SID match).

    Never opens the native session file/profile; only checks the ``path``
    basename stem equals ``native_id``. Unknown keys are ignored. All errors
    are coarse with field names only (no value dumps).
    """
    file = Path(path)
    if file.is_symlink():
        raise ResponderError("mapping must not be a symlink")
    if not file.exists():
        raise ResponderError("mapping is missing; initialize explicitly")
    if not file.is_file():
        raise ResponderError("mapping is not a regular file")
    try:
        mode = stat.S_IMODE(file.stat().st_mode)
    except OSError:
        raise ResponderError("mapping stat failed") from None
    if mode != 0o600:
        raise ResponderError("mapping permissions must be 0600")
    try:
        raw = json.loads(file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ResponderError("mapping has invalid JSON") from None
    except OSError:
        raise ResponderError("mapping read failed") from None
    return _validate_mapping_dict(raw, expected_session_id)


def _validate_mapping_dict(raw: Any, expected_session_id: str) -> dict[str, str]:
    """Dict half of :func:`_load_mapping` (shared by load and rotation save).

    Same checks, same messages: 5 strict keys, strict native SID grammar,
    title/audit binding to the expected session, path-stem match. Unknown
    keys are ignored here; the caller owns rotation-key validation.
    """
    if not isinstance(raw, dict):
        raise ResponderError("mapping top level must be an object")
    for key in _MAPPING_STRICT_KEYS:
        if key not in raw:
            raise ResponderError(f"mapping missing key {key!r}")
        value = raw[key]
        if not isinstance(value, str) or not value or len(value) > 1000:
            raise ResponderError(f"mapping has invalid {key!r}")
    native_id = raw["native_id"]
    if sessions.validate_native_sid(native_id) is None:
        raise ResponderError("mapping has invalid native_id")
    if raw["title"] != expected_session_id or raw["audit_label"] != expected_session_id:
        raise ResponderError("mapping title mismatch")
    try:
        stem = Path(raw["path"]).stem
    except Exception:
        raise ResponderError("mapping has invalid path") from None
    if stem != native_id:
        raise ResponderError("mapping path mismatch")
    return {
        "native_id": native_id,
        "title": raw["title"],
        "audit_label": raw["audit_label"],
        "created_at": raw["created_at"],
        "path": raw["path"],
    }


def load_session_registry(mapping_path: Path, expected_session_id: str) -> dict[str, Any]:
    """Load the mapping registry: 5 strict keys + rotation state.

    Returns a detached dict with the strict base plus ``active_date``
    (``str | None``) and ``sessions`` (detached day -> SID history, ``{}``
    when absent). Legacy dateless files load with ``active_date`` None
    and empty history (the adopt path labels them on first tick).

    Rotation-state invariant (fail closed): ``active_date`` is set if
    and only if history is non-empty, and the active day always binds
    the current ``native_id``.
    """
    base = _load_mapping(mapping_path, expected_session_id)
    file = Path(mapping_path)
    try:
        raw = json.loads(file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        raise ResponderError("mapping read failed") from None
    if not isinstance(raw, dict):
        raise ResponderError("mapping top level must be an object")
    for key in _MAPPING_STRICT_KEYS:
        if raw.get(key) != base[key]:
            raise ResponderError("mapping changed during load")
    active_date = raw.get("active_date")
    if active_date is not None and sessions.validate_day(active_date) is None:
        raise ResponderError("mapping has invalid active_date")
    if "sessions" not in raw:
        history: dict[str, str] = {}
    else:
        validated = sessions.validate_sessions_history(raw.get("sessions"))
        if validated is None:
            raise ResponderError("mapping has invalid sessions")
        history = validated
    if (active_date is None) != (history == {}):
        raise ResponderError("mapping has inconsistent rotation state")
    if active_date is not None and history.get(active_date) != base["native_id"]:
        raise ResponderError("mapping has inconsistent rotation state")
    return {**base, "active_date": active_date, "sessions": history}


def build_rotation_history(
    old_history: dict[str, str],
    *,
    old_day: str,
    old_sid: str,
    new_day: str,
    new_sid: str,
) -> dict[str, str]:
    """Return the post-rotation day -> SID map (detached, re-validated).

    Seeds the outgoing day binding and binds the new day; never drops
    an entry. The new day must advance (never equal the old day) and
    must not already bind a DIFFERENT session (a stale recorded entry
    is removed by the caller only after a verified native 404).
    """
    if sessions.validate_day(old_day) is None:
        raise ResponderError("rotation has invalid previous day")
    if sessions.validate_native_sid(old_sid) is None:
        raise ResponderError("rotation has invalid previous session")
    if sessions.validate_day(new_day) is None:
        raise ResponderError("rotation has invalid day")
    if sessions.validate_native_sid(new_sid) is None:
        raise ResponderError("rotation has invalid session")
    if old_day == new_day:
        raise ResponderError("rotation day must advance")
    base = sessions.validate_sessions_history(old_history)
    if base is None:
        raise ResponderError("rotation has invalid session history")
    recorded = base.get(new_day)
    if recorded is not None and recorded != new_sid:
        raise ResponderError("rotation target conflicts with recorded session")
    merged = {**base, old_day: old_sid, new_day: new_sid}
    clean = sessions.validate_sessions_history(merged)
    if clean is None:  # pragma: no cover - defensive; inputs validated above
        raise ResponderError("rotation has invalid session history")
    return clean


def derive_session_path(mapping_path: Path, native_sid: str) -> str:
    """Derive the native session-file path beside the mapping registry."""
    return str(Path(mapping_path).parent / "sessions" / f"{native_sid}.jsonl")


def save_session_registry(
    mapping_path: Path,
    old_registry: dict[str, Any],
    new_registry: dict[str, Any],
    *,
    expected_session_id: str,
) -> None:
    """Atomically replace the mapping registry (0600 preserved, no create).

    Reloads the file fresh and requires it to still hold ``old_registry``
    (5 strict keys + rotation keys; any concurrent change fails closed).
    Merges ``new_registry`` over the fresh raw (unknown operator keys
    preserved), validates the merged file (strict base + rotation
    invariant: active day binds the new SID), then commits via a private
    0600 temp inode + fsync + ``os.replace`` + parent-dir fsync.
    """
    file = Path(mapping_path)
    if file.is_symlink():
        raise ResponderError("mapping must not be a symlink")
    if not file.exists():
        raise ResponderError("mapping is missing; initialize explicitly")
    if not file.is_file():
        raise ResponderError("mapping is not a regular file")
    try:
        mode = stat.S_IMODE(file.stat().st_mode)
    except OSError:
        raise ResponderError("mapping stat failed") from None
    if mode != 0o600:
        raise ResponderError("mapping permissions must be 0600")
    parent = file.parent
    if parent.is_symlink() or not parent.is_dir():
        raise ResponderError("mapping parent is not a directory")
    try:
        raw = json.loads(file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ResponderError("mapping has invalid JSON") from None
    except OSError:
        raise ResponderError("mapping read failed") from None
    if not isinstance(raw, dict):
        raise ResponderError("mapping top level must be an object")
    for key in _MAPPING_STRICT_KEYS:
        if raw.get(key) != old_registry.get(key):
            raise ResponderError("mapping changed during rotation")
    if raw.get("active_date") != old_registry.get("active_date"):
        raise ResponderError("mapping changed during rotation")
    if raw.get("sessions", {}) != old_registry.get("sessions", {}):
        raise ResponderError("mapping changed during rotation")
    merged = dict(raw)
    for key in _MAPPING_STRICT_KEYS:
        value = new_registry.get(key)
        if not isinstance(value, str):
            raise ResponderError(f"mapping has invalid {key!r}")
        merged[key] = value
    merged["active_date"] = new_registry.get("active_date")
    merged["sessions"] = new_registry.get("sessions")
    base = _validate_mapping_dict(merged, expected_session_id)
    if sessions.validate_day(merged["active_date"]) is None:
        raise ResponderError("mapping has invalid active_date")
    history = sessions.validate_sessions_history(merged["sessions"])
    if history is None:
        raise ResponderError("mapping has invalid sessions")
    if history.get(merged["active_date"]) != base["native_id"]:
        raise ResponderError("mapping has inconsistent rotation state")
    fd, tmp_name = tempfile.mkstemp(dir=str(parent), prefix=file.name + ".tmp.")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(merged, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, file)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    try:
        dir_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        raise ResponderError("mapping durability uncertain") from None
    try:
        try:
            os.fsync(dir_fd)
        except OSError:
            raise ResponderError("mapping durability uncertain") from None
    finally:
        try:
            os.close(dir_fd)
        except OSError:
            pass


def adopt_session_day(
    *,
    mapping_path: Path,
    expected_session_id: str,
    today: str,
    registry: dict[str, Any],
) -> dict[str, Any]:
    """Label a legacy (dateless) registry with today's day (no native calls).

    ``registry`` is the fresh-loaded registry. Seals ``active_date`` +
    ``sessions[today]`` to the current SID via
    :func:`save_session_registry` (concurrent changes fail closed). An
    already-sealed registry syncs with no rewrite (``adopted`` False).
    Returns ``{"native_sid", "day", "adopted"}``.
    """
    if sessions.validate_day(today) is None:
        raise ResponderError("session day provider returned invalid day")
    if registry.get("active_date") is not None:
        if registry.get("active_date") == today and (registry.get("sessions") or {}).get(
            today
        ) == registry.get("native_id"):
            return {"native_sid": registry["native_id"], "day": today, "adopted": False}
        raise ResponderError("mapping changed during rotation")
    new_registry = {key: registry[key] for key in _MAPPING_STRICT_KEYS}
    new_registry["active_date"] = today
    new_registry["sessions"] = {today: registry["native_id"]}
    save_session_registry(
        mapping_path, registry, new_registry, expected_session_id=expected_session_id
    )
    return {"native_sid": registry["native_id"], "day": today, "adopted": True}


def _is_native_not_found(exc: ThycaClientError) -> bool:
    """True only for a definitive native HTTP 404 (session verifiably gone)."""
    return isinstance(exc, ThycaPreconditionError) and exc.status_code == 404


async def rotate_to_today(
    *,
    client: Any,
    mapping_path: Path,
    delivery_path: Path,
    ledger_path: Path | None,
    expected_session_id: str,
    today: str,
    registry: dict[str, Any],
) -> dict[str, Any]:
    """Rotate the stale registry to ``today`` (reuse-or-create, then converge).

    ``registry`` is the fresh-loaded STALE registry (``active_date`` set
    and != ``today``). Steps: reuse today's recorded session when present
    (GET-verified; a verified 404 drops the stale entry and creates
    fresh), else create a fresh blank native session; build history;
    converge delivery then ledger (``ledger_path`` None skips the ledger
    for injected/unowned ledgers); activate the mapping registry last.
    Transient native failures (timeout/disconnect/busy/5xx) raise
    :class:`RotationDeferred` (the caller keeps serving the old session
    and retries later); definitive failures (malformed shapes, 400s,
    verify mismatch, store refusals) raise :class:`ResponderError` with
    zero partial activation (a delivery/ledger half-commit fails closed
    loudly, never silently resumed). Returns ``{"native_sid", "day",
    "reused", "history", "registry"}`` (metadata only, no chat).
    """
    if sessions.validate_day(today) is None:
        raise ResponderError("session day provider returned invalid day")
    old_day = registry.get("active_date")
    old_sid = registry.get("native_id")
    if sessions.validate_day(old_day) is None:
        raise ResponderError("rotation requires an adopted session day")
    if sessions.validate_native_sid(old_sid) is None:
        raise ResponderError("rotation requires a valid current session")
    if old_day == today:
        raise ResponderError("rotation requires a stale session day")
    old_history = dict(registry.get("sessions") or {})
    target = old_history.get(today)
    reused = target is not None
    if target is not None:
        try:
            snap = await client.get_session_snapshot(target)
        except ThycaClientError as exc:
            if not _is_native_not_found(exc):
                raise _defer_or_halt(exc, op="verify") from None
            # Verified gone (only a never-served blank session can vanish
            # natively): drop the stale entry and create fresh below.
            old_history.pop(today, None)
            target = None
            reused = False
        else:
            if not isinstance(snap, dict) or snap.get("native_id_match") is not True:
                raise ResponderError("daily rotation native session mismatch")
    if target is None:
        try:
            created = await client.create_session()
        except ThycaClientError as exc:
            raise _defer_or_halt(exc, op="create") from None
        if not isinstance(created, dict):
            raise ResponderError("daily rotation create failed")
        fresh_sid = created.get("native_sid")
        if sessions.validate_native_sid(fresh_sid) is None:
            raise ResponderError("daily rotation create failed")
        if created.get("message_count") != 0:
            raise ResponderError("daily rotation create failed")
        target = fresh_sid
    history = build_rotation_history(
        old_history, old_day=old_day, old_sid=old_sid, new_day=today, new_sid=target
    )
    try:
        delivery.rotate_native_session(
            delivery_path,
            expected_current=old_sid,
            new_sid=target,
            day=today,
            history=history,
        )
    except delivery.DeliveryDurabilityUncertain:
        raise ResponderError("delivery durability uncertain") from None
    except delivery.DeliveryError as exc:
        raise ResponderError(f"daily rotation delivery refused: {exc}") from None
    if ledger_path is not None:
        try:
            participation.rotate_native_session(
                ledger_path,
                expected_current=old_sid,
                new_sid=target,
                day=today,
                history=history,
            )
        except participation.ParticipationDurabilityUncertain:
            raise ResponderError("participation durability uncertain") from None
        except participation.ParticipationError as exc:
            raise ResponderError(f"daily rotation ledger refused: {exc}") from None
    new_registry = {
        "native_id": target,
        "title": registry["title"],
        "audit_label": registry["audit_label"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "path": derive_session_path(mapping_path, target),
        "active_date": today,
        "sessions": history,
    }
    save_session_registry(
        mapping_path, registry, new_registry, expected_session_id=expected_session_id
    )
    return {
        "native_sid": target,
        "day": today,
        "reused": reused,
        "history": history,
        "registry": new_registry,
    }


__all__ = [
    "RotationDeferred",
    "_load_mapping",
    "adopt_session_day",
    "build_rotation_history",
    "derive_session_path",
    "load_session_registry",
    "rotate_to_today",
    "save_session_registry",
]
