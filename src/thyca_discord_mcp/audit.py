"""Append-only JSONL audit log. One record per tool call / notable event."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REDACT_KEYS = ("token", "secret", "password", "authorization")


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: ("***" if any(s in k.lower() for s in _REDACT_KEYS) else _redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


def append_audit(
    path: str | Path,
    *,
    session_id: str,
    actor: str,
    tool: str,
    args: dict[str, Any] | None = None,
    ok: bool = True,
    error: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Append one record and return it. Creates parent dirs as needed."""
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "actor": actor,
        "tool": tool,
        "args": _redact(args or {}),
        "ok": ok,
        "error": error,
        "note": note,
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record
