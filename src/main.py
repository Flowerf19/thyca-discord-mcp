"""Entry point: single-instance lock, then the MCP server over stdio."""

import sys

from config import Settings
from instance_lock import (
    InstanceLockBusy,
    InstanceLockError,
    acquire_service_lock,
    derive_lock_path,
)
from server import create_server


def _acquire_startup_lock(settings: Settings):
    """Take the single-writer lock beside the SQLite store (held for life)."""
    return acquire_service_lock(derive_lock_path(settings.db_path))


def main() -> None:
    settings = Settings()
    try:
        lock = _acquire_startup_lock(settings)
    except InstanceLockBusy as exc:
        print(f"thyca-discord-mcp: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    except InstanceLockError as exc:
        print(f"thyca-discord-mcp: {exc}", file=sys.stderr)
        raise SystemExit(3) from None
    try:
        server = create_server(settings)
        server.run(transport="stdio")
    finally:
        lock.release()


if __name__ == "__main__":
    main()
