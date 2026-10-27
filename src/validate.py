"""Strict integer checks for untrusted boundary inputs.

Single home of the two strict-int concepts used at untrusted inputs only
(MCP tool args, Thyca HTTP JSON, Jev SDK JSON, DB-read values): genuine
``int`` with ``bool`` excluded, and the positive variant. Internal call
sites receiving already-typed values (ints from SQLite INTEGER columns,
``discord.py`` objects, pydantic-validated settings) trust their
annotations instead of re-checking here.

Importing this module performs no I/O: stdlib only, no env reads.
"""

from __future__ import annotations

import hashlib
from typing import Any

__all__ = [
    "ID_MAX",
    "is_sha256_hex",
    "is_strict_int",
    "is_strict_positive_int",
    "sha256_hex",
    "validate_id_string",
]

# Single bound for nonce-style IDs (Thyca job_ref, Jev attempt_id, ...).
ID_MAX = 128


def is_strict_int(value: Any) -> bool:
    """True for genuine ints (bools excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_strict_positive_int(value: Any) -> bool:
    """True for genuine ints > 0 (bools excluded)."""
    return is_strict_int(value) and value > 0


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def is_sha256_hex(value: Any) -> bool:
    """Exact 64-char hex (context fingerprint); stdlib-free manual check."""
    if not isinstance(value, str) or len(value) != 64:
        return False
    for ch in value:
        if ch not in "0123456789abcdefABCDEF":
            return False
    return True


def validate_id_string(value: Any) -> str | None:
    """Nonce guard mirroring the existing native bound: 1..128, no whitespace."""
    if not isinstance(value, str):
        return None
    if not (1 <= len(value) <= ID_MAX):
        return None
    if value.strip() != value or not value:
        return None
    for ch in value:
        if ch.isspace():
            return None
    return value
