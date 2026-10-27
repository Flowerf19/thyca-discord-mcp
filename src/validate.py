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

from typing import Any

__all__ = [
    "is_strict_int",
    "is_strict_positive_int",
]


def is_strict_int(value: Any) -> bool:
    """True for genuine ints (bools excluded)."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_strict_positive_int(value: Any) -> bool:
    """True for genuine ints > 0 (bools excluded)."""
    return is_strict_int(value) and value > 0
