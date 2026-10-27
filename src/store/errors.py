from __future__ import annotations


class StoreError(ValueError):
    """Corrupt DB, schema mismatch, or an invalid store operation."""
