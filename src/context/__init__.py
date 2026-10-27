"""Scoped conversation-context collector package.

Canonical re-exports of the previous :mod:`context` surface:
:mod:`context.snapshot` holds the pure assembly (no ``discord``/``bot``
imports) and :mod:`context.collector` holds the Discord I/O plus the
``build_context_collector`` orchestration. Import from here, e.g.
``from context import build_context_collector``.
"""

from source_messages import SourceDeleted  # noqa: F401 - canonical re-export

from .collector import build_context_collector  # noqa: F401 - canonical re-export
from .snapshot import (  # noqa: F401 - canonical re-exports
    FIT_HASH,
    FIT_NONCE,
    MAX_MESSAGES_LIMIT,
    ContextUnavailable,
    context_fingerprint,
)

__all__ = [
    "FIT_HASH",
    "FIT_NONCE",
    "MAX_MESSAGES_LIMIT",
    "ContextUnavailable",
    "SourceDeleted",
    "build_context_collector",
    "context_fingerprint",
]

# ``SourceDeleted`` is intentionally NOT redeclared here: the name above is the
# authoritative ``source_messages.SourceDeleted`` object,
# re-exported so the router and the existing checker catch the same class.
