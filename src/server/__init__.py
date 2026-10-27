"""MCP server package (composition root).

``core`` holds the body of the former ``server`` module
(``create_server`` plus its tool closures). This package preserves the
previous ``server`` public surface via plain re-exports; tests patch
``server.core.<name>`` (the canonical definition site), never this
namespace.
"""

from .core import (  # noqa: F401 - canonical re-exports
    Responder,
    append_audit,
    build_bot,
    create_server,
)
