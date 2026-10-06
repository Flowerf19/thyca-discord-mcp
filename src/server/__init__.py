"""MCP server package (composition root).

``core`` holds the verbatim body of the former ``server`` module
(``create_server`` plus its tool closures). This package preserves the
previous ``server`` public surface via plain re-exports; tests patch
``server.core.<name>`` (the canonical definition site), never this
namespace.
"""

from .core import (
    Responder,  # noqa: F401 - canonical re-exports
    append_audit,  # noqa: F401 - canonical re-exports
    build_bot,  # noqa: F401 - canonical re-exports
    create_server,  # noqa: F401 - canonical re-exports
    get_job,  # noqa: F401 - canonical re-exports
    mark_uncertain,  # noqa: F401 - canonical re-exports
)
