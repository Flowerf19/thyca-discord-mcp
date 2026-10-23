"""Responder package: FIFO worker + ACK controller (no sends, no scheduler).

[DEPRECATED worker path] Retained for the legacy correlated tools only.
The MCP lifespan no longer starts the auto-reply / recovery loops, and the
neutral tools (``tools/transport.py`` -> ``services/``) never import this
package. ``ack_outbox`` is now a pure sequence cursor and does not consult
:class:`Responder`.

Canonical re-exports of the previous :mod:`responder` surface.
Internal layout: :mod:`responder.core` (orchestrator),
:mod:`responder.submit` (pre-POST submit mixin),
:mod:`responder.input` (pure input validation),
:mod:`responder.recovery` (native POST recovery + trace proof),
:mod:`responder.rotation` (mapping registry + daily session rotation).
"""

import delivery  # noqa: F401 - module re-export so the established

from .core import (  # noqa: F401 - canonical re-exports
    _ALL_TERMINAL,
    _MID_FLIGHT_STAGES,
    _ROUTED_POST_PARTICIPATIONS,
    _SAFE_TERMINAL,
    _SID_RE,
    _VALID_PARTICIPATIONS,
    CONTENT_MAX,
    PROMPT_MAX,
    READ_LIMIT,
    Responder,
    ResponderError,
    ResponderRecovery,
    ResponderSubmitMixin,
    _is_strict_int,
    _is_strict_ledger_id,
    _is_strict_positive_int,
    _load_mapping,
    _sha256_hex,
    _validate_id_string,
    build_auto_prompt,
    historical_pairs,
    validate_entry,
)
# ``responder.delivery.<fn>`` patch path keeps resolving to the
# canonical delivery module object.
