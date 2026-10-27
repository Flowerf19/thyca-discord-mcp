"""Responder package: FIFO worker + ACK controller (no sends, no scheduler).

Canonical re-exports of the previous :mod:`responder` surface.
Internal layout: :mod:`responder.core` (orchestrator),
:mod:`responder.submit` (pre-POST submit mixin),
:mod:`responder.routing` (routing-validation mixin),
:mod:`responder.input` (pure input validation).
"""

from .core import (  # noqa: F401 - canonical re-exports
    _ALL_TERMINAL,
    _MID_FLIGHT_STAGES,
    _ROUTED_POST_PARTICIPATIONS,
    _SAFE_TERMINAL,
    _VALID_PARTICIPATIONS,
    CONTENT_MAX,
    PROMPT_MAX,
    READ_LIMIT,
    Responder,
    ResponderError,
    ResponderSubmitMixin,
    _is_strict_int,
    _is_strict_positive_int,
    _sha256_hex,
    _validate_id_string,
    validate_entry,
)
from .input import (  # noqa: F401 - canonical re-exports
    ID_MAX,
)
from .input import (
    ResponderError as _ResponderErrorInput,
)
from .input import (
    _is_strict_int as _is_strict_int_input,
)
from .input import (
    _is_strict_positive_int as _is_strict_positive_int_input,
)
from .input import (
    _validate_id_string as _validate_id_string_input,
)
from .input import (
    validate_entry as validate_entry_input,
)

assert ResponderError is _ResponderErrorInput
assert _is_strict_int is _is_strict_int_input
assert _is_strict_positive_int is _is_strict_positive_int_input
assert _validate_id_string is _validate_id_string_input
assert validate_entry is validate_entry_input
