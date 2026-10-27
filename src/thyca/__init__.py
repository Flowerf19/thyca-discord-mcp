"""Thyca native-agent adapter: errors, validation, transport, prompt, client.

Single merged module (was six ``thyca_*`` modules, merged verbatim in order:
errors, validate, trace, transport, prompt, client; the trace-evidence
machinery was deleted in the simplify refactor -- only the session snapshot
poll remains); now split into this package with the same order
(:mod:`thyca.errors`, :mod:`thyca.validation`, :mod:`thyca.transport`,
:mod:`thyca.prompt`, :mod:`thyca.client`). Import from here, e.g.
``from thyca import ThycaClient, build_conversation_prompt``.
"""

from validate import ID_MAX  # noqa: F401 - re-exported (in __all__)

from .client import ThycaClient  # noqa: F401 - canonical re-export
from .errors import (  # noqa: F401 - canonical re-exports
    ThycaBusyError,
    ThycaClientError,
    ThycaPreconditionError,
    ThycaProtocolError,
    ThycaSubmissionUncertainError,
    _NetworkMarker,
    _TimeoutMarker,
)
from .prompt import (  # noqa: F401 - canonical re-exports
    CONTENT_MAX,
    PROMPT_MAX,
    ThycaPromptError,
    build_conversation_prompt,
)
from .transport import (  # noqa: F401 - canonical re-exports
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    MAX_RESPONSE_BYTES,
    _declared_body_len,
    _read_bounded,
    sync_http_request,
)
from .validation import (  # noqa: F401 - canonical re-exports
    ARG_CHANNEL,
    ARG_CONTENT,
    ARG_JOB_REF,
    ARG_REASON,
    ARG_SOURCE_MID,
    ARG_STATUS,
    EFFORT_MAX,
    JOB_REF_MAX,
    MODEL_MAX,
    NO_REPLY_REASONS,
    NO_REPLY_STATUS_DECLARED,
    NO_REPLY_TOOL_NAME,
    SEND_TOOL_NAME,
    SESSION_ID_PATTERN,
    TEXT_MAX,
    TITLE_EXPECTED_MAX,
    validate_day,
    validate_native_sid,
)

__all__ = [
    "ARG_CHANNEL",
    "ARG_CONTENT",
    "ARG_JOB_REF",
    "ARG_REASON",
    "ARG_SOURCE_MID",
    "ARG_STATUS",
    "CONTENT_MAX",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "EFFORT_MAX",
    "ID_MAX",
    "JOB_REF_MAX",
    "MAX_RESPONSE_BYTES",
    "MODEL_MAX",
    "NO_REPLY_REASONS",
    "NO_REPLY_STATUS_DECLARED",
    "NO_REPLY_TOOL_NAME",
    "PROMPT_MAX",
    "SEND_TOOL_NAME",
    "SESSION_ID_PATTERN",
    "TEXT_MAX",
    "TITLE_EXPECTED_MAX",
    "ThycaBusyError",
    "ThycaClient",
    "ThycaClientError",
    "ThycaPreconditionError",
    "ThycaPromptError",
    "ThycaProtocolError",
    "ThycaSubmissionUncertainError",
    "build_conversation_prompt",
    "sync_http_request",
    "validate_day",
    "validate_native_sid",
]
