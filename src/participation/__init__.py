"""Durable metadata-only Jev participation ledger (package surface).

``participation`` remains the single public import surface; the
implementation lives in :mod:`.ledger`, :mod:`.store`, :mod:`.validate`,
and :mod:`.errors` (same objects, canonical re-exports below).
"""

from .errors import (  # noqa: F401 - canonical re-exports
    ParticipationDurabilityUncertain,
    ParticipationError,
    ParticipationPending,
    ParticipationReason,
)
from .ledger import (  # noqa: F401 - canonical re-exports
    JEV_MODEL,  # noqa: F401 - canonical re-export
    PARTICIPATION_OPTIONS,
    ConversationError,
    JevError,
    ParticipationLedger,
    build_questions,
    project_conversation_state,
    rotate_native_session,
    validate_answers,
    validate_model_id,
)
from .store import (  # noqa: F401 - canonical re-exports
    _create_ledger,
    _default_day_provider,
    _ensure_parent_dir,
    _fsync_parent_dir,
    _load_ledger,
    _now_iso,
    _save_ledger,
)
from .validate import (  # noqa: F401 - canonical re-exports
    _LEGACY_QUESTION_IDS,
    INPUT_USD_PER_TOKEN,
    MAX_ID_LEN,
    PARTICIPATION_STATUSES,
    PARTICIPATION_VERSION,
    SHA256_HEX_LEN,
    _canonical_decimal,
    _check_prob,
    _cost_for_input_tokens,
    _estimated_cost_str,
    _extract_unknown_halt,
    _extract_valid_usage,
    _fingerprint_snapshot,
    _has_whitespace,
    _is_sha256_hex,
    _is_strict_positive_int,
    _is_valid_day,
    _job_key,
    _known_cost_for_today,
    _parse_job_key,
    _validate_choice_token,
    _validate_id_string,
    _validate_ledger,
    _validate_record,
    _validate_stored_answers,
    _validate_stored_usage,
)

__all__ = [
    "INPUT_USD_PER_TOKEN",
    "JEV_MODEL",
    "PARTICIPATION_STATUSES",
    "PARTICIPATION_VERSION",
    "ParticipationDurabilityUncertain",
    "ParticipationError",
    "ParticipationLedger",
    "ParticipationPending",
    "ParticipationReason",
    "rotate_native_session",
]
