"""Typed settings for thyca-discord-mcp.

Precedence (highest first): constructor args, environment variables,
then the project-local ``.env`` file. Env names match field names
case-insensitively, e.g. ``DISCORD_TOKEN``.
"""

import math
import os
import re
from pathlib import Path

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _lexical_path_key(path: Path) -> str:
    """Pure lexical identity for collision checks (no filesystem access).

    ``os.path.abspath`` + ``os.path.normpath`` only: collapses ``.``/``..``
    and maps relative paths against the cwd without touching the filesystem
    (no ``Path.resolve()``/``samefile()``, no existence reads). Compares
    canonical lexical identities; chosen values are never rewritten.
    """
    return os.path.normpath(os.path.abspath(os.fspath(path)))


class Settings(BaseSettings):
    """Runtime configuration. IDs are ints; the token stays a SecretStr."""

    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", extra="ignore")

    discord_token: SecretStr = Field(description="Discord bot token. Never log or echo.")
    owner_uid: int = Field(description="Discord user ID of the owner.")
    guild_id: int = Field(description="Guild (server) ID the bot is scoped to.")
    default_channel_id: int = Field(description="Channel used when a tool omits channel_id.")
    target_bot_id: str = Field(
        default="march7th",
        description="Target bot whose fixed DM session receives auto-reply turns. "
        "Required-effective: empty or invalid values fail Settings construction.",
    )
    audit_path: Path = Field(default=PROJECT_ROOT / "audit.jsonl")
    outbox_path: Path = Field(
        default=PROJECT_ROOT / "outbox.jsonl",
        description="Append-only JSONL outbox of captured Discord messages.",
    )
    outbox_state_path: Path = Field(
        default=PROJECT_ROOT / "outbox_state.json",
        description="Outbox cursors (seq/acked/poll). Saved atomically.",
    )
    watch_channel_ids: list[int] = Field(
        default_factory=list,
        description="Channels captured into the outbox. Empty means [default_channel_id].",
    )
    command_prefix: str = "!"
    intent_message_content: bool = True
    intent_members: bool = True
    chunk_guilds_at_startup: bool = False
    thyca_base_url: str = Field(
        default="http://127.0.0.1:8765",
        description="Thyca agent HTTP base URL (loopback). Validated by the native client.",
    )
    delivery_state_path: Path = Field(
        default=PROJECT_ROOT / "delivery_state.json",
        description="Responder delivery cursors. Saved atomically by the delivery module.",
    )
    responder_check_seconds: float = Field(
        default=1.0,
        gt=0,
        description="Operational poll tick for the responder loop. Must be finite and > 0.",
    )
    thyca_request_timeout_seconds: float = Field(
        default=900.0,
        gt=0,
        description="Technical upper bound for one Thyca agent request. Must be finite and > 0.",
    )
    recovery_poll_seconds: float = Field(
        default=30.0,
        ge=30,
        description="Native recovery poll interval floor. Must be finite and >= 30.",
    )
    typesafe_api_key: SecretStr | None = Field(
        default=None,
        description="TypeSafe Jev API key for paid classification. Lazy: only read when "
        "the responder builds its real Jev adapter. Never log or echo.",
    )
    jev_state_path: Path | None = Field(
        default=None,
        description="Jev participation ledger state. Defaults beside the configured "
        "delivery state (same parent, jev_state.json) so tmp delivery paths "
        "never leak ledger writes to the repo root. Must not collide with "
        "delivery/outbox/audit paths.",
    )
    conversation_max_messages: int = Field(
        default=20,
        ge=1,
        le=20,
        description="Canonical context window: whole recent messages, 1..20 "
        "(source + exact parent + recents, same approved channel). Env "
        "strings with clean ints coerce; bools/floats are rejected.",
    )

    @field_validator(
        "responder_check_seconds",
        "thyca_request_timeout_seconds",
        "recovery_poll_seconds",
        mode="before",
    )
    @classmethod
    def _reject_bool_numeric(cls, value: object) -> object:
        """Reject bools: pydantic lax mode would coerce True -> 1.0, hiding config errors."""
        if isinstance(value, bool):
            raise ValueError("must be a number, not a bool")
        return value

    @field_validator(
        "responder_check_seconds",
        "thyca_request_timeout_seconds",
        "recovery_poll_seconds",
        mode="after",
    )
    @classmethod
    def _require_finite_numeric(cls, value: float) -> float:
        """Reject NaN/Inf: bounds checks alone would let Inf through (Inf > 0)."""
        if not math.isfinite(value):
            raise ValueError("must be finite")
        return value

    @field_validator("conversation_max_messages", mode="before")
    @classmethod
    def _reject_bool_float_max(cls, value: object) -> object:
        """Reject bools/floats before lax int coercion (env strings coerce)."""
        if isinstance(value, bool) or isinstance(value, float):
            raise ValueError("must be an int 1..20, not a bool or float")
        return value

    @field_validator("target_bot_id", mode="before")
    @classmethod
    def _validate_target_bot_id(cls, value: object) -> object:
        """Require an explicit target bot: empty/None is now an error.

        Non-string values are invalid; surrounding whitespace is stripped;
        an empty result fails (there is no legacy fallback). Anything else
        must match the serve bot-id grammar (same shape the native routes
        enforce)."""
        if not isinstance(value, str):
            raise ValueError("must be a bot id string")
        stripped = value.strip()
        if not stripped:
            raise ValueError("must be a non-empty bot id")
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", stripped):
            raise ValueError("must match the bot id grammar")
        return stripped

    @model_validator(mode="after")
    def _default_watch_channels(self) -> "Settings":
        """Empty/missing watch list falls back to the default channel."""
        if not self.watch_channel_ids:
            self.watch_channel_ids = [self.default_channel_id]
        return self

    @model_validator(mode="after")
    def _default_jev_state_path(self) -> "Settings":
        """Derive the ledger path beside delivery state; reject owned collisions.

        Runs after ``_default_watch_channels``. An explicit ``jev_state_path``
        is kept; otherwise it defaults to the delivery parent joined with
        ``jev_state.json`` so tmp delivery paths never leak ledger writes to
        the repo root. The pure-lexically-normalized path (``os.path.abspath``
        + ``os.path.normpath``, no filesystem access) must not equal delivery,
        "delivery/outbox/outbox-state/audit paths by the same normalization.
        Chosen values are preserved exactly; only canonical lexical identities
        are compared. Symlink/hardlink aliases remain the ledger store's
        fail-closed responsibility; this guard claims no physical alias detection.
        """
        if self.jev_state_path is None:
            self.jev_state_path = self.delivery_state_path.parent / "jev_state.json"
        assert self.jev_state_path is not None
        jev_key = _lexical_path_key(self.jev_state_path)
        for name in (
            "delivery_state_path",
            "outbox_path",
            "outbox_state_path",
            "audit_path",
        ):
            if jev_key == _lexical_path_key(getattr(self, name)):
                raise ValueError(f"jev_state_path collides with {name}")
        return self
