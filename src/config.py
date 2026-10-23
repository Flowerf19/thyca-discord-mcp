"""Typed settings for thyca-discord-mcp.

Precedence (highest first): constructor args, environment variables,
then the project-local ``.env`` file. Env names match field names
case-insensitively, e.g. ``DISCORD_TOKEN``.
"""

import math
import os
from pathlib import Path

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from sessions import parse_tz_name

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
    session_id: str = Field(
        default="discord-main",
        description="Audit/heartbeat label for this gateway instance. "
        "[DEPRECATED] Thyca session semantics moved to the dedicated bot; "
        "the neutral MCP path only uses this as a log label.",
    )
    daily_session_rotation: bool = Field(
        default=True,
        description="[DEPRECATED worker path] Rotate to a fresh native Thyca "
        "session once per logical day (one session per day). Rotation is "
        "drain-gated: the worker "
        "safe-terminal, then adopts the new day session. Old sessions are "
        "never deleted. Set false to pin the single mapped session forever.",
    )
    session_day_timezone: str = Field(
        default="Asia/Ho_Chi_Minh",
        description="[DEPRECATED worker path] IANA timezone for the rotation "
        "logical day. Must parse via zoneinfo; validated eagerly at Settings construction.",
    )
    session_day_rollover_hour: int = Field(
        default=4,
        ge=0,
        le=23,
        description="[DEPRECATED worker path] Logical-day rollover hour "
        "(0..23) in session_day_timezone: "
        "the day flips at this wall hour, not at midnight, so a late-night "
        "conversation stays in one session. Env strings with clean ints "
        "coerce; bools/floats are rejected.",
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
        description="[DEPRECATED worker path] Thyca agent HTTP base URL "
        "(loopback); legacy setting outside the neutral MCP path. Validated "
        "by the native client.",
    )
    thyca_session_mapping_path: Path = Field(
        default=Path.home() / ".thyca/discord_session.json",
        description="[DEPRECATED worker path] Channel -> Thyca SID mapping "
        "file; legacy setting outside the neutral MCP path. Stored only; "
        "never read by Settings. When absent the server runs as a pure "
        "Discord gateway.",
    )
    delivery_state_path: Path = Field(
        default=PROJECT_ROOT / "delivery_state.json",
        description="[DEPRECATED worker path] Responder delivery cursors. "
        "Saved atomically by the delivery module.",
    )
    responder_check_seconds: float = Field(
        default=1.0,
        gt=0,
        description="[DEPRECATED worker path] Tick interval for the (now "
        "unstarted) responder loop.",
    )
    thyca_request_timeout_seconds: float = Field(
        default=900.0,
        gt=0,
        description="[DEPRECATED worker path] Technical upper bound for one "
        "Thyca agent request. Must be finite and > 0.",
    )
    recovery_poll_seconds: float = Field(
        default=30.0,
        ge=30,
        description="[DEPRECATED worker path] Native recovery poll interval "
        "floor. Must be finite and >= 30.",
    )
    typesafe_api_key: SecretStr | None = Field(
        default=None,
        description="[DEPRECATED worker path] TypeSafe Jev API key for paid "
        "classification. Lazy: only read when the responder builds its real "
        "Jev adapter. Never log or echo.",
    )
    jev_state_path: Path | None = Field(
        default=None,
        description="[DEPRECATED worker path] Jev participation ledger "
        "state. Defaults beside the configured "
        "delivery state (same parent, jev_state.json) so tmp delivery paths "
        "never leak ledger writes to the repo root. Must not collide with "
        "delivery/outbox/audit/mapping paths.",
    )
    conversation_max_messages: int = Field(
        default=20,
        ge=1,
        le=20,
        description="[DEPRECATED worker path] Correlated read_context "
        "window: whole recent messages, 1..20 "
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

    @field_validator("session_day_rollover_hour", mode="before")
    @classmethod
    def _reject_bool_float_rollover(cls, value: object) -> object:
        """Reject bools/floats before lax int coercion (env strings coerce)."""
        if isinstance(value, bool) or isinstance(value, float):
            raise ValueError("must be an int 0..23, not a bool or float")
        return value

    @field_validator("session_day_timezone", mode="after")
    @classmethod
    def _validate_session_day_timezone(cls, value: str) -> str:
        """Reject unknown timezones eagerly (rotation must never fail mid-tick)."""
        if parse_tz_name(value) is None:
            raise ValueError("must be a valid IANA timezone")
        return value

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
        outbox, outbox-state, audit, or mapping paths by the same normalization.
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
            "thyca_session_mapping_path",
        ):
            if jev_key == _lexical_path_key(getattr(self, name)):
                raise ValueError(f"jev_state_path collides with {name}")
        return self
