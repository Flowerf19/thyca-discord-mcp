"""Typed settings for thyca-discord-mcp.

Precedence (highest first): constructor args, environment variables,
then the project-local ``.env`` file. Env names match field names
case-insensitively, e.g. ``DISCORD_TOKEN``.
"""

import math
from pathlib import Path

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """Runtime configuration. IDs are ints; the token stays a SecretStr."""

    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", extra="ignore")

    discord_token: SecretStr = Field(description="Discord bot token. Never log or echo.")
    owner_uid: int = Field(description="Discord user ID of the owner.")
    guild_id: int = Field(description="Guild (server) ID the bot is scoped to.")
    default_channel_id: int = Field(description="Channel used when a tool omits channel_id.")
    session_id: str = Field(
        default="discord-main",
        description="Fixed memory session for Discord activity.",
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
    thyca_session_mapping_path: Path = Field(
        default=Path.home() / ".thyca/discord_session.json",
        description="Channel -> Thyca SID mapping file. Stored only; never read by Settings.",
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

    @model_validator(mode="after")
    def _default_watch_channels(self) -> "Settings":
        """Empty/missing watch list falls back to the default channel."""
        if not self.watch_channel_ids:
            self.watch_channel_ids = [self.default_channel_id]
        return self
