"""Typed settings for thyca-discord-mcp.

Precedence (highest first): constructor args, environment variables,
then the project-local ``.env`` file. Env names match field names
case-insensitively, e.g. ``DISCORD_TOKEN``.
"""

from pathlib import Path

from pydantic import Field, SecretStr, model_validator
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

    @model_validator(mode="after")
    def _default_watch_channels(self) -> "Settings":
        """Empty/missing watch list falls back to the default channel."""
        if not self.watch_channel_ids:
            self.watch_channel_ids = [self.default_channel_id]
        return self
