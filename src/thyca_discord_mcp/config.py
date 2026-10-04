"""Typed settings for thyca-discord-mcp.

Precedence (highest first): constructor args, environment variables,
then the project-local ``.env`` file. Env names match field names
case-insensitively, e.g. ``DISCORD_TOKEN``.
"""

from pathlib import Path

from pydantic import Field, SecretStr
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
    command_prefix: str = "!"
    intent_message_content: bool = True
    intent_members: bool = True
    chunk_guilds_at_startup: bool = False
