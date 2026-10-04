from thyca_discord_mcp.config import Settings


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test-token")
    monkeypatch.setenv("OWNER_UID", "222")
    monkeypatch.setenv("GUILD_ID", "333")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "444")
    settings = Settings()
    assert settings.discord_token.get_secret_value() == "test-token"
    assert settings.owner_uid == 222
    assert settings.guild_id == 333
    assert settings.default_channel_id == 444
    assert settings.session_id == "discord-main"
    assert settings.intent_message_content is True
    assert settings.intent_members is True


def test_settings_init_args_override_all():
    settings = Settings(
        discord_token="arg-token",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
    )
    assert settings.discord_token.get_secret_value() == "arg-token"
    assert (settings.owner_uid, settings.guild_id, settings.default_channel_id) == (1, 2, 3)


def test_token_not_leaked_in_repr(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "super-secret")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    assert "super-secret" not in repr(Settings())
