from pathlib import Path

import pytest
from pydantic import ValidationError

from config import PROJECT_ROOT, Settings


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


def test_watch_channel_ids_default_to_default_channel(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test-token")
    monkeypatch.setenv("OWNER_UID", "222")
    monkeypatch.setenv("GUILD_ID", "333")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "444")
    monkeypatch.delenv("WATCH_CHANNEL_IDS", raising=False)
    assert Settings().watch_channel_ids == [444]


def test_watch_channel_ids_explicit_empty_list_falls_back():
    settings = Settings(
        discord_token="t",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        watch_channel_ids=[],
    )
    assert settings.watch_channel_ids == [3]


def test_watch_channel_ids_json_env_list(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test-token")
    monkeypatch.setenv("OWNER_UID", "222")
    monkeypatch.setenv("GUILD_ID", "333")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "444")
    monkeypatch.setenv("WATCH_CHANNEL_IDS", "[7, 8]")
    assert Settings().watch_channel_ids == [7, 8]


def test_watch_channel_ids_constructor_override():
    settings = Settings(
        discord_token="t",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        watch_channel_ids=[9],
    )
    assert settings.watch_channel_ids == [9]


def test_outbox_paths_default_under_project_root():
    settings = Settings(
        discord_token="t",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
    )
    assert settings.outbox_path == PROJECT_ROOT / "outbox.jsonl"
    assert settings.outbox_state_path == PROJECT_ROOT / "outbox_state.json"


_V5_ENV_VARS = (
    "THYCA_BASE_URL",
    "THYCA_SESSION_MAPPING_PATH",
    "DELIVERY_STATE_PATH",
    "RESPONDER_CHECK_SECONDS",
    "THYCA_REQUEST_TIMEOUT_SECONDS",
    "RECOVERY_POLL_SECONDS",
)


def _clear_v5_env(monkeypatch):
    for name in _V5_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_v5_defaults(monkeypatch):
    _clear_v5_env(monkeypatch)
    settings = Settings(
        discord_token="test",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
    )
    assert settings.thyca_base_url == "http://127.0.0.1:8765"
    assert settings.thyca_session_mapping_path == Path.home() / ".thyca/discord_session.json"
    assert settings.delivery_state_path == PROJECT_ROOT / "delivery_state.json"
    assert settings.responder_check_seconds == 1.0
    assert settings.thyca_request_timeout_seconds == 900.0
    assert settings.recovery_poll_seconds == 30.0


def test_v5_env_overrides(monkeypatch, tmp_path):
    mapping = tmp_path / "mapping.json"  # fake paths; must be stored, never read
    state = tmp_path / "delivery.json"
    monkeypatch.setenv("DISCORD_TOKEN", "test")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    monkeypatch.setenv("THYCA_BASE_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("THYCA_SESSION_MAPPING_PATH", str(mapping))
    monkeypatch.setenv("DELIVERY_STATE_PATH", str(state))
    monkeypatch.setenv("RESPONDER_CHECK_SECONDS", "2.5")
    monkeypatch.setenv("THYCA_REQUEST_TIMEOUT_SECONDS", "60")
    monkeypatch.setenv("RECOVERY_POLL_SECONDS", "45")
    settings = Settings()
    assert settings.thyca_base_url == "http://127.0.0.1:9999"
    assert settings.thyca_session_mapping_path == mapping
    assert settings.delivery_state_path == state
    assert settings.responder_check_seconds == 2.5
    assert settings.thyca_request_timeout_seconds == 60.0
    assert settings.recovery_poll_seconds == 45.0
    assert not mapping.exists() and not state.exists()


def test_v5_constructor_paths_not_read(tmp_path):
    mapping = tmp_path / "nonexistent" / "mapping.json"
    state = tmp_path / "nonexistent" / "delivery.json"
    settings = Settings(
        discord_token="test",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        thyca_session_mapping_path=mapping,
        delivery_state_path=state,
    )
    assert settings.thyca_session_mapping_path == mapping
    assert settings.delivery_state_path == state


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("responder_check_seconds", 0),
        ("responder_check_seconds", 0.0),
        ("responder_check_seconds", -1.0),
        ("responder_check_seconds", float("nan")),
        ("responder_check_seconds", float("inf")),
        ("responder_check_seconds", True),
        ("responder_check_seconds", False),
        ("thyca_request_timeout_seconds", 0),
        ("thyca_request_timeout_seconds", -5.0),
        ("thyca_request_timeout_seconds", float("nan")),
        ("thyca_request_timeout_seconds", float("inf")),
        ("thyca_request_timeout_seconds", True),
        ("recovery_poll_seconds", 0),
        ("recovery_poll_seconds", -1.0),
        ("recovery_poll_seconds", 29.9),
        ("recovery_poll_seconds", float("nan")),
        ("recovery_poll_seconds", float("inf")),
        ("recovery_poll_seconds", True),
    ],
)
def test_v5_numeric_rejections(monkeypatch, field, bad_value):
    _clear_v5_env(monkeypatch)
    with pytest.raises(ValidationError):
        Settings(
            discord_token="test",
            owner_uid=1,
            guild_id=2,
            default_channel_id=3,
            **{field: bad_value},
        )


def test_recovery_poll_seconds_floor_boundary(monkeypatch):
    _clear_v5_env(monkeypatch)
    ok = Settings(
        discord_token="test",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        recovery_poll_seconds=30.0,
    )
    assert ok.recovery_poll_seconds == 30.0
    with pytest.raises(ValidationError):
        Settings(
            discord_token="test",
            owner_uid=1,
            guild_id=2,
            default_channel_id=3,
            recovery_poll_seconds=29.999,
        )


def test_v5_numeric_int_coercion_preserved(monkeypatch):
    """Plain ints still coerce to float; only bool/NaN/Inf/out-of-range are rejected."""
    _clear_v5_env(monkeypatch)
    settings = Settings(
        discord_token="test",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        responder_check_seconds=2,
        thyca_request_timeout_seconds=60,
        recovery_poll_seconds=30,
    )
    assert settings.responder_check_seconds == 2.0
    assert settings.thyca_request_timeout_seconds == 60.0
    assert settings.recovery_poll_seconds == 30.0


def test_v5_env_bool_string_rejected(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    monkeypatch.setenv("RESPONDER_CHECK_SECONDS", "true")
    with pytest.raises(ValidationError):
        Settings()
