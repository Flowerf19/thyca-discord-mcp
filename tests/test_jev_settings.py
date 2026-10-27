"""Settings tests for Jev routing configuration (offline, tmp paths only).

Covers the two remaining routing fields: ``typesafe_api_key`` (lazy
SecretStr) and ``conversation_max_messages`` (strict 1..20), plus the
one-line lexical guard keeping ``db_path`` and ``audit_path`` distinct.
The ``jev_state_path``/``delivery_state_path`` collision matrix died with
the split files: one SQLite store holds jobs + Jev, so there is nothing
left to collide. Per-call Jev spend has no daily stop. No network, no
real .env (all constructions pass ``_env_file=None`` unless testing env
override), no key material asserted beyond non-leak checks.
"""

import pytest
from pydantic import SecretStr, ValidationError

from config import Settings

_JEV_ENV_VARS = (
    "TYPESAFE_API_KEY",
    "CONVERSATION_MAX_MESSAGES",
)


def _clear_jev_env(monkeypatch):
    for name in _JEV_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _base_kwargs():
    return dict(
        discord_token="test",
        owner_uid=1,
        guild_id=2,
        default_channel_id=3,
        _env_file=None,
    )


def test_jev_defaults(monkeypatch):
    _clear_jev_env(monkeypatch)
    settings = Settings(**_base_kwargs())
    assert settings.typesafe_api_key is None
    assert settings.conversation_max_messages == 20


def test_db_audit_collision_rejected(monkeypatch, tmp_path):
    """The store and the audit log must not be the same file."""
    _clear_jev_env(monkeypatch)
    victim = tmp_path / "victim.db"
    with pytest.raises(ValidationError, match="db_path collides"):
        Settings(**_base_kwargs(), db_path=victim, audit_path=victim)


def test_db_audit_dotdot_alias_rejected(monkeypatch, tmp_path):
    """Lexical ../ aliases collide though exact Path equality differs (no I/O)."""
    _clear_jev_env(monkeypatch)
    victim = tmp_path / "victim.db"
    alias = tmp_path / "unused" / ".." / "victim.db"
    assert victim != alias  # exact equality misses the alias
    assert not victim.exists() and not (tmp_path / "unused").exists()
    with pytest.raises(ValidationError, match="db_path collides"):
        Settings(**_base_kwargs(), db_path=victim, audit_path=alias)
    with pytest.raises(ValidationError, match="db_path collides"):
        Settings(**_base_kwargs(), db_path=alias, audit_path=victim)
    assert not victim.exists() and not (tmp_path / "unused").exists()


def test_db_audit_distinct_accepted_no_io(monkeypatch, tmp_path):
    """Lexically different paths are accepted; parents need not exist."""
    _clear_jev_env(monkeypatch)
    db = tmp_path / "no-such-db" / "state.db"
    audit = tmp_path / "no-such-audit" / "sub" / ".." / "audit.jsonl"
    assert not (tmp_path / "no-such-db").exists()
    assert not (tmp_path / "no-such-audit").exists()
    settings = Settings(**_base_kwargs(), db_path=db, audit_path=audit)
    assert settings.db_path == db
    assert settings.audit_path == audit
    assert not db.exists() and not audit.exists()


def test_typesafe_key_secret_not_leaked(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    monkeypatch.setenv("TYPESAFE_API_KEY", "jev-super-secret")
    monkeypatch.delenv("CONVERSATION_MAX_MESSAGES", raising=False)
    settings = Settings()
    assert isinstance(settings.typesafe_api_key, SecretStr)
    assert settings.typesafe_api_key.get_secret_value() == "jev-super-secret"
    assert "jev-super-secret" not in repr(settings)
    assert "jev-super-secret" not in str(settings.model_dump())


def test_jev_env_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("DISCORD_TOKEN", "test")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    monkeypatch.setenv("TYPESAFE_API_KEY", "env-key")
    monkeypatch.delenv("JEV_STATE_PATH", raising=False)
    monkeypatch.setenv("CONVERSATION_MAX_MESSAGES", "12")
    settings = Settings()
    assert settings.typesafe_api_key.get_secret_value() == "env-key"
    assert settings.conversation_max_messages == 12


@pytest.mark.parametrize("good", [1, 20])
def test_max_messages_boundaries(monkeypatch, good):
    _clear_jev_env(monkeypatch)
    assert (
        Settings(**_base_kwargs(), conversation_max_messages=good).conversation_max_messages == good
    )


@pytest.mark.parametrize("bad", [0, 21, -1, True, False, 2.0, 2.5, None])
def test_max_messages_rejections(monkeypatch, bad):
    """Int 1..20: bools/floats/out-of-range all rejected."""
    _clear_jev_env(monkeypatch)
    with pytest.raises(ValidationError):
        Settings(**_base_kwargs(), conversation_max_messages=bad)


def test_max_messages_env_string_coerces(monkeypatch):
    """Env strings with clean ints coerce (pydantic-settings reads env as str)."""
    _clear_jev_env(monkeypatch)
    assert (
        Settings(**_base_kwargs(), conversation_max_messages="12").conversation_max_messages == 12
    )


def test_removed_path_kwargs_ignored_no_write(tmp_path):
    """Stale removed-field kwargs are ignored (extra=ignore); no files made.

    Guards the documented trap: a fixture still passing
    ``delivery_state_path``/``jev_state_path`` would silently write to the
    real default ``state.db``. Grep must show zero such constructions
    outside this guard test; this test pins the ignore behavior itself.
    """
    settings = Settings(
        **_base_kwargs(),
        delivery_state_path=tmp_path / "stale-delivery.json",
        jev_state_path=tmp_path / "stale-jev.json",
    )
    assert not (tmp_path / "stale-delivery.json").exists()
    assert not (tmp_path / "stale-jev.json").exists()
    assert settings.db_path is not None
