"""Settings tests for Jev routing configuration (offline, tmp paths only).

Covers the three routing fields: ``typesafe_api_key`` (lazy SecretStr),
``jev_state_path`` (derived beside delivery, collision-rejected), and
``conversation_max_messages`` (strict 1..20). Per-call Jev spend has no
daily stop. No network, no real .env
(all constructions pass ``_env_file=None`` unless testing env override),
no key material asserted beyond non-leak checks.
"""

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from config import PROJECT_ROOT, Settings

_JEV_ENV_VARS = (
    "TYPESAFE_API_KEY",
    "JEV_STATE_PATH",
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
    assert settings.jev_state_path == PROJECT_ROOT / "jev_state.json"
    assert settings.conversation_max_messages == 20


def test_jev_state_derives_beside_tmp_delivery(monkeypatch, tmp_path):
    """Tmp delivery paths never leak ledger writes to the repo root."""
    _clear_jev_env(monkeypatch)
    delivery = tmp_path / "sub" / "delivery.json"
    settings = Settings(**_base_kwargs(), delivery_state_path=delivery)
    assert settings.jev_state_path == tmp_path / "sub" / "jev_state.json"
    assert settings.jev_state_path != PROJECT_ROOT / "jev_state.json"


def test_jev_state_explicit_kept(monkeypatch, tmp_path):
    _clear_jev_env(monkeypatch)
    explicit = tmp_path / "custom-jev.json"
    settings = Settings(**_base_kwargs(), jev_state_path=explicit)
    assert settings.jev_state_path == explicit


@pytest.mark.parametrize(
    "field",
    [
        "delivery_state_path",
        "outbox_path",
        "outbox_state_path",
        "audit_path",
    ],
)
def test_jev_state_collisions_rejected(monkeypatch, tmp_path, field):
    """The owned ledger path must not collide with delivery/outbox/audit."""
    _clear_jev_env(monkeypatch)
    victim = tmp_path / "victim.json"
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(**_base_kwargs(), **{field: victim, "jev_state_path": victim})


def test_typesafe_key_secret_not_leaked(monkeypatch):
    monkeypatch.setenv("DISCORD_TOKEN", "test")
    monkeypatch.setenv("OWNER_UID", "1")
    monkeypatch.setenv("GUILD_ID", "2")
    monkeypatch.setenv("DEFAULT_CHANNEL_ID", "3")
    monkeypatch.setenv("TYPESAFE_API_KEY", "jev-super-secret")
    for name in ("JEV_STATE_PATH", "CONVERSATION_MAX_MESSAGES"):
        monkeypatch.delenv(name, raising=False)
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
    custom = tmp_path / "env-jev.json"
    monkeypatch.setenv("TYPESAFE_API_KEY", "env-key")
    monkeypatch.setenv("JEV_STATE_PATH", str(custom))
    monkeypatch.setenv("CONVERSATION_MAX_MESSAGES", "12")
    settings = Settings()
    assert settings.typesafe_api_key.get_secret_value() == "env-key"
    assert settings.jev_state_path == custom
    assert settings.conversation_max_messages == 12
    assert not custom.exists()


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


@pytest.mark.parametrize(
    "field",
    [
        "delivery_state_path",
        "outbox_path",
        "outbox_state_path",
        "audit_path",
    ],
)
def test_jev_state_dotdot_aliases_rejected(monkeypatch, tmp_path, field):
    """Lexical ../ aliases collide though exact Path equality differs (no I/O)."""
    _clear_jev_env(monkeypatch)
    victim = tmp_path / "victim.json"
    alias = tmp_path / "unused" / ".." / "victim.json"
    assert victim != alias  # exact equality misses the alias (prepatch RED)
    assert not victim.exists() and not (tmp_path / "unused").exists()
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            **{field: victim, "jev_state_path": alias},
        )
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            **{field: alias, "jev_state_path": victim},
        )
    assert not victim.exists() and not (tmp_path / "unused").exists()


@pytest.mark.parametrize(
    "field",
    [
        "delivery_state_path",
        "outbox_path",
        "outbox_state_path",
        "audit_path",
    ],
)
def test_jev_state_dot_aliases_rejected(monkeypatch, tmp_path, field):
    """Single-dot aliases collide (Path collapses .; guard must also reject)."""
    _clear_jev_env(monkeypatch)
    victim = tmp_path / "victim.json"
    alias = tmp_path / "." / "victim.json"
    assert not victim.exists()
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            **{field: victim, "jev_state_path": alias},
        )
    assert not victim.exists()


@pytest.mark.parametrize(
    "field",
    [
        "delivery_state_path",
        "outbox_path",
        "outbox_state_path",
        "audit_path",
    ],
)
def test_jev_state_absolute_relative_aliases_rejected(monkeypatch, tmp_path, field):
    """Absolute-vs-relative aliases collide via pure abspath (no I/O, chdir TMP)."""
    _clear_jev_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    absolute = tmp_path / "victim.json"
    relative = Path("victim.json")
    assert absolute != relative  # exact equality misses cwd-relative alias
    assert not absolute.exists()
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            **{field: absolute, "jev_state_path": relative},
        )
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            **{field: relative, "jev_state_path": absolute},
        )
    assert not absolute.exists()


@pytest.mark.parametrize(
    "field",
    [
        "outbox_path",
        "outbox_state_path",
        "audit_path",
    ],
)
def test_jev_state_default_derived_dotdot_collision_rejected(monkeypatch, tmp_path, field):
    """Schema-native default (jev=None) rejects lexical aliases of derived path."""
    _clear_jev_env(monkeypatch)
    delivery = tmp_path / "sub" / "delivery.json"
    derived = tmp_path / "sub" / "jev_state.json"
    derived_alias = tmp_path / "sub" / "unused" / ".." / "jev_state.json"
    assert derived != derived_alias  # exact equality misses derived alias
    assert not (tmp_path / "sub").exists()
    with pytest.raises(ValidationError, match="jev_state_path collides"):
        Settings(
            **_base_kwargs(),
            typesafe_api_key=None,
            delivery_state_path=delivery,
            **{field: derived_alias},
        )
    assert not (tmp_path / "sub").exists()
    assert not derived.exists() and not derived_alias.exists()


def test_jev_state_default_beside_delivery_name_collision(monkeypatch, tmp_path):
    """Derived default beside delivery collides when delivery is named jev_state."""
    _clear_jev_env(monkeypatch)
    delivery = tmp_path / "sub" / "jev_state.json"
    assert not (tmp_path / "sub").exists()
    with pytest.raises(ValidationError, match="collides with delivery"):
        Settings(**_base_kwargs(), typesafe_api_key=None, delivery_state_path=delivery)
    assert not (tmp_path / "sub").exists()
    assert not delivery.exists()


def test_jev_state_lexically_different_paths_accepted_no_io(monkeypatch, tmp_path):
    """Lexically different paths (even with ..) are accepted; parents need not exist."""
    _clear_jev_env(monkeypatch)
    owned = tmp_path / "no-such-owned" / "victim.json"
    other = tmp_path / "no-such-jev" / "sub" / ".." / "other.json"
    assert not (tmp_path / "no-such-owned").exists()
    assert not (tmp_path / "no-such-jev").exists()
    settings = Settings(
        **_base_kwargs(),
        typesafe_api_key=None,
        audit_path=owned,
        jev_state_path=other,
    )
    assert settings.audit_path == owned
    assert settings.jev_state_path == other
    assert not owned.exists() and not other.exists()
    assert not (tmp_path / "no-such-owned").exists()
    assert not (tmp_path / "no-such-jev").exists()


def test_jev_state_all_tmp_distinct_preserved_no_io(monkeypatch, tmp_path):
    """All TMP distinct owned+ledger paths accepted, values preserved, no files made."""
    _clear_jev_env(monkeypatch)
    kwargs = dict(
        delivery_state_path=tmp_path / "d" / "delivery.json",
        outbox_path=tmp_path / "o" / "outbox.jsonl",
        outbox_state_path=tmp_path / "os" / "state.json",
        audit_path=tmp_path / "a" / "audit.jsonl",
        jev_state_path=tmp_path / "j" / "jev.json",
        typesafe_api_key=None,
    )
    settings = Settings(**_base_kwargs(), **kwargs)
    for name, value in kwargs.items():
        if name == "typesafe_api_key":
            assert settings.typesafe_api_key is None
        else:
            assert getattr(settings, name) == value
    for value in kwargs.values():
        if isinstance(value, Path):
            assert not value.exists()
            assert not value.parent.exists()


def test_jev_state_preserves_values_and_hides_key(monkeypatch, tmp_path):
    """Chosen .. values kept exactly (not normalized); fake key never printed."""
    _clear_jev_env(monkeypatch)
    explicit = tmp_path / "sub" / ".." / "custom-jev.json"
    audit = tmp_path / "audit.jsonl"
    settings = Settings(
        **_base_kwargs(),
        typesafe_api_key="FAKE-KEY-NOT-REAL",
        audit_path=audit,
        jev_state_path=explicit,
    )
    assert settings.jev_state_path == explicit
    assert ".." in str(settings.jev_state_path)  # proves no rewrite to normalized
    assert settings.audit_path == audit
    assert isinstance(settings.typesafe_api_key, SecretStr)
    assert "FAKE-KEY-NOT-REAL" not in repr(settings)
    assert "FAKE-KEY-NOT-REAL" not in str(settings.model_dump())
    assert not audit.exists()
    assert not (tmp_path / "sub").exists()
