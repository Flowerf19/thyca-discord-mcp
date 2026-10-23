"""Packaging checks for the flat-src layout (offline).

Portable: repo-relative paths only, no baselines, no env reads, no network,
no keys, no production paths. Verifies the retired namespaces are gone,
every expected top-level module/package imports, the console entry target
resolves (never invoked), and the distribution name stays unchanged.
"""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

# Built at runtime so this file itself never contains the retired namespaces.
_RETIRED = "thyca" + "_discord_" + "mcp"
_RETIRED_PKG = "mod" + "um"

EXPECTED_FILES = (
    "audit.py",
    "bot.py",
    "config.py",
    "context.py",
    "conversation.py",
    "delivery_schema.py",
    "delivery_storage.py",
    "identity.py",
    "instance_lock.py",
    "jev.py",
    "main.py",
    "outbox.py",
    "server_helpers.py",
    "server_ticks.py",
    "sessions.py",
    "source_messages.py",
    "tasks.py",
    "thyca.py",
)

EXPECTED_PACKAGES = {
    "adapters": ("__init__.py", "discord_gateway.py"),
    "delivery": ("__init__.py", "queries.py", "store.py", "transitions.py"),
    "participation": ("__init__.py", "errors.py", "ledger.py", "store.py", "validate.py"),
    "policy": ("__init__.py", "scope_guard.py"),
    "responder": ("__init__.py", "core.py", "input.py", "recovery.py", "rotation.py", "submit.py"),
    "server": ("__init__.py", "core.py"),
    "services": (
        "__init__.py",
        "context_reader.py",
        "message_sender.py",
        "outbox_service.py",
    ),
    "tools": ("__init__.py", "transport.py"),
}

EXPECTED_IMPORTS = (
    "adapters.discord_gateway",
    "audit",
    "bot",
    "config",
    "context",
    "conversation",
    "delivery",
    "delivery.queries",
    "delivery.store",
    "delivery.transitions",
    "delivery_schema",
    "delivery_storage",
    "identity",
    "instance_lock",
    "jev",
    "main",
    "outbox",
    "participation",
    "participation.errors",
    "participation.ledger",
    "participation.store",
    "participation.validate",
    "policy.scope_guard",
    "responder",
    "responder.core",
    "responder.input",
    "responder.recovery",
    "responder.rotation",
    "responder.submit",
    "server",
    "server.core",
    "server_helpers",
    "server_ticks",
    "services.context_reader",
    "services.message_sender",
    "services.outbox_service",
    "sessions",
    "source_messages",
    "tasks",
    "thyca",
    "tools.transport",
)


def test_retired_namespace_not_importable():
    assert importlib.util.find_spec(_RETIRED) is None
    assert importlib.util.find_spec(_RETIRED_PKG) is None


def test_retired_package_dir_gone():
    assert not (ROOT / "src" / _RETIRED).exists()
    assert not (ROOT / "src" / _RETIRED_PKG).exists()
    assert sorted(p.name for p in ROOT.glob("src/*") if p.is_dir() and p.name != "__pycache__") == [
        "adapters",
        "delivery",
        "participation",
        "policy",
        "responder",
        "server",
        "services",
        "tools",
    ]


def test_no_package_root_leftover():
    # Flat layout: no top-level package module, version lives in pyproject.
    assert (SRC / "__init__.py").exists() is False
    assert (SRC / _RETIRED_PKG).exists() is False


def test_package_file_surface_exact():
    assert sorted(p.name for p in SRC.glob("*.py")) == sorted(EXPECTED_FILES)
    for dirname, files in EXPECTED_PACKAGES.items():
        assert sorted(p.name for p in (SRC / dirname).glob("*.py")) == sorted(files)


def test_all_modules_import():
    for name in EXPECTED_IMPORTS:
        assert importlib.import_module(f"{name}") is not None


def test_console_entry_target_resolves_without_invoking():
    from main import main

    assert callable(main)
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'thyca-discord-mcp = "main:main"' in pyproject
    assert 'name = "thyca-discord-mcp"' in pyproject


def test_no_retired_namespace_in_shipped_sources():
    targets = [ROOT / "pyproject.toml", *sorted(SRC.glob("*.py"))]
    targets.extend(sorted((ROOT / "tests").glob("*.py")))
    targets.extend(sorted((ROOT / "experiments").rglob("*.py")))
    assert len(targets) > 30
    for path in targets:
        assert _RETIRED not in path.read_text(encoding="utf-8"), path.name


def test_moved_namespaces_resolve_under_new_package():
    from bot import build_bot
    from config import Settings
    from responder import Responder
    from server import create_server
    from thyca import ThycaClient

    assert build_bot.__module__ == "bot"
    assert Settings.__module__ == "config"
    assert Responder.__module__ == "responder.core"
    assert create_server.__module__ == "server.core"
    assert ThycaClient.__module__ == "thyca"
