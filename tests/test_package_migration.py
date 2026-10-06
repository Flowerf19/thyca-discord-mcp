"""Packaging checks for the completed single-package migration (offline).

Portable: repo-relative paths only, no baselines, no env reads, no network,
no keys, no production paths. Verifies the retired package namespace is
gone, every expected ``modum`` module imports, the console entry target
resolves (never invoked), and the distribution name stays unchanged.
"""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path

import modum

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "modum"

# Built at runtime so this file itself never contains the retired namespace.
_RETIRED = "thyca" + "_discord_" + "mcp"

EXPECTED_FILES = (
    "__init__.py",
    "__main__.py",
    "audit.py",
    "bot.py",
    "config.py",
    "context.py",
    "conversation.py",
    "delivery.py",
    "delivery_schema.py",
    "delivery_storage.py",
    "identity.py",
    "jev.py",
    "outbox.py",
    "participation.py",
    "responder.py",
    "responder_input.py",
    "responder_recovery.py",
    "server.py",
    "server_helpers.py",
    "server_ticks.py",
    "source_messages.py",
    "tasks.py",
    "thyca_client.py",
    "thyca_errors.py",
    "thyca_prompt.py",
    "thyca_trace.py",
    "thyca_transport.py",
    "thyca_validate.py",
)

EXPECTED_IMPORTS = (
    "__main__",
    "audit",
    "bot",
    "config",
    "context",
    "conversation",
    "delivery",
    "delivery_schema",
    "delivery_storage",
    "identity",
    "jev",
    "outbox",
    "participation",
    "responder",
    "responder_input",
    "responder_recovery",
    "server",
    "server_helpers",
    "server_ticks",
    "source_messages",
    "tasks",
    "thyca_client",
    "thyca_errors",
    "thyca_prompt",
    "thyca_trace",
    "thyca_transport",
    "thyca_validate",
)


def test_retired_namespace_not_importable():
    assert importlib.util.find_spec(_RETIRED) is None


def test_retired_package_dir_gone():
    assert not (ROOT / "src" / _RETIRED).exists()
    assert sorted(p.name for p in ROOT.glob("src/*") if p.is_dir()) == ["modum"]


def test_package_root_minimal():
    assert modum.__version__ == "0.1.0"
    assert modum.__all__ == ["__version__"]
    for heavy in ("Settings", "Responder", "create_server", "ThycaClient"):
        assert not hasattr(modum, heavy), heavy


def test_package_file_surface_exact():
    assert sorted(p.name for p in SRC.glob("*.py")) == sorted(EXPECTED_FILES)


def test_all_modules_import():
    for name in EXPECTED_IMPORTS:
        assert importlib.import_module(f"modum.{name}") is not None


def test_console_entry_target_resolves_without_invoking():
    from modum.__main__ import main

    assert callable(main)
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'thyca-discord-mcp = "modum.__main__:main"' in pyproject
    assert 'name = "thyca-discord-mcp"' in pyproject


def test_no_retired_namespace_in_shipped_sources():
    targets = [ROOT / "pyproject.toml", *sorted(SRC.glob("*.py"))]
    targets.extend(sorted((ROOT / "tests").glob("*.py")))
    targets.extend(sorted((ROOT / "experiments").rglob("*.py")))
    assert len(targets) > 30
    for path in targets:
        assert _RETIRED not in path.read_text(encoding="utf-8"), path.name


def test_moved_namespaces_resolve_under_new_package():
    from modum.bot import build_bot
    from modum.config import Settings
    from modum.responder import Responder
    from modum.server import create_server
    from modum.thyca_client import ThycaClient

    assert build_bot.__module__ == "modum.bot"
    assert Settings.__module__ == "modum.config"
    assert Responder.__module__ == "modum.responder"
    assert create_server.__module__ == "modum.server"
    assert ThycaClient.__module__ == "modum.thyca_client"
