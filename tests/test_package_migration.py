"""Packaging smoke: every shipped module imports, console entry resolves.

Portable: repo-relative paths only, no baselines, no env reads, no network,
no keys, no production paths. Import surface is derived from the tree itself
(no exact file list to rot); the console entry target resolves without
invoking. Deleted-module absence is covered by the wheel-content check in the
Phase 5 report, not pinned here.
"""

from __future__ import annotations

import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"


def _all_module_names() -> list[str]:
    names: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(SRC).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if parts:
            names.append(".".join(parts))
    assert names, "no modules found under src/"
    return names


def test_all_tree_modules_import():
    for name in _all_module_names():
        assert importlib.import_module(name) is not None


def test_console_entry_target_resolves_without_invoking():
    from main import main

    assert callable(main)
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'thyca-discord-mcp = "main:main"' in pyproject
    assert 'name = "thyca-discord-mcp"' in pyproject
