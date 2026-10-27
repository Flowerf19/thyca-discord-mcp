"""Offline regression for the jev_conversation fixture relocation.

The canonical 30-case fixture moved byte-identical from
``experiments/jev_conversation/cases.json`` to
``tests/fixtures/jev_conversation_cases.json``; the experiment runner and
all of its tests/parity checks stay. This pins the relocation so a future
move or stale default path fails loudly instead of silently loading nothing.

Fake/offline only: reads the fixture file and the runner's default path.
No SDK, no network, no env, no paid calls.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

RUNNER_PATH = (
    Path(__file__).resolve().parent.parent / "experiments" / "jev_conversation" / "run_eval.py"
)
FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "jev_conversation_cases.json"
LEGACY_PATH = (
    Path(__file__).resolve().parent.parent / "experiments" / "jev_conversation" / "cases.json"
)


def _load_runner():
    name = "jev_run_eval_relocation_check"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, RUNNER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_fixture_lives_under_tests_fixtures_with_thirty_cases():
    assert FIXTURE_PATH.is_file()
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert len(payload["cases"]) == 30
    assert all(set(case) >= {"id", "state"} for case in payload["cases"])


def test_legacy_experiment_fixture_path_is_gone():
    assert not LEGACY_PATH.exists()


def test_runner_default_cases_path_resolves_to_fixture():
    runner = _load_runner()
    assert runner.default_cases_path().resolve() == FIXTURE_PATH.resolve()
