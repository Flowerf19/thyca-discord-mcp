"""Runtime SQL separation: all SQL text lives in store.queries/schema.

Structural guard (not a behavior test): the logic modules
(base/outbox/jobs/jev) must not embed SQL string literals -- every
``execute`` goes through ``sql.*`` constants or the two narrow builders,
except the pre-existing DDL migration iteration. The check anchors on
statement-leading keywords plus SQL body structure so ordinary prose
(e.g. 'update'/'with' inside error messages) never trips it.
"""

import ast
import re
from pathlib import Path

import pytest

from store import queries as sql
from store.schema import SCHEMA_VERSION

STORE_DIR = Path(__file__).resolve().parent.parent / "src" / "store"
LOGIC_MODULES = ("base.py", "outbox.py", "jobs.py", "jev.py")

_LEAD = re.compile(
    r"^(SELECT|INSERT|UPDATE|DELETE|PRAGMA|BEGIN|COMMIT|ROLLBACK"
    r"|ALTER|CREATE|DROP|WITH|EXPLAIN|VACUUM|REINDEX)\b",
    re.IGNORECASE,
)
_BODY = re.compile(r"FROM|WHERE|VALUES|INTO|TABLE|\bSET\b|\(|\?|;", re.IGNORECASE)


def _is_sql_literal(text: str) -> bool:
    text = text.strip()
    return bool(_LEAD.match(text) and _BODY.search(text))


def _module_tree(name: str) -> ast.Module:
    return ast.parse((STORE_DIR / name).read_text())


def _docstring_ids(tree: ast.Module) -> set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.body and isinstance(node.body[0], ast.Expr):
                value = node.body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    out.add(id(value))
    return out


@pytest.mark.parametrize("module", LOGIC_MODULES)
def test_no_sql_literals_in_logic_modules(module):
    """No SQL statement text embedded in store logic modules."""
    tree = _module_tree(module)
    skip = _docstring_ids(tree)
    bad = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in skip
        and _is_sql_literal(node.value)
    ]
    assert bad == [], f"{module} embeds SQL literals: {bad!r}"


@pytest.mark.parametrize("module", LOGIC_MODULES)
def test_execute_calls_routed_via_queries(module):
    """Every execute() first arg is sql.*, a sql builder, or DDL iteration."""
    tree = _module_tree(module)
    bad = []
    bare_stmt = 0
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("execute", "executescript")
            and node.args
        ):
            continue
        first = node.args[0]
        if (
            isinstance(first, ast.Attribute)
            and isinstance(first.value, ast.Name)
            and first.value.id == "sql"
        ):
            continue
        if (
            isinstance(first, ast.Call)
            and isinstance(first.func, ast.Attribute)
            and isinstance(first.func.value, ast.Name)
            and first.func.value.id == "sql"
        ):
            continue
        if isinstance(first, ast.Name) and first.id == "stmt":
            bare_stmt += 1
            continue
        if isinstance(first, ast.BinOp) and isinstance(first.op, ast.Add):
            continue  # executescript(_SCHEMA_V1_SQL + _SCHEMA_V2_SQL)
        bad.append(ast.dump(first)[:120])
    assert bad == [], f"{module} has unrouted SQL: {bad!r}"
    assert bare_stmt <= 2, f"{module}: unexpected bare-stmt execute sites ({bare_stmt})"


def test_user_version_builder_exact():
    assert sql.set_user_version(SCHEMA_VERSION) == f"PRAGMA user_version={SCHEMA_VERSION};"
    assert sql.set_user_version(1) == "PRAGMA user_version=1;"


def test_stage_filter_builder_exact():
    assert sql.jobs_in_stages("?,?") == "SELECT * FROM jobs WHERE stage IN (?,?) ORDER BY seq ASC;"


def test_representative_constants_exact():
    assert sql.TXN_BEGIN == "BEGIN IMMEDIATE;"
    assert sql.TXN_COMMIT == "COMMIT;"
    assert sql.TXN_ROLLBACK == "ROLLBACK;"
    assert sql.JOB_GET == "SELECT * FROM jobs WHERE channel_id=? AND message_id=?;"
    assert sql.JEV_GET == "SELECT * FROM jev_decisions WHERE channel_id=? AND message_id=?;"
    assert sql.OUTBOX_INSERT == (
        "INSERT OR IGNORE INTO outbox "
        "(seq, type, channel_id, message_id, author_uid, author_name, "
        "actor_kind, content, ts, author_is_bot, reply_to_message_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    )
    assert sql.JOB_SKIP.endswith("AND s.message_id=jobs.message_id);")
