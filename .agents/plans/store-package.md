---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

# PLAN store.py -> store/ package with SQL separated

> Owner directive 2026-10-27 (overrides the module-split audit's
> "keep" verdict for store.py): split it into a directory module and
> take the SQL out. Repo-wide grep confirms store.py is the ONLY file
> embedding SQL/sqlite3; every other module goes through Store, so no
> other file changes pattern. Base: main@68c377b, 899 tests.

## Layout (one Store class, mixin composition — atomicity preserved)

The audit's warning stands and shapes the design: cross-domain
transactions (`mark_jev_observe` = jev+jobs in one txn, `_job_dict`
joins jobs+sends+jev, `enqueue_job` FK to outbox) must NOT be split
across connections or classes. So: one shared base, domain mixins,
one composed `Store`.

```
src/store/__init__.py  # Store, StoreError + the 6 open-use-close wrappers
src/store/schema.py    # DDL: SCHEMA_VERSION, _SCHEMA_V1_SQL, _SCHEMA_V2_SQL
src/store/queries.py   # named query/PRAGMA/transaction SQL constants;
                       # narrowly scoped builders for existing dynamic SQL
src/store/errors.py    # StoreError
src/store/base.py      # StoreBase: open/close, pragmas, quick_check,
                       # user_version gate + v1->v2 migration, _txn,
                       # _state, _require_id/_check_sid/_check_day,
                       # row converters (_row_to_entry/_row_to_jev),
                       # get_native_sid/adopt_session (singleton state)
src/store/outbox.py    # OutboxMixin: _insert_one, append_entry,
                       # append_poll_batch, read_entries, ack_entries,
                       # get/set_poll_cursor
src/store/jobs.py      # JobsMixin: _get_job_row/_sends_for/_job_dict/
                       # _require_job_row, _transition_job, enqueue/get/
                       # list/first_unresolved/processed_prefix,
                       # transitions, sweep_submitted_failed
src/store/jev.py       # JevMixin: get_jev, insert_jev_pending,
                       # bump_jev_retry, resolve_jev, mark_jev_observe,
                       # jev_cost_for_day, jev_unknown_blocked
```

`class Store(OutboxMixin, JobsMixin, JevMixin, StoreBase)` — no method
collisions expected (verify); mixins call `self._txn()` etc. exactly as
today. DDL lives in schema.py; queries/PRAGMAs/transaction statements
live in queries.py, separated from validation and execution logic.
Use named constants, plus small builders only for existing dynamic SQL
(version pragma and variable stage placeholders). Preserve SQL text,
parameter order, binding and transaction boundaries. No ORM, generic
query loader, or new dependency. Python SQL modules avoid introducing
runtime resource loading and additional wheel data configuration.

Initial extraction f9eddbba separated only DDL. Lead requested a
completion round to separate ALL runtime SQL as required by the owner,
not just schema creation.

## Rules

- Pure move: method bodies verbatim; `Store.open` behavior (0600, WAL,
  FULL, quick_check, version gate, migration) identical; public surface
  `from store import Store, StoreError, append_entry, append_poll_batch,
  read_entries, ack_entries, get_poll_cursor, set_poll_cursor` unchanged
  — grep every src/test import and patch site of `store.*` internals
  first and keep them resolving (re-export from __init__ if a test
  imports a private name).
- pyproject: add `src/store` to wheel packages, drop the `src/store.py`
  force-include (same pattern as thyca/context/jev splits).
- Validation: `uv run pytest -q` (899), golden, ruff check+format,
  `git diff --check`, `uv build` (wheel lists store/*.py, no store.py);
  import-surface snippet for all public names. Independent read-only
  review afterward (this touches the durability layer), then plan
  close-out. Commit/restart on separate owner orders as usual.

## Completion evidence

Implemented in retained writer runs f9eddbba and 728c67c8. Runtime SQL
is separated into 48 constants and two narrow builders in queries.py;
schema scripts remain in schema.py. No executable SQL was found outside
store/. Existing tracked tests are unchanged; 11 new SQL structural
checks bring the suite to 910 tests.

Lead verification: all original Store methods are AST-identical after
SQL-constant/builder substitution and normalization of the local loop
variable and return annotations; all 48 SQL constants and both schema
scripts match the original text. No method collisions; one composed
Store, connection and transaction boundary remain intact.

`uv run pytest -q`: 910 passed; ruff lint and format checks, diff check,
and `uv build` passed. Isolated built-wheel imports and a temporary DB
smoke passed (creation, version/quick_check/foreign_keys, dynamic stage
filter, job transition, reopen, ACK, mode 0600). No live DB was used.
Independent read-only review 1e785fec: Approve, no confirmed defects.
Plans and current documentation updated. Not committed or restarted.
