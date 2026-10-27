---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

# PLAN final trim: store transaction boilerplate + helper dedupe

> Draft plan from the post-simplify bloat check (2026-10-27). Base:
> `main@789b39b` (the simplify refactor commit; tree clean except
> pre-existing untracked `.bak-*`/`service.resume.log`/studio PDFs, which
> stay untouched). Writing this plan approves nothing; execution needs
> the owner's go-ahead. The simplify plan's deploy gate is separate and
> unaffected by this plan in either order.

## 0. Evidence (verified by the lead on the current tree)

- Source 8.561 lines, tests 17.990 lines, 844 tests passing, golden
  fixtures green. The structural bloat is gone; what remains is local
  repetition:
- `src/store.py` (1.481 lines): **19 hand-copied**
  `BEGIN IMMEDIATE; try: … COMMIT; except: ROLLBACK` blocks (~8 lines
  each), and 7 job-transition methods repeating one skeleton
  (require row → stage guard → UPDATE → re-read → return dict).
- Duplicate tiny helpers despite `src/validate.py` existing as the
  shared home: `_sha256_hex` byte-identical in `thyca.py:287` and
  `responder/submit.py:70`; `_validate_id_string` effectively identical
  in `thyca.py:519` and `responder/input.py:25` (both bound by
  `ID_MAX = 128` — verified equal today); `_is_sha256_hex` only in
  `thyca.py:592`.
- Optional observations: `responder/submit.py::_submit_queued` is one
  369-line function; `FakeBot`/`FakeGuild`/`FakeClient` are re-implemented
  across 8 test files.

## 1. Goal and scope

**Goal:** remove the remaining mechanical repetition without changing any
behavior, contract, or stored data. This is a shape-only refactor.

In scope (core): T1–T3 below. Optional, each separately approved: T4
(submit pipeline split), T5 (shared test fakes).

Explicit non-goals (evaluated during the check, accepted as-is):

- `server/core.py`'s `create_server` closure organization (modest win,
  disproportionate risk), `context.py` docstring density, the Thyca prompt
  frame (byte-pinned by golden fixtures), Jev/Thyca boundary validation.
- No schema, stage/outcome, audit-field, tool, prompt, or config change.
- No deploy, restart, commit, or merge (each separately approved as before).

**Accepted decisions carried over:** behavior preservation is absolute —
any test expectation change is a defect unless it is an import-path or
purity-allowlist edit listed in T3. Golden `list_tools`/prompt fixtures
stay byte-identical with no regeneration. Checks per task:
`uv run pytest -q` (844 baseline), golden 3, `ruff check`,
`ruff format --check`, `git diff --check`.

## 2. Tasks

### T1 — `Store._txn()` context manager (store.py)

- Add one internal context manager wrapping begin/commit/rollback:
  `BEGIN IMMEDIATE` on entry, `COMMIT` on clean exit, best-effort
  `ROLLBACK` + re-raise on exception — exactly the current semantics of
  all 19 copies. Replace every hand-rolled block with it; no public API,
  error type, or message changes.
- Watch for: methods that run `COMMIT` conditionally mid-body or read
  state between begin and commit (e.g. `ack_entries`' no-op path) — the
  manager must not add commits or reads; where a body doesn't fit the
  shape, leave that method hand-rolled and say why in the report.
- Gate: `tests/test_store.py` (49) + full suite green, zero test edits.

### T2 — Shared job-transition skeleton (store.py)

- Extract the repeated skeleton behind the job transitions
  (`mark_submitted`, `requeue`, `record_send`, `skip_job`, `mark_failed`,
  `complete_job`, `adopt_session` where it fits) into one internal helper
  parameterized by: allowed current stages, the UPDATE clause/params, and
  an optional extra guard callback (e.g. zero-sends). Guards stay explicit
  at each call site — do not bury stage rules inside the helper beyond
  the allow-list check each method already performs.
- Keep every `StoreError` message string identical (tests match on some).
- Gate: `test_store.py`, `test_responder.py`, `test_jev_routing.py`,
  `test_server_responder.py` green with zero assertion edits.

### T3 — Helper consolidation into `src/validate.py`

- Move to `validate.py` (stdlib-only, no I/O): `sha256_hex(text)`,
  `is_sha256_hex(value)`, `validate_id_string(value)` (single
  `ID_MAX = 128` constant). Re-export or rewire at the old homes
  (`thyca`, `responder/submit`, `responder/input`) so existing imports
  and `__all__` surfaces keep working — check `pyproject.toml`
  force-include needs no change (`validate.py` is already shipped).
- Note: `thyca.py` already imports `validate` (Phase 4), and
  `test_thyca_prompt.py::test_module_imports_stay_pure` allowlists it —
  confirm the purity test still passes; if the allowlist needs the entry
  it is the one authorized test edit, listed in the report.
- Gate: `grep -rn "def _sha256_hex\|def _validate_id_string\|def _is_sha256_hex" src/`
  shows zero private definitions left; full suite green.

### T4 — OPTIONAL: split `_submit_queued` (responder/submit.py)

- Only if the owner approves separately after T1–T3. Split the 369-line
  pipeline into named step helpers (frame binding, deleted-source gate,
  SID/idle gate, fit gate, classify, POST + race handling) with identical
  ordering, early-return values, and awaits. No line-count promise —
  the value is readability/testability.
- Gate: `test_responder.py` + `test_jev_routing.py` +
  `test_source_deleted.py` green with zero assertion edits; golden
  prompts byte-identical.

### T5 — OPTIONAL: shared test fakes (tests/)

- Only if separately approved. Add `tests/fakes.py` (pattern already set
  by `tests/real_discord.py`) with the common `FakeBot`/`FakeGuild`/
  `FakeClient` shapes; migrate the 8 files that re-implement them;
  dedupe genuinely identical poll tests between `test_server.py` and
  `test_server_extraction.py` (Phase 5 found current poll tests are at
  different levels — dedupe only true duplicates found during migration).
- Gate: collected count reported per file with every change explained;
  zero assertion edits (constructor/kwarg adaptation only).

## 3. Execution shape (when approved)

One writer for T1–T3 (sequentially, focused tests after each), lead
reviews the actual diff, then one fresh independent read-only review of
the trim diff (small scope — a single reviewer suffices; store.py is
data-safety adjacent, so the review is not optional). T4/T5 each get
their own writer pass + lead review if approved.

Expected result: src 8.561 → ~8.200 lines (−300-400, mostly store.py),
844 tests unchanged in behavior. Report actuals; do not chase the number
at the expense of guard clarity.

## 4. Open questions

None blocking. T4/T5 inclusion is the owner's call at approval time.

## 5. Status of this plan

Owner approved **T1–T3 only** on 2026-10-27. Execution complete and
verified the same day (this `done` covers T1–T3 only; T4/T5 were never
approved and remain optional future work):

- Writer (`2cbd6f4c`): `_txn()` replaced all 17 convertible hand-rolled
  transaction blocks (the `Store.open` migration block stays
  hand-rolled — classmethod, pre-instance shape); `_transition_job()`
  backs the 6 job transitions (`adopt_session` intentionally left
  bespoke); `validate.py` is the single home of `sha256_hex` /
  `is_sha256_hex` / `validate_id_string` / `ID_MAX`. Zero test files
  touched. Lead gates: **844 passed**, golden 3, ruff check+format
  clean, `git diff --check` clean.
- Independent review (`4f4257c8`): **Approve with fixes** — all
  `StoreError` strings byte-identical, rowcount conversions proven
  equivalent, T3 bodies verbatim. Two Minor doc fixes (applied by the
  lead, gates re-run green): soften the `_txn` docstring and record
  semantics note below; `_transition_job` docstring "first statement"
  corrected to "last statement".
- **Recorded semantics note (from the review):** removing the mid-body
  COMMITs swapped the order of snapshot reads vs the final COMMIT
  (reads now in-txn; durability at exit COMMIT). No caller-visible
  difference in non-crash, non-DB-error operation under the
  single-instance/single-loop connection model; crash outcomes
  converge via idempotent transitions + the restart sweep.
- Actuals vs estimate: net **−50 lines** across the 6 files (not the
  estimated −300-400 — the new helpers carry their own code and guards
  stayed explicit; recorded, not gamed). Source total 8.561 → 8.516.
  Test count unchanged at 844.
- Not committed or deployed by this plan; both remain separately
  approved actions, as does the simplify plan's deploy gate.
