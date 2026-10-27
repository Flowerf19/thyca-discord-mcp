---
status: in-progress
created: 2026-10-27
last_updated: 2026-10-27
---

# Reconcile local SQLite/admin implementation with origin/main

## Status and authority

The owner requested commit, push, then restart. Local commit `817c59c`
succeeded; push was rejected as non-fast-forward. After inspecting the
remote divergence, the owner selected review and then **plan reconciliation**.
The owner approved the proposed reconciliation scope with "chốt" on
2026-10-27. R0-R2 implementation and verification are authorized. Per R3,
publication and restart require approval of the reviewed candidate. The
restart remains pending.

## Verified baseline

- Local `main`: `817c59c` (six local-only commits).
- Fetched `origin/main`: `092c3b8` (four remote-only commits).
- Common ancestor: `eaee955b7853e1c6f7db29b767e7c4bb98eaa148`.
- Remote substantive changes: `0ed5dba` (neutral MCP transport),
  `f507ef3` (remove auto-reply tick and conversation experiment), plus
  merge commits `b911c8e` and `092c3b8`.
- Local suite: **910 passed**, Ruff lint/format clean, diff check clean.
  Previous isolated built-wheel import and temporary SQLite smoke passed.
- Tracked worktree is clean. Existing untracked backups, log and PDFs are
  outside this task and must remain untouched.
- Read-only source/diff audits: `a4fbbfcd` (API/security), `9f31e001`
  (runtime/storage/tests). Both reject a naive merge because the endpoint
  architectures differ. No merged tree has been built or tested.
- Read-only legacy merge-tree preview confirmed overlapping textual and
  delete/modify conflicts. It is not a rename-aware execution result and
  does not establish that a merge can run correctly.

## Required behavior to preserve

The owner's selected planning scope preserves the local SQLite, auto-reply
and 31-tool implementation:

1. A single SQLite state DB and one connection/transaction discipline;
   SQL remains separated into `store/schema.py` and `store/queries.py`.
2. Discord capture -> durable outbox/job -> protected worker -> Jev
   participation -> target bot's live Thyca DM -> correlated tool reply.
   Preserve source freshness, post-await revalidation, reply threading,
   multi-chunk evidence, and safe-prefix ACK.
3. Best-effort semantics: ambiguous POST/send fails without blind retry;
   restart fails `submitted` jobs, queued work resumes. Preserve the
   accepted Jev retry and cost-accounting rules.
4. All 31 tools, signatures, payloads, raised errors and descriptions;
   owner policy stays agent guidance, not a claim of MCP authentication.
5. The 17 guild-scoped admin tools remain available. Discord permissions
   and hierarchy decide whether an operation is permitted. No new caller
   ACL, confirmation flow, or guild/channel expansion is introduced.
6. `allowed_mentions` remains `everyone=False`, `roles=False`,
   `users=True`, `replied_user=True` (the accepted local policy).
7. Metadata-only correlated audit allowlist: no raw `job_ref`, message
   content, reason text, or fingerprint hashes.
8. Existing package layout (`context/`, `jev/`, `thyca/`, `store/`,
   `responder/`, `server/`) and caller imports stay intact.

## Proposed remote disposition — requires approval with this plan

This is selective reconciliation, not adoption of both runtime designs.
Remote neutral-only startup is superseded by the local required worker;
its feature additions below are deferred, not claimed as implemented.

| Remote change | Proposed resolution | Evidence / rationale |
|---|---|---|
| Neutral lifespan: worker/recovery not started | Keep local lifecycle, fail-closed DB/Responder startup and protected task registration | Local `server/core.py:217-277`; remote lifespan starts heartbeat only |
| Delete `make_auto_reply_tick` as unused | Keep local factory and native Loop error wiring | Local core imports, constructs and starts it; it is not dead code here |
| JSONL/JSON outbox service and pure cursor ACK | Keep `Store` and `Responder.guard_ack` | Remote services import deleted `outbox`; dropping the guard can hide unread work from local worker |
| `commands.Bot.add_listener` setup | Keep `discord.Client` with `bot.event(on_message)` and start/ready race | Local bare Client has no add_listener interface |
| Remote `send_message` plain reply argument; reply-ping policy | Defer extension; keep current signature and mention policy | A plain reply reference does not record correlated send evidence |
| `record_no_reply` declared-only / trace completion | Keep local terminal `done/no_reply` at tool time | Old trace/journal machinery was deliberately removed |
| Deprecate correlated tools / restore resolve_uncertain_drop | Do not adopt deprecation or restore removed tool | Contradicts active auto-reply and best-effort design |
| `read_messages`, `get_message`, Markdown fields and error envelopes | Defer to a separate API decision below | Not necessary to reconcile history; changes accepted tools/schemas and agent parsing |
| New adapters/policy/services/tools packages | Do not ship unused legacy-dependent layers in the proposed tree | Their old persistence/config/lifecycle assumptions do not match local packages; reusable behavior can be ported when its API is approved |
| `sessions.py` and monolithic `thyca.py` deprecation-only modifications | Keep local deletions and `thyca/` package | Remote changes to these files are header-only; avoid resurrecting obsolete modules |
| Remote wheel/package configuration | Preserve local packages and current console entry | Do not restore delivery/outbox/thyca.py force-includes |
| Move cases.json to tests/fixtures | Adopt the byte-identical fixture relocation, updating every retained consumer | This cleanup does not require retiring the experiment |
| Delete experiment runner and its tests/parity checks | Keep runner, runner tests and parity checks | Local tests still load run_eval.py; deletion would discard current verification |
| Remote tests assuming gateway-only lifecycle or 17 tools | Do not substitute them for local contracts; retain compatible regression scenarios only | Tests describe different accepted products; no deletion/weakening of local coverage to make merging pass |
| Documentation/config deprecation wording | Reconcile against the resulting local behavior | No stale JSON paths, worker deprecation claims, or universal envelope claims |

Remote source is not labelled defective merely because it implements another
architecture. Omitting its optional facade from the proposed endpoint is an
explicit scope decision that must be accepted before a merge is committed.

## Candidate API work — not part of the proposed merge

These are useful but need a separate accepted contract:

- Exact-message read (`get_message`): genuinely absent as a public local
  tool; potentially raises the tool count from 31 to 32.
- History pagination: preferably extend existing `read_history` rather
  than add a redundant history tool; preserve its newest-first ordering,
  default/limit behavior unless explicitly changed.
- Plain threaded sends: optional `reply_to_message_id`, with an explicit
  rule preventing combination with correlated `source_message_id/job_ref`.
- Markdown rendering: optional presentation, not authentication, redaction,
  or prompt-injection isolation. Do not alter canonical/frozen Jev context.
- Error envelope unification: migration work, not an incidental merge fix.
  The remote only implements `{ok,data}` envelopes consistently on its new
  read tools; its existing send/outbox tools retain raw payloads/errors.

Do not silently add these APIs, retire benchmarks, or change ping policy
while resolving conflicts. If the owner wants any of these in this merge,
settle the exact scope/contracts and revise this draft before execution.

## Execution plan (only after approval)

### R0 — Revalidate and isolate

- Re-fetch/read refs; if origin/main or local HEAD moved, review the new
  delta before continuing. The hashes above are the proposed baseline.
- Preserve untracked files. Create an integration branch/worktree outside
  the live project directory so live imports cannot observe half-resolved
  source. No .env/live DB copy, live network actions, dependency upgrade,
  reset, force-push, or service restart in this phase.
- Capture the current 31-tool fixture and prompt digests and the local
  910 collected node IDs. Record both parent commit hashes.

### R1 — Merge history, resolve against the explicit behavior matrix

- Start a two-parent merge of origin/main into the integration branch
  based on local `817c59c`. Do not use an ours/theirs whole-tree strategy
  or automatically select one side of whole files.
- Resolve runtime, storage, config, package and documentation conflicts
  using the matrix above. Keep no unused remote source that imports
  deleted outbox/delivery modules or depends on removed settings.
- Retain the experiment and all its existing tests. Relocate the unchanged
  fixture to `tests/fixtures/jev_conversation_cases.json` and update:
  - `tests/test_conversation.py` fixture path (retain runner loader/parity).
  - `tests/test_jev.py` fixture path (retain runner loader/parity).
  - `experiments/jev_conversation/run_eval.py:default_cases_path()`.
  - All additional fixture references discovered by a repository search,
    including the runner README if it names the old default location.
- Reconcile tests by accepted behavior, not by whichever side is smaller.
  List incompatible remote expectations that are not adopted, linked to
  the approved disposition. Preserve every local test node; additions are
  allowed, weakening or silently dropping checks is not.

### R2 — Verify offline, then independent review

Required gates on the combined candidate:

- `uv run pytest -q`: preserve all 910 local baseline test nodes; all
  retained/adapted/new tests must pass. Count alone is not sufficient.
- `uv run pytest tests/test_golden_contracts.py -q`: 31-tool JSON and both
  prompt fixtures byte-identical; do not regenerate them to mask drift.
- Targeted regressions: auto-reply/recovery registration and fail-closed
  errors, source-deletion/partial-send races, no-reply terminal outcome,
  safe-prefix ACK refusal, scoped admin APIs and strict inputs, audit
  privacy, SQLite migration/restart/atomic poll/one-retry Jev behavior.
- Fixture relocation/parity and runner tests remain green. No paid Jev
  calls or experiment live evaluation.
- `uv run ruff check src tests`,
  `uv run ruff format --check src tests`, `git diff --check`, `uv build`.
- Inspect wheel: all current packages; no resurrected store.py/thyca.py,
  legacy delivery modules or unresolved imports. Repeat isolated wheel
  import/entry-resolution and temporary-DB transition/reopen smoke.
- Fresh-context independent review of the actual candidate against BOTH
  parents and this accepted matrix (security/data-safety requirement).
  Lead verifies findings and final diff/check evidence before acceptance.

### R3 — Publish, then restart under explicit operational approval

This phase needs explicit approval after the reviewed candidate is ready;
the current request is planning only.

- Commit the reviewed integration with both histories preserved. Verify
  origin/main is an ancestor of the final candidate; if remote changed,
  stop and re-evaluate rather than force-push.
- Fast-forward local main to the approved integration result and perform
  an ordinary push. Do not overwrite remote history. If push is rejected,
  stop before restart and report the new divergence.
- Before restart, inspect current live jobs/readiness to avoid interrupting
  work. Stop/start through Thyca's supported CLI, not by launching a second
  standalone stdio MCP. Preserve state.db and configuration; no reseed or
  state reset is needed for this source reconciliation.
- Check only fresh post-start logs for bot ready and tool registration;
  inspect startup/adoption/job errors. Do not claim auto-reply success from
  historical job rows or old log lines. Let the owner send a test message;
  ask separately before test actions that incur paid provider usage or
  mutate Discord server objects.
- On failure, preserve logs/state and report. A rollback that changes
  live code/state requires its own decision; never silently restore old
  JSON files over current SQLite activity.

## Acceptance / remaining decisions

The owner approved the following scope on 2026-10-27:

1. Keep local architecture as the resulting product; do not adopt remote's
   worker retirement or legacy storage/ACK behavior.
2. Adopt only compatible fixture relocation from the cleanup; retain the
   experiment and all current tests.
3. Defer remote new APIs/Markdown/envelopes/plain replies; preserve exactly
   31 tools in this merge. Any chosen addition needs a revised contract.

No runtime code, index, branch history or live service was changed while
preparing this plan. Execution will use an isolated integration worktree;
no live-service actions are authorized in R0-R2.

## Verified R0-R2 outcome (2026-10-27)

- Isolated worktree: `/tmp/thyca-reconcile-mHaVFznl`, branch
  `integrate/sqlite-admin-main`. Local HEAD remains `817c59c`;
  open MERGE_HEAD is `092c3b84d13fe5d019c94dc3559dc797a59b68f3`.
  No unmerged index entries; no merge commit created.
- Candidate deliberately retains all local runtime/config/package code.
  Only the unchanged fixture relocation, three consumer-path changes,
  three new relocation tests, and current README test count differ from
  local. Remote APIs and rejected worker/storage alternatives were not
  introduced. Original runner/parity tests remain intact.
- Lead reran full pytest: **913 passed**. Exact collected-node comparison
  retains all **910** local baseline nodes and adds only the three fixture
  regressions. Relocated fixture bytes match both parents. All three
  golden fixtures are byte-identical to local `817c59c`.
- Ruff lint/format and diff checks passed; lead built sdist and wheel.
  An isolated wheel-import smoke verified actual import paths and no
  rejected modules. Temporary DB smoke passed adoption, append, enqueue,
  submit, send evidence, reopen, terminal completion, ACK and mode 0600.
  No live DB, Discord mutation or paid provider call was used.
- Independent review `8f4566be`: Approve with fixes, only a Minor stale
  README test-count finding. Lead corrected current count 910 -> 913;
  historical counts were left intact. No runtime/security/data-safety
  findings remain.
- R0-R2 are complete. R3 (merge commit, publication, live main update,
  restart) awaits explicit owner approval of this reviewed candidate.
