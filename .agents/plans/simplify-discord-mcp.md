---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

# PLAN simplify thyca-discord-mcp: use discord.py, one SQLite store, best-effort delivery

> Plan from the 2026-10-27 codebase audit; owner approved implementation
> execution on 2026-10-27 (see §7). Deploying,
> restarting the live service, resetting/archiving live state, committing, or
> merging each need their own approval (see Deploy gate).

## 0. Base, evidence, permissions

- Base: `main@a309191`, Python 3.14.6, discord.py 2.7.1, mcp 1.x.
- Baseline verified 2026-10-27: `pytest -q` **1266 passed**; `ruff check src
  tests` clean. Source 12.823 lines / tests 27.921 lines.
- Only 2.637 source lines mention discord.py at all; the actual SDK surface is
  `channel.history` x4, `fetch_message` x3, `channel.send` x2,
  `fetch_channel/fetch_member/fetch_user/fetch_members` x1 each.
- Untracked files already present (`.env.bak-*`, `*_state.json.bak-*`,
  `service.resume.log`) are out of scope: do not touch, stage, or delete them.
- Audit findings behind this plan: `commands.Bot` with zero commands; typed
  `discord.Message` objects re-validated via `getattr`/`Any` everywhere
  (including parsing `jump_url` back into IDs); cache bypassed then
  reconstructed with 3 copies of member/user fallback cascades;
  `discord.ext.tasks.Loop` wrapped in a parallel registry; `Client.start()`
  hand-split; no `allowed_mentions` on any send; 4 hand-rolled durable JSON
  stores with 3 copies of fsync/atomic-save; `_is_strict_positive_int`
  defined 5 times; ~2.700-line delivery journal while README already states
  "No exactly-once guarantee is claimed"; docstrings 1.993 lines (16%).

## 1. Goal and accepted decisions

**Goal:** keep every user-visible feature, remove code that re-implements
discord.py/stdlib/pydantic, and make the remaining code typed and boring.

Accepted by the owner (2026-10-27 questionnaire):

1. **Scope: keep features, cut code.** **15** MCP tools' behavior (Phase 0
   golden proved 15, not the 14 originally written here), always-on
   auto-reply into the target bot's fixed Thyca DM session (incl. DM
   adoption and per-day session-history bookkeeping — there is no separate
   rotation gate in current code), Jev 3-question participation routing with
   decided-cache reuse and reported-cost accounting, guild/channel scope
   guard, identity policy (self/owner/other by metadata UID), and the
   `audit.jsonl` log all stay. (One tool is proposed for removal in Phase 3
   because its job ceases to exist; that is an explicit gate, not assumed.)
2. **Delivery: best-effort.** No intent/receipt journal, no uncertain holds.
   A crash exactly between a Discord send / Thyca POST and its state update
   may rarely duplicate or drop one reply in the owner's private channel.
   Never blindly re-POST or re-send after an ambiguous failure: ambiguous =>
   failed, logged, move on.
3. **Storage: SQLite (stdlib `sqlite3`).** One database replaces
   `outbox.jsonl` + `outbox_state.json` + `delivery_state.json` +
   `jev_state.json`. `audit.jsonl` stays an append-only text log.
4. **Live state: clean reset, separately approved.** No importer for old
   JSON state. At the deploy gate the old files are archived and the new DB
   is seeded past current history so nothing replays. Writing this plan (or
   approving implementation) does **not** approve the reset or a restart.

### Non-goals

- No new tools, no v2/v3 admin/destructive tools, no new dependencies.
- No change to Jev questions, model pin, routing outcomes, or cost reporting
  semantics; no new thresholds or heuristics.
- No change to Thyca native API usage beyond deleting now-unneeded reads
  (Phase 0 must prove which ones are unneeded).
- No live deploy, restart, paid Jev call, or Discord write during
  implementation phases; everything is offline until the deploy gate.

### Success measures (report, do not game)

- Source drops from 12.823 to roughly **<= 7.000 lines**, no module > ~900
  lines, with the per-phase before/after table recorded in this plan.
- Tool list/schemas and rendered Thyca prompts are byte-identical to the
  Phase 0 golden fixtures, except changes this plan explicitly lists
  (Phase 1 `allowed_mentions`, Phase 3 tool removal if approved).
- Full offline checks per phase: `uv run pytest`, `uv run ruff check src
  tests`, `uv run ruff format --check src tests`, `git diff --check`.
  Expected: all green; deleted tests are listed per phase with the behavior
  they covered either retained, replaced, or intentionally dropped.
- Validation stays strict **at untrusted boundaries only**: MCP tool inputs,
  Thyca HTTP responses, Jev SDK responses, env config, and DB reads of
  rows written by older code within the same DB version. Objects returned
  by discord.py and rows/objects produced by our own typed code are trusted.

## 2. Target shape (for orientation, not a file-by-file mandate)

- `bot.py`: `discord.Client` + intents; no `commands.Bot`, no command prefix.
- `store.py` (new): the only state module. Tables (corrected by fact
  check B, §6): `outbox` (PK `seq`; `UNIQUE(channel_id, message_id)` +
  `INSERT OR IGNORE` ends realtime/poll duplication; first write wins on
  edit conflicts — document, never `ON CONFLICT` update a job-linked row),
  singleton `state(id=1 CHECK, acked, seq_high_water, native_sid)` —
  `seq` is allocated from `seq_high_water` inside the append transaction
  (bare rowid can be reused after deleting the max row, and deploy seeding
  needs a high-water that survives an empty table),
  `poll_cursors(channel_id PK, last_message_id)`,
  `jobs` (PK `(channel_id, message_id)`, `seq INTEGER UNIQUE NOT NULL
  REFERENCES outbox(seq)`, `job_ref` partial-unique where not null,
  `stage`, `outcome`, `skip_reason`, `fail_kind`/`http_status`,
  `native_sid` set at submit time, timestamps; index `(stage, seq)`),
  `sends(channel_id, message_id, sent_message_id, created_at, PK(all
  three))` FK->jobs — a job legitimately has several correlated sends
  (multi-chunk replies); there is no single `sent_message_id` column,
  `jev_decisions` (PK **`(channel_id, message_id)`** FK->jobs — keying by
  context hash too would allow a second paid call when context changes;
  `context_sha256` is a checked column; statuses stay
  `pending|decided|failed_known|failed_unknown` plus an `attempts`
  column implementing the §5.2 one-retry rule;
  `attempt_id`, `participation` as its own CHECK column, `author_uid`,
  `attempted_day`, `input_tokens`/`output_tokens`, nullable `answers_json`;
  indexes `(status)`, `(attempted_day)`),
  `sessions(day TEXT PK, native_sid, adopted_at)` provenance.
  Job stages: `queued|submitted|sent|done|skipped|failed` with
  `outcome CHECK('sent','no_reply','observed')` on `done` (Jev `observe`
  and Thyca no-reply are terminals that never pass through `submitted`).
  Every outbox row gets a job row at ingest (filtered ones inserted
  directly as `skipped`), so prefix SQL needs no `actor_kind` exceptions.
  Pragmas: `journal_mode=WAL`, **`synchronous=FULL`** (WAL's default
  NORMAL can lose the last commit on power loss — unacceptable for the
  Jev pending marker), `foreign_keys=ON`, `busy_timeout=5000`;
  `user_version=1`; `isolation_level=None` + explicit `BEGIN IMMEDIATE`;
  never hold a transaction across an `await` (Jev paid call, Thyca POST,
  Discord send sit between transactions by design). Startup runs
  `PRAGMA quick_check` + `user_version` gate and fails loudly — never
  auto-reset. DB file created `0600` like today's state files. One
  connection used from the single asyncio loop (this service serves one
  channel; sqlite calls are sub-millisecond here — state this assumption
  in the module docstring instead of building an async DB layer).
  `build_message_entry` (pure capture filter) is rehomed with capture,
  not into the store; `sessions.py` SID/day validators are Thyca-boundary
  validation — rehome to `thyca.py`, delete only the bulk history
  validators (row-level checks at `adopt_session` replace them; the
  retired-SID concept dies with per-record SID allowlist validation).
- `delivery/`, `delivery_schema.py`, `delivery_storage.py`,
  `participation/store.py`, `participation/validate.py`, `outbox.py`,
  `sessions.py`: deleted; callers use `store.py` + `participation/ledger.py`
  trimmed to Jev decision logic over the store.
- No `uncertain` stage, no intent/receipt journal (stages/outcomes in
  the `store.py` bullet above).
- `server/core.py`: one shared helper for the correlated-tool preamble
  (scope check + `correlated_job` + audit with fixed safe fields). Audit
  never logs content or `job_ref` at all — this preserves the current
  privacy property without the two-phase hash/raw argument dance.

## 3. Phases (execute by dependency; each phase is a separately reviewable diff)

Every phase: update its tests **in the same phase**, run the full offline
checks above, and get lead review of the actual diff before the next phase
starts. Security/data-safety scope => a fresh independent read-only review
is required before the work is called merge-ready (per project rules).

### Phase 0 — Baseline fixtures and two fact checks

Files: `tests/fixtures/` (new), no `src/` changes.

- Capture `list_tools` output (names + input schemas + descriptions) from
  `create_server(Settings(...))` as a golden JSON fixture with a test that
  diffs against it.
- Capture golden rendered prompts for `respond` and `need_context` from
  `thyca.build_conversation_prompt` on a fixed conversation fixture
  (reuse existing prompt tests' data where possible).
- Fact check A (gates Phase 3 trace deletion): prove from `thyca.py` +
  native behavior recorded in tests whether `submit_turn` returning
  `completed` means the agent turn is finished (so "turn done, no send and
  no `record_no_reply` call" can be closed as `done` by the worker). If not
  provable, Phase 3 keeps the cheap `get_session_snapshot` poll and deletes
  only the trace-evidence parser (`build_trace_evidence`,
  `get_trace_evidence`, `responder/recovery.py`).
- Fact check B (gates Phase 2): confirm sqlite3 usage conventions in this
  repo (none today) and pick connection handling; confirm WAL is acceptable
  on the deployment filesystem (local disk, yes per current JSON files).
- Gate: fixtures committed in the diff; fact-check results written into
  this plan (edit §6) before Phase 3/2 respectively.

### Phase 1 — Use discord.py idiomatically (no state or behavior change yet)

Files: `bot.py`, `config.py`, `source_messages.py`, `context.py`,
`tasks.py`, `server_ticks.py`, `server/core.py` (lifespan, send,
`read_context`), `responder/core.py` (display-name/bot-flag resolution).

- `discord.Client` instead of `commands.Bot`; delete `command_prefix`
  setting and `.env.example` entry.
- Startup **keeps a race**, on `bot.start()` instead of split
  login/connect: create `start` and `wait_until_ready` as two tasks and
  `asyncio.wait(FIRST_COMPLETED)` (bare `await bot.wait_until_ready()`
  raises `RuntimeError` before `login()` creates `_ready`, verified in
  `discord/client.py`; and a pre-ready gateway failure must surface, not
  hang). `discord.Client` has **no `add_listener`** (verified
  `Client.add_listener == False`): register realtime capture via
  `bot.event` with a handler literally named `on_message`; drop the
  `remove_listener` teardown step. Keep the cleanup-error retention
  (`thyca_cleanup_errors`) or record its removal as a behavior change at
  the gate — do not silently swallow `responder.close()` failures.
  Note `!help` (default help command) disappears with `commands.Bot`;
  acknowledge at the gate.
- **Deliberate behavior fix:** every `send` passes
  `allowed_mentions=discord.AllowedMentions(everyone=False, roles=False,
  users=True, replied_user=True)` so agent text cannot mass-ping. Owner
  confirms the `users/replied_user` choice at this phase's gate (§5).
- Type the Discord boundary: signatures take `discord.TextChannel` /
  `discord.Message`; delete `getattr`-based re-validation of SDK objects in
  `context.py` (`_message_scope`, `_message_timestamp`,
  `_reference_parent`), `read_context`'s per-message loop, and the send
  receipt check incl. `_jump_url_ids_match` (`server_helpers.py`).
  Keep scope **decisions** (wrong guild/channel => refuse) reading typed
  attributes directly.
- Reply path: send via the existing `_discord_send` chokepoint with
  `discord.MessageReference.from_message(partial, fail_if_not_exists=True)`.
  Do **not** use bare `PartialMessage.reply()`: its reference dict omits
  `fail_if_not_exists`, and the server-side default when omitted is not
  verifiable offline (would change deleted-source behavior in Phase 1).
  Parent resolution in `context.py`: use `message.reference.resolved`
  (treat `DeletedReferencedMessage` as missing parent) **with the
  `fetch_message` fallback when `resolved is None`** — presence of
  `referenced_message` in every REST payload is not guaranteed by the lib;
  check cross-channel via `reference.channel_id` before trusting a
  resolved parent.
- One strict-deletion helper (404 + code 10008) in `source_messages.py`;
  `context.py` imports it instead of its private copy.
- Cache instead of cascades, with two corrections from the Phase 1 map:
  (a) context display names must stay **sync cache-only** (the async
  `resolve_display_name` performs REST fetches and would change snapshot
  names -> `context_sha256` -> Jev cache misses): extract a sync
  cache-only helper into `bot.py`, keep the async fetch version for
  `whoami` only, and assert the golden prompts stay byte-identical.
  (b) `responder._resolve_bot_flag` resolves a bare UID for legacy outbox
  frames (no `Message` exists there): rewrite typed, keep the
  cascade decisions and fail-closed `None`; delete only the `getattr`
  probing. Chunking: writer must explicitly choose flip-default vs drop
  the setting (SDK default = `intents.members`); note
  `intent_members=False` + chunking raises at Client construction.
  `list_members`: read `guild.members` only behind a `guild.chunked`
  check, else `fetch_members`; add the first real payload tests for
  `whoami`/`guild_info`/`list_members`/`list_text_channels` (currently
  zero behavioral coverage) and `allowed_mentions` kwarg assertions on
  manual + correlated sends.
- `tasks.py`: registry holds only `name -> tasks.Loop` (+ protected set);
  interval reads `loop.seconds` (drop `_intervals`). Auto-reply tick moves
  to a `@loop.error` handler **plus `loop.clear_exception_types()`** —
  without it, discord.py retries `OSError`/`GatewayNotFound`/aiohttp
  errors with backoff and never calls the handler, breaking fail-closed
  stopping. `TaskRegistry.stop`'s idle-failed exception consumption
  **stays** (a failed Loop task still holds its exception). Add a test: an
  `OSError` tick failure still stops the loop and audits.
- Tests: for the touched paths, build real `discord.Message` objects from
  gateway payload dicts (small test helper) instead of `SimpleNamespace`
  fakes; keep fakes only at Thyca/Jev HTTP boundaries.
- Gate: golden `list_tools`/prompt fixtures unchanged; full checks green.

### Phase 2a — Outbox onto SQLite

Files: new `store.py` (schema/meta/outbox only), `server/core.py`
(realtime capture, `read_outbox`, `ack_outbox`), `server_ticks.py` (poll),
`server_helpers.py` (drop `_check_outbox_consistent`), `config.py`
(`db_path` replaces `outbox_path`/`outbox_state_path`), delete `outbox.py`.

- `append_entry` equivalent is one `INSERT OR IGNORE`; `seq` is the rowid;
  ack and poll cursors are `meta` rows updated in the same transaction as
  the work they record. The whole "state file vs log" consistency class
  (stale seq, gaps, missing-state guards) disappears with the two-file
  design.
- `read_outbox` output shape `{entries, acked, latest}` unchanged; entries
  are now actually unique per message (intentional improvement; update
  `docs/TOOLS.md` dedupe wording).
- Gate: new store tests cover insert dedupe (same message via realtime +
  poll => one row), ack monotonicity, ack-beyond-latest rejection, poll
  cursor resume across reopen, and corrupt-DB startup failing loudly.

### Phase 2b+3 (merged, lead decision 2026-10-27) — Jobs/Jev onto SQLite with best-effort semantics

Merging rationale (fact check B sequencing conflict): porting the intent/
receipt/uncertain journal onto SQLite in 2b only to delete it in Phase 3
would build the exact machinery this plan removes. Phase 2a stays a
separate small diff; jobs+Jev land directly in the Phase 3 stage/outcome
model below. §5.2 (pending rule) and §5.3 (`resolve_uncertain_drop`
removal) were owner-confirmed 2026-10-27 (see §5.2/§5.3).

#### Phase 2b part — storage move

Files: `store.py` (jobs/jev/sessions tables), `responder/*`,
`participation/ledger.py` (slim), delete `delivery/`,
`delivery_schema.py`, `delivery_storage.py`, `participation/store.py`,
`participation/validate.py`, `participation/errors.py`, `sessions.py`,
`config.py` (drop `delivery_state_path`/`jev_state_path` collision logic).

- Reimplement the delivery functions the responder actually calls
  (`enqueue`, `get_job`, stage updates, `processed_seq_prefix`-equivalent
  query) as `store.py` functions with the stage/outcome set in §2, per
  fact check B's function-by-function mapping (artifact in §6). Do **not**
  port schema validators for fields SQLite constraints + typed writers
  already guarantee; keep `CHECK` constraints on stage/outcome/reason
  enums. Write-only proof fields (submitted-text fingerprint,
  `turn_index` binding, intent/receipt hashes, `trace_refs`, duplicated
  Jev fields on the job) are not recreated. Transaction boundaries per
  fact check B §2: poll batch + cursor, prefix+ack, ingest, Jev pending
  insert / decided+routing commit, submit marker before POST, adoption
  as one transaction; the correlated send is verify -> Discord await ->
  conditional `record_send ... WHERE job_ref=? AND stage IN
  ('submitted','sent')` (the transaction guards the recording only;
  concurrent multi-chunk sends interleave at the await — accepted under
  best-effort, stated not hidden). Startup sweep `submitted->failed`
  lives in `Store.open()` before loops start.
- Jev paid-call protection stays, cheaply: `INSERT` a `pending` row before
  the paid call, `UPDATE` to `decided` with the validated decision/cost
  after. A `pending` row found on a later tick means the outcome — and
  whether money was spent — is unknown. **Proposed rule (§5, must confirm):
  treat as `observe`, never pay twice for the same
  `(channel, message, context_sha256)`.**
- Jev SDK response validation in `jev.py` largely **stays**: it is an
  untrusted paid boundary, unlike discord.py objects.
- Gate: tests for decided-cache reuse (no second provider call), pending
  rule, ledger/journal binding checks replaced by single-DB invariants,
  session adoption/rotation behavior unchanged (existing responder tests
  that assert behavior, not JSON shapes, should pass with store swaps).

#### Phase 3 part — best-effort semantics (same diff as 2b above)

Files: `store.py`, `responder/core.py`, `responder/submit.py`, delete
`responder/recovery.py`, `thyca.py`, `server/core.py`
(`_correlated_send`, `record_no_reply`, `resolve_uncertain_drop`).

- Correlated `send_message`: verify job + stage, `.reply(...)`, then one
  store update (`sent` + `sent_message_id`). No pre-flight `fetch_message`
  (the reply's own `fail_if_not_exists` semantics report a deleted source;
  map that to `skipped/source_deleted`), no intent/receipt rows, no
  jump-URL parsing, no `send_blocked` latch, no second correlation re-check
  after awaits (single loop + stage check in the same transaction as the
  update is the guard).
- `record_no_reply`: terminal `done` at tool time (no trace-proof wait).
- Worker completion: per Phase 0 fact check A. On restart, jobs left in
  `submitted` become `failed` (never resumed or re-POSTed); `queued` jobs
  resume normally. ACK advances over terminal jobs in seq order.
- Thyca timeout/ambiguous submit => `failed`, never re-POST; definitive
  busy (409) => back to `queued` (existing behavior).
- Trace-proof machinery deletion follows **fact check A verdict (§6):
  allowed only with the snapshot-poll fallback.** `submit_turn`'s
  `completed` is locally synthesized (can be `True` while `running` is
  `True`, ignores `stored_text_match`) and must never alone close a
  `submitted` job. Fallback design: after POST, if no send/no-reply was
  recorded, require `running is False` + `stored_text_match is True` +
  expected `turn_index`; while `running` is `True`, poll
  `get_session_snapshot` on later ticks until an idle instant is observed,
  then close via a stage-guarded transaction. A completed turn with no
  send and no `record_no_reply` closes as `failed` (reason
  `turn_no_outcome`), not `done` — both routed prompts obligate one of
  the two calls. Then delete `thyca.build_trace_evidence` (~450 lines),
  `get_trace_evidence`, `responder/recovery.py`, and the submitted-text
  fingerprint/turn-binding prerequisites.
- **Multi-chunk replies:** kept via the `sends` table (§2). Stage stays
  send-eligible (`submitted`/`sent`) until the worker closes the job at
  turn end; without intents the worker knows ">=1 send + turn ended",
  never "all chunks" — that is the accepted best-effort trade, stated
  in docs.
- **Tool removal (§5.3, owner-confirmed):** `resolve_uncertain_drop`
  has no purpose once `uncertain` and stuck-head states are gone
  (restart auto-fails `submitted`). Remove the tool (15 -> 14), its
  docs/prompt references, and deliberately regenerate the golden
  `list_tools` fixture in this phase (review the diff).
- Gate: failure-case tests — deleted source at reply time, send raises
  `Forbidden`/5xx, crash simulation by reopening the DB with a `submitted`
  job, `record_no_reply` terminality, ACK prefix with mixed
  done/skipped/failed.

### Phase 4 — Validation, config, and boilerplate diet

Files: `config.py`, `instance_lock.py`, `server/core.py`,
`server_helpers.py` (likely deleted), `context.py`, `conversation.py`,
`thyca.py`.

- One shared correlated-tool helper (scope + correlation + fixed-field
  audit + bounded error mapping) replacing the 4 near-copies; audit fields
  are a fixed allowlist (tool, channel_id, source_message_id, content_len,
  ok, error_kind).
- Exactly one definition of any remaining strict-int check, used only at
  MCP/HTTP boundaries; internal APIs trust their type annotations.
- `config.py` (corrected per config inventory, pydantic 2.13.5 verified):
  annotated strict types **cannot** replace the bool/float `mode="before"`
  guards without behavior change (lax `PositiveFloat` accepts `True->1.0`;
  `StrictFloat`/`StrictInt` reject env strings like `"60"`/`"12"`).
  Replace only the finite check with `allow_inf_nan=False`; keep both
  before-guards. Net saving is ~11 lines here — the real win is deleting
  the jev path-collision model validator in Phase 2b. Add tests for env
  string forms `"inf"`/`"nan"`. Decide at the Phase 2b gate whether any
  replacement guard is needed (e.g. `db_path` != `audit_path`) since the
  plan drops collision logic entirely.
- `instance_lock.py`: a small `flock` context manager (~50 lines). Keep the
  busy-error exit codes in `main.py`; drop `/proc` cmdline scraping (PID in
  the message is enough).
- Docstrings: module docs describe the current contract in <= ~15 lines;
  historical incident relitigation moves to this plan's history, not code.
- Gate: `rg` shows one `_is_strict` definition (or zero), one fsync/atomic
  implementation (audit append only), no `getattr(` on discord objects in
  `src/` outside HTTP-boundary modules.

### Phase 5 — Test suite and docs re-baseline

Files: `tests/*`, `README.md`, `docs/TOOLS.md`, `docs/CONTEXT.md`,
`.env.example`, `pyproject.toml`.

- Delete tests that assert deleted internals: `test_*_extraction.py`
  (file-layout assertions), fsync-fault/durability-uncertain suites,
  JSON schema validators, jump-URL/bool-spoof cases against SDK outputs.
  Keep/strengthen behavior tests: tool schema golden, correlation accept/
  reject, deleted/edited source, Jev cache + pending rule, prompt golden,
  Thyca HTTP error mapping, restart semantics, store dedupe/cursors.
- Every deleted test file is listed in the PR/plan update with where its
  behavioral coverage went. Report collected count before/after (1266
  today); a big drop is expected and must be explained, not padded back.
- Docs: replace journal/exactly-once language with the honest best-effort
  contract; state file list becomes `state.db` (+ WAL sidecars) and
  `audit.jsonl`; tool count updated if Phase 3 gate approved.
- `pyproject.toml`: hatch `force-include`/packages updated for deleted and
  new modules; verify with an offline wheel build + import smoke
  (`uv build` or existing project equivalent) — record the command output.

### Deploy gate — SEPARATE approval required (not part of implementation)

Only after Phases 0-5, independent review, and an explicit owner go-ahead:

1. Stop the service (its current supervisor/launch path per README).
2. Archive `delivery_state.json`, `jev_state.json`, `outbox.jsonl`,
   `outbox_state.json` to a timestamped directory (do not delete `.bak-*`
   files; they are not ours to clean up).
3. Seed `state.db`: `acked` at latest, poll cursor at the newest message
   ID in the approved channel (one `history(limit=1)` read), so the
   reset cannot replay old messages or re-pay Jev for them.
4. Start the new build; smoke test: owner message -> one threaded reply;
   other-user message follows Jev routing; restart once -> no duplicate
   reply for completed jobs; `list_tasks`/heartbeat healthy.
   (The smoke test may incur one paid Jev classification — included in the
   deploy approval ask.)
5. Rollback: stop, checkout `a309191`, restore archived state files,
   restart old build. DB file is simply left aside.

## 4. Interface changes summary

| Surface | Change | Phase |
|---|---|---|
| Env/config | `COMMAND_PREFIX`, `OUTBOX_PATH`, `OUTBOX_STATE_PATH`, `DELIVERY_STATE_PATH`, `JEV_STATE_PATH` removed; `DB_PATH` (default `<root>/state.db`) added | 1, 2 |
| State files | 4 JSON files -> `state.db` (+`-wal`/`-shm`); `audit.jsonl` unchanged format, fixed field allowlist | 2 |
| MCP tools | shapes unchanged except: deduped `read_outbox` entries; `resolve_uncertain_drop` removal **only if approved** | 2a, 3 |
| Discord sends | `allowed_mentions` blocks `@everyone`/role pings from agent text | 1 |
| Failure semantics | ambiguous send/POST => `failed` (possible rare duplicate/drop on crash); restart: `submitted -> failed`, `queued` resumes | 3 |
| Jev | same questions/cache/cost reporting; crashed `pending` => per confirmed rule in §5 | 2b |

\* Phase 1 keeps behavior identical apart from `allowed_mentions`; using
`.reply()` on a partial message still sends through the same channel with a
message reference — the send remains agent-owned via MCP, the worker still
never sends.

## 5. Decisions (owner-confirmed) and resolved questions

1. **Phase 1: DONE by lead choice** — `AllowedMentions(users=True,
   replied_user=True)` (replies still ping the replied-to user).
2. **Phase 2b+3: owner-confirmed 2026-10-27 — crashed Jev `pending` gets
   exactly ONE retry.** Semantics the writer must implement: a `pending`
   row carries `attempts` (starts at 1 when inserted before the paid
   call). On encountering a crashed/unknown `pending` for a job (at tick
   or restart): if `attempts < 2`, increment to 2, re-run the paid call
   once, and resolve to `decided` (or `failed_*` per the error outcome).
   If `attempts == 2` and the outcome is still unknown, resolve
   terminally as **observe** — never a third paid call for that
   `(channel_id, message_id)`. A `decided` row is reused from cache as
   today (no re-pay). This deliberately accepts possibly paying twice
   for one classification when the first attempt actually completed
   server-side; that cost risk is the owner's choice over losing the
   reply. (This changes §2's Jev status note: `attempts` column added.)
   Pinned by the Jev audit (2026-10-27, lead decisions): **D1** the
   same-key pending retry resolves in the existing-key branch BEFORE
   the cross-key `failed_unknown` block scan (otherwise the retry is
   dead code after any historical unknown); **D3** the exhausted
   (attempts==2) terminal-observe is STORED as `status='decided'`,
   `participation='observe'`, `answers_json=NULL`, keeping the retry
   attempt's usage/cost — never left as `failed_unknown` (that would
   fail the job AND permanently block all future paid calls via the
   global block); it is a synthetic owner-ordered observe, documented
   as such, not a model decision. **D2 (literal reading adopted):**
   retry eligibility covers unresolved `pending` rows ONLY — a live
   `failed_unknown` resolution (e.g. provider timeout) gets no retry;
   extending it needs separate owner sign-off (owner flagged 2026-10-27
   with the asymmetry: crash=>retry, live timeout=>no retry). **D4:**
   `attempted_day`/timestamp update at the retry bump (cost lands on
   the day actually paid; attempt 1 contributed no known cost). **D5:**
   the attempts==2 terminal-observe resolves regardless of fingerprint
   drift; the paid retry never does (mismatch-first before any
   payment, §2). Forced by HEAD design (not decisions): the bump +
   fresh `attempt_id` commit in ONE conditional update BEFORE the
   retry paid call; retry fires only inside classify at the FIFO head
   (never at startup/ingest); `failed_known` never retries; a crashed
   attempt's context is unrecoverable (metadata-only), so in a busy
   channel drift will often defer the paid retry — accepted.
3. **Phase 2b+3: owner-confirmed 2026-10-27 — remove
   `resolve_uncertain_drop`** (15 -> 14 tools), with its docs/prompt
   references and tests. Restart `submitted->failed` covers its use case.
   The golden `list_tools` fixture gets its one deliberate regeneration
   in this phase; review the diff.
4. **Fact check A: resolved** — `submit_turn` `completed` does not prove
   turn end; snapshot-poll fallback is in Phase 3.

Also owner-approved alongside: a format-only pass over the 4 pre-existing
dirty files (`participation/ledger.py`, `responder/submit.py`,
`tests/test_delivery.py`, `tests/test_responder.py`), verified with
`git diff -w` (zero token changes) — schedule with Phase 2a review.

## 6. Verification log (fill during execution)

- Baseline 2026-10-27: `pytest -q` 1266 passed (13.88s); `ruff check src
  tests` clean; base `a309191`.
- Phase 0 done (writer `e850fc75`, lead-verified): `tests/fixtures/
  list_tools.json` (**15 tools**), both prompt fixtures,
  `tests/test_golden_contracts.py` (3 tests). Lead reran: golden 3 passed,
  full suite **1269 passed**. Tool count in README/plan said 14 — wrong;
  fixture is the authority.
- Fact check A result (reviewer `0200e38c`): **keep the
  `get_session_snapshot` poll fallback**; `completed` alone never closes a
  job (see Phase 3 text). Trace parser deletable with that fallback.
- Phase 1 change map (reviewer `d9065f97`, lead spot-verified on installed
  discord.py): 2 Critical corrections (`Client.add_listener` does not
  exist; bare `await wait_until_ready()` raises before login) + Important
  corrections on display-name helper, `_resolve_bot_flag`, tasks
  exception types, `fail_if_not_exists`. Folded into Phase 1/3 text.
- Test categorization (reviewer `2a17f1da`): KEEP 10 files/~361 tests,
  REPLACE 13/~671, DELETE 6/~244 (+~170 delete-splits); expected end-state
  ~850-950 collected. Gaps recorded: multi-chunk decision (Phase 3),
  zero behavioral tests for 4 read tools + `allowed_mentions` (Phase 1
  must add), fixture seeding via `delivery.*` breaks at Phase 2b.
- Config/docs/packaging (reviewer `cb16d613`): full field/env inventory;
  pydantic correction folded into Phase 4; `extra="ignore"` trap: stale
  removed-field kwargs are silently ignored, so Phase 2 must grep-verify
  zero remaining removed-kwarg `Settings(...)` constructions (a missed
  fixture would write to the real default `state.db`); `main.py:17` lock
  derivation must move to `db_path` in Phase 2b; packaging diff and
  per-phase false-docs list are in the reviewer artifact (authoritative
  checklist for Phases 2-5 docs edits).
- Fact check B result (reviewer `33f455c8`, after a 30-min timeout +
  narrowed resume): **PASS with corrections**, all folded into §2/Phase
  2 text: Jev PK `(channel,message)`, `sends` table, stage/outcome split,
  4 Jev statuses + `attempted_day`, seq high-water + singleton state row,
  `synchronous=FULL`, per-function API mapping and transaction boundaries
  (artifact is the authoritative Phase 2 checklist). Lead decision:
  **Phases 2b+3 merged** (see Phase 2b+3 header). Deploy note: archiving
  `state.db` must checkpoint (`wal_checkpoint(TRUNCATE)`) or move
  `-wal`/`-shm` with it.
- Phase 1 done (writer `08aac926`, lead diff-reviewed): 16 files,
  +973/-890. Lead gates rerun: golden 3 passed (byte-identical), full
  **1272 passed** (1269 + 11 new - 8 deleted), ruff check clean,
  `git diff --check` clean. Lead-verified: `add_listener` removed for
  `bot.event(on_message)`, start/ready task race kept, `_ALLOWED_MENTIONS`
  on both send branches, `_jump_url_ids_match` deleted with typed scope
  equalities retained (None reference -> uncertain path), context typed
  reads + `resolved` parent with fetch fallback gated on `not cross`,
  sync cache-only `cached_display_name`, `Loop.error` +
  `clear_exception_types()`. Behavior changes acknowledged: `!help`
  gone, `allowed_mentions` guard, chunk default False->True (config tests
  pin the loud construction failure). Deleted tests (8, all
  malformed-SDK-spoof class, listed with reasons in the writer report):
  4 context spoof/builder cases, 2 source-deleted recheck cases, 2
  jump-URL receipt params. New coverage: real-`Message` helper
  (`tests/real_discord.py`), `resolved` parent reuse, mention-guard on
  manual+correlated sends, OSError tick failure stops loop, minimal
  payload tests for `whoami`/`guild_info`/`list_text_channels`/
  `list_members` (chunked+unchunked), chunking config. In-scope src
  3940->3873 lines (Phase 1 is mostly behavior-neutral typing; the big
  cuts land in 2a/2b+3). Pre-existing format drift: 6 files dirty at
  base, 4 remain out of scope (`ruff format --check src tests` is
  therefore not repo-clean; recorded honestly — a format-only pass over
  those 4 files needs its own approval, not conflated with this phase).
- Phase 1 independent review (fresh reviewer `6bae4d60`, Phase-1 hunks
  only): **Approve with fixes** — no Critical/Important; 4 Minors:
  (1) `list_members` chunked order is now ascending — comment claiming
  "REST ascending" is wrong about the SDK; acknowledge order as
  deliberate + fix comment; (2) OSError stop test replicates lifespan
  wiring by hand — add a lifespan-level wiring assertion; (3) chunked
  test fake expresses an SDK-impossible state (`_chunked=True` with
  member_count != len(members)) — derive the fake flag; (4) stale
  `_make_auto_reply_tick` annotation `tuple[TickFn, dict[str, Any]]` ->
  `tuple[TickFn, ErrorFn]`. Also: §5.1 allowed-mentions was a lead
  choice, owner-visible in reports (recorded); startup exception type
  changed (LoginFailure wrapped in RuntimeError, cause preserved) —
  acknowledged. These 4 fixes are batched with two 2a residuals below
  before Phase 2b+3 starts.
- Phase 2a done (writer `554fce51`, lead-verified incl. full read of
  `store.py`): `src/store.py` new (459 lines; schema v1 outbox + state
  high-water + poll_cursors; WAL/FULL/0600/quick_check/user_version
  fail-loud); `src/outbox.py` deleted; `db_path` replaces
  outbox/outbox_state paths; poll batch+cursor one txn; responder reads/
  acks via store. Lead gates rerun: **1215 passed** (1272 - 57 net:
  -86 removed/+29 added, itemized by writer), golden 3 passed
  (fixtures untouched — `read_outbox` description kept verbatim),
  ruff check clean, diff-check clean; writer's grep verification
  reproduced by lead (0 stale kwargs/imports). pyproject force-include
  swapped outbox->store (writer `uv build` OK). Residuals for the fix
  batch: (a) `store.read_entries` bool-limit check is dead code after
  `int(limit)` (bool silently becomes 1, matching old behavior — fix
  the check order or drop the claim); (b) 3 inline `from store import
  Store` inside responder methods (no import cycle exists; server files
  import top-level) — hoist; (c) realtime capture opens a Store per
  message (open-use-close + quick_check each) — accepted at this scale,
  revisit only if volume grows; (d) `-wal` sidecar not chmod 0600 like
  the db file (parent dir is 0700) — handle at deploy/Phase 5.
- Phase 2b+3 done (writer `1d515818` + resumed `06f38c97` after a
  30-min timeout; lead-steered mid-flight with the Jev audit decisions
  D1/D3/D4/D5 and checklist resolutions 3.1-3.8; all reconciled).
  Lead gates rerun: **841 passed** (1217 - 376 net; deletions are the
  authorized obsolete suites — delivery/observation/trace/extraction +
  journal/latch/nonce cases — pending independent test-integrity
  review), golden 3 passed with `list_tools` at 14 tools
  (`resolve_uncertain_drop` removed per §5.3; prompt fixtures
  byte-identical), ruff check clean, `git diff --check` clean,
  `uv build` wheel has no delivery payload. Deleted: `src/delivery/`,
  `delivery_schema.py`, `delivery_storage.py`, `participation/store.py`,
  `participation/validate.py`, `sessions.py`, `responder/recovery.py`,
  Thyca trace machinery; `test_responder.py` rewritten 3088->1373
  lines over real Store+ledger. Source total now **8.687 lines** (from
  12.823; store.py 1.480 replaces ~3.800 lines of outbox+delivery+
  participation-store code). Independent correctness + test-integrity
  reviews of the 2b+3 diff are running; findings gate Phase 4.
- Phase 2b+3 independent reviews: correctness (`4bc021f8`) Request
  changes — all §1 checklist items, §3 resolutions, §5.2/§5.3 verified
  preserved EXCEPT: **F1 [Important, blocks Phase 4]** `_reconcile_after_post`
  closes a `sent` job at POST return even when the turn is still
  `running` (fact-check-A violation; truncates multi-chunk; writer's
  test `test_agent_send_during_running_post_closes_sent` enshrines it
  and must change with recorded justification); **F2 [Important]** raw
  `job_ref` still in audit args — formally carried to Phase 4's
  allowlist work (blocks merge-ready, not Phase 4); **F3 [Minor]**
  `jev_unknown_blocked(exclude_channel=...)` excludes a whole channel,
  not the caller's key (latent, no production caller) — delete the
  parameter. Lead decision on the reviewer's open question: an idle
  snapshot whose turn index exceeds the expected one (manual turn
  interleaved) closes as `failed(turn_no_outcome)`, never infinite
  pending (consistent with fact-check A: mismatch => failed, never
  re-POST; pending-forever bricks the FIFO head).
  Test-integrity (`1eb8c539`) **Approve with fixes** — 1217->841 fully
  explained by the authorized DELETE list; D1/D3/D4/D5, migration,
  sweep-once genuinely asserted; 6 Minors (test-only): M1 missing
  deleted-skip-wins-over-mismatch test; M2 snapshot-close criteria
  parametrized cases; M3 two stale `delivery_state_path=` kwargs
  contradicting the grep-zero claim (no real write occurs — fixtures
  pin db_path — but the claim must be made true); M4 three test names
  overclaim their assertions; M5 = F2 deferral (recorded); M6 dead
  `trace_fn` block + stale headers. Fix round (F1/F3/M1/M2/M3/M4/M6 +
  turn-index decision) goes back to the same writer before Phase 4.
- Phase 2b+3 fix round done (writer `8a3fe6a1`, lead gates rerun:
  **848 passed**): F1 fixed (running POST return => pending; pinned by
  restoring the old order and watching the new test fail), turn-index
  decision implemented (idle+index-mismatch => failed, +
  `test_poll_interleaved_turn_index_closes_failed`), F3 parameter
  deleted, M1/M2/M3/M4/M6 test corrections landed (+7 tests). F2
  (audit allowlist) formally moves to Phase 4. **Phase 2b+3 closed.**
- Phase 4 done (writer `aad51f4c`, lead gates rerun: **849 passed**,
  golden untouched): correlated-tool helper shared by all 3 tools with
  identical decisions/error strings; **F2 discharged** (audit args are
  the fixed allowlist; raw `job_ref` in zero audit constructions,
  rejected inputs keep len/sha only); strict-int consolidated to one
  home (`src/validate.py`, 2 functions; writer verified every remaining
  check is fed untrusted values by boundary tests — none deleted);
  config finite check via `allow_inf_nan=False` (+env inf/nan tests);
  `instance_lock.py` 190->141 (writer honestly missed the ~50-70
  estimate: test-pinned API/messages floor it there); docstring diet on
  touched modules. Source 8.687 -> **8.586**. Note vs §1 target: the
  <=7.000-line aspiration will NOT be met — remaining bulk is store.py
  (1.470, consolidated state), Thyca/Jev boundary validation, and the
  responder state machine, all load-bearing; the structural goals (one
  store, no journal, typed Discord boundary, one validation home) are
  met. Line count was a report metric, not a gate; recorded honestly
  rather than gaming it by deleting boundary validation. Independent
  Phase 4 review running; findings gate Phase 5.
- Phase 4 independent review (`89f196c5`): Request changes — one
  Important: hash fingerprints (`job_ref_sha256`, `reason_sha256`,
  manual `content_sha256`) still reached audit args, so F2 was not
  closed literally (unsalted SHA-256 of low-entropy values is
  dictionary-confirmable). Fix round resumed to the Phase 4 writer:
  drop all hash fields (keep `*_invalid` flags), restore `ServiceLock.__del__`,
  route the last 3 exact-type checks through `validate.py`. Minors
  otherwise: multi-fault precedence unification **accepted explicitly
  by the lead** (single-fault strings/decisions identical); IntEnum
  widening from the strict-int consolidation accepted (all real inputs
  produce plain ints); helper equivalence, config, packaging verified
  clean. Phase 5 waits for this fix round + lead verification.
- Phase 4 fix round done (writer `19c5a6d3`, lead rerun: **849
  passed**): all audit hash fingerprints removed (zero `sha256` in
  `src/server/`), `ServiceLock.__del__` restored, last exact-type
  checks routed through `validate.py`. **Phase 4 closed.**
- Phase 5 done (writer `77294acb`) + final-review fix round
  (`1763fe3b`), lead gates rerun at close: **844 passed / 844
  collected**, golden 3, `ruff check` clean, `ruff format --check`
  49 files clean, `git diff --check` clean, `uv build` + isolated
  import smoke OK. Docs re-baselined (14 tools, best-effort contract,
  one-retry disclosure, audit allowlist, `DB_PATH`); `state.db*` added
  to `.gitignore` (check-ignore verified); dead Thyca trace remnant +
  uncalled `create_session` deleted; WAL sidecar 0600 chmod.
- Final whole-change independent review (`d63d198a`): **Approve with
  fixes**; both Important items (gitignore, 4 false doc passages)
  fixed above; no Critical findings; no confirmed behavior regressions
  vs the preservation checklist. **Source implementation is DONE and
  merge-ready by review. Nothing is committed, deployed, or restarted;
  the live service still runs the old build.**
- Final numbers: source **12.823 -> ~8.600 lines** (target <=7.000 not
  met, recorded honestly in the Phase 4 entry); tests **1266 -> 844
  collected** (all reductions itemized per phase above and in test-file
  headers; behavior kernels preserved per two independent reviews);
  15 -> 14 MCP tools (only `resolve_uncertain_drop`, §5.3).
- Per-phase line counts and test counts: _record at each gate_

## 7. Status of this plan

Owner approved execution on 2026-10-27 (subagent fan-out, one task per
agent; lead reviews each phase diff before the next writer starts). §5
decisions were all confirmed at their gates (§5.1 lead choice; §5.2
retry-once and §5.3 tool removal owner-confirmed). **All implementation
phases (0-5) are complete, independently reviewed, and verified (see
§6). This `done` covers SOURCE implementation only** (same convention
as the Modum plan): nothing is committed, deployed, or restarted. The
deploy gate below remains a separate owner approval; commit/merge also
need explicit owner request.
