---
status: in-progress
created: 2026-10-05
last_updated: 2026-10-05
---

# PLAN v5: Always-on auto-reply via agent-owned MCP send

> Approved implementation plan (docs-only artifact). Base commit `75c5ea0`
> (v4 checkpoint, 123 tests passing). v4 plan `v4-outbox-digest.md` stays DONE.
> This file is the ONLY file this planning task owns; no runtime code here.
> Implementation is approved; the earlier 'NO further commit beyond v4' line is
> HISTORICAL (superseded). The owner authorized review, verified fixes, and a V5
> commit after independent review and passing checks; these review/check gates
> are now satisfied.
> Primary coder: Meta Muse (`meta/muse-spark-1.3`, max thinking);
> parent orchestrator reviews only; fallback to MiMo allowed after limits.

## 0. Base, ownership, evidence

- Base: branch `main` @ `75c5ea0` (v4 checkpoint; 123 tests green AT THAT COMMIT).
  `75c5ea0` stays the historical v4 checkpoint, not the commit ceiling; V5 source
  was uncommitted at doc time and its commit follows the owner authorization above.
  v4 plan `v4-outbox-digest.md` stays DONE.
- CURRENT at `c8b6f94` (historical snapshot; superseded by §6A — verified after the 6-item review cleanup; supersedes the 454 baseline
  below): new `delivery.py` / `thyca_client.py` / `responder.py` + new
  `tests/test_delivery.py` / `tests/test_thyca_client.py` /
  `tests/test_responder.py` / `tests/test_server_responder.py`; modified
  `config.py` / `outbox.py` / `server.py` / `tasks.py` / `.env.example`
  (+ matching tests). 455 collected: delivery 74 (73 + 1 new `updated_at` pin),
  native client 57, responder 88 (same count: old 401/503 case expanded to
  401/503/409), server 65 + server-responder 53 (= 118 combined), tasks 19,
  rest 99; `pytest` green, `ruff check src tests` clean, `ruff format --check`
  clean (22 files). The 454 figure is retained below as the historical
  pre-cleanup baseline, 446 as the pre-edge-case baseline — neither is current. T0 capture metadata (`author_is_bot` /
  `reply_to_message_id`, absent = legacy unknown → SDK resolve) is in the
  tree with core-9 compat. All runtime/source writers are DONE (no active
  writer).
- This plan writer owns ONLY `.agents/plans/v5-auto-reply.md`. No code/tests/
  config/auth/profile/session/state/API/Discord/`.env`/commit changes in this
  task. The original planning task was documentation-only: its pre-cutover
  no-seed/no-ACK/no-restart/no-live statement is historical. Controlled
  migration/deployment and two live turns are now recorded in §6. Source edits
  are not hot-reloaded; V5 was loaded by the verified native cold restart.
- Reviews (same tree): native client independently approved earlier; separate
  component reviews clean. Combined security/product review is DONE: "Approve
  with fixes offline", 3 findings (hold-write durability failure AFTER an
  earlier intent/receipt commit → process-lifetime send latch set BEFORE the
  attempted uncertainty I/O; `sys.exception()` → `sys.exc_info()[1]` for Python
  3.10 compat; idle failed-Task exceptions consumed). The earlier §0 phrase
  "durable-hold write must precede commit" was erroneous and is corrected here:
  the failure is a hold-write PRE-commit failure AFTER the earlier
  intent/receipt commit, and the memory latch precedes the attempted
  uncertainty read/write — it does not mean the receipt had not committed.
  All 3 fixes are now present in-tree with new tests. Fresh independent review
  (fresh reader, no authorship of the fixed source/tests; same-family residual
  risk noted, not hidden — no invented different-model requirement) is DONE:
  APPROVE OFFLINE IMPLEMENTATION ONLY, NO findings — 8 new tests, targeted 137
  plus full 454 independently rerun, Ruff clean. Report (private, 0600):
  `/tmp/thyca-v5-final-safety-review.txt`. Exclusive seed/rollout helper is
  TMP-only under `/tmp/thyca-discord-v5-rollout` (NOT committed product
  source): helper tests 28 pass, Ruff clean, fresh helper-only re-review
  APPROVE HELPER ONLY with all 7 findings resolved. Report (private, 0600):
  `/tmp/thyca-v5-migration-rereview.txt`. Post-freeze cleanup (source-only,
  verified locally, running backend NOT restarted — still pre-cleanup code): two
  fresh reviewers read all four >1k-line modules plus production callers and
  found 6 MINOR only (no critical/important; `initialize` kept as the needed
  operator bootstrap, not dead; size alone forced no extraction) — unused
  `Responder.reconcile` + `busy` property removed (public worker surface stays
  `tick`/`close`), one canonical 4-stage mid-flight set, redundant GET snapshot
  branch merged (409-busy distinct), unused `_map_http_status` arg dropped,
  `requeue_definitive_busy` refreshes `updated_at`; guards intact. The final
  independent cleanup re-review is COMPLETED: findings NONE, all 6 deltas
  verified against source/tests, full 455 PASS + Ruff clean — scoped APPROVE of
  cleanup-commit readiness ONLY (not deploy/full live-failure; report
  `/tmp/thyca-v5-cleanup-final-review.txt`, 0600). T4 checks + reviews
  are DONE; T5
  DEPLOYMENT + owner happy/recall/alias + restart/selfbot/privacy verified;
  T6 evidence PARTIAL (see section 6). Live product traffic: two owner turns
  COMPLETED. The independent live-evidence review is COMPLETED: APPROVE of the
  operational cutover + owner happy-path evidence ONLY, NO confirmed product
  findings (report `/tmp/thyca-v5-live-evidence-review.txt`, 0600; scope is that
  happy-path proof only — not the full live-failure matrix, not merge
  permission).

## 1. Goal and approved scope

- Goal: always-on auto-reply with the service running. Flow:
  `event -> outbox -> FIFO worker -> existing native Thyca SID -> agent calls
  MCP send_message -> receipt + native Trace correlation -> controller ACK
  (safe processed prefix)`.
- ACCEPTED: OPTION 2 agent-owned MCP send (agent, not worker, calls MCP
  `send_message`). The worker/controller NEVER send a reply copy via Discord
  SDK (no bridge-owned SDK fallback path).
- Channel scope: current guild `1067690340359880724` / channel
  `1395103735725821982` ONLY. No DMs, no destructive actions, no v2/v3 tools,
  no wider channels.
- Single existing bot / MCP process / loop writer. Native Discord (tasks /
  history / MCP) and native Thyca HTTP API only. Forbidden: homemade Gateway,
  Scheduler, Pagination, new provider, upstream Thyca patch, new session
  creation.
- Identity: UID metadata + `whoami`; owner-only privileges, all other humans
  may chat. Names / typed IDs / profile autoload do NOT prove owner. No
  manual `USER.md` / private-profile I/O or disclosure.
- SharedSession incl. auto owner profile is accepted as explicit GUIDANCE,
  not as a new caller-ACL claim.
- Same SID `2026-10-05T01-59-55_0f25`, audit `discord-main`,
  `~/.thyca/discord_session.json` mode 600.
- ALL humans in the approved channel may chat; bot input scope is still
  hard-filtered to the approved guild/channel.
- Initial staging held the service down until review/cutover gates cleared.
  The backend is now operating; auto-reply is always-on by default, never a
  manual-enable product mode.

## 2. Verified contracts (do not redesign)

### 2.1 Native Thyca SDK (verified package source)

- `ChatApp.__init__` EAGER `spawn_all(cfg.mcpServers)` at startup -> cold-boot
  MCP without priming a paid LLM.
- Config MCP is stdio ONLY (`command`/`args`/`env`); no native URL MCP.
- Same-app MCP pool is shared with other SIDs; `resource_key=mcp:discord`
  serializes calls, BUT the tool callback carries NO caller SID/job identity.
- `POST /api/sessions/<SID>/turn` is SYNC; body `{text, model?, effort?,
  retry?}`, `len(text) <= 4000` (no silent truncation); `409` = busy.
- Timeout/disconnect does NOT cancel the native LLM turn; late background
  tool calls are possible. `GET` session `running` + native Trace IDs for
  correlation. No durable jobs, run keys, idempotency, per-turn tool
  allowlist, or native Discord connector.
- Native HTTP MUST NOT block the bot gateway (dedicated coroutine with
  installed `httpx`, or MCP-dep async client + `asyncio`; stdlib `urllib` in
  `to_thread` is acceptable). No exactly-once guarantee; idle alone is NOT
  proof of non-acceptance.
- Native SDK default models / provider / auth values stay unchanged; no
  hidden override.

### 2.2 Current bridge behavior (parent-verified)

- `create_server` lifespan guard runs BEFORE listener / login / readiness
  (native order preserved).
- Capture is v4-faithful: realtime `on_message` appends (only OUR bot self
  excluded); native poll includes self. See correction A.
- `TaskRegistry` uses the native Loop; no self-made scheduler.
- Outbox seq reserve allows gaps; global ACK = highest ACTUAL processed
  prefix; corruption fails loudly, never resets.
- `StopTask` can stop all loops; `RemovePoll` protects ONLY `heartbeat`.
- New reserved `auto`/`recovery` tasks are protected from stop/remove, but
  existing `heartbeat` STOP stays temporarily allowed (v4 compat); heartbeat
  still cannot be REMOVED. Protect new task names WITHOUT silently banning
  heartbeat stop.

## 3. Corrections vs the first draft (binding)

- A) Capture stays v4-faithful: only OUR bot self is excluded realtime, poll
  self is kept, OTHER BOTS ARE STILL CAPTURED. The future WORKER filters
  records with actual `author_is_bot=true`. Capture adds OPTIONAL new fields
  `author_is_bot` (bool, native `message.author.bot`) and
  `reply_to_message_id` (native `message.reference.message_id` or None). The
  core 9 required fields are unchanged. Legacy records missing the new fields
  mean UNKNOWN, not assumed human (may fetch actual member `bot` via SDK
  where eligible for legacy/new messages). NEVER say the listener drops
  other bots.
- B) Agent-owned delivery only; the worker never SDK-sends a reply copy.
  Existing MCP `send_message` content/channel default shape is preserved;
  planned optional correlation params (`source_message_id`, `job_ref`)
  plus verified NATIVE auto session / new Trace / tool-call results. A shared
  SDK helper records intent BEFORE the send await and the candidate receipt
  MID AFTER SDK success (private journal). A tool-helper receipt alone NEVER
  marks a job COMPLETE: native SID / turn / call result must correlate to the
  actual requested job. A global active-job / ContextVar is NOT caller ACL.
  Manual uncorrelated sends stay allowed (this is NOT option-1 blocking); a
  manual send is never attached to a job. ACK guard: never beyond the
  processed journal prefix; no future-ACK, no blind-latest. ACK has a
  controller owner-auto path; the v4 manual flow with a proper confirmed
  prefix stays allowed, otherwise explicit error, never silent clamp.
- C) Job states MUST include `DELIVERING` (send-intent boundary recorded
  BEFORE the Discord SDK side effect) plus `queued`, `turn_submitting`,
  `inflight`, `sent` (candidate), `completed` (trace-verified), `skipped`,
  `uncertain`. Rationale: a crash after Discord accepts but before the
  journal persists the MID -> `uncertain`, with NO automatic blind retry.
  Receipts store metadata only (IDs / hash / length); NO raw user text,
  reply text, Profile, or provider error strings. Error field is structured
  coarse `kind`/`presence`/`type`/`status` only, never freeform `error: str`.
  Journal file mode 600 under a parent-owned new dir mode 700; atomic write
  via secured temp inode then `os.replace`; no fallible post-commit chmod,
  no arbitrary parent permission changes.
- D) Rollback is NEVER "restore old acked/outbox state or a delivery backup,
  then restart" automatically: rewinding ACK or losing receipts can re-reply
  to already-delivered Discord posts. Rollback = code/stage service while
  PRESERVING authoritative outbox / current journal / ACK; unresolved
  inflight stays `UNCERTAIN`. A corrupt state is restored ONLY from a
  matching valid backup PLUS explicit external delivery reconciliation and
  operator approval; no guessing, autoreset, or manual deletion.
- E) Security claims cover: hardcoded guild/channel/auto-job input scope,
  native bot flag, and state validations; plus the existing UID owner/other
  policy incl. builtin profile-access LLM GUIDANCE (unless a verified new
  native authorization exists). Neither worker context nor a source-MID
  argument authenticates the caller. No unsupported per-turn allowlist or
  upstream-API pretense. A non-owner trigger needs an ACTUAL test; if no
  second account is available, report `LIVE-other NOT verified` instead of
  faking an owner-UID test. No manual profile reads anywhere in tests.

## 4. Delivered modules and interfaces

- New files (fixed names): `src/thyca_discord_mcp/delivery.py` (journal +
  job state machine), `src/thyca_discord_mcp/thyca_client.py` (native HTTP
  turn client), `src/thyca_discord_mcp/responder.py` (FIFO worker + tasks +
  lifespan wiring). Tests: `tests/test_delivery.py`,
  `tests/test_thyca_client.py`, `tests/test_responder.py`.
- Config settings (actual stable 6; bounded technical values, not business
  weights): `thyca_base_url`, `thyca_session_mapping_path`,
  `delivery_state_path`, `responder_check_seconds`,
  `thyca_request_timeout_seconds`, `recovery_poll_seconds`. SID is derived
  from the existing native mapping file only (no `thyca_session_id`
  setting). Technical defaults `1.0` / `900.0` / `30.0`, finite validation
  with bool rejection (parent-diff verified; no live proof claimed). No
  keyword triggers, no invented drop/score/budget/grouping rules.
- Scheduling: one UID job per MID, FIFO; native Loop tick is configurable
  (writer picks the default technical value, justified by measured tests).
- State functions (actual `delivery.py` public API — delivery/core API finalized,
  including `requeue_definitive_busy` (now also refreshes `updated_at`, created /
  SIDs / aliases preserved) and the metadata fingerprint
  (`submitted_text_sha256` + character length, never raw text); the lifecycle-fault
  fixes are implemented in-tree as verified (see §0/§9), not still under work;
  no whole-product DONE claim):
  `initialize_delivery_state` (explicit exclusive fresh init ONLY; atomic,
  §3C), `load_delivery_state` (missing state FAILS, never autoresets),
  `enqueue_job`, `get_job` / `list_jobs`, `mark_turn_submitting`,
  `mark_inflight`, `record_send_intent` (BEFORE SDK send),
  `record_send_receipt`, `complete_job`, `skip_job`, `mark_uncertain`,
  `processed_seq_prefix` = safe ACK prefix of ACTUAL ordered records (gaps
  legal; NOT mathematically every-integer-contiguous; never blind-latest).
- Correlation choice (implemented as verified — same business behavior, not a plan):
  `source_message_id`/`job_ref` passed through the agent-owned MCP send path
  (agent, not worker, calls MCP `send_message`; worker/controller never send
  a reply copy), verified against native Trace + tool-call results; candidate
  MID stays `sent` until the trace match promotes it to `completed`.
  `source_message_id` is `StrictInt` at the MCP boundary (only JSON ints pass;
  bools and digit-strings are rejected before any helper guard runs) and the
  pair is both-or-neither: unpaired manual sends are unchanged and never
  job-attached. The tool surface stays 12 tools (no new names); auto-reply
  control is via the existing task tools and native loop flags, not new CLI
  subcommands (single stdio entry point, unchanged).
- Remaining architecture limits are explicit, not hard guarantees: shared
  MCP pool without caller identity, no durable native jobs, no per-turn
  allowlist, timeout != cancel, no exactly-once.

## 5. Tasks and gates

- T0 Capture metadata (depends on capture writer `ac2a3692`, same 4 files;
  v5 coder does NOT touch them): consume optional `author_is_bot` /
  `reply_to_message_id`; treat absent as unknown; verify core-9 compat.
  Gate: capture writer's files merged/green first.
- T1 Config + delivery core (`config.py` settings; new `delivery.py` +
  `tests/test_delivery.py`): journal atomicity/permissions, state machine
  incl. `DELIVERING`, `safe_ack_prefix`. Gate: offline unit tests green.
- T2 Native HTTP client (new `thyca_client.py` +
  `tests/test_thyca_client.py`): SYNC turn `len(text) <= 4000` characters
  (NOT bytes), 409-busy, timeout semantics (no cancel assumption),
  running/Trace getters, non-blocking use. Gate: mocked + native-shape tests
  green; no gateway blocking.
- T3 Responder + send/ACK/tasks/lifespan (new `responder.py` +
  `tests/test_responder.py`; `server.py` wiring AFTER T0 lands to avoid the
  race): FIFO worker, other-bot filter, agent-owned send, receipt/Trace
  correlation, controller ACK safe prefix, reserved auto/recovery tasks with
  heartbeat-stop compat (§2.2). Gate: offline worker tests green.
- T4 Strong offline checks + independent review: DONE. Full `pytest` 455 passed
  (delivery 74, native client 57, responder 88, server 65 + server-responder 53
  = 118 combined, tasks 19, rest 99; the 454 figure is the historical
  pre-cleanup baseline, 446 the pre-edge-case baseline), `ruff check src tests`
  clean, `ruff format --check` clean (22 files); reviewer audit of correlation, ACK guard, permissions, privacy (no
  raw text in journal). The project requires an INDEPENDENT reviewer, not a
  different-model-family rule: a fresh-context review with no authorship
  qualifies (same-family residual risk noted, not hidden; no fabricated
  different-model requirement). A reviewer quota limit is not an automatic block
  on all implementation — what is actually required is this strong combined
  review plus live-scope authorization. Combined review DONE ("Approve with
  fixes offline", 3 findings, all fixed in-tree); fresh review DONE (APPROVE
  OFFLINE IMPLEMENTATION ONLY, NO findings; 8 new tests; targeted 137 + full
  454 independently rerun; report `/tmp/thyca-v5-final-safety-review.txt`, 0600).
  T4 sign-off does NOT approve a commit and the product is NOT done; live
  scope/results are recorded under T5/§6. T4 proof alone was offline.
- T5 Docs + native controlled deploy (after T4 sign-off + migration approval):
  real `HumanMessage`-triggered run in the approved channel covering owner +
  other human, same-session recall, native MCP send receipts, ACK advance,
  duplicate/poll-redelivery handling, crash-recovery (`uncertain`, no blind
  retry), privacy (no raw/profile leakage). Gate: actual proof logs.
- T6 Final docs: record evidence, set plan status `done` ONLY on actual
  proof. A plan alone never marks code DONE or tests passed.

## 6. Migration: exact 2 skipped messages (approved, COMPLETED one-time)

- Backlog question is CLOSED by explicit owner verdict (test data, nothing
  to keep). Parent fresh production metadata ONLY: acked 21 / latest 23 /
  pending 2, owner UID + approved channel match.
- Authorized skip of EXACTLY: seq 22 MID `1556471434610483303` + seq 23 MID
  `1556471963957067780`. No other pending existed at that snapshot.
- Migration COMPLETED (was: exclusive one-time controlled-deployment fresh-state
  initialization, after reviews, while the old writer was drained): persisted
  these 2 `SKIPPED` receipts keyed by MID+seq via the coded fresh initializer,
  then advanced the safe ACK through 23 (guarded ACK 21 to 23). Same native SID
  preserved (no new SID, no pruning); config/auth/mapping preserved; private
  backup dir 700, outbox+state byte-identical 600. Subsequent missing
  state FAILS (no reset). No promise of server auto-initialize every boot.
  Nothing else was skipped, deleted, or reset. Do NOT rerun the initializer
  and do NOT rerun the helper dry-run (it rejects when delivery state exists).
  The cutover itself made no runtime-source changes, deletions, rollback,
  staging, or commit; the V5 source changes are separate from the historical
  `75c5ea0` checkpoint and are covered by the owner-authorized reviewed commit
  workflow.
  Evidence (metadata-only pointers, private 0600, recorded here only):
  `~/.thyca/discord-integration-evidence/v5-cutover-20261005T080836Z/`
  (`manifest.json`, `migration-apply.json`, `live-turn-2.json`,
  `live-turn-3.json`, `restart-before.json`, `acceptance-metadata.json`).
- New seq >= 24 MUST queue/respond only ELIGIBLE human NEW source entries;
  self / other-bot / historical / skipped-MID aliases are filtered (no
  response); never blind-startup-latest-skip, delete, or reset.
- Cutover COMPLETED (supersedes the earlier NOT-EXECUTED/BLOCKED claim, kept
  below in history): the operator AGREED global idle and cutover and paused new
  native work; parent reran 454 pytest PASS (11.53 s) + Ruff 22 formatted +
  diff check. The prior foreground-with-no-pidfile account is REFUTED
  hypothesis, kept here only as labeled history: the native pidfile DID exist
  and matched the sole native PID/identity, and native `--serve --stop`
  successfully stopped that known PID; all 6 old native/MCP processes verified
  dead. The native pidfile/log footprint already existed — the native
  `--serve --daemon` cold start with native flock is not an invented
  systemd/custom-gateway/new-MCP-bridge/scheduler, and the existing CLI flags
  stayed the original controlled entry (PID change is normal). Operational rule:
  native `--serve --stop` targets the pidfile — before using it verify the file
  PID is the same owner/process identity (never infer from an argv `--daemon`
  flag, never blindly trust a stale pidfile). Backups with fingerprints were
  taken; then the exclusive exact seed only. Later seq 24+ preserved
  (poll-recovered old-SKIP aliases 24/25 safely ACKed 25 with NO paid turn).
  There is no runnable "delete the journal" fallback and
  no blind-ACK command — not here, not in the README, not in TOOLS.
- Live turns COMPLETED (same SID `2026-10-05T01-59-55_0f25`, approved guild /
  channel, real owner UID): seq 26 / `1556580145895186443` (alias 27) → turn 2
  → receipt `1556580282809978963` → COMPLETED; seq 29 / `1556581565377679361`
  (alias 30) → turn 3 → receipt `1556581720827101255` → COMPLETED. Controller
  ACK 31 / latest 31, including actual selfbot-filtered polls 28/31. Verified
  via existing client GET trace 2/3 (no bulk old reads): input fingerprint /
  SID / turn index / error-free completion / all-intents + receipts + tool
  results 1:1 / zero legacy unpaired. UI matched actual receipts/reference plus
  confirmation and recall code; the second message did NOT repeat the code. No
  worker reply copy, no paid priming, no new session. Native `list_tasks` from
  turn 3 proves heartbeat 300 / auto-reply 1 / recovery 30 running. Source
  bootstrap never every boot and still fails closed on missing state; source
  protectedness verified offline with no forced external stop. All humans always
  on source, unchanged — NO owner-only flag. No exactly-once, caller-SID, or
  actual-UID-from-typed-text claim. Raw reply/profile bodies, hashes, and test
  canaries are NOT copied into shared docs (privacy canaries byte-absent from
  journal/audit).
- Future poll duplicates of the exact skipped MIDs reuse the `SKIP` receipt;
  historical MIDs already baseline-acked (<= 21) are also NOT autoreplied
  after poll redelivery.
- Controlled native CLI stop+start COMPLETED after all jobs finished (main idle,
  global owner pause still held): old 6 dead; current native PID 1453387. After
  the recovery tick: max native turn index 3 UNCHANGED, same jobs/intents/
  receipt MIDs/aliases, ACK = latest 31, no resubmission or duplicate SDK send.
  All old non-target session-dir metadata unchanged with no new files; ONLY the
  target SID journal bytes+mtime changed for the expected 2 turns.
  Config/auth/mapping hash+size/mode/mtime unchanged. Default provider/model/
  effort unchanged.
- Pre-flight dry run DONE (read-only, NOT a seed): exit 0 on production
  mapping/outbox; existing SID with title match; acked 21 / latest 23 / 23
  entries / 21 historical / exactly the 2 authorized SKIP candidates (seq 22 +
  23 with the MIDs above); safe prefix and ACK target 23; delivery absent;
  committed false; no new pending messages. Outbox/state bytes verified
  unchanged after the run. Target-SID native metadata GET only: running false,
  pre-max-turn-index 1, SID + title match, zero LLM POSTs — proves only that
  SID idle, not global. Private proofs (0600, not embedded here):
  `/tmp/thyca-discord-v5-rollout/preflight-dryrun.json` and
  `/tmp/thyca-discord-v5-rollout/preflight-native-metadata.json`. Raw metadata
  hashes/bodies are not copied into shared docs.
- Helper gates (TMP-only scripts, NOT committed product source, no new SID /
  model / effort override): report-authority collision pre-read guard, EXACT
  2-authorized-SKIP pin, mandatory apply PID-death + hash + SID gates; reviewed
  cutover, not automatic reset; no unsafe CLI commands reproduced here.
- No owner business migration stays OPEN now. Preserve the prior 21 sessions /
  auth / defaults / config / mapping bytes / outbox ACK / receipts. Bulk
  sessions/traces reads are forbidden (private legacy files) and are not a
  workaround. LIVE-other is NOT verified unless an actual second human tests it
  — never fake an owner-UID test as that proof. This planning task mutated NO
  state.

## 6A. Accepted identity update (SID-pinned; historical source proof)

- The owner approved ignoring the native display title for identity: all 3 snapshot
  gates (`_submit_queued`, `_reconcile_after_post`, `_reconcile_job`; 6 clauses) check
  `native_id_match` only, `expected_title` omitted. `title_match` stays optional
  diagnostic metadata; internal audit/`session_id` label stays `discord-main`, separate
  from the renamable native UI title (cosmetic; native SID fixed). Own-SID trace index, fresh nonce, first-user
  SHA/len proof, and receipts-gated safe ACK are unchanged; true SID mismatch still
  fails closed. Checked on SID-fix source (historical checkpoint): 464 PASS in 13.16 s (responder 97 = 88 + 9
  new title-ignored cases); `ruff check src tests` + format (27 files) + diff check
  clean. Later external restart observed 2026-10-05 18:58+07; no retroactive
  deploy approval from that event, and open live-fail gates unchanged.
  No commit after `c8b6f94`. PAUSED: server helper extraction
  is wired (server 931 + helpers 127 lines); delivery/client helper files remain
  inert, full refactor incomplete. Peer re-review (`bd47`) is COMPLETE: NO confirmed
  findings, scoped APPROVE of the SOURCE SID fix ONLY (not deploy/live).
  T5/T6 stay in-progress.

## 6B. Accepted deleted-source no-reply (source-only; rollout pending)

- Policy: a source confirmed deleted (exact MID) BEFORE any send attempt is a
  proven-no-send durable `skipped` / `source_deleted` with zero send evidence —
  no intent, no SDK send, no plain fallback — checked before the LLM POST and
  again before the correlated MCP send; safe-prefix ACK. Proof is strict: exact
  `fetch_message` GET bound to the approved guild/channel/MID raising native
  NotFound 404 + 10008; on GET success the returned MID/channel/guild must
  match. `SourceUnavailable`/permission/403/other 404s (e.g. 10003)/timeout/5xx
  is NOT deletion (pre-POST `queued`/`pending`, no POST/nonce; pre-intent MCP
  bounded error, no SDK). Existing intent/receipt/trace or
  `uncertain`/`completed` never skips; partial sends hold `uncertain` (receipt
  kept, further chunks withheld). No MCP send after a mid-generation deletion
  can hold `uncertain` (no always-unblocks-FIFO claim). After any SDK call ALL
  errors stay `uncertain`. Durable zero-evidence skip wins over late native
  POST/result/snapshot/trace outcomes; cancellation propagates. Storage v1 +
  new skip enum (aliases/unknown fields preserved);
  `MessageReference(fail_if_not_exists=True)` kept; single GET read, no history
  scan or content processing, needs ViewChannel + ReadMessageHistory. Symbols:
  `source_messages.py` (`build_source_checker`/`check_source_message`/
  `_resolve_text_channel`, alias via `server_helpers`), `Responder` optional
  `source_checker` (default real SDK), `skip_job` zero-evidence gate. Reply
  policy only: no AI-history/message-body deletion; owner/clone/shared-memory
  design tabled.
- No full-refactor claim: server helper wired, schema/client splits inert and
  paused; no always-no-reply/exactly-once claim (narrow HTTP-write races hold).
- Checks, dated 2026-10-05: FINAL 503 PASS in 12.78 s (464 baseline + 39
  source-deleted: 36 initial + 3 guard), Ruff check clean, 33 formatted, diff
  check clean. Both hardenings DONE and checked (common fresh post-GET latch +
  correlated_job guard on both paths; `source_deleted`-only tombstone
  contradiction rejection, legacy unchanged; INERT schema mirrored, not wired).
  3 new regressions failed pre-patch, pass fixed. 500 (12.73 s) stays a dated
  initial-feature historical marker. Review `0ca0d96d-8efe-46c` (APPROVE at the
  500 checkpoint) stays historical; fresh final review `0887b525-8634-4c6`
  is complete: no findings, APPROVE of source handling after fixes only.
  Own checks: 39 feature tests + 3 guard regressions, lint/format/diff check
  and cold imports; no deploy/live acceptance.
  §6A 464 and §0/§7/§9 455 stay historical.
  Source frozen, all authors DONE. Source-only; rollout pending; no restart/commit here. Seq-66 job stays
  `uncertain`, untouched. T5/T6 in-progress; plan NOT done.

## 7. Tests (native / offline / security)

- Preserve all v4 tests and keep full `pytest` + `ruff check` +
  format green. Current on the V5 source at doc time: 455 collected
  (delivery 74, native client 57, responder 88, server 65 + server-responder 53
  = 118 combined, tasks 19, rest 99) — this replaces the stale base-commit figure of 123
  (which described `75c5ea0` only) and supersedes the 446 pre-edge-case
  baseline (historical, not current). SDK API body `len(text) <= 4000` characters, never silently
  truncated.
- Required coverage: ACK interference; misattribution of manual/other-SID
  sends; send-timeout-late arrival; cancel; startup 401/503; corrupt journal
  (fail, no reset); no-fallback (worker never SDK-sends); unwatched channel;
  other-bots filtered by worker; shared-SID pool; the 2 skip MIDs; new
  messages preserved; provider-error canary (coarse kind only); meta
  bot/reference fields; no raw user/reply/profile text anywhere in receipts.
- Live matrix (T5): owner trigger, other-human trigger (or explicit
  `LIVE-other NOT verified`), same-session recall, native MCP send receipt,
  ACK advance, duplicate suppression, crash `uncertain`, privacy audit.

## 8. Rollback and limitations

- Rollback preserves authoritative outbox / journal / ACK (§3D); inflight
  stays `UNCERTAIN` pending reconciliation. No auto-restore of old state.
- Restart/migration uncertainty is explicit: any restart must preserve ACK and
  receipts (no stale-backup restore, never delete/restore a stale backup), and
  the pre-cutover foreground-process description is now historical — the
  corrected model is the native pidfile controlled source-backed stop/start
  under the same existing control footprint. No daemon
  service migration is recommended or described anywhere here.
- Known limits (explicit): shared MCP pool has no caller identity, so
  correlation is journal+Trace based, never an auth proof; native timeouts
  may still deliver late; no exactly-once; idle is not proof of
  non-acceptance; reserved-task protection must not silently ban heartbeat
  stop. Fencing is process-lifetime only (verified present, not pending): the
  combined-review fix sets a memory latch BEFORE any uncertainty read/write
  whenever persistence of the unknown state fails, rejecting retries before
  intent/SDK while preserving the already-committed intent/receipt and
  protecting ongoing same-process retry even if disk is unwritable — NOT a
  durable cross-crash guarantee (a crash loses the memory latch, so no
  exactly-once claim).
- Security posture: hardcoded scope + validations + UID policy + guidance
  (§3E). Anything stronger (per-turn allowlist, caller auth) needs a
  verified native feature first and is NOT claimed.

## 9. Handoff

- Plan path: `.agents/plans/v5-auto-reply.md` (this file). Status stays
  `in-progress` until T6 proof. NOT done. The backend IS deployed and auto-reply
  is operating now; T6 evidence is PARTIAL — do NOT mark the whole V5 done on
  this partial proof, and do not invent an exclusive-gate claim.
- Progress on the V5 source at doc time (commit follows owner authorization above):
  T0 capture metadata + T1 delivery core + T2 native client + T3
  responder/send/ACK/tasks/lifespan are implemented offline (455 historical c8 snapshot,
  superseded by §6A:
  delivery 74, native client 57, responder 88, server 65 + server-responder 53
  = 118 combined, tasks 19, rest 99; `ruff check` and format clean for 22
  files; 454 retained only as the pre-cleanup baseline, 446 as the
  pre-edge-case baseline). Post-freeze 6-item cleanup verified locally (see §0);
  the running backend still executes pre-cleanup code (no restart this request);
  the final independent cleanup re-review is COMPLETED with scoped APPROVE of
  cleanup-commit readiness ONLY (findings NONE; not deploy/full live-failure).
  Component-only reviews are clean (native client approved earlier;
  separate component reviews clean, same-family risk noted); the combined
  security/product review is DONE ("Approve with fixes offline", 3 findings, all
  fixed in-tree with new tests: latch-first uncertainty fencing,
  `sys.exc_info()[1]` 3.10-compat with only the newer runtime actually executed,
  idle failed-Task consume preserving protection/running stop); the fresh review
  is DONE (APPROVE OFFLINE IMPLEMENTATION ONLY, NO findings; 8 new tests;
  targeted 137 + full 454 independently rerun;
  `/tmp/thyca-v5-final-safety-review.txt`, 0600). The exclusive seed/rollout
  helper is TMP-only under `/tmp/thyca-discord-v5-rollout` (NOT committed
  product source; 28 helper tests pass; fresh HELPER-ONLY re-review approves
  with 7 findings resolved; `/tmp/thyca-v5-migration-rereview.txt`, 0600).
- Accepted requirements (§1), verified contracts (§2), and binding corrections
  (§3) above are preserved from the approved plan; the stale first-draft counts
  (59/33 item figures, unknown-API notes) are superseded by the verified
  per-file figures in §0/§7. Shared tool contracts live in `docs/TOOLS.md` and
  the operator summary in `README.md` — this file does not duplicate them.
- Open gates: T4 is DONE; T5 deployment + owner happy/recall/alias + restart /
  selfbot / privacy verified; T6 evidence PARTIAL. Accepted scope distinction
  (not new gates): LIVE-other-human explicitly NOT verified is an accepted
  alternative, not a mandatory second-account/synthetic test; wrong-guild/DM
  live testing is OUTSIDE the authorized channel-only/no-DM scope (offline
  coverage only, not a deployment gate); other-bot simply not available — NOT
  run, not an invented gate. What keeps the full T5 matrix/T6 partial and the
  plan in-progress is the original remaining T5 failure/crash-uncertainty LIVE
  gate NOT exercised. No production fault injection/drop is authorized. The
  concurrent independent live-evidence review is COMPLETED (APPROVE scoped as
  above) — its scope stays happy-path proof only; do NOT claim the reviewer
  approved everything or mark the product done.
  No permission to fault/crash the running native work or blind-drop a new job.
  No owner business migration stays OPEN now. This planning task mutated NO
  state.
- Next assignee: work stays on the V5 files/interfaces within the approved scope;
  parent owns review, production metadata, migration approval, and the
  stop-old-writer gate (single writer; stopping it ends the live service).
