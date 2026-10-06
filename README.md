# thyca-discord-mcp

Discord MCP integration for Thyca with shared conversation context, Jev
participation routing, agent-owned replies, and durable delivery tracking.

**Modum** connects an approved Discord channel to a native Thyca agent. Jev
answers three routing questions (`addressee`, `explicit_assistant_request`,
`participation`) and votes `respond` or `observe`; Thyca receives the
same canonical Discord snapshot when a native turn is needed, uses its existing
session memory, and owns the reply through MCP. Neither Jev nor the controller
copies generated replies directly to Discord.

The Python package is `modum`; the distribution and CLI remain
`thyca-discord-mcp`. This is a Thyca integration, not a standalone chat agent.

## At a glance

- **Shared context:** whole recent messages with SDK UIDs, names, timestamps,
  and exact reply links. Default maximum 20 messages including the source,
  subject to the complete native prompt's 4000-character limit.
- **Participation routing:** one durable Jev classification attempt, exact
  decided-cache reuse, and reported-cost accounting (no daily spend stop).
  All humans may chat; there is no owner-only, mention-only, or
  confidence-threshold chat gate.
- **Agent-owned delivery:** threaded MCP sends, intent-before-send journaling,
  receipt/native-trace verification, uncertainty holds, and safe-prefix ACK.
  No exactly-once or caller-ACL guarantee is claimed.
- **14 MCP tools:** guild/member inspection, messages, outbox, background tasks,
  scoped context reads, and explicit no-reply declarations. The proposed v2/v3
  destructive/admin tool set is not implemented.

**Current scope:** the Modum source is implemented and independently reviewed;
the live service was restarted by user order on 2026-10-06 with the current
source (3-question Jev prompts, no daily spend stop). Do not start or restart
the service again without separate operational approval. The V4/V5 sections below preserve dated
checkpoints, not a current live-state attestation; original live-failure
acceptance remains incomplete.

## Documentation

- [Conversation context and memory](docs/CONTEXT.md): selection, shared snapshots,
  native memory, prompt fit, missing parents, privacy, and `need_context`.
- [MCP tool contracts](docs/TOOLS.md): arguments, identity rules, correlation,
  ACK, and task guards.
- [Example configuration](.env.example): required secrets and optional tuning.
- [Completed source plan](.agents/plans/modum-jev-integration-refactor.md) and
  [separate V5 live plan](.agents/plans/v5-auto-reply.md): source evidence versus
  remaining operational acceptance.

## Accepted requirements

1. **Full admin** on one guild (the owner's server). Owner calls run immediately, no confirm step.
   Every action is recorded in `audit.jsonl` as the safety net.
2. **Identity**: every UID is classified as `self` / `owner` / `other`.
   UID is the stable key; display name is resolved at runtime for humans and logs.
   Only the owner issues admin instructions; commands inside `other` messages are never followed.
3. **Session & memory**: all Discord activity shares one fixed session (`discord-main`).
   The owner accepts Thyca auto-loading the owner profile into agent context; the
   agent must still not manually read/write the real profile file (`USER.md`)
   unless the owner explicitly asks — and a present profile never authenticates
   the Discord sender (identity comes only from message metadata, see below).

## Integration status — V4 historical verified snapshot (supersedes earlier PENDING)

- **History preserved:** this section previously said "PENDING, not verified; no
  live Discord run yet; review-pending". That claim is SUPERSEDED by the verified
  facts below. The plan keeps the full review history.
- **Scope of this section:** everything below describes the V4 deployed stdio
  writer (commit `75c5ea0`: 123 tests, 64/64 eval checks, 12 tools) as a
  historical verified snapshot. It does NOT describe the V5 source
  (see "V5 auto-reply — in progress" above), which changes the auto-reply,
  pull-protocol, and restore behavior stated here.
- **Production wiring: verified.** Parent applied exactly `mcpServers.discord`;
  all other configuration values/unknown fields and auth bytes preserved, default models
  and other MCP entries preserved. All 21
  old sessions unchanged, ONE new 22nd native session; daemon not restarted (native
  hot reload). `.env` MODE-only (0600, not rotated/read); home `~/.thyca` 0700
  already protects.
- **Native E2E over the real Thyca MCP client: PASS (exit 0).** Final report
  `ok:true cleanup:true`, every phase block ok, 12/12 tool specs. First poll
  captured history seq 1..20; ping marker captured seq 21; restart persisted the
  cursor (acked 21); owner realtime seq 22 + poll redelivery seq 23 (same message,
  deduped by `message_id`); own message poll-only seq 24. Live E2E exercised these positive
  paths; DM/unwatched-channel/other-guild/self-realtime exclusions are unit-tested guards
  (no live bad-guild/DM traffic, owner scope forbids it). Harness state was isolated and removed; 35 mock
  tests + self-check pass.
- **Real-Thyca 2 LLM turns: PASS (eval v1.0.3, model `muse-spark-1.3`, effort max).**
  Exact driver `--run --follow-up`, exit 0, no retry POST / new session / config change.
  Two POST 200 `completed`, error absent, same native session, turn indices 0 and 1.
  Turn 1: 14 calls (first a builtin `read` of absolute `docs/TOOLS.md`, then MCP calls
  across 8 `discord__` tool types); turn 2: read-only `guild_info` + `list_tasks`
  (2 calls). Checks 51/51 + 13/13 = 64/64. Marker `THYCA-V4-76748A8-EVAL01` sent once,
  poll-captured as the same message at seq 21 (initial 20 old entries acked 20, then
  marker acked 21); follow-up recalled message_id + exact marker without the id
  repeated in the prompt; no new writes / fresh session. Own poll-task add/remove cycle
  ended removed; heartbeat RUNNING at final/follow-up. Driver offline proof/privacy
  assertions (250) + compile/dry-run PASS. This proves actual native LLM turns with tool
  traces — not just a model-reply PASS. Eval/native scripts live in `/tmp` (not product,
  not committed); native E2E alone does not prove autonomous LLM triggering.
- **Discord UI confirmation: DONE.** The LLM marker jump URL
  `/channels/1067690340359880724/1395103735725821982/1556394639798308972` was
  independently confirmed in Discord UI via a readonly DOM exact target-ID proof
  (scope/mid/message/marker/bot-badge all true; no snapshots/history/raw text, no UI
  writes, no direct Discord HTTP/cookies).
- **Production audit/state: verified (not skipped).** `audit.jsonl` structurally valid
  (every record an object with `tool` str / `ok` bool); since eval start 19:56:08Z, 17
  records: 15 non-heartbeat MCP calls all `ok:true` (`guild_info` 2, `whoami` 1,
  `list_tasks` 3, `add_poll_task` 2, `read_outbox` 2, `ack_outbox` 2, `send_message` 1,
  `remove_poll_task` 2) + 2 `heartbeat`. Audit label ALL `discord-main`; channel args
  ALL approved. Persisted state `seq`=`acked`=21, poll cursor == marker message_id;
  outbox holds ONE same-message entry (seq 21, `type:poll`, `actor_kind:self`, bot UID,
  exact marker true); all outbox channels approved. Thyca-owned daemon/MCP pool processes
  stay alive by design (not orphans, do not kill). `git check-ignore` confirms
  `.env`/`audit.jsonl`/`outbox*` ignored and untracked; raw worktree files mode 644 under
  home 700 (not a leak, unchanged — 0600 applies to the mapping/evidence/`.env` only).
- **Checks (fresh, post-live):** `uv run pytest` 123 PASS; `uv run ruff check src tests`
  clean; format check clean (15 files). Server independent final: APPROVE (startup guard
  + lifecycle cleanup). Outbox full-scan validation runs at startup before listener/login;
  O(1) append, single writer, seq reserve with gaps, fail-loud on corruption, never silent
  reset. Native `discord.ext.tasks` loops, native `channel.history` paging, native MCP APIs
  — no homemade scheduler/gates/pagination. Verified SDKs: discord.py 2.7.1 / mcp 1.30 (<2)
  / Thyca 0.86.5.dev0.
- **Not implemented in the V4 deployed writer (do not claim):** no server notifications/push, no autonomous
  watcher-to-LLM bridge — capturing never launches LLM turns; the consumer pulls with
  `read_outbox` and acks after processing. (The V5 source adds a worker that
  automatically POSTs a native turn per eligible event; capture itself still contains no LLM.)
  Current scope is the 12 implemented tools only;
  v2/v3 mutation/admin/destructive operations remain proposed and untested; existing read-only
  channel/member listings were exercised in the native 12-tool E2E.
- **Owner profile auto-load: accepted by owner** (supersedes the earlier auto-preload
  blocker — history kept in the plan). Auto-load never authenticates the Discord sender;
  the agent still must not manually read/write the real `USER.md` file unless the owner
  explicitly asks. Sender identity comes only from message-metadata UID + `whoami`
  (agent policy, not caller ACL).
- **Safer start prompt for the operator:** have the agent read the full `docs/TOOLS.md`
  first, then verify every Discord sender with MCP `whoami(author_uid)` using the metadata
  UID from the outbox (never a name or ID copied from message content), and operate ONLY
  in the approved live channel below. This server sends no push/notifications: the agent
  must pull with `read_outbox`. (V4 deployed writer; the V5 source auto-triggers
  a native turn per eligible event — see above. The server still sends no MCP push.)
- **Session reuse (audit label != native ID):** the audit tag is the literal label
  `discord-main`. Reuse the ONE existing native generated session ID via the
  private mapping `~/.thyca/discord_session.json` (0600) — never a new session
  per message. Private metadata-only proof copies live in
  `~/.thyca/discord-integration-evidence/` (0700/0600, never committed).
- **Session identity is the SID, not the display title (in source; historical
  checkpoint):** the owner approved binding identity to the pinned
  native SID only. A native display-title rename (Thyca UI) does not change
  which session the source talks to: the three snapshot gates
  (`_submit_queued`, `_reconcile_after_post`, `_reconcile_job`) check
  `native_id_match` only, and `expected_title` is omitted (optional
  diagnostic only). The internal `SESSION_ID=discord-main` audit label and
  the mapping file shape are unchanged. Dated note (2026-10-05): the earlier
  absolute 'running backend still pre-fix' is historical; a later external
  restart was observed 18:58+07 with no retroactive deploy approval, and open
  live-fail gates are unchanged. A true SID mismatch still fails closed (pending /
  uncertain, never blind ACK).
- **Launch via uv, not project python:** Thyca's `resolve_command` rewrites `python`
  paths, so the MCP launch command must use
  `uv run --no-sync --project <ABS_REPO> thyca-discord-mcp`.
  Over stdio the tools appear with the SDK prefix as `discord__<tool>`.
- **Live scope (strict):** guild `1067690340359880724` / channel `1395103735725821982`
  only — no other channel, even in the same guild. Every call enforces the guild scope
  guard on the server.

## V5 auto-reply — deployed backend, operating (plan still in-progress, gaps open)

- **Status:** V5 source reviewed; base `75c5ea0` stays the historical v4
  checkpoint, not the commit ceiling. The owner authorized review, verified
  fixes, and a V5 commit after independent review and passing checks; these
  review/check gates are now satisfied. The plan
  `.agents/plans/v5-auto-reply.md` stays `in-progress`. The V5 backend IS
  deployed and auto-reply is operating now: the exclusive one-time seed is
  COMPLETED (baseline ACK 21 + exactly the two authorized SKIPs, ACK advanced
  to 23), two live owner-triggered turns completed with receipts and controller
  ACK at 31/latest 31, and a controlled native restart preserved state with no
  resubmission. T4 is DONE; T5 deployment + owner happy/recall/alias + restart /
  selfbot / privacy verified; T6 evidence PARTIAL. Live failure-injection gaps
  remain (see Honest limits); the product is NOT marked done.
- **What it is:** always-on auto-reply for ALL humans in the already-approved live
  channel, over the same existing native Thyca SID and audit label
  `discord-main`. Flow: outbox event → FIFO worker → native turn on the existing
  SID → agent-owned MCP `send_message` → receipt + native Trace correlation →
  controller ACK of the safe processed prefix. The worker never sends a Discord
  reply itself (no bridge-owned SDK copy).
- **As checked on the V5 source at doc time:** `uv run pytest` 464 passed
  (parent independently reran: 464 PASS in 13.16 s);
  `uv run ruff check src tests` clean; `ruff format --check` clean (27 files,
  parent diff-check confirms formatted).
  Per-file collected: delivery 74 (73 + 1 new `updated_at` pin), native client
  57, responder 97 (88 + 9 new SID-pinned title-ignored cases), server 65 +
  server-responder 53 (= 118 combined), tasks 19, rest 99. The 455 figure is
  the pre-SID-fix baseline (historical), 454 the pre-cleanup baseline
  (historical), 446 the earlier pre-edge-case baseline (historical).
- **Reviews:** the native client was independently approved earlier (original
  Codex approval kept as history); a strong independent combined review found
  3 findings, all now fixed in-tree with new tests; a fresh authorless review
  returned APPROVE FOR OFFLINE IMPLEMENTATION ONLY with NO findings
  (same-family residual risk disclosed, not hidden). After that, two fresh
  reviewers read all four >1k-line modules plus production callers end to end
  and found 6 MINOR items only (no critical/important, no dead API: the
  `initialize` entry is the needed operator bootstrap, not dead; file size
  alone forced no module extraction). All 6 cleanup fixes are verified locally
  (455 PASS, Ruff clean) — unused `Responder.reconcile` + `busy` property
  removed (internal tick reconciliation still works; public worker surface stays
  `tick`/`close`), one canonical 4-stage mid-flight set, merged redundant GET
  snapshot branch (409-busy kept distinct), unused `_map_http_status` arg
  dropped, `requeue_definitive_busy` now refreshes `updated_at` (created/SIDs/
  aliases preserved); guards (privacy/fsync/FIFO/fingerprint/protected loops)
  intact — but the running backend still executes the pre-cleanup code: this
  request did NOT restart/redeploy the bot, so no native-runtime cleanup claim.
  The final independent cleanup re-review is COMPLETED: findings NONE, all 6
  deltas verified against source/tests, full 455 PASS + Ruff clean — verdict
  scoped APPROVE of cleanup-commit readiness ONLY (not deploy/full
  live-failure; report `/tmp/thyca-v5-cleanup-final-review.txt`, 0600). Same-family residual risk is
  noted, not hidden; the project requires an independent reviewer, not a
  different model family. All runtime/source writing is DONE (no active writer).
  T0-T3 are implemented offline; T4 checks + reviews are DONE. T5 deployment +
  owner happy-path proof is verified (two live turns, receipts, ACK 31, restart
  with no resubmission); T6 evidence is PARTIAL and the plan stays `in-progress`
  — the product is NOT done and the remaining live-failure acceptance gate is
  NOT waived. The exclusive seed/rollout preparation lives in TMP
  only (not committed product source). A reviewer quota limit is not an
  automatic block on all implementation — what is actually required is the
  strong combined review plus live-scope authorization, both tracked in the
  plan.
- **Fixes present in-tree (not planned):** a process-lifetime send latch is set
  BEFORE any uncertainty read/write and rejects retries before intent/SDK when
  disk cannot persist the unknown state; already-committed intent/receipt is
  preserved. A crash loses the memory latch, so this is process fencing only —
  no exactly-once claim. Python 3.10-compatible `sys.exc_info()[1]` is used
  (only the newer runtime was actually executed here); failed idle task
  exceptions are consumed while stop protection and the running path are
  preserved.
- **Deleted source = no reply (source-only; rollout pending):** a source
  confirmed deleted BEFORE any send attempt takes a durable zero-evidence
  `skipped` / `source_deleted` path: no intent, no SDK send, no plain
  fallback, no delivered MID. The agent must then stop ALL replies for that
  source (no retries, no unthreaded/manual fallback; unrelated manual sends
  stay allowed). Existence is re-checked before the LLM POST and again before
  the correlated MCP send. Proof is strict: an exact `fetch_message` GET bound
  to the approved guild/channel/MID raising native NotFound status 404 +
  code 10008; on GET success the returned MID/channel/guild must match (no
  message-object metadata exists on a 404). `SourceUnavailable`,
  permission/403, other 404 codes (e.g. 10003 Unknown Channel), timeouts,
  and 5xx are NOT deletion (pre-POST holds `queued`/`pending` with no
  POST/nonce; pre-intent MCP returns a bounded error with no SDK send). Any
  existing intent/receipt/trace — or an `uncertain`/`completed` stage —
  refuses the skip; partial sends hold `uncertain` with receipts kept and
  further chunks withheld. If the agent never calls MCP send after a
  mid-generation deletion, the controller can hold `uncertain` (no
  always-unblocks-FIFO claim). After any SDK call ALL errors stay
  `uncertain` (hidden SDK retries); no no-reply guarantee once Discord
  accepted a send before deletion. A durable zero-evidence skip wins over
  late native POST/result/snapshot/trace outcomes; cancellation still
  propagates. Storage v1 + new `source_deleted` enum (aliases/unknown fields
  preserved); `MessageReference(fail_if_not_exists=True)` kept; the check is
  a single GET read — no history scan or content processing — needing
  ViewChannel + ReadMessageHistory on the scoped channel. Reply policy only:
  no AI-history/message-body deletion; owner/clone/shared-memory design
  tabled. Checks, dated 2026-10-05: FINAL 503 PASS in 12.78 s (464 baseline +
  39 source-deleted cases: 36 initial + 3 guard), Ruff check clean, 33
  formatted, diff check clean. Both hardenings DONE and checked: common fresh
  post-GET latch + correlated_job guard on both source-present/gone paths;
  persisted `source_deleted` skip/intent-receipt contradiction rejected for the
  new reason only (legacy reasons unchanged); INERT schema mirrored, not wired.
  Author's 3 new regressions failed pre-patch and pass fixed. The 500 checkpoint
  (12.73 s) stays a dated initial-feature historical marker; 464/455 counts stay
  historical. Initial review `0ca0d96d-8efe-46c` (APPROVE of source handling at
  the 500 checkpoint) stays historical; fresh final review `0887b525-8634-4c6`
  is complete: no findings, APPROVE of source handling after fixes only.
  Reviewer reran 39 feature tests and 3 guard regressions, lint/format/diff
  checks and cold imports. No deployment or live acceptance is implied.
  Source-only; rollout pending; no commit or native restart. The seq-66
  job stays `uncertain`, untouched (no autodrop/ACK).
- **Startup gate (read this before `uv run thyca-discord-mcp`):** T4 offline
  sign-off is DONE and does NOT authorize a commit, stop/restart, seed, or live
  traffic. The V5 server
  requires the preexisting native SID mapping and delivery journal and fails
  closed without them — it never auto-creates and never resets. Do not start it
  until (a) T4 review sign-off is recorded and (b) the old stdio writer is
  stopped: single writer only, and stopping it ends the live service, so this
  gate needs explicit operator approval. Cutover already ran — do NOT rerun the
  exclusive initializer, and do NOT rerun the helper dry-run (it rejects when
  delivery state exists). There is no supported "delete the
  journal and restart" fallback and no blind-ACK shortcut.
- **Migration seed (approved, COMPLETED one-time):** exclusive fresh-state
  initialization keying the historical MID index at baseline ACK 21 plus EXACTLY
  two authorized SKIPs — seq 22 / `1556471434610483303` and seq 23 /
  `1556471963957067780` — then guarded ACK 21 → 23. Same native SID preserved
  (no new SID, no pruning); config/auth/mapping preserved. The operator AGREED
  global idle and cutover (prior "asked, NOT replied" claim is superseded).
  Prior hypothesis that the old writer was "foreground with no pidfile" is
  REFUTED: the native pidfile existed and matched the sole native PID/identity,
  and native `--serve --stop` targets that pidfile — before using it, verify the
  file PID is the same owner/process identity (never infer from an argv
  `--daemon` flag, never blindly trust a stale pidfile). Later arrivals were not
  discarded: poll-recovered old-SKIP aliases 24/25 safely ACKed 25 with NO paid
  turn. Two live owner turns then COMPLETED on the same SID with receipts and
  controller ACK 31/latest 31; a second controlled native CLI stop+start
  preserved jobs/receipts/ACK with no resubmission. See plan §6.
- **Honest limits:** the shared native MCP pool carries no caller SID, so there
  is no per-turn ACL and no idempotency; correlation (journal + Trace) is NOT
  authentication and gives no exactly-once guarantee. Owner-profile auto-load is
  accepted GUIDANCE, not a disclosure privilege: UID metadata authenticates,
  names and typed IDs never do. LIVE-other-human triggering is NOT verified until
  an actual second account tests it (an owner-UID test is never faked as that
  proof). Restart/migration must preserve ACK and receipts (no stale-backup
  restore, never delete/restore a stale backup); a normal restart preserves the
  authoritative journal/outbox ACK/receipts. No systemd/daemon migration is
  recommended; the native pidfile controlled source-backed stop/start model
  uses the same existing control footprint. Live gaps still open: actual LIVE
  timeout/cancel/crash/disk-fault NOT injected (covered OFFLINE by the 464
  only); live other-human NOT verified — an accepted alternative (an owner-UID
  test is never faked as that proof), not a mandatory second-account test;
  wrong-guild/DM live testing is OUTSIDE the authorized channel-only/no-DM scope
  (offline coverage only, not a deployment gate); other-bot simply not available
  (NOT run, not an invented gate). The original remaining T5
  failure/crash-uncertainty LIVE gate NOT exercised is what keeps T6 partial;
  no production fault injection/drop is authorized. The concurrent
  independent live-evidence review is COMPLETED: APPROVE of the operational
  cutover + owner happy-path evidence ONLY, with NO confirmed product findings
  (report `/tmp/thyca-v5-live-evidence-review.txt`, 0600). Scope is that happy-path
  proof only — not the full live-failure matrix, not merge permission. No
  permission to fault/crash the running native work or blind-drop a new job.
  Tool contracts: [docs/TOOLS.md](docs/TOOLS.md). Full plan:
  `.agents/plans/v5-auto-reply.md`.

## Modum Jev integration — source complete (never deployed)

- **Status:** Plan `.agents/plans/modum-jev-integration-refactor.md` is
  `done` for the authorized SOURCE work only. Never deployed; source completion
  is not production activation or completion of the separate V5 live plan. T1/T2a/T3/T2b SOURCE gate COMPLETE (full checks + two
  fresh non-author whole reviews, NoFindings, Approve SOURCE only; same-family
  limits disclosed; one scope-breaching report excluded/replaced). T4
  responsibility refactor DONE (parent 1227 PASS 14.15s, Ruff clean / 57
  formatted / lock 59 / diff clean, 134 AST + independent NoFindings
  equivalence). T5 mechanical migration COMPLETE (parent 1235 PASS 14.26s,
  Ruff clean / 57 formatted / lock 59 / diff clean; offline wheel holds only
  28 `modum` files; console `thyca-discord-mcp=modum.__main__:main`; old
  `src/thyca_discord_mcp` absent; DRY imports, no `main()` invoke; 56-file
  Py3.10 GRAMMAR only, runtime 3.14 verified). T6 candidate audit is complete:
  no additional genuine removal justified; public compatibility and safety
  contracts retained. T7 package/routing/privacy review approved with no
  findings; recovery review found one Minor late-skip tick-status gap. Parent
  reproduced and fixed it (durable skip/ACK were already safe); eight portable
  cases pass. Independent core follow-up Approve and whole guidance/package
  follow-up Approve NoFindings; no open findings. Fresh non-author same-family
  review limits remain disclosed. No merge/live/paid/deploy/restart/commit; seq-66
  `uncertain` untouched; no current production attestation.
- **Final source checks (post-T7-fix; independently approved):** offline full pytest
  1243 PASS 13.42s; Ruff clean / 57 formatted / lock 59 / diff clean;
  offline wheel rebuilt. Original assertions preserved; only the recovery
  late-skip guard and eight additional cases changed since the T5 checkpoint.
- **Historical T5 checkpoint:** `uv run pytest` 1235 PASS 14.26s; `uv run ruff check` clean; `ruff format --check` clean (57 files);
  `uv lock --check` pass (59 packages); `git diff --check` clean. Base
  `main@c8b6f94` plus uncommitted SID/title-ignored + deleted-source + Modum
  source. `src/modum` owns all 28 runtime files (`__init__` 0.1.0 minimal, no
  side effects; `import modum.*`); `src/thyca_discord_mcp` ABSENT, no compat
  alias. Hatch wheel `packages=["src/modum"]`; console
  `thyca-discord-mcp=modum.__main__:main` (external dash CLI/distribution
  preserved; Python namespace is `modum`). Tool 14/config/SID/state/audit-label
  guards preserved. Keys never read; no live SDK/API call. Earlier
  1227/1184/734/568 stay dated history. V4/V5 paths above are dated history;
  active current source is `src/modum` per this section.
- **Binding order (not negotiable):** integration gate COMPLETE, T4 DONE, T5
  migration COMPLETE (no old dir left), THEN genuine duplicate removal only
  (T6 audit complete, no additional source deletion justified). Refactor/rename/removal
  were never unlocked by adapter-only work.
- **Business R1–R3 (delegated, pending user review):** the user delegated ALL
  unresolved decisions to the assistant; parent selected most-reasonable picks.
  `respond`/`observe` from Jev (a stored `need_context` decision from older
  records still validates for delivery compat but is never emitted anymore).
  `need_context` routes Thyca to scoped additional context or clarification, no
  guessing. Max 20 whole source-adjacent messages including source + priority
  exact reply parent, same approved guild/channel, 4000-char shared prompt fit,
  no truncation, mandatory-pair oversize fails closed. Per-call Jev spend has
  NO daily stop (removed by user order); spend is reported, NOT capped, and
  NOT a provider cap; lazy key / single call / no SDK retry, unknown billing
  halts new paid attempts, same MID/fingerprint cached, changed context held
  with no automatic rebill. Numeric values are assistant-most-reasonable, NOT
  user-personally-picked (20). No new manual-enable, owner-only, or mention
  threshold product mode.
- **Config (see `.env.example`):** `TYPESAFE_API_KEY` required for the real
  responder (lazy SecretStr, never log/echo); `JEV_STATE_PATH` optional, derived
  BESIDE the configured `DELIVERY_STATE_PATH` (`jev_state.json`); lexical
  `../` and abs-vs-relative collision guard against all 5 owned paths before
  any I/O. Stores refuse symlink paths; hardlink aliases are not detected
  by this lexical guard. No all-physical-alias safety is claimed. Per-call Jev
  spend has no daily stop; `CONVERSATION_MAX_MESSAGES` strict 1..20 default 20. Pinned SDK
  `typesafe-sdk==0.7.2`, model `jev-1.13.0`, one call, retry 0, timeout 10 s.
  Ledger metadata only, file 0600, new dirs 0700; known Usage is retained BEFORE
  failures as a lower bound; unknown/started halts NEW attempts across
  restart/day rollover; exact decided cache returns with no rebill. Root owns
  the durable `ledger_id` marker: constructor bind + fresh compare before every
  classify detects loss/changed SID-compatible ID and fails closed with no
  autoinit/reset; operator resolves, no invented automatic repair steps.
- **Context/routing (source-verified):** canonical SDK names/UIDs/time/reply
  relations with whole bodies, same guild/default channel; newest ≤20
  source-adjacent messages before the source (SDK `oldest_first=False` paging);
  source + exact visible parent mandatory, optional whole oldest drop to fit the
  ACTUAL both `respond`/`need_context` 4000-CHAR prompts (not tokens),
  mandatory oversize holds with no truncate. Profiles/`USER.md`/native private
  history/key/errors never enter the Jev payload; trusted SDK UID + `whoami`
  authenticate, Jev diagnostic/name/text claims never grant permission; all
  humans may chat. Vague routed `TASK` wording was replaced during integration. The same frozen
  projection renders to native so perception is shared; `respond` means native
  agent ONLY sends threaded correlated (controller/Jev send no copied text);
  `observe` is positive durable Jev zero-post/zero-evidence ACK; `need_context`
  means the agent reads additional approved scoped context/clarifies OR records
  explicit `record_no_reply` (`no_reply_needed`|`missing_context`) then zero ALL
  sends with exact SID/index/single user-input fingerprint/error-free/nonce/
  reason plus unique ≥1 confirmed trace, declarations → observed ACK, never
  receipt-absence inference. Declaration load requires NON-NULL fingerprint for
  new fields, never legacy-null contradictions; old jobs missing optional new
  fields stay preserved. Exact source GET 404+10008 deletion takes no unthreaded
  fallback, unavailable holds; seq-66 stays `uncertain` (1 intent, 0 receipts)
  untouched; original live-fault gates not completed. Correlation nonce is NOT
  caller ACL in the shared MCP pool; no exactly-once, account-cap, or
  live-quality claim. Current surface is 14 tools (12 preserved +
  `read_context` + `record_no_reply`); contracts in [docs/TOOLS.md](docs/TOOLS.md).
- **RunGate (do not start current source):** current source is a different
  Python namespace (`modum`) though the external CLI name is unchanged; native
  processes run old cached modules with no hot reload; deployment is separate
  with no operational approval; no cold start/restart. `USER.md`/profiles never
  export.

## Prerequisites

- Python 3.10+ and [uv](https://docs.astral.sh/uv/).
- A Discord application + bot token ([portal](https://discord.com/developers/applications)).
  Bot invited to the guild with Administrator.
- Privileged intents enabled on the portal: **Message Content** and **Server Members**.
- IDs (Discord Settings -> Advanced -> Developer Mode, right-click -> Copy ID):
  owner user ID, server ID, default channel ID.

## Setup

```bash
cd ~/Projects/thyca-discord-mcp
uv sync --extra dev
cp .env.example .env   # fill DISCORD_TOKEN, TYPESAFE_API_KEY and verified scope IDs
```

V5 auto-reply tuning (6 optional non-secret fields, documented in `.env.example`:
base URL, session-mapping path, delivery-state path, responder tick default 1.0 s,
request timeout default 900.0 s, recovery poll floor 30 s / default 30.0 s) needs
no secrets. The V5 server fails closed when the mapping/journal are missing — see
the V5 startup gate above, not a deletion fallback.

Modum Jev tuning (3 fields, documented in `.env.example`: `TYPESAFE_API_KEY`
secret required for the real responder, plus `JEV_STATE_PATH` optional derived
beside delivery, `CONVERSATION_MAX_MESSAGES`
default 20) is source-complete, not deployed — see the Modum section above. The
ledger fails closed on loss/mismatch with no autoinit/reset.

## Run

```bash
uv run thyca-discord-mcp        # MCP over stdio (bot starts in the same loop)
uv run pytest                   # offline: no live Discord/Thyca/provider (synthetic local HTTP loopback allowed)
uv run ruff check src tests     # lint
uv run ruff format --check src tests  # format gate
```

The V5 tree adds a startup gate before that first command: `uv run
thyca-discord-mcp` now requires the preexisting native SID mapping and delivery
journal (missing/corrupt fails closed, never auto-creates). Start it only after
T4 review sign-off with the old stdio writer stopped (single-writer rule) — see
the V5 section above.

The MCP server speaks stdio, so an MCP client launches it as a subprocess.
FastMCP drives the asyncio loop; the lifespan hook `await`s `bot.login()`, then runs
`bot.connect()` in the background and races it against `wait_until_ready()`, so tools
`await` discord.py directly with no thread bridge.

## Tools (current source: 14 total; V4 historical snapshot was 12)

| Tool | What it does |
|---|---|
| `guild_info` | id, name, member/channel counts of the scoped guild |
| `list_text_channels` | id/name/topic/position of text channels |
| `send_message` | send to a channel (default if omitted); refuses out-of-guild targets |
| `whoami` | classify a UID as self/owner/other + current display name |
| `list_members` | members as uid/display_name/kind (max 500) |
| `list_tasks` / `start_task` / `stop_task` | manage named background tasks |
| `add_poll_task` | poll a channel into the outbox every ≥30s (capture-only) |
| `remove_poll_task` | stop + remove a poll task (refuses `heartbeat`) |
| `read_outbox` | pull captured messages oldest-first (+ `acked`/`latest`) |
| `ack_outbox` | mark entries up to a seq as read (rejects beyond latest) |
| `read_context` | read approved-channel context for a correlated turn (≤20, before source) |
| `record_no_reply` | declare no reply for a correlated `need_context` turn (nonterminal) |

v2 (messages/channels) and v3 (members/roles) in [docs/TOOLS.md](docs/TOOLS.md) stay
proposed; v4 (outbox + poll tasks) is implemented. The V4 historical snapshot
above was 12 tools; the current source is 14 tools (12 preserved + `read_context`
and `record_no_reply` for Modum `need_context`, source-complete, not deployed). No
v2/v3 mutation/admin/destructive behavior is implemented or tested (existing
read-only channel/member listings were exercised in the native 12-tool E2E).
V5 (in-progress plan, V5 source, backend deployed and operating) adds no tool names beyond its 12 — `send_message`
gains optional `source_message_id` (StrictInt) + `job_ref` as a both-or-neither pair
for agent-owned auto replies (unpaired manual sends work exactly as before and are
never job-attached), `ack_outbox` additionally refuses any seq beyond the
controller's safe processed prefix, and the `auto-reply` / `auto-reply-recovery`
loops are protected while `heartbeat` may stop but may never be removed. When MCP tool descriptions are not enough for an agent, load the full
`docs/TOOLS.md` into its context.

## Outbox (agent pull protocol, no LLM in server — V4 deployed writer)

- **Scope:** this section describes the V4 deployed writer. The V5 tree (same
  V5 source, backend now deployed and operating) adds a FIFO worker that automatically POSTs a native turn per eligible event with
  controller-guarded ACK (see "V5 auto-reply — in progress" above); capture itself
  still contains no LLM, and the server still sends no MCP push.

- The server only **captures**: a realtime `on_message` listener (watched channels,
  bot's own messages dropped) plus periodic poll tasks (bot's own kept, tagged `self`).
  There is no server→client push and no server-side summary — the agent pulls with
  `read_outbox` and summarizes itself.
- Protocol: `read_outbox` → process entries in `seq` order → `ack_outbox` the highest
  processed seq. **Never ack before processing.** Dedupe by `message_id` (realtime and
  poll can overlap). Empty `content` means a non-text message (embed/attachment) — do
  not invent content.
- Poll cursors persist in `outbox_state.json`, so re-adding a task resumes where it
  left off. Task definitions are memory-only (lost on restart, not restored).
- Single writer: one process / one asyncio loop. If state and log ever disagree
  (state loss, corruption), startup refuses to boot and ack/poll paths fail loudly
  and refuse to write — state is never silently reset. Authoritative ACK, journal,
  and receipts are preserved: restore ONLY from a matching valid backup PLUS
  external delivery reconciliation and explicit operator approval — never a stale
  ACK rewind, never deleting state. (V4 corruption still fails closed, unchanged.)

A `heartbeat` background task snapshots the member count to the audit log every
5 minutes as a liveness signal. All background tasks are native `discord.ext.tasks`
loops (no homemade scheduler); poll ticks use native `channel.history` paging.

## Safety notes

- This bot is Administrator: tools run without confirm.
  Check `audit.jsonl` to trace what happened.
- `DISCORD_TOKEN` lives only in local `.env`. Regenerate it if it ever leaks
  (portal -> Bot -> Reset Token). It is never written to memory, logs, or audit.
- `USER.md` (profile): the owner accepts the auto-loaded owner profile in agent
  context, but the agent must not manually read/write the real profile file unless
  the owner explicitly asks; report every access. A present profile never means the
  current Discord sender is the owner — verify with `whoami(author_uid)`.
