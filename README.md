# thyca-discord-mcp

MCP server around discord.py for single-agent Discord guild management with multi-task support.

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
  `discord-main`. Reuse the ONE existing native generated session ID titled `discord-main`
  via the private mapping `~/.thyca/discord_session.json` (0600) — never a new session
  per message. Private metadata-only proof copies live in
  `~/.thyca/discord-integration-evidence/` (0700/0600, never committed).
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
- **As checked on the V5 source at doc time:** `uv run pytest` 455 passed
  (parent independently reran: 455 PASS in ~11.4 s);
  `uv run ruff check src tests` clean; `ruff format --check` clean (22 files,
  parent diff-check confirms formatted).
  Per-file collected: delivery 74 (73 + 1 new `updated_at` pin), native client
  57, responder 88 (same count: the old 401/503 case was expanded to
  401/503/409), server 65 + server-responder 53 (= 118 combined), tasks 19,
  rest 99. The 454 figure is the pre-cleanup baseline (historical), 446 the
  earlier pre-edge-case baseline (historical).
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
  timeout/cancel/crash/disk-fault NOT injected (covered OFFLINE by the 455
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
cp .env.example .env   # then fill in DISCORD_TOKEN (IDs are pre-filled)
```

V5 auto-reply tuning (6 optional non-secret fields, documented in `.env.example`:
base URL, session-mapping path, delivery-state path, responder tick default 1.0 s,
request timeout default 900.0 s, recovery poll floor 30 s / default 30.0 s) needs
no secrets. The V5 server fails closed when the mapping/journal are missing — see
the V5 startup gate above, not a deletion fallback.

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

## Tools (v1 + v4: 12 total)

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

v2 (messages/channels) and v3 (members/roles) in [docs/TOOLS.md](docs/TOOLS.md) stay
proposed; v4 (outbox + poll tasks) is implemented. That is the full current surface —
12 tools only; no v2/v3 mutation/admin/destructive behavior is implemented or tested
(existing read-only channel/member listings were exercised in the native 12-tool E2E).
V5 (in-progress plan, V5 source, backend deployed and operating) adds no tool names — still 12 tools: `send_message`
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
