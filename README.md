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

## Integration status — verified (supersedes earlier PENDING)

- **History preserved:** this section previously said "PENDING, not verified; no
  live Discord run yet; review-pending". That claim is SUPERSEDED by the verified
  facts below. The plan keeps the full review history.
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
- **Not implemented (do not claim):** no server notifications/push, no autonomous
  watcher-to-LLM bridge — capturing never launches LLM turns; the consumer pulls with
  `read_outbox` and acks after processing. Current scope is the 12 implemented tools only;
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
  must pull with `read_outbox`.
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

## Run

```bash
uv run thyca-discord-mcp        # MCP over stdio (bot starts in the same loop)
uv run pytest                   # unit tests (no network)
uv run ruff check src tests     # lint
```

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
(existing read-only channel/member listings were exercised in the native 12-tool E2E). When MCP tool descriptions are not enough for an agent, load the full
`docs/TOOLS.md` into its context.

## Outbox (agent pull protocol, no LLM in server)

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
  and refuse to write — state is never silently reset. Restore `outbox_state.json`
  from backup instead.

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
