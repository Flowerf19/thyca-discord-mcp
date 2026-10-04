# thyca-discord-mcp

MCP server around discord.py for single-agent Discord guild management with multi-task support.

## Accepted requirements

1. **Full admin** on one guild (the owner's server). Owner calls run immediately, no confirm step.
   Every action is recorded in `audit.jsonl` as the safety net.
2. **Identity**: every UID is classified as `self` / `owner` / `other`.
   UID is the stable key; display name is resolved at runtime for humans and logs.
3. **Session & memory**: all Discord activity shares one fixed session (`discord-main`).
   The user profile (`USER.md`) is off-limits unless the owner explicitly asks.

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
FastMCP drives the asyncio loop; the lifespan hook starts `bot.start()` in that
same loop, so tools `await` discord.py directly with no thread bridge.

## Tools (v1)

| Tool | What it does |
|---|---|
| `guild_info` | id, name, member/channel counts of the scoped guild |
| `list_text_channels` | id/name/topic/position of text channels |
| `send_message` | send to a channel (default if omitted); refuses out-of-guild targets |
| `whoami` | classify a UID as self/owner/other + current display name |
| `list_members` | members as uid/display_name/kind (max 500) |
| `list_tasks` / `start_task` / `stop_task` | manage named background tasks |

Planned tools are drafted in [docs/TOOLS.md](docs/TOOLS.md) (proposal, not accepted yet).

A `heartbeat` background task snapshots the member count to the audit log every
5 minutes as a liveness signal.

## Safety notes

- This bot is Administrator: destructive tools (planned in v2) run without confirm.
  Check `audit.jsonl` to trace what happened.
- `DISCORD_TOKEN` lives only in local `.env`. Regenerate it if it ever leaks
  (portal -> Bot -> Reset Token). It is never written to memory, logs, or audit.
