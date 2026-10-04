# MCP tools — DRAFT proposal (not accepted yet)

Scope guard (all tools): operate only on the configured `GUILD_ID`.
Every call is audited to `audit.jsonl`. Destructive tools run without confirm
per the accepted "full admin" requirement.

## v1 — implemented

| Tool | Args | Returns |
|---|---|---|
| `guild_info` | — | id, name, member_count, channel counts |
| `list_text_channels` | — | id/name/topic/position list |
| `send_message` | `content`, `channel_id?` (default channel) | message_id, jump_url |
| `whoami` | `uid` | uid, display_name, kind (self/owner/other) |
| `list_members` | `limit?` (≤500) | uid/display_name/kind list |
| `list_tasks` | — | name/interval/running/current_loop list |
| `start_task` / `stop_task` | `name` | task status |

Background: `heartbeat` loop snapshots member count every 5 min.

## v2 — proposed: messages & channels

| Tool | Args | Discord perm | Note |
|---|---|---|---|
| `read_history` | `channel_id?`, `limit?` (≤100) | Read Messages | newest-first, with author uid/kind + timestamps |
| `edit_message` | `channel_id`, `message_id`, `content` | — (own msgs) | bot can only edit its own messages |
| `delete_message` | `channel_id`, `message_id` | Manage Messages | single delete; audit content snippet |
| `purge` | `channel_id`, `limit?` | Manage Messages | bulk delete; record count only, not contents |
| `create_text_channel` | `name`, `category_id?`, `topic?` | Manage Channels | |
| `delete_channel` | `channel_id` | Manage Channels | destructive; audit name + id |
| `create_voice_channel` | `name`, `category_id?` | Manage Channels | |

## v3 — proposed: members & roles

| Tool | Args | Discord perm | Note |
|---|---|---|---|
| `get_member` | `uid` | — | nick, roles, joined_at, kind |
| `set_nick` | `uid`, `nick?` | Manage Nicknames | empty clears |
| `add_role` / `remove_role` | `uid`, `role_id` | Manage Roles | bot role must outrank target |
| `create_role` | `name`, `permissions?`, `colour?`, `mentionable?` | Manage Roles | permissions as int or names — TBD |
| `list_roles` | — | — | id/name/position/members count |
| `timeout` | `uid`, `minutes` (0 clears) | Moderate Members | |
| `kick` | `uid`, `reason?` | Kick Members | destructive |
| `ban` / `unban` | `uid`, `reason?` / `uid` | Ban Members | destructive |

## v4 — proposed: tasks & events

| Tool | Note |
|---|---|
| `add_poll_task` | register a periodic "read channel X, summarize to owner" loop — needs agent callback design (TBD) |
| `on_message_digest` | event subscription is NOT request/response; needs outbox/queue design (TBD) |

Open questions for v4: how does a background task reach the agent (MCP has no
server→client push over stdio)? Options: digest-on-demand tool, or file outbox
the agent polls. Decide before implementing.
