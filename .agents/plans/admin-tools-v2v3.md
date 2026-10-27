---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

# PLAN admin tools v2+v3: full guild management surface

> Owner approved 2026-10-27: implement the complete proposed v2/v3 set
> (docs/TOOLS.md). Capability is decided by the bot's server role and the
> owner — code does not pre-gate permissions; Discord enforces and
> `discord.Forbidden` surfaces as a bounded error. Base: `main@8cf7008`,
> 844 tests, 14 tools.

## Scope (17 new tools; 14 -> 31)

Guild-scoped (configured `guild_id` only, as every tool today). Admin
tools may target any channel/member/role **inside that guild** — server
management is the point; the approved-channel restriction stays only on
capture/auto-reply paths. Owner authority follows the existing identity
policy (metadata UID + `whoami`; agent-facing text in descriptions).
No confirm step (accepted requirement #1). Every call audited.

v2 — messages & channels:
- `read_history(channel_id?, limit=20 (1..100))`: newest-first,
  `{message_id, author_uid, author_name, kind, content, timestamp,
  reply_to_message_id}` via `channel.history`.
- `edit_message(channel_id, message_id, content)`: `Message.edit`;
  Discord enforces own-message-only.
- `delete_message(channel_id, message_id)`: `Message.delete`.
- `purge(channel_id, limit=20 (1..100))`: `TextChannel.purge(bulk=True)`;
  returns deleted **count only**. (`channel_id` REQUIRED here and for
  `delete_channel` — broad destruction must name its channel; fixed in
  the review round below. `edit_message`/`delete_message` keep the
  default-channel convenience: the `message_id` names the target,
  consistent with `send_message` since v1 — lead acceptance recorded
  2026-10-27.)
- `create_text_channel(name, category_id?, topic?)`,
  `create_voice_channel(name, category_id?)`: `Guild.create_*`;
  category must resolve to a `CategoryChannel` in the guild.
- `delete_channel(channel_id)`: text/voice/category in the guild
  (forum/stage refused; review-round restriction).

v3 — members & roles:
- `get_member(uid)`: `{uid, display_name, nick, kind, roles:[{id,name,
  position}], joined_at, bot}`.
- `set_nick(uid, nick="")`: empty clears (`Member.edit(nick=None)`).
- `add_role(uid, role_id)` / `remove_role(uid, role_id)`:
  `guild.get_role` must resolve, then `Member.add/remove_roles`.
- `create_role(name, permissions=0 (int), colour? (int), mentionable=False)`:
  `Permissions(value)`, `Colour(value)` (resolves the old TBD: int form).
- `list_roles`: `{id, name, position, member_count, mentionable}` from
  `guild.roles`.
- `timeout(uid, minutes (0 clears), reason?)`: `Member.timeout`.
- `kick(uid, reason?)`, `ban(uid, reason?)` (`delete_message_seconds=0`),
  `unban(uid, reason?)` (`Guild.unban(Object(uid))`).

## Rules for the writer

- Follow the existing simple-tool pattern (typed resolution via
  `get_scoped_guild`/`_resolve_text_channel` where applicable, audit via
  `_ok`/`_fail`); member resolution reuses cache→fetch cascade.
  `identity_policy` text goes into every mutating tool's description.
- Audit args are metadata only (ids, name lengths/counts, reason length) —
  **never message content** (this supersedes the old TOOLS.md
  "content snippet" note; purge/delete record ids + counts only).
- `discord.Forbidden`/`NotFound` propagate through `_fail` like other
  simple tools (audited, then raised) — no new error taxonomy, no
  permission pre-checks beyond scope/type resolution.
- Tests per tool with the existing fake patterns (+ real payload objects
  where the suite already does): happy path, out-of-guild refusal,
  unresolved category/role/member, `Forbidden` surfaces, purge count.
  Regenerate the golden `list_tools` fixture deliberately; summarize the
  diff (14 -> 31, names only + descriptions).
- Docs: TOOLS.md v2/v3 sections become "implemented" with exact
  signatures/returns; README tool count/table (31); plan-file discipline
  as before. Golden prompts untouched.

## Checks

Per writer report: `uv run pytest -q` (844 + new), golden, ruff
check+format, `git diff --check`, `uv build`. Independent read-only
review afterward (destructive surface = security-sensitive per project
rules) before merge-ready. Deploy/commit separately approved as usual.

## Review round (2026-10-27)

Implemented by writer `2e7322d8` (889 passed, golden 14->31 with the 14
existing entries byte-identical; one plan-vs-library correction:
`Permissions(...)` is positional in discord.py 2.7.1). Independent
review (`28bb6ce1`): **Approve with fixes**, no scope escape. Fixes
resumed to the writer: required `channel_id` for purge/delete_channel
(above); forum/stage restriction; `ban` works for non-member IDs via
`guild.ban(Object(uid))`; add/remove_role return a fresh
`fetch_member` payload; `StrictInt` on all new-tool ID params;
Forbidden-path tests for kick/ban/purge/delete_channel + non-bot-author
edit + fetch-fallback thread refusal; README count.

Fix round complete (writer `ca196d86`); lead gates re-run: **899
passed** (844 + 55 admin tests), golden regenerated (only purge/
delete_channel/ban entries changed; other 28 byte-identical), ruff
check+format clean, `git diff --check` clean. **This plan is DONE for
source implementation** (31 tools). Not committed or deployed; live
service still runs the pre-simplify build and the seeded `state.db`
waits for the owner's Thyca start (see simplify plan §6 deploy prep).
