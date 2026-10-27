---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

# PLAN server/core.py: extract the correlated-send cluster

> Owner flagged the two remaining large files (store.py 1437 —
> handled by store-package.md — and server/core.py 1157). The earlier
> audit advised keeping core together because the correlated tools are
> closure- and patch-bound; the admin split has since proven the
> defusing pattern (core-defined closures passed into a registrar), so
> this extraction is now low-risk. Base: main@68c377b + store package.

## Move to `src/server/correlated.py`

`register_correlated_tools(mcp, *, settings, bot_fn, ok_fn, fail_fn,
deny_fn, get_responder_fn, identity_policy)` containing:

- Send chokepoint + mention guard (`_ALLOWED_MENTIONS`, `_discord_send`,
  `_fail_job`)
- Correlated preamble (`_correlated_base_args`, `_require_correlated`)
  and `_correlated_send`; `_correlated_deny` stays in core as the audit
  closure passed through `deny_fn`.
- Tools `send_message` (whole tool; its manual branch uses the same
  chokepoint), `record_no_reply`, `read_context` — registered
  consecutively at the exact position `send_message` occupies today,
  so MCP registration order and the golden fixture stay byte-identical.

## Stays in core.py

Factory + audit closures (`audit/_ok/_fail`), `_bot`, lifespan (bot
construction — the `server.core.Responder`/`build_bot` patch sites
tests rely on), identity/read tools (`guild_info`, `list_text_channels`,
`whoami`, `list_members`), admin registrar call, task + outbox tools.

## Patch-safety rules (the audit's trap, defused)

- Anything tests patch on `server.core` (`Responder`, `build_bot`,
  `append_audit`) must remain resolved through core's namespace at call
  time: `_ok/_fail` stay defined in core and are passed in; the
  responder is reached via the passed `get_responder_fn` closure over
  core's state dict. correlated.py never imports server.core and never
  constructs Responder/Store itself except through what core already
  does today — the writer greps the correlated block for such names
  first and reports how each is threaded.
- Pure move otherwise: bodies verbatim, golden byte-identical (do NOT
  regenerate), 899 tests unchanged, same validation battery as prior
  splits. Independent review after (this is the send path — delivery
  semantics must not drift).

## Completion evidence

Implemented in retained writer run 728c67c8: core.py 1157 -> 619 lines,
new correlated.py 586 lines. Audit closures and bot/responder state stay
in core; `deny_fn` is the additional collaborator required by the actual
code. Callback timing and tool registration order are unchanged.

Lead AST comparison: all eight moved functions, including signatures
and decorators, match HEAD after callback-name/state-lookup substitution.
Golden list_tools digest remains
`a96086f99f42bf03af4c643ff451015d558b30bffee3c0515dcd00add5a561ed`;
no fixtures regenerated. The original 899 tests remain unchanged;
combined suite with the 11 new store checks passes all 910 tests.
Lint/format, diff check, build and isolated wheel import smoke passed.
Independent read-only review 1e785fec: Approve, no confirmed defects.
Current tool/context docs updated. Not committed or restarted; no live
Discord behavior is claimed by these offline checks.
