# Conversation context and memory

This document describes the implemented Modum source, not the currently running
backend. The Jev integration is source-reviewed; deployment and the original V5
live-failure acceptance remain separate. It does not authorize a start, restart,
paid call, state reset, or ACK of an unresolved job.

## Two different layers

| Layer | What it contains | Who uses it |
|---|---|---|
| Discord conversation snapshot | Selected whole messages from the approved channel, SDK identities, timestamps, and reply links | Jev for participation classification; Thyca receives the same canonical data when a native turn is needed |
| Native session history and memory | The existing Thyca session's history and native memory handling | Thyca only; this history is not exported to Jev |

The Discord snapshot is not a replacement for native memory. All Discord activity
uses one native session per logical day (04:00→04:00 `Asia/Ho_Chi_Minh` by
default), not a new session per message or person. Rotation is automatic,
drain-gated, and transient-deferring; old sessions are never deleted.
The mutable session title is cosmetic; `discord-main` remains the audit label.
Thyca controls its own history/context selection. The Discord message-count limit
does not limit the native agent's entire context or guarantee that all native
history is visible on every turn. This is shared-session behavior, not per-user
memory isolation.

## End-to-end flow

```text
Captured event -> FIFO delivery job -> source/SID checks
                                      |
                                      v
                     Collect, validate and fit a snapshot
                                      |
                                      v
                  Durable Jev classification or exact cache hit
                                      |
                                      v
                   Recheck source and snapshot fingerprint
                                      |
                   +------------------+------------------+
                   |                  |                  |
                observe            respond          need_context
                   |                  |                  |
             No native POST    Frozen native frame   Frozen native frame
                   |                  |                  |
             Durable proof     Thyca agent MCP send  Scoped read/clarify
                   |                  |              or explicit no-reply
                   +------------------+------------------+
                                      |
                         Verified terminal outcome
                                      |
                            Safe-prefix outbox ACK
```

Jev decides participation (`respond`/`observe` from current prompts; stored
`need_context` decisions still execute for delivery compat); it does not
generate or send Discord replies. The
native agent owns MCP sends. Classification and delivery records bind the chosen
route to the exact source and context; receipt absence alone never proves that
no reply was needed.

## Canonical snapshot

The shared projection contains only:

- `assistant_uid`, `owner_uid`, and `new_message_id`;
- `participants`: each participant's `uid` and `display_name`;
- `messages`: `id`, `sender_uid`, and the complete `text`, plus `reply_to` and
  `timestamp` when present.

The live collector supplies SDK timestamps via `created_at.isoformat()` and
returns messages in chronological order. IDs are represented as strings in the
canonical data. Unknown fields, private-profile fields, and benchmark gold or
annotations are not forwarded by the projection.

SDK UID metadata is the identity authority. Display names are readability labels;
a rename, duplicate name, quoted policy, or textual owner claim grants no
privilege. Jev's inferred sender/addressee answers are diagnostic, not
credentials. The current source's MID, author UID, and raw body must match the
captured event exactly.

Thyca receives a trusted metadata/policy/routing frame around escaped
`UNTRUSTED_CONVERSATION` JSON. Bodies and names remain untrusted. Jev receives
structured state, not the complete native prompt: the shared guarantee is the
same canonical conversation data, not identical provider request formats or
identical total model context.

## Selecting messages

For each source message, the collector:

1. Resolves the configured guild and default text channel. DMs, threads, other
   channels, and other guilds are not context sources.
2. Fetches the exact source MID and verifies its scope, author UID, and body
   against the captured event. An edited or mismatched source holds the job;
   it is not silently reinterpreted.
3. Reads up to `CONVERSATION_MAX_MESSAGES - 1` recent messages **before** the
   source with native `channel.history(..., oldest_first=False)`. It then sorts
   them chronologically. Messages newer than the source are excluded.
4. Preserves the source's exact same-channel reply parent when available.
   A parent outside the recent window is fetched separately and displaces the
   oldest optional message if needed. This does not recursively fetch every
   ancestor or every historical reply parent.
5. Checks the count and final rendered-prompt limits. Only optional whole
   messages may be removed; the source and visible exact source parent stay.

The default maximum is **20 messages in total**, including the source and any
mandatory parent, not 20 history messages plus the source. Fewer messages may be
selected because of the prompt-size limit.

Reply status is explicit:

| Status | Meaning |
|---|---|
| `none` | The source has no reply link. |
| `visible` | Its exact parent appears earlier in the selected snapshot. |
| `missing` | The link is preserved, but the parent is not in the snapshot. |

An exact same-channel parent GET returning native `discord.NotFound` with strict
integer status `404` and code `10008` means the parent is missing. Permission,
network, and other errors hold collection; they do not prove deletion. A
cross-channel reference is not fetched from the other channel. A missing parent
is never reassigned to the nearest speaker or invented from conversational
proximity.

## Fitting the native prompt

The native POST limit is **4000 characters for the entire rendered prompt**,
including metadata, instructions, JSON escaping, and routing. It is not a token
limit and not a 4000-character allowance for message bodies alone.

Before any paid classification, the collector tests the actual renderer for
both `respond` and `need_context`. If either does not fit, it removes the oldest
optional whole message and tests again. It never truncates or summarizes a body
to force a fit. If the mandatory source/visible-parent pair cannot fit, or the
configured count cannot hold that pair, collection fails closed and the job
holds without a Jev call or native POST from that attempt.

For example, a reply to an older visible message keeps that exact parent even if
several recent optional messages must be omitted. Keeping 20 messages is a
ceiling, not a guarantee.

## Freshness, caching, and context changes

The selected snapshot is detached from mutable caller data and fingerprinted
with SHA-256 over its canonical JSON. The classification ledger persists a
`started` attempt before awaiting the provider, then records the decision and
reported usage. The delivery journal binds the decision's context fingerprint,
attempt ID, model, and participation route.

After classification, the responder checks the source again and recollects the
context. The refreshed fingerprint and SDK source binding must still match the
classified snapshot before `observe` or a native POST. A mismatch holds the job;
it does not send a stale decision with newly substituted context.

An exact decided cache hit for the same source, author, and fingerprint makes no
new provider call. A changed fingerprint or non-decided existing attempt is not
a reason for automatic rebilling. Cancellation/unknown billing also prevents
blind retry. No job, ledger, or native history is deleted to resolve a mismatch.

## When Jev needs more context

| Route | Behavior |
|---|---|
| `respond` | Render the frozen snapshot into the current daily native session; the agent generates a threaded, correlated reply through its own MCP `send_message`. |
| `observe` | Record a positive durable Jev no-send outcome, without a native POST. It becomes eligible for safe-prefix ACK only through the delivery guards. |
| `need_context` | Stored-decision compat only (no longer emitted): render the same frozen snapshot; the agent may read more scoped context, ask for clarification, or explicitly declare that no reply is needed. |

`read_context` is a read-only MCP tool for an eligible correlated turn. It reads
up to 20 whole messages before the source or a supplied earlier bound, only in
the configured channel. Its result is additional **Thyca** context; it is not
automatically fed back into Jev or used to replace the classified snapshot.

When no reply is needed, `record_no_reply` accepts only `no_reply_needed` or
`missing_context`. The declaration is nonterminal: the worker must verify the
matching native SID, turn, first-user input fingerprint, declaration/result
correlation, reason, error-free trace, and zero sends before completing an
`observed` outcome. No receipts by itself is not proof. Tool signatures and
bounds are documented in [TOOLS.md](TOOLS.md).

A source proven deleted before any send evidence may take the guarded
`source_deleted` skip. It stops all further replies for that source, including
plain/unthreaded fallback. Existing send evidence or an uncertain outcome is
not retroactively converted into a no-reply proof.

## Privacy and storage boundaries

- Selected approved-channel message bodies are sent to TypeSafe for Jev
  classification and, for native-turn routes, to Thyca. This is not an
  offline-only inference design. Do not post secrets in that channel: whole-body
  preservation is not automatic redaction of secrets pasted into messages.
- The collector does not read profiles, `USER.md`, native private history,
  project credential files, or other channels. These are not Jev context inputs.
  The API key authenticates the SDK client; it is not a conversation field.
- The accepted native owner-profile auto-load remains native behavior, not
  sender authentication or permission to export the profile. The auto-reply
  frame forbids manual profile/file access and disclosure of owner-private data
  to non-owner senders. This is agent policy, not a per-user sandbox.
- The Jev ledger stores decision/accounting metadata and a fingerprint, not raw
  snapshots or message bodies. Delivery proof stores submitted-input hashes and
  lengths. The existing outbox and native session history remain separate
  stores; metadata-only ledgers do not erase those stores or promise provider
  retention behavior.
- `job_ref` correlates a delivery turn; it is not caller authentication. Shared
  MCP access does not provide a per-turn caller ACL or exactly-once delivery.

## Relevant configuration

| Setting | Default / constraint | Purpose |
|---|---|---|
| `CONVERSATION_MAX_MESSAGES` | `20`, strict integer `1..20` | Maximum total selected messages; mandatory parent and fit rules still apply. |
| `TYPESAFE_API_KEY` | Required for the real adapter; lazy secret | SDK authentication, never included as a context field or logged. |
| `JEV_STATE_PATH` | `jev_state.json` beside the delivery state | Private metadata-only classification ledger. |
| `DAILY_SESSION_ROTATION` | `true`; `false` pins the single mapped session | One native session per logical day; drain-gated, transient-deferring, lazy on quiet days, old sessions kept. |
| `SESSION_DAY_TIMEZONE` | `Asia/Ho_Chi_Minh`, valid IANA zone | Timezone of the rotation logical day; bad zones fail at startup. |
| `SESSION_DAY_ROLLOVER_HOUR` | `4`, strict int `0..23` | Wall hour the logical day flips; 04:00 keeps late-night context in one session. |

Per-call Jev spend has no daily stop (removed by user order); per-day cost is
reported in the ledger summary for observability only.

The 20-message default was assistant-selected under the user's
delegation, not a number personally chosen by the user. Unknown/started billing
blocks new paid attempts across restart/day rollover; exact decided cache hits
remain possible. Missing/replaced activated ledger identity fails closed without
reset or rebinding. See [the example configuration](../.env.example) and
[README.md](../README.md) for setup and startup restrictions.

## Implementation and checks

| Responsibility | Source |
|---|---|
| Canonical projection and reply status | [`conversation.py`](../src/conversation.py) |
| Scoped collection, mandatory parent, fit, fingerprint | [`context.py`](../src/context.py) |
| Jev participation adapter | [`jev.py`](../src/jev.py) |
| Durable classification/cache/accounting | [`participation/`](../src/participation/ledger.py) |
| Prepaid/postpaid guards and routing | [`responder/`](../src/responder/core.py) |
| Daily session rotation (registry, drain gate, lazy create) | [`sessions.py`](../src/sessions.py), [`responder/rotation.py`](../src/responder/rotation.py) |
| Trusted frame and untrusted JSON rendering | [`thyca.py`](../src/thyca.py) |
| MCP context/declaration tools | [`server/`](../src/server/core.py), [tool contracts](TOOLS.md) |

Relevant source tests run offline with fake identities/providers and owned TMP
state; synthetic local HTTP loopback is allowed:

```bash
uv run --offline pytest -q tests/test_conversation.py tests/test_context.py \
  tests/test_thyca_prompt.py tests/test_jev_routing.py \
  tests/test_observation_delivery.py tests/test_observation_trace.py \
  tests/test_observation_server.py tests/test_daily_session.py
```

These checks cover selection, reply relations, fit, identity binding, changed
context/cache holds, routing, and positive no-reply proof. They do not establish
live-channel quality, current production activation, provider billing caps, or
atomic Discord/journal transactions. Existing uncertain jobs stay unresolved
unless separately reconciled with evidence and explicit authorization.

## Daily rotation recovery

Rotation failures are loud and manual-safe (no silent resume, no auto-reset).
With the MCP server STOPPED (single-writer rule), compare the three SID
bindings: the mapping `~/.thyca/discord_session.json` (`native_id` +
`active_date` + `sessions`), `delivery_state.json` (`native_session_id` +
`native_sessions`), and `jev_state.json` (same two keys).

- Deferred rotation (stderr `rotation deferred for <day> (<kind>)`): a
  transient native blip; the worker keeps serving the previous day and
  retries every tick. No action; a later `rotated daily session` line
  confirms recovery.
- Pruned today-session (every submit 404s to `uncertain`): a foreign native
  `create` removed the still-blank day session before its first turn. Drop
  each stuck job with the operator `resolve_uncertain_drop` tool; the next
  04:00 boundary then rotates fresh automatically. For immediate recovery
  instead of waiting: `POST /api/sessions` with an empty `{}` body to mint
  a replacement, then with the server stopped set the SAME new SID in all
  three files (`native_id` + `sessions[<today>]` + updated `path` stem in
  the mapping; `native_session_id` + `native_sessions` in delivery and
  ledger, keeping every older day entry). Restart only with explicit
  approval.
- Half-commit (startup `native_session_id mismatch` naming both SIDs after
  a crash between the sync store writes): the mapping registry is the
  commit record. If the mapping already seals today, copy its `native_id`
  + `sessions` map into the lagging delivery/ledger file(s); if the
  mapping still seals yesterday while delivery/ledger already carry
  today's SID (present in their `native_sessions`), activate the mapping
  to match (`native_id`, `active_date`, `sessions`, `path` stem, fresh
  `created_at`). Never delete old day entries or old native sessions.
