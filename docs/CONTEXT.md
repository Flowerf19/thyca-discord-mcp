# Conversation context and memory

This document describes the current simplified source (one SQLite store,
best-effort delivery). It does not authorize a start, restart,
paid call, state reset, or ACK of an unresolved job.

## Two different layers

| Layer | What it contains | Who uses it |
|---|---|---|
| Discord conversation snapshot | Selected whole messages from the approved channel, SDK identities, timestamps, and reply links | Jev for participation classification; Thyca receives the same canonical data when a native turn is needed |
| Native session history and memory | The existing Thyca session's history and native memory handling | Thyca only; this history is not exported to Jev |

The Discord snapshot is not a replacement for native memory. All Discord activity
flows into the target bot's fixed DM session (`TARGET_BOT_ID`, default
`march7th`), resolved live every tick — not a new session per message or
person. Adoption is automatic and drain-gated (no `submitted`/`sent` job may be
mid-flight); a blocked tick still runs one reconcile-only pass for the head
blocker on the old session and retries adoption next tick. Old sessions are
never deleted; every adopted day is kept as provenance in the `sessions` table.
The mutable session title is cosmetic; the audit label is the target bot id.
Thyca controls its own history/context selection. The Discord message-count limit
does not limit the native agent's entire context or guarantee that all native
history is visible on every turn. This is shared-session behavior, not per-user
memory isolation.

## End-to-end flow

```text
Captured event -> FIFO job -> source/SID checks
                                      |
                                      v
                     Collect, validate and fit a snapshot
                                      |
                                      v
                  Jev classification or exact decided-cache hit
                                      |
                                      v
                   Recheck source and snapshot fingerprint
                                      |
                   +------------------+------------------+
                   |                  |                  |
                observe            respond          need_context
                   |                  |            (legacy stored
             No native POST    Frozen native frame   decisions only)
                   |                  |                  |
             Durable no-send   Thyca agent MCP send  Scoped read/clarify
             outcome           or explicit no-reply  or explicit no-reply
                   +------------------+------------------+
                                      |
                           Terminal job outcome
                                      |
                            Safe-prefix outbox ACK
```

Jev decides participation (`respond`/`observe`; the model is never asked for
`need_context` anymore, though stored legacy `need_context` decisions still
execute); it does not generate or send Discord replies. The
native agent owns MCP sends. Classification decisions and job rows bind the chosen
route to the exact source and context; a `pending` Jev row found on a later
tick is an unknown outcome — it gets exactly ONE retry when `attempts < 2`
(fresh attempt id, one more paid call), and resolves terminally as `observe`
at `attempts == 2` (see below).

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
over its canonical JSON. Before the paid call the responder inserts a `pending`
Jev row (attempts = 1); after the call it resolves the row to `decided` with
the validated decision and reported usage. A `pending` row found on a later
tick means the outcome — and whether money was spent — is unknown: with
`attempts < 2` the row gets exactly ONE retry (attempts bumped to 2 with a
fresh attempt id, one more paid call); a still-unknown outcome at
`attempts == 2` resolves terminally as `observe` — a synthetic owner-ordered
observe stored as `decided` with the retry attempt's usage/cost (never a third
paid call for that source). A live `failed_unknown` (e.g. provider timeout)
gets no retry. Fingerprint/author drift never re-pays: the paid retry only
runs on an exact fingerprint match.

After classification, the responder checks the source again and recollects the
context. The refreshed fingerprint and SDK source binding must still match the
classified snapshot before `observe` or a native POST. A mismatch holds the job;
it does not send a stale decision with newly substituted context.

An exact decided cache hit for the same source, author, and fingerprint makes no
new provider call. A changed fingerprint or non-decided existing attempt is not
a reason for automatic rebilling. No job or native history is deleted to resolve
a mismatch.

## When Jev needs more context

| Route | Behavior |
|---|---|
| `respond` | Render the frozen snapshot into the target bot's fixed DM session; the agent generates a threaded, correlated reply through its own MCP `send_message`. |
| `observe` | Record a durable Jev no-send outcome, without a native POST. It becomes eligible for safe-prefix ACK through the worker guards. |
| `need_context` | Stored-decision compat only (no longer emitted): render the same frozen snapshot; the agent may read more scoped context, ask for clarification, or explicitly declare that no reply is needed. |

`read_context` is a read-only MCP tool for an eligible correlated turn. It reads
up to 20 whole messages before the source or a supplied earlier bound, only in
the configured channel. Its result is additional **Thyca** context; it is not
automatically fed back into Jev or used to replace the classified snapshot.

When no reply is needed, `record_no_reply` accepts only `no_reply_needed` or
`missing_context` and closes the job `done` (`no_reply`) AT TOOL TIME — no
worker proof pass. Tool signatures and
bounds are documented in [TOOLS.md](TOOLS.md).

A source proven deleted with zero sends takes the durable
`skipped`/`source_deleted` path. It stops all further replies for that source,
including plain/unthreaded fallback. Existing send evidence refuses a no-reply
declaration; a delete discovered after sends fails the job.

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
- The Jev ledger is the `jev_decisions` table inside `state.db`: decision/
  accounting metadata and a context fingerprint, not raw snapshots or message
  bodies. Job rows carry the serving native SID; per-chunk sends live in the
  `sends` table. `state.db` (plus `-wal`/`-shm` sidecars) is the only state
  store; `audit.jsonl` stays an append-only text log whose fixed allowlist
  carries no content, no `job_ref`, and no hashes.
- `job_ref` correlates a delivery turn; it is not caller authentication. Shared
  MCP access does not provide a per-turn caller ACL or exactly-once delivery.

## Relevant configuration

| Setting | Default / constraint | Purpose |
|---|---|---|
| `CONVERSATION_MAX_MESSAGES` | `20`, strict integer `1..20` | Maximum total selected messages; mandatory parent and fit rules still apply. |
| `TYPESAFE_API_KEY` | Required for the real adapter; lazy secret | SDK authentication, never included as a context field or logged. |
| `DB_PATH` | `state.db` under the project root | The only state store (outbox, jobs, Jev decisions, sessions, cursors). Must differ from `AUDIT_PATH`. |
| `TARGET_BOT_ID` | `march7th`; required, must match bot-id grammar | Bot whose fixed DM session receives auto-reply turns; resolved live every tick. |

Per-call Jev spend has no daily stop (removed by user order); per-day cost is
reported in the ledger summary for observability only.

The 20-message default was assistant-selected under the user's
delegation, not a number personally chosen by the user. Unknown billing
(`pending`/`failed_unknown` anywhere in the table) GLOBALLY blocks all new
paid calls (`jev_unknown_blocked`) until resolved — cached `decided` rows
still return; there is no per-source allowlist. See [the example configuration](../.env.example) and
[README.md](../README.md) for setup and startup restrictions.

## Implementation and checks

| Responsibility | Source |
|---|---|
| Canonical projection and reply status | [`conversation.py`](../src/conversation.py) |
| Scoped collection, mandatory parent, fit, fingerprint | [`context/`](../src/context/collector.py), [`context/snapshot.py`](../src/context/snapshot.py) |
| Jev participation adapter | [`jev/`](../src/jev/adapter.py) |
| Durable classification/cache/accounting | [`participation/ledger.py`](../src/participation/ledger.py) over the `jev_decisions` table in [`store.py`](../src/store.py) |
| Prepaid/postpaid guards and routing | [`responder/`](../src/responder/core.py), [`responder/submit.py`](../src/responder/submit.py), [`responder/routing.py`](../src/responder/routing.py) |
| Target-bot DM adoption (drain gate, reconcile pass, provenance) | [`responder/core.py`](../src/responder/core.py), `sessions` table in [`store.py`](../src/store.py); native SID/day validation at the Thyca boundary in [`thyca/validation.py`](../src/thyca/validation.py) |
| Trusted frame and untrusted JSON rendering | [`thyca/prompt.py`](../src/thyca/prompt.py) |
| MCP context/declaration tools | [`server/`](../src/server/core.py), [tool contracts](TOOLS.md) |

Relevant source tests run offline with fake identities/providers and owned TMP
state; synthetic local HTTP loopback is allowed:

```bash
uv run pytest -q tests/test_conversation.py tests/test_context.py \
  tests/test_thyca_prompt.py tests/test_jev_routing.py \
  tests/test_observation_server.py tests/test_server_responder.py \
  tests/test_store.py tests/test_participation.py
```

These checks cover selection, reply relations, fit, identity binding, changed
context/cache holds, routing, and the terminal no-reply path. They do not establish
live-channel quality, current production activation, or provider billing caps.

## Target-bot DM adoption recovery

Adoption failures are loud and manual-safe (no silent resume, no auto-reset).
There is a single store: the serving native SID and the per-day adoption
provenance both live in `state.db` (no two-file compare, no half-commit
repair class). The DM is resolved live every tick.

- Deferred adoption (stderr `target DM adoption deferred for <day> (<kind>)`):
  a transient native blip (retries every tick, no action), or a
  mid-flight (`submitted`/`sent`) job blocking the drain gate. While blocked,
  each tick still runs one reconcile-only pass for the head blocker on
  the old session (no submits, no classify), so an inflight job drains
  and adoption converges next tick. A later `adopted target bot DM session`
  line confirms recovery.
- Pruned serving session: SELF-RECOVERING. A definitive pre-POST 404 on the
  serving SID holds that tick `pending` with zero state mutation; the next
  tick re-resolves the target DM (recreated natively when deleted) and adopts
  it. No action. A 404 between snapshot and POST (millisecond race) fails
  that one job (best-effort, never a blind re-POST) and the next message
  adopts. Never delete old day rows or old native sessions.
