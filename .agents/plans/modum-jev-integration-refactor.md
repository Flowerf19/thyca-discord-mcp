---
status: done
created: 2026-10-05
last_updated: 2026-10-06
---

# PLAN modum Jev integration, refactor, rename, removal

> Ordered implementation plan (docs-only artifact). Base `main@c8b6f94`.
> This writer owns ONLY `.agents/plans/modum-jev-integration-refactor.md`.
> No runtime edits here. Docs do NOT prove implementation.
> Source: user explicit request + user delegation of ALL unresolved decisions
> to assistant (sleep + tomorrow review). Parent selected/announced
> most-reasonable picks in §2/§3 with delegated provenance, pending user
> review; no user-picked numeric value is claimed. `done` is scoped to this
> authorized SOURCE plan only. T1/T2a/T3/T2b SOURCE gate is COMPLETE
> (checks + two whole approvals). T4 DONE with combined checks and independent
> equivalence approval; T5 COMPLETE with parent checks/wheel/import; T6
> candidate audit COMPLETE (no additional genuine removal justified); T7
> final review COMPLETE: package/routing/privacy approved; recovery Minor
> reproduced and fixed; fresh non-author follow-ups Approve NoFindings for
> the fix and whole guidance/package scope. SOURCE plan DONE, not production
> activation or completion of the separate V5 live plan. No
> merge/live/paid/deploy/restart/commit.

## 0. Base, ownership, evidence

- Base: `main@c8b6f94`. Current post-fix checks: offline full pytest
  1243 PASS 13.42s; Ruff clean / 57 formatted; lock 59 / diff clean;
  offline wheel rebuilt. One T7 Minor was confirmed by 3 failing / 5 passing
  new cases, then fixed with six late-skip guard lines and eight passing cases;
  original regression assertions preserved. Independent re-review COMPLETE:
  fix Approve, guidance/package Approve NoFindings; hashes parent-verified.
  Historical T5 migration checkpoint: `uv run pytest` 1235 PASS 14.26s, Ruff
  src/tests/experiments clean, 57 formatted, `uv lock --check` pass (59
  packages), `git diff --check` clean; offline wheel holds only 28 `modum`
  files; console `thyca-discord-mcp=modum.__main__:main`; old
  `src/thyca_discord_mcp` dir absent, zero old imports; DRY imports, no
  `main()` invoke; 56-file Py3.10 GRAMMAR only (runtime 3.14 verified).
  `src/modum` owns all 28 runtime files (`__init__` 0.1.0 minimal, no side
  effects); external distribution/CLI stays `thyca-discord-mcp` (Python
  namespace `modum`); tool 14/config/SID/state/audit-label guards preserved.
  Keys never read; no live SDK/API call. Earlier 1227 PASS 14.15s (T4),
  1184 PASS 14.03s/13.86s, 734 PASS 12.30s, 568 stay dated history.
  Uncommitted at plan time: approved prod SID/title-ignored and deleted-source
  fixes, paused splits, experimental Jev synthetic 30-call paid batch (all
  completed, est. 0.00139209). Source-only, never deployed; seq-66 `uncertain`
  untouched; no current production attestation.
- Measured modules (historical pre-integration vs verified T4, no size caps):
  pre-integration historical `delivery.py` 1720, `thyca_client.py` 1010,
  `responder.py` 1256, `server.py` 1013; verified T4 `delivery.py` 1315,
  `thyca_client.py` 356, `responder.py` 1512, `server.py` 1267, leaf
  `delivery_schema.py` 917, `delivery_storage.py` 189,
  `responder_recovery.py` 488, `server_ticks.py` 235. Size alone never forces
  split.
- All extraction/migration/docs writers completed. Parent owns the focused
  T7 fix in `responder_recovery.py`, eight portable cases in
  `tests/test_responder_extraction.py`, and directly affected status docs.
  The five-question canonical contract is verified on actual `modum` imports. T1/T2a/T3/T2b SOURCE gate COMPLETE with checks + two whole
  approvals. T4 DONE: parent 1227 PASS 14.15s, Ruff clean / 57 formatted,
  lock 59 / diff clean, 134 structural checks plus delivery AST acceptance,
  and independent NoFindings equivalence approval. T5 COMPLETE (parent 1235
  PASS 14.26s, wheel/import/no-old-dir per checkpoint above). T6 candidate
  audit is complete; no additional behavior-preserving removal justified
  (see §5). T7 final independent review and follow-ups COMPLETE.
- Reviewer statuses (source-only, never deployed; whole SOURCE gate
  COMPLETE): pre-source prompt `No Findings` is historical pure un-routed
  renderer slice ONLY, not current routed approval. Slice reviews: collector
  Approve SOURCE ONLY (one Minor doc precision, fixed), participation ledger
  Approve SOURCE ONLY NoFindings, SDK logging hardening Approve SOURCE ONLY
  NoFindings, observation durability hold fixed by declaration-load
  correction. Two fresh non-author whole reviews Approve SOURCE ONLY
  NoFindings; one scope-breaching report excluded/replaced. T4 independent
  equivalence review NoFindings. Same-family limits disclosed; two indep
  T4/Integration approvals valid; project requires independent reviewer, not
  different family. T5 COMPLETE; T6 audit COMPLETE; T7 fresh Muse MAX
  COMPLETE: package/routing/privacy Approve NoFindings; initial recovery
  Approve with fixes (one Minor, no Important/Critical). Parent confirmed and
  fixed the late-skip status gap; fresh core follow-up Approve and whole
  guidance/package follow-up Approve NoFindings. All remaining findings closed. No
  merge/restart/rollout/live paid-chat gate. Source
  authorization is not production activation. Seq-66 (`1` intent, `0`
  receipt, `uncertain`) untouched; original live-fault gates not completed.
- Artifact correction: the original Root activation report file is mode 0644
  (actual `stat`), NOT 0600; parent holds a byte-identical private 0600 copy
  VERIFIED (not pending). Do NOT claim the original report file private
  unless actual `stat` says so. Public docs carry no private paths/report IDs.
- Historical planning note: `Muse Spark 1.3 MAX` was initially verified
  catalog-only. Subsequent writers/reviewers used the verified model;
  fresh non-author independence is same-family, not cross-family.
- Prior v5 plan stays authoritative for v5 flow; this plan does not rewrite it.

## 1. Goal, scope, binding order

- Goal: integrate Jev classification into the actual respond pipeline with a
  careful shared canonical context to Thyca; then responsibility-refactor and
  mechanically rename the internal package to `src/modum`; then remove only
  proven-duplicate conversation-inference code Jev now owns.
- Fixed flow (unchanged): outbox -> Responder FIFO -> native Thyca SID prompt
  -> agent-owned MCP send -> intent/SDK receipt/trace -> safe-prefix ACK.
  Scope: guild `1067690340359880724`, channel `1395103735725821982`, same SID
  `2026-10-05T01-59-55_0f25`. All humans may chat; no OwnerGate, mention-only,
  or manual-enable product mode.
- Binding user order (not negotiable by delegation/review status):
  1. Integrate Jev into the real pipeline. The adapter-as-FIRST-
     NON-BLOCKING-SLICE-ONLY wording is historical; integration is now
     COMPLETE per the T2b+T3 SOURCE gate (checks + two whole approvals).
  2. Only after integration is complete (not merely an unwired adapter):
     responsibility refactor, then mechanical rename to `src/modum`.
  3. Only after (1)+(2): remove PROVEN duplicated inference/prompt/context code.
- T4 was unlocked by the complete-integration gate and has passed its
  checks/review. T5 is COMPLETE with parent checks/wheel/import; T6 audit
  is COMPLETE without a justified additional cut; T7 COMPLETE.
- Out of scope: new DMs/channels, destructive actions, v2/v3 tools, new
  provider/session/scheduler/gateway, upstream Thyca patch, whole 1230-line
  benchmark runner into `src`, gold/Brier/CLI billing reports (stay in
  `experiments/`), extra paid batches, real-channel send APIs, merge/restart/
  rollout/live forwarding beyond the approved source edit.

## 2. Accepted decisions

- Jev classifies; it does NOT write Discord and its outputs are NOT permission
  UIDs. Sender UID comes from SDK authority, never the classifier. Source: user.
- Jev call shape: SDK `0.7.2`, pinned `jev-1.13.0`, lazy import, exact retry 0,
  timeout 10, one call, state + 5 questions, participation from assistant UID,
  no score threshold. External key is supplied at the composition root and must
  never be printed or appear in `repr`. Source: verified interface + previous
  synthetic benchmark (30 real calls); user accepted the approach, not signatures.
- Canonical context is frozen pre-classification; the same snapshot feeds Jev
  and the Thyca prompt (current author/new-message marks, current reply,
  parent-missing/interleaved labels preserved). Raw message bodies are
  UNTRUSTED; UID/time/guild/channel/refs from validated SDK are trusted
  metadata; display names are NON-AUTHORITATIVE. Shared-server history is
  accepted; this is not personal-user-data isolation. Private native
  USER/SOUL/owner profiles, secrets, auth, and provider errors never enter the
  Jev payload. Source: user.
- Prompt frame keeps current `jobRef`/current MID exact trusted first-user
  match; 4000 CHAR hard technical limit with no blind truncation, no silent
  drop of source parent/old history. Existing `NEVER channel history` prompt
  language needs approved scoped-context wording in T2b; UID/security/privilege
  guards stay. Source: user + verified prompt contract.
- Existing safety/lifecycle contracts stay: public `Responder`
  `tick`/`close`/`guard_ack`/`correlated_job`/`build_auto_prompt`; Thyca
  snapshot/submission/trace APIs; 14 MCP tools in current source (12 preserved
  + `read_context` + `record_no_reply`, source-only, whole SOURCE reviews
  complete); `delivery._now_iso`/
  `_fsync_parent_dir` fault/timestamp hooks; native `ProxyHandler{}`; definite
  409-only zero-intent requeue, unknown-write NO retry; thread/cancel FSM,
  OSError/fsync committed-uncertain, clone semantics; SourceDeleted only on
  strict GET 404+10008 with zero evidence, unavailable != absent; late-Skip
  wins, cancel propagates; SID authoritative, mutable title ignored; existing
  seq-66 job (`1` intent, `0` receipt, `uncertain`) untouched by any new Jev
  prediction (no retry/drop/ACK/change). Source: v5 plan + current source.
- `server_helpers._resolve_text_channel` re-export via `source_messages` stays;
  native code unchanged. Inert `delivery_schema`/`delivery_storage` and
  `thyca_errors`/`thyca_validate` must rebase current SID/deleted-source fixes,
  never blindly wire stale copies. Source: parent-actual.
- Internal package is `src/modum` (T5 COMPLETE: no `thyca_discord_mcp` dir
  left, no compat alias; old dual-dir state is dated history). External
  distribution/CLI stays `thyca-discord-mcp` (dash); Python namespace is
  `modum`. Config keys/defaults, auth, SID, outbox/state paths, tool payloads,
  and audit labels preserved. Hatch wheel `packages=["src/modum"]`; console
  target `modum.__main__:main`; tests/imports/monkeypatches updated to
  `modum.*`. New modules follow responsibilities, no facade/`__getattr__`
  coupling, no fake one-class splits, no hidden-failure alias errors. Source:
  user.
- Delegated (user delegated ALL unresolved to assistant; parent
  selected/announced; pending tomorrow user review; not user-picked numerics):
  `need_context` routes Thyca to scoped additional context or clarification,
  no guessing. Max 20 whole source-adjacent messages including source +
  priority exact reply parent, same hard approved guild/channel. Collector
  selects newest whole optional messages fitting 4000-char shared prompt,
  never truncates body/drops mandatory source or visible parent; mandatory-pair
  oversize fails closed. Source: delegated decision, pending review.
- Delegated ongoing Jev (same provenance, pending review): ongoing config
  reported-usage stop default USD 0.01 / UTC day, NOT provider-account hard
  cap, final request may overshoot; lazy caller key / single call / no SDK
  retry, persist precall started + decision/usage, unknown billing halts new
  paid attempts even restart/date-change, same MID/fingerprint cached, changed
  context held with no automatic rebill. Source: delegated decision.
- Delegated ledger/proof (same provenance, pending review): ledger metadata
  only, file private `0600`, no new raw-chat store; original delivery journal
  preserved unknowns + new observed proof = terminal positive Jev decision or
  exact Thyca MCP `record_no_reply` + whole trace, no absent-receipt
  deduction. Existing seq-66 never retro-changed. Source: delegated decision.
- Delegated tools (same provenance, implemented in source, whole SOURCE
  reviews complete, NOT deployed): 12 payloads preserved externally compatible plus new
  `read_context` and `record_no_reply` for need-context (14 total, verified in
  `src/modum/server.py`: exact params/default limits/source-before bounds/`declared`
  nonterminal/audit metadata/read-only context/no profile export; see
  `docs/TOOLS.md`). Only source authorized; no additional paid batch / real
  forwarding / restart / rollout / stage / commit. Source: delegated.
- RunGate (do not start current source): current source is a different Python
  namespace (`modum`) though the external CLI name is unchanged; native
  processes run old cached modules with no hot reload; deployment is separate
  with no operational approval; no cold start/restart. `USER.md`/profiles never
  export; vague routed `TASK` wording was replaced during integration; input
  SDK UID + `whoami` authenticate, Jev diagnostics never grant permission;
  MCP nonce is NOT caller ACL; symlink paths fail closed, hardlink aliases not
  detected (no all-physical safety).

## 3. Delegated decisions + technical gate (R1-R3 decided by delegation, pending user review)

- Delegated provenance: user explicitly delegated ALL unresolved decisions to
  assistant to pick most reasonable (sleep + tomorrow review). Parent
  selected/announced the §2 picks below. Do NOT present numeric values as
  user-picked; they are assistant-most-reasonable pending review.
- T2-R1 `need_context` action (delegated): routes Thyca to scoped additional
  context or clarification, no guessing. Prior fetch-vs-wait user question is
  superseded by delegation; routing must not invent other actions.
- T2-R2 channel context source/window/retention/missing-parent (delegated):
  max 20 whole source-adjacent messages including source + priority exact
  reply parent, same hard approved guild/channel; collector selects newest
  whole optional messages fitting 4000-char shared prompt, never truncates
  body/drops mandatory source or visible parent; mandatory-pair oversize
  fails closed. Do NOT record 50-msg or NearestSpeaker as fact.
- T2-R3 ongoing paid Jev budget/scope/usage-cache persist/retry/error
  (delegated): ongoing config reported-usage stop default USD 0.01 / UTC day,
  NOT provider-account hard cap, final request may overshoot; lazy caller key
  / single call / no SDK retry, persist precall started + decision/usage,
  unknown billing halts new paid attempts even restart/date-change, same
  MID/fingerprint cached, changed context held with no automatic rebill.
  Ledger metadata only (`0600`), no new raw-chat store.
- T3-G4 technical gate (NOT a user question; COMPLETE per the T2b+T3 SOURCE
  gate): durable classification + `observe` proof design/review work inside
  authorized scope, verified by checks + two whole approvals. Fresh shared
  snapshot+fingerprint, classification durability, known-usage ledger, and
  compatible FSM need interface-verified design; the ordinary durable
  zero-evidence marker for terminal `observe` (zero SEND) belongs here, with
  storage/audit/ACK compat review. `0 receipts` alone never proves `observe`;
  proof is terminal positive Jev decision or exact Thyca MCP `record_no_reply`
  + whole trace; no lightweight Skip and no retroactive skip of existing
  intent/receipt/trace/uncertain/seq-66 job.
- Native Brain may no-op; routes must not pretend a finite completed task.
  Observer/noop intents cause no action and no sends. Source: user direction.

## 4. Tasks (follow dependencies, not number order)

- T1 Adapter first slice (depends: none; source slices complete with checks;
  checkpoint in §0): minimal non-blocking
  `src/modum/{__init__,conversation,jev}.py` + focused tests; lazy Jev import,
  key at composition root, no Discord writes, no gold/metrics in `src`. Gate:
  focused pytest + Ruff clean; adapter-alone was explicitly NOT complete
  integration (historical slice marker; integration COMPLETE per T2b+T3).
- T2a Canonical context + integration preparation (depends: T1; source slice
  complete with checks, whole SOURCE reviews complete): freeze snapshot once,
  feed Jev and Thyca from it, preserve §2 trust/prompt/4000-char/
  delegated-collector rules, keep FIFO/cancel/uncertain/receipt/trace/ACK
  semantics. Gate: context
  round-trip/no-gold/no-profile/no-mutation/thread/UID tests.
- T3 Persist/cache/accounting (depends: T2a; G4 design gate COMPLETE; source
  slices complete with checks incl. declaration-load fix, whole SOURCE
  reviews complete):
  durable decision snapshot+fingerprint, cache, metadata-only ledger (`0600`),
  unknown-outcome no-rebill rule, `observe` zero-evidence audit/ACK proof with
  compat review. Gate: restart + unknown-paid-no-call tests, no every-tick
  rebilling.
- T2b Final routing / whole integration (depends: T3 + delegated R1-R3;
  COMPLETE): core routing + Root activation in source with checks, whole
  SOURCE reviews COMPLETE (two approvals). respond/observe/needContext
  mapping on delegated basis, approved scoped-context wording without
  weakening UID/security guards. Gate: mapping + FIFO + restart
  decision-persist tests. T2b+T3 complete counts as integration COMPLETE;
  T4 DONE; T5 COMPLETE; T6 audit COMPLETE; T7 COMPLETE.
- T4 Responsibility refactor (depends: T2b AND T3 complete with checks/review;
  strictly AFTER complete integration, NOT adapter-only; COMPLETE with
  parent 1227-test checks and independent NoFindings equivalence approval):
  split by responsibility across delivery/client/responder/server (+ rebased
  inert helpers); preserve public imports, hooks, payloads, event order, stored
  compatibility. Gate: per-module AST/semantics + monkeypatch-hook diff vs HEAD
  INCLUDING current uncommitted changes, full pytest + Ruff.
- T5 Mechanical rename to `src/modum` (depends: T4 checks; strictly AFTER
  T2b+T3 complete; COMPLETE after the verified T4 gate): moved package,
  updated hatch wheel/console target (`packages=["src/modum"]`,
  `thyca-discord-mcp=modum.__main__:main`), imports, tests, fixtures, scripts
  metadata; grep proves zero old `thyca_discord_mcp` imports; CLI dry-run
  import with no network, no `main()` invoke. Gate PASSED: wheel build + CLI
  dry import + full tests (parent 1235 PASS 14.26s, 57 formatted, lock 59,
  diff clean; 28 `modum` files only; old dir absent).
- T6 Proven-duplicate removal (depends: T2b+T3+T4+T5; strictly AFTER T2b+T3
  complete; AUDIT COMPLETE after verified T5): every §5 candidate is
  dispositioned. No additional genuine removal is justified; existing public
  compatibility and safety contracts stay. No new runtime deletion or
  removal of native judgment is claimed. Gate: §5 evidence + unchanged
  1235-test source checkpoint / Ruff / import checks.
- T7 Independent review (depends: T6; COMPLETE): package/routing/privacy
  Approve NoFindings; initial recovery Approve with fixes, followed by
  independent fix Approve and whole guidance/package Approve NoFindings. Confirmed Minor: a durable
  source-deleted skip landing during the bound trace await could mislabel one
  tick as uncertain (durable skip and ACK already safe), or make one redundant
  trace GET on a matching result. Six guard lines now prioritize the durable
  skip on error and success, before interpreting trace; cancellation and
  no-skip behavior unchanged. Eight portable Root/real-journal regressions
  cover error/mismatch/match/cancel with and without late skip; confirmed RED
  3 failed / 5 passed, then focused 154 passed and full 1243 passed. No status
  rename for the existing no-skip transient trace hold. Fresh Muse MAX
  independent re-review COMPLETE (same-family limits disclosed; two indep
  T4/Integration approvals valid; independent reviewer required, not different
  family). No merge/restart/rollout/live paid-chat gate here; historical seq66
  uncertainty is untouched. The historical 30 real Jev calls used synthetic
  dialogue fixtures and are NOT production acceptance.

## 5. Removal table

| Candidate (remove ONLY if proven duplicate after T2b+T3+T4+T5) | Keep (never remove for Jev) |
|---|---|
| `DECIDE`-reply text duplicated in Thyca prompt | Uncertainty/fencing/receipt/trace/safety-ACK/auth IDs |
| Repeated conversational-target inference / generic context prose | Trusted/untrusted guards, task lifecycle, capture-bot scope |
| Duplicate SDK helpers only if benchmark-thin production adapter matches 5-question/state outputs (gold/metrics stay experiments, lazy API default) | Nonce, source-deletion 404+10008 guard, v4 tool aliases, 4000-char guard |
| Inert/unused schema/client child defs AFTER rebase, leaving one owner (no duplicate enums/validators) | `SourceNoop` terminal semantics (not an optional disabled flag); downstream Thyca creative judgment unless proven unneeded |

T6 disposition (source audit + parent caller verification):

| Candidate | Disposition and proof |
|---|---|
| Active native participation/`DECIDE` task | Already resolved during integration: all three canonical-render callers pass the participation/hash pair; `_routed_task` replaces the base task, never appends it. |
| `build_auto_prompt` and unrouted canonical renderer | Retain accepted byte-exact public compatibility. No live submission caller is not proof of external non-use. |
| Conversational-target inference | One live Jev owner; diagnostic identities never replace SDK UID authority. Structural reply-status extraction is not model inference. |
| Benchmark SDK helpers | Retain separate standalone runner contracts: scoring, key loading, errors, logging, serial accounting, and reporting differ from the production adapter. |
| Schema/client child definitions | Already consolidated during T4 into single canonical owners with same-object aliases; no inert copies remain. |
| Native creative/read/clarify judgment and safety guards | Required behavior; not cleanup candidates. |

No additional source deletion was made in T6. This is a verified negative
candidate disposition, not a claim that all native judgment was removed.

## 6. Checks

- `uv run pytest tests/test_jev*.py` (focused), then full `uv run pytest`
  (final parent 1243 PASS 13.42s; T5 1235 PASS 14.26s and 1227
  PASS 14.15s, 1184 PASS 14.03s/13.86s, 734 PASS 12.30s and 568 stay dated
  history; future count unpredictable, checkpoint not current); `ruff check`
  + `ruff format --check` + `uv lock --check` + `git diff --check` (checkpoint
  src/tests/experiments clean, 57 formatted, lock pass 59 packages, diff clean).
- Fixtures: fake SDK/Discord, `TMPDIR` `.env` with `_env_file=None`; synthetic
  local HTTP loopback is allowed, no external network,
  no live API, no native restart, no outbox/journal mutation as a plan gate.
  No private/native/env/log read, no paid tests/deps/agents.
- Package: wheel build + `modum.__main__:main` dry import + old-import grep +
  metadata check (T5 COMPLETE: offline wheel holds only 28 `modum` files;
  old dir absent; DRY imports, no `main()` invoke).

## 7. Handoff

- Plan path: `.agents/plans/modum-jev-integration-refactor.md`. Status
  `done` for this authorized SOURCE plan only. SOURCE gate COMPLETE
  (T1/T2a/T3/T2b checks + two whole approvals); T4 DONE with checks and
  independent equivalence approval; T5 COMPLETE; T6 audit COMPLETE; T7
  final independent reviews COMPLETE, Minor resolved. Docs do NOT prove
  implementation; source/check/report hashes were parent-verified. Private migrated-package
  docs-update note (`0600`): `/tmp/modum-migrated-package-docs-update.txt`
  (this task, actual `date -u` + source facts/diff/no runtime mutation);
  prior notes (`0600`): `/tmp/modum-integration-gate-docs-update.txt`,
  `/tmp/modum-integration-docs-update.txt`,
  `/tmp/modum-delegated-decisions-docs.txt`, `/tmp/modum-jev-plan-docs.txt`,
  `/tmp/modum-jev-plan-corrections.txt`. Migrated docs and T6 disposition
  parent-verified; final follow-up reviews accepted with no open findings.
- Completed sequence: integration → responsibility refactor → whole package
  migration → proven-duplicate audit (no additional cut justified) → final
  independent source review and Minor-fix follow-up.
- Handoff: user may review the source and assistant-selected R1–R3 defaults.
  There is no remaining implementation task in this source plan. G4 technical
  gate COMPLETE, never a user question. Separate V5 live-failure acceptance,
  deployment/restart, and seq66 uncertainty remain outside this completion.
  No additional paid batch / real forwarding / restart / rollout / stage /
  commit. Commits need separate permission; plan status grants no permission.
