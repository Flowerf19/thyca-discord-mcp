---
status: done
created: 2026-10-27
last_updated: 2026-10-27
---

> Owner approved all 5 splits 2026-10-27. Executing sequentially.
>
> DONE 2026-10-27: all 5 splits landed as pure moves (writers cbe21a9b,
> 6623b657, 5e1211e1, 4b143cea, e86ced8e), lead gates re-run after each
> (899 passed throughout, golden byte-identical at split 1). Final
> independent review (249224cf): Approve, no Critical/Important; two
> Minor stale doc links (docs/CONTEXT.md, docs/TOOLS.md) fixed by lead.
> Resulting >400-line files, all dispositioned: store.py 1437 (keep),
> server/core.py 1157 (closure remainder), responder/core.py 933
> (keep), server/admin_tools.py 601, responder/submit.py 548,
> ledger.py 452 (keep), context/collector.py 441. Not committed; the
> live service still runs pre-split code until the owner-ordered
> restart.

# PLAN module splits for oversized files (audit-driven)

> Audits: three read-only sweeps over the 8 files >400 lines
> (ef1629bc server/store, 646d68d3 thyca/responder, 19e3bbf2
> jev/context/ledger). Owner rule: when a split is warranted, make a
> proper directory module like server/responder/participation; no tiny
> fragmentation. External import paths stay stable via `__init__`
> re-exports. Base: working tree (admin tools uncommitted), 899 tests.

## Do (5 files)

1. **server/core.py (1714) -> extract `server/admin_tools.py`**
   `register_admin_tools(mcp, *, settings, bot_fn, ok_fn, fail_fn,
   identity_policy)` holds `_resolve_member/_member_payload/
   _resolve_guild_channel/_require_name` + the 17 admin tools; dedupe the
   category resolver inside. core keeps factory/lifespan/correlated/
   identity/task/outbox (~1150). Zero import/patch-site changes; golden
   must stay byte-identical (registration order preserved).
2. **thyca.py (992) -> package `thyca/`**
   `errors.py` (33-107), `validation.py` (110-355; NOT validate.py —
   avoids clash with top-level validate), `transport.py` (357-458),
   `prompt.py`, `client.py` (706-959). `__init__` re-exports `__all__`
   + test-imported privates (`_declared_body_len, _read_bounded,
   _TimeoutMarker, _NetworkMarker`). One test fix:
   `tests/test_thyca_prompt.py:532` AST purity check iterates the
   package instead of the single file.
3. **responder/submit.py (873) -> extract `responder/routing.py`**
   `ResponderRoutingMixin` + `_VALID_PARTICIPATIONS`: the validation
   cluster (126-425) + `_source_message_of`. The 370-line `_submit_queued`
   driver stays intact. core composes both mixins; `__init__` surface
   unchanged. One test fix: module-set assertion in test_responder.py.
4. **jev.py (651) -> package `jev/`** (low urgency)
   `types.py` (constants/errors/dataclasses), `contract.py`
   (build_questions + validate_*), `provider.py` (SDK transport),
   `adapter.py` (JevAdapter). Types at the bottom to avoid cycles;
   `__init__` re-exports `__all__` + `_classify_sdk_error` (test shim).
5. **context.py (556) -> package `context/`**
   `snapshot.py` (ContextUnavailable, FIT constants, context_fingerprint,
   participants/messages builders, fit_and_project; no discord/bot deps)
   and `collector.py` (fetch/resolve I/O + build_context_collector, with
   `_message_record` dedup of the 3 record-building sites). `cached
   display name` passed as a callable to avoid bot import in snapshot.

## Keep (3 files — audited, cohesive)

- `store.py` (1437): one DB aggregate; cross-domain atomic txns are the
  design. Revisit only if a 5th domain lands (Jev extracts first).
- `responder/core.py` (930): one tick state machine; clean extractions
  already done (input/submit). Revisit if adoption grows own state.
- `participation/ledger.py` (452): spend protocol must read as one
  sequence. Side notes (not this plan): dead `INPUT_USD_PER_TOKEN`
  re-export duplicates store.py's rate; `_fingerprint_snapshot` vs
  `context_fingerprint` 6-line dup — both owner decisions, leave alone.

## Execution (on approval)

Sequential writer runs (one worktree), one split per run, each gated:
full pytest (899), golden byte-identical (except the two sanctioned
test updates), ruff, `git diff --check`, `uv build`; `from X import`
surface diffed against the pre-split `__all__`. Independent review of
the full refactor diff at the end (no behavior change expected —
any behavior diff is a defect). Order: 1 (biggest seam), 2, 3, 5, 4.
Live service note: it runs from this tree; restart Thyca after all
splits land (owner order, as before). Commit separately approved.
