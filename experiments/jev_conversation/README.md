# Jev conversation benchmark (synthetic, draft)

Goal: learn how context changes Jev's reading of a chat message (reflex on the
new message alone vs. judgement with the visible dialogue). All 30 cases are
synthetic Vietnamese micro-dialogues with fake users (`alice_demo`,
`bob_demo`, ...). No real messages, customers, keys, or profiles.

## Case groups

| Group | Cases | What it tests |
|---|---|---|
| human_to_human | `h2h_plain`, `h2h_reply` | Humans resolving things themselves; bot is only an observer |
| direct_bot_request | `direct_bot_mention`, `direct_bot_reply_request` | Clear requests addressed to the bot |
| continuing_bot_conversation | `continue_bot_thread`, `continue_bot_noreply` | Follow-ups to the bot's answer, with/without a reply link |
| general_channel_question | `channel_question_open`, `channel_question_anyone` | Open questions to the whole channel |
| unclear_pronouns | `pronoun_ambiguous`, `pronoun_resolved` | Referent ambiguous vs. reply-resolved; `pronoun_ambiguous` addresses the channel (`mọi người`), referent not scored |
| missing_parent | `missing_parent_deleted`, `missing_parent_unknown` | Reply target not in the visible state (deleted/unknown) |
| interleaved_topics | `interleaved_two_topics`, `interleaved_reply_disambiguates` | Two topics interleaved; reply picks the thread |
| multiple_users_same_name | `same_name_reply`, `same_name_noreply` | Two `Minh`s; identity by UID reply vs. unresolvable by name |
| nickname_change | `nickname_change_reply`, `nickname_change_noreply` | Old nickname `Bé Bơ` matches no display name; reply decides |
| textual_owner_claims | `owner_claim_false`, `owner_claim_owner_speaking` | Text claims vs. `owner_uid` in state (state wins; any human can chat) |
| reply_overriding_proximity | `reply_overrides_proximity`, `proximity_noreply_vague` | Reply beats recency; vague reaction with no anchor stays unclear |
| quoting_other_person | `quote_third_person`, `quote_bot_statement` | Mentioning someone is not addressing them |
| question_about_bot | `about_bot_third_person` | Talking *about* the bot to the group, not *to* it |
| bot_just_answered | `bot_just_answered_human_follows` | Human explains the bot's answer to another human |
| thanks | `thanks_to_bot`, `thanks_human_to_human` | Gratitude is not a new request |
| group_selection | `group_multi_followup`, `group_addressed_individual` | Multi-party thread; reply vs. explicit name selection |

No keyword/classifier business rules are encoded. `addressee` and
`explicit_assistant_request` are human-authored bench annotations, not
mathematically guaranteed truth. Sender identity is trusted SDK metadata
and reply-link state is structural context, so neither is asked or scored.
Current scoring is exact match to these annotations, not to universally
correct ground truth.

Annotation notes: `bot_just_answered_human_follows` is gold `alice_demo` for
THIS CASE because the reply quotes the bot message while the text says
`Alice ơi` and explains Thyca's answer to Alice. This is a fixture
annotation, not a universal `vocative always wins over reply link` production
rule; UID stability and name collisions may prevent resolution in other cases.
`proximity_noreply_vague` and `nickname_change_noreply` are ambiguous
annotation-agreement cases: a model may legitimately infer Cara/Bob from
recency or nickname context. Disagreement there needs human inspection and is
reported as annotation ambiguity, not proof the model cannot understand and
not a keyword policy. `pronoun_ambiguous` keeps category `unclear_pronouns` as
a natural pronoun-ambiguity example, but the scored question is only who is
ADDRESSED (`channel` via `mọi người`); who `anh ấy` REFERS TO (Bob/Dan) is not
separately scored, and this pair does not fully test reference resolution.

## Stable JSON interface (v1)

Root: `{schema_version: 1, description, cases: [...]}`. Each case:

- `id` (unique snake_case), `category`, `description` (human-readable)
- `state`: `{assistant_uid, owner_uid, participants: [{uid, display_name}],
  messages: [{id, sender_uid, text, reply_to, timestamp?}], new_message_id}`
- `expected`: `{addressee (UID | "channel" | "unclear"),
  explicit_assistant_request (bool)}`
- `rationale`: human-authored annotation explaining the gold label (not an objective proof for `addressee`/`explicit_assistant_request`)
- `participation_label`: always `null` (a human later decides personal taste);
  `participation_notes`: an open question about bot style, never a gold answer

Rules: ONLY `state` is sent to the model. `expected`, `rationale`,
`description`, `category`, and labels are NEVER part of the model payload. A
response from the bot is OPTIONAL; this bench does not encode any production
participation policy. The model's role is participant observer. `reply_to`
pointing at an unknown ID is allowed ONLY in `missing_parent` cases.

## Usage

Default is OFFLINE validation only (no API, no key, no cost):

```bash
python experiments/jev_conversation/run_eval.py
```

Paid runs require explicit user authorization, with `--max-calls <N>` and
`--max-estimated-usd <AMOUNT>` as caller-side stop limits (not a hard
provider-side budget):

```bash
uv run --no-project --with typesafe-sdk==0.7.2 --with python-dotenv python experiments/jev_conversation/run_eval.py --run --max-calls <N> --max-estimated-usd <AMOUNT>
```

The `--run` path uses pinned `jev-1.13.0` (do not use moving alias
`jev-latest`) / `typesafe-sdk==0.7.2` via that ephemeral invocation; no
SDK/project dependency changes. SDK retries disabled; any error or unknown
billing stops the batch (no retries). `--max-calls` / `--max-estimated-usd`
are caller-side stop limits, not a provider-side prepaid cap; one in-flight
call may overshoot the estimate before the stop takes effect. Key comes from
the environment (`TYPESAFE_API_KEY` preferred) or an authorized dotenv app
fallback ONLY with `--run`; never paste or print a key. Reported price at time
of writing: $0.042 / M input tokens, output free; reports show estimated and
actual usage (estimate, not a provider-side cap, not a 64k-derived price
ceiling). Keep auto-refill OFF.

> Status (2026-10-05, updated 2026-10-06): `run_eval.py` exists. Default OFFLINE validation was
> checked (30 cases / 90 questions offline, 3 questions per case; final
> checkpoint: full pytest suite green). The
> dataset and the OFFLINE runner implementation were each independently
> reviewed with NoFindings. One authorized paid batch is now complete (run under
> the old 5-question set): 30
> serial API requests (5 questions batched in each state request), 30/30
> completed, 0 failed, all responses `jev-1.13.0`; small synthetic results
> only, not a Jev quality proof. Any future paid batch needs separate explicit
> user confirmation; no SDK/project dependency changes; this is not a merge,
> quality, or native sign-off.

## Safety

- Synthetic offline data only. No real Discord reads/writes, no real user
  history or profiles, no journal/outbox access from this experiment.
- Reports go to a private tmp path (mode `0600` files, `0700` dirs); do not
  `chmod` arbitrary parent directories.

## Status and limits

- Dataset: draft. `addressee` / `explicit_assistant_request` are human
  annotations. Style preference (`participation_*`: respond / observe) stays
  UNSCORED with no labels or thresholds by design.
- This bench is NOT proof of Jev quality, NOT a production threshold, and NOT
  a participation policy. It is an experiment only: no production Noop-terminal
  change, no Seq66 change, no native reply guarantee. Open annotation
  disagreements (notably `proximity_noreply_vague`, `nickname_change_noreply`)
  are reported as ambiguity for human inspection, not as major production bugs.
- Source budget note: TypeSafe documents a ~64k-token context limit per
  request (not a documented billable-input upper bound: state is ingested
  once but per-question billing is unspecified, so do NOT derive a hard price
  ceiling from 64k — billable usage comes from the response's actual usage
  fields); the native bot POST / `build_auto_prompt` cap is ~4000
  characters, not tokens. Keep this experiment isolated; no main-bot change.
- UNRESOLVED: personal participation taste (when the bot should speak vs. stay
  silent) is explicitly out of scope for gold labels and stays unscored. The
  independent dataset review and the independent OFFLINE runner review are
  complete. Authorization for this 30-request batch was granted and the run is
  complete; broader real-world validation and any Jev quality claim remain
  pending. Future paid batches each need separate explicit user consent.

### First authorized synthetic run

- 30/30 cases attempted and completed, 0 failed, `billing_unknown=false`;
  model pin `jev-1.13.0` on all responses; input 33,145 / output 6,220 tokens,
  estimated $0.001392090 at the published $0.042/M input (output free) rate
  from reported usage — an estimate, not a provider bill or cost guarantee.
- Annotation agreement: `addressee` 26/30 (4 plausible mismatches; the last 2 are documented
  ambiguous cases); explicit-request Brier 0.0095566667 (n=30, lower better,
  no threshold). `participation` unscored by design: 21 observe / 8 respond /
  1 need_context.
- Caller-measured latencies 297..424 ms (median 335, mean ~336.1), SDK/client
  network included — not Discord end-to-end, not a pure model benchmark.
- Limits: small synthetic batch, full-state cases only, no single-message
  baseline yet; exact UID-semantic matches are not calibration proof; no
  unrun jobs and no automatic follow-up batch. Private typed report kept under
  `/tmp/thyca-jev-runner-wjjdr8ag/` (0600 files, 0700 dir); no gold labels or
  rationales were sent to the model.
