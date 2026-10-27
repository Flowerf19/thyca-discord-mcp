"""Correlated MCP send/read tools (registrar pattern, same as admin_tools).

Core owns the audit closures (``audit``/``_ok``/``fail``/``_correlated_deny``),
the bot/responder state, and the identity policy; this module owns the send
chokepoint, the correlation preamble, and the
``send_message``/``record_no_reply``/``read_context`` tool bodies, threaded
through :func:`register_correlated_tools` callbacks. This module never imports
``server.core``; the responder is reached only via ``get_responder_fn``.
Existing ``Store`` operations in the moved code are unchanged (no persistence
ownership change, no new connection strategy).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, NoReturn

import discord
from mcp.server.fastmcp import FastMCP
from pydantic import StrictInt

from bot import get_scoped_guild
from config import Settings
from server_helpers import _coarse_error_kind, _resolve_text_channel
from source_messages import (
    SourceDeleted,
    SourceUnavailable,
    check_source_message,
    is_unknown_message,
)
from store import Store, StoreError


def register_correlated_tools(
    mcp: FastMCP,
    *,
    settings: Settings,
    bot_fn: Callable[[], discord.Client],
    ok_fn: Callable[..., str],
    fail_fn: Callable[..., NoReturn],
    deny_fn: Callable[..., NoReturn],
    get_responder_fn: Callable[[], Any],
    identity_policy: str,
) -> None:
    """Register send_message/record_no_reply/read_context at the caller's position."""
    # Mention guard for every send (Phase 1 deliberate behavior fix): agent
    # text must never mass-ping. users/replied_user stay on (owner-confirmed
    # choice); the !help command vanished with commands.Bot (noted at gate).
    _ALLOWED_MENTIONS = discord.AllowedMentions(
        everyone=False, roles=False, users=True, replied_user=True
    )

    async def _discord_send(
        channel: discord.TextChannel,
        content: str,
        reference: discord.MessageReference | None = None,
    ) -> discord.Message:
        """Single SDK send chokepoint for manual + correlated MCP sends.

        This MCP tool is the ONLY auto outbound path: the worker tick and the
        Thyca POST body never send. Correlated calls pass a native reply
        reference; manual calls send plain content exactly as v4 did. Both
        paths carry the mention guard.
        """
        if reference is None:
            return await channel.send(content, allowed_mentions=_ALLOWED_MENTIONS)
        return await channel.send(content, reference=reference, allowed_mentions=_ALLOWED_MENTIONS)

    def _fail_job(channel_id: int, source_message_id: int, *, kind: str) -> None:
        """Best-effort fail after a send-side failure (never raises).

        The Discord send already side-effected (or ambiguous): the job
        fails with a coarse kind and the worker moves on -- never a blind
        retry or resend. Terminal jobs keep their recorded state (completed
        wins over a failed close). Never raises: the caller's bounded
        error (or CancelledError) stays the visible failure.
        """
        try:
            with Store.open(settings.db_path) as _store:
                fresh = _store.get_job(channel_id, source_message_id)
                if fresh is None or fresh.get("stage") in ("done", "skipped", "failed"):
                    return
                _store.mark_failed(channel_id, source_message_id, kind=kind)
        except Exception:
            return

    # Fixed audit allowlist for the three correlated tools (send_message
    # correlated branch, record_no_reply, read_context): channel_id,
    # source_message_id, content_len (correlated sends only), reason
    # (record_no_reply only, enum values) plus error_kind on failures.
    # job_ref NEVER appears -- accepted or rejected. Rejected untrusted
    # inputs leave no audit trace at all on rejection -- not raw, not even a hash.
    def _correlated_base_args(
        cid: int,
        source_mid: Any,
        job_ref: Any,
        *,
        content: Any = None,
        reason: Any = None,
    ) -> dict[str, Any]:
        """Pre-acceptance audit args: metadata only for untrusted inputs."""
        args: dict[str, Any] = {"channel_id": cid}
        if isinstance(content, str):
            args["content_len"] = len(content)
        if isinstance(source_mid, int) and not isinstance(source_mid, bool) and source_mid > 0:
            args["source_message_id"] = source_mid
        # Rejected untrusted inputs leave no audit trace at all (never raw,
        # never a hash); non-string non-None shapes keep a boolean flag only.
        if not isinstance(job_ref, str) and job_ref is not None:
            args["job_ref_invalid"] = True
        if not isinstance(reason, str) and reason is not None:
            args["reason_invalid"] = True
        return args

    def _require_correlated(
        tool: str,
        cid: int,
        source_mid: Any,
        job_ref: Any,
        base_args: dict[str, Any],
        *,
        scope_public: str,
    ) -> dict[str, Any]:
        """Shared preamble for the three correlated tools.

        Covers: default-channel scope, source/job_ref shape, controller
        presence, correlated_job accept/reject, bounded error mapping via
        _coarse_error_kind, and audit emission. No send_blocked latch
        remains (that machinery died in Phase 3), so no extra state is
        checked here. Returns the accepted allowlist args (channel_id +
        source_message_id, never job_ref); callers add content_len/reason.
        """
        if cid != settings.default_channel_id:
            deny_fn(tool, base_args, "validation_error", scope_public)
        if not isinstance(source_mid, int) or isinstance(source_mid, bool) or source_mid <= 0:
            deny_fn(tool, base_args, "validation_error", f"{tool}: invalid source_message_id")
        if not isinstance(job_ref, str) or not job_ref:
            deny_fn(tool, base_args, "validation_error", f"{tool}: invalid job_ref")
        responder = get_responder_fn()
        if responder is None:
            deny_fn(
                tool,
                base_args,
                "validation_error",
                f"{tool}: auto controller not running",
            )
        try:
            responder.correlated_job(source_mid, job_ref, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            deny_fn(
                tool,
                base_args,
                _coarse_error_kind(exc),
                f"{tool}: correlation rejected",
            )
        return {"channel_id": cid, "source_message_id": source_mid}

    async def _correlated_send(content: str, cid: int, source_mid: Any, job_ref: Any) -> str:
        """Agent-owned auto reply: correlate, then SDK send, then record.

        Correlation selects the exact active job; it never authenticates
        the MCP caller. The job + send-eligible stage is verified, the
        reply is sent with an explicit reference
        (``fail_if_not_exists=True``), then ONE store update records the
        send. A deleted source skips durably (zero sends only); any other
        send failure fails the job (never a blind retry or resend).
        CancelledError propagates untouched.
        """
        # Pre-acceptance audit args: correlation inputs are UNTRUSTED here.
        args = _correlated_base_args(cid, source_mid, job_ref, content=content)

        def _bounded(kind: str, public: str) -> NoReturn:
            deny_fn("send_message", args, kind, public)

        if (source_mid is None) != (job_ref is None):
            _bounded(
                "validation_error",
                "send_message: source_message_id and job_ref must be paired",
            )
        accepted = _require_correlated(
            "send_message",
            cid,
            source_mid,
            job_ref,
            args,
            # No explicit scope gate existed here: scope rejects via
            # correlated_job, so the public string stays identical.
            scope_public="send_message: correlation rejected",
        )
        # Correlation ACCEPTED: the allowlist carries channel_id +
        # source_message_id (+ content_len); job_ref never appears.
        args = dict(accepted)
        if isinstance(content, str):
            args["content_len"] = len(content)
        if not isinstance(content, str) or not content or len(content) > 2000:
            _bounded("validation_error", "send_message: auto content must be 1..2000 chars")
        try:
            bot = bot_fn()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded(_coarse_error_kind(exc), "send_message: channel unavailable")
        # Best-effort send: verify job + stage, send with an explicit reply
        # reference (fail_if_not_exists reports a deleted source), then ONE
        # conditional store update recording the send. No pre-flight fetch,
        # no intent/receipt journal, no latches. The reference send above is
        # the only SDK side effect; cancellation propagates untouched.
        try:
            reference = discord.MessageReference(
                message_id=source_mid,
                channel_id=cid,
                guild_id=settings.guild_id,
                fail_if_not_exists=True,
            )
            message = await _discord_send(channel, content, reference=reference)
        except asyncio.CancelledError:
            raise
        except discord.NotFound as exc:
            # The reply reference reports a deleted source. With zero sends
            # recorded the job skips durably (the agent must stop ALL replies
            # for this source); after a partial send (e.g. before chunk 2)
            # further sends are withheld and the job fails instead -- never
            # a skip over send evidence, never a plain fallback send.
            if not is_unknown_message(exc):
                _fail_job(cid, source_mid, kind="transport_error")
                _bounded("discord_error", "send_message: reply uncertain")
            try:
                with Store.open(settings.db_path) as _store:
                    fresh = _store.get_job(cid, source_mid)
                    if fresh is not None and fresh.get("send_count", 0) == 0:
                        _store.skip_job(cid, source_mid, reason="source_deleted")
                        skipped = True
                    else:
                        skipped = False
            except StoreError:
                _fail_job(cid, source_mid, kind="transport_error")
                _bounded("delivery_error", "send_message: delivery uncertain")
            if skipped:
                return ok_fn(
                    "send_message",
                    args,
                    {
                        "status": "skipped",
                        "reason": "source_deleted",
                        "channel_id": cid,
                        "source_message_id": source_mid,
                    },
                )
            _fail_job(cid, source_mid, kind="transport_error")
            _bounded("delivery_error", "send_message: delivery uncertain")
        except Exception as exc:
            # No unthreaded fallback: Forbidden/5xx/ambiguous failures fail
            # the job -- never a blind retry, never a duplicate send.
            _fail_job(cid, source_mid, kind="transport_error")
            _bounded(_coarse_error_kind(exc), "send_message: reply uncertain")
        # The transaction guards the recording only: concurrent multi-chunk
        # sends interleave at the await above (accepted under best-effort).
        # A terminal job (or stale nonce) records nothing and raises instead
        # of duplicating; a stray success against a terminal job still
        # returns the posted receipt (the send DID happen).
        stale_terminal = False
        try:
            with Store.open(settings.db_path) as _store:
                _store.record_send(cid, source_mid, job_ref=job_ref, sent_message_id=message.id)
        except StoreError:
            try:
                with Store.open(settings.db_path) as _store:
                    fresh = _store.get_job(cid, source_mid)
                    if fresh is not None and fresh.get("stage") in ("done", "skipped"):
                        stale_terminal = True
                    else:
                        _store.mark_failed(cid, source_mid, kind="transport_error")
            except StoreError:
                pass
            if not stale_terminal:
                _bounded("delivery_error", "send_message: delivery uncertain")
        return ok_fn(
            "send_message",
            args,
            {
                "message_id": message.id,
                "channel_id": message.channel.id,
                "jump_url": message.jump_url,
                "source_message_id": source_mid,
                "reply_to_message_id": source_mid,
            },
        )

    @mcp.tool(
        description=(
            "Send a message; defaults to the configured channel. Rejects out-of-guild "
            "targets. Auto-reply: the agent calls this with the exact "
            "source_message_id/job_ref from its task; the worker never sends itself. "
            "If this tool returns status skipped with reason source_deleted, the "
            "source message was deleted: stop ALL replies for that source -- no "
            "manual/unthreaded fallback and no retries. "
            f"{identity_policy}"
        )
    )
    async def send_message(
        content: str,
        channel_id: int | None = None,
        source_message_id: StrictInt | None = None,
        job_ref: str | None = None,
    ) -> str:
        """Send a message. (Agent-facing text is the decorator description above.)

        source_message_id is StrictInt so bools/strings are rejected at the MCP
        boundary: a lax int annotation would let FastMCP coerce True -> 1 BEFORE
        the helper's bool guard runs.
        """
        cid = channel_id if channel_id is not None else settings.default_channel_id
        if source_message_id is None and job_ref is None:
            args = {"channel_id": cid}
            if isinstance(content, str):
                args["content_len"] = len(content)
            try:
                bot = bot_fn()
                guild = get_scoped_guild(bot, settings.guild_id)
                channel = await _resolve_text_channel(bot, guild, cid)
                message = await _discord_send(channel, content)
                return ok_fn(
                    "send_message",
                    args,
                    {"message_id": message.id, "channel_id": cid, "jump_url": message.jump_url},
                )
            except Exception as exc:
                fail_fn("send_message", args, exc)
        return await _correlated_send(content, cid, source_message_id, job_ref)

    @mcp.tool(
        description=(
            "Terminally declare NO reply for a correlated turn; defaults to the "
            "configured channel. Call with the exact source_message_id/job_ref from "
            "the task plus reason no_reply_needed|missing_context. Only a correlated "
            "mid-flight job with zero sends may declare; the job closes done "
            "(no_reply) AT TOOL TIME. If this tool returns status skipped with "
            "reason source_deleted, the source was deleted: stop ALL replies "
            "for that source -- no fallback and no retries. "
            f"{identity_policy}"
        )
    )
    async def record_no_reply(
        source_message_id: StrictInt,
        job_ref: str,
        reason: str,
        channel_id: int | None = None,
    ) -> str:
        """Declare no reply. (Agent-facing text is the decorator description above.)

        Only an eligible mid-flight job with zero sends may declare; the
        store enforces the guards and closes the job done (no_reply) at
        tool time. Shared MCP correlation selects the job but never
        authenticates the caller.
        """
        cid = channel_id if channel_id is not None else settings.default_channel_id
        # Pre-acceptance audit args: UNTRUSTED correlation inputs leave
        # no audit trace until the helper accepts them below.
        args: dict[str, Any] = _correlated_base_args(cid, source_message_id, job_ref, reason=reason)

        def _bounded_no_reply(kind: str, public: str) -> NoReturn:
            deny_fn("record_no_reply", args, kind, public)

        if reason not in ("no_reply_needed", "missing_context"):
            _bounded_no_reply("validation_error", "record_no_reply: invalid reason")
        accepted = _require_correlated(
            "record_no_reply",
            cid,
            source_message_id,
            job_ref,
            args,
            scope_public="record_no_reply: channel out of scope",
        )
        # Correlation ACCEPTED: the allowlist carries channel_id +
        # source_message_id + the enum reason; job_ref never appears.
        args = {**accepted, "reason": reason}
        responder = get_responder_fn()
        try:
            with Store.open(settings.db_path) as _store:
                pre = _store.get_job(cid, source_message_id)
        except StoreError as exc:
            _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: delivery uncertain")
        if pre is None or pre.get("job_ref") != job_ref:
            _bounded_no_reply("delivery_error", "record_no_reply: correlation rejected")
        if pre.get("send_count", 0) > 0:
            _bounded_no_reply("delivery_error", "record_no_reply: send evidence present")
        # Source freshness (last pre-declaration GET): a deleted source with
        # zero sends skips durably instead of declaring; unproven
        # existence never declares and never mutates.
        try:
            bot = bot_fn()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: channel unavailable")
        try:
            await check_source_message(channel, source_message_id)
            source_gone = False
        except asyncio.CancelledError:
            raise
        except SourceUnavailable:
            _bounded_no_reply("discord_error", "record_no_reply: source unavailable")
        except SourceDeleted:
            source_gone = True
        # Post-await re-validation: a send may have landed during the source
        # check (stage submitted -> sent is still eligible, but any send
        # evidence refuses the declaration). No new await runs between this
        # guard and the declare/skip below.
        try:
            responder.correlated_job(source_message_id, job_ref, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: correlation rejected")
        try:
            with Store.open(settings.db_path) as _store:
                fresh = _store.get_job(cid, source_message_id)
        except StoreError as exc:
            _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: delivery uncertain")
        if fresh is None or fresh.get("job_ref") != job_ref:
            _bounded_no_reply("delivery_error", "record_no_reply: correlation rejected")
        if fresh.get("send_count", 0) > 0:
            _bounded_no_reply("delivery_error", "record_no_reply: send evidence present")
        if source_gone:
            # Mirror the send gate: proven-no-send skip, no declaration.
            try:
                with Store.open(settings.db_path) as _store:
                    _store.skip_job(cid, source_message_id, reason="source_deleted")
            except StoreError as exc:
                _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: skip rejected")
            return ok_fn(
                "record_no_reply",
                args,
                {
                    "status": "skipped",
                    "reason": "source_deleted",
                    "channel_id": cid,
                    "source_message_id": source_message_id,
                },
            )
        # Terminal at tool time: the store enforces correlated mid-flight
        # (submitted/sent) + zero sends; the worker treats done/no_reply as
        # the turn's outcome, never re-POSTing or re-sending for this job.
        try:
            with Store.open(settings.db_path) as _store:
                _store.complete_job(cid, source_message_id, outcome="no_reply")
        except StoreError as exc:
            _bounded_no_reply(_coarse_error_kind(exc), "record_no_reply: declaration rejected")
        return ok_fn(
            "record_no_reply",
            args,
            {
                "status": "no_reply",
                "channel_id": cid,
                "source_message_id": source_message_id,
                "job_ref": job_ref,
                "reason": reason,
            },
        )

    @mcp.tool(
        description=(
            "Read approved-channel context for a correlated turn; defaults to the "
            "configured channel. Returns up to `limit` (1..20) complete messages "
            "before the source (or before `before_message_id` when paginating "
            "backwards, never future messages). Thyca-only additional context -- "
            "never auto-forwarded to the classifier. "
            f"{identity_policy}"
        )
    )
    async def read_context(
        source_message_id: StrictInt,
        job_ref: str,
        before_message_id: StrictInt | None = None,
        limit: StrictInt = 20,
        channel_id: int | None = None,
    ) -> str:
        """Read channel context. (Agent-facing text is the decorator description.)

        Only a correlated active job may read, and only from the scoped
        channel/guild (no other channel, DM, or profile fetch). Shared MCP
        correlation selects the job but never authenticates the caller.
        """
        cid = channel_id if channel_id is not None else settings.default_channel_id
        args: dict[str, Any] = _correlated_base_args(cid, source_message_id, job_ref)

        def _bounded_ctx(kind: str, public: str) -> NoReturn:
            deny_fn("read_context", args, kind, public)

        if before_message_id is not None and (
            not isinstance(before_message_id, int)
            or isinstance(before_message_id, bool)
            or before_message_id <= 0
        ):
            _bounded_ctx("validation_error", "read_context: invalid before_message_id")
        if (
            before_message_id is not None
            and isinstance(source_message_id, int)
            and before_message_id > source_message_id
        ):
            _bounded_ctx("validation_error", "read_context: before_message_id is future")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 20:
            _bounded_ctx("validation_error", "read_context: limit must be 1..20")
        accepted = _require_correlated(
            "read_context",
            cid,
            source_message_id,
            job_ref,
            args,
            scope_public="read_context: channel out of scope",
        )
        # Correlation ACCEPTED: the allowlist carries channel_id +
        # source_message_id only (message bodies never touch the audit log).
        args = dict(accepted)
        responder = get_responder_fn()
        try:
            bot = bot_fn()
            guild = get_scoped_guild(bot, settings.guild_id)
            channel = await _resolve_text_channel(bot, guild, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded_ctx(_coarse_error_kind(exc), "read_context: channel unavailable")
        bound = before_message_id if before_message_id is not None else source_message_id
        try:
            collected = [
                m
                async for m in channel.history(
                    limit=limit, before=discord.Object(bound), oldest_first=False
                )
            ]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded_ctx(_coarse_error_kind(exc), "read_context: history unavailable")
        collected.reverse()  # old->new chronological context
        # Post-await freshness: never serve context for a job that moved
        # terminal during the fetch.
        try:
            responder.correlated_job(source_message_id, job_ref, cid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _bounded_ctx(_coarse_error_kind(exc), "read_context: correlation rejected")
        messages: list[dict[str, Any]] = []
        for m in collected:
            # Typed SDK reads: history of this channel can only hold its own
            # messages with native int ids, str bodies, and datetimes. Scope
            # mismatches and future rows are selection-policy refusals, not
            # SDK distrust.
            if m.id >= bound:
                _bounded_ctx("validation_error", "read_context: future message")
            if m.channel.id != cid:
                _bounded_ctx("validation_error", "read_context: channel mismatch")
            if m.guild is None or m.guild.id != settings.guild_id:
                _bounded_ctx("validation_error", "read_context: guild mismatch")
            ref = m.reference
            reply_to = ref.message_id if ref is not None else None
            messages.append(
                {
                    "message_id": m.id,
                    "author_uid": m.author.id,
                    "author_name": m.author.display_name,
                    "content": m.content,
                    "reply_to_message_id": reply_to,
                    "timestamp": m.created_at.isoformat(),
                }
            )
        return ok_fn(
            "read_context",
            args,
            {
                "source_message_id": source_message_id,
                "job_ref": job_ref,
                "channel_id": cid,
                "messages": messages,
                "count": len(messages),
            },
        )
