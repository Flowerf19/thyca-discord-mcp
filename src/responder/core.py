"""FIFO worker for agent-owned MCP auto-reply.

Flow: outbox (capture-only) -> FIFO worker -> target bot DM session
(agent calls MCP ``send_message``) -> controller ACK (safe processed
prefix).

This module never sends Discord replies itself (no bridge-owned SDK send)
and never runs a scheduler. Scheduling is owned by the server via native
``discord.ext.tasks.Loop``; this worker only exposes ``tick``/``close``
coroutines plus sync guards. Every tick resolves the target bot's fixed
DM session (``GET /api/bots/<bot_id>/dm``) and adopts it when the
serving SID trails: convergence is drain-gated (no ``submitted``/``sent``
job may be mid-flight) and transient-deferring (a native blip defers the
whole tick instead of halting). A turn posted into the DM session runs as
that bot, so the user reads auto-replies directly in the bot's DM chat
with no manual delegate step. Old sessions are never deleted.

Durability: all state lives in the single SQLite store
(:mod:`store`); every mutation is a short explicit transaction, never
held across an ``await`` (Jev paid call, Thyca POST, and Discord send sit
between transactions by design). Single-process / single-loop only. No
threads, no new deps.

Best-effort delivery: no intent/receipt journal, no uncertain holds. A
crash exactly between a Discord send / Thyca POST and its state update
may rarely duplicate or drop one reply. Ambiguous outcomes fail
(``failed``), are logged, and moved on from -- never blindly re-POSTed
or re-sent. Restart fails jobs left ``submitted`` (never resumes or
re-POSTs them); ``queued`` jobs resume normally.

No work happens on import: constructing :class:`Responder` opens the
store (quick_check + user_version gate, fail loud), runs the restart
sweep once, and loads the serving SID (missing FAILS, never
auto-creates). Network happens only inside ``tick`` via the injected
:class:`ThycaClient` (or a fake in tests).
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import discord

from config import Settings
from context import build_context_collector
from jev import JevAdapter
from participation import ParticipationLedger
from source_messages import SourceDeleted, build_source_checker
from store import Store, StoreError
from thyca import (
    ThycaClient,
    ThycaClientError,
    ThycaPreconditionError,
    validate_day,
    validate_native_sid,
)

from .input import (
    ResponderError,
    _is_strict_int,
    _is_strict_positive_int,
    _validate_id_string,
    validate_entry,
)
from .submit import (  # noqa: F401 - intentional canonical re-exports
    _VALID_PARTICIPATIONS,
    ResponderSubmitMixin,
    _sha256_hex,
)

READ_LIMIT = 500
PROMPT_MAX = 4000
CONTENT_MAX = 2000

# Canonical mid-flight set: stages that authorize a correlated send.
_MID_FLIGHT_STAGES = frozenset({"submitted", "sent"})
# Every terminal ACKs (failed included): ambiguous outcomes are failed,
# logged, and moved on from -- nothing head-blocks for an operator.
_ALL_TERMINAL = frozenset({"done", "skipped", "failed"})
_SAFE_TERMINAL = _ALL_TERMINAL

# Jev routing: participation values that POST to native Thyca. ``observe``
# never POSTs (durable zero-send terminal instead).
_ROUTED_POST_PARTICIPATIONS = frozenset({"respond", "need_context"})
# _VALID_PARTICIPATIONS canonical in responder_submit; _sha256_hex canonical
# in validate (both imported above for internal use).


def _ho_chi_minh_today() -> str:
    """Current calendar day in Asia/Ho_Chi_Minh (adoption history key).

    The day only keys the converged ``sessions`` provenance; the wall
    clock never gates adoption itself. Tests inject a fake day via
    ``session_day_provider``."""
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()


def _is_transient_native_error(exc: ThycaClientError) -> bool:
    """True for maybe-later native failures (timeout/disconnect/busy/5xx).

    A single blip during the DM resolve must not stop the auto-reply loop:
    the previous session stays valid, so the tick defers and retries next
    tick. Definitive failures (malformed shapes, 400s, verify mismatch,
    store refusals) fail closed instead.
    """
    from thyca import ThycaBusyError, ThycaProtocolError, ThycaSubmissionUncertainError

    if isinstance(exc, (ThycaSubmissionUncertainError, ThycaBusyError)):
        return True
    return isinstance(exc, ThycaProtocolError) and exc.reason in (
        "timeout",
        "disconnect",
        "http_5xx",
    )


def _is_native_not_found(exc: ThycaClientError) -> bool:
    """True only for a definitive native HTTP 404 (session verifiably gone)."""
    return isinstance(exc, ThycaPreconditionError) and exc.status_code == 404


class Responder(ResponderSubmitMixin):
    """FIFO worker + ACK controller (no sends, no scheduler, no session mgmt)."""

    def __init__(
        self,
        settings: Settings,
        bot: discord.Client,
        *,
        client: ThycaClient | Any | None = None,
        source_checker: Any | None = None,
        jev_adapter: Any | None = None,
        context_collector: Any | None = None,
        participation_ledger: Any | None = None,
        session_day_provider: Callable[[], str] | None = None,
    ) -> None:
        self._settings = settings
        self._bot = bot
        self._busy = False
        self._closed = False
        if session_day_provider is None:
            # Production default: real-clock Ho Chi Minh day for the
            # adoption history key. The clock never gates adoption itself;
            # tests inject a fake day.
            self._session_day_provider: Callable[[], str] = _ho_chi_minh_today
        else:
            if not callable(session_day_provider):
                raise ResponderError("session_day_provider must be callable")
            self._session_day_provider = session_day_provider
        if source_checker is None:
            # Production default: the REAL bot-backed existence proof (live
            # SDK reads), never a permissive no-op. Tests inject a fake.
            self._source_checker = build_source_checker(bot, settings.guild_id)
        else:
            if not callable(source_checker):
                raise ResponderError("source_checker must be callable")
            self._source_checker = source_checker
        # Startup: open the store (quick_check + user_version gate, fail
        # loud on corruption -- never auto-reset), run the restart sweep
        # ONCE (submitted -> failed; queued resumes), then load the serving
        # SID (missing FAILS -- deploy seeds past old history; never
        # auto-create). The sweep lives here, not in Store.open: opens
        # happen per operation, and sweeping on every open would fail live
        # submitted jobs.
        try:
            with Store.open(settings.db_path) as _store:
                _store.sweep_submitted_failed()
                stored_sid = _store.get_native_sid()
        except StoreError:
            raise
        if stored_sid is None or validate_native_sid(stored_sid) is None:
            raise ResponderError("native session not seeded; adopt before start")
        self._native_sid = stored_sid
        # Deferral note day: a transient DM-resolve failure warns once per
        # logical day (never 1 Hz spam); a new day re-arms automatically.
        self._target_deferred_day: str | None = None
        if client is None:
            self._client: Any = ThycaClient(
                base_url=settings.thyca_base_url,
                request_timeout_seconds=settings.thyca_request_timeout_seconds,
            )
        else:
            self._client = client
        # Jev routing dependencies (no opt-out, no legacy fallback): injected
        # fakes in tests, real path by default. Built LAST so existing
        # client validations keep their error precedence.
        if jev_adapter is None:
            key = settings.typesafe_api_key
            if key is None:
                raise ResponderError("jev api key missing; configure typesafe_api_key")
            self._jev_adapter: Any = JevAdapter(api_key=key.get_secret_value())
        else:
            if not callable(getattr(jev_adapter, "decide", None)):
                raise ResponderError("jev_adapter must provide async decide(state)")
            self._jev_adapter = jev_adapter
        if context_collector is None:
            self._context_collector: Any = build_context_collector(
                bot,
                settings.guild_id,
                settings.default_channel_id,
                settings.owner_uid,
                max_messages=settings.conversation_max_messages,
            )
        else:
            if not callable(context_collector):
                raise ResponderError("context_collector must provide async collector")
            self._context_collector = context_collector
        if participation_ledger is None:
            # Owned ledger over the same DB (no files, no activation
            # marker, no binding dance, no loss guard -- one database
            # deletes that failure class by construction). The ledger takes
            # no SID: per-record SID validation died with the split files.
            self._participation_ledger: Any = ParticipationLedger(settings.db_path)
            self._participation_owned = True
        else:
            if not callable(getattr(participation_ledger, "classify", None)):
                raise ResponderError("participation_ledger must provide async classify")
            self._participation_ledger = participation_ledger
            self._participation_owned = False

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def native_sid(self) -> str:
        return self._native_sid

    def _bot_uid(self) -> int:
        user = self._bot.user
        if user is None:
            raise ResponderError("bot not ready")
        return user.id

    def _open_store(self) -> Store:
        """Open the store (fail loud on corruption); callers close promptly."""
        return Store.open(self._settings.db_path)

    def guard_ack(self, up_to_seq: int) -> None:
        """Allow ``up_to_seq`` only at/below the safe processed prefix.

        Returns None when safe (including no-op at/below current acked).
        Raises bounded :class:`ResponderError` when beyond the safe prefix
        (never silently clamps). Corrupt store fails loudly.
        """
        if not _is_strict_int(up_to_seq) or up_to_seq < 0:
            raise ResponderError("guard_ack requires seq >= 0")
        with self._open_store() as _store:
            data = _store.read_entries(limit=READ_LIMIT, unread_only=True)
            safe = _store.processed_prefix(data["entries"], data["acked"])
        if up_to_seq > safe:
            raise ResponderError("ack beyond safe prefix")

    def correlated_job(
        self, source_message_id: int, job_ref: str, channel_id: int
    ) -> dict[str, Any]:
        """Validate exact correlation for the future MCP send helper (no send).

        Requires scoped channel, exact ``job_ref`` nonce match, and a
        send-eligible stage (``submitted``/``sent`` only -- queued jobs have
        no turn running, terminals never re-send). Returns bounded metadata
        only (no raw, no unknown keys).
        """
        if not _is_strict_positive_int(source_message_id):
            raise ResponderError("correlated_job has invalid source_message_id")
        if not _is_strict_positive_int(channel_id):
            raise ResponderError("correlated_job has invalid channel_id")
        if _validate_id_string(job_ref) is None:
            raise ResponderError("correlated_job has invalid job_ref")
        if channel_id != self._settings.default_channel_id:
            raise ResponderError("correlated_job channel out of scope")
        try:
            with self._open_store() as _store:
                job = _store.get_job(channel_id, source_message_id)
        except StoreError as exc:
            raise ResponderError(f"correlated_job store unavailable: {exc}") from None
        if job is None:
            raise ResponderError("correlated_job unknown job")
        if job.get("job_ref") != job_ref:
            raise ResponderError("correlated_job job_ref mismatch")
        if job.get("stage") not in _MID_FLIGHT_STAGES:
            raise ResponderError("correlated_job stage not eligible")
        return {
            "channel_id": job["channel_id"],
            "message_id": job["message_id"],
            "job_ref": job["job_ref"],
            "stage": job["stage"],
            "send_count": job["send_count"],
        }

    async def tick(self) -> dict[str, Any]:
        """Single FIFO driver: ingest up to 500, submit/reconcile one, ACK safe."""
        if self._closed:
            raise ResponderError("responder is closed")
        if self._busy:
            return {"status": "busy"}
        self._busy = True
        try:
            return await self._tick_once()
        finally:
            self._busy = False

    async def close(self) -> None:
        """Fail mid-flight jobs on shutdown (ambiguous => failed, never retry).

        Queued jobs stay queued; every ``submitted``/``sent`` job becomes
        ``failed`` exactly once (already-terminal jobs keep their recorded
        state). Committed sends are preserved, never rewound.
        """
        self._closed = True
        try:
            with self._open_store() as _store:
                mid_flight = _store.list_jobs(stages=_MID_FLIGHT_STAGES)
        except StoreError as exc:
            raise ResponderError(f"responder close store unavailable: {exc}") from None
        for job in mid_flight:
            self._mark_failed(job["channel_id"], job["message_id"], kind="shutdown")

    # -- internal helpers (sync fresh loads between awaits, no stale saves) --

    def _first_unresolved(self) -> dict[str, Any] | None:
        try:
            with self._open_store() as _store:
                return _store.first_unresolved()
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None

    def _ack_safe_prefix(self, entries: list[dict[str, Any]], acked: int) -> int:
        try:
            with self._open_store() as _store:
                safe = _store.processed_prefix(entries, acked)
                if safe > acked:
                    _store.ack_entries(safe)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        return safe

    async def _maybe_adopt_target_dm(self) -> dict[str, Any] | None:
        """Resolve the target bot DM and adopt it; None = proceed tick.

        Every tick resolves ``GET /api/bots/<target>/dm`` BEFORE any
        submit/reconcile work. When the DM SID trails the serving SID the
        tick converges onto it (drain-gated: no ``submitted``/``sent`` job
        may be mid-flight) and proceeds into the DM normally -- so Discord
        messages auto-flow into the target bot's DM every tick with no
        manual delegate step.

        Returns None when the serving SID already is the DM (proceed).
        Returns a pending outcome (no job mutation, no submit) when the
        resolve transiently fails or jobs block adoption; the next tick
        retries. Raises :class:`ResponderError` on a definitive 404
        (bad target bot config) or any state mismatch (fail closed).
        SIDs never reach logs.
        """
        try:
            today = self._session_day_provider()
        except Exception:
            raise ResponderError("session day provider failed") from None
        if validate_day(today) is None:
            raise ResponderError("session day provider returned invalid day")
        target = self._settings.target_bot_id
        try:
            dm = await self._client.get_bot_dm(target)
        except ThycaClientError as exc:
            if _is_native_not_found(exc):
                raise ResponderError("target bot DM missing") from None
            if _is_transient_native_error(exc):
                return self._defer_target_dm(today, exc.kind)
            raise ResponderError("target bot DM resolve failed") from None
        if not isinstance(dm, dict):
            raise ResponderError("target bot DM resolve failed")
        dm_sid = dm.get("native_sid")
        if validate_native_sid(dm_sid) is None:
            raise ResponderError("target bot DM resolve failed")
        if dm_sid == self._native_sid:
            return None
        if not await self._converge_target_dm(dm_sid, today):
            return self._defer_target_dm(today, "jobs_blocked", reason="target_dm_blocked")
        return None

    def _defer_target_dm(
        self, today: str, kind: str, *, reason: str = "target_dm_deferred"
    ) -> dict[str, Any]:
        """Pending whole-tick deferral with one stderr note per day.

        Transient blips defer as ``target_dm_deferred``; jobs blocking
        adoption defer as ``target_dm_blocked`` (the tick still runs one
        reconcile-only pass for the head blocker before returning).
        """
        if self._target_deferred_day != today:
            print(
                f"thyca-discord-mcp: target DM adoption deferred for {today} ({kind})",
                file=sys.stderr,
            )
            self._target_deferred_day = today
        return {"status": "pending", "reason": reason}

    async def _converge_target_dm(self, dm_sid: str, today: str) -> bool:
        """Converge onto ``dm_sid`` in ONE store transaction; True on success.

        Returns False (no writes) when a job is mid-flight
        (``submitted``/``sent``) -- the caller defers with a reconcile-only
        pass for the head blocker and retries adoption next tick.
        Raises :class:`ResponderError` on any state mismatch or store
        refusal (fail closed). The ``sessions`` table keeps every adopted
        day as provenance; the old session still exists, retired from
        service only. The owned ledger needs no swap (same DB, no SID
        binding).
        """
        old_sid = self._native_sid
        try:
            with self._open_store() as _store:
                current = _store.get_native_sid()
        except StoreError as exc:
            raise ResponderError(f"target DM adoption store unavailable: {exc}") from None
        if current != old_sid:
            raise ResponderError("delivery native_session_id mismatch")
        try:
            with self._open_store() as _store:
                mid_flight = _store.list_jobs(stages=_MID_FLIGHT_STAGES)
        except StoreError as exc:
            raise ResponderError(f"target DM adoption store unavailable: {exc}") from None
        if mid_flight:
            return False
        try:
            with self._open_store() as _store:
                _store.adopt_session(expected_current=old_sid, new_sid=dm_sid, day=today)
        except StoreError as exc:
            raise ResponderError(f"target DM adoption refused: {exc}") from None
        self._native_sid = dm_sid
        print(
            f"thyca-discord-mcp: adopted target bot DM session for {today}",
            file=sys.stderr,
        )
        return True

    async def _tick_once(self) -> dict[str, Any]:
        # DM-only routing: resolve + adopt the target bot DM before any
        # submit/reconcile work. A transient deferral holds the whole tick
        # (no job mutation, no submit) and the next tick retries. A
        # jobs-blocked deferral additionally runs ONE reconcile-only pass
        # for the head blocker on the CURRENT (old) SID -- reconcile only,
        # never a submit, never classify -- so a submitted blocker can
        # drain and adoption can converge next tick.
        target_hold = await self._maybe_adopt_target_dm()
        if target_hold is not None and target_hold.get("reason") != "target_dm_blocked":
            with self._open_store() as _store:
                data = _store.read_entries(limit=READ_LIMIT, unread_only=True)
            safe = self._ack_safe_prefix(data["entries"], data["acked"])
            target_hold["safe_prefix"] = safe
            target_hold["acked"] = safe
            target_hold["latest"] = data["latest"]
            return target_hold
        with self._open_store() as _store:
            data = _store.read_entries(limit=READ_LIMIT, unread_only=True)
        entries: list[dict[str, Any]] = data["entries"]
        acked: int = data["acked"]
        latest: int = data["latest"]
        try:
            with self._open_store() as _store:
                current_sid = _store.get_native_sid()
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if current_sid != self._native_sid:
            raise ResponderError("delivery native_session_id mismatch")
        bot_uid = self._bot_uid()
        # Strict frame validation for EVERY entry BEFORE any
        # return/side effect: a corrupt source frame fails the tick with
        # zero state mutation.
        for entry in entries:
            self._validate_entry(entry, bot_uid)
        for entry in entries:
            await self._ingest_entry(entry, bot_uid)
        candidate = self._first_unresolved()
        if candidate is None:
            safe = self._ack_safe_prefix(entries, acked)
            return {
                "status": "idle",
                "safe_prefix": safe,
                "acked": safe,
                "latest": latest,
            }
        stage = candidate.get("stage")
        if target_hold is not None:
            # Jobs-blocked adoption deferral only (transient holds return
            # above): one reconcile-only pass for the head blocker on the
            # CURRENT (old) SID so it can drain toward terminal and
            # adoption can converge next tick. Never a submit, never
            # classify: queued candidates are left queued.
            if stage != "queued":
                await self._reconcile_job(candidate)
            with self._open_store() as _store:
                fresh = _store.read_entries(limit=READ_LIMIT, unread_only=True)
            safe = self._ack_safe_prefix(fresh["entries"], fresh["acked"])
            target_hold["safe_prefix"] = safe
            target_hold["acked"] = safe
            target_hold["latest"] = fresh["latest"]
            return target_hold
        if stage == "queued":
            outcome = await self._submit_queued(candidate, entries)
        else:
            outcome = await self._reconcile_job(candidate)
        # Always advance the safe prefix past newly terminal jobs.
        with self._open_store() as _store:
            fresh = _store.read_entries(limit=READ_LIMIT, unread_only=True)
        safe = self._ack_safe_prefix(fresh["entries"], fresh["acked"])
        outcome["safe_prefix"] = safe
        outcome["acked"] = safe
        outcome["latest"] = fresh["latest"]
        return outcome

    def _validate_entry(self, entry: Any, bot_uid: int) -> dict[str, Any]:
        """Thin wrapper over the pure input validator (explicit UIDs)."""
        return validate_entry(entry, bot_uid=bot_uid, owner_uid=self._settings.owner_uid)

    async def _ingest_entry(self, entry: dict[str, Any], bot_uid: int) -> None:
        """Insert the job row for a new outbox row (filtered ones pre-skipped).

        Already-ingested rows (same channel+message) return immediately:
        the outbox UNIQUE key means redelivery resolves to the same row,
        so no contradiction or alias bookkeeping is possible. Scope,
        self, and bot filters insert directly as ``skipped`` (ingest never
        enqueues-then-skips). Legacy frames without ``author_is_bot`` keep
        the job queued with an SDK bot-flag resolve; an unresolvable flag
        defers to the submit path (pending, retried next tick).
        """
        frame = self._validate_entry(entry, bot_uid)
        channel_id = frame["channel_id"]
        message_id = frame["message_id"]
        actor_kind = frame["actor_kind"]
        try:
            with self._open_store() as _store:
                existing = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if existing is not None:
            return
        # New MID: scope, self/bot filters, then eligible human enqueue.
        if channel_id != self._settings.default_channel_id:
            self._enqueue_and_skip(channel_id, message_id, entry["seq"], "out_of_scope")
            return
        if entry["author_uid"] == bot_uid:
            # Self wins by UID regardless of any author_is_bot value.
            self._enqueue_and_skip(channel_id, message_id, entry["seq"], "bot_filtered")
            return
        if "author_is_bot" in entry:
            flag = entry["author_is_bot"]  # strict bool, validated above
            if flag is True:
                self._enqueue_and_skip(channel_id, message_id, entry["seq"], "bot_filtered")
                return
            if actor_kind not in ("owner", "other"):
                raise ResponderError("outbox entry actor mismatch")
            self._enqueue(channel_id, message_id, entry["seq"])
            return
        # Legacy frame without the flag: resolve the ACTUAL bot flag via SDK
        # cache then guild/native fetch (never assumed False). The submit
        # path re-checks before any paid call; an unresolvable UID defers
        # there (pending, retried next tick) instead of stranding the job.
        resolved = await self._resolve_bot_flag(entry["author_uid"])
        if resolved is True:
            self._enqueue_and_skip(channel_id, message_id, entry["seq"], "bot_filtered")
            return
        if actor_kind not in ("owner", "other"):
            raise ResponderError("outbox entry actor mismatch")
        self._enqueue(channel_id, message_id, entry["seq"])

    def _enqueue(self, channel_id: int, message_id: int, seq: int) -> None:
        try:
            with self._open_store() as _store:
                _store.enqueue_job(channel_id, message_id, seq)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None

    def _enqueue_and_skip(self, channel_id: int, message_id: int, seq: int, reason: str) -> None:
        try:
            with self._open_store() as _store:
                _store.enqueue_job(channel_id, message_id, seq, skip_reason=reason)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None

    async def _resolve_bot_flag(self, author_uid: int) -> bool | None:
        """Resolve legacy unknown ``author_is_bot`` via SDK cache + fetch.

        Cache first (guild member, then user), then one guild fetch and one
        user fetch. Each step failing means "try the next"; an unresolvable
        UID returns None (fail-closed: the caller defers, never assumes).
        Cancellation propagates untouched.
        """
        try:
            guild: discord.Guild | None = None
            try:
                guild = self._bot.get_guild(self._settings.guild_id)
            except Exception:
                guild = None
            if guild is not None:
                try:
                    member = guild.get_member(author_uid)
                except Exception:
                    member = None
                if member is not None:
                    return member.bot
            try:
                user = self._bot.get_user(author_uid)
            except Exception:
                user = None
            if user is not None:
                return user.bot
            if guild is not None:
                try:
                    member = await guild.fetch_member(author_uid)
                except Exception:
                    member = None
                if member is not None:
                    return member.bot
            try:
                user = await self._bot.fetch_user(author_uid)
            except Exception:
                user = None
            if user is not None:
                return user.bot
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        return None

    def _mark_failed(
        self, channel_id: int, message_id: int, *, kind: str, http_status: int | None = None
    ) -> None:
        """Persist a durable ambiguous-outcome failure exactly once.

        Always re-reads the job fresh first. Terminal jobs keep their
        recorded state (completed/skipped/failed are never rewound).
        Store refusals surface as bounded errors; they are never hidden.
        """
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            raise ResponderError("job missing for fail mark")
        if fresh.get("stage") in _ALL_TERMINAL:
            return
        try:
            with self._open_store() as _store:
                _store.mark_failed(channel_id, message_id, kind=kind, http_status=http_status)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None

    def _fresh_source_deleted_skip(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return the fresh job when it is a durable source_deleted skip.

        Matches only stage ``skipped`` with reason ``source_deleted``.
        Skips are refused any send evidence at write time, so the stage
        alone is the proof. Late native/POST outcomes must never rewrite,
        retry, or complete such a job.
        """
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            return None
        if fresh.get("stage") != "skipped" or fresh.get("skip_reason") != "source_deleted":
            return None
        return fresh

    def _source_deleted_status(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Skipped status for a durable source_deleted skip."""
        if self._fresh_source_deleted_skip(channel_id, message_id) is not None:
            return {"status": "skipped", "reason": "source_deleted"}
        return None

    def _skip_source_deleted(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Fresh guarded skip for a confirmed-deleted source.

        ``skip_job`` itself refuses ANY send evidence or a terminal stage.
        When the skip is refused (a send landed first, or the MCP gate
        already terminalized the job), a durable source_deleted skip still
        wins and other terminals are respected; otherwise the job fails
        (ambiguous => failed, never a blind retry, never a forced skip).
        """
        try:
            with self._open_store() as _store:
                _store.skip_job(channel_id, message_id, reason="source_deleted")
        except StoreError:
            # A send landed first, or the MCP gate already terminalized the
            # job: never force the skip. A durable source_deleted skip
            # still wins; other terminals are respected; otherwise fail.
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                with self._open_store() as _store:
                    fresh = _store.get_job(channel_id, message_id)
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            if fresh is not None and fresh.get("stage") in _ALL_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "send_evidence_present"}
        return {"status": "skipped", "reason": "source_deleted"}

    def _skip_source_edited(self, channel_id: int, message_id: int) -> dict[str, Any]:
        """Fresh guarded skip for a source edited after classification.

        The live snapshot proves the source text changed after the paid
        classification bound its fingerprint; answering the stale text
        would be wrong and reclassification would rebill, so the stranded
        job converges to this terminal instead of pending forever.
        ``skip_job`` itself refuses ANY send evidence or a non-eligible
        stage. When the skip is refused, a durable source_deleted skip
        still wins and other terminals are respected; otherwise fail
        (never force the skip).
        """
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            with self._open_store() as _store:
                _store.skip_job(channel_id, message_id, reason="source_edited_after_classify")
        except StoreError:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            try:
                with self._open_store() as _store:
                    fresh = _store.get_job(channel_id, message_id)
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            if fresh is not None and fresh.get("stage") in _ALL_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            self._mark_failed(channel_id, message_id, kind="transport_error")
            return {"status": "failed", "reason": "edit_skip_refused"}
        return {"status": "skipped", "reason": "source_edited_after_classify"}

    async def _reconcile_after_post(
        self,
        channel_id: int,
        message_id: int,
        pre_max: int | None,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """Close a just-posted turn WITHOUT trace proof (snapshot fallback only).

        Reloads the job fresh: a durable source_deleted skip recorded
        during the POST await stands as-is. A result that is still
        running holds ``submitted`` (pending/turn_running) EVEN when the
        agent's send already landed -- the send stands, but only an idle
        :meth:`_reconcile_job` poll may complete it to ``done``. A
        ``done`` outcome stands as-is. Otherwise the POST result itself
        must prove a clean idle turn -- ``completed`` and not
        ``cancelled``, native id match, ``running`` is False, stored text
        match, and ``turn_index`` == expected (``pre_max + 1``, or 0 for
        a known-empty session). A completed idle turn with no send and no
        ``record_no_reply`` closes as ``failed`` (reason
        ``turn_no_outcome``) -- never ``done``: both routed prompts
        obligate one of the two calls.
        """
        expected = (pre_max + 1) if pre_max is not None else 0
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            raise ResponderError("job missing after post")
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        if result.get("running"):
            # Still running: hold submitted even when the agent's send
            # already landed (the send stands; only the idle poll may
            # complete it). Native id/cancelled outcome checks below apply
            # to finished turns only; a sent/done stage is closed by the
            # idle poll, never here.
            return {"status": "pending", "reason": "turn_running"}
        stage = fresh.get("stage")
        if stage == "done":
            return {"status": fresh.get("outcome") or "idle"}
        if stage == "sent":
            try:
                with self._open_store() as _store:
                    _store.complete_job(channel_id, message_id, outcome="sent")
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            return {"status": "sent"}
        if stage != "submitted":
            if stage in _ALL_TERMINAL:
                return {"status": "idle", "reason": "already_terminal"}
            raise ResponderError("job missing submit marker")
        if result.get("cancelled") or not result.get("completed"):
            self._mark_failed(channel_id, message_id, kind="post_cancelled")
            return {"status": "failed", "reason": "post_cancelled"}
        if not result.get("native_id_match"):
            self._mark_failed(channel_id, message_id, kind="snapshot_mismatch")
            return {"status": "failed", "reason": "post_sid_mismatch"}
        if not result.get("stored_text_match"):
            self._mark_failed(channel_id, message_id, kind="turn_no_outcome")
            return {"status": "failed", "reason": "turn_no_outcome"}
        turn_index = result.get("turn_index")
        if not _is_strict_int(turn_index) or turn_index != expected:
            self._mark_failed(channel_id, message_id, kind="turn_no_outcome")
            return {"status": "failed", "reason": "turn_no_outcome"}
        self._mark_failed(channel_id, message_id, kind="turn_no_outcome")
        return {"status": "failed", "reason": "turn_no_outcome"}

    async def _reconcile_job(self, job: dict[str, Any]) -> dict[str, Any]:
        """Poll a ``submitted``/``sent`` job until idle, then close it.

        Proven-no-send gate for zero-send jobs: when the serving turn
        produced no send evidence yet and the source is now verifiably
        gone, converge to the durable source_deleted skip here (the MCP
        pre-send gate only fires on send attempts, so without this check
        the job would strand with the FIFO head blocked). Otherwise poll
        ``get_session_snapshot``: while ``running``, stay pending; once
        idle, a recorded send/no-reply stands, else the turn ended with
        no outcome and the job fails (``turn_no_outcome``). Snapshot
        errors fail the job only when definitive (404 gone, id mismatch);
        transient failures stay pending for the next tick. Never re-POSTs,
        never re-sends.
        """
        channel_id = job.get("channel_id") if isinstance(job, dict) else None
        message_id = job.get("message_id") if isinstance(job, dict) else None
        if not _is_strict_positive_int(channel_id) or not _is_strict_positive_int(message_id):
            raise ResponderError("reconcile job has invalid ids")
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            raise ResponderError("job missing for reconcile")
        stage = fresh.get("stage")
        if stage in _ALL_TERMINAL:
            # A durable source_deleted skip reports itself explicitly;
            # other terminals keep the accepted idle signal.
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            return {"status": "idle", "reason": "already_terminal"}
        if stage == "queued":
            return {"status": "pending", "reason": "not_queued"}
        if stage not in ("submitted", "sent"):
            raise ResponderError("job stage not reconcilable")
        # Proven-no-send gate: zero sends + verifiably deleted source.
        if fresh.get("send_count", 0) == 0:
            try:
                await self._source_checker(channel_id, message_id)
            except asyncio.CancelledError:
                raise
            except SourceDeleted:
                return self._skip_source_deleted(channel_id, message_id)
            except Exception:
                return {"status": "pending", "reason": "source_unavailable"}
        try:
            snap = await self._client.get_session_snapshot(self._native_sid)
        except ThycaClientError as exc:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            if _is_native_not_found(exc):
                # The serving session is verifiably gone: the turn cannot
                # complete. Fail (a racing late send correlation-rejects
                # against the failed stage instead of duplicating).
                self._mark_failed(channel_id, message_id, kind="transport_error", http_status=404)
                return {
                    "status": "failed",
                    "reason": "snapshot_gone",
                    "error_kind": exc.kind,
                    "error_status": exc.status_code,
                }
            return {
                "status": "pending",
                "reason": "snapshot_unavailable",
                "error_kind": exc.kind,
                "error_status": exc.status_code,
            }
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        if not snap.get("native_id_match"):
            self._mark_failed(channel_id, message_id, kind="snapshot_mismatch")
            return {"status": "failed", "reason": "snapshot_mismatch"}
        if snap.get("running"):
            return {"status": "pending", "reason": "turn_running"}
        pre_max = snap.get("pre_max_turn_index")
        if pre_max is not None and (not _is_strict_int(pre_max) or pre_max < 0):
            return {"status": "pending", "reason": "snapshot_malformed"}
        # The snapshot await above is the last yield before closing: a
        # durable source_deleted skip wins over the idle close, and a
        # recorded send/no-reply stands.
        skipped = self._source_deleted_status(channel_id, message_id)
        if skipped is not None:
            return skipped
        try:
            with self._open_store() as _store:
                fresh = _store.get_job(channel_id, message_id)
        except StoreError as exc:
            raise ResponderError(f"responder store unavailable: {exc}") from None
        if fresh is None:
            raise ResponderError("job missing for reconcile")
        if fresh.get("stage") in _ALL_TERMINAL:
            skipped = self._source_deleted_status(channel_id, message_id)
            if skipped is not None:
                return skipped
            if fresh.get("stage") == "done":
                return {"status": fresh.get("outcome") or "idle"}
            return {"status": "idle", "reason": "already_terminal"}
        if fresh.get("send_count", 0) >= 1:
            try:
                with self._open_store() as _store:
                    _store.complete_job(channel_id, message_id, outcome="sent")
            except StoreError as exc:
                raise ResponderError(f"responder store unavailable: {exc}") from None
            return {"status": "sent"}
        # Idle snapshot, zero sends, no durable skip: the serving turn
        # ended with no outcome. Any turn-index mismatch (a manual turn
        # interleaved, a pruned session, ...) closes the same way --
        # failed, never re-POSTed, never stranded pending forever. Sent
        # jobs already closed above; only the zero-send case reaches here.
        self._mark_failed(channel_id, message_id, kind="turn_no_outcome")
        return {"status": "failed", "reason": "turn_no_outcome"}
