"""MCP observation-tool tests: record_no_reply + read_context (14-tool server).

Drives the REAL installed lifespan (patched only to inject a fake Discord
bot and fake Jev deps into the otherwise-REAL Responder) via the native
FastMCP ``list_tools``/``call_tool`` API. The store is REAL (tmp files);
seeded mid-flight jobs are submitted with a real job_ref. No network, no
real .env, no paid calls, no prod writes.

Seeds land INSIDE the running lifespan: the Responder constructor runs
the restart sweep once (submitted -> failed), so a pre-seeded submitted
job would be swept before any tool runs -- exactly as in production,
where jobs become submitted while the server runs.

Channel history is a SEMANTIC fake: it honors ``before``/``after``/
``limit``/``oldest_first`` exactly like the verified native behavior
(``oldest_first=False`` + ``before`` yields newest-first below the bound;
``oldest_first=True`` pages forward from ``after``/start, ignoring
``before``). A regression test proves ``read_context`` returns
source-adjacent recents, never oldest-ever rows.

Deleted (reasons): the ``resolve_uncertain_drop`` x3 suite (tool removed:
restart auto-fails submitted jobs), ``validates_each_message`` (typed-SDK
revalidation deleted), one post-await-race test plus the latched-job
test (no latches, no two-phase race; the surviving post-await-race test
proves a racing delete correlation-rejects), the respond-binding refusal
(record_no_reply guards are stage-based now), and the journaled-
declaration suites (record_no_reply is terminal at tool time).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from config import Settings
from responder import Responder
from server import create_server
from store import Store, append_entry

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 555
GUILD_ID = 333
CHANNEL_ID = 444
OTHER_CHANNEL = 445
FAKE_SID = "2026-01-02T03-04-05_ab12"
FAKE_DAY = "2026-01-02"
SOURCE_MID = 700000000000000007

OLD_TOOLS = [
    "ack_outbox",
    "add_poll_task",
    "guild_info",
    "list_members",
    "list_tasks",
    "list_text_channels",
    "read_outbox",
    "remove_poll_task",
    "send_message",
    "start_task",
    "stop_task",
    "whoami",
]


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _jump(mid: int) -> str:
    return f"https://discord.com/channels/{GUILD_ID}/{CHANNEL_ID}/{mid}"


# --- semantic fakes ------------------------------------------------------------


class _FakeUser:
    def __init__(self, uid, display_name, bot=False):
        self.id = uid
        self.display_name = display_name
        self.bot = bot


class _FakeMessage:
    def __init__(
        self,
        mid,
        author,
        content="",
        channel_id=CHANNEL_ID,
        guild_id=GUILD_ID,
        reference=None,
        created_at=None,
    ):
        self.id = mid
        self.author = author
        self.content = content
        self.channel = SimpleNamespace(id=channel_id)
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        scope = guild_id if guild_id is not None else "@me"
        self.jump_url = f"https://discord.com/channels/{scope}/{channel_id}/{mid}"
        self.reference = reference
        self.created_at = created_at


class _SemanticHistory:
    """Parameter-honoring history iterator (verified native semantics)."""

    def __init__(self, store, *, limit, before, after, oldest_first):
        self._rows = sorted(store, key=lambda m: m.id)
        self._limit = limit
        self._before = before
        self._after = after
        self._oldest_first = oldest_first

    def _bound_id(self, bound):
        return bound.id if isinstance(bound, discord.Object) else bound

    async def _collect(self):
        rows = self._rows
        if self._oldest_first:
            low = self._bound_id(self._after) if self._after is not None else 0
            return [m for m in rows if m.id > low][: self._limit]
        high = self._bound_id(self._before) if self._before is not None else None
        low = self._bound_id(self._after) if self._after is not None else 0
        picked = [m for m in rows if m.id > low and (high is None or m.id < high)]
        return list(reversed(picked[-self._limit :])) if picked else []

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in await self._collect():
            yield message


def _deleted_exc():
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def _make_channel(cid, guild_id=GUILD_ID, history=()):
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    store: dict = {
        "deleted": set(),
        "exc": None,
        "fetch_calls": [],
        "history": list(history),
        "history_calls": 0,
        "sent": [],
    }

    async def fake_fetch(mid):
        store["fetch_calls"].append(mid)
        hook = store.get("fetch_hook")
        if hook is not None:
            await hook(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_exc()
        return SimpleNamespace(
            id=mid, channel=SimpleNamespace(id=cid), guild=SimpleNamespace(id=guild_id)
        )

    def fake_history(*, limit=100, before=None, after=None, oldest_first=False):
        store["history_calls"] += 1
        return _SemanticHistory(
            store["history"],
            limit=limit,
            before=before,
            after=after,
            oldest_first=oldest_first,
        )

    async def fake_send(content, **kwargs):
        msg = _FakeMessage(
            9000 + len(store["sent"]),
            _FakeUser(BOT_UID, "B", True),
            channel_id=cid,
            guild_id=guild_id,
            reference=kwargs.get("reference"),
        )
        store["sent"].append((content, kwargs))
        return msg

    channel.fetch_message = fake_fetch
    channel.history = fake_history
    channel.send = fake_send
    channel.store = store
    return channel


class _FakeGuild:
    def __init__(self, gid, channels=None):
        self.id = gid
        self._channels = dict(channels or {})

    def get_channel(self, cid):
        channel = self._channels.get(cid)
        if channel is None:
            channel = _make_channel(cid, self.id)
            self._channels[cid] = channel
        return channel

    def get_member(self, uid):
        return None


class _FakeBot:
    def __init__(self, guild, fetch_map=None):
        self.user = _FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self.listeners: dict = {}
        self._ready = False
        self._closed = False
        self._setup = False  # mirrors Client._ready existing after _async_setup_hook
        # Mirrors the gateway READY dispatch: set only by a successful start.
        self._ready_event = asyncio.Event()
        self._close_event = asyncio.Event()

    def event(self, func):
        self.listeners.setdefault(func.__name__, []).append(func)
        return func

    async def start(self, token):  # noqa: ARG002 - signature mirrors discord.py
        self._setup = True  # mirrors _async_setup_hook at login() entry
        # Successful gateway connect dispatches READY, then stays open.
        self._ready_event.set()
        await self._close_event.wait()

    async def wait_until_ready(self):
        if not self._setup:
            # Mirrors the real Client: raises before login() creates _ready.
            raise RuntimeError(
                "Client has not been properly initialised. "
                "Please use the login method before calling this method"
            )
        await self._ready_event.wait()
        self._ready = True

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
        self._closed = True
        self._close_event.set()

    def get_guild(self, gid):
        return self._guild if gid == self._guild.id else None

    async def fetch_channel(self, cid):
        if cid in self._fetch_map:
            return self._fetch_map[cid]
        raise LookupError("channel not found")

    def get_user(self, uid):
        return None

    async def fetch_user(self, uid):
        raise LookupError("user not found")


class _StubAdapter:
    async def decide(self, state):
        raise AssertionError("MCP tool tests never classify")


async def _stub_collector(entry):
    raise AssertionError("MCP tool tests never collect")


class _StubLedger:
    async def classify(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never classify")


class _StubNative:
    async def get_session_snapshot(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never snapshot")

    async def submit_turn(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never POST")

    async def get_bot_dm(self, *args, **kwargs):
        raise AssertionError("MCP tool tests never resolve the DM")


# --- harness --------------------------------------------------------------------


def _settings_kwargs(tmp_path):
    return dict(
        _env_file=None,
        discord_token="fake",
        owner_uid=OWNER_UID,
        guild_id=GUILD_ID,
        default_channel_id=CHANNEL_ID,
        audit_path=tmp_path / "audit.jsonl",
        db_path=tmp_path / "state.db",
        watch_channel_ids=[CHANNEL_ID],
        responder_check_seconds=0.05,
        thyca_request_timeout_seconds=5.0,
        recovery_poll_seconds=30.0,
    )


@pytest.fixture
def settings(tmp_path):
    settings = Settings(**_settings_kwargs(tmp_path))
    with Store.open(settings.db_path) as store:
        store.adopt_session(expected_current=None, new_sid=FAKE_SID, day=FAKE_DAY)
    return settings


@asynccontextmanager
async def _running(monkeypatch, settings, bot):
    """Real lifespan with a fake bot and stub-Jev REAL Responder; loops parked."""
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr(
        "server.core.Responder",
        lambda s, b: Responder(
            s,
            b,
            client=_StubNative(),
            jev_adapter=_StubAdapter(),
            context_collector=_stub_collector,
            participation_ledger=_StubLedger(),
        ),
    )
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx, bot


async def _call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def _audit_text(settings):
    if not settings.audit_path.exists():
        return ""
    return settings.audit_path.read_text(encoding="utf-8")


def _audit_records(settings, tool):
    out = []
    if not settings.audit_path.exists():
        return out
    for line in settings.audit_path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("tool") == tool:
            out.append(record)
    return out


def _job(settings, mid=SOURCE_MID):
    with Store.open(settings.db_path) as store:
        return store.get_job(CHANNEL_ID, mid)


def _seed_submitted(settings, mid=SOURCE_MID, job_ref="job-need-1"):
    """Outbox row + queued job + submit marker (the correlated mid-flight job)."""
    entry = append_entry(
        settings.db_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content=f"m{mid}",
        author_is_bot=False,
    )
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, mid, entry["seq"])
        store.mark_submitted(
            CHANNEL_ID, mid, job_ref=job_ref, native_sid=FAKE_SID, pre_turn_index=0
        )
    return job_ref


# --- tool surface -----------------------------------------------------------------


async def test_tool_count_14_old_12_unchanged(settings):
    mcp = create_server(settings)
    tools = await mcp.list_tools()
    names = sorted(t.name for t in tools)
    assert len(names) == 14
    for old in OLD_TOOLS:
        assert old in names
    assert "read_context" in names and "record_no_reply" in names
    assert "resolve_uncertain_drop" not in names
    descs = {t.name: t.description or "" for t in tools}
    # Old tool contracts intact (identity policy + correlation wording).
    assert str(OWNER_UID) in descs["send_message"]
    assert "author_uid" in descs["whoami"]
    assert "USER.md" in descs["read_outbox"]
    # record_no_reply is terminal at tool time now (never the old journaling).
    assert "no_reply_needed" in descs["record_no_reply"]
    assert "Terminal" in descs["record_no_reply"]
    assert "TOOL TIME" in descs["record_no_reply"]
    assert "1..20" in descs["read_context"]
    assert "never auto-forwarded" in descs["read_context"]


async def test_manual_send_unchanged(settings, monkeypatch):
    """Unrelated manual sends bypass correlation/Jev entirely (plain send)."""
    channel = _make_channel(CHANNEL_ID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        payload = await _call_json(mcp, "send_message", {"content": "plain manual"})
    assert payload["channel_id"] == CHANNEL_ID
    assert payload["message_id"] == 9000
    assert channel.store["sent"][0][0] == "plain manual"
    # Manual sends carry no reply reference but DO carry the mention guard.
    assert channel.store["sent"][0][1].get("reference") is None
    assert channel.store["sent"][0][1]["allowed_mentions"].to_dict() == {
        "replied_user": True,
        "parse": ["users"],
    }


# --- correlated send ----------------------------------------------------------------


async def test_correlated_send_records_send(settings, monkeypatch):
    channel = _make_channel(CHANNEL_ID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "send_message",
            {"content": "chunk one", "source_message_id": SOURCE_MID, "job_ref": ref},
        )
        assert payload["source_message_id"] == SOURCE_MID
        assert payload["reply_to_message_id"] == SOURCE_MID
        assert payload["message_id"] == 9000
        # Assert inside the lifespan: orderly shutdown fails in-flight sent.
        job = _job(settings)
        assert job["stage"] == "sent"
        assert job["send_count"] == 1
        assert job["sent_message_ids"] == [9000]
    sent_content, sent_kwargs = channel.store["sent"][0]
    assert sent_content == "chunk one"
    assert sent_kwargs["reference"].message_id == SOURCE_MID
    assert sent_kwargs["reference"].fail_if_not_exists is True


async def test_correlated_send_deleted_source_skips_zero_sends(settings, monkeypatch):
    channel = _make_channel(CHANNEL_ID)

    async def boom_send(content, **kwargs):
        raise _deleted_exc()

    channel.send = boom_send
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "send_message",
            {"content": "chunk", "source_message_id": SOURCE_MID, "job_ref": ref},
        )
    assert payload == {
        "status": "skipped",
        "reason": "source_deleted",
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
    }
    job = _job(settings)
    assert job["stage"] == "skipped"
    assert job["skip_reason"] == "source_deleted"


async def test_correlated_send_deleted_source_with_sends_fails(settings, monkeypatch):
    """A reply WAS delivered: never skip over send evidence -- fail instead."""
    channel = _make_channel(CHANNEL_ID)

    async def boom_send(content, **kwargs):
        raise _deleted_exc()

    channel.send = boom_send
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        with Store.open(settings.db_path) as store:
            store.record_send(CHANNEL_ID, SOURCE_MID, job_ref=ref, sent_message_id=9000)
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "send_message",
                {"content": "chunk two", "source_message_id": SOURCE_MID, "job_ref": ref},
            )
    job = _job(settings)
    assert job["stage"] == "failed"
    assert job["send_count"] == 1


async def test_correlated_send_ambiguous_fails_no_duplicate(settings, monkeypatch):
    channel = _make_channel(CHANNEL_ID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)

        async def boom_send(content, **kwargs):
            raise discord.HTTPException(
                SimpleNamespace(status=500, reason="x"), {"message": "boom"}
            )

        channel.send = boom_send
        # Ambiguous SDK failure: the job fails, never a blind retry/resend.
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "send_message",
                {"content": "chunk", "source_message_id": SOURCE_MID, "job_ref": ref},
            )
        # Retry correlation-rejects against the failed stage (no duplicate).
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "send_message",
                {"content": "chunk", "source_message_id": SOURCE_MID, "job_ref": ref},
            )
    job = _job(settings)
    assert job["stage"] == "failed"
    assert job["fail_kind"] == "transport_error"
    assert job["send_count"] == 0
    assert channel.store["sent"] == []


# --- record_no_reply ----------------------------------------------------------------


async def test_record_no_reply_terminal_at_tool_time(settings, monkeypatch):
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
        # Terminal INSIDE the lifespan: the worker must not re-POST/re-send.
        job = _job(settings)
        assert (job["stage"], job["outcome"]) == ("done", "no_reply")
    assert payload == {
        "status": "no_reply",
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
        "job_ref": ref,
        "reason": "no_reply_needed",
    }


async def test_record_no_reply_repeat_rejected_terminal(settings, monkeypatch):
    """Once terminal, repeat (or mismatched-reason) calls correlation-reject."""
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        first = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
        assert first["status"] == "no_reply"
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "missing_context"},
            )
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "no_reply")


@pytest.mark.parametrize(
    "args",
    [
        {"source_message_id": 999, "job_ref": "job-need-1", "reason": "no_reply_needed"},
        {"source_message_id": SOURCE_MID, "job_ref": "wrong-ref", "reason": "no_reply_needed"},
        {
            "source_message_id": SOURCE_MID,
            "job_ref": "job-need-1",
            "reason": "no_reply_needed",
            "channel_id": OTHER_CHANNEL,
        },
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "reason": "bogus"},
        {"source_message_id": 0, "job_ref": "job-need-1", "reason": "no_reply_needed"},
        {"source_message_id": SOURCE_MID, "job_ref": "", "reason": "no_reply_needed"},
    ],
)
async def test_record_no_reply_rejects_bad_correlation(settings, monkeypatch, args):
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        _seed_submitted(settings)
        with pytest.raises(ToolError):
            await _call_json(mcp, "record_no_reply", args)
        job = _job(settings)
        assert job["stage"] == "submitted"  # zero mutation on reject


async def test_record_no_reply_invalid_reason_canary_not_audited(settings, monkeypatch):
    canary = "CANARY-PRIVATE-REASON-xyz"
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": canary},
            )
    assert canary not in _audit_text(settings)
    failures = [r for r in _audit_records(settings, "record_no_reply") if not r["ok"]]
    assert failures and failures[0]["args"] == {
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
        "error_kind": "validation_error",
    }  # literal F2: no reason_len/sha fingerprints


async def test_record_no_reply_rejects_send_evidence(settings, monkeypatch):
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        with Store.open(settings.db_path) as store:
            store.record_send(CHANNEL_ID, SOURCE_MID, job_ref=ref, sent_message_id=9000)
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
        job = _job(settings)
        assert job["stage"] == "sent"  # the send stands; no declaration
        assert job["send_count"] == 1


async def test_record_no_reply_accepts_any_routed_job(settings, monkeypatch):
    """Guards are stage-based (mid-flight + zero sends), not binding-based."""
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
    assert payload["status"] == "no_reply"
    assert _job(settings)["outcome"] == "no_reply"


async def test_record_no_reply_source_deleted_skips(settings, monkeypatch):
    channel = _make_channel(CHANNEL_ID)
    channel.store["deleted"].add(SOURCE_MID)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "record_no_reply",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
        )
    assert payload["status"] == "skipped" and payload["reason"] == "source_deleted"
    job = _job(settings)
    assert job["stage"] == "skipped" and job["skip_reason"] == "source_deleted"


async def test_record_no_reply_source_unavailable_no_mutation(settings, monkeypatch):
    channel = _make_channel(CHANNEL_ID)
    channel.store["exc"] = discord.Forbidden(
        SimpleNamespace(status=403, reason="Forbidden"), {"code": 50013, "message": "x"}
    )
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
        # Assert inside the lifespan: the job is untouched (still submitted).
        job = _job(settings)
        assert job["stage"] == "submitted"


async def test_record_no_reply_post_await_race_rejected(settings, monkeypatch):
    """A terminal landing during the source await fails the post-await guard."""

    channel = _make_channel(CHANNEL_ID)

    async def race(mid):
        with Store.open(settings.db_path) as store:
            store.complete_job(CHANNEL_ID, SOURCE_MID, outcome="no_reply")

    channel.store["fetch_hook"] = race
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "record_no_reply",
                {"source_message_id": SOURCE_MID, "job_ref": ref, "reason": "no_reply_needed"},
            )
    job = _job(settings)
    assert (job["stage"], job["outcome"]) == ("done", "no_reply")


# --- read_context -------------------------------------------------------------------


def _history_store(base_mid, count, author_uid=OTHER_UID, name="Ramon"):
    import datetime

    store = []
    for i in range(count):
        mid = base_mid - count + i
        store.append(
            _FakeMessage(
                mid,
                _FakeUser(author_uid, name),
                content=f"history body {mid}",
                reference=None,
                created_at=datetime.datetime(2026, 10, 5, 12, 0, 0),
            )
        )
    return store


async def test_read_context_returns_source_adjacent_recents(settings, monkeypatch):
    """Semantic regression: newest-before-source, never oldest-ever rows."""
    store = _history_store(SOURCE_MID, 100)
    channel = _make_channel(CHANNEL_ID, history=store)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 5},
        )
    assert payload["count"] == 5
    got = [m["message_id"] for m in payload["messages"]]
    assert got == [SOURCE_MID - 5, SOURCE_MID - 4, SOURCE_MID - 3, SOURCE_MID - 2, SOURCE_MID - 1]
    first = payload["messages"][0]
    assert first["author_uid"] == OTHER_UID and first["author_name"] == "Ramon"
    assert first["content"] == f"history body {SOURCE_MID - 5}"
    assert first["reply_to_message_id"] is None
    assert "2026-10-05" in first["timestamp"]
    # Bodies are served to the agent but never touch the audit log.
    assert f"history body {SOURCE_MID - 5}" not in _audit_text(settings)
    records = [r for r in _audit_records(settings, "read_context") if r["ok"]]
    # Phase 4/F2 allowlist: success audits carry channel_id +
    # source_message_id only -- counts/ids stay in the tool payload.
    assert records and records[0]["args"] == {
        "channel_id": CHANNEL_ID,
        "source_message_id": SOURCE_MID,
    }


async def test_read_context_paginates_backwards_never_future(settings, monkeypatch):
    store = _history_store(SOURCE_MID, 40)
    channel = _make_channel(CHANNEL_ID, history=store)
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        page1 = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 10},
        )
        oldest = page1["messages"][0]["message_id"]
        page2 = await _call_json(
            mcp,
            "read_context",
            {
                "source_message_id": SOURCE_MID,
                "job_ref": ref,
                "before_message_id": oldest,
                "limit": 10,
            },
        )
        with pytest.raises(ToolError):
            await _call_json(
                mcp,
                "read_context",
                {
                    "source_message_id": SOURCE_MID,
                    "job_ref": ref,
                    "before_message_id": SOURCE_MID + 1,
                    "limit": 10,
                },
            )
    assert [m["message_id"] for m in page1["messages"]][-1] == SOURCE_MID - 1
    assert [m["message_id"] for m in page2["messages"]][-1] == oldest - 1
    assert all(m["message_id"] < SOURCE_MID for m in page2["messages"])


@pytest.mark.parametrize(
    "args",
    [
        {"source_message_id": 999, "job_ref": "job-need-1"},
        {"source_message_id": SOURCE_MID, "job_ref": "wrong"},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "limit": 0},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "limit": 21},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "channel_id": OTHER_CHANNEL},
        {"source_message_id": SOURCE_MID, "job_ref": "job-need-1", "before_message_id": 0},
    ],
)
async def test_read_context_rejects_bad_input(settings, monkeypatch, args):
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: _make_channel(CHANNEL_ID)}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        _seed_submitted(settings)
        with pytest.raises(ToolError):
            await _call_json(mcp, "read_context", args)


async def test_read_context_never_touches_other_channel(settings, monkeypatch):
    other = _make_channel(OTHER_CHANNEL, history=_history_store(SOURCE_MID, 10))
    channel = _make_channel(CHANNEL_ID, history=_history_store(SOURCE_MID, 3))
    bot = _FakeBot(_FakeGuild(GUILD_ID, {CHANNEL_ID: channel, OTHER_CHANNEL: other}))
    async with _running(monkeypatch, settings, bot) as (mcp, ctx, _):
        ref = _seed_submitted(settings)
        payload = await _call_json(
            mcp,
            "read_context",
            {"source_message_id": SOURCE_MID, "job_ref": ref, "limit": 10},
        )
    assert payload["count"] == 3
    assert other.store["history_calls"] == 0
