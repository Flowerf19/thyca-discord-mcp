"""V5 server<->responder integration: always-on lifecycle + agent-owned MCP send.

Uses the REAL Responder and the REAL SQLite store with a fake Thyca backend
and fake Discord objects — no network, no real .env/state (all paths are tmp
fixtures with a fake SID/token). The native FastMCP public API
(``list_tools``/``call_tool``) and the real installed lifespan drive every test.

Positive auto-flow tests in this file encode the approved end-to-end behavior
(listener -> native auto loop -> fake POST -> same-loop MCP send callback ->
conditional record -> worker snapshot-poll close -> controller ACK).
They REQUIRE the final Core/Worker/Client APIs; while a peer API is pending
they fail with the real dependency error (reported, never mocked away).

Seeds land INSIDE the running lifespan: the Responder constructor runs the
restart sweep once (submitted -> failed), so a pre-seeded submitted job
would be swept before any tool runs -- exactly as in production, where jobs
become submitted while the server runs. Queued/skipped seeds are
sweep-safe and may be seeded before.

Deleted (reasons): intent/receipt journaling + 1:1 proof suites, the
uncertain-hold/latch/no-new-send suites, receipt-scope validation
(no receipt journal exists), the trace-evidence block (own-trace
uncorrelated test), the fsync/post-replace durability-fault suites
(single sqlite commits have no two-phase ambiguity), the nonce-rotation
race (no pre-flight fetch to race), and the JSON tombstone/load-validator
suite (CHECK constraints + typed writers own that class now).
"""

import asyncio
import hashlib
import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from config import Settings
from responder import Responder
from server import create_server
from store import Store, append_entry, read_entries
from thyca import ThycaBusyError, ThycaClient

BOT_UID = 111
OWNER_UID = 222
OTHER_UID = 999
GUILD_ID = 333
CHANNEL_ID = 444
FAKE_SID = "2026-01-02T03-04-05_ab12"
FAKE_DAY = "2026-01-02"


# --- fakes (self-contained, native-faithful) ---------------------------------


class FakeUser:
    def __init__(self, uid, display_name, bot=False):
        self.id = uid
        self.display_name = display_name
        self.bot = bot


class FakeMessage:
    def __init__(
        self, mid, author, content="", channel_id=CHANNEL_ID, guild_id=GUILD_ID, reference=None
    ):
        self.id = mid
        self.author = author
        self.content = content
        self.channel = SimpleNamespace(id=channel_id)
        self.guild = SimpleNamespace(id=guild_id) if guild_id is not None else None
        scope = guild_id if guild_id is not None else "@me"
        self.jump_url = f"https://discord.com/channels/{scope}/{channel_id}/{mid}"
        self.reference = reference


class FakeGuild:
    def __init__(self, gid, channels=None, member_count=10, members=None):
        self.id = gid
        self._channels = dict(channels or {})
        self.member_count = member_count
        self._members = dict(members or {})

    def get_channel(self, cid):
        return self._channels.get(cid)

    def get_member(self, uid):
        return self._members.get(uid)

    async def fetch_member(self, uid):
        try:
            return self._members[uid]
        except KeyError:
            raise LookupError(f"member {uid} not found") from None


class FakeBot:
    """Minimal bot surface: start/ready/channels/event capture/user lookup.

    Mirrors ``discord.Client`` (no add_listener): realtime capture registers
    via ``event()`` keyed by function name, startup runs ``start(token)``
    raced against ``wait_until_ready()``.
    """

    def __init__(self, guild, *, user=None, fetch_map=None, start_exc=None, users=None):
        self.user = user or FakeUser(BOT_UID, "ThycaBot", bot=True)
        self._guild = guild
        self._fetch_map = dict(fetch_map or {})
        self._start_exc = start_exc
        self._users = dict(users or {})
        self.events: list[str] = []
        self.listeners: dict[str, list] = {}
        self._closed = False
        self._ready = False
        self._setup = False  # mirrors Client._ready existing after _async_setup_hook
        # Mirrors the gateway READY dispatch: set only by a successful start.
        self._ready_event = asyncio.Event()
        self._close_event = asyncio.Event()

    def event(self, func):
        self.events.append("event")
        self.listeners.setdefault(func.__name__, []).append(func)
        return func

    async def start(self, token):  # noqa: ARG002 - signature mirrors discord.py
        self.events.append("start")
        self._setup = True  # mirrors _async_setup_hook at login() entry
        if self._start_exc is not None:
            raise self._start_exc
        # Successful gateway connect dispatches READY, then stays open.
        self._ready_event.set()
        await self._close_event.wait()

    async def wait_until_ready(self):
        self.events.append("wait_ready")
        if not self._setup:
            # Mirrors the real Client: raises before login() creates _ready.
            raise RuntimeError(
                "Client has not been properly initialised. "
                "Please use the login method before calling this method"
            )
        self._ready = True
        await self._ready_event.wait()

    def is_ready(self):
        return self._ready

    def is_closed(self):
        return self._closed

    async def close(self):
        self.events.append("close")
        self._closed = True
        self._close_event.set()

    def get_guild(self, gid):
        if self._guild is not None and self._guild.id == gid:
            return self._guild
        return None

    async def fetch_channel(self, cid):
        self.events.append(f"fetch_channel:{cid}")
        try:
            return self._fetch_map[cid]
        except KeyError:
            raise LookupError(f"channel {cid} not found") from None

    def get_user(self, uid):
        return self._users.get(uid)

    async def fetch_user(self, uid):
        try:
            return self._users[uid]
        except KeyError:
            raise LookupError(f"user {uid} not found") from None


def make_history(messages=()):
    """Fake TextChannel.history: records kwargs, yields configured messages."""
    calls: list[dict] = []
    state = {"messages": list(messages)}

    def history(**kwargs):
        calls.append(kwargs)

        async def gen():
            for m in state["messages"]:
                yield m

        return gen()

    return history, calls, state


def _deleted_not_found():
    """Real strict discord.NotFound (404/10008 Unknown Message), no network."""
    return discord.NotFound(
        SimpleNamespace(status=404, reason="Not Found"),
        {"code": 10008, "message": "Unknown Message"},
    )


def make_text_channel(cid, guild_id=GUILD_ID, history=None):
    """Spec'd TextChannel mock with native-faithful send (reference + hooks)."""
    channel = MagicMock(spec=discord.TextChannel)
    channel.id = cid
    channel.guild = SimpleNamespace(id=guild_id)
    sent: list[FakeMessage] = []
    hooks: dict = {"send_exc": None, "send_hook": None}
    store: dict = {"deleted": set(), "exc": None, "calls": []}

    async def fake_fetch(mid):
        store["calls"].append(mid)
        if store["exc"] is not None:
            raise store["exc"]
        if mid in store["deleted"]:
            raise _deleted_not_found()
        return FakeMessage(
            mid,
            FakeUser(OWNER_UID, "Owner"),
            "",
            channel_id=cid,
            guild_id=guild_id,
        )

    async def fake_send(content, **kwargs):
        if hooks["send_exc"] is not None:
            raise hooks["send_exc"]
        if hooks["send_hook"] is not None:
            return await hooks["send_hook"](content, **kwargs)
        msg = FakeMessage(
            9000 + len(sent),
            FakeUser(BOT_UID, "ThycaBot", bot=True),
            content,
            channel_id=cid,
            guild_id=guild_id,
            reference=kwargs.get("reference"),
        )
        sent.append(msg)
        return msg

    # isinstance(channel, discord.TextChannel) passes via the Mock spec (v4 pattern).
    channel.send = fake_send
    channel.fetch_message = fake_fetch
    channel.history = history or make_history()[0]
    channel.sent = sent
    channel.hooks = hooks
    channel.source_store = store
    return channel


class _DmMixin:
    async def get_bot_dm(self, bot_id):
        assert bot_id == "march7th"
        return {
            "native_sid": FAKE_SID,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }


class FakeThycaBackend(_DmMixin):
    """Scripted native Thyca backend with same-loop MCP send callback.

    ``submit_turn`` reads the FRESH job (job_ref) from the store -- never
    fabricated -- then calls back into the SAME server/loop via
    ``mcp.call_tool('send_message', ...)`` before the POST returns. The POST
    result is a clean idle turn (index follows the gate pre_max exactly) so
    the worker closes via the snapshot-poll path with no trace proof.
    """

    def __init__(self, db_path, *, pre_max=3, reply_text="fake agent reply"):
        self.db_path = db_path
        self.pre_max = pre_max
        self.reply_text = reply_text
        self.mcp = None
        self.posts: list[str] = []
        self.tool_calls: list[dict] = []
        self.call_seq = 0
        self.posted = asyncio.Event()
        # Foreign-manual send: another SID sharing the pool sends a plain
        # manual message. It hits the SDK but is never correlated to the job.
        self.foreign_manual_content: str | None = None

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        assert native_sid == FAKE_SID
        total = self.pre_max + 1 + len(self.posts)
        return {
            "native_sid": FAKE_SID,
            "native_id_match": True,
            "title_match": True,
            "title_len": 5,
            "title_sha256": "0" * 64,
            "running": False,
            "turn_count": total,
            "turn_indices": list(range(total)),
            "pre_max_turn_index": (total - 1) if total else None,
            "message_count": total,
            "proof": "fake",
        }

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        assert native_sid == FAKE_SID
        self.posts.append(text)
        with Store.open(self.db_path) as store:
            subs = store.list_jobs(stages={"submitted"})
        assert len(subs) == 1
        job = subs[0]
        assert job["job_ref"] in text  # prompt carries the nonce delimiters
        res = await self.mcp.call_tool(
            "send_message",
            {
                "content": self.reply_text,
                "channel_id": job["channel_id"],
                "source_message_id": job["message_id"],
                "job_ref": job["job_ref"],
            },
        )
        blocks = res[0] if isinstance(res, tuple) else res
        payload = json.loads(blocks[0].text)
        self.call_seq += 1
        self.tool_calls.append(
            {
                "call_id": f"call_{self.call_seq}",
                "content": self.reply_text,
                "job_ref": job["job_ref"],
                "source_mid": job["message_id"],
                "channel_id": job["channel_id"],
                "result": payload,
            }
        )
        if self.foreign_manual_content is not None:
            # Foreign SID manual: SDK side effect only, never correlated.
            await self.mcp.call_tool(
                "send_message",
                {"content": self.foreign_manual_content, "channel_id": job["channel_id"]},
            )
        self.posted.set()
        return {
            "cancelled": False,
            "completed": True,
            "native_id_match": True,
            "turn_index": self.pre_max + len(self.posts),
            "running": False,
            "stored_text_match": True,
        }

    async def get_bot_dm(self, bot_id):
        return await _DmMixin.get_bot_dm(self, bot_id)


class BusyThycaBackend(_DmMixin):
    """Backend whose snapshot reports busy: tick must hold without any POST."""

    def __init__(self):
        self.posts: list[str] = []

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        raise ThycaBusyError()

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        raise AssertionError("no POST must be attempted while the native side is busy")


class PoisonThycaBackend(_DmMixin):
    """Backend whose snapshot raises a NON-Thyca error: the tick must propagate it.

    A plain RuntimeError (never ThycaClientError) bypasses the worker's
    snapshot/busy handling, so the server auto-tick wrapper is the layer
    that audits coarse + stops the loop. Used with the REAL Responder."""

    def __init__(self, exc):
        self.exc = exc
        self.snapshots = 0

    async def get_session_snapshot(self, native_sid, *, expected_title=None):
        self.snapshots += 1
        raise self.exc

    async def submit_turn(self, native_sid, text, *, model=None, effort=None):
        raise AssertionError("unreachable: snapshot poison fires first")


# --- fixtures / helpers ------------------------------------------------------


def _settings_kwargs(tmp_path):
    """Fixture kwargs: tmp/fake-only, immune to ambient .env and env vars."""
    return dict(
        # No real dotenv/config reads: constructor pins everything the suite
        # depends on (init args beat env, and _env_file=None disables .env).
        _env_file=None,
        discord_token="fake-token-unit-only",
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


def _make_settings(tmp_path, **overrides):
    kwargs = _settings_kwargs(tmp_path)
    kwargs.update(overrides)
    return Settings(**kwargs)


@pytest.fixture
def settings(tmp_path):
    return _make_settings(tmp_path)


def _init_store(settings):
    with Store.open(settings.db_path) as store:
        store.adopt_session(expected_current=None, new_sid=FAKE_SID, day=FAKE_DAY)


def _get_job(settings, mid, channel_id=CHANNEL_ID):
    with Store.open(settings.db_path) as store:
        return store.get_job(channel_id, mid)


# -- legacy Jev stub (module-only autouse; preserves pre-routing regressions) --
# Every Responder built in THIS module gets a fake respond-only Jev stack
# unless the test passes explicit deps. The fake returns the store-shape
# decided record (respond, no provider call) so the full submit/POST/
# snapshot/ACK path runs unchanged; the fake provider is never called (it
# raises). New routing tests do NOT use this fixture (explicit deps only).


class _LegacyJevAdapter:
    def __init__(self):
        self.calls = 0

    async def decide(self, state):
        self.calls += 1
        raise AssertionError("legacy fake ledger must never call the provider")


def _legacy_collector(owner_uid, bot_uid):
    async def collect(entry):
        mid = entry["message_id"]
        author = entry["author_uid"]
        parts = [
            {"uid": str(owner_uid), "display_name": "Owner"},
            {"uid": str(bot_uid), "display_name": "Bot"},
        ]
        if str(author) not in (str(owner_uid), str(bot_uid)):
            parts.append(
                {
                    "uid": str(author),
                    "display_name": entry.get("author_name") or f"uid-{author}",
                }
            )
        return {
            "assistant_uid": str(bot_uid),
            "owner_uid": str(owner_uid),
            "participants": parts,
            "messages": [{"id": str(mid), "sender_uid": str(author), "text": entry["content"]}],
            "new_message_id": str(mid),
        }

    return collect


class _LegacyLedger:
    def __init__(self):
        self.calls = 0

    async def classify(self, adapter, state, *, channel_id, message_id, author_uid):
        self.calls += 1
        from context import context_fingerprint

        attempt = hashlib.sha256(f"{channel_id}:{message_id}".encode()).hexdigest()[:32]
        return {
            "status": "decided",
            "context_sha256": context_fingerprint(state),
            "attempt_id": attempt,
            "participation": "respond",
        }

    def get(self, channel_id, message_id):
        return None


@pytest.fixture(autouse=True)
def _inject_legacy_jev(monkeypatch):
    orig_init = Responder.__init__

    def patched(
        self,
        settings,
        bot,
        *,
        client=None,
        source_checker=None,
        jev_adapter=None,
        context_collector=None,
        participation_ledger=None,
    ):
        if jev_adapter is None:
            jev_adapter = _LegacyJevAdapter()
        if context_collector is None:
            context_collector = _legacy_collector(settings.owner_uid, bot.user.id)
        if participation_ledger is None:
            participation_ledger = _LegacyLedger()
        return orig_init(
            self,
            settings,
            bot,
            client=client,
            source_checker=source_checker,
            jev_adapter=jev_adapter,
            context_collector=context_collector,
            participation_ledger=participation_ledger,
        )

    monkeypatch.setattr(Responder, "__init__", patched)


def seed_active_job(settings, *, mid=701, author_uid=OWNER_UID, actor_kind="owner"):
    """Outbox entry + queued job advanced to submitted (POST started).

    Call INSIDE the running lifespan: the constructor sweep would fail a
    pre-seeded submitted job. Returns the bound job_ref.
    """
    entry = append_entry(
        settings.db_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=mid,
        author_uid=author_uid,
        author_name="Owner" if author_uid == OWNER_UID else "Rando",
        actor_kind=actor_kind,
        content=f"m{mid}",
        author_is_bot=False,
    )
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, mid, entry["seq"])
        job_ref = f"job-{CHANNEL_ID}-{mid}-{entry['seq']}"
        store.mark_submitted(
            CHANNEL_ID, mid, job_ref=job_ref, native_sid=FAKE_SID, pre_turn_index=3
        )
    return job_ref


@asynccontextmanager
async def parked_server(monkeypatch, settings, bot):
    """Real lifespan with the REAL Responder; auto loops parked before first tick."""
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)

    # DM-only routing resolves the target DM live on every tick. Stub just
    # that resolve to the seeded SID so parked ticks stay offline; every
    # other native call uses the real client exactly as production does.
    async def _fake_dm(self, bot_id):
        return {
            "native_sid": FAKE_SID,
            "running": False,
            "message_count": 0,
            "proof": "bot_dm",
        }

    monkeypatch.setattr(ThycaClient, "get_bot_dm", _fake_dm)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        yield mcp, ctx, bot


async def call_json(mcp, name, args=None):
    res = await mcp.call_tool(name, args or {})
    blocks = res[0] if isinstance(res, tuple) else res
    return json.loads(blocks[0].text)


def read_audit(settings):
    if not settings.audit_path.exists():
        return []
    return [
        json.loads(line) for line in settings.audit_path.read_text().splitlines() if line.strip()
    ]


def find_audit(records, tool, ok):
    return [r for r in records if r["tool"] == tool and r["ok"] is ok]


# --- A: always-on lifecycle ----------------------------------------------------


async def test_auto_loops_start_by_default_after_ready(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp):
        assert bot.is_ready()
    assert bot.is_closed()


async def test_lifespan_creates_store_on_open(settings, monkeypatch):
    # No store file yet: the lifespan opens (creating 0600), seeds nothing.
    assert not settings.db_path.exists()
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    with pytest.raises(ValueError, match="native session not seeded"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - responder init must raise first
    assert settings.db_path.exists()
    assert bot.events == ["close"]
    assert bot.listeners.get("on_message") is None
    assert bot.is_closed()


async def test_store_bad_sid_fails_before_capture(settings, monkeypatch):
    _init_store(settings)
    with Store.open(settings.db_path) as store:
        # autocommit connection: the UPDATE lands immediately, no COMMIT.
        store._conn.execute("UPDATE state SET native_sid='not-a-sid' WHERE id=1;")
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    with pytest.raises(ValueError, match="bad native_sid"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - responder init must raise first
    assert bot.events == ["close"]
    assert bot.listeners.get("on_message") is None
    assert bot.is_closed()


async def test_start_failure_closes_real_responder_no_orphans(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(
        FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
        start_exc=discord.LoginFailure("bad token"),
    )
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    # Seed BEFORE the lifespan: the constructor sweep fails the submitted
    # job (restart) before gateway start raises; shutdown then has no
    # mid-flight job and must leave the swept row alone, never drop it.
    seed_active_job(settings)
    assert _get_job(settings, 701)["stage"] == "submitted"
    with pytest.raises(RuntimeError, match="gateway failed before ready"):
        async with mcp.settings.lifespan(mcp):
            pass  # pragma: no cover - start raises before the body runs
    assert bot.events == ["event", "start", "wait_ready", "close"]
    assert len(bot.listeners.get("on_message") or []) == 1
    assert bot.is_closed()
    job = _get_job(settings, 701)
    assert job["stage"] == "failed"
    assert job["fail_kind"] == "restart"
    leftovers = [
        t
        for t in asyncio.all_tasks()
        if t.get_name() in ("discord-gateway", "discord-ready") and not t.done()
    ]
    assert leftovers == []


# --- lifespan cleanup without sys.exception (3.10-safe exc_info) ----------------


class _FailingCloseRealResponder(Responder):
    """REAL Responder whose close() fails: cleanup-failure shutdown coverage."""

    async def close(self):
        raise RuntimeError("responder close boom")


async def test_lifespan_cleanup_without_sys_exception_keeps_primary(settings, monkeypatch):
    """3.10-safe cleanup: sys.exc_info()[1] keeps the primary with no sys.exception.

    sys.exception is 3.11+ only; the lifespan cleanup must use the 3.10-safe
    sys.exc_info()[1]. Deleting sys.exception on this 3.14 runtime proves it
    behaviorally: the OLD implementation would raise AttributeError here and
    mask the primary. (No 3.10 runtime is executed in this suite; the
    deletion fault-injects the 3.10 attribute surface onto 3.14. Neither the
    stdlib asyncio/logging nor the installed discord/mcp packages reference
    sys.exception, so the deletion only faults the server code under test.)
    """
    assert hasattr(sys, "exception")  # fault-injection precondition on 3.14
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", _FailingCloseRealResponder)
    monkeypatch.delattr(sys, "exception")
    # monkeypatch auto-restores the deleted attribute at teardown; no manual
    # finally needed (a manual restore would fight the monkeypatch undo).
    with pytest.raises(RuntimeError, match="gateway failed before ready"):
        failing = FakeBot(
            FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}),
            start_exc=discord.LoginFailure("bad token"),
        )
        monkeypatch.setattr("server.core.build_bot", lambda s: failing)
        mcp2 = create_server(settings)
        async with mcp2.settings.lifespan(mcp2):
            pass  # pragma: no cover


async def test_lifespan_cancel_without_sys_exception_stays_cancelled(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    task = asyncio.create_task(_cancel_after_ready(mcp))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bot.is_closed()


async def _cancel_after_ready(mcp):
    async with mcp.settings.lifespan(mcp):
        raise asyncio.CancelledError()


def test_server_source_has_no_sys_exception_attribute():
    import pathlib

    src = (pathlib.Path(__file__).resolve().parent.parent / "src" / "server" / "core.py").read_text(
        encoding="utf-8"
    )
    # 3.10-safe: no sys.exception attribute USE in code (the 3.10 note
    # comment may name it) and no sys_exception alias; cleanup reads the
    # primary via sys.exc_info()[1].
    code = "\n".join(ln for ln in src.splitlines() if not ln.strip().startswith("#"))
    assert "sys.exception" not in code
    assert "sys_exception" not in code
    assert "sys.exc_info()[1]" in code


async def test_shutdown_stops_protected_loops_and_clears_state(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["tasks"].status("auto-reply").running is True
    assert bot.is_closed()


async def test_lifespan_wires_auto_reply_fail_closed(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        loop = ctx["tasks"]._loops["auto-reply"]
        assert loop._error is not None  # error handler registered
        # clear_exception_types: every failure reaches the handler (no retry).
        assert getattr(loop, "_Loop__exception_types", ()) == ()


async def test_auto_tick_failure_audits_coarse_and_stops_loop(settings, monkeypatch):
    from audit import append_audit
    from server_ticks import make_auto_reply_tick

    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        responder = ctx["responder"]

        async def _boom():
            raise RuntimeError("tick boom")

        responder.tick = _boom
        tick, on_error = make_auto_reply_tick(settings, responder, append_audit)
        with pytest.raises(RuntimeError, match="tick boom"):
            await tick()  # propagates: the loop ends failed, never retried
        await on_error(RuntimeError("tick boom"))
        recs = [r for r in read_audit(settings) if r["tool"] == "auto_reply_tick"]
        assert len(recs) == 1 and recs[0]["ok"] is False
        # Coarse kinds never carry class names: unlisted errors map to unknown.
        assert recs[0]["args"] == {"error_kind": "unknown"}


async def test_failing_auto_loop_stops_itself_without_reset(settings, monkeypatch):
    _init_store(settings)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        assert ctx["tasks"].status("auto-reply").running is True

        # Fail the loop by poisoning the responder tick; the loop must end
        # failed (fail closed) rather than retry or reset.
        async def _boom():
            raise RuntimeError("tick boom")

        ctx["responder"].tick = _boom
        for _ in range(200):
            if not ctx["tasks"].status("auto-reply").running:
                break
            await asyncio.sleep(0.05)
        assert ctx["tasks"].status("auto-reply").running is False


# --- B: correlated send ---------------------------------------------------------


async def test_correlated_send_records_send(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
        assert res["channel_id"] == CHANNEL_ID
        assert res["source_message_id"] == 701
        assert res["reply_to_message_id"] == 701
        assert res["jump_url"].startswith("https://discord.com/channels/")
        (sent,) = channel.sent
        assert isinstance(sent.reference, discord.MessageReference)
        assert sent.reference.message_id == 701
        assert sent.reference.channel_id == CHANNEL_ID
        assert sent.reference.guild_id == GUILD_ID
        assert sent.reference.fail_if_not_exists is True
        job = _get_job(settings, 701)
        assert job["stage"] == "sent"
        assert job["send_count"] == 1
        assert job["sent_message_ids"] == [res["message_id"]]
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1
    assert ok_recs[0]["args"]["source_message_id"] == 701
    assert "content" not in ok_recs[0]["args"]


async def test_correlated_send_partial_pair_rejected(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool("send_message", {"content": "hi", "source_message_id": 701})
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool("send_message", {"content": "hi", "job_ref": job_ref})
        assert channel.sent == []
        job = _get_job(settings, 701)
        assert job["send_count"] == 0


async def test_correlated_send_unknown_job_rejected(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 4242,
                    "job_ref": "job-444-4242-9",
                },
            )
        assert channel.sent == []


async def test_correlated_send_job_ref_mismatch_rejected(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        seed_active_job(settings)
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "job-444-701-999",
                },
            )
        assert channel.sent == []
        job = _get_job(settings, 701)
        assert job["send_count"] == 0


async def test_correlated_send_wrong_channel_rejected(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    other = make_text_channel(555)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel, 555: other}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": 555,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == [] and other.sent == []


@pytest.mark.parametrize("stage", ["queued", "done", "skipped", "failed"])
async def test_correlated_send_stage_ineligible_rejected(settings, monkeypatch, stage):
    _init_store(settings)
    entry = append_entry(
        settings.db_path,
        type="message",
        channel_id=CHANNEL_ID,
        message_id=702,
        author_uid=OWNER_UID,
        author_name="Owner",
        actor_kind="owner",
        content="m702",
        author_is_bot=False,
    )
    with Store.open(settings.db_path) as store:
        store.enqueue_job(CHANNEL_ID, 702, entry["seq"])
        if stage == "skipped":
            store.skip_job(CHANNEL_ID, 702, reason="out_of_scope")
        elif stage == "done":
            store.mark_submitted(
                CHANNEL_ID, 702, job_ref="job-444-702-9", native_sid=FAKE_SID, pre_turn_index=0
            )
            store.record_send(CHANNEL_ID, 702, job_ref="job-444-702-9", sent_message_id=9001)
            store.complete_job(CHANNEL_ID, 702, outcome="sent")
        elif stage == "failed":
            store.mark_submitted(
                CHANNEL_ID, 702, job_ref="job-444-702-9", native_sid=FAKE_SID, pre_turn_index=0
            )
            store.mark_failed(CHANNEL_ID, 702, kind="timeout")
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 702,
                    "job_ref": "job-444-702-1",
                },
            )
        assert channel.sent == []


@pytest.mark.parametrize("bad", ["", "x" * 2001])
async def test_correlated_send_text_bounds_rejected(settings, monkeypatch, bad):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        with pytest.raises(ToolError, match="1..2000"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": bad,
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == []
        job = _get_job(settings, 701)
        assert job["send_count"] == 0


@pytest.mark.parametrize("bad_mid", [0, -5])
async def test_correlated_send_invalid_source_mid_rejected(settings, monkeypatch, bad_mid):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        with pytest.raises(ToolError, match="invalid source_message_id"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": bad_mid,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == []


async def test_correlated_send_empty_job_ref_rejected(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        seed_active_job(settings)
        with pytest.raises(ToolError, match="invalid job_ref"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "hi",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "",
                },
            )
        assert channel.sent == []


async def test_correlated_send_sdk_error_fails_no_retry(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    channel.hooks["send_exc"] = discord.DiscordException("native boom")
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        with pytest.raises(ToolError, match="reply uncertain"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        job = _get_job(settings, 701)
        assert job["stage"] == "failed"
        assert job["fail_kind"] == "transport_error"
        assert job["send_count"] == 0
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1
    assert recs[0]["args"]["error_kind"] == "discord_error"
    assert "native boom" not in json.dumps(recs[0])
    # A retry correlation-rejects against the failed stage (no duplicate).
    async with parked_server(monkeypatch, settings, bot) as (mcp2, ctx2, _):
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp2.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        assert channel.sent == []


async def test_correlated_send_cancel_propagates_without_mutation(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    channel.hooks["send_exc"] = asyncio.CancelledError()
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        # Precise: FastMCP must propagate CancelledError unwrapped (a broad
        # BaseException assertion would also accept a wrapped ToolError).
        with pytest.raises(asyncio.CancelledError):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": job_ref,
                },
            )
        job = _get_job(settings, 701)
        assert job["stage"] == "submitted"  # untouched: no fail mark, no send
        assert job["send_count"] == 0
    # Cancelled sends audit nothing (the cancellation propagates untouched).
    assert find_audit(read_audit(settings), "send_message", False) == []


async def test_correlated_send_carries_mention_guard_and_reference(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    seen: dict = {}
    orig_send = channel.send

    async def capture(content, **kwargs):
        seen.update(kwargs)
        return await orig_send(content, **kwargs)

    channel.send = capture
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
    assert res["reply_to_message_id"] == 701
    assert seen["allowed_mentions"].to_dict() == {
        "replied_user": True,
        "parse": ["users"],
    }
    ref = seen["reference"]
    assert (ref.message_id, ref.channel_id, ref.guild_id) == (701, CHANNEL_ID, GUILD_ID)
    assert ref.fail_if_not_exists is True


async def test_manual_send_does_not_attach_active_job(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        seed_active_job(settings)
        res = await call_json(mcp, "send_message", {"content": "operator note"})
        assert set(res) == {"message_id", "channel_id", "jump_url"}
        (sent,) = channel.sent
        assert sent.reference is None  # plain legacy send, no correlation
        job = _get_job(settings, 701)
        assert job["stage"] == "submitted"  # untouched by the manual send
        assert job["send_count"] == 0


async def test_concurrent_manual_sends_never_deadlock(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    gate = asyncio.Event()

    async def gated_send(content, **kwargs):
        await gate.wait()
        return FakeMessage(
            9200 + len(channel.sent),
            FakeUser(BOT_UID, "ThycaBot", bot=True),
            content,
            channel_id=CHANNEL_ID,
            guild_id=GUILD_ID,
            reference=kwargs.get("reference"),
        )

    channel.hooks["send_hook"] = gated_send
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        t1 = asyncio.create_task(mcp.call_tool("send_message", {"content": "one"}))
        t2 = asyncio.create_task(mcp.call_tool("send_message", {"content": "two"}))
        await asyncio.sleep(0.1)
        assert not t1.done() and not t2.done()  # both parked at the gate, none stuck
        gate.set()
        r1 = await asyncio.wait_for(t1, timeout=5)
        r2 = await asyncio.wait_for(t2, timeout=5)
        assert r1 and r2


async def test_send_audits_never_carry_content_or_canary(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        seed_active_job(settings)
        await call_json(mcp, "send_message", {"content": "note CANARY_SEND_X9"})
        with pytest.raises(ToolError, match="must be paired"):
            await mcp.call_tool(
                "send_message",
                {"content": "oops CANARY_SEND_X9", "source_message_id": 701},
            )
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "oops CANARY_SEND_X9",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": "job-444-701-xyz",
                },
            )
    audit_text = settings.audit_path.read_text(encoding="utf-8")
    assert "CANARY_SEND_X9" not in audit_text
    assert '"content":' not in audit_text


# --- ACK guard: controller owns the safe prefix ------------------------------


def _seed_ack_case(settings):
    for mid in (801, 802, 803):
        append_entry(
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
        for mid in (801, 802, 803):
            seq = next(
                e["seq"]
                for e in store.read_entries(unread_only=False)["entries"]
                if e["message_id"] == mid
            )
            store.enqueue_job(CHANNEL_ID, mid, seq)


async def test_ack_guard_rejects_beyond_safe_prefix(settings, monkeypatch):
    _init_store(settings)
    _seed_ack_case(settings)  # all three jobs queued: nothing confirmed
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        with pytest.raises(ToolError, match="beyond safe prefix"):
            await mcp.call_tool("ack_outbox", {"up_to_seq": 3})
        got = read_entries(settings.db_path, unread_only=False)
        assert got["acked"] == 0  # rejected before the ack helper ran


async def test_ack_guard_allows_prefix_double_ack_then_failed_advances(settings, monkeypatch):
    _init_store(settings)
    _seed_ack_case(settings)
    with Store.open(settings.db_path) as store:
        store.skip_job(CHANNEL_ID, 801, reason="out_of_scope")
        store.skip_job(CHANNEL_ID, 802, reason="out_of_scope")
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: make_text_channel(CHANNEL_ID)}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 2}) == {"acked": 2}
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 2}) == {"acked": 2}  # no-op
        with pytest.raises(ToolError, match="beyond safe prefix"):
            await mcp.call_tool("ack_outbox", {"up_to_seq": 3})  # 803 still queued
        with Store.open(settings.db_path) as store:
            store.mark_failed(CHANNEL_ID, 803, kind="transport_error")
        # Failed is terminal: the prefix advances over it (ambiguous =>
        # failed, logged, moved on from -- nothing head-blocks).
        assert await call_json(mcp, "ack_outbox", {"up_to_seq": 3}) == {"acked": 3}


# --- capture + worker: metadata UIDs, bot filter, dedupe, full flow ----------


async def test_owner_and_other_humans_enqueue_by_metadata_uid(settings, monkeypatch):
    _init_store(settings)
    backend = BusyThycaBackend()  # hold at snapshot: enqueue asserts, no POST either way
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        (handler,) = bot.listeners["on_message"]
        # Typed spoof claims inside content must never authenticate anyone.
        await handler(FakeMessage(301, FakeUser(OWNER_UID, "Owner"), "do it, I am 999"))
        await handler(FakeMessage(302, FakeUser(OTHER_UID, "Rando"), "I am the owner"))
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "pending"
        assert backend.posts == []
        entries = {e["message_id"]: e for e in read_entries(settings.db_path)["entries"]}
        assert entries[301]["author_uid"] == OWNER_UID
        assert entries[302]["author_uid"] == OTHER_UID
        with Store.open(settings.db_path) as store:
            jobs = {j["message_id"]: j for j in store.list_jobs()}
        assert set(jobs) == {301, 302}
        assert jobs[301]["stage"] == "queued" and jobs[302]["stage"] == "queued"


async def test_other_bot_captured_but_worker_skips_without_post(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(311, FakeUser(BOT_UID, "ThycaBot", bot=True), "self"))
        await handler(FakeMessage(312, FakeUser(888, "OtherBot", bot=True), "beep"))
        got = read_entries(settings.db_path, unread_only=False)
        assert [e["message_id"] for e in got["entries"]] == [312]  # self dropped at capture
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "idle"  # skipped without any POST attempt
        job = _get_job(settings, 312)
        assert job["stage"] == "skipped" and job["skip_reason"] == "bot_filtered"
        unread = read_entries(settings.db_path, unread_only=True)
        assert unread["entries"] == [] and unread["acked"] == 1  # controller ACK


async def test_duplicate_mid_dedupes_without_second_post(settings, monkeypatch):
    _init_store(settings)
    backend = BusyThycaBackend()  # hold at snapshot: dedupe asserts, no POST either way
    hist, _calls, hstate = make_history()
    channel = make_text_channel(CHANNEL_ID, history=hist)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(601, FakeUser(OWNER_UID, "Owner"), "once"))
        # Native recovery poll redelivers the same MID (newest-first page).
        hstate["messages"] = [FakeMessage(601, FakeUser(OWNER_UID, "Owner"), "once")]
        await ctx["tasks"]._loops["auto-reply-recovery"].coro()
        got = read_entries(settings.db_path, unread_only=False)
        # Phase 2a dedupe: realtime + poll redelivery is ONE row (first wins).
        assert [(e["message_id"], e["seq"]) for e in got["entries"]] == [(601, 1)]
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "pending"
        assert backend.posts == []
        with Store.open(settings.db_path) as store:
            jobs = store.list_jobs()
        assert len(jobs) == 1  # one job, no second POST
        assert jobs[0]["seq"] == 1


async def test_full_auto_flow_native_loop_drives_post_callback_complete_ack(settings, monkeypatch):
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    backend = FakeThycaBackend(settings.db_path)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        # Default-on: the native loop (not a manual POST) must drive the turn.
        assert ctx["tasks"].status("auto-reply").running is True
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(501, FakeUser(OWNER_UID, "Owner"), "hello thyca"))
        await asyncio.wait_for(backend.posted.wait(), timeout=5)
        await asyncio.sleep(0.5)  # let the completing tick finish past the callback
        assert len(backend.posts) == 1
        # The worker-generated job_ref is a caller-opaque random32-hex nonce
        # per the accepted contract, never a pinned format. The prompt must
        # still carry the exact live nonce from the store.
        live_ref = _get_job(settings, 501)["job_ref"]
        assert live_ref and live_ref in backend.posts[0]
        (call,) = backend.tool_calls
        job = _get_job(settings, 501)
        assert (job["stage"], job["outcome"]) == ("done", "sent")
        assert job["send_count"] == 1
        assert job["sent_message_ids"] == [call["result"]["message_id"]]
        got = read_entries(settings.db_path, unread_only=False)
        assert got["acked"] == 1  # controller ACK, never the model
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1 and ok_recs[0]["args"]["source_message_id"] == 501


async def test_foreign_manual_send_not_attributed_to_auto_job(settings, monkeypatch):
    _init_store(settings)
    backend = FakeThycaBackend(settings.db_path)
    backend.foreign_manual_content = "foreign sid manual"
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    monkeypatch.setattr("server.core.build_bot", lambda s: bot)
    monkeypatch.setattr("server.core.Responder", lambda s, b: Responder(s, b, client=backend))
    mcp = create_server(settings)
    backend.mcp = mcp
    async with mcp.settings.lifespan(mcp) as ctx:
        for name in ("auto-reply", "auto-reply-recovery"):
            await ctx["tasks"].stop(name, allow_protected=True)
        (handler,) = bot.listeners["on_message"]
        await handler(FakeMessage(503, FakeUser(OTHER_UID, "Rando"), "hi"))
        outcome = await ctx["responder"].tick()
        assert outcome["status"] == "sent"
        job = _get_job(settings, 503)
        assert (job["stage"], job["outcome"]) == ("done", "sent")
        assert job["send_count"] == 1
        assert len(channel.sent) == 2  # correlated + foreign manual both hit the SDK
        got = read_entries(settings.db_path, unread_only=False)
        assert got["acked"] == 1  # foreign send never blocked completion or ACK


# --- F5: bool/str source MID rejected at the MCP boundary --------------------


async def test_bool_and_str_source_mid_rejected_int_control_accepted(settings, monkeypatch):
    # MID 1 is seeded on purpose: under the old lax-int coercion True/"1"
    # would BOTH have become int 1 and hit this very job. StrictInt must
    # reject them before the tool body (zero sends, zero audits); int 1 works.
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings, mid=1)
        for bad in (True, "1"):
            with pytest.raises(ToolError):
                await mcp.call_tool(
                    "send_message",
                    {
                        "content": "auto reply",
                        "channel_id": CHANNEL_ID,
                        "source_message_id": bad,
                        "job_ref": job_ref,
                    },
                )
        assert channel.sent == []  # rejected before any SDK send
        job = _get_job(settings, 1)
        assert job["send_count"] == 0
        # Rejected before the tool body audits (heartbeat may audit; send must not).
        assert find_audit(read_audit(settings), "send_message", True) == []
        assert find_audit(read_audit(settings), "send_message", False) == []
        res = await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 1,
                "job_ref": job_ref,
            },
        )
        assert res["source_message_id"] == 1 and res["reply_to_message_id"] == 1
        assert len(channel.sent) == 1
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1  # only the int control audited


# --- F7: unvalidated job_ref leaves no audit trace; accepted IDs allowlisted ------


async def test_invalid_job_ref_canary_never_reaches_audit(settings, monkeypatch):
    canary = "CANARY_JOBREF_F7_XQ"
    bogus_ref = f"bogus-{canary}-ref"
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        seed_active_job(settings)
        with pytest.raises(ToolError, match="correlation rejected"):
            await mcp.call_tool(
                "send_message",
                {
                    "content": "auto reply",
                    "channel_id": CHANNEL_ID,
                    "source_message_id": 701,
                    "job_ref": bogus_ref,
                },
            )
        assert channel.sent == []
    assert canary not in settings.audit_path.read_text(encoding="utf-8")
    recs = find_audit(read_audit(settings), "send_message", False)
    assert len(recs) == 1
    args = recs[0]["args"]
    assert "job_ref" not in args  # rejected IDs never audited raw
    assert args["source_message_id"] == 701  # strict-positive MID is metadata-safe raw
    assert "job_ref_len" not in args  # literal F2: no fingerprints, not even hashes
    assert "job_ref_sha256" not in args
    assert recs[0]["error"] == "send_message failed kind=validation_error"


async def test_accepted_correlation_audited_allowlist(settings, monkeypatch):
    # Phase 4/F2: accepted correlated audits carry the fixed allowlist
    # (channel_id + source_message_id + content_len) -- job_ref NEVER
    # appears, accepted or rejected.
    _init_store(settings)
    channel = make_text_channel(CHANNEL_ID)
    bot = FakeBot(FakeGuild(GUILD_ID, {CHANNEL_ID: channel}))
    async with parked_server(monkeypatch, settings, bot) as (mcp, ctx, _):
        job_ref = seed_active_job(settings)
        await call_json(
            mcp,
            "send_message",
            {
                "content": "auto reply",
                "channel_id": CHANNEL_ID,
                "source_message_id": 701,
                "job_ref": job_ref,
            },
        )
    ok_recs = find_audit(read_audit(settings), "send_message", True)
    assert len(ok_recs) == 1
    assert ok_recs[0]["args"] == {
        "channel_id": CHANNEL_ID,
        "source_message_id": 701,
        "content_len": len("auto reply"),
    }


# --- F8: tmp/fake-only settings, no real dotenv/env reads --------------------


def test_settings_fixture_disables_dotenv_source(tmp_path, monkeypatch):
    """The fixture construction must not select or read any dotenv file."""
    import pathlib

    from pydantic_settings.sources import DotEnvSettingsSource

    selected: list = []
    env_stats: list = []
    file_reads: list = []
    orig_init = DotEnvSettingsSource.__init__
    orig_is_file = pathlib.Path.is_file

    def spy_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        selected.append(self.env_file)

    def spy_is_file(self):
        if self.name == ".env":
            env_stats.append(str(self))
        return orig_is_file(self)

    def no_real_reads(self, *args, **kwargs):
        # Guard: the control below must attempt (not perform) a real read.
        file_reads.append(True)
        return {}

    monkeypatch.setattr(DotEnvSettingsSource, "__init__", spy_init)
    monkeypatch.setattr(pathlib.Path, "is_file", spy_is_file)
    monkeypatch.setattr(DotEnvSettingsSource, "_read_env_file", no_real_reads)
    _make_settings(tmp_path)
    assert selected == [None]  # dotenv source disabled
    assert env_stats == []  # never even stats a dotenv path
    assert file_reads == []  # zero real file reads

    # Control: the same kwargs WITHOUT _env_file select the project .env and
    # stat its path (any actual read stays blocked by the guard above).
    selected.clear()
    env_stats.clear()
    file_reads.clear()
    legacy_kwargs = _settings_kwargs(tmp_path)
    legacy_kwargs.pop("_env_file")
    Settings(**legacy_kwargs)
    assert len(selected) == 1 and selected[0] is not None
    assert env_stats != []


def test_settings_fixture_pins_env_sensitive_defaults(tmp_path, monkeypatch):
    """Explicit fixture values win over ambient environment variables."""
    monkeypatch.setenv("TARGET_BOT_ID", "env-hijack")
    monkeypatch.setenv("RESPONDER_CHECK_SECONDS", "999")
    monkeypatch.setenv("RECOVERY_POLL_SECONDS", "999")
    made = _make_settings(tmp_path, target_bot_id="pinned-bot")
    assert made.target_bot_id == "pinned-bot"
    assert made.responder_check_seconds == 0.05
    assert made.recovery_poll_seconds == 30.0
