import asyncio

import pytest

from thyca_discord_mcp.tasks import (
    AUTO_RECOVERY_TASK_NAME,
    AUTO_REPLY_TASK_NAME,
    TaskRegistry,
)


async def test_register_start_stop():
    calls = []

    async def tick():
        calls.append(1)

    registry = TaskRegistry()
    registry.register("t", tick, seconds=60.0)
    assert [t.name for t in registry.list_tasks()] == ["t"]
    assert registry.status("t").running is False

    registry.start("t")
    assert registry.status("t").running is True

    await registry.stop("t")
    assert registry.status("t").running is False


async def test_duplicate_unknown_and_double_start():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("t", tick, seconds=60.0)
    with pytest.raises(KeyError):
        registry.register("t", tick, seconds=60.0)
    with pytest.raises(KeyError):
        registry.start("nope")
    with pytest.raises(KeyError):
        await registry.stop("nope")

    registry.start("t")
    with pytest.raises(RuntimeError):
        registry.start("t")
    await registry.stop("t")
    await registry.stop("t")  # stopping an idle task is a no-op


async def test_first_tick_runs_immediately():
    fired = asyncio.Event()

    async def tick():
        fired.set()

    registry = TaskRegistry()
    registry.register("fast", tick, seconds=3600.0)
    registry.start("fast")
    try:
        # Tick must fire immediately on start, not after the first 3600s interval.
        await asyncio.wait_for(fired.wait(), timeout=5.0)
    finally:
        await registry.stop("fast")
    assert fired.is_set()


async def test_unregister_running_task():
    async def tick():
        pass

    registry = TaskRegistry()
    loop = registry.register("t", tick, seconds=3600.0)
    registry.start("t")
    await registry.unregister("t")
    with pytest.raises(KeyError):
        registry.status("t")
    assert registry.list_tasks() == []
    assert loop.is_running() is False
    task = loop.get_task()
    assert task is None or task.done()  # no leaked background task


async def test_unregister_idle_task():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("t", tick, seconds=60.0)
    await registry.unregister("t")
    with pytest.raises(KeyError):
        registry.status("t")
    assert registry.list_tasks() == []


async def test_unregister_unknown_raises():
    registry = TaskRegistry()
    with pytest.raises(KeyError, match="unknown task"):
        await registry.unregister("nope")


def test_reserved_task_name_constants():
    assert AUTO_REPLY_TASK_NAME == "auto-reply"
    assert AUTO_RECOVERY_TASK_NAME == "auto-reply-recovery"


async def test_protected_stop_blocked_by_default():
    fired = asyncio.Event()

    async def tick():
        fired.set()

    registry = TaskRegistry()
    registry.register("sys", tick, seconds=3600.0, protected=True)
    registry.start("sys")
    await asyncio.wait_for(fired.wait(), timeout=5.0)
    with pytest.raises(PermissionError, match="protected"):
        await registry.stop("sys")
    assert registry.status("sys").running is True  # no running-state change
    assert "sys" in [t.name for t in registry.list_tasks()]  # name preserved
    await registry.stop("sys", allow_protected=True)  # cleanup via owned forced API
    assert registry.status("sys").running is False


async def test_protected_stop_idle_blocked():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("sys", tick, seconds=60.0, protected=True)
    with pytest.raises(PermissionError, match="protected"):
        await registry.stop("sys")  # blocked even though idle (no silent no-op)
    assert registry.status("sys").running is False
    assert [t.name for t in registry.list_tasks()] == ["sys"]
    await registry.unregister("sys", allow_protected=True)


async def test_protected_unregister_blocked_by_default():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("sys", tick, seconds=3600.0, protected=True)
    registry.start("sys")
    with pytest.raises(PermissionError, match="protected"):
        await registry.unregister("sys")
    assert registry.status("sys").running is True
    assert [t.name for t in registry.list_tasks()] == ["sys"]
    await registry.unregister("sys", allow_protected=True)
    assert registry.list_tasks() == []


async def test_protected_forced_stop_settles_task():
    async def tick():
        pass

    registry = TaskRegistry()
    loop = registry.register("sys", tick, seconds=3600.0, protected=True)
    registry.start("sys")
    await registry.stop("sys", allow_protected=True)
    assert registry.status("sys").running is False
    task = loop.get_task()
    assert task is None or task.done()  # no leaked background task


async def test_unrelated_unprotected_stop_still_allowed():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("heartbeat", tick, seconds=300.0)
    registry.register("sys", tick, seconds=3600.0, protected=True)
    registry.start("heartbeat")
    registry.start("sys")
    await registry.stop("heartbeat")  # legacy heartbeat stop path unchanged
    assert registry.status("heartbeat").running is False
    assert registry.status("sys").running is True
    await registry.stop("sys", allow_protected=True)


async def test_protected_register_rejects_non_bool():
    async def tick():
        pass

    registry = TaskRegistry()
    with pytest.raises(TypeError):
        registry.register("sys", tick, seconds=60.0, protected="yes")  # type: ignore[arg-type]
    with pytest.raises(KeyError):
        registry.status("sys")  # failed registration leaves no entry


async def test_protected_start_twice_raises_runtime_error():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("sys", tick, seconds=3600.0, protected=True)
    registry.start("sys")
    with pytest.raises(RuntimeError, match="already running"):
        registry.start("sys")
    await registry.stop("sys", allow_protected=True)


async def test_stop_allow_protected_rejects_non_bool():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register("t", tick, seconds=60.0)
    with pytest.raises(TypeError):
        await registry.stop("t", allow_protected="yes")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await registry.unregister("t", allow_protected=1)  # type: ignore[arg-type]
    await registry.unregister("t")  # ordinary unregister still works


async def test_protected_duplicate_register_raises_key_error():
    async def tick():
        pass

    registry = TaskRegistry()
    registry.register(AUTO_REPLY_TASK_NAME, tick, seconds=1.0, protected=True)
    with pytest.raises(KeyError):
        registry.register(AUTO_REPLY_TASK_NAME, tick, seconds=1.0, protected=True)
    assert registry.status(AUTO_REPLY_TASK_NAME).running is False
    await registry.unregister(AUTO_REPLY_TASK_NAME, allow_protected=True)


async def test_failed_idle_stop_consumes_exception_without_raising():
    """Idle failed Loop: stop consumes the stale error, never raises it."""

    class _SafeTickBoom(RuntimeError):
        pass

    async def tick():
        raise _SafeTickBoom("safe test-only tick boom")

    registry = TaskRegistry()
    loop = registry.register("t", tick, seconds=3600.0)

    async def _quiet_error(exc):  # test-only: no log noise, record only
        _quiet_error.seen.append(exc)

    _quiet_error.seen = []
    loop.error(_quiet_error)

    registry.start("t")
    task = loop.get_task()
    assert task is not None
    try:
        settled, _ = await asyncio.wait({task}, timeout=5.0)
        assert settled == {task}
        assert task.done()
        assert not task.cancelled()
        assert loop.failed() is True
        assert registry.status("t").running is False
        assert task._log_traceback is True  # unretrieved before stop
        assert len(_quiet_error.seen) == 1
        assert isinstance(_quiet_error.seen[0], _SafeTickBoom)

        await registry.stop("t")  # consumes, never raises the stale error
        assert task._log_traceback is False
        assert loop.failed() is True  # consuming is not failure masking
        assert registry.status("t").running is False

        await registry.stop("t")  # repeat idle stop still no-op/no-raise
        assert task._log_traceback is False
        await registry.unregister("t")
        assert registry.list_tasks() == []
    finally:
        # If an assert failed before stop, retrieve for a clean test run.
        if task.done() and not task.cancelled():
            try:
                task.exception()
            except asyncio.CancelledError:
                pass
        if "t" in [info.name for info in registry.list_tasks()]:
            await registry.unregister("t")


async def test_protected_failed_idle_stop_blocked_before_consume():
    """Protected idle-failed Loop: PermissionError first, consume only via internal."""

    class _SafeProtectedBoom(RuntimeError):
        pass

    async def tick():
        raise _SafeProtectedBoom("safe protected tick boom")

    registry = TaskRegistry()
    loop = registry.register("sys", tick, seconds=3600.0, protected=True)

    async def _quiet_error(exc):  # test-only: no log noise, record only
        _quiet_error.seen.append(exc)

    _quiet_error.seen = []
    loop.error(_quiet_error)

    registry.start("sys")
    task = loop.get_task()
    assert task is not None
    try:
        settled, _ = await asyncio.wait({task}, timeout=5.0)
        assert settled == {task}
        assert task.done()
        assert not task.cancelled()
        assert loop.failed() is True
        assert task._log_traceback is True

        with pytest.raises(PermissionError, match="protected"):
            await registry.stop("sys")  # protection first, even when idle-failed
        assert task._log_traceback is True  # still unretrieved
        assert loop.failed() is True
        assert not task.cancelled()
        assert task.done()
        assert registry.status("sys").running is False
        assert "sys" in [info.name for info in registry.list_tasks()]

        await registry.stop("sys", allow_protected=True)  # internal consumes
        assert task._log_traceback is False
        assert loop.failed() is True
        assert not task.cancelled()

        await registry.stop("sys", allow_protected=True)  # repeat nonraise
        await registry.unregister("sys", allow_protected=True)
        assert registry.list_tasks() == []
    finally:
        if task.done() and not task.cancelled():
            try:
                task.exception()
            except asyncio.CancelledError:
                pass
        if "sys" in [info.name for info in registry.list_tasks()]:
            await registry.unregister("sys", allow_protected=True)


async def test_idle_variants_stop_and_unregister_noop():
    """Idle None / normal-done / cancelled: stop + repeat + unregister nonraise."""
    registry = TaskRegistry()

    # Idle-None: never started, no task at all.
    async def idle_tick():
        pass

    idle_loop = registry.register("idle", idle_tick, seconds=60.0)
    assert idle_loop.get_task() is None
    await registry.stop("idle")
    await registry.stop("idle")
    await registry.unregister("idle")
    assert "idle" not in [info.name for info in registry.list_tasks()]

    # Normal-done: loop.stop() inside the first tick -> graceful success.
    holder: dict = {}

    async def done_tick():
        holder["loop"].stop()

    done_loop = registry.register("done", done_tick, seconds=3600.0)
    holder["loop"] = done_loop
    registry.start("done")
    done_task = done_loop.get_task()
    assert done_task is not None
    settled, _ = await asyncio.wait({done_task}, timeout=5.0)
    assert settled == {done_task}
    assert done_task.done()
    assert not done_task.cancelled()
    assert done_loop.failed() is False
    assert done_task.exception() is None  # normally done, no error
    await registry.stop("done")
    await registry.stop("done")
    await registry.unregister("done")

    # Cancelled: start then cancel before the first tick settles.
    async def slow_tick():
        await asyncio.sleep(3600.0)

    cancel_loop = registry.register("cancelled", slow_tick, seconds=3600.0)
    registry.start("cancelled")
    cancel_task = cancel_loop.get_task()
    assert cancel_task is not None
    cancel_loop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task
    assert cancel_task.done()
    assert cancel_task.cancelled()
    assert cancel_loop.failed() is False
    # Idle stop must NOT call task.exception() (would raise CancelledError);
    # the implementation guards with `not task.cancelled()`.
    await registry.stop("cancelled")
    await registry.stop("cancelled")
    await registry.unregister("cancelled")
    assert registry.list_tasks() == []
