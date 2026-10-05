import asyncio

import pytest

from thyca_discord_mcp.tasks import TaskRegistry


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
