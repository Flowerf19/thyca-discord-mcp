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
