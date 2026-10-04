"""Named background-task registry over discord.ext.tasks.Loop.

Each entry runs as its own asyncio task; start/stop/list by name is what
gives the MCP tools their multi-task support.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass

from discord.ext import tasks

TickFn = Callable[[], Coroutine[None, None, None]]


@dataclass(frozen=True)
class TaskInfo:
    name: str
    interval_seconds: float
    running: bool
    current_loop: int


class TaskRegistry:
    def __init__(self) -> None:
        self._loops: dict[str, tasks.Loop] = {}
        self._intervals: dict[str, float] = {}

    def register(self, name: str, coro_fn: TickFn, *, seconds: float) -> tasks.Loop:
        if name in self._loops:
            raise KeyError(f"task already registered: {name}")
        loop = tasks.loop(seconds=seconds)(coro_fn)
        self._loops[name] = loop
        self._intervals[name] = seconds
        return loop

    def start(self, name: str) -> None:
        """Start a registered task. Needs a running event loop (tools always have one)."""
        loop = self._get(name)
        if loop.is_running():
            raise RuntimeError(f"task already running: {name}")
        loop.start()

    async def stop(self, name: str) -> None:
        """Cancel the task and wait until it finishes. Stopping an idle task is a no-op."""
        loop = self._get(name)
        if not loop.is_running():
            return
        loop.cancel()
        task = loop.get_task()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def status(self, name: str) -> TaskInfo:
        loop = self._get(name)
        return TaskInfo(
            name=name,
            interval_seconds=self._intervals[name],
            running=loop.is_running(),
            current_loop=loop.current_loop,
        )

    def list_tasks(self) -> list[TaskInfo]:
        return [self.status(name) for name in self._loops]

    def _get(self, name: str) -> tasks.Loop:
        try:
            return self._loops[name]
        except KeyError:
            raise KeyError(f"unknown task: {name}") from None
