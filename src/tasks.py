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

AUTO_REPLY_TASK_NAME = "auto-reply"
AUTO_RECOVERY_TASK_NAME = "auto-reply-recovery"


@dataclass(frozen=True)
class TaskInfo:
    name: str
    interval_seconds: float
    running: bool
    current_loop: int


class TaskRegistry:
    def __init__(self) -> None:
        self._loops: dict[str, tasks.Loop] = {}
        self._protected: dict[str, bool] = {}

    def register(
        self, name: str, coro_fn: TickFn, *, seconds: float, protected: bool = False
    ) -> tasks.Loop:
        if not isinstance(protected, bool):
            raise TypeError(f"protected must be bool, got {type(protected).__name__}")
        if name in self._loops:
            raise KeyError(f"task already registered: {name}")
        loop = tasks.loop(seconds=seconds)(coro_fn)
        self._loops[name] = loop
        self._protected[name] = protected
        return loop

    def start(self, name: str) -> None:
        """Start a registered task. Needs a running event loop (tools always have one)."""
        loop = self._get(name)
        if loop.is_running():
            raise RuntimeError(f"task already running: {name}")
        loop.start()

    async def stop(self, name: str, *, allow_protected: bool = False) -> None:
        """Cancel the task and wait until it finishes. Stopping an idle task is a no-op.

        Protected tasks (internal v5 system loops) raise PermissionError unless
        allow_protected=True. The check runs before any cancel, even when idle.
        """
        if not isinstance(allow_protected, bool):
            raise TypeError(f"allow_protected must be bool, got {type(allow_protected).__name__}")
        loop = self._get(name)
        if self._protected[name] and not allow_protected:
            raise PermissionError(f"task is protected: {name}")
        if not loop.is_running():
            # Idle with an already-done failed task: retrieve (consume) the
            # exception so GC never logs "Task exception was never
            # retrieved". Never raise the stale error; the native Loop
            # error handler already logged it. Cancelled/None/success are
            # untouched; running-loop cancel/await behavior below unchanged.
            task = loop.get_task()
            if task is not None and task.done() and not task.cancelled():
                task.exception()
            return
        loop.cancel()
        task = loop.get_task()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def unregister(self, name: str, *, allow_protected: bool = False) -> None:
        """Stop a task (waiting for it) and remove it from the registry.

        Unknown names raise KeyError; unregistering an idle task just removes it.
        Protected tasks raise PermissionError unless allow_protected=True; the
        check (via stop) runs before any stop/removal, even when idle.
        """
        await self.stop(name, allow_protected=allow_protected)
        del self._loops[name]
        del self._protected[name]

    def status(self, name: str) -> TaskInfo:
        loop = self._get(name)
        # Registry always passes seconds=, so Loop.seconds is never None
        # (it is None only for explicit-time loops, which never occur here).
        seconds = loop.seconds
        assert seconds is not None
        return TaskInfo(
            name=name,
            interval_seconds=seconds,
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
