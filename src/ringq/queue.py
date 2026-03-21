# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Callable, Literal

from ringq._core import RingBuffer
from ringq._exceptions import QueueEmpty, QueueFull, QueueShutDown
from ringq._fast_validate import is_jsonable

try:
    import orjson

    def _json_validate(item):
        try:
            orjson.dumps(item)
        except orjson.JSONEncodeError as e:
            raise TypeError(f"not JSON-serializable: {e}") from None

except ImportError:
    import json as _json_mod

    def _json_validate(item):
        try:
            _json_mod.dumps(item)
        except (TypeError, ValueError) as e:
            raise TypeError(f"not JSON-serializable: {e}") from None


class Queue:
    """Async queue backed by a Cython ring buffer.

    Drop-in replacement for asyncio.Queue with optional eviction,
    deduplication, and JSON validation.

    Not thread-safe — intended for use within a single asyncio event loop.
    """

    __slots__ = (
        "_maxsize",
        "_eviction",
        "_core",
        "_getters",
        "_putters",
        "_finished",
        "_finished_flag",
        "_slow_flags",  # bits: 0=shutdown, 1-2=validate_mode
    )

    def __init__(
        self,
        maxsize: int = 0,
        *,
        eviction: bool | Literal["old", "new"] = False,
        dedup: bool | Literal["drop", "replace"] = False,
        key: Callable[[Any], Any] | None = None,
        validate: bool | Literal["fast", "full"] = False,
    ) -> None:
        # Normalize eviction
        if eviction is True or eviction == "old":
            eviction_mode = 1  # EVICT_OLD
        elif eviction == "new":
            eviction_mode = 2  # EVICT_NEW
        elif eviction is False or eviction is None:
            eviction_mode = 0  # EVICT_NONE
        else:
            raise ValueError(f"invalid eviction mode: {eviction!r}")

        if eviction_mode != 0 and maxsize <= 0:
            raise ValueError("eviction requires maxsize > 0")

        # Normalize dedup
        if dedup is True:
            dedup_mode = 1  # drop
        elif dedup == "drop":
            dedup_mode = 1
        elif dedup == "replace":
            dedup_mode = 2
        elif dedup is False or dedup is None:
            dedup_mode = 0
        else:
            raise ValueError(f"invalid dedup mode: {dedup!r}")

        if key is not None and dedup_mode == 0:
            raise ValueError("key requires dedup to be enabled")

        # Normalize validate
        if validate is True or validate == "fast":
            validate_mode = 1  # fast (Cython)
        elif validate == "full":
            validate_mode = 2  # orjson/json serialization
        elif validate is False or validate is None:
            validate_mode = 0
        else:
            raise ValueError(f"invalid validate mode: {validate!r}")

        self._maxsize = maxsize
        self._eviction = eviction_mode
        self._slow_flags = (
            validate_mode << 1
        )  # bits 1-2 = validate_mode, bit 0 = shutdown
        self._core = RingBuffer(maxsize, eviction_mode, dedup_mode, key)

        # Async state
        self._getters = deque()
        self._putters = deque()
        self._finished_flag = True
        self._finished = asyncio.Event()
        self._finished.set()

    @property
    def maxsize(self) -> int:
        """The queue's maximum capacity (0 means unbounded)."""
        return self._maxsize

    @property
    def _unfinished_tasks(self) -> int:
        return self._core._unfinished

    def qsize(self) -> int:
        """Return the number of items in the queue."""
        return self._core._live

    def __len__(self) -> int:
        """Return the number of items in the queue."""
        return self._core._live

    def empty(self) -> bool:
        """Return True if the queue is empty."""
        return self._core._live == 0

    def full(self) -> bool:
        """Return True if bounded and at capacity."""
        if self._maxsize <= 0:
            return False
        return self._core._live >= self._maxsize

    def put_nowait(self, item: Any) -> bool:
        """Put an item into the queue without blocking.

        Returns True if the item was inserted, False if the item was
        silently discarded (dedup-drop or eviction='new' when full).
        Raises QueueFull if bounded and full without eviction.
        Raises QueueShutDown if the queue has been shut down.
        """
        sf = self._slow_flags
        if sf:
            if sf & 1:
                raise QueueShutDown("queue has been shut down")
            vm = sf >> 1
            if vm == 1:
                if not is_jsonable(item):
                    raise TypeError(
                        f"type {type(item).__name__} is not JSON-compatible"
                    )
            elif vm == 2:
                _json_validate(item)

        core = self._core
        flags = core.put(item)

        if flags == 1:
            # Fast path: simple insert, no eviction or replacement
            if self._finished_flag:
                self._finished_flag = False
            if self._getters:
                self._wakeup_next(self._getters)
            return True

        if flags == 0:
            return False

        # Slow path: eviction and/or replacement involved
        unfinished = core._unfinished
        if unfinished > 0:
            self._finished_flag = False
        elif unfinished == 0:
            self._finished_flag = True
            self._finished.set()

        if self._getters:
            self._wakeup_next(self._getters)

        return True

    def get_nowait(self) -> Any:
        """Remove and return an item. Raises QueueEmpty if empty.
        Raises QueueShutDown if the queue has been shut down and is empty.
        """
        core = self._core
        if self._slow_flags & 1 and core._live == 0:
            raise QueueShutDown("queue has been shut down")
        item = core.get()
        if self._putters:
            self._wakeup_next(self._putters)
        return item

    async def put(self, item: Any) -> bool:
        """Put an item into the queue, waiting if necessary.

        Returns True if inserted, False if dedup-dropped.
        With eviction enabled, never blocks.
        Raises QueueShutDown if the queue has been shut down.
        """
        while True:
            if self._slow_flags & 1:
                raise QueueShutDown("queue has been shut down")
            try:
                return self.put_nowait(item)
            except QueueFull:
                pass
            future = asyncio.get_running_loop().create_future()
            self._putters.append(future)
            try:
                await future
            except:
                future.cancel()
                try:
                    self._putters.remove(future)
                except ValueError:
                    pass
                if not self.full() and not future.cancelled():
                    self._wakeup_next(self._putters)
                raise

    async def get(self) -> Any:
        """Remove and return an item, waiting if necessary.
        Raises QueueShutDown if the queue has been shut down and is empty.
        """
        while self.empty():
            if self._slow_flags & 1:
                raise QueueShutDown("queue has been shut down")
            future = asyncio.get_running_loop().create_future()
            self._getters.append(future)
            try:
                await future
            except:
                future.cancel()
                try:
                    self._getters.remove(future)
                except ValueError:
                    pass
                if not self.empty() and not future.cancelled():
                    self._wakeup_next(self._getters)
                raise
        return self.get_nowait()

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        core = self._core
        if core._unfinished <= 0:
            raise ValueError("task_done() called too many times")
        core._unfinished -= 1
        if core._unfinished == 0:
            self._finished_flag = True
            self._finished.set()

    async def join(self) -> None:
        """Block until all items have been gotten and processed."""
        if self._core._unfinished == 0:
            return
        self._finished.clear()
        if self._core._unfinished == 0:
            self._finished.set()
            return
        await self._finished.wait()

    def peek_nowait(self) -> Any:
        """Return the next item without removing it. Raises QueueEmpty if empty."""
        if self.empty():
            raise QueueEmpty("queue is empty")
        return self._core.peek()

    def clear(self) -> None:
        """Remove all items from the queue and reset unfinished tasks."""
        self._core.clear()
        self._finished_flag = True
        self._finished.set()

    def stats(self) -> dict[str, Any]:
        """Return queue statistics."""
        s = self._core.get_stats()
        s["maxsize"] = self._maxsize
        return s

    def shutdown(self, immediate: bool = False) -> None:
        """Shut down the queue, disallowing further puts.

        If *immediate* is True, also disallow gets and cancel all waiters.
        Remaining items are discarded and unfinished tasks are reset so
        that ``join()`` unblocks.

        Compatible with asyncio.Queue.shutdown() from Python 3.13+.
        """
        self._slow_flags |= 1

        if immediate:
            # Discard remaining items and reset unfinished count
            self._core.clear()
            self._finished_flag = True
            self._finished.set()
            # Wake all waiters with QueueShutDown
            while self._getters:
                waiter = self._getters.popleft()
                if not waiter.done():
                    waiter.set_result(None)
            while self._putters:
                waiter = self._putters.popleft()
                if not waiter.done():
                    waiter.set_result(None)
        else:
            # Wake putters so they see the shutdown flag and raise
            while self._putters:
                waiter = self._putters.popleft()
                if not waiter.done():
                    waiter.set_result(None)
            # Wake getters — if queue is empty they'll raise QueueShutDown
            if self._core._live == 0:
                while self._getters:
                    waiter = self._getters.popleft()
                    if not waiter.done():
                        waiter.set_result(None)

    def _wakeup_next(self, waiters: deque[asyncio.Future[None]]) -> None:
        """Wake up the next waiter in the deque."""
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break
