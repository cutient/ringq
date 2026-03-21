# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import asyncio

import pytest

from ringq import Queue, QueueShutDown


class TestShutdownPutBlocked:
    """put_nowait and async put must raise after shutdown."""

    async def test_put_nowait_raises_after_shutdown(self):
        q = Queue()
        q.shutdown()
        with pytest.raises(QueueShutDown):
            q.put_nowait(1)

    async def test_async_put_raises_after_shutdown(self):
        q = Queue()
        q.shutdown()
        with pytest.raises(QueueShutDown):
            await q.put(1)

    async def test_blocked_putter_raises_on_shutdown(self):
        q = Queue(1)
        q.put_nowait("fill")

        async def blocked_put():
            await q.put("blocked")

        task = asyncio.create_task(blocked_put())
        await asyncio.sleep(0.01)
        q.shutdown()
        with pytest.raises(QueueShutDown):
            await task


class TestShutdownGetGraceful:
    """Graceful shutdown: gets drain remaining items, then raise."""

    async def test_get_nowait_returns_remaining_items(self):
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.shutdown()
        assert q.get_nowait() == 1
        assert q.get_nowait() == 2

    async def test_get_nowait_raises_when_empty_after_shutdown(self):
        q = Queue()
        q.put_nowait(1)
        q.shutdown()
        q.get_nowait()  # drain
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    async def test_async_get_returns_remaining_items(self):
        q = Queue()
        q.put_nowait(1)
        q.shutdown()
        assert await q.get() == 1

    async def test_async_get_raises_when_empty_after_shutdown(self):
        q = Queue()
        q.shutdown()
        with pytest.raises(QueueShutDown):
            await q.get()

    async def test_blocked_getter_raises_on_shutdown_empty(self):
        q = Queue()

        async def blocked_get():
            return await q.get()

        task = asyncio.create_task(blocked_get())
        await asyncio.sleep(0.01)
        q.shutdown()
        with pytest.raises(QueueShutDown):
            await task

    async def test_blocked_getter_gets_item_then_next_raises(self):
        """Getter blocked on empty queue; item is put, then shutdown.
        First get succeeds, next raises."""
        q = Queue()

        results = []

        async def getter():
            results.append(await q.get())

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.put_nowait(42)
        await task
        assert results == [42]

        q.shutdown()
        with pytest.raises(QueueShutDown):
            await q.get()


class TestShutdownImmediate:
    """immediate=True: discard items, cancel all waiters."""

    async def test_immediate_discards_items(self):
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.shutdown(immediate=True)
        assert q.qsize() == 0

    async def test_immediate_get_raises(self):
        q = Queue()
        q.put_nowait(1)
        q.shutdown(immediate=True)
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    async def test_immediate_put_raises(self):
        q = Queue()
        q.shutdown(immediate=True)
        with pytest.raises(QueueShutDown):
            q.put_nowait(1)

    async def test_immediate_unblocks_join(self):
        q = Queue()
        q.put_nowait(1)
        # unfinished tasks = 1, join would block
        q.shutdown(immediate=True)
        # join should return immediately
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_immediate_wakes_blocked_getter(self):
        q = Queue()

        async def blocked_get():
            return await q.get()

        task = asyncio.create_task(blocked_get())
        await asyncio.sleep(0.01)
        q.shutdown(immediate=True)
        with pytest.raises(QueueShutDown):
            await task

    async def test_immediate_wakes_blocked_putter(self):
        q = Queue(1)
        q.put_nowait("fill")

        async def blocked_put():
            await q.put("blocked")

        task = asyncio.create_task(blocked_put())
        await asyncio.sleep(0.01)
        q.shutdown(immediate=True)
        with pytest.raises(QueueShutDown):
            await task


class TestShutdownIdempotent:
    async def test_double_shutdown_is_noop(self):
        q = Queue()
        q.shutdown()
        q.shutdown()  # should not raise

    async def test_shutdown_then_immediate(self):
        q = Queue()
        q.put_nowait(1)
        q.shutdown()
        # items still drainable
        assert q.qsize() == 1
        q.shutdown(immediate=True)
        assert q.qsize() == 0


class TestShutdownWithFeatures:
    async def test_shutdown_with_eviction_queue(self):
        q = Queue(3, eviction=True)
        for i in range(3):
            q.put_nowait(i)
        q.shutdown()
        # Can still drain
        assert q.get_nowait() == 0
        with pytest.raises(QueueShutDown):
            q.put_nowait(99)

    async def test_shutdown_with_dedup_queue(self):
        q = Queue(5, dedup=True)
        q.put_nowait("a")
        q.put_nowait("b")
        q.shutdown()
        assert q.get_nowait() == "a"
        assert q.get_nowait() == "b"
        with pytest.raises(QueueShutDown):
            q.get_nowait()
