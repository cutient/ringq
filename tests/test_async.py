# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import asyncio

import pytest

from ringq import Queue


class TestAsyncPutGet:
    async def test_async_put_get(self):
        q = Queue()
        await q.put(1)
        assert await q.get() == 1

    async def test_get_waits_for_put(self):
        q = Queue()

        async def producer():
            await asyncio.sleep(0.01)
            await q.put(42)

        async def consumer():
            return await q.get()

        result = await asyncio.gather(consumer(), producer())
        assert result[0] == 42

    async def test_put_waits_when_full(self):
        q = Queue(1)
        await q.put("first")

        put_done = False

        async def delayed_put():
            nonlocal put_done
            await q.put("second")
            put_done = True

        async def delayed_get():
            await asyncio.sleep(0.01)
            return await q.get()

        task = asyncio.create_task(delayed_put())
        result = await delayed_get()
        await task
        assert result == "first"
        assert put_done

    async def test_multiple_getters(self):
        q = Queue()
        results = []

        async def getter(idx):
            val = await q.get()
            results.append((idx, val))

        tasks = [asyncio.create_task(getter(i)) for i in range(3)]
        await asyncio.sleep(0.01)

        for i in range(3):
            await q.put(i)
        await asyncio.gather(*tasks)

        # All getters should have received a value
        assert len(results) == 3
        values = sorted(v for _, v in results)
        assert values == [0, 1, 2]

    async def test_eviction_put_never_blocks(self):
        q = Queue(2, eviction=True)
        await q.put(1)
        await q.put(2)
        await q.put(3)  # should not block, evicts 1
        assert q.qsize() == 2


class TestCancellation:
    async def test_get_cancellation(self):
        q = Queue()
        task = asyncio.create_task(q.get())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Queue should still be functional
        q.put_nowait(1)
        assert q.get_nowait() == 1

    async def test_put_cancellation(self):
        q = Queue(1)
        q.put_nowait("fill")
        task = asyncio.create_task(q.put("blocked"))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Queue should still be functional
        assert q.get_nowait() == "fill"
