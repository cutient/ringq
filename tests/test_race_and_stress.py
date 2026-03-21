# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

"""Tests for join() race condition, EVICT_NEW + dedup replace, and stress scenarios."""

import asyncio


from ringq import Queue


class TestJoinRace:
    """Verify that join() doesn't hang when task_done() fires concurrently."""

    async def test_join_concurrent_task_done(self):
        """task_done() completing between join()'s flag check and Event.clear()
        must not cause join() to hang."""
        q = Queue()
        await q.put(1)

        # Simulate: task_done fires right as join starts
        async def do_task_done():
            await asyncio.sleep(0)  # yield once so join() starts
            q.task_done()

        task = asyncio.create_task(do_task_done())
        await asyncio.wait_for(q.join(), timeout=1.0)
        await task

    async def test_join_immediate_when_unfinished_zero(self):
        """join() returns immediately when no unfinished tasks."""
        q = Queue()
        await q.put(1)
        q.get_nowait()
        q.task_done()

        # Should return instantly, not hang
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_multiple_concurrent_joiners_with_task_done(self):
        """Multiple coroutines joining while task_done() fires."""
        q = Queue()
        for i in range(5):
            await q.put(i)

        results = []

        async def joiner(idx):
            await q.join()
            results.append(idx)

        tasks = [asyncio.create_task(joiner(i)) for i in range(3)]
        await asyncio.sleep(0.01)

        # Drain and complete all tasks
        for _ in range(5):
            q.get_nowait()
            q.task_done()

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1.0)
        assert sorted(results) == [0, 1, 2]

    async def test_join_put_task_done_interleaved(self):
        """Rapidly interleaving put/task_done with join."""
        q = Queue()

        async def producer():
            for i in range(100):
                await q.put(i)

        async def consumer():
            for _ in range(100):
                await q.get()
                q.task_done()

        await asyncio.gather(producer(), consumer())
        await asyncio.wait_for(q.join(), timeout=1.0)


class TestEvictNewDedupReplace:
    """Verify EVICT_NEW + dedup=replace interaction is correct."""

    def test_replace_existing_when_full(self):
        """Replacing an existing key when queue is full should succeed."""
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        assert q.qsize() == 3

        # Replace existing key — should work even though full
        result = q.put_nowait("a")
        assert result is True
        assert q.qsize() == 3

        # Verify ordering: b, c, a (replaced 'a' moved to tail)
        assert q.get_nowait() == "b"
        assert q.get_nowait() == "c"
        assert q.get_nowait() == "a"

    def test_new_key_dropped_when_full(self):
        """A new key when queue is full with EVICT_NEW should be dropped."""
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")

        result = q.put_nowait("d")
        assert result is False
        assert q.qsize() == 3

        # Original items preserved
        assert q.get_nowait() == "a"
        assert q.get_nowait() == "b"
        assert q.get_nowait() == "c"

    def test_replace_with_key_func_when_full(self):
        """Replace via key function when queue is full."""
        q = Queue(2, eviction="new", dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "old1"})
        q.put_nowait({"id": 2, "v": "old2"})
        assert q.qsize() == 2

        # Replace existing key
        result = q.put_nowait({"id": 1, "v": "new1"})
        assert result is True
        assert q.qsize() == 2

        # id=2 first (was not replaced), then id=1 (moved to tail)
        assert q.get_nowait() == {"id": 2, "v": "old2"}
        assert q.get_nowait() == {"id": 1, "v": "new1"}

    def test_unfinished_correct_after_replace_when_full(self):
        """Unfinished count stays correct during replace on a full queue."""
        q = Queue(2, eviction="new", dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        assert q._unfinished_tasks == 2

        # Replace 'a' — net unfinished stays the same
        q.put_nowait("a")
        assert q._unfinished_tasks == 2

        # Drop 'c' (new key, full, EVICT_NEW) — no change
        q.put_nowait("c")
        assert q._unfinished_tasks == 2

    def test_stats_after_replace_and_drop(self):
        """Stats correctly track replacements and non-evictions."""
        q = Queue(2, eviction="new", dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")

        q.put_nowait("a")  # replace
        q.put_nowait("c")  # dropped (EVICT_NEW, new key)

        stats = q.stats()
        assert stats["dedup_replacements"] == 1
        assert stats["evictions"] == 0  # EVICT_NEW doesn't count as eviction


class TestConcurrentStress:
    """Stress tests with concurrent put/get/task_done/join."""

    async def test_many_producers_many_consumers_with_join(self):
        """Multiple producers and consumers with join()."""
        q = Queue(100)
        total_items = 1000
        n_producers = 5
        n_consumers = 5
        items_per_producer = total_items // n_producers

        produced = []
        consumed = []

        async def producer(start):
            for i in range(items_per_producer):
                await q.put(start + i)
                produced.append(start + i)

        async def consumer():
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=0.5)
                    consumed.append(item)
                    q.task_done()
                except (asyncio.TimeoutError, Exception):
                    break

        producers = [
            asyncio.create_task(producer(i * items_per_producer))
            for i in range(n_producers)
        ]
        consumers = [asyncio.create_task(consumer()) for _ in range(n_consumers)]

        await asyncio.gather(*producers)
        await asyncio.wait_for(q.join(), timeout=5.0)

        # Cancel consumers
        for c in consumers:
            c.cancel()
        await asyncio.gather(*consumers, return_exceptions=True)

        assert sorted(consumed) == sorted(produced)

    async def test_rapid_put_get_bounded(self):
        """Rapid put/get on a tiny bounded queue."""
        q = Queue(1)
        n = 500

        async def producer():
            for i in range(n):
                await q.put(i)

        async def consumer():
            results = []
            for _ in range(n):
                results.append(await q.get())
            return results

        prod = asyncio.create_task(producer())
        cons = asyncio.create_task(consumer())
        results = await asyncio.wait_for(cons, timeout=5.0)
        await prod
        assert results == list(range(n))

    async def test_eviction_under_pressure(self):
        """Many producers with eviction — queue stays bounded."""
        q = Queue(10, eviction=True)
        n = 5000

        async def producer():
            for i in range(n):
                await q.put(i)

        await producer()
        assert q.qsize() <= 10

        # Drain and verify FIFO of remaining items
        items = []
        while not q.empty():
            items.append(q.get_nowait())
        # Last 10 items should be the final 10 produced
        assert items == list(range(n - 10, n))

    async def test_dedup_replace_under_pressure(self):
        """Many replacements on a dedup queue."""
        q = Queue(100, dedup="replace")
        # Insert 100 unique keys
        for i in range(100):
            q.put_nowait(i)

        # Replace each key 50 times
        for _ in range(50):
            for i in range(100):
                q.put_nowait(i)

        assert q.qsize() == 100
        stats = q.stats()
        assert stats["dedup_replacements"] == 5000
