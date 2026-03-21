# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

"""Tests ensuring ringq.Queue is a complete drop-in for asyncio.Queue.

Covers every public method, semantic guarantee, and edge case
from the asyncio.Queue contract that isn't already exercised
in the feature-specific test modules.
"""

import asyncio

import pytest

from ringq import Queue, QueueEmpty, QueueFull


# ---------------------------------------------------------------------------
# get_nowait / put_nowait edge cases
# ---------------------------------------------------------------------------


class TestGetNowaitEmpty:
    def test_raises_queue_empty(self):
        q = Queue()
        with pytest.raises(QueueEmpty):
            q.get_nowait()

    def test_raises_queue_empty_after_drain(self):
        q = Queue()
        q.put_nowait(1)
        q.get_nowait()
        with pytest.raises(QueueEmpty):
            q.get_nowait()

    def test_raises_queue_empty_bounded(self):
        q = Queue(5)
        with pytest.raises(QueueEmpty):
            q.get_nowait()


class TestPutNowaitReturnValue:
    def test_returns_true_on_insert(self):
        q = Queue()
        assert q.put_nowait(42) is True

    def test_returns_true_bounded(self):
        q = Queue(5)
        assert q.put_nowait(42) is True

    def test_unbounded_never_raises_full(self):
        q = Queue()
        for i in range(1000):
            q.put_nowait(i)
        assert q.qsize() == 1000


# ---------------------------------------------------------------------------
# maxsize=1 — tightest bounded queue
# ---------------------------------------------------------------------------


class TestMaxsizeOne:
    def test_single_slot(self):
        q = Queue(1)
        q.put_nowait("a")
        assert q.full()
        with pytest.raises(QueueFull):
            q.put_nowait("b")
        assert q.get_nowait() == "a"
        assert q.empty()

    async def test_put_blocks_then_unblocks(self):
        q = Queue(1)
        q.put_nowait("a")

        order = []

        async def putter():
            await q.put("b")
            order.append("put_done")

        async def getter():
            await asyncio.sleep(0.01)
            val = q.get_nowait()
            order.append("get_done")
            return val

        task = asyncio.create_task(putter())
        val = await getter()
        await task
        assert val == "a"
        assert q.get_nowait() == "b"
        assert "get_done" in order
        assert "put_done" in order

    async def test_alternating_put_get(self):
        q = Queue(1)
        for i in range(50):
            await q.put(i)
            assert await q.get() == i


# ---------------------------------------------------------------------------
# Negative maxsize — asyncio.Queue treats it as unbounded
# ---------------------------------------------------------------------------


class TestNegativeMaxsize:
    def test_negative_maxsize_acts_unbounded(self):
        q = Queue(-1)
        assert not q.full()
        for i in range(100):
            q.put_nowait(i)
        assert not q.full()
        assert q.qsize() == 100


# ---------------------------------------------------------------------------
# Multiple concurrent putters
# ---------------------------------------------------------------------------


class TestMultiplePutters:
    async def test_multiple_putters_wake_in_order(self):
        q = Queue(1)
        q.put_nowait("first")

        order = []

        async def putter(label, item):
            await q.put(item)
            order.append(label)

        t1 = asyncio.create_task(putter("p1", "second"))
        await asyncio.sleep(0.005)
        t2 = asyncio.create_task(putter("p2", "third"))
        await asyncio.sleep(0.005)
        t3 = asyncio.create_task(putter("p3", "fourth"))
        await asyncio.sleep(0.005)

        # Drain one-by-one, allowing putters to wake
        assert q.get_nowait() == "first"
        await asyncio.sleep(0.01)
        assert q.get_nowait() == "second"
        await asyncio.sleep(0.01)
        assert q.get_nowait() == "third"
        await asyncio.sleep(0.01)

        await asyncio.gather(t1, t2, t3)
        assert q.get_nowait() == "fourth"

        # Putters should wake in FIFO order
        assert order == ["p1", "p2", "p3"]

    async def test_multiple_getters_wake_in_order(self):
        q = Queue()
        results = []

        async def getter(label):
            val = await q.get()
            results.append((label, val))

        t1 = asyncio.create_task(getter("g1"))
        await asyncio.sleep(0.005)
        t2 = asyncio.create_task(getter("g2"))
        await asyncio.sleep(0.005)
        t3 = asyncio.create_task(getter("g3"))
        await asyncio.sleep(0.005)

        q.put_nowait(10)
        await asyncio.sleep(0.01)
        q.put_nowait(20)
        await asyncio.sleep(0.01)
        q.put_nowait(30)

        await asyncio.gather(t1, t2, t3)
        labels = [label for label, _ in results]
        # Getters should wake in FIFO order
        assert labels == ["g1", "g2", "g3"]
        values = [val for _, val in results]
        assert values == [10, 20, 30]


# ---------------------------------------------------------------------------
# Wake-up chains: get wakes putter, put wakes getter
# ---------------------------------------------------------------------------


class TestWakeUpChains:
    async def test_get_nowait_wakes_putter(self):
        q = Queue(2)
        q.put_nowait(1)
        q.put_nowait(2)

        put_done = asyncio.Event()

        async def blocked_putter():
            await q.put(3)
            put_done.set()

        task = asyncio.create_task(blocked_putter())
        await asyncio.sleep(0.01)
        assert not put_done.is_set()

        q.get_nowait()  # Should wake the putter
        await asyncio.wait_for(put_done.wait(), timeout=0.5)
        await task
        assert q.qsize() == 2

    async def test_put_nowait_wakes_getter(self):
        q = Queue()
        got = asyncio.Event()
        result = []

        async def blocked_getter():
            val = await q.get()
            result.append(val)
            got.set()

        task = asyncio.create_task(blocked_getter())
        await asyncio.sleep(0.01)
        assert not got.is_set()

        q.put_nowait(99)  # Should wake the getter
        await asyncio.wait_for(got.wait(), timeout=0.5)
        await task
        assert result == [99]


# ---------------------------------------------------------------------------
# Cancellation: cancelled waiter doesn't block others
# ---------------------------------------------------------------------------


class TestCancellationDoesNotBlock:
    async def test_cancelled_getter_doesnt_block_others(self):
        q = Queue()

        async def getter():
            return await q.get()

        t1 = asyncio.create_task(getter())
        await asyncio.sleep(0.005)
        t2 = asyncio.create_task(getter())
        await asyncio.sleep(0.005)

        # Cancel t1
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        # t2 should still be waiting and receive the value
        q.put_nowait(42)
        result = await asyncio.wait_for(t2, timeout=0.5)
        assert result == 42

    async def test_cancelled_putter_doesnt_block_others(self):
        q = Queue(1)
        q.put_nowait("fill")

        async def putter(item):
            await q.put(item)

        t1 = asyncio.create_task(putter("a"))
        await asyncio.sleep(0.005)
        t2 = asyncio.create_task(putter("b"))
        await asyncio.sleep(0.005)

        # Cancel t1
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        # t2 should still wake when space is available
        q.get_nowait()  # remove "fill"
        await asyncio.wait_for(t2, timeout=0.5)
        assert q.get_nowait() == "b"

    async def test_get_functional_after_multiple_cancellations(self):
        q = Queue()
        tasks = [asyncio.create_task(q.get()) for _ in range(5)]
        await asyncio.sleep(0.01)

        # Cancel all
        for t in tasks:
            t.cancel()
        for t in tasks:
            with pytest.raises(asyncio.CancelledError):
                await t

        # Queue must still be fully functional
        q.put_nowait("alive")
        assert q.get_nowait() == "alive"

    async def test_put_functional_after_multiple_cancellations(self):
        q = Queue(1)
        q.put_nowait("fill")

        tasks = [asyncio.create_task(q.put(f"item_{i}")) for i in range(5)]
        await asyncio.sleep(0.01)

        for t in tasks:
            t.cancel()
        for t in tasks:
            with pytest.raises(asyncio.CancelledError):
                await t

        # Queue must still be functional
        assert q.get_nowait() == "fill"
        q.put_nowait("new")
        assert q.get_nowait() == "new"


# ---------------------------------------------------------------------------
# join() — advanced scenarios
# ---------------------------------------------------------------------------


class TestJoinAdvanced:
    async def test_multiple_joiners(self):
        q = Queue()
        q.put_nowait(1)

        done_count = 0

        async def joiner():
            nonlocal done_count
            await q.join()
            done_count += 1

        t1 = asyncio.create_task(joiner())
        t2 = asyncio.create_task(joiner())
        t3 = asyncio.create_task(joiner())
        await asyncio.sleep(0.01)

        assert done_count == 0
        q.get_nowait()
        q.task_done()

        await asyncio.wait_for(asyncio.gather(t1, t2, t3), timeout=0.5)
        assert done_count == 3

    async def test_join_cancellation(self):
        q = Queue()
        q.put_nowait(1)

        task = asyncio.create_task(q.join())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Queue is still functional — complete the task
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_already_done(self):
        """join() returns immediately when no unfinished tasks."""
        q = Queue()
        q.put_nowait(1)
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_put_get_taskdone_cycle(self):
        q = Queue()
        for cycle in range(5):
            for i in range(3):
                q.put_nowait(i)
            for i in range(3):
                q.get_nowait()
                q.task_done()
            await asyncio.wait_for(q.join(), timeout=0.1)


# ---------------------------------------------------------------------------
# task_done — advanced scenarios
# ---------------------------------------------------------------------------


class TestTaskDoneAdvanced:
    def test_multiple_task_done(self):
        q = Queue()
        for i in range(5):
            q.put_nowait(i)
        for i in range(5):
            q.get_nowait()
        for i in range(5):
            q.task_done()
        assert q._unfinished_tasks == 0

    def test_partial_task_done_then_overcall(self):
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.get_nowait()
        q.get_nowait()
        q.task_done()
        q.task_done()
        with pytest.raises(ValueError, match="task_done.*too many"):
            q.task_done()

    def test_unfinished_tracks_puts_not_gets(self):
        """_unfinished_tasks counts puts, not gets. get() does NOT decrement."""
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        assert q._unfinished_tasks == 2
        q.get_nowait()
        assert q._unfinished_tasks == 2  # get does not change it
        q.task_done()
        assert q._unfinished_tasks == 1


# ---------------------------------------------------------------------------
# full() / empty() transition semantics
# ---------------------------------------------------------------------------


class TestStateTransitions:
    def test_empty_full_transitions(self):
        q = Queue(2)
        assert q.empty()
        assert not q.full()

        q.put_nowait(1)
        assert not q.empty()
        assert not q.full()

        q.put_nowait(2)
        assert not q.empty()
        assert q.full()

        q.get_nowait()
        assert not q.empty()
        assert not q.full()

        q.get_nowait()
        assert q.empty()
        assert not q.full()

    def test_qsize_matches_operations(self):
        q = Queue()
        ops = []
        for i in range(10):
            q.put_nowait(i)
            ops.append(("put", q.qsize()))
        for i in range(10):
            q.get_nowait()
            ops.append(("get", q.qsize()))
        put_sizes = [s for op, s in ops if op == "put"]
        get_sizes = [s for op, s in ops if op == "get"]
        assert put_sizes == list(range(1, 11))
        assert get_sizes == list(range(9, -1, -1))


# ---------------------------------------------------------------------------
# Various item types
# ---------------------------------------------------------------------------


class TestItemTypes:
    def test_callable_items(self):
        q = Queue()

        def fn(x):
            return x + 1

        q.put_nowait(fn)
        got = q.get_nowait()
        assert got(1) == 2

    def test_nested_containers(self):
        q = Queue()
        item = {"a": [1, {"b": (2, 3)}], "c": None}
        q.put_nowait(item)
        assert q.get_nowait() == item

    def test_large_item(self):
        q = Queue()
        item = list(range(100_000))
        q.put_nowait(item)
        assert q.get_nowait() == item

    def test_mixed_types_fifo(self):
        q = Queue()
        items = [42, "hello", 3.14, None, True, [1, 2], {"k": "v"}, b"bytes"]
        for item in items:
            q.put_nowait(item)
        for item in items:
            assert q.get_nowait() == item

    def test_exception_as_item(self):
        q = Queue()
        exc = ValueError("test error")
        q.put_nowait(exc)
        got = q.get_nowait()
        assert isinstance(got, ValueError)
        assert str(got) == "test error"


# ---------------------------------------------------------------------------
# Interleaved producer/consumer patterns
# ---------------------------------------------------------------------------


class TestProducerConsumer:
    async def test_concurrent_producers_consumers(self):
        q = Queue(10)
        produced = []
        consumed = []

        async def producer(start, count):
            for i in range(start, start + count):
                await q.put(i)
                produced.append(i)

        async def consumer(count):
            for _ in range(count):
                val = await q.get()
                consumed.append(val)

        await asyncio.gather(
            producer(0, 50),
            producer(50, 50),
            consumer(50),
            consumer(50),
        )

        assert sorted(consumed) == list(range(100))
        assert q.empty()

    async def test_producer_consumer_with_task_done(self):
        q = Queue()
        n = 20

        async def producer():
            for i in range(n):
                await q.put(i)

        async def consumer():
            for _ in range(n):
                await q.get()
                q.task_done()

        await asyncio.gather(producer(), consumer())
        await asyncio.wait_for(q.join(), timeout=0.5)


# ---------------------------------------------------------------------------
# Drain and refill cycles
# ---------------------------------------------------------------------------


class TestDrainRefill:
    def test_drain_and_refill(self):
        q = Queue(5)
        for cycle in range(10):
            for i in range(5):
                q.put_nowait(i + cycle * 5)
            assert q.full()
            for i in range(5):
                assert q.get_nowait() == i + cycle * 5
            assert q.empty()

    def test_partial_drain_refill(self):
        q = Queue(4)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)
        # Drain half
        assert q.get_nowait() == 1
        assert q.get_nowait() == 2
        # Refill
        q.put_nowait(5)
        q.put_nowait(6)
        assert q.full()
        # Drain all — verify FIFO
        assert q.get_nowait() == 3
        assert q.get_nowait() == 4
        assert q.get_nowait() == 5
        assert q.get_nowait() == 6
        assert q.empty()


# ---------------------------------------------------------------------------
# async put() return value
# ---------------------------------------------------------------------------


class TestAsyncPutReturn:
    async def test_async_put_returns_true(self):
        q = Queue()
        result = await q.put(42)
        assert result is True

    async def test_async_put_bounded_returns_true(self):
        q = Queue(5)
        result = await q.put(42)
        assert result is True


# ---------------------------------------------------------------------------
# Exception types match asyncio.Queue
# ---------------------------------------------------------------------------


class TestExceptionTypes:
    def test_queue_empty_is_exception(self):
        assert issubclass(QueueEmpty, Exception)

    def test_queue_full_is_exception(self):
        assert issubclass(QueueFull, Exception)

    def test_queue_empty_has_message(self):
        q = Queue()
        try:
            q.get_nowait()
        except QueueEmpty as e:
            assert e is not None

    def test_queue_full_has_message(self):
        q = Queue(1)
        q.put_nowait(1)
        try:
            q.put_nowait(2)
        except QueueFull as e:
            assert e is not None


# ---------------------------------------------------------------------------
# Stress / high-volume
# ---------------------------------------------------------------------------


class TestHighVolume:
    def test_many_items_fifo(self):
        q = Queue()
        n = 10_000
        for i in range(n):
            q.put_nowait(i)
        assert q.qsize() == n
        for i in range(n):
            assert q.get_nowait() == i
        assert q.empty()

    async def test_async_high_volume(self):
        q = Queue()
        n = 5_000

        async def producer():
            for i in range(n):
                await q.put(i)

        async def consumer():
            results = []
            for _ in range(n):
                results.append(await q.get())
            return results

        _, results = await asyncio.gather(producer(), consumer())
        assert results == list(range(n))

    def test_bounded_high_volume_wrap(self):
        """Exercises ring buffer wrap-around many times."""
        q = Queue(8)
        for i in range(1000):
            q.put_nowait(i)
            assert q.get_nowait() == i
        assert q.empty()
