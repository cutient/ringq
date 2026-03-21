# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT


class TestFIFOOrdering:
    def test_basic_fifo(self, queue):
        for i in range(5):
            queue.put_nowait(i)
        for i in range(5):
            assert queue.get_nowait() == i

    def test_fifo_with_strings(self, queue):
        items = ["alpha", "beta", "gamma"]
        for item in items:
            queue.put_nowait(item)
        for item in items:
            assert queue.get_nowait() == item

    def test_fifo_interleaved(self, queue):
        queue.put_nowait(1)
        queue.put_nowait(2)
        assert queue.get_nowait() == 1
        queue.put_nowait(3)
        assert queue.get_nowait() == 2
        assert queue.get_nowait() == 3


class TestQueueSize:
    def test_qsize(self, queue):
        assert queue.qsize() == 0
        queue.put_nowait(1)
        assert queue.qsize() == 1
        queue.put_nowait(2)
        assert queue.qsize() == 2
        queue.get_nowait()
        assert queue.qsize() == 1

    def test_len(self, queue):
        assert len(queue) == 0
        queue.put_nowait(1)
        assert len(queue) == 1

    def test_empty(self, queue):
        assert queue.empty()
        queue.put_nowait(1)
        assert not queue.empty()
        queue.get_nowait()
        assert queue.empty()

    def test_full_unbounded(self, queue):
        for i in range(100):
            queue.put_nowait(i)
        assert not queue.full()

    def test_full_bounded(self, bounded_queue):
        assert not bounded_queue.full()
        for i in range(5):
            bounded_queue.put_nowait(i)
        assert bounded_queue.full()
