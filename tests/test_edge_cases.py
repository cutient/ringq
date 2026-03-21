# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import pytest

from ringq import Queue, QueueEmpty


class TestClear:
    def test_clear_empties_queue(self, queue):
        for i in range(10):
            queue.put_nowait(i)
        queue.clear()
        assert queue.qsize() == 0
        assert queue.empty()

    def test_clear_resets_unfinished(self):
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.clear()
        assert q._unfinished_tasks == 0

    def test_clear_preserves_stats(self):
        q = Queue(3, eviction=True)
        for i in range(5):
            q.put_nowait(i)
        q.clear()
        s = q.stats()
        assert s["evictions"] == 2

    def test_usable_after_clear(self, queue):
        queue.put_nowait(1)
        queue.clear()
        queue.put_nowait(2)
        assert queue.get_nowait() == 2


class TestPeek:
    def test_peek(self, queue):
        queue.put_nowait(1)
        assert queue.peek_nowait() == 1
        assert queue.qsize() == 1  # not removed

    def test_peek_empty(self, queue):
        with pytest.raises(QueueEmpty):
            queue.peek_nowait()

    def test_peek_fifo(self, queue):
        queue.put_nowait(1)
        queue.put_nowait(2)
        assert queue.peek_nowait() == 1


class TestStats:
    def test_initial_stats(self, queue):
        s = queue.stats()
        assert s["evictions"] == 0
        assert s["dedup_drops"] == 0
        assert s["dedup_replacements"] == 0
        assert s["invalidated_skips"] == 0
        assert s["maxsize"] == 0

    def test_stats_maxsize(self, bounded_queue):
        assert bounded_queue.stats()["maxsize"] == 5


class TestNoneAsItem:
    def test_none_item(self, queue):
        queue.put_nowait(None)
        assert queue.qsize() == 1
        assert queue.get_nowait() is None

    def test_none_item_fifo(self, queue):
        queue.put_nowait(None)
        queue.put_nowait(1)
        queue.put_nowait(None)
        assert queue.get_nowait() is None
        assert queue.get_nowait() == 1
        assert queue.get_nowait() is None


class TestGrowth:
    def test_unbounded_grows(self):
        q = Queue()
        # Push more than initial capacity (16)
        for i in range(100):
            q.put_nowait(i)
        assert q.qsize() == 100
        for i in range(100):
            assert q.get_nowait() == i


class TestConstructorValidation:
    def test_invalid_dedup_mode(self):
        with pytest.raises(ValueError, match="invalid dedup"):
            Queue(dedup="invalid")  # type: ignore

    def test_key_without_dedup(self):
        with pytest.raises(ValueError, match="key requires dedup"):
            Queue(key=lambda x: x)
