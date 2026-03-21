# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import pytest

from ringq import Queue


class TestEviction:
    def test_eviction_requires_maxsize(self):
        with pytest.raises(ValueError, match="eviction requires maxsize"):
            Queue(eviction=True)

    def test_eviction_discards_oldest(self, eviction_queue):
        for i in range(5):
            eviction_queue.put_nowait(i)
        eviction_queue.put_nowait(99)
        # Item 0 should have been evicted
        assert eviction_queue.get_nowait() == 1
        assert eviction_queue.qsize() == 4

    def test_eviction_stats(self, eviction_queue):
        for i in range(5):
            eviction_queue.put_nowait(i)
        eviction_queue.put_nowait(99)
        eviction_queue.put_nowait(100)
        s = eviction_queue.stats()
        assert s["evictions"] == 2

    def test_eviction_never_raises_full(self, eviction_queue):
        for i in range(100):
            eviction_queue.put_nowait(i)  # no exception
        assert eviction_queue.qsize() == 5

    def test_eviction_preserves_fifo(self, eviction_queue):
        for i in range(10):
            eviction_queue.put_nowait(i)
        # Last 5 should remain
        for i in range(5, 10):
            assert eviction_queue.get_nowait() == i

    def test_unfinished_tasks_with_eviction(self):
        q = Queue(3, eviction=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q._unfinished_tasks == 3
        q.put_nowait(4)  # evicts 1
        # Net: +1 insert, -1 eviction = 0 delta
        assert q._unfinished_tasks == 3

    def test_eviction_old_string_same_as_true(self):
        q = Queue(3, eviction="old")
        for i in range(5):
            q.put_nowait(i)
        assert q.qsize() == 3
        assert q.get_nowait() == 2

    def test_invalid_eviction_mode(self):
        with pytest.raises(ValueError, match="invalid eviction mode"):
            Queue(3, eviction="invalid")  # type: ignore


class TestEvictionNew:
    def test_eviction_new_requires_maxsize(self):
        with pytest.raises(ValueError, match="eviction requires maxsize"):
            Queue(eviction="new")

    def test_drops_new_when_full(self):
        q = Queue(3, eviction="new")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        result = q.put_nowait(4)  # dropped
        assert result is False
        assert q.qsize() == 3
        assert q.get_nowait() == 1  # oldest preserved

    def test_preserves_all_original_items(self):
        q = Queue(5, eviction="new")
        for i in range(5):
            q.put_nowait(i)
        for i in range(10):
            q.put_nowait(100 + i)  # all dropped
        assert q.qsize() == 5
        for i in range(5):
            assert q.get_nowait() == i

    def test_accepts_after_drain(self):
        q = Queue(2, eviction="new")
        q.put_nowait(1)
        q.put_nowait(2)
        assert q.put_nowait(3) is False  # dropped
        q.get_nowait()  # drain one
        assert q.put_nowait(3) is True  # now accepted
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3

    def test_never_raises_queue_full(self):
        q = Queue(3, eviction="new")
        for i in range(100):
            q.put_nowait(i)  # no exception
        assert q.qsize() == 3

    def test_unfinished_tasks_not_incremented_on_drop(self):
        q = Queue(2, eviction="new")
        q.put_nowait(1)
        q.put_nowait(2)
        assert q._unfinished_tasks == 2
        q.put_nowait(3)  # dropped
        assert q._unfinished_tasks == 2

    def test_stats_no_evictions_counted(self):
        """eviction='new' drops silently — no eviction stat incremented."""
        q = Queue(3, eviction="new")
        for i in range(10):
            q.put_nowait(i)
        s = q.stats()
        assert s["evictions"] == 0

    async def test_async_put_never_blocks(self):
        q = Queue(2, eviction="new")
        await q.put(1)
        await q.put(2)
        result = await q.put(3)  # should return False, not block
        assert result is False
        assert q.qsize() == 2

    def test_eviction_new_with_dedup(self):
        q = Queue(3, eviction="new", dedup=True)
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        result = q.put_nowait("d")  # full, dropped
        assert result is False
        assert q.qsize() == 3
        # dedup still works within capacity
        result = q.put_nowait("a")  # duplicate, dropped by dedup
        assert result is False
