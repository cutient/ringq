# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import pytest

from ringq import Queue, QueueFull


class TestBounded:
    def test_raises_queue_full(self, bounded_queue):
        for i in range(5):
            bounded_queue.put_nowait(i)
        with pytest.raises(QueueFull):
            bounded_queue.put_nowait(99)

    def test_maxsize_property(self, bounded_queue):
        assert bounded_queue.maxsize == 5

    def test_maxsize_zero_unbounded(self, queue):
        assert queue.maxsize == 0

    def test_put_after_get(self, bounded_queue):
        for i in range(5):
            bounded_queue.put_nowait(i)
        bounded_queue.get_nowait()
        bounded_queue.put_nowait(99)  # should not raise
        assert bounded_queue.qsize() == 5

    def test_wrap_around(self):
        q = Queue(3)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.get_nowait() == 1
        q.put_nowait(4)  # wraps around
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3
        assert q.get_nowait() == 4
