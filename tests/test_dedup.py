# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

from ringq import Queue


class TestDedupDropNew:
    def test_drop_duplicate(self, dedup_queue):
        assert dedup_queue.put_nowait(1) is True
        assert dedup_queue.put_nowait(1) is False
        assert dedup_queue.qsize() == 1

    def test_drop_stats(self, dedup_queue):
        dedup_queue.put_nowait(1)
        dedup_queue.put_nowait(1)
        dedup_queue.put_nowait(1)
        s = dedup_queue.stats()
        assert s["dedup_drops"] == 2

    def test_dedup_with_key_func(self):
        q = Queue(10, dedup=True, key=lambda x: x["id"])
        q.put_nowait({"id": 1, "val": "a"})
        assert q.put_nowait({"id": 1, "val": "b"}) is False
        assert q.qsize() == 1
        assert q.get_nowait() == {"id": 1, "val": "a"}

    def test_dedup_different_items(self, dedup_queue):
        dedup_queue.put_nowait(1)
        dedup_queue.put_nowait(2)
        dedup_queue.put_nowait(3)
        assert dedup_queue.qsize() == 3

    def test_dedup_after_get(self, dedup_queue):
        dedup_queue.put_nowait(1)
        dedup_queue.get_nowait()
        # Should be able to add 1 again after it's been consumed
        assert dedup_queue.put_nowait(1) is True
        assert dedup_queue.qsize() == 1

    def test_unfinished_tasks_drop(self):
        q = Queue(5, dedup=True)
        q.put_nowait(1)
        q.put_nowait(1)  # dropped
        assert q._unfinished_tasks == 1


class TestDedupReplace:
    def test_replace_duplicate(self, replace_queue):
        replace_queue.put_nowait(1)
        replace_queue.put_nowait(1)
        assert replace_queue.qsize() == 1

    def test_replace_returns_new_value(self):
        q = Queue(10, dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "val": "old"})
        q.put_nowait({"id": 1, "val": "new"})
        assert q.qsize() == 1
        assert q.get_nowait() == {"id": 1, "val": "new"}

    def test_replace_stats(self):
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)
        q.put_nowait(1)
        s = q.stats()
        assert s["dedup_replacements"] == 2

    def test_replace_ordering(self):
        q = Queue(10, dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "val": "a"})
        q.put_nowait({"id": 2, "val": "b"})
        q.put_nowait({"id": 1, "val": "c"})  # replaces first
        # Get order: id=2 (original position maintained for non-replaced),
        # id=1 replacement goes to new tail position
        item1 = q.get_nowait()
        item2 = q.get_nowait()
        assert item1 == {"id": 2, "val": "b"}
        assert item2 == {"id": 1, "val": "c"}

    def test_unfinished_tasks_replace(self):
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)  # replace: +1 insert, -1 replace = 0 delta
        assert q._unfinished_tasks == 1
