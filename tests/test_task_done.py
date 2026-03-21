# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import asyncio

import pytest

from ringq import Queue


class TestTaskDone:
    def test_task_done_basic(self):
        q = Queue()
        q.put_nowait(1)
        q.get_nowait()
        q.task_done()
        assert q._unfinished_tasks == 0

    def test_task_done_overcall(self):
        q = Queue()
        with pytest.raises(ValueError, match="task_done.*too many"):
            q.task_done()

    def test_task_done_overcall_after_done(self):
        q = Queue()
        q.put_nowait(1)
        q.get_nowait()
        q.task_done()
        with pytest.raises(ValueError, match="task_done.*too many"):
            q.task_done()


class TestJoin:
    async def test_join_empty(self):
        q = Queue()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_waits(self):
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        join_done = False

        async def joiner():
            nonlocal join_done
            await q.join()
            join_done = True

        task = asyncio.create_task(joiner())
        await asyncio.sleep(0.01)
        assert not join_done

        q.get_nowait()
        q.task_done()
        await asyncio.sleep(0.01)
        assert not join_done

        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(task, timeout=0.1)
        assert join_done

    async def test_join_with_eviction(self):
        q = Queue(2, eviction=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)  # evicts 1
        # unfinished = 3 puts - 1 eviction = 2 (net items in queue)

        q.get_nowait()
        q.task_done()
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)
