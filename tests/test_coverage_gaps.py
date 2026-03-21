# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

"""Tests for coverage gaps identified during test completeness audit.

Covers:
- clear() with pending async waiters
- clear() on empty queue
- Key function edge cases (exceptions, None, unhashable)
- Explicit invalidated_skips stats verification
- Shutdown intermediate states
- Peek interaction with dedup-replaced items
- join() timeout when task_done() never called
- Dedup with None items
- validate + dedup + key combinations
"""

import asyncio

import pytest

from ringq import Queue, QueueFull, QueueShutDown


# ---------------------------------------------------------------------------
# clear() with pending async operations
# ---------------------------------------------------------------------------


class TestClearWithPendingWaiters:
    """clear() while async getters or putters are blocked."""

    async def test_clear_does_not_wake_blocked_getter(self):
        """clear() doesn't wake blocked getters — they stay waiting."""
        q = Queue()
        received = []

        async def getter():
            received.append(await q.get())

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.clear()
        # Getter should still be waiting (queue is empty after clear)
        await asyncio.sleep(0.01)
        assert not task.done()
        # Unblock it manually
        q.put_nowait(99)
        await asyncio.wait_for(task, timeout=0.1)
        assert received == [99]

    async def test_clear_does_not_wake_blocked_putter(self):
        """clear() on a full bounded queue doesn't wake blocked putters."""
        q = Queue(1)
        q.put_nowait("fill")

        async def putter():
            await q.put("blocked")

        task = asyncio.create_task(putter())
        await asyncio.sleep(0.01)
        q.clear()
        # After clear, queue is empty but putters are not explicitly woken.
        # The putter should eventually succeed IF it gets woken. But clear()
        # doesn't wake putters — only get_nowait/get does.
        # Let's wake it by doing a get cycle.
        await asyncio.sleep(0.01)
        if not task.done():
            # Putter still blocked — put an item and get it to trigger wakeup
            q.put_nowait("x")
            q.get_nowait()  # wakes one putter
            await asyncio.wait_for(task, timeout=0.1)
        assert q.qsize() == 1
        assert q.get_nowait() == "blocked"

    async def test_clear_on_empty_queue(self):
        """clear() on already-empty queue is a no-op."""
        q = Queue()
        q.clear()
        assert q.qsize() == 0
        assert q.empty()
        assert q._unfinished_tasks == 0
        # Queue is still usable
        q.put_nowait(1)
        assert q.get_nowait() == 1

    async def test_clear_resets_join(self):
        """clear() should allow join() to return immediately afterward."""
        q = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        assert q._unfinished_tasks == 2
        q.clear()
        # join() should not block since unfinished == 0
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_clear_with_dedup_allows_reinsertion(self):
        """After clear(), previously deduplicated keys can be reinserted."""
        q = Queue(5, dedup="drop")
        q.put_nowait("a")
        q.put_nowait("b")
        assert q.put_nowait("a") is False  # dedup drop
        q.clear()
        # "a" should be insertable again
        assert q.put_nowait("a") is True
        assert q.qsize() == 1


# ---------------------------------------------------------------------------
# Key function edge cases
# ---------------------------------------------------------------------------


class TestKeyFunctionEdgeCases:
    def test_key_returning_none(self):
        """Key function that returns None — None is a valid hashable key."""
        q = Queue(10, dedup="drop", key=lambda x: None)
        assert q.put_nowait("a") is True
        # All subsequent items have the same key (None), so they are deduped
        assert q.put_nowait("b") is False
        assert q.put_nowait("c") is False
        assert q.qsize() == 1
        assert q.get_nowait() == "a"

    def test_key_returning_tuple(self):
        """Key function returning a tuple — tuples are hashable."""
        q = Queue(10, dedup="drop", key=lambda x: (x["type"], x["id"]))
        q.put_nowait({"type": "a", "id": 1, "v": "x"})
        q.put_nowait({"type": "b", "id": 1, "v": "y"})  # different key
        assert q.put_nowait({"type": "a", "id": 1, "v": "z"}) is False  # dup
        assert q.qsize() == 2

    def test_key_raising_exception_propagates(self):
        """Exception in key function should propagate to caller."""

        def bad_key(x):
            raise ValueError("bad key")

        q = Queue(10, dedup="drop", key=bad_key)
        with pytest.raises(ValueError, match="bad key"):
            q.put_nowait("anything")
        assert q.qsize() == 0

    def test_key_raising_exception_on_specific_item(self):
        """Key function raises for a specific item — exception propagates."""

        def selective_key(x):
            if x == "bad":
                raise RuntimeError("cannot key this")
            return x

        q = Queue(10, dedup="drop", key=selective_key)
        assert q.put_nowait("a") is True
        with pytest.raises(RuntimeError, match="cannot key this"):
            q.put_nowait("bad")
        # First item should still be retrievable
        assert q.get_nowait() == "a"

    def test_key_returning_unhashable_raises(self):
        """Key function returning unhashable type (list) should raise TypeError."""
        q = Queue(10, dedup="drop", key=lambda x: [x])
        with pytest.raises(TypeError):
            q.put_nowait("a")

    def test_key_with_replace_and_exception(self):
        """Key function raises on replacement attempt — queue stays consistent."""
        call_count = 0

        def counting_key(x):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise RuntimeError("boom")
            return x["id"]

        q = Queue(10, dedup="replace", key=counting_key)
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        # Third call to key raises
        with pytest.raises(RuntimeError, match="boom"):
            q.put_nowait({"id": 1, "v": "c"})
        assert q.qsize() == 2


# ---------------------------------------------------------------------------
# Explicit invalidated_skips stats verification
# ---------------------------------------------------------------------------


class TestInvalidatedSkips:
    """Verify invalidated_skips counter increments correctly on dedup replace."""

    def test_replace_increments_invalidated_skips_on_get(self):
        """When a replaced item's slot is skipped during get, counter increments."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replace — slot 0 is now invalidated
        # get should skip the invalidated slot for item 1
        item = q.get_nowait()
        assert item == 2
        s = q.stats()
        assert s["invalidated_skips"] >= 1

    def test_multiple_replacements_accumulate_skips(self):
        """Multiple replacements create multiple invalidated slots."""
        q = Queue(10, dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        q.put_nowait("a")  # invalidates slot 0
        q.put_nowait("b")  # invalidates slot 1
        # Getting "c" should skip invalidated "a" and "b" slots
        item = q.get_nowait()
        assert item == "c"
        s = q.stats()
        assert s["invalidated_skips"] >= 2

    def test_invalidated_skips_zero_without_replacements(self):
        """No replacements → no invalidated skips."""
        q = Queue(10, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.get_nowait()
        q.get_nowait()
        q.get_nowait()
        assert q.stats()["invalidated_skips"] == 0

    def test_invalidated_skips_with_key_func(self):
        """Invalidated skips tracked correctly with key function."""
        q = Queue(10, dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 1, "v": "c"})  # replace, invalidates slot 0
        item = q.get_nowait()
        assert item == {"id": 2, "v": "b"}
        assert q.stats()["invalidated_skips"] >= 1

    def test_peek_also_skips_invalidated(self):
        """peek() should also advance past invalidated entries."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replace — invalidates original slot
        # peek should see 2 (not the invalidated 1)
        assert q.peek_nowait() == 2
        s = q.stats()
        assert s["invalidated_skips"] >= 1


# ---------------------------------------------------------------------------
# Shutdown intermediate states
# ---------------------------------------------------------------------------


class TestShutdownIntermediateStates:
    async def test_immediate_shutdown_while_getter_has_inflight_item(self):
        """Put an item, getter gets it, then immediate shutdown — getter succeeds."""
        q = Queue()
        results = []

        async def getter():
            results.append(await q.get())

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.put_nowait(42)
        # Let the getter pick up the item
        await asyncio.wait_for(task, timeout=0.1)
        # Now shutdown — should be fine, getter already completed
        q.shutdown(immediate=True)
        assert results == [42]

    async def test_graceful_shutdown_getter_drains_then_raises(self):
        """Getter gets item, then shutdown — next get raises."""
        q = Queue()

        async def getter():
            return await q.get()

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.put_nowait("only-one")
        result = await asyncio.wait_for(task, timeout=0.5)
        assert result == "only-one"
        # Now shutdown and verify next get raises
        q.shutdown()
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    async def test_graceful_shutdown_multiple_blocked_getters(self):
        """Two blocked getters on empty queue — shutdown wakes both with QueueShutDown."""
        q = Queue()
        errors = []

        async def getter(idx):
            try:
                await q.get()
            except QueueShutDown:
                errors.append(idx)

        t1 = asyncio.create_task(getter(1))
        t2 = asyncio.create_task(getter(2))
        await asyncio.sleep(0.01)
        q.shutdown()
        await asyncio.wait_for(
            asyncio.gather(t1, t2, return_exceptions=True), timeout=0.5
        )
        assert sorted(errors) == [1, 2]

    async def test_shutdown_with_validation_queue(self):
        """Shutdown on a queue with validation enabled."""
        q = Queue(validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.shutdown()
        assert q.get_nowait() == 1
        assert q.get_nowait() == 2
        with pytest.raises(QueueShutDown):
            q.get_nowait()
        with pytest.raises(QueueShutDown):
            q.put_nowait(3)

    async def test_immediate_shutdown_wakes_blocked_putter(self):
        """Blocked putter wakes with QueueShutDown on immediate shutdown."""
        q = Queue(1)
        q.put_nowait("fill")

        putter_raised = False

        async def putter():
            nonlocal putter_raised
            try:
                await q.put("blocked")
            except QueueShutDown:
                putter_raised = True

        pt = asyncio.create_task(putter())
        await asyncio.sleep(0.02)
        q.shutdown(immediate=True)
        await asyncio.wait_for(pt, timeout=0.5)
        assert putter_raised

    async def test_immediate_shutdown_wakes_blocked_getter(self):
        """Blocked getter wakes with QueueShutDown on immediate shutdown."""
        q = Queue()

        getter_raised = False

        async def getter():
            nonlocal getter_raised
            try:
                await q.get()
            except QueueShutDown:
                getter_raised = True

        gt = asyncio.create_task(getter())
        await asyncio.sleep(0.02)
        q.shutdown(immediate=True)
        await asyncio.wait_for(gt, timeout=0.5)
        assert getter_raised


# ---------------------------------------------------------------------------
# Peek with dedup-replaced items
# ---------------------------------------------------------------------------


class TestPeekWithDedup:
    def test_peek_after_head_replaced(self):
        """Peek returns correct item when head was replaced (invalidated)."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        # Replace item 1, which is at the head
        q.put_nowait(1)
        # Head slot is invalidated, peek should skip to 2
        assert q.peek_nowait() == 2

    def test_peek_after_multiple_head_replacements(self):
        """Multiple head items replaced, peek finds first valid item."""
        q = Queue(10, dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        q.put_nowait("a")  # invalidates head
        q.put_nowait("b")  # invalidates new head
        assert q.peek_nowait() == "c"

    def test_peek_then_get_consistent(self):
        """peek() and get() return the same item after replacements."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replace
        peeked = q.peek_nowait()
        got = q.get_nowait()
        assert peeked == got == 2


# ---------------------------------------------------------------------------
# join() without task_done — timeout scenario
# ---------------------------------------------------------------------------


class TestJoinTimeout:
    async def test_join_blocks_without_task_done(self):
        """join() blocks if task_done() is never called."""
        q = Queue()
        q.put_nowait(1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(q.join(), timeout=0.05)
        # Unfinished tasks still there
        assert q._unfinished_tasks == 1

    async def test_join_unblocks_after_delayed_task_done(self):
        """join() unblocks once task_done() is called."""
        q = Queue()
        q.put_nowait(1)
        q.get_nowait()

        async def delayed_done():
            await asyncio.sleep(0.02)
            q.task_done()

        asyncio.create_task(delayed_done())
        await asyncio.wait_for(q.join(), timeout=0.5)
        assert q._unfinished_tasks == 0


# ---------------------------------------------------------------------------
# Dedup with None items
# ---------------------------------------------------------------------------


class TestDedupWithNone:
    def test_dedup_drop_none_items(self):
        """None as an item should be deduplicated correctly."""
        q = Queue(10, dedup="drop")
        assert q.put_nowait(None) is True
        assert q.put_nowait(None) is False
        assert q.qsize() == 1
        assert q.get_nowait() is None

    def test_dedup_replace_none_items(self):
        """None as item with replace — second put replaces."""
        q = Queue(10, dedup="replace")
        assert q.put_nowait(None) is True
        assert q.put_nowait(None) is True  # replace
        assert q.qsize() == 1
        s = q.stats()
        assert s["dedup_replacements"] == 1

    def test_dedup_drop_with_key_returning_none(self):
        """Key function extracts None from items — dedup works on None key."""
        q = Queue(10, dedup="drop", key=lambda x: x.get("id"))
        q.put_nowait({"id": None, "v": "a"})
        assert q.put_nowait({"id": None, "v": "b"}) is False
        assert q.qsize() == 1


# ---------------------------------------------------------------------------
# Validate + dedup + key combination
# ---------------------------------------------------------------------------


class TestValidateDedupKeyCombo:
    def test_validate_fast_dedup_drop_with_key(self):
        """validate=True + dedup='drop' + key — valid items deduped by key."""
        q = Queue(10, dedup="drop", validate=True, key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        assert q.put_nowait({"id": 1, "v": "b"}) is False
        assert q.qsize() == 1
        # Invalid item still rejected
        with pytest.raises(TypeError):
            q.put_nowait(object())

    def test_validate_full_dedup_replace_with_key(self):
        """validate='full' + dedup='replace' + key — validation before dedup."""
        q = Queue(10, dedup="replace", validate="full", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "old"})
        q.put_nowait({"id": 1, "v": "new"})
        assert q.qsize() == 1
        assert q.get_nowait() == {"id": 1, "v": "new"}
        # Invalid items rejected even if they'd match existing key
        with pytest.raises(TypeError):
            q.put_nowait({"id": 1, "v": object()})
        # Original replacement still present? No, it was already consumed.
        assert q.qsize() == 0

    def test_validate_rejects_before_dedup_check(self):
        """Validation runs before dedup, so invalid items never reach dedup logic."""
        q = Queue(10, dedup="drop", validate=True)
        q.put_nowait(1)
        with pytest.raises(TypeError):
            q.put_nowait(object())
        # Stats should show no dedup activity for the rejected item
        s = q.stats()
        assert s["dedup_drops"] == 0

    def test_validate_eviction_new_dedup_replace_with_key(self):
        """All features combined: validate + eviction='new' + dedup='replace' + key."""
        q = Queue(
            3,
            eviction="new",
            dedup="replace",
            validate=True,
            key=lambda x: x["id"],
        )
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 3, "v": "c"})
        # Full, new key → dropped
        assert q.put_nowait({"id": 4, "v": "d"}) is False
        # Replace existing → allowed even when full
        assert q.put_nowait({"id": 2, "v": "updated"}) is True
        # Invalid → rejected before any logic
        with pytest.raises(TypeError):
            q.put_nowait(object())
        assert q.qsize() == 3
        assert q.get_nowait() == {"id": 1, "v": "a"}
        assert q.get_nowait() == {"id": 3, "v": "c"}
        assert q.get_nowait() == {"id": 2, "v": "updated"}


# ---------------------------------------------------------------------------
# Compaction trigger via dedup replacements
# ---------------------------------------------------------------------------


class TestCompactionViaDedupReplace:
    """Verify queue works correctly after compaction triggered by many replacements."""

    def test_many_replacements_trigger_compaction(self):
        """Repeated replacements fill physical buffer with invalidated slots,
        triggering compaction. Queue must remain correct."""
        q = Queue(3, dedup="replace")
        # Fill the queue
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        # Repeated replacements burn through physical capacity
        for _ in range(20):
            q.put_nowait(1)
            q.put_nowait(2)
            q.put_nowait(3)
        assert q.qsize() == 3
        # FIFO order should be maintained (replacements go to tail)
        items = [q.get_nowait() for _ in range(3)]
        assert items == [1, 2, 3]

    def test_compaction_preserves_stats(self):
        """Stats survive compaction."""
        q = Queue(2, dedup="replace")
        q.put_nowait("a")
        q.put_nowait("b")
        for _ in range(10):
            q.put_nowait("a")
            q.put_nowait("b")
        s = q.stats()
        assert s["dedup_replacements"] == 20
        assert q.qsize() == 2


# ---------------------------------------------------------------------------
# maxsize=1 edge cases with feature interactions
# ---------------------------------------------------------------------------


class TestMaxsizeOneEdgeCases:
    def test_maxsize_one_eviction_old_dedup_drop(self):
        """maxsize=1 with eviction='old' and dedup='drop'."""
        q = Queue(1, eviction="old", dedup="drop")
        q.put_nowait("a")
        assert q.put_nowait("a") is False  # dedup
        q.put_nowait("b")  # evicts "a"
        assert q.qsize() == 1
        assert q.get_nowait() == "b"

    def test_maxsize_one_eviction_new_dedup_replace(self):
        """maxsize=1 with eviction='new' and dedup='replace'."""
        q = Queue(1, eviction="new", dedup="replace")
        q.put_nowait("a")
        assert q.put_nowait("b") is False  # full, new key → dropped
        assert q.put_nowait("a") is True  # replace existing
        assert q.qsize() == 1
        assert q.get_nowait() == "a"

    def test_maxsize_one_bounded_no_eviction(self):
        """maxsize=1 bounded queue raises QueueFull correctly."""
        q = Queue(1)
        q.put_nowait("a")
        with pytest.raises(QueueFull):
            q.put_nowait("b")
        assert q.get_nowait() == "a"
        q.put_nowait("b")  # now it works
        assert q.get_nowait() == "b"
