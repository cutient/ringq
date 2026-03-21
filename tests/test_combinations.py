# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

"""Tests for all parameter combinations and edge cases.

Covers: eviction × dedup × validate × key cross-products,
key function edge cases, dedup edge cases (peek, clear, compaction),
and shutdown/join interactions with combined features.
"""

import asyncio

import pytest

from ringq import Queue, QueueEmpty, QueueFull, QueueShutDown


# ---------------------------------------------------------------------------
# Eviction + Dedup combinations
# ---------------------------------------------------------------------------


class TestEvictionOldDedupDrop:
    """eviction='old' + dedup='drop'"""

    def test_duplicate_dropped_when_not_full(self):
        q = Queue(5, eviction="old", dedup="drop")
        q.put_nowait(1)
        assert q.put_nowait(1) is False
        assert q.qsize() == 1

    def test_evicts_oldest_when_full(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)  # evicts 1
        assert q.qsize() == 3
        assert q.get_nowait() == 2

    def test_duplicate_of_evicted_item_accepted(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)  # evicts 1
        # 1 was evicted, so it should be accepted again
        assert q.put_nowait(1) is True
        assert q.qsize() == 3  # evicted 2 to make room
        assert q.get_nowait() == 3

    def test_stats_combined(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # dedup drop
        q.put_nowait(4)  # evicts 1
        s = q.stats()
        assert s["dedup_drops"] == 1
        assert s["evictions"] == 1

    def test_unfinished_tasks(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q._unfinished_tasks == 3
        q.put_nowait(1)  # dedup drop — no change
        assert q._unfinished_tasks == 3
        q.put_nowait(4)  # evicts 1: +1 insert, -1 evict = net 0
        assert q._unfinished_tasks == 3

    def test_never_raises_queue_full(self):
        q = Queue(3, eviction="old", dedup="drop")
        for i in range(100):
            q.put_nowait(i)
        assert q.qsize() == 3


class TestEvictionOldDedupReplace:
    """eviction='old' + dedup='replace'"""

    def test_replace_within_capacity(self):
        q = Queue(5, eviction="old", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replaces first 1
        assert q.qsize() == 2
        assert q.get_nowait() == 2
        assert q.get_nowait() == 1

    def test_replace_with_key(self):
        q = Queue(5, eviction="old", dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 1, "v": "c"})  # replaces
        assert q.qsize() == 2
        assert q.get_nowait() == {"id": 2, "v": "b"}
        assert q.get_nowait() == {"id": 1, "v": "c"}

    def test_evicts_then_replaces(self):
        q = Queue(3, eviction="old", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)  # evicts 1
        q.put_nowait(2)  # replaces old 2
        assert q.qsize() == 3
        assert q.get_nowait() == 3
        assert q.get_nowait() == 4
        assert q.get_nowait() == 2

    def test_stats_combined(self):
        q = Queue(3, eviction="old", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)  # evicts 1
        q.put_nowait(2)  # replaces
        s = q.stats()
        assert s["evictions"] == 1
        assert s["dedup_replacements"] == 1

    def test_unfinished_tasks(self):
        q = Queue(3, eviction="old", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q._unfinished_tasks == 3
        q.put_nowait(2)  # replace: +1 -1 = net 0
        assert q._unfinished_tasks == 3
        q.put_nowait(4)  # evict: +1 -1 = net 0
        assert q._unfinished_tasks == 3


class TestEvictionNewDedupDrop:
    """eviction='new' + dedup='drop'"""

    def test_drops_new_when_full(self):
        q = Queue(3, eviction="new", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.put_nowait(4) is False  # full, dropped
        assert q.qsize() == 3
        assert q.get_nowait() == 1

    def test_dedup_still_works_within_capacity(self):
        q = Queue(3, eviction="new", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        assert q.put_nowait(1) is False  # dedup drop
        assert q.qsize() == 2

    def test_dedup_drop_vs_full_drop(self):
        """Dedup drop should apply even when not full."""
        q = Queue(5, eviction="new", dedup="drop")
        q.put_nowait("a")
        q.put_nowait("b")
        assert q.put_nowait("a") is False  # dedup
        assert q.qsize() == 2
        s = q.stats()
        assert s["dedup_drops"] == 1
        assert s["evictions"] == 0

    def test_stats_no_evictions(self):
        q = Queue(3, eviction="new", dedup="drop")
        for i in range(10):
            q.put_nowait(i)
        s = q.stats()
        assert s["evictions"] == 0

    def test_with_key(self):
        q = Queue(3, eviction="new", dedup="drop", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        assert q.put_nowait({"id": 1, "v": "b"}) is False
        assert q.qsize() == 1
        assert q.get_nowait() == {"id": 1, "v": "a"}


class TestEvictionNewDedupReplace:
    """eviction='new' + dedup='replace'"""

    def test_replace_within_capacity(self):
        q = Queue(5, eviction="new", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replaces
        assert q.qsize() == 2

    def test_drops_new_when_full_no_dedup_match(self):
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        result = q.put_nowait(4)  # full, no dedup match → dropped
        assert result is False
        assert q.qsize() == 3

    def test_replace_when_full_allowed(self):
        """Replace of existing key should work even when at capacity."""
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        # Replacing existing key — live count stays the same
        result = q.put_nowait(1)
        assert result is True
        assert q.qsize() == 3
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3
        assert q.get_nowait() == 1

    def test_replace_with_key_when_full(self):
        q = Queue(3, eviction="new", dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 3, "v": "c"})
        # Replace existing — should succeed
        result = q.put_nowait({"id": 2, "v": "updated"})
        assert result is True
        assert q.qsize() == 3
        assert q.get_nowait() == {"id": 1, "v": "a"}
        assert q.get_nowait() == {"id": 3, "v": "c"}
        assert q.get_nowait() == {"id": 2, "v": "updated"}

    def test_stats(self):
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # replace
        q.put_nowait(4)  # dropped (new)
        s = q.stats()
        assert s["dedup_replacements"] == 1
        assert s["evictions"] == 0


# ---------------------------------------------------------------------------
# Eviction + Validate combinations
# ---------------------------------------------------------------------------


class TestEvictionOldValidateFast:
    """eviction='old' + validate=True (fast)"""

    def test_valid_item_evicts(self):
        q = Queue(3, eviction="old", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(4)  # valid, evicts 1
        assert q.qsize() == 3
        assert q.get_nowait() == 2

    def test_invalid_item_rejected_before_eviction(self):
        q = Queue(3, eviction="old", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        with pytest.raises(TypeError, match="not JSON-compatible"):
            q.put_nowait(object())
        # Nothing was evicted
        assert q.qsize() == 3
        assert q.get_nowait() == 1

    def test_nan_accepted(self):
        q = Queue(3, eviction="old", validate=True)
        q.put_nowait(float("nan"))
        assert q.qsize() == 1


class TestEvictionOldValidateFull:
    """eviction='old' + validate='full'"""

    def test_valid_item_evicts(self):
        q = Queue(3, eviction="old", validate="full")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        q.put_nowait("d")
        assert q.qsize() == 3
        assert q.get_nowait() == "b"

    def test_invalid_item_rejected(self):
        q = Queue(3, eviction="old", validate="full")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        with pytest.raises(TypeError, match="not JSON-serializable"):
            q.put_nowait(object())
        assert q.qsize() == 3


class TestEvictionNewValidate:
    """eviction='new' + validate"""

    def test_valid_item_dropped_when_full(self):
        q = Queue(3, eviction="new", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.put_nowait(4) is False
        assert q.qsize() == 3

    def test_invalid_item_rejected_even_when_not_full(self):
        q = Queue(3, eviction="new", validate=True)
        q.put_nowait(1)
        with pytest.raises(TypeError, match="not JSON-compatible"):
            q.put_nowait(object())
        assert q.qsize() == 1

    def test_invalid_item_rejected_before_full_check(self):
        """Validation runs before eviction logic — invalid items raise TypeError."""
        q = Queue(3, eviction="new", validate="full")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        with pytest.raises(TypeError, match="not JSON-serializable"):
            q.put_nowait({1, 2})
        assert q.qsize() == 3


# ---------------------------------------------------------------------------
# Dedup + Validate combinations
# ---------------------------------------------------------------------------


class TestDedupDropValidate:
    """dedup='drop' + validate"""

    def test_valid_duplicate_dropped(self):
        q = Queue(10, dedup="drop", validate=True)
        q.put_nowait("hello")
        assert q.put_nowait("hello") is False
        assert q.qsize() == 1

    def test_invalid_item_rejected_before_dedup(self):
        q = Queue(10, dedup="drop", validate=True)
        with pytest.raises(TypeError, match="not JSON-compatible"):
            q.put_nowait(object())
        assert q.qsize() == 0

    def test_valid_items_dedup_then_get(self):
        q = Queue(10, dedup="drop", validate="full")
        q.put_nowait("hello")
        q.put_nowait("hello")
        assert q.qsize() == 1
        assert q.get_nowait() == "hello"

    def test_stats(self):
        q = Queue(10, dedup="drop", validate=True)
        q.put_nowait(1)
        q.put_nowait(1)
        with pytest.raises(TypeError):
            q.put_nowait(object())
        s = q.stats()
        assert s["dedup_drops"] == 1


class TestDedupReplaceValidate:
    """dedup='replace' + validate"""

    def test_valid_duplicate_replaced(self):
        q = Queue(10, dedup="replace", validate=True, key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "old"})
        q.put_nowait({"id": 1, "v": "new"})
        assert q.qsize() == 1
        assert q.get_nowait() == {"id": 1, "v": "new"}

    def test_invalid_replacement_rejected(self):
        q = Queue(10, dedup="replace", validate=True, key=lambda x: x)
        q.put_nowait("hello")
        with pytest.raises(TypeError, match="not JSON-compatible"):
            q.put_nowait(object())
        assert q.qsize() == 1
        assert q.get_nowait() == "hello"


# ---------------------------------------------------------------------------
# Triple combination: eviction + dedup + validate
# ---------------------------------------------------------------------------


class TestTripleCombination:
    """eviction + dedup + validate all enabled"""

    def test_old_drop_fast(self):
        q = Queue(3, eviction="old", dedup="drop", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # dedup drop
        q.put_nowait(4)  # evicts 1
        with pytest.raises(TypeError):
            q.put_nowait(object())
        assert q.qsize() == 3
        s = q.stats()
        assert s["dedup_drops"] == 1
        assert s["evictions"] == 1

    def test_old_replace_fast(self):
        q = Queue(3, eviction="old", dedup="replace", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(2)  # replace
        assert q.qsize() == 3
        with pytest.raises(TypeError):
            q.put_nowait(b"bad")
        s = q.stats()
        assert s["dedup_replacements"] == 1

    def test_new_drop_fast(self):
        q = Queue(3, eviction="new", dedup="drop", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.put_nowait(4) is False  # full, dropped
        assert q.put_nowait(1) is False  # dedup drop
        with pytest.raises(TypeError):
            q.put_nowait({1, 2})
        assert q.qsize() == 3

    def test_new_replace_fast(self):
        q = Queue(3, eviction="new", dedup="replace", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.put_nowait(4) is False  # full, new key → dropped
        assert q.put_nowait(2) is True  # replace existing
        with pytest.raises(TypeError):
            q.put_nowait(object())
        assert q.qsize() == 3

    def test_old_drop_full_validate(self):
        q = Queue(3, eviction="old", dedup="drop", validate="full")
        q.put_nowait("a")
        q.put_nowait("b")
        q.put_nowait("c")
        q.put_nowait("d")  # evicts "a"
        assert q.put_nowait("b") is False  # dedup
        with pytest.raises(TypeError, match="not JSON-serializable"):
            q.put_nowait(object())
        assert q.qsize() == 3

    def test_old_replace_full_validate_with_key(self):
        q = Queue(
            3, eviction="old", dedup="replace", validate="full", key=lambda x: x["id"]
        )
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 3, "v": "c"})
        q.put_nowait({"id": 4, "v": "d"})  # evicts id=1
        q.put_nowait({"id": 2, "v": "updated"})  # replaces
        with pytest.raises(TypeError):
            q.put_nowait(object())
        assert q.qsize() == 3
        s = q.stats()
        assert s["evictions"] == 1
        assert s["dedup_replacements"] == 1


# ---------------------------------------------------------------------------
# Key function edge cases
# ---------------------------------------------------------------------------


class TestKeyFunctionEdgeCases:
    def test_key_returns_none(self):
        """None is a valid key."""
        q = Queue(10, dedup="drop", key=lambda x: None)
        assert q.put_nowait("a") is True
        assert q.put_nowait("b") is False  # all map to None
        assert q.qsize() == 1
        assert q.get_nowait() == "a"

    def test_key_returns_tuple(self):
        """Tuple keys are hashable and should work."""
        q = Queue(10, dedup="drop", key=lambda x: (x["type"], x["id"]))
        q.put_nowait({"type": "a", "id": 1})
        q.put_nowait({"type": "b", "id": 1})  # different key
        assert q.put_nowait({"type": "a", "id": 1}) is False  # same key
        assert q.qsize() == 2

    def test_key_returns_empty_string(self):
        q = Queue(10, dedup="drop", key=lambda x: "")
        q.put_nowait(1)
        assert q.put_nowait(2) is False
        assert q.qsize() == 1

    def test_key_func_with_replace(self):
        q = Queue(10, dedup="replace", key=lambda x: x % 10)
        q.put_nowait(11)
        q.put_nowait(21)  # same key (1), replaces
        assert q.qsize() == 1
        assert q.get_nowait() == 21

    def test_key_func_exception_propagates(self):
        """If key function raises, the exception should propagate."""

        def bad_key(x):
            raise ValueError("bad key")

        q = Queue(10, dedup="drop", key=bad_key)
        with pytest.raises(ValueError, match="bad key"):
            q.put_nowait("anything")
        assert q.qsize() == 0

    def test_key_func_called_on_each_put(self):
        calls = []

        def tracking_key(x):
            calls.append(x)
            return x

        q = Queue(10, dedup="drop", key=tracking_key)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # duplicate
        assert calls == [1, 2, 1]

    def test_key_func_with_eviction_old(self):
        q = Queue(3, eviction="old", dedup="drop", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "data": "a"})
        q.put_nowait({"id": 2, "data": "b"})
        q.put_nowait({"id": 3, "data": "c"})
        # Duplicate
        assert q.put_nowait({"id": 2, "data": "dup"}) is False
        # New item, evicts id=1
        q.put_nowait({"id": 4, "data": "d"})
        assert q.qsize() == 3
        assert q.get_nowait() == {"id": 2, "data": "b"}

    def test_key_func_with_eviction_new(self):
        q = Queue(3, eviction="new", dedup="drop", key=lambda x: x["id"])
        q.put_nowait({"id": 1})
        q.put_nowait({"id": 2})
        q.put_nowait({"id": 3})
        assert q.put_nowait({"id": 4}) is False  # full, dropped
        assert q.put_nowait({"id": 1}) is False  # dedup
        assert q.qsize() == 3

    def test_key_func_with_replace_and_eviction(self):
        q = Queue(3, eviction="old", dedup="replace", key=lambda x: x["id"])
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.put_nowait({"id": 3, "v": "c"})
        # Replace id=1 — should not evict
        q.put_nowait({"id": 1, "v": "updated"})
        assert q.qsize() == 3
        assert q.get_nowait() == {"id": 2, "v": "b"}
        assert q.get_nowait() == {"id": 3, "v": "c"}
        assert q.get_nowait() == {"id": 1, "v": "updated"}


# ---------------------------------------------------------------------------
# Dedup edge cases
# ---------------------------------------------------------------------------


class TestDedupEdgeCases:
    def test_dedup_unbounded_queue(self):
        """Dedup works with unbounded queue (maxsize=0)."""
        q = Queue(dedup="drop")
        for i in range(100):
            q.put_nowait(i)
        # All unique
        assert q.qsize() == 100
        # Now add duplicates
        for i in range(100):
            assert q.put_nowait(i) is False
        assert q.qsize() == 100

    def test_dedup_replace_unbounded(self):
        q = Queue(dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replaces
        assert q.qsize() == 2
        assert q.get_nowait() == 2
        assert q.get_nowait() == 1

    def test_peek_skips_invalidated(self):
        """Peek should skip invalidated entries from dedup replace."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # invalidates head entry for 1
        # Peek should show 2 (first live item), not invalidated 1
        assert q.peek_nowait() == 2

    def test_peek_after_multiple_replacements(self):
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)  # replace
        q.put_nowait(1)  # replace again
        assert q.qsize() == 1
        assert q.peek_nowait() == 1

    def test_clear_resets_dedup_keys(self):
        """After clear, previously deduped items can be added again."""
        q = Queue(5, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        assert q.put_nowait(1) is False  # dedup
        q.clear()
        assert q.put_nowait(1) is True  # allowed after clear
        assert q.put_nowait(2) is True
        assert q.qsize() == 2

    def test_clear_resets_replace_keys(self):
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.clear()
        q.put_nowait(1)
        assert q.qsize() == 1
        s = q.stats()
        # clear doesn't reset stats, but the second put was an insert, not a replace
        assert s["dedup_replacements"] == 0

    def test_dedup_with_none_item(self):
        q = Queue(5, dedup="drop")
        assert q.put_nowait(None) is True
        assert q.put_nowait(None) is False
        assert q.qsize() == 1
        assert q.get_nowait() is None

    def test_dedup_with_string_items(self):
        q = Queue(5, dedup="drop")
        q.put_nowait("hello")
        q.put_nowait("world")
        assert q.put_nowait("hello") is False
        assert q.qsize() == 2

    def test_dedup_drop_after_get_allows_readd(self):
        """After consuming an item, its key is removed from dedup tracking."""
        q = Queue(5, dedup="drop")
        q.put_nowait("x")
        q.get_nowait()
        assert q.put_nowait("x") is True
        assert q.qsize() == 1

    def test_replace_after_get_is_insert(self):
        """After consuming, re-adding same key is a fresh insert, not replace."""
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.get_nowait()
        q.put_nowait(1)
        assert q.qsize() == 1
        s = q.stats()
        assert s["dedup_replacements"] == 0

    def test_dedup_many_replacements_compaction(self):
        """Heavy replacement load triggers compaction; verify correctness."""
        q = Queue(5, dedup="replace")
        for cycle in range(10):
            for i in range(5):
                q.put_nowait(i)
        assert q.qsize() == 5
        # All items should still be retrievable
        items = [q.get_nowait() for _ in range(5)]
        assert sorted(items) == [0, 1, 2, 3, 4]

    def test_dedup_invalidated_skips_stat(self):
        """Invalidated skips counter tracks head skips during get."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # invalidates original 1 at head
        q.get_nowait()  # should skip invalidated, return 2
        s = q.stats()
        assert s["invalidated_skips"] >= 1

    def test_dedup_drop_string_mode(self):
        """dedup='drop' is equivalent to dedup=True."""
        q = Queue(5, dedup="drop")
        q.put_nowait(1)
        assert q.put_nowait(1) is False
        assert q.qsize() == 1


# ---------------------------------------------------------------------------
# Async with combined features
# ---------------------------------------------------------------------------


class TestAsyncCombinations:
    async def test_async_put_eviction_old_dedup_drop(self):
        q = Queue(3, eviction="old", dedup="drop")
        await q.put(1)
        await q.put(2)
        await q.put(3)
        result = await q.put(1)  # dedup drop
        assert result is False
        result = await q.put(4)  # evicts 1
        assert result is True
        assert q.qsize() == 3

    async def test_async_put_eviction_new_never_blocks(self):
        q = Queue(2, eviction="new", dedup="drop")
        await q.put(1)
        await q.put(2)
        # Should not block — drops new
        result = await asyncio.wait_for(q.put(3), timeout=0.1)
        assert result is False

    async def test_async_get_with_dedup_replace(self):
        q = Queue(10, dedup="replace")
        await q.put(1)
        await q.put(2)
        await q.put(1)  # replaces
        assert await q.get() == 2
        assert await q.get() == 1
        assert q.empty()

    async def test_async_producer_consumer_with_eviction(self):
        """With eviction, producer never blocks. Consumer gets what's available."""
        q = Queue(5, eviction="old")

        async def producer():
            for i in range(20):
                await q.put(i)

        async def consumer():
            items = []
            while len(items) < 5:
                item = await q.get()
                items.append(item)
            return items

        await producer()
        # After producing 20 into a size-5 queue with eviction, last 5 remain
        assert q.qsize() == 5
        items = [q.get_nowait() for _ in range(5)]
        assert items == [15, 16, 17, 18, 19]

    async def test_async_put_with_validation(self):
        q = Queue(validate=True)
        await q.put(42)
        await q.put("hello")
        with pytest.raises(TypeError, match="not JSON-compatible"):
            await q.put(object())

    async def test_async_eviction_old_validate_dedup(self):
        """Full triple combo with async API."""
        q = Queue(3, eviction="old", dedup="drop", validate=True)
        await q.put(1)
        await q.put(2)
        await q.put(3)
        result = await q.put(1)  # dedup
        assert result is False
        await q.put(4)  # evicts 1
        with pytest.raises(TypeError):
            await q.put(object())
        assert q.qsize() == 3


# ---------------------------------------------------------------------------
# Shutdown + join with combined features
# ---------------------------------------------------------------------------


class TestShutdownCombined:
    async def test_shutdown_eviction_old_dedup_drop(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.shutdown()
        # Can still drain
        assert q.get_nowait() == 1
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    async def test_shutdown_eviction_new_dedup_replace(self):
        q = Queue(3, eviction="new", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.shutdown()
        with pytest.raises(QueueShutDown):
            q.put_nowait(4)
        assert q.get_nowait() == 1

    async def test_immediate_shutdown_with_dedup(self):
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replace
        q.shutdown(immediate=True)
        assert q.qsize() == 0
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    async def test_immediate_shutdown_with_eviction_and_validation(self):
        q = Queue(3, eviction="old", validate=True)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.shutdown(immediate=True)
        assert q.qsize() == 0

    async def test_shutdown_with_all_features(self):
        q = Queue(
            3, eviction="old", dedup="replace", validate=True, key=lambda x: x["id"]
        )
        q.put_nowait({"id": 1, "v": "a"})
        q.put_nowait({"id": 2, "v": "b"})
        q.shutdown()
        assert q.get_nowait() == {"id": 1, "v": "a"}
        assert q.get_nowait() == {"id": 2, "v": "b"}
        with pytest.raises(QueueShutDown):
            q.get_nowait()


class TestJoinCombined:
    async def test_join_with_eviction(self):
        """Eviction decrements unfinished; join should still work."""
        q = Queue(2, eviction="old")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)  # evicts 1, unfinished stays 2

        q.get_nowait()
        q.task_done()
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_with_dedup_drop(self):
        """Dedup drop doesn't increment unfinished."""
        q = Queue(5, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(1)  # dropped, unfinished = 1
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_with_dedup_replace(self):
        """Dedup replace nets zero on unfinished."""
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)  # replace, unfinished stays 1
        assert q._unfinished_tasks == 1
        q.get_nowait()
        q.task_done()
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_after_clear_with_features(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.clear()
        # Join should return immediately — unfinished is 0
        await asyncio.wait_for(q.join(), timeout=0.1)

    async def test_join_with_eviction_and_dedup(self):
        """Combined eviction + dedup: unfinished accounting must be correct."""
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # dedup drop, unfinished stays 3
        q.put_nowait(4)  # evicts 1, unfinished stays 3

        for _ in range(3):
            q.get_nowait()
            q.task_done()

        await asyncio.wait_for(q.join(), timeout=0.1)


# ---------------------------------------------------------------------------
# Constructor validation (comprehensive)
# ---------------------------------------------------------------------------


class TestConstructorEdgeCases:
    def test_eviction_old_requires_maxsize(self):
        with pytest.raises(ValueError, match="eviction requires maxsize"):
            Queue(eviction="old")

    def test_eviction_new_requires_maxsize(self):
        with pytest.raises(ValueError, match="eviction requires maxsize"):
            Queue(eviction="new")

    def test_eviction_none_is_no_eviction(self):
        q = Queue(3, eviction=None)  # type: ignore
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        with pytest.raises(QueueFull):
            q.put_nowait(4)

    def test_eviction_false_is_no_eviction(self):
        q = Queue(3, eviction=False)
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        with pytest.raises(QueueFull):
            q.put_nowait(4)

    def test_dedup_none_is_no_dedup(self):
        q = Queue(5, dedup=None)  # type: ignore
        q.put_nowait(1)
        q.put_nowait(1)
        assert q.qsize() == 2

    def test_dedup_false_is_no_dedup(self):
        q = Queue(5, dedup=False)
        q.put_nowait(1)
        q.put_nowait(1)
        assert q.qsize() == 2

    def test_validate_none_is_no_validate(self):
        q = Queue(validate=None)  # type: ignore
        q.put_nowait(object())  # no error

    def test_validate_false_is_no_validate(self):
        q = Queue(validate=False)
        q.put_nowait(object())  # no error

    def test_key_with_dedup_drop(self):
        q = Queue(5, dedup="drop", key=lambda x: x)
        q.put_nowait(1)
        assert q.put_nowait(1) is False

    def test_key_with_dedup_replace(self):
        q = Queue(5, dedup="replace", key=lambda x: x)
        q.put_nowait(1)
        q.put_nowait(1)
        assert q.qsize() == 1

    def test_negative_maxsize_is_unbounded(self):
        q = Queue(-1)
        for i in range(100):
            q.put_nowait(i)
        assert q.qsize() == 100
        assert q.full() is False

    def test_maxsize_zero_is_unbounded(self):
        q = Queue(0)
        assert q.full() is False

    def test_eviction_with_negative_maxsize(self):
        with pytest.raises(ValueError, match="eviction requires maxsize"):
            Queue(-1, eviction="old")

    def test_all_features_constructor(self):
        """All features enabled at once — constructor should not raise."""
        q = Queue(5, eviction="old", dedup="replace", validate=True, key=lambda x: x)
        assert q.maxsize == 5


# ---------------------------------------------------------------------------
# Stats accuracy under complex scenarios
# ---------------------------------------------------------------------------


class TestStatsComplex:
    def test_stats_after_eviction_and_dedup(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # dedup drop
        q.put_nowait(2)  # dedup drop
        q.put_nowait(4)  # evicts 1
        q.put_nowait(5)  # evicts 2
        s = q.stats()
        assert s["dedup_drops"] == 2
        assert s["evictions"] == 2
        assert s["dedup_replacements"] == 0
        assert s["maxsize"] == 3

    def test_stats_after_replace_and_eviction(self):
        q = Queue(3, eviction="old", dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # replace
        q.put_nowait(4)  # evicts 2 (1 was moved to tail)
        s = q.stats()
        assert s["dedup_replacements"] == 1
        assert s["evictions"] == 1

    def test_stats_preserved_after_clear(self):
        q = Queue(3, eviction="old", dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        q.put_nowait(1)  # dedup
        q.put_nowait(4)  # evict
        q.clear()
        s = q.stats()
        assert s["dedup_drops"] == 1
        assert s["evictions"] == 1

    def test_stats_initial_all_features(self):
        q = Queue(5, eviction="old", dedup="replace", validate=True)
        s = q.stats()
        assert s["evictions"] == 0
        assert s["dedup_drops"] == 0
        assert s["dedup_replacements"] == 0
        assert s["invalidated_skips"] == 0
        assert s["maxsize"] == 5


# ---------------------------------------------------------------------------
# Peek edge cases with dedup
# ---------------------------------------------------------------------------


class TestPeekDedup:
    def test_peek_with_dedup_drop(self):
        q = Queue(10, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        assert q.peek_nowait() == 1

    def test_peek_after_replacement_at_head(self):
        """If head item is replaced, peek should skip invalidated head."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # invalidates 1 at head, new 1 at tail
        assert q.peek_nowait() == 2

    def test_peek_empty_with_dedup(self):
        q = Queue(5, dedup="drop")
        with pytest.raises(QueueEmpty):
            q.peek_nowait()

    def test_peek_after_all_replaced(self):
        """All original items replaced — peek still works."""
        q = Queue(10, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)  # replace
        q.put_nowait(1)  # replace again
        assert q.peek_nowait() == 1
        assert q.qsize() == 1


# ---------------------------------------------------------------------------
# Full/empty state edge cases with features
# ---------------------------------------------------------------------------


class TestFullEmptyWithFeatures:
    def test_full_with_dedup_counts_live(self):
        """full() should check live count, not physical size."""
        q = Queue(3, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.full() is True
        q.put_nowait(1)  # replace — still 3 live
        assert q.full() is True

    def test_not_full_after_dedup_get(self):
        q = Queue(3, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)
        assert q.full() is True
        q.get_nowait()
        assert q.full() is False

    def test_empty_after_eviction_and_drain(self):
        q = Queue(3, eviction="old")
        for i in range(10):
            q.put_nowait(i)
        for _ in range(3):
            q.get_nowait()
        assert q.empty() is True

    def test_empty_with_dedup_replace_all_consumed(self):
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(1)  # replace
        q.get_nowait()
        assert q.empty() is True

    def test_len_with_dedup(self):
        q = Queue(5, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # dropped
        assert len(q) == 2

    def test_len_with_eviction(self):
        q = Queue(3, eviction="old")
        for i in range(5):
            q.put_nowait(i)
        assert len(q) == 3


# ---------------------------------------------------------------------------
# Stress tests for combined features
# ---------------------------------------------------------------------------


class TestStressCombined:
    def test_high_volume_eviction_dedup_drop(self):
        q = Queue(100, eviction="old", dedup="drop")
        for i in range(10000):
            q.put_nowait(i % 500)  # lots of duplicates and evictions
        assert q.qsize() <= 100

    def test_high_volume_eviction_dedup_replace(self):
        q = Queue(50, eviction="old", dedup="replace")
        for i in range(5000):
            q.put_nowait(i % 200)
        assert q.qsize() <= 50
        # Drain all
        items = []
        while not q.empty():
            items.append(q.get_nowait())
        assert len(items) == q.stats()["maxsize"] or len(items) <= 50

    def test_high_volume_replace_with_key(self):
        q = Queue(50, dedup="replace", key=lambda x: x % 50)
        for i in range(5000):
            q.put_nowait(i)
        assert q.qsize() == 50
        # All items should be the last 50 values with those keys
        items = []
        while not q.empty():
            items.append(q.get_nowait())
        for item in items:
            assert item >= 4950

    def test_high_volume_eviction_new_dedup(self):
        q = Queue(100, eviction="new", dedup="drop")
        for i in range(10000):
            q.put_nowait(i % 200)
        assert q.qsize() <= 100

    async def test_async_high_volume_combined(self):
        q = Queue(50, eviction="old", dedup="drop", validate=True)
        for i in range(1000):
            await q.put(i)
        assert q.qsize() == 50
        s = q.stats()
        assert s["evictions"] == 950


# ---------------------------------------------------------------------------
# task_done edge cases with features
# ---------------------------------------------------------------------------


class TestTaskDoneCombined:
    def test_task_done_with_eviction_accounting(self):
        """After eviction, unfinished should be correct for task_done."""
        q = Queue(2, eviction="old")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)  # evicts 1; unfinished = 2
        q.get_nowait()
        q.task_done()
        q.get_nowait()
        q.task_done()
        assert q._unfinished_tasks == 0

    def test_task_done_overcall_with_dedup(self):
        q = Queue(5, dedup="drop")
        q.put_nowait(1)
        q.put_nowait(1)  # dropped
        q.get_nowait()
        q.task_done()
        with pytest.raises(ValueError, match="task_done.*too many"):
            q.task_done()

    def test_task_done_with_replace(self):
        q = Queue(5, dedup="replace")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(1)  # replace: unfinished stays 2
        q.get_nowait()
        q.task_done()
        q.get_nowait()
        q.task_done()
        assert q._unfinished_tasks == 0

    def test_task_done_overcall_after_eviction(self):
        q = Queue(2, eviction="old")
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)  # evicts 1
        # unfinished = 2
        q.get_nowait()
        q.task_done()
        q.get_nowait()
        q.task_done()
        with pytest.raises(ValueError, match="task_done.*too many"):
            q.task_done()
