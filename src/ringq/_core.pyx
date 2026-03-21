# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

# cython: language_level=3, boundscheck=False, wraparound=False, initializedcheck=False, nonecheck=False, cdivision=True
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.string cimport memset, memcpy

from ringq._exceptions import QueueEmpty, QueueFull

# Bit flags for put() return value
DEF FLAG_INSERTED = 1
DEF FLAG_EVICTED = 2
DEF FLAG_REPLACED = 4

# Eviction modes
DEF EVICT_NONE = 0
DEF EVICT_OLD = 1
DEF EVICT_NEW = 2


cdef inline Py_ssize_t _next_power_of_2(Py_ssize_t n):
    """Round up to the next power of 2 (or n itself if already a power of 2)."""
    if n <= 1:
        return 1
    n -= 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    n |= n >> 32
    return n + 1


cdef class RingBuffer:
    """Ring buffer core for the async queue.

    Handles storage, dedup, eviction, and compaction.
    No async, no Futures — pure synchronous data structure.
    """

    def __cinit__(self, Py_ssize_t maxsize, int eviction_mode,
                  int dedup_mode, object key_func):
        cdef Py_ssize_t cap

        self._maxsize = maxsize
        self._eviction_mode = eviction_mode
        self._dedup_mode = dedup_mode
        self._has_dedup = dedup_mode != 0
        self._key_func = key_func

        if maxsize > 0:
            # Extra physical capacity for dedup to reduce compaction frequency
            cap = maxsize * 2 if self._has_dedup else maxsize
        else:
            cap = 16

        cap = _next_power_of_2(cap)
        self._capacity = cap
        self._mask = cap - 1
        self._items = [None] * cap
        self._head = 0
        self._size = 0
        self._live = 0

        self._valid = NULL
        if self._has_dedup:
            self._valid = <char*>PyMem_Malloc(cap)
            if self._valid == NULL:
                raise MemoryError()
            memset(self._valid, 0, cap)
            self._keys = {}

        self._unfinished = 0
        self._evictions = 0
        self._dedup_drops = 0
        self._dedup_replacements = 0
        self._invalidated_skips = 0

    def __dealloc__(self):
        if self._valid != NULL:
            PyMem_Free(self._valid)
            self._valid = NULL

    cpdef int put(self, object item) except -1:
        """Insert an item into the buffer.

        Returns bit flags:
          bit 0 (1): item was inserted
          bit 1 (2): an item was evicted
          bit 2 (4): an item was replaced (dedup invalidation)
        """
        cdef int flags = 0
        cdef object key
        cdef Py_ssize_t old_idx, tail

        # --- Fast path for common case (no dedup, unbounded or not full) ---
        if not self._has_dedup:
            if self._maxsize > 0 and self._size >= self._maxsize:
                if self._eviction_mode == EVICT_OLD:
                    self._evict_one()
                    flags = FLAG_EVICTED
                elif self._eviction_mode == EVICT_NEW:
                    return 0
                else:
                    raise QueueFull("queue is full")

            if self._size >= self._capacity:
                self._compact_or_grow()

            tail = (self._head + self._size) & self._mask
            self._items[tail] = item
            self._size += 1
            self._live += 1
            # +1 for insert, -1 if eviction occurred
            if flags == 0:
                self._unfinished += 1
            return flags | FLAG_INSERTED

        # --- Dedup path ---
        key = self._extract_key(item)
        if key in self._keys:
            if self._dedup_mode == 1:  # drop
                self._dedup_drops += 1
                return 0
            else:  # replace
                old_idx = self._keys[key]
                self._items[old_idx] = None
                self._valid[old_idx] = 0
                self._live -= 1
                self._dedup_replacements += 1
                flags |= FLAG_REPLACED

        # --- Fullness check ---
        if self._maxsize > 0 and self._live >= self._maxsize:
            if self._eviction_mode == EVICT_OLD:
                self._evict_one()
                flags |= FLAG_EVICTED
            elif self._eviction_mode == EVICT_NEW:
                return 0
            else:
                raise QueueFull("queue is full")

        # --- Physical capacity check ---
        if self._size >= self._capacity:
            self._compact_or_grow()

        # --- Inline enqueue + key store (avoid cdef call + redundant mask) ---
        tail = (self._head + self._size) & self._mask
        self._items[tail] = item
        self._valid[tail] = 1
        self._size += 1
        self._live += 1
        self._keys[key] = tail
        flags |= FLAG_INSERTED

        # Update unfinished: +1 for insert, -1 for evict, -1 for replace
        self._unfinished += 1 - ((flags >> 1) & 1) - ((flags >> 2) & 1)

        return flags

    cpdef object get(self):
        """Remove and return the next live item. Raises QueueEmpty if empty."""
        cdef Py_ssize_t idx
        cdef object item

        if self._live == 0:
            raise QueueEmpty("queue is empty")

        if not self._has_dedup:
            # Fast path: no dedup, inline dequeue
            idx = self._head
            item = self._items[idx]
            self._items[idx] = None
            self._head = (idx + 1) & self._mask
            self._size -= 1
            self._live -= 1
            return item

        self._skip_invalidated_head()
        return self._dequeue()

    cpdef object peek(self):
        """Return the next live item without removing it. Raises QueueEmpty if empty."""
        if self._live == 0:
            raise QueueEmpty("queue is empty")

        if self._has_dedup:
            self._skip_invalidated_head()

        cdef Py_ssize_t idx = self._head & self._mask
        return self._items[idx]

    cpdef Py_ssize_t qsize(self):
        """Return the number of live items in the buffer."""
        return self._live

    cpdef void clear(self) except *:
        """Reset the buffer. Does NOT reset stat counters."""
        cdef Py_ssize_t i
        for i in range(self._capacity):
            self._items[i] = None
        self._head = 0
        self._size = 0
        self._live = 0
        self._unfinished = 0
        if self._has_dedup:
            memset(self._valid, 0, self._capacity)
            self._keys.clear()

    cpdef dict get_stats(self):
        """Return a dict of stats counters."""
        return {
            "evictions": self._evictions,
            "dedup_drops": self._dedup_drops,
            "dedup_replacements": self._dedup_replacements,
            "invalidated_skips": self._invalidated_skips,
        }

    # --- Internal methods ---

    cdef object _dequeue(self):
        """Remove and return the head item."""
        cdef Py_ssize_t idx = self._head & self._mask
        cdef object item = self._items[idx]
        self._items[idx] = None
        if self._has_dedup:
            self._valid[idx] = 0
            key = self._extract_key(item)
            self._keys.pop(key, None)
        self._head = (self._head + 1) & self._mask
        self._size -= 1
        self._live -= 1
        return item

    cdef void _skip_invalidated_head(self):
        """Advance head past invalidated (dedup-replaced) entries."""
        cdef Py_ssize_t idx
        while self._size > 0:
            idx = self._head & self._mask
            if self._valid[idx]:
                break
            self._items[idx] = None
            self._head = (self._head + 1) & self._mask
            self._size -= 1
            self._invalidated_skips += 1

    cdef void _evict_one(self):
        """Evict the oldest live entry."""
        if self._has_dedup:
            self._skip_invalidated_head()

        cdef Py_ssize_t idx = self._head & self._mask
        cdef object item = self._items[idx]
        self._items[idx] = None
        if self._has_dedup:
            self._valid[idx] = 0
            key = self._extract_key(item)
            self._keys.pop(key, None)
        self._head = (self._head + 1) & self._mask
        self._size -= 1
        self._live -= 1
        self._evictions += 1

    cdef void _compact_or_grow(self):
        """Reallocate buffer, copying only live entries. Grow if needed."""
        cdef Py_ssize_t new_cap, i, src_idx, dst
        cdef list new_items
        cdef char* new_valid = NULL

        # Determine new capacity — dedup needs headroom for invalidated slots
        if self._maxsize > 0:
            new_cap = self._maxsize * 2 if self._has_dedup else self._maxsize
        else:
            new_cap = self._capacity * 2

        # If we can't fit live items, double
        if new_cap <= self._live:
            new_cap = self._live * 2

        new_cap = _next_power_of_2(new_cap)
        new_items = [None] * new_cap

        if self._has_dedup:
            new_valid = <char*>PyMem_Malloc(new_cap)
            if new_valid == NULL:
                raise MemoryError()
            memset(new_valid, 0, new_cap)

        # Copy only live entries
        dst = 0
        for i in range(self._size):
            src_idx = (self._head + i) & self._mask
            if self._has_dedup and not self._valid[src_idx]:
                continue
            new_items[dst] = self._items[src_idx]
            if self._has_dedup:
                new_valid[dst] = 1
            dst += 1

        # Rebuild keys
        if self._has_dedup:
            self._keys.clear()
            for i in range(dst):
                key = self._extract_key(new_items[i])
                self._keys[key] = i

        # Swap
        if self._has_dedup and self._valid != NULL:
            PyMem_Free(self._valid)
        self._valid = new_valid

        self._items = new_items
        self._capacity = new_cap
        self._mask = new_cap - 1
        self._head = 0
        self._size = dst
        self._live = dst

    cdef object _extract_key(self, object item):
        """Extract the dedup key from an item."""
        if self._key_func is not None:
            return self._key_func(item)
        return item
