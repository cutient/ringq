# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

from cpython.mem cimport PyMem_Malloc, PyMem_Free

cdef class RingBuffer:
    cdef list _items
    cdef char* _valid
    cdef Py_ssize_t _head
    cdef Py_ssize_t _size
    cdef public Py_ssize_t _live
    cdef Py_ssize_t _capacity
    cdef Py_ssize_t _mask
    cdef Py_ssize_t _maxsize
    cdef int _eviction_mode
    cdef bint _has_dedup
    cdef int _dedup_mode
    cdef object _key_func
    cdef dict _keys
    cdef public Py_ssize_t _unfinished
    cdef Py_ssize_t _evictions
    cdef Py_ssize_t _dedup_drops
    cdef Py_ssize_t _dedup_replacements
    cdef Py_ssize_t _invalidated_skips

    cpdef int put(self, object item) except -1
    cpdef object get(self)
    cpdef object peek(self)
    cpdef Py_ssize_t qsize(self)
    cpdef void clear(self) except *
    cpdef dict get_stats(self)

    cdef object _dequeue(self)
    cdef void _skip_invalidated_head(self)
    cdef void _evict_one(self)
    cdef void _compact_or_grow(self)
    cdef object _extract_key(self, object item)
