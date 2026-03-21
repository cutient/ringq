# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

# cython: language_level=3, boundscheck=False, wraparound=False, initializedcheck=False, nonecheck=False, cdivision=True
from cpython.ref cimport PyObject
from cpython.dict cimport PyDict_CheckExact, PyDict_Next
from cpython.list cimport PyList_CheckExact, PyList_GET_SIZE, PyList_GET_ITEM
from cpython.tuple cimport PyTuple_CheckExact, PyTuple_GET_SIZE, PyTuple_GET_ITEM
from cpython.unicode cimport PyUnicode_CheckExact
from cpython.bool cimport PyBool_Check
from cpython.long cimport PyLong_CheckExact
from cpython.float cimport PyFloat_CheckExact, PyFloat_AS_DOUBLE
from libc.math cimport isnan, isinf


cdef inline bint _is_primitive_exact(object x, bint allow_nan_inf) except -1:
    cdef double v

    if x is None:
        return True

    # bool check first because bool subclasses int
    if PyBool_Check(x):
        return True

    if PyLong_CheckExact(x):
        return True

    if PyUnicode_CheckExact(x):
        return True

    if PyFloat_CheckExact(x):
        if allow_nan_inf:
            return True
        v = PyFloat_AS_DOUBLE(x)
        return not (isnan(v) or isinf(v))

    return False


cdef bint _validate(object x, bint allow_nan_inf, Py_ssize_t max_depth, Py_ssize_t depth) except -1:
    cdef Py_ssize_t pos = 0
    cdef Py_ssize_t n, i
    cdef object item
    cdef PyObject *pk
    cdef PyObject *pv

    if depth > max_depth:
        return False

    if _is_primitive_exact(x, allow_nan_inf):
        return True

    if PyList_CheckExact(x):
        n = PyList_GET_SIZE(x)
        for i in range(n):
            item = <object>PyList_GET_ITEM(x, i)
            if not _validate(item, allow_nan_inf, max_depth, depth + 1):
                return False
        return True

    if PyTuple_CheckExact(x):
        n = PyTuple_GET_SIZE(x)
        for i in range(n):
            item = <object>PyTuple_GET_ITEM(x, i)
            if not _validate(item, allow_nan_inf, max_depth, depth + 1):
                return False
        return True

    if PyDict_CheckExact(x):
        while PyDict_Next(x, &pos, &pk, &pv):
            if not PyUnicode_CheckExact(<object>pk):
                return False
            if not _validate(<object>pv, allow_nan_inf, max_depth, depth + 1):
                return False
        return True

    return False


cpdef bint is_jsonable(object x, bint allow_nan_inf=True, Py_ssize_t max_depth=256):
    """Fast Cython validator for JSON-compatible Python objects.

    Accepted: None, bool, int, float (including NaN/Inf), str,
              list, tuple, dict[str, ...]
    Rejected: set, bytes, custom classes, subclasses of containers,
              non-str dict keys
    """
    return _validate(x, allow_nan_inf, max_depth, 0)
