# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import pytest

from ringq import Queue


class TestValidation:
    """Tests for validate=True (fast Cython validator, default)."""

    def setup_method(self):
        self.q = Queue(validate=True)

    def test_valid_primitives(self):
        for item in [None, True, False, 42, 3.14, "hello"]:
            self.q.put_nowait(item)

    def test_valid_dict(self):
        self.q.put_nowait({"key": "value", "nested": {"a": 1}})

    def test_valid_list(self):
        self.q.put_nowait([1, "two", None, [3, 4]])

    def test_valid_tuple(self):
        self.q.put_nowait((1, 2, "three"))

    def test_invalid_set(self):
        with pytest.raises(TypeError, match="not JSON-compatible"):
            self.q.put_nowait({1, 2, 3})

    def test_invalid_object(self):
        with pytest.raises(TypeError, match="not JSON-compatible"):
            self.q.put_nowait(object())

    def test_invalid_bytes(self):
        with pytest.raises(TypeError, match="not JSON-compatible"):
            self.q.put_nowait(b"bytes")

    def test_invalid_dict_key(self):
        with pytest.raises(TypeError, match="not JSON-compatible"):
            self.q.put_nowait({1: "value"})

    def test_invalid_nested(self):
        with pytest.raises(TypeError, match="not JSON-compatible"):
            self.q.put_nowait({"key": {1, 2}})

    def test_validation_disabled(self):
        q = Queue(validate=False)
        q.put_nowait({1, 2, 3})  # no error
        q.put_nowait(object())  # no error

    def test_nan_inf_accepted(self):
        self.q.put_nowait(float("nan"))
        self.q.put_nowait(float("inf"))
        self.q.put_nowait(float("-inf"))

    def test_validate_fast_explicit(self):
        q = Queue(validate="fast")
        q.put_nowait({"key": [1, 2, "three"]})
        with pytest.raises(TypeError):
            q.put_nowait(object())

    def test_invalid_validate_mode(self):
        with pytest.raises(ValueError, match="invalid validate mode"):
            Queue(validate="invalid")  # type: ignore


class TestValidationFull:
    """Tests for validate='full' (orjson/json serialization check)."""

    def setup_method(self):
        self.q = Queue(validate="full")

    def test_valid_primitives(self):
        for item in [None, True, False, 42, 3.14, "hello"]:
            self.q.put_nowait(item)

    def test_valid_dict(self):
        self.q.put_nowait({"key": "value", "nested": {"a": 1}})

    def test_valid_list(self):
        self.q.put_nowait([1, "two", None, [3, 4]])

    def test_invalid_set(self):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            self.q.put_nowait({1, 2, 3})

    def test_invalid_object(self):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            self.q.put_nowait(object())

    def test_invalid_bytes(self):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            self.q.put_nowait(b"bytes")

    def test_invalid_dict_key(self):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            self.q.put_nowait({1: "value"})

    def test_invalid_nested(self):
        with pytest.raises(TypeError, match="not JSON-serializable"):
            self.q.put_nowait({"key": {1, 2}})

    def test_nan_inf_accepted(self):
        """NaN/Inf are accepted by both orjson and stdlib json paths."""
        self.q.put_nowait(float("nan"))
        self.q.put_nowait(float("inf"))
        self.q.put_nowait(float("-inf"))

    def test_nested_nan_inf_accepted(self):
        """NaN/Inf nested inside dicts/lists are also accepted."""
        self.q.put_nowait({"val": float("nan")})
        self.q.put_nowait([1, float("inf"), 2])
