# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT


class QueueEmpty(Exception):
    """Raised when get is called on an empty queue."""


class QueueFull(Exception):
    """Raised when put is called on a full bounded queue without eviction."""


class QueueShutDown(Exception):
    """Raised when put/get is called on a shut-down queue."""
