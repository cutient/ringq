# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

import pytest


@pytest.fixture
def queue():
    """Create a default unbounded queue."""
    from ringq import Queue

    return Queue()


@pytest.fixture
def bounded_queue():
    """Create a bounded queue with maxsize=5."""
    from ringq import Queue

    return Queue(5)


@pytest.fixture
def eviction_queue():
    """Create a bounded queue with eviction enabled."""
    from ringq import Queue

    return Queue(5, eviction=True)


@pytest.fixture
def eviction_new_queue():
    """Create a bounded queue with eviction='new' enabled."""
    from ringq import Queue

    return Queue(5, eviction="new")


@pytest.fixture
def dedup_queue():
    """Create a bounded queue with dedup (drop) enabled."""
    from ringq import Queue

    return Queue(5, dedup=True)


@pytest.fixture
def replace_queue():
    """Create a bounded queue with dedup (replace) enabled."""
    from ringq import Queue

    return Queue(5, dedup="replace")
