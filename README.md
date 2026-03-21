# ringq

[![CI](https://github.com/cutient/ringq/actions/workflows/ci.yml/badge.svg)](https://github.com/cutient/ringq/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/ringq)](https://pypi.org/project/ringq/)
[![Python](https://img.shields.io/pypi/pyversions/ringq)](https://pypi.org/project/ringq/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

High-performance async queue backed by a Cython ring buffer. Drop-in replacement for `asyncio.Queue` with optional eviction, deduplication, and JSON validation.

## Why ringq?

- **Faster than `asyncio.Queue`** — Cython ring buffer with power-of-2 bitmask indexing, zero Python overhead in the hot path. 10–50% faster depending on configuration.
- **Eviction policies** — bounded queues that never block: discard the oldest item or silently reject the newest.
- **Built-in deduplication** — drop or replace duplicates by value or custom key, without maintaining a separate set.
- **JSON validation** — catch non-serializable data at enqueue time, not when you try to send it downstream.
- **Drop-in compatible** — same interface as `asyncio.Queue`, including `shutdown()` (Python 3.13+), `task_done()`, and `join()`.
- **Zero runtime dependencies** — optional `orjson` for faster full validation.

## Install

```bash
pip install ringq
```

Optional: install `orjson` for faster `validate="full"` mode:

```bash
pip install ringq orjson
```

## Quick start

```python
import asyncio
from ringq import Queue

async def main():
    # Basic FIFO (same as asyncio.Queue)
    q = Queue()
    await q.put("hello")
    print(await q.get())  # "hello"

    # Bounded with eviction (discard oldest when full)
    q = Queue(maxsize=100, eviction=True)

    # Deduplication — drop new duplicates
    q = Queue(maxsize=100, dedup=True)

    # Deduplication — replace existing with new value
    q = Queue(maxsize=100, dedup="replace", key=lambda x: x["id"])

    # JSON validation
    q = Queue(validate=True)
    q.put_nowait({"key": "value"})  # OK
    # q.put_nowait(set())           # raises TypeError

asyncio.run(main())
```

## Features

### Eviction policies

Control what happens when a bounded queue is full.

```python
# Default: raise QueueFull (same as asyncio.Queue)
q = Queue(maxsize=100)

# Discard oldest item to make room for the new one
q = Queue(maxsize=100, eviction=True)   # or eviction="old"

# Silently reject the new item, never blocks
q = Queue(maxsize=100, eviction="new")
```

With `eviction="old"`, `put()` and `put_nowait()` never block or raise `QueueFull` — the oldest item is evicted automatically. With `eviction="new"`, the new item is silently dropped and `put_nowait()` returns `False`.

### Deduplication

Prevent duplicate items from accumulating in the queue.

```python
# Drop duplicates — keep the original, reject the new one
q = Queue(dedup=True)         # or dedup="drop"
q.put_nowait("a")             # True
q.put_nowait("a")             # False (duplicate dropped)

# Replace duplicates — update the value in-place
q = Queue(dedup="replace")
q.put_nowait("old_value")     # True
q.put_nowait("old_value")     # True (original replaced)

# Custom key function — deduplicate by a specific field
q = Queue(dedup="replace", key=lambda x: x["id"])
q.put_nowait({"id": 1, "status": "pending"})
q.put_nowait({"id": 1, "status": "done"})     # replaces previous
print(q.get_nowait())  # {"id": 1, "status": "done"}
```

`put_nowait()` returns `True` if the item was inserted, `False` if it was dropped as a duplicate.

### JSON validation

Catch non-JSON-serializable data at enqueue time.

```python
# Fast mode (Cython, type checks only — no serialization)
q = Queue(validate=True)      # or validate="fast"
q.put_nowait({"key": [1, 2]}) # OK
q.put_nowait({1: "value"})    # TypeError — dict keys must be strings

# Full mode (actual JSON serialization via orjson or stdlib json)
q = Queue(validate="full")
q.put_nowait({"key": "value"}) # OK
q.put_nowait(set())            # TypeError
```

Fast mode checks basic types recursively via Cython (None, bool, int, float, str, list, tuple, dict with string keys). Full mode performs an actual serialization round-trip and accepts anything that `json.dumps` (or `orjson.dumps`) accepts.

### Combining features

All features compose freely:

```python
# Bounded queue with eviction, dedup by key, and JSON validation
q = Queue(
    maxsize=1000,
    eviction=True,
    dedup="replace",
    key=lambda x: x["id"],
    validate=True,
)
```

### Shutdown

Gracefully shut down a queue, compatible with Python 3.13+ `asyncio.Queue.shutdown()`.

```python
# Graceful — allow consumers to drain remaining items
q.shutdown()
# q.put_nowait(item)  # raises QueueShutDown
await q.get()          # returns remaining items, then raises QueueShutDown

# Immediate — discard all items, cancel all waiters
q.shutdown(immediate=True)
```

### Statistics

Track eviction and deduplication counters:

```python
q = Queue(maxsize=2, eviction=True, dedup=True)
q.put_nowait("a")
q.put_nowait("b")
q.put_nowait("c")  # evicts "a"
q.put_nowait("c")  # duplicate dropped

print(q.stats())
# {
#     "evictions": 1,
#     "dedup_drops": 1,
#     "dedup_replacements": 0,
#     "invalidated_skips": 0,
#     "maxsize": 2,
# }
```

## API reference

### Constructor

```python
Queue(
    maxsize=0,
    *,
    eviction=False,     # False | True | "old" | "new"
    dedup=False,        # False | True | "drop" | "replace"
    key=None,           # callable(item) -> hashable key
    validate=False,     # False | True | "fast" | "full"
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `maxsize` | `int` | `0` | Maximum number of items. `0` = unbounded. |
| `eviction` | `bool \| str` | `False` | `True`/`"old"`: evict oldest. `"new"`: reject newest. |
| `dedup` | `bool \| str` | `False` | `True`/`"drop"`: drop duplicates. `"replace"`: update in-place. |
| `key` | `callable` | `None` | Extract dedup key from items. Requires `dedup` to be enabled. |
| `validate` | `bool \| str` | `False` | `True`/`"fast"`: Cython basic type check. `"full"`: actual JSON serialization round-trip. |

### Methods

| Method | Returns | Raises | Description |
|--------|---------|--------|-------------|
| `put_nowait(item)` | `bool` | `QueueFull`, `QueueShutDown` | Insert item. Returns `True` if inserted, `False` if dropped. |
| `get_nowait()` | item | `QueueEmpty`, `QueueShutDown` | Remove and return next item. |
| `await put(item)` | `bool` | `QueueShutDown` | Async put. Waits if bounded and full (unless eviction enabled). |
| `await get()` | item | `QueueShutDown` | Async get. Waits if empty. |
| `peek_nowait()` | item | `QueueEmpty` | Return next item without removing it. |
| `task_done()` | `None` | `ValueError` | Mark a retrieved item as processed. |
| `await join()` | `None` | — | Wait until all items have been processed (`task_done()` called for each). |
| `clear()` | `None` | — | Remove all items and reset unfinished task counter. |
| `shutdown(immediate=False)` | `None` | — | Shut down the queue. Idempotent. |
| `stats()` | `dict` | — | Return `{"evictions", "dedup_drops", "dedup_replacements", "invalidated_skips", "maxsize"}`. |
| `qsize()` / `len(q)` | `int` | — | Number of items currently in the queue. |
| `empty()` | `bool` | — | `True` if the queue is empty. |
| `full()` | `bool` | — | `True` if bounded and at capacity. Always `False` for unbounded queues. |
| `maxsize` (property) | `int` | — | The queue's capacity (from constructor). |

### Exceptions

| Exception | When raised |
|-----------|-------------|
| `QueueEmpty` | `get_nowait()` on an empty queue |
| `QueueFull` | `put_nowait()` on a full bounded queue (without eviction) |
| `QueueShutDown` | `put()`/`get()` after `shutdown()` |

All exceptions are importable from `ringq`:

```python
from ringq import Queue, QueueEmpty, QueueFull, QueueShutDown
```

## Migrating from asyncio.Queue

ringq is a drop-in replacement. Change one import:

```diff
-from asyncio import Queue
+from ringq import Queue
```

Existing code continues to work. To take advantage of new features, add keyword arguments:

```python
# Before (asyncio.Queue)
q = asyncio.Queue(maxsize=100)

# After (ringq — same behavior, faster)
q = Queue(maxsize=100)

# After (ringq — with features)
q = Queue(maxsize=100, eviction=True, dedup="replace", key=lambda x: x["id"])
```

**Behavioral difference:** `put_nowait()` returns `bool` (always `True` for standard FIFO usage) instead of `None`.

## Benchmarks

1,000,000 `put_nowait` + 1,000,000 `get_nowait` operations, Python 3.14, Linux x86_64:

| Configuration | asyncio.Queue | ringq | Speedup |
|---|--:|--:|--:|
| Unbounded | 5.2M ops/s | 8.3M ops/s | **1.6x** |
| Bounded (maxsize=1000) | 4.7M ops/s | 9.0M ops/s | **1.9x** |
| Eviction old (maxsize=1000) | — | 7.6M ops/s | — |
| Eviction new (maxsize=1000) | — | 9.5M ops/s | — |
| Dedup drop (maxsize=1000) | — | 7.0M ops/s | — |
| Dedup replace (maxsize=1000) | — | 5.0M ops/s | — |
| Validate fast | — | 3.7M ops/s | — |
| Validate full | — | 2.4M ops/s | — |

Run benchmarks yourself:

```bash
uv run python benchmarks/run.py
```

## Development

### Setup

```bash
git clone https://github.com/cutient/ringq.git
cd ringq
uv sync --extra dev
```

### Test

```bash
uv run pytest tests/ -v
```

### Benchmark

```bash
uv run python benchmarks/run.py
```

## License

[MIT](LICENSE)
