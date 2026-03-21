# Changelog

## [Unreleased]

## [0.1.0] — 2026-03-21

### Added
- `Queue` class — drop-in `asyncio.Queue` replacement backed by a Cython ring buffer
- Bounded mode with `maxsize` parameter
- Eviction policies: `eviction="old"` (discard oldest) and `eviction="new"` (reject newest)
- Deduplication: `dedup=True` / `"drop"` (drop duplicates) and `dedup="replace"` (update in-place)
- Custom key function for dedup via `key=` parameter
- JSON validation on put via `validate=True` (Cython fast path) or `validate="full"` (orjson)
- `peek_nowait()` — inspect next item without removing
- `stats()` — eviction/dedup counters
- `shutdown(immediate=False)` — Python 3.13-compatible shutdown
- `task_done()` / `join()` support with Cython-side `_unfinished` counter
- Full async API: `put()`, `get()`, `join()`
- PEP 561 type stubs (`py.typed`, `.pyi` files)
