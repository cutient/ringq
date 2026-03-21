# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

"""Benchmark: asyncio.Queue vs ringq.Queue throughput comparison."""

import asyncio
import time


def bench_sync(queue_factory, n, label):
    """Benchmark put_nowait/get_nowait throughput."""
    q = queue_factory()
    start = time.perf_counter_ns()
    for i in range(n):
        q.put_nowait(i)
    for i in range(n):
        q.get_nowait()
    elapsed_ns = time.perf_counter_ns() - start
    ops = 2 * n
    ops_per_sec = ops / (elapsed_ns / 1e9)
    print(f"  {label:40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)")
    return ops_per_sec


def main():
    from ringq import Queue

    n = 1_000_000
    maxsizes = [100, 1000, 10000]

    print(f"Benchmark: {n:,} put + {n:,} get operations\n")

    # --- Unbounded ---
    print("=== Unbounded ===")
    bench_sync(lambda: asyncio.Queue(), n, "asyncio.Queue()")
    bench_sync(lambda: Queue(), n, "ringq.Queue()")
    print()

    # --- Bounded ---
    for ms in maxsizes:
        print(f"=== Bounded maxsize={ms} ===")
        bench_sync(lambda ms=ms: asyncio.Queue(ms), ms, f"asyncio.Queue({ms})")
        bench_sync(lambda ms=ms: Queue(ms), ms, f"ringq.Queue({ms})")
        print()

    # --- Eviction old ---
    for ms in maxsizes:
        print(f"=== Eviction old maxsize={ms} (fill to 2x) ===")
        q = Queue(ms, eviction="old")
        start = time.perf_counter_ns()
        for i in range(ms * 2):
            q.put_nowait(i)
        for i in range(ms):
            q.get_nowait()
        elapsed_ns = time.perf_counter_ns() - start
        ops = ms * 3
        ops_per_sec = ops / (elapsed_ns / 1e9)
        label = 'ringq.Queue(eviction="old")'
        print(
            f"  {label:40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
        )
        print()

    # --- Eviction new ---
    for ms in maxsizes:
        print(f"=== Eviction new maxsize={ms} (fill to 2x) ===")
        q = Queue(ms, eviction="new")
        start = time.perf_counter_ns()
        for i in range(ms * 2):
            q.put_nowait(i)
        for i in range(ms):
            q.get_nowait()
        elapsed_ns = time.perf_counter_ns() - start
        ops = ms * 3
        ops_per_sec = ops / (elapsed_ns / 1e9)
        label = 'ringq.Queue(eviction="new")'
        print(
            f"  {label:40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
        )
        print()

    # --- Dedup drop ---
    for ms in maxsizes:
        print(f"=== Dedup drop maxsize={ms} ===")
        q = Queue(ms, dedup=True)
        start = time.perf_counter_ns()
        for i in range(ms):
            q.put_nowait(i)
        # Put duplicates (all dropped)
        for i in range(ms):
            q.put_nowait(i)
        for i in range(ms):
            q.get_nowait()
        elapsed_ns = time.perf_counter_ns() - start
        ops = ms * 3
        ops_per_sec = ops / (elapsed_ns / 1e9)
        print(
            f"  {'ringq.Queue(dedup=True)':40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
        )
        print()

    # --- Dedup replace ---
    for ms in maxsizes:
        print(f"=== Dedup replace maxsize={ms} ===")
        q = Queue(ms, dedup="replace")
        start = time.perf_counter_ns()
        for i in range(ms):
            q.put_nowait(i)
        # Replace all
        for i in range(ms):
            q.put_nowait(i)
        for i in range(ms):
            q.get_nowait()
        elapsed_ns = time.perf_counter_ns() - start
        ops = ms * 3
        ops_per_sec = ops / (elapsed_ns / 1e9)
        label = 'ringq.Queue(dedup="replace")'
        print(
            f"  {label:40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
        )
        print()

    # --- Validation ---
    payload = {"key": "value", "nested": {"a": 1, "b": [1, 2, "three"]}}
    print(f"=== Validation (dict payload, {n:,} puts) ===")
    for mode, label in [
        ("fast", 'ringq.Queue(validate="fast")'),
        ("full", 'ringq.Queue(validate="full")'),
    ]:
        q = Queue(validate=mode)  # type: ignore
        start = time.perf_counter_ns()
        for i in range(n):
            q.put_nowait(payload)
        elapsed_ns = time.perf_counter_ns() - start
        ops_per_sec = n / (elapsed_ns / 1e9)
        print(
            f"  {label:40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
        )
    # No validation baseline
    q = Queue()
    start = time.perf_counter_ns()
    for i in range(n):
        q.put_nowait(payload)
    elapsed_ns = time.perf_counter_ns() - start
    ops_per_sec = n / (elapsed_ns / 1e9)
    print(
        f"  {'ringq.Queue(validate=False)':40s} {ops_per_sec:>12,.0f} ops/sec  ({elapsed_ns / 1e6:.1f} ms)"
    )
    print()


if __name__ == "__main__":
    main()
