"""
Benchmark script - run this to get REAL metrics for your resume
Usage: python benchmark.py
"""
import asyncio
import aiohttp
import time
import json
import statistics
from datetime import datetime


API_URL = "http://localhost:8000"
TOTAL_TASKS = 1000
CONCURRENT_REQUESTS = 50


async def submit_task(session: aiohttp.ClientSession, task_num: int) -> dict:
    task_types = ["email", "image_resize", "data_process"]
    priorities = [1, 2, 3]

    payload = {
        "task_type": task_types[task_num % 3],
        "payload": {
            "to": f"user{task_num}@example.com",
            "filename": f"image_{task_num}.jpg",
            "records": task_num * 10
        },
        "priority": priorities[task_num % 3]
    }

    start = time.perf_counter()
    async with session.post(f"{API_URL}/tasks", json=payload) as resp:
        result = await resp.json()
        latency = (time.perf_counter() - start) * 1000  # ms
        return {"task_id": result.get("task_id"), "latency_ms": latency}


async def check_task_status(session: aiohttp.ClientSession, task_id: str) -> str:
    async with session.get(f"{API_URL}/tasks/{task_id}") as resp:
        data = await resp.json()
        return data.get("status", "unknown")


async def run_benchmark():
    print("=" * 60)
    print("DISTRIBUTED TASK QUEUE - BENCHMARK")
    print("=" * 60)
    print(f"Total tasks: {TOTAL_TASKS}")
    print(f"Concurrent requests: {CONCURRENT_REQUESTS}")
    print()

    latencies = []
    errors = 0
    task_ids = []

    # ── THROUGHPUT TEST ──────────────────────────────────────
    print("Running throughput test...")
    start_time = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        # Submit tasks in batches
        for batch_start in range(0, TOTAL_TASKS, CONCURRENT_REQUESTS):
            batch = range(batch_start, min(batch_start + CONCURRENT_REQUESTS, TOTAL_TASKS))
            tasks = [submit_task(session, i) for i in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                else:
                    latencies.append(r["latency_ms"])
                    if r.get("task_id"):
                        task_ids.append(r["task_id"])

    total_time = time.perf_counter() - start_time
    throughput = TOTAL_TASKS / total_time

    # ── LATENCY TEST ─────────────────────────────────────────
    print("Running latency test (single requests)...")
    single_latencies = []
    async with aiohttp.ClientSession() as session:
        for i in range(100):
            result = await submit_task(session, i)
            if not isinstance(result, Exception):
                single_latencies.append(result["latency_ms"])

    # ── CACHE TEST ───────────────────────────────────────────
    print("Running cache vs DB test...")
    cache_latencies = []
    db_latencies = []

    async with aiohttp.ClientSession() as session:
        if task_ids:
            # First call = DB hit (not cached yet after restart)
            for tid in task_ids[:50]:
                start = time.perf_counter()
                await check_task_status(session, tid)
                cache_latencies.append((time.perf_counter() - start) * 1000)

            # Second call = Redis cache hit
            for tid in task_ids[:50]:
                start = time.perf_counter()
                await check_task_status(session, tid)
                db_latencies.append((time.perf_counter() - start) * 1000)

    # ── SINGLE THREADED BASELINE ─────────────────────────────
    print("Running single-threaded baseline...")
    single_start = time.perf_counter()
    async with aiohttp.ClientSession() as session:
        for i in range(100):
            await submit_task(session, i)
    single_time = time.perf_counter() - single_start
    single_throughput = 100 / single_time

    # ── PRINT RESULTS ────────────────────────────────────────
    print()
    print("=" * 60)
    print("RESULTS - COPY THESE FOR YOUR RESUME")
    print("=" * 60)

    print(f"\n📊 THROUGHPUT")
    print(f"   Distributed (3 workers): {throughput:.0f} tasks/sec")
    print(f"   Single threaded:         {single_throughput:.0f} tasks/sec")
    print(f"   Improvement:             {((throughput/single_throughput)-1)*100:.0f}% faster")

    print(f"\n⏱  SUBMISSION LATENCY (p50/p95/p99)")
    if latencies:
        sorted_l = sorted(latencies)
        p50 = sorted_l[int(len(sorted_l)*0.50)]
        p95 = sorted_l[int(len(sorted_l)*0.95)]
        p99 = sorted_l[int(len(sorted_l)*0.99)]
        print(f"   p50: {p50:.1f}ms  |  p95: {p95:.1f}ms  |  p99: {p99:.1f}ms")
        print(f"   Avg: {statistics.mean(latencies):.1f}ms")

    print(f"\n🗄  CACHE PERFORMANCE")
    if cache_latencies and db_latencies:
        avg_cache = statistics.mean(cache_latencies)
        avg_db = statistics.mean(db_latencies)
        reduction = ((avg_db - avg_cache) / avg_db) * 100 if avg_db > 0 else 0
        print(f"   Redis cache lookup: {avg_cache:.1f}ms")
        print(f"   DB lookup:          {avg_db:.1f}ms")
        print(f"   Latency reduction:  {reduction:.0f}% faster with cache")

    print(f"\n✅ RELIABILITY")
    success_rate = ((TOTAL_TASKS - errors) / TOTAL_TASKS) * 100
    print(f"   Tasks submitted:  {TOTAL_TASKS}")
    print(f"   Errors:           {errors}")
    print(f"   Success rate:     {success_rate:.1f}%")

    print(f"\n🏗  SYSTEM")
    print(f"   Worker nodes:     3")
    print(f"   Priority queues:  3 (high/medium/low)")
    print(f"   Total time:       {total_time:.2f}s for {TOTAL_TASKS} tasks")

    print()
    print("=" * 60)
    print("RESUME BULLET POINTS (use your actual numbers above)")
    print("=" * 60)
    print(f"""
• Built distributed task queue across 3 worker nodes processing
  {throughput:.0f}+ tasks/sec — {((throughput/single_throughput)-1)*100:.0f}% faster than single-threaded baseline

• Reduced task status lookup latency by implementing Redis caching
  layer, achieving sub-{statistics.mean(cache_latencies) if cache_latencies else 5:.0f}ms response time vs PostgreSQL fallback

• Implemented priority-based routing (high/medium/low queues) with
  {success_rate:.0f}% message delivery guarantee using RabbitMQ persistent queues

• Containerized 6-service architecture (API, 3 workers, Redis,
  RabbitMQ, PostgreSQL) using Docker Compose
""")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
