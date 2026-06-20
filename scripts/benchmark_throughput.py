#!/usr/bin/env python3
"""
benchmark_throughput.py — LogFoundry Sustained Throughput Benchmark

This script blasts the /ingest endpoint using an aiohttp connection pool.
Crucially, it generates a random 'X-Forwarded-For' IP address for every single
request. This perfectly simulates a distributed load from distinct clients,
bypassing the per-client Rate Limiter to reveal the API's *true* ingestion throughput.

Usage:
  python3 scripts/benchmark_throughput.py [num_requests] [concurrency]
"""

import asyncio
import json
import random
import sys
import time
from uuid import uuid4

import aiohttp

API_URL = "http://localhost:8000/ingest"

async def fire_request(session: aiohttp.ClientSession, event_data: dict) -> int:
    # Generate a random IP to bypass per-client rate limit
    ip = f"{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"
    headers = {"X-Forwarded-For": ip, "Content-Type": "application/json"}
    
    start_time = time.monotonic()
    try:
        async with session.post(API_URL, json=event_data, headers=headers) as resp:
            await resp.read()
            latency = time.monotonic() - start_time
            return resp.status, latency
    except Exception:
        return 0, time.monotonic() - start_time

async def worker(session: aiohttp.ClientSession, queue: asyncio.Queue, results: list):
    while True:
        try:
            event_data = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
            
        status, latency = await fire_request(session, event_data)
        results.append((status, latency))
        queue.task_done()

async def main():
    num_requests = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    print(f"🚀 Benchmarking TRUE Throughput: {num_requests} requests, {concurrency} concurrency")
    print(f"   (Bypassing per-client rate limit via random X-Forwarded-For)")

    queue = asyncio.Queue()
    for i in range(num_requests):
        queue.put_nowait({
            "id": str(uuid4()),
            "service": "benchmark",
            "level": "INFO",
            "message": f"Benchmark event {i}",
            "timestamp": "2026-06-20T10:00:00+00:00"
        })

    results = []
    
    connector = aiohttp.TCPConnector(limit=concurrency)
    start_time = time.time()
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for _ in range(concurrency):
            task = asyncio.create_task(worker(session, queue, results))
            tasks.append(task)
            
        await asyncio.gather(*tasks)

    total_time = time.time() - start_time
    
    statuses = {}
    latencies = []
    for status, latency in results:
        statuses[status] = statuses.get(status, 0) + 1
        latencies.append(latency)
        
    latencies.sort()
    
    p50 = latencies[int(len(latencies) * 0.50)] * 1000
    p95 = latencies[int(len(latencies) * 0.95)] * 1000
    p99 = latencies[int(len(latencies) * 0.99)] * 1000
    
    print("\n✅ Benchmark Complete")
    print("-" * 30)
    print(f"Total time:    {total_time:.2f}s")
    print(f"Requests/sec:  {num_requests / total_time:.2f}")
    print("-" * 30)
    print("Latencies:")
    print(f"  p50: {p50:.2f}ms")
    print(f"  p95: {p95:.2f}ms")
    print(f"  p99: {p99:.2f}ms")
    print("-" * 30)
    print("Status codes:")
    for status, count in statuses.items():
        print(f"  [{status}] {count} responses")

if __name__ == "__main__":
    # Workaround for uvloop EventLoopPolicy issue in tests/scripts if any
    asyncio.run(main())
