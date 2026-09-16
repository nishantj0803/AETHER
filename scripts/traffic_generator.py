#!/usr/bin/env python3
"""
Aether Synthetic Traffic Generator
Simulates steady customer checkout traffic against demo payment service.
"""

import argparse
import asyncio
import random
import time
import httpx

ENDPOINTS = [
    {"path": "/health", "weight": 20, "method": "GET"},
    {"path": "/api/v1/payments/charge", "weight": 80, "method": "POST"},
]

PAYMENT_AMOUNTS = [19.99, 49.50, 89.00, 120.00, 250.75, 499.99, 1200.00]
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "INR"]

async def send_request(client: httpx.AsyncClient, base_url: str):
    choice = random.choices(ENDPOINTS, weights=[e["weight"] for e in ENDPOINTS])[0]
    url = f"{base_url}{choice['path']}"
    
    try:
        if choice["method"] == "GET":
            resp = await client.get(url, timeout=3.0)
        else:
            payload = {
                "account_id": f"acc_{random.randint(100000, 999999)}",
                "amount": random.choice(PAYMENT_AMOUNTS),
                "currency": random.choice(CURRENCIES)
            }
            resp = await client.post(url, json=payload, timeout=3.0)
        return resp.status_code
    except Exception as e:
        return 500

async def traffic_worker(base_url: str, stop_event: asyncio.Event, stats: dict):
    async with httpx.AsyncClient() as client:
        while not stop_event.is_set():
            status = await send_request(client, base_url)
            stats["total"] += 1
            if status == 200:
                stats["200"] += 1
            else:
                stats["errors"] += 1
            # Rate limiter sleep (~20-50ms)
            await asyncio.sleep(random.uniform(0.02, 0.06))

async def main():
    parser = argparse.ArgumentParser(description="Aether Traffic Generator")
    parser.add_argument("--url", default="http://localhost:8000", help="Target base URL")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of concurrent workers")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (0 for indefinite)")
    args = parser.parse_args()

    print(f"🚀 Starting Aether Traffic Generator against {args.url}")
    print(f"⚡ Concurrency: {args.concurrency} workers | Duration: {args.duration or 'infinite'}s\n")

    stop_event = asyncio.Event()
    stats = {"total": 0, "200": 0, "errors": 0}

    workers = [asyncio.create_task(traffic_worker(args.url, stop_event, stats)) for _ in range(args.concurrency)]

    start_time = time.time()
    try:
        while True:
            await asyncio.sleep(2.0)
            elapsed = time.time() - start_time
            rps = stats["total"] / elapsed if elapsed > 0 else 0
            err_pct = (stats["errors"] / stats["total"] * 100) if stats["total"] > 0 else 0
            print(f"[{int(elapsed)}s] Total: {stats['total']} | 200s: {stats['200']} | 5xx Errors: {stats['errors']} ({err_pct:.1f}%) | Throughput: {rps:.1f} req/s")
            
            if args.duration and elapsed >= args.duration:
                break
    except KeyboardInterrupt:
        print("\nStopping traffic generator...")
    finally:
        stop_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        print("\n🏁 Traffic generation complete.")

if __name__ == "__main__":
    asyncio.run(main())
