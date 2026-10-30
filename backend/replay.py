"""Replay activity rows through POST /posts, compressed in time by --speed."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
import psycopg
from backend.activity import busiest_window, find_activity, load_events, synthetic_events
from backend.config import API_BASE, DATABASE_URL, ROOT as PROJECT_ROOT, redis_client


def load_replay_events(data_dir: Path, limit: int):
    path = find_activity(data_dir)
    if path is None:
        print("no activity file; generating synthetic posts", flush=True)
        conn = psycopg.connect(DATABASE_URL, autocommit=True)
        events = synthetic_events(conn, n=limit or 20000)
        conn.close()
        return events, True
    events = load_events(path)
    if limit:
        events = events[:limit]
    return events, False


def _fail(errors: dict, message: str) -> None:
    errors["n"] += 1
    if "first" not in errors:
        errors["first"] = message
        print(f"first error: {message}", flush=True)


async def _post(client: httpx.AsyncClient, sem: asyncio.Semaphore, event, errors: dict) -> None:
    body = {"author": event.user_a, "text": event.text, "created_at": event.ts}
    try:
        # Only a refused connection is retried: the request never reached the
        # API, so retrying cannot create a duplicate post.
        for attempt in range(5):
            try:
                response = await client.post("/posts", json=body)
                break
            except httpx.ConnectError:
                if attempt == 4:
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))
        if response.status_code >= 300:
            _fail(errors, f"{response.status_code} {response.text[:200]}")
    except Exception as exc:
        _fail(errors, f"{type(exc).__name__}: {exc}")
    finally:
        sem.release()


async def replay(events, speed: float, base_url: str, wait_loadtest: bool) -> dict:
    client_redis = redis_client()
    if wait_loadtest:
        deadline = time.time() + 90
        while time.time() < deadline:
            if client_redis.get("loadtest:ready") == "1":
                break
            time.sleep(0.2)
        else:
            print("loadtest was not ready; starting replay anyway", flush=True)

    busy_start, busy_end, busy_posts = busiest_window(events)
    origin = events[0].ts
    wall0 = time.perf_counter()
    client_redis.set("replay:state", "running")
    client_redis.set("replay:speed", speed)
    client_redis.set("replay:started_wall", f"{time.time():.3f}")
    client_redis.set("replay:busy_start", busy_start)
    client_redis.set("replay:busy_end", busy_end)
    client_redis.set("replay:busy_posts", busy_posts)
    client_redis.set("replay:posts", 0)
    errors: dict = {"n": 0}
    sent = 0
    sem = asyncio.Semaphore(400)
    timeout = httpx.Timeout(60.0, connect=10.0)
    # uvicorn closes idle keep-alive connections after 5 s; expire them first so
    # a post is never sent on a socket the server is closing.
    limits = httpx.Limits(max_connections=400, max_keepalive_connections=200, keepalive_expiry=3.0)
    tasks: list[asyncio.Task] = []
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout, limits=limits, trust_env=False) as client:
        for index, event in enumerate(events, start=1):
            target = wall0 + (event.ts - origin) / speed
            delay = target - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            await sem.acquire()
            tasks.append(asyncio.create_task(_post(client, sem, event, errors)))
            sent += 1
            if index % 20000 == 0:
                behind = time.perf_counter() - (wall0 + (event.ts - origin) / speed)
                print(f"sent {index:,} behind {behind:.1f}s errors {errors['n']:,}", flush=True)
            if len(tasks) > 5000:
                tasks = [task for task in tasks if not task.done()]
        if tasks:
            await asyncio.gather(*tasks)
    schedule_lag = time.perf_counter() - (wall0 + (events[-1].ts - origin) / speed)
    client_redis.set("replay:posts", sent)
    client_redis.set("replay:errors", errors["n"])
    client_redis.set("replay:schedule_lag_s", f"{max(schedule_lag, 0):.3f}")
    client_redis.set("replay:finished_wall", f"{time.time():.3f}")
    client_redis.set("replay:state", "done")
    client_redis.close()
    print(
        f"replay done sent={sent:,} errors={errors['n']:,} schedule_lag={max(schedule_lag, 0):.1f}s",
        flush=True,
    )
    if errors["n"]:
        print("first error:", errors.get("first", ""), flush=True)
    return {"sent": sent, "errors": errors["n"], "schedule_lag_s": max(schedule_lag, 0)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=60)
    parser.add_argument("--data", default=str(PROJECT_ROOT / "data"))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--base-url", default=API_BASE)
    parser.add_argument("--wait-loadtest", action="store_true")
    args = parser.parse_args()
    data_dir = Path(args.data)
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    events, synthetic = load_replay_events(data_dir, args.limit)
    if not events:
        raise SystemExit("no events to replay")
    label = "synthetic" if synthetic else "higgs"
    print(f"replaying {len(events):,} {label} posts at speed {args.speed}", flush=True)
    try:
        result = asyncio.run(replay(events, args.speed, args.base_url, args.wait_loadtest))
    except Exception:
        client = redis_client()
        client.set("replay:state", "error")
        client.close()
        raise
    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
