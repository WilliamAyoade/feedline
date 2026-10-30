"""Concurrent feed readers and SSE clients. Runs while replay.py is posting."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
import psycopg
from backend.config import API_BASE, DATABASE_URL, WARMUP_S, redis_client
from backend.store import parse_iso


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * p
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[int(rank)]
    return ordered[lo] * (hi - rank) + ordered[hi] * (rank - lo)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def sample_readers(n_uniform: int = 100, n_celeb: int = 100) -> list[tuple[int, bool]]:
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    celeb_ids: list[int] = []
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE follower_count >= 10000 ORDER BY follower_count DESC")
        celebrities = [int(row[0]) for row in cur.fetchall()]
        for author in celebrities:
            if len(celeb_ids) >= n_celeb:
                break
            cur.execute(
                """
                SELECT follower FROM follows
                WHERE followee = %s
                ORDER BY random()
                LIMIT %s
                """,
                (author, n_celeb - len(celeb_ids)),
            )
            for row in cur.fetchall():
                user_id = int(row[0])
                if user_id not in celeb_ids:
                    celeb_ids.append(user_id)
        celeb_set = set(celeb_ids)
        uniform: list[int] = []
        while len(uniform) < n_uniform:
            cur.execute("SELECT id FROM users ORDER BY random() LIMIT %s", (n_uniform * 2,))
            for row in cur.fetchall():
                user_id = int(row[0])
                if user_id in celeb_set or user_id in uniform:
                    continue
                uniform.append(user_id)
                if len(uniform) >= n_uniform:
                    break
    conn.close()
    return [(user_id, True) for user_id in celeb_ids] + [(user_id, False) for user_id in uniform]


def parse_sse(block: str) -> tuple[str | None, dict | None]:
    event_id = None
    data = None
    for line in block.split("\n"):
        if not line or line.startswith(":"):
            continue
        if line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data = line[5:].strip()
    if not data:
        return event_id, None
    return event_id, json.loads(data)


class Samples:
    def __init__(self) -> None:
        self.reads: list[float] = []
        self.celeb_reads: list[float] = []
        self.fresh: list[float] = []
        self.fresh_busy: list[float] = []
        self.read_errors = 0
        self.sse_events = 0
        self.record_reads = False
        self.record_fresh = False
        self.busy_start = 0.0
        self.busy_end = 0.0


async def sse_loop(client: httpx.AsyncClient, user: int, celeb: bool, samples: Samples, stop: asyncio.Event) -> None:
    last_id = None
    seen: set[int] = set()
    while not stop.is_set():
        headers = {"Last-Event-ID": last_id} if last_id else {}
        try:
            async with client.stream("GET", f"/stream/{user}", headers=headers) as response:
                buffer = ""
                async for chunk in response.aiter_text():
                    if stop.is_set():
                        return
                    buffer += chunk
                    while "\n\n" in buffer:
                        raw, buffer = buffer.split("\n\n", 1)
                        event_id, payload = parse_sse(raw)
                        if event_id:
                            last_id = event_id
                        if not payload or payload.get("replayed"):
                            continue
                        post_id = int(payload["id"])
                        if post_id in seen:
                            continue
                        seen.add(post_id)
                        if not samples.record_fresh:
                            continue
                        accepted = parse_iso(payload["accepted_at"])
                        delivered = parse_iso(payload["delivered_at"])
                        delay_ms = max(0.0, (delivered - accepted).total_seconds() * 1000)
                        samples.fresh.append(delay_ms)
                        samples.sse_events += 1
                        created = parse_iso(payload["created_at"]).timestamp()
                        if samples.busy_start <= created <= samples.busy_end:
                            samples.fresh_busy.append(delay_ms)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(0.5)


async def reader_loop(
    client: httpx.AsyncClient,
    user: int,
    celeb: bool,
    samples: Samples,
    stop: asyncio.Event,
) -> None:
    sse_task = asyncio.create_task(sse_loop(client, user, celeb, samples, stop))
    try:
        while not stop.is_set():
            started = time.perf_counter()
            try:
                response = await client.get(f"/feed/{user}", params={"limit": 50})
                elapsed = (time.perf_counter() - started) * 1000
                if response.status_code == 200 and samples.record_reads:
                    samples.reads.append(elapsed)
                    if celeb:
                        samples.celeb_reads.append(elapsed)
                elif response.status_code != 200:
                    samples.read_errors += 1
            except Exception:
                samples.read_errors += 1
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
    finally:
        sse_task.cancel()
        try:
            await sse_task
        except asyncio.CancelledError:
            pass


def _stat_block(values: list[float]) -> dict:
    return {
        "p50": _round(percentile(values, 0.50)),
        "p95": _round(percentile(values, 0.95)),
        "p99": _round(percentile(values, 0.99)),
        "count": len(values),
    }


async def run(out_path: Path) -> None:
    readers = sample_readers()
    print(f"sampled {len(readers)} readers", flush=True)
    client_redis = redis_client()
    client_redis.delete("loadtest:ready")
    samples = Samples()
    stop = asyncio.Event()
    timeout = httpx.Timeout(60.0, connect=10.0, read=None)
    limits = httpx.Limits(max_connections=400, max_keepalive_connections=300)
    async with httpx.AsyncClient(base_url=API_BASE, timeout=timeout, limits=limits, trust_env=False) as client:
        tasks = [
            asyncio.create_task(reader_loop(client, user, celeb, samples, stop))
            for user, celeb in readers
        ]
        await asyncio.sleep(1.0)
        client_redis.set("loadtest:ready", "1")
        print("loadtest ready", flush=True)
        started = None
        deadline = time.time() + 6 * 3600
        try:
            while time.time() < deadline:
                state = client_redis.get("replay:state")
                if started is None and state == "running":
                    started = float(client_redis.get("replay:started_wall") or time.time())
                    samples.busy_start = float(client_redis.get("replay:busy_start") or 0)
                    samples.busy_end = float(client_redis.get("replay:busy_end") or 0)
                if started is not None and time.time() >= started + WARMUP_S:
                    samples.record_reads = state == "running"
                    samples.record_fresh = True
                if state == "done":
                    samples.record_reads = False
                    lag = int(client_redis.get("stats:backlog") or 0)
                    if lag == 0:
                        break
                if state == "error":
                    break
                await asyncio.sleep(1.0)
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
    name = client_redis.get("config:strategy") or "push"
    threshold = int(client_redis.get("config:threshold") or 0)
    posts = int(client_redis.get("replay:posts") or 0)
    writes = int(client_redis.get("stats:feed_writes") or 0)
    memory = client_redis.info("memory")
    read_window = 0.0
    if started is not None:
        finished = float(client_redis.get("replay:finished_wall") or time.time())
        read_window = max(0.0, finished - (started + WARMUP_S))
    strategy_label = name if name != "hybrid" else f"hybrid-{threshold}"
    payload = {
        "strategy": name,
        "threshold": threshold if name == "hybrid" else None,
        "label": strategy_label,
        "speed": float(client_redis.get("replay:speed") or 0),
        "posts": posts,
        "errors": int(client_redis.get("replay:errors") or 0),
        "schedule_lag_s": float(client_redis.get("replay:schedule_lag_s") or 0),
        "feed_writes": writes,
        "write_amp": round(writes / posts, 4) if posts else 0,
        "redis_mb": round(int(memory["used_memory"]) / (1024 * 1024), 1),
        "worst_post": int(client_redis.get("stats:worst_post") or 0),
        "backlog_peak": int(client_redis.get("stats:backlog_peak") or 0),
        "drain_s": float(client_redis.get("stats:drain_s") or 0),
        "read": _stat_block(samples.reads),
        "celeb_read": _stat_block(samples.celeb_reads),
        "fresh": {
            "p50": _round(percentile(samples.fresh, 0.50)),
            "p99": _round(percentile(samples.fresh, 0.99)),
            "count": len(samples.fresh),
        },
        "fresh_busy": {
            "p99": _round(percentile(samples.fresh_busy, 0.99)),
            "count": len(samples.fresh_busy),
        },
        "req_per_s": round(len(samples.reads) / read_window, 2) if read_window else 0,
        "read_errors": samples.read_errors,
        "readers": len(readers),
        "warmup_s": WARMUP_S,
        "busy_start": samples.busy_start,
        "busy_end": samples.busy_end,
        "busy_posts": int(client_redis.get("replay:busy_posts") or 0),
        "match": None,
        "finished_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    client_redis.close()
    print(f"wrote {out_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    asyncio.run(run(Path(args.out)))


if __name__ == "__main__":
    main()
