"""Fan-out worker. Consumes the `fanout` stream and writes follower feeds.

Several workers can run at once; each joins the `workers` consumer group under
its own name.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import logging

import psycopg
import redis

from backend.config import DATABASE_URL, GROUP, STREAM, redis_client
from backend.store import Post, ensure_group, following_of, make_strategy, parse_iso, queue_live

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("feedline.worker")

# Backlog is XLEN: delivered messages are XDEL'd, so whatever remains is
# accepted but not yet delivered (queued or in flight on some worker).
STATS_LUA = """
local posts = tonumber(ARGV[2])
local writes = tonumber(ARGV[3])
local worst = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
if posts > 0 then redis.call('INCRBY', 'stats:posts', posts) end
if writes > 0 then redis.call('INCRBY', 'stats:feed_writes', writes) end
if worst > tonumber(redis.call('GET', 'stats:worst_post') or '0') then
  redis.call('SET', 'stats:worst_post', worst)
end
local lag = redis.call('XLEN', ARGV[1])
redis.call('SET', 'stats:backlog', lag)
local peak = tonumber(redis.call('GET', 'stats:backlog_peak') or '0')
if lag > peak then
  redis.call('SET', 'stats:backlog_peak', lag)
  redis.call('SET', 'stats:backlog_peak_at', ARGV[5])
  redis.call('DEL', 'stats:drain_s')
elseif lag == 0 and redis.call('EXISTS', 'stats:backlog_peak_at') == 1
    and redis.call('EXISTS', 'stats:drain_s') == 0 then
  local at = tonumber(redis.call('GET', 'stats:backlog_peak_at'))
  redis.call('SET', 'stats:drain_s', string.format('%.3f', now - at))
end
return lag
"""


class Listeners:
    """Which connected readers follow each author, refreshed every couple of seconds."""

    def __init__(self, conn, client, every: float = 2.0):
        self.conn = conn
        self.client = client
        self.every = every
        self.at = 0.0
        self.ids: set[int] = set()
        self.by_followee: dict[int, list[int]] = {}

    def targets(self, author: int) -> list[int]:
        now = time.time()
        if now - self.at >= self.every:
            self.at = now
            self.ids = {int(item) for item in self.client.smembers("listeners")}
            self.by_followee = following_of(self.conn, sorted(self.ids))
        return self.by_followee.get(author, [])


def handle(conn, client, listeners: Listeners, message_id: str, fields: dict) -> int:
    strategy = make_strategy(fields["strategy"], int(fields["threshold"]))
    post = Post(
        id=int(fields["id"]),
        author=int(fields["author"]),
        created_at=parse_iso(fields["created_at"]),
        text=fields["text"],
    )
    accepted = parse_iso(fields["accepted_at"])
    writes = strategy.post(conn, client, post, int(fields["follower_count"]))
    pipe = client.pipeline(transaction=False)
    queue_live(pipe, listeners.targets(post.author), post, accepted)
    pipe.xack(STREAM, GROUP, message_id)
    pipe.xdel(STREAM, message_id)
    pipe.execute()
    return writes


def process_batch(conn, client, stats, name: str, listeners: Listeners, messages) -> None:
    client.hset("stats:worker_busy", name, "1")
    total = 0
    worst = 0
    try:
        for message_id, fields in messages:
            writes = handle(conn, client, listeners, message_id, fields)
            total += writes
            worst = max(worst, writes)
    finally:
        client.hset("stats:worker_busy", name, "0")
        stats(args=[STREAM, len(messages), total, worst, f"{time.time():.3f}"])


def claim_stale(conn, client, stats, name: str, listeners: Listeners) -> None:
    """Take over messages another worker read but never acknowledged."""
    start = "0-0"
    while True:
        next_id, messages, *_ = client.xautoclaim(
            STREAM, GROUP, name, min_idle_time=30_000, start_id=start, count=50
        )
        if messages:
            process_batch(conn, client, stats, name, listeners, messages)
        if next_id == "0-0" or not messages:
            return
        start = next_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=f"w{os.getpid()}")
    args = parser.parse_args()
    name = args.name
    client = redis_client()
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    stats = client.register_script(STATS_LUA)
    listeners = Listeners(conn, client)
    ensure_group(client)
    claim_stale(conn, client, stats, name, listeners)
    clock = {"log": time.time(), "claim": time.time()}
    log.info("worker %s reading %s", name, STREAM)
    while True:
        try:
            step(conn, client, stats, name, listeners, clock)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            # Unacknowledged messages stay pending and are reclaimed after 30 s.
            log.warning("%s lost redis (%s); retrying", name, exc)
            time.sleep(2)


def step(conn, client, stats, name: str, listeners: Listeners, clock: dict) -> None:
    if client.get("config:paused") == "1":
        client.hset("stats:worker_busy", name, "0")
        time.sleep(0.1)
        return
    try:
        resp = client.xreadgroup(GROUP, name, {STREAM: ">"}, count=50, block=1000)
    except redis.ResponseError as exc:
        text = str(exc)
        if "NOGROUP" in text or "no such key" in text.lower():
            ensure_group(client)
            return
        raise
    if resp:
        for _stream, messages in resp:
            process_batch(conn, client, stats, name, listeners, messages)
    else:
        stats(args=[STREAM, 0, 0, 0, f"{time.time():.3f}"])
    now = time.time()
    if now - clock["claim"] >= 30:
        clock["claim"] = now
        claim_stale(conn, client, stats, name, listeners)
    if now - clock["log"] >= 5:
        clock["log"] = now
        log.info(
            "%s posts=%s writes=%s lag=%s peak=%s",
            name,
            client.get("stats:posts") or 0,
            client.get("stats:feed_writes") or 0,
            client.get("stats:backlog") or 0,
            client.get("stats:backlog_peak") or 0,
        )


if __name__ == "__main__":
    main()
