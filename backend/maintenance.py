from __future__ import annotations

import time

from backend.config import STREAM
from backend.store import ensure_group, workers_busy


def _delete_match(redis, pattern: str) -> int:
    removed = 0
    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=pattern, count=1000)
        if keys:
            removed += int(redis.delete(*keys))
        if cursor == 0:
            break
    return removed


def reset_storage(conn, redis, strategy: str, threshold: int) -> None:
    redis.set("config:paused", "1")
    deadline = time.time() + 120
    while time.time() < deadline:
        if not workers_busy(redis):
            break
        time.sleep(0.1)
    # The blocked read can take up to a second to notice the pause flag.
    time.sleep(1.2)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE posts RESTART IDENTITY")
    if not getattr(conn, "autocommit", False):
        conn.commit()
    for pattern in ("feed:*", "pending:*", "stats:*", "replay:*"):
        _delete_match(redis, pattern)
    redis.delete("listeners", STREAM, "loadtest:ready")
    try:
        redis.xgroup_destroy(STREAM, "workers")
    except Exception:
        pass
    ensure_group(redis)
    redis.set("config:strategy", strategy)
    redis.set("config:threshold", str(threshold))
    redis.set("config:paused", "0")
