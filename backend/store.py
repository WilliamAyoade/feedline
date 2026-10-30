"""Push, pull, and hybrid feed strategies.

`post` writes a post into follower feeds. `read` returns one reader's feed,
newest first. The pull query is the ground truth the other strategies must match.
"""

from __future__ import annotations

import heapq
import json
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from backend.config import BATCH, GROUP, KEEP, SCHEMA_PATH, STREAM, hybrid_threshold, strategy_name

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# One round trip updates this many follower feeds, then trims each to KEEP.
FANOUT_LUA = """
local keep = tonumber(ARGV[1])
local score = tonumber(ARGV[2])
local member = ARGV[3]
for i = 1, #KEYS do
  redis.call('ZADD', KEYS[i], score, member)
  local n = redis.call('ZCARD', KEYS[i])
  if n > keep then
    redis.call('ZREMRANGEBYRANK', KEYS[i], 0, n - keep - 1)
  end
end
return #KEYS
"""

_scripts: dict[int, Any] = {}


def epoch_us(dt: datetime) -> int:
    """Microseconds since the Unix epoch, exact for timezone-aware datetimes."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    delta = dt - EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def parse_iso(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass(frozen=True)
class Post:
    id: int
    author: int
    created_at: datetime
    text: str


def is_older(post: Post, before: tuple[datetime, int]) -> bool:
    ts, pid = before
    return post.created_at < ts or (post.created_at == ts and post.id < pid)


def kway_merge(lists: Sequence[Sequence[Post]], limit: int) -> list[Post]:
    """Merge newest-first lists into the `limit` newest posts. Ties break on higher id."""
    heap: list[tuple[int, int, int, int]] = []
    for li, posts in enumerate(lists):
        if not posts:
            continue
        post = posts[0]
        heapq.heappush(heap, (-epoch_us(post.created_at), -post.id, li, 0))
    out: list[Post] = []
    seen: set[int] = set()
    while heap and len(out) < limit:
        _us, _pid, li, idx = heapq.heappop(heap)
        post = lists[li][idx]
        if post.id not in seen:
            seen.add(post.id)
            out.append(post)
        nxt = idx + 1
        if nxt < len(lists[li]):
            post = lists[li][nxt]
            heapq.heappush(heap, (-epoch_us(post.created_at), -post.id, li, nxt))
    return out


def ensure_schema(conn) -> None:
    statements: list[str] = []
    buf: list[str] = []
    for line in SCHEMA_PATH.read_text().splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(buf))
            buf = []
    if buf and "".join(buf).strip():
        statements.append("\n".join(buf))
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    if not getattr(conn, "autocommit", False):
        conn.commit()


def _script(redis):
    cached = _scripts.get(id(redis))
    if cached is None:
        cached = redis.register_script(FANOUT_LUA)
        _scripts[id(redis)] = cached
    return cached


def fanout_feeds(redis, followers: list[int], post: Post) -> int:
    """Copy one post into each follower's sorted set, in batches of 1,000, trimmed to 800."""
    if not followers:
        return 0
    script = _script(redis)
    score = float(epoch_us(post.created_at))
    member = str(post.id)
    for offset in range(0, len(followers), BATCH):
        chunk = followers[offset : offset + BATCH]
        keys = [f"feed:{uid}" for uid in chunk]
        script(keys=keys, args=[KEEP, score, member])
    return len(followers)


def add_posts_to_feed(redis, user_id: int, posts: list[Post]) -> None:
    if not posts:
        return
    key = f"feed:{user_id}"
    mapping = {str(post.id): float(epoch_us(post.created_at)) for post in posts}
    redis.zadd(key, mapping)
    size = int(redis.zcard(key))
    if size > KEEP:
        redis.zremrangebyrank(key, 0, size - KEEP - 1)


def remove_author_posts(conn, redis, follower: int, author: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM posts WHERE author = %s", (author,))
        ids = [str(row[0]) for row in cur.fetchall()]
    if not ids:
        return
    key = f"feed:{follower}"
    for offset in range(0, len(ids), BATCH):
        redis.zrem(key, *ids[offset : offset + BATCH])


def fetch_follower_ids(conn, followee: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT follower FROM follows WHERE followee = %s", (followee,))
        return [int(row[0]) for row in cur.fetchall()]


def fetch_by_ids(conn, ids: list[int]) -> list[Post]:
    if not ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, author, created_at, text
            FROM posts
            WHERE id = ANY(%s)
            ORDER BY created_at DESC, id DESC
            """,
            (ids,),
        )
        return [Post(int(row[0]), int(row[1]), row[2], row[3]) for row in cur.fetchall()]


def resolve_before(conn, before: str | None) -> tuple[datetime, int] | None:
    if before is None or before == "":
        return None
    post_id = int(before)
    with conn.cursor() as cur:
        cur.execute("SELECT created_at, id FROM posts WHERE id = %s", (post_id,))
        row = cur.fetchone()
    if row is None:
        raise LookupError(before)
    return row[0], int(row[1])


def posts_from_feed(conn, redis, user: int, limit: int, before: tuple[datetime, int] | None) -> list[Post]:
    raw = redis.zrevrangebyscore(f"feed:{user}", "+inf", "-inf", start=0, num=KEEP)
    if not raw:
        return []
    posts = fetch_by_ids(conn, [int(item) for item in raw])
    if before is not None:
        posts = [post for post in posts if is_older(post, before)]
    posts.sort(key=lambda post: (post.created_at, post.id), reverse=True)
    return posts[:limit]


def pull_read(conn, user: int, limit: int, before: tuple[datetime, int] | None) -> list[Post]:
    """Newest posts across followees. Lateral index scans, same rows as the plain join."""
    if before is None:
        sql = """
            SELECT p.id, p.author, p.created_at, p.text
            FROM follows f
            JOIN LATERAL (
                SELECT id, author, created_at, text
                FROM posts
                WHERE author = f.followee
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            ) p ON TRUE
            WHERE f.follower = %s
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT %s
        """
        params: tuple = (limit, user, limit)
    else:
        sql = """
            SELECT p.id, p.author, p.created_at, p.text
            FROM follows f
            JOIN LATERAL (
                SELECT id, author, created_at, text
                FROM posts
                WHERE author = f.followee
                  AND (created_at, id) < (%s, %s)
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            ) p ON TRUE
            WHERE f.follower = %s
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT %s
        """
        params = (before[0], before[1], limit, user, limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [Post(int(row[0]), int(row[1]), row[2], row[3]) for row in cur.fetchall()]


def spec_join_read(conn, user: int, limit: int, before: tuple[datetime, int] | None) -> list[Post]:
    """The pull query from the spec, plus id as a stable tie break."""
    if before is None:
        sql = """
            SELECT p.id, p.author, p.created_at, p.text
            FROM posts p
            JOIN follows f ON p.author = f.followee
            WHERE f.follower = %s
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT %s
        """
        params: tuple = (user, limit)
    else:
        sql = """
            SELECT p.id, p.author, p.created_at, p.text
            FROM posts p
            JOIN follows f ON p.author = f.followee
            WHERE f.follower = %s
              AND (p.created_at, p.id) < (%s, %s)
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT %s
        """
        params = (user, before[0], before[1], limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [Post(int(row[0]), int(row[1]), row[2], row[3]) for row in cur.fetchall()]


def celebrity_followees(conn, user: int, threshold: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.id
            FROM follows f
            JOIN users u ON u.id = f.followee
            WHERE f.follower = %s AND u.follower_count >= %s
            ORDER BY u.id
            """,
            (user, threshold),
        )
        return [int(row[0]) for row in cur.fetchall()]


def latest_by_authors(
    conn,
    authors: list[int],
    limit: int,
    before: tuple[datetime, int] | None,
) -> dict[int, list[Post]]:
    if not authors:
        return {}
    if before is None:
        sql = """
            SELECT id, author, created_at, text
            FROM (
                SELECT id, author, created_at, text,
                       ROW_NUMBER() OVER (
                           PARTITION BY author ORDER BY created_at DESC, id DESC
                       ) AS rn
                FROM posts
                WHERE author = ANY(%s)
            ) t
            WHERE rn <= %s
            ORDER BY author, created_at DESC, id DESC
        """
        params: tuple = (authors, limit)
    else:
        sql = """
            SELECT id, author, created_at, text
            FROM (
                SELECT id, author, created_at, text,
                       ROW_NUMBER() OVER (
                           PARTITION BY author ORDER BY created_at DESC, id DESC
                       ) AS rn
                FROM posts
                WHERE author = ANY(%s)
                  AND (created_at, id) < (%s, %s)
            ) t
            WHERE rn <= %s
            ORDER BY author, created_at DESC, id DESC
        """
        params = (authors, before[0], before[1], limit)
    grouped: dict[int, list[Post]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            grouped[int(row[1])].append(Post(int(row[0]), int(row[1]), row[2], row[3]))
    return grouped


class FeedStrategy(ABC):
    name: str

    @abstractmethod
    def should_fanout(self, follower_count: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def post(self, conn, redis, post: Post, follower_count: int) -> int:
        """Write the post into follower feeds. Returns the number of feed writes."""
        raise NotImplementedError

    @abstractmethod
    def read(self, conn, redis, user: int, limit: int, before: str | None) -> list[Post]:
        raise NotImplementedError


class PushStrategy(FeedStrategy):
    name = "push"

    def should_fanout(self, follower_count: int) -> bool:
        return True

    def post(self, conn, redis, post: Post, follower_count: int) -> int:
        return fanout_feeds(redis, fetch_follower_ids(conn, post.author), post)

    def read(self, conn, redis, user: int, limit: int, before: str | None) -> list[Post]:
        return posts_from_feed(conn, redis, user, limit, resolve_before(conn, before))


class PullStrategy(FeedStrategy):
    name = "pull"

    def should_fanout(self, follower_count: int) -> bool:
        return False

    def post(self, conn, redis, post: Post, follower_count: int) -> int:
        return 0

    def read(self, conn, redis, user: int, limit: int, before: str | None) -> list[Post]:
        return pull_read(conn, user, limit, resolve_before(conn, before))


class HybridStrategy(FeedStrategy):
    name = "hybrid"

    def __init__(self, threshold: int):
        self.threshold = threshold

    def should_fanout(self, follower_count: int) -> bool:
        return follower_count < self.threshold

    def post(self, conn, redis, post: Post, follower_count: int) -> int:
        if not self.should_fanout(follower_count):
            return 0
        return fanout_feeds(redis, fetch_follower_ids(conn, post.author), post)

    def read(self, conn, redis, user: int, limit: int, before: str | None) -> list[Post]:
        bound = resolve_before(conn, before)
        base = posts_from_feed(conn, redis, user, limit, bound)
        celebs = celebrity_followees(conn, user, self.threshold)
        grouped = latest_by_authors(conn, celebs, limit, bound)
        lists: list[list[Post]] = [base]
        for celeb in celebs:
            posts = grouped.get(celeb)
            if posts:
                lists.append(posts)
        return kway_merge(lists, limit)


def make_strategy(name: str, threshold: int | None = None) -> FeedStrategy:
    cleaned = name.strip().lower()
    if cleaned == "push":
        return PushStrategy()
    if cleaned == "pull":
        return PullStrategy()
    if cleaned == "hybrid":
        return HybridStrategy(hybrid_threshold() if threshold is None else threshold)
    raise ValueError(f"unknown strategy {name}")


def read_config(redis) -> tuple[str, int]:
    name, raw = redis.mget("config:strategy", "config:threshold")
    name = name or strategy_name()
    threshold = int(raw) if raw is not None else hybrid_threshold()
    return str(name), threshold


def active_strategy(redis) -> FeedStrategy:
    name, threshold = read_config(redis)
    return make_strategy(name, threshold)


def insert_post(conn, author: int, text: str, created_at: datetime) -> tuple[Post, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO posts (author, created_at, text)
            VALUES (%s, %s, %s)
            RETURNING id, created_at,
                (SELECT follower_count FROM users WHERE id = %s)
            """,
            (author, created_at, text, author),
        )
        post_id, stored_at, count = cur.fetchone()
    return Post(int(post_id), author, stored_at, text), int(count or 0)


def enqueue_post(redis, post: Post, follower_count: int, accepted_at: datetime, name: str, threshold: int) -> None:
    redis.xadd(
        STREAM,
        {
            "id": str(post.id),
            "author": str(post.author),
            "text": post.text,
            "created_at": iso(post.created_at),
            "accepted_at": iso(accepted_at),
            "follower_count": str(follower_count),
            "strategy": name,
            "threshold": str(threshold),
        },
    )


def listener_ids(redis) -> list[int]:
    return [int(item) for item in redis.smembers("listeners")]


def followers_among(conn, author: int, listeners: list[int]) -> list[int]:
    if not listeners:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT follower FROM follows
            WHERE followee = %s AND follower = ANY(%s)
            """,
            (author, listeners),
        )
        return [int(row[0]) for row in cur.fetchall()]


def following_of(conn, listeners: list[int]) -> dict[int, list[int]]:
    """Map each followee to the listeners who follow it."""
    out: dict[int, list[int]] = {}
    if not listeners:
        return out
    with conn.cursor() as cur:
        cur.execute(
            "SELECT followee, follower FROM follows WHERE follower = ANY(%s)",
            (listeners,),
        )
        for followee, follower in cur.fetchall():
            out.setdefault(int(followee), []).append(int(follower))
    return out


def workers_busy(redis) -> bool:
    return "1" in redis.hvals("stats:worker_busy")


def queue_live(pipe, targets: list[int], post: Post, accepted_at: datetime) -> None:
    """Add PUBLISH and catch-up list writes for `targets` to a pipeline."""
    delivered = datetime.now(timezone.utc)
    payload = json.dumps(
        {
            "id": post.id,
            "author": post.author,
            "text": post.text,
            "created_at": iso(post.created_at),
            "accepted_at": iso(accepted_at),
            "delivered_at": iso(delivered),
            "replayed": False,
        },
        separators=(",", ":"),
    )
    for uid in targets:
        pipe.publish(f"live:{uid}", payload)
        pipe.lpush(f"pending:{uid}", payload)
        pipe.ltrim(f"pending:{uid}", 0, 199)


def publish_live(redis, targets: list[int], post: Post, accepted_at: datetime) -> None:
    if not targets:
        return
    delivered = datetime.now(timezone.utc)
    payload = json.dumps(
        {
            "id": post.id,
            "author": post.author,
            "text": post.text,
            "created_at": iso(post.created_at),
            "accepted_at": iso(accepted_at),
            "delivered_at": iso(delivered),
            "replayed": False,
        },
        separators=(",", ":"),
    )
    pipe = redis.pipeline(transaction=False)
    for uid in targets:
        pipe.publish(f"live:{uid}", payload)
        pipe.lpush(f"pending:{uid}", payload)
        pipe.ltrim(f"pending:{uid}", 0, 199)
    pipe.execute()


def deliver_post(conn, redis, strategy: FeedStrategy, post: Post, follower_count: int, accepted_at: datetime) -> int:
    writes = strategy.post(conn, redis, post, follower_count)
    targets = followers_among(conn, post.author, listener_ids(redis))
    publish_live(redis, targets, post, accepted_at)
    return writes


def follow_user(conn, redis, strategy: FeedStrategy, follower: int, followee: int) -> dict:
    if follower == followee:
        raise ValueError("cannot follow yourself")
    posts: list[Post] = []
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, follower_count) VALUES (%s, 0), (%s, 0)
                ON CONFLICT (id) DO NOTHING
                """,
                (follower, followee),
            )
            cur.execute(
                """
                INSERT INTO follows (follower, followee) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (follower, followee),
            )
            inserted = cur.rowcount == 1
            if inserted:
                cur.execute(
                    """
                    UPDATE users
                    SET follower_count = follower_count + 1
                    WHERE id = %s
                    RETURNING follower_count
                    """,
                    (followee,),
                )
                count = int(cur.fetchone()[0])
            else:
                cur.execute("SELECT follower_count FROM users WHERE id = %s", (followee,))
                count = int(cur.fetchone()[0])
            if inserted and strategy.should_fanout(count):
                cur.execute(
                    """
                    SELECT id, author, created_at, text
                    FROM posts
                    WHERE author = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (followee, KEEP),
                )
                posts = [Post(int(row[0]), int(row[1]), row[2], row[3]) for row in cur.fetchall()]
    if posts:
        add_posts_to_feed(redis, follower, posts)
    return {"following": True, "created": inserted, "follower_count": count}


def unfollow_user(conn, redis, follower: int, followee: int) -> dict:
    if follower == followee:
        raise ValueError("cannot unfollow yourself")
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM follows WHERE follower = %s AND followee = %s",
                (follower, followee),
            )
            deleted = cur.rowcount == 1
            count = 0
            if deleted:
                cur.execute(
                    """
                    UPDATE users
                    SET follower_count = GREATEST(follower_count - 1, 0)
                    WHERE id = %s
                    RETURNING follower_count
                    """,
                    (followee,),
                )
                row = cur.fetchone()
                count = int(row[0]) if row else 0
            else:
                cur.execute("SELECT follower_count FROM users WHERE id = %s", (followee,))
                row = cur.fetchone()
                count = int(row[0]) if row else 0
    if deleted:
        remove_author_posts(conn, redis, follower, followee)
    return {"following": False, "removed": deleted, "follower_count": count}


def featured_users(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, follower_count FROM users
            ORDER BY follower_count DESC, id
            LIMIT 1
            """
        )
        top = cur.fetchone()
        if top is None:
            return {"author": None, "reader": None, "average": None, "median_followers": 0}
        cur.execute(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY follower_count) FROM users"
        )
        median = float(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT u.id, u.follower_count
            FROM follows f
            JOIN users u ON u.id = f.follower
            WHERE f.followee = %s
            ORDER BY abs(u.follower_count - %s), u.id
            LIMIT 1
            """,
            (top[0], median),
        )
        reader = cur.fetchone()
        cur.execute(
            """
            SELECT id, follower_count FROM users
            ORDER BY abs(follower_count - %s), id
            LIMIT 1
            """,
            (median,),
        )
        average = cur.fetchone()

    def pack(row, label: str):
        if row is None:
            return None
        return {"id": int(row[0]), "follower_count": int(row[1]), "label": label}

    return {
        "author": pack(top, "Most followed"),
        "reader": pack(reader, "Typical reader"),
        "average": pack(average, "Median account"),
        "median_followers": median,
    }


def user_profile(conn, user_id: int, viewer: int | None) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.id, u.follower_count,
                   (SELECT count(*) FROM follows WHERE follower = u.id)
            FROM users u
            WHERE u.id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        viewer_follows = None
        if viewer is not None:
            cur.execute(
                "SELECT 1 FROM follows WHERE follower = %s AND followee = %s",
                (viewer, user_id),
            )
            viewer_follows = cur.fetchone() is not None
    return {
        "id": int(row[0]),
        "follower_count": int(row[1]),
        "following_count": int(row[2]),
        "viewer_follows": viewer_follows,
    }


def lookup_following(conn, follower: int, followees: list[int]) -> list[int]:
    if not followees:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT followee FROM follows
            WHERE follower = %s AND followee = ANY(%s)
            """,
            (follower, followees),
        )
        return [int(row[0]) for row in cur.fetchall()]


def post_dict(post: Post) -> dict:
    return {
        "id": post.id,
        "author": post.author,
        "created_at": iso(post.created_at),
        "text": post.text,
    }


def ensure_group(redis) -> None:
    try:
        redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise
