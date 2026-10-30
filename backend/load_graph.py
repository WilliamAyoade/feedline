"""Load the follower graph. Column 1 follows column 2 unless the heavy tail says otherwise."""

from __future__ import annotations

import gzip
import json
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg

from backend.activity import busiest_window, find_activity, find_edges, load_events
from backend.config import DATABASE_URL, ROOT as PROJECT_ROOT


def _open(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open("rt")


def main() -> None:
    data_dir = Path(sys.argv[1] if len(sys.argv) > 1 else PROJECT_ROOT / "data")
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    edge_path = find_edges(data_dir)
    started = time.time()
    print(f"reading {edge_path.name}", flush=True)

    count_a: Counter[int] = Counter()
    count_b: Counter[int] = Counter()
    staged = 0
    self_loops = 0
    conn = psycopg.connect(DATABASE_URL, autocommit=False)
    with conn.cursor() as cur:
        cur.execute("SET work_mem = '256MB'")
        cur.execute("SET maintenance_work_mem = '512MB'")
        cur.execute("DROP TABLE IF EXISTS posts, follows, users, staging CASCADE")
        cur.execute("CREATE TABLE staging (a BIGINT NOT NULL, b BIGINT NOT NULL)")
        with cur.copy("COPY staging (a, b) FROM STDIN") as copy:
            batch: list[str] = []
            with _open(edge_path) as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split()
                    a = int(parts[0])
                    b = int(parts[1])
                    if a == b:
                        self_loops += 1
                        continue
                    count_a[a] += 1
                    count_b[b] += 1
                    batch.append(f"{a}\t{b}\n")
                    staged += 1
                    if len(batch) >= 200_000:
                        copy.write("".join(batch))
                        batch.clear()
                        if staged % 2_000_000 == 0:
                            print(f"  copied {staged:,}", flush=True)
            if batch:
                copy.write("".join(batch))
        print(f"copied {staged:,} edges", flush=True)
        max_a = count_a.most_common(1)[0][1] if count_a else 0
        max_b = count_b.most_common(1)[0][1] if count_b else 0
        # The column whose ids repeat the most is the account being followed.
        if max_b >= max_a:
            direction = "column 1 follows column 2"
            cur.execute(
                """
                CREATE TABLE follows (
                    follower BIGINT NOT NULL,
                    followee BIGINT NOT NULL
                )
                """
            )
            cur.execute("INSERT INTO follows (follower, followee) SELECT DISTINCT a, b FROM staging")
        else:
            direction = "column 2 follows column 1"
            cur.execute(
                """
                CREATE TABLE follows (
                    follower BIGINT NOT NULL,
                    followee BIGINT NOT NULL
                )
                """
            )
            cur.execute("INSERT INTO follows (follower, followee) SELECT DISTINCT b, a FROM staging")
        cur.execute("SELECT count(*) FROM follows")
        follows = int(cur.fetchone()[0])
        duplicates = staged - follows
        cur.execute(
            """
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                follower_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            INSERT INTO users (id, follower_count)
            SELECT followee, COUNT(*)::int
            FROM follows
            GROUP BY followee
            """
        )
        cur.execute(
            """
            INSERT INTO users (id, follower_count)
            SELECT DISTINCT follower, 0
            FROM follows
            ON CONFLICT (id) DO NOTHING
            """
        )
        cur.execute("ALTER TABLE follows ADD PRIMARY KEY (follower, followee)")
        cur.execute("CREATE INDEX follows_followee_idx ON follows (followee, follower)")
        cur.execute(
            """
            CREATE TABLE posts (
                id BIGSERIAL PRIMARY KEY,
                author BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                text TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX posts_author_created_idx ON posts (author, created_at DESC, id DESC)"
        )
        cur.execute("DROP TABLE staging")
        cur.execute("ANALYZE users")
        cur.execute("ANALYZE follows")
        cur.execute("SELECT count(*) FROM users")
        users = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY follower_count),
                   max(follower_count)
            FROM users
            """
        )
        p99, maximum = cur.fetchone()
        cur.execute(
            """
            SELECT id, follower_count
            FROM users
            ORDER BY follower_count DESC, id
            LIMIT 5
            """
        )
        top = [(int(row[0]), int(row[1])) for row in cur.fetchall()]
    conn.commit()

    activity_rows = 0
    busy_start = None
    busy_end = None
    busy_posts = 0
    announcement_posts = 0
    push_writes = 0
    worst_post = 0
    worst_author = None
    activity_path = find_activity(data_dir)
    if activity_path is not None:
        print(f"reading {activity_path.name}", flush=True)
        events = load_events(activity_path)
        activity_rows = len(events)
        busy_start, busy_end, busy_posts = busiest_window(events)
        announce_lo = 1341388800  # 2012-07-04 08:00:00 UTC
        announce_hi = 1341396000  # 2012-07-04 10:00:00 UTC
        with conn.cursor() as cur:
            cur.execute("SELECT id, follower_count FROM users")
            follower_counts = {int(row[0]): int(row[1]) for row in cur.fetchall()}
        for event in events:
            degree = follower_counts.get(event.user_a, 0)
            push_writes += degree
            if degree > worst_post:
                worst_post = degree
                worst_author = event.user_a
            if announce_lo <= event.ts < announce_hi:
                announcement_posts += 1
    conn.close()

    print(f"users {users:,}")
    print(f"follows {follows:,}")
    print(f"self-follows dropped {self_loops:,}")
    print(f"duplicate edges dropped {duplicates:,}")
    print(f"direction: {direction}")
    print(f"max followers {int(maximum):,}")
    print(f"p99 followers {int(p99):,}")
    print("top 5 follower counts:")
    for user_id, count in top:
        print(f"  {user_id}  {count:,}")
    if activity_rows:
        from datetime import datetime, timezone

        start = datetime.fromtimestamp(busy_start, tz=timezone.utc)
        print(f"activity rows {activity_rows:,}")
        print(f"busiest 10 min: {busy_posts:,} posts starting {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"announcement window 2012-07-04 08:00-10:00 UTC: {announcement_posts:,} posts")
        print(f"estimated push feed writes {push_writes:,} worst post {worst_post:,} author {worst_author}")

    out_dir = PROJECT_ROOT / "results"
    out_dir.mkdir(exist_ok=True)
    payload = {
        "users": users,
        "follows": follows,
        "self_follows_dropped": self_loops,
        "duplicate_edges_dropped": duplicates,
        "direction": direction,
        "max_followers": int(maximum),
        "p99_followers": int(p99),
        "top5": [{"id": user_id, "followers": count} for user_id, count in top],
        "activity_rows": activity_rows,
        "busy_start": busy_start,
        "busy_end": busy_end,
        "busy_posts": busy_posts,
        "announcement_posts": announcement_posts,
        "estimated_push_writes": push_writes,
        "estimated_worst_post": worst_post,
        "estimated_worst_author": worst_author,
        "edge_file": edge_path.name,
        "seconds": round(time.time() - started, 1),
    }
    (out_dir / "m1.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote results/m1.json in {payload['seconds']}s")


if __name__ == "__main__":
    main()
