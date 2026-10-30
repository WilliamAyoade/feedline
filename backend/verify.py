"""Compare each strategy's feed with the pull query for 1,000 sampled users."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg
from backend.config import DATABASE_URL, ROOT as PROJECT_ROOT, redis_client
from backend.store import active_strategy, pull_read, workers_busy


def wait_until_idle(client, timeout: float = 3600) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        lag = int(client.get("stats:backlog") or 0)
        busy = workers_busy(client)
        if lag == 0 and not busy:
            return
        time.sleep(0.5)


def sample_users(conn, n: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT follower FROM (
                SELECT DISTINCT follower FROM follows
            ) s
            ORDER BY random()
            LIMIT %s
            """,
            (n,),
        )
        return [int(row[0]) for row in cur.fetchall()]


def explain(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT follower FROM follows
            GROUP BY follower
            ORDER BY count(*) DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            return
        user_id = int(row[0])
        lateral = """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT p.id, p.author, p.created_at, p.text
            FROM follows f
            JOIN LATERAL (
                SELECT id, author, created_at, text
                FROM posts
                WHERE author = f.followee
                ORDER BY created_at DESC, id DESC
                LIMIT 50
            ) p ON TRUE
            WHERE f.follower = %s
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT 50
        """
        plain = """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT p.id, p.author, p.created_at, p.text
            FROM posts p
            JOIN follows f ON p.author = f.followee
            WHERE f.follower = %s
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT 50
        """
        sections = [f"heavy reader {user_id}\n"]
        for title, sql in (("lateral pull", lateral), ("spec join", plain)):
            cur.execute("SET statement_timeout = '120s'")
            try:
                cur.execute(sql, (user_id,))
                plan = "\n".join(line[0] for line in cur.fetchall())
            except Exception as exc:
                plan = f"failed: {exc}"
                if not conn.autocommit:
                    conn.rollback()
            sections.append(f"## {title}\n{plan}\n")
        cur.execute("SET statement_timeout = 0")
    path = PROJECT_ROOT / "results" / "explain.txt"
    path.parent.mkdir(exist_ok=True)
    path.write_text("\n".join(sections))
    print(f"wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=1000)
    parser.add_argument("--results", default="")
    parser.add_argument("--explain", action="store_true")
    args = parser.parse_args()
    client = redis_client()
    wait_until_idle(client)
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    if args.explain:
        explain(conn)
    strategy = active_strategy(client)
    name, threshold = strategy.name, getattr(strategy, "threshold", None)
    print(f"verifying {name} threshold={threshold} sample={args.sample}", flush=True)
    users = sample_users(conn, args.sample)
    mismatches = []
    for index, user_id in enumerate(users, start=1):
        got = [post.id for post in strategy.read(conn, client, user_id, 50, None)]
        truth = [post.id for post in pull_read(conn, user_id, 50, None)]
        if got != truth:
            mismatches.append({"user": user_id, "got": got, "truth": truth})
        if index % 100 == 0:
            print(f"  {index}/{len(users)} mismatches {len(mismatches)}", flush=True)
    matched = len(users) - len(mismatches)
    print(f"match {matched}/{len(users)}")
    if args.results:
        path = Path(args.results)
        payload = json.loads(path.read_text()) if path.exists() else {}
        payload["match"] = f"{matched}/{len(users)}"
        payload["match_ok"] = matched
        payload["match_n"] = len(users)
        path.write_text(json.dumps(payload, indent=2) + "\n")
    if mismatches:
        out = PROJECT_ROOT / "results" / f"mismatches-{name}.json"
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(mismatches[:8], indent=2) + "\n")
        print(f"wrote {out}")
        raise SystemExit(1)
    conn.close()
    client.close()
    return


if __name__ == "__main__":
    main()
