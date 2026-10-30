"""Drop posts and Redis feeds, then select the strategy for the next run."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg
from backend.config import DATABASE_URL, hybrid_threshold, redis_client, strategy_name
from backend.maintenance import reset_storage


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default=strategy_name())
    parser.add_argument("--threshold", type=int, default=hybrid_threshold())
    args = parser.parse_args()
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    client = redis_client()
    reset_storage(conn, client, args.strategy, args.threshold)
    conn.close()
    client.close()
    print(f"flushed posts and feeds; strategy={args.strategy} threshold={args.threshold}")


if __name__ == "__main__":
    main()
