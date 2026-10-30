"""Read the Higgs activity file and locate its busiest 10 minutes."""

from __future__ import annotations

import gzip
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Event:
    user_a: int
    user_b: int
    ts: int
    interaction: str

    @property
    def text(self) -> str:
        # Authors and timestamps are real. The text is a label, not a tweet.
        return f"{self.interaction} @{self.user_b}"

    @property
    def created_at(self) -> datetime:
        return datetime.fromtimestamp(self.ts, tz=timezone.utc)


def find_edges(data_dir: Path) -> Path:
    for name in (
        "higgs-social_network.edgelist.gz",
        "higgs-social_network.edgelist",
        "twitter_combined.txt.gz",
        "twitter_combined.txt",
    ):
        path = data_dir / name
        if path.exists():
            return path
    raise SystemExit(f"no edge list in {data_dir}")


def find_activity(data_dir: Path) -> Path | None:
    for name in ("higgs-activity_time.txt.gz", "higgs-activity_time.txt"):
        path = data_dir / name
        if path.exists():
            return path
    return None


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt")
    return path.open("rt")


def load_events(path: Path) -> list[Event]:
    events: list[Event] = []
    with _open_text(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            user_a, user_b, ts, interaction = line.split()
            events.append(Event(int(user_a), int(user_b), int(ts), interaction))
    events.sort(key=lambda event: (event.ts, event.user_a, event.user_b, event.interaction))
    return events


def busiest_window(events: list[Event], seconds: int = 600) -> tuple[int, int, int]:
    """Return (start_ts, end_ts, count) for the busiest window of `seconds`."""
    if not events:
        return (0, 0, 0)
    stamps = [event.ts for event in events]
    left = 0
    best = 1
    best_left = 0
    best_right = 0
    for right, stamp in enumerate(stamps):
        while stamp - stamps[left] > seconds:
            left += 1
        width = right - left + 1
        if width > best:
            best = width
            best_left = left
            best_right = right
    return stamps[best_left], stamps[best_right], best


def synthetic_events(conn, n: int = 20000, seed: int = 1) -> list[Event]:
    """Posts with authors drawn proportional to follower count. Used only if Higgs activity is missing."""
    random.seed(seed)
    with conn.cursor() as cur:
        cur.execute("SELECT id, follower_count FROM users WHERE follower_count > 0")
        rows = cur.fetchall()
    if not rows:
        raise SystemExit("cannot synthesize posts: users table is empty")
    ids = [int(row[0]) for row in rows]
    weights = [int(row[1]) for row in rows]
    start = 1341100972
    events: list[Event] = []
    for i in range(n):
        author = random.choices(ids, weights=weights, k=1)[0]
        target = ids[i % len(ids)]
        events.append(Event(author, target, start + i, "RT"))
    return events
