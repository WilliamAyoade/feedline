from datetime import datetime, timedelta, timezone

from backend.store import PullStrategy, PushStrategy, follow_user, insert_post
from tests.conftest import HIGH, cleanup


def _collect(strategy, conn, rdb, reader: int, page_size: int) -> list[int]:
    before = None
    seen: list[int] = []
    while True:
        page = strategy.read(conn, rdb, reader, page_size, before)
        if not page:
            break
        seen.extend(post.id for post in page)
        if len(page) < page_size:
            break
        before = str(page[-1].id)
    return seen


def _seed(conn, rdb, strategy, follower: int, author: int, stamps: list[datetime]):
    conn.execute(
        "INSERT INTO users (id, follower_count) VALUES (%s, 0), (%s, 4)",
        (follower, author),
    )
    conn.execute(
        "INSERT INTO follows (follower, followee) VALUES (%s, %s)",
        (follower, author),
    )
    ids = []
    for stamp in stamps:
        post, _count = insert_post(conn, author, "page", stamp)
        ids.append(post.id)
    if isinstance(strategy, PushStrategy):
        follow_user(conn, rdb, strategy, follower, author)
        # The row already exists; backfill happens only on insert. Write feeds directly.
        from backend.store import add_posts_to_feed, fetch_by_ids

        add_posts_to_feed(rdb, follower, fetch_by_ids(conn, ids))
    return ids


def test_cursor_pagination_has_no_gaps_or_repeats(conn, rdb):
    follower, author = HIGH + 41, HIGH + 42
    base = datetime(2012, 7, 6, tzinfo=timezone.utc)
    try:
        expected = _seed(
            conn,
            rdb,
            PullStrategy(),
            follower,
            author,
            [base + timedelta(seconds=i) for i in range(120)],
        )
        got = _collect(PullStrategy(), conn, rdb, follower, 50)
        assert len(got) == len(set(got)) == 120
        assert got == list(reversed(expected))
    finally:
        cleanup(conn, rdb, [follower, author])


def test_push_cursor_pagination_and_timestamp_ties(conn, rdb):
    follower, author = HIGH + 43, HIGH + 44
    base = datetime(2012, 7, 6, 1, tzinfo=timezone.utc)
    tie = base + timedelta(seconds=5)
    stamps = [base + timedelta(seconds=i) for i in range(4)] + [tie, tie, tie]
    try:
        expected = _seed(conn, rdb, PushStrategy(), follower, author, stamps)
        got = _collect(PushStrategy(), conn, rdb, follower, 2)
        assert len(got) == len(set(got)) == len(expected)
        assert set(got) == set(expected)
        ordered = list(reversed(expected[:4]))
        tie_ids = sorted(expected[4:], reverse=True)
        assert got == tie_ids + ordered
    finally:
        cleanup(conn, rdb, [follower, author])
