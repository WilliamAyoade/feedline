from datetime import datetime, timedelta, timezone

from backend.store import (
    HybridStrategy,
    Post,
    PullStrategy,
    PushStrategy,
    insert_post,
    pull_read,
    join_read,
)
from tests.conftest import HIGH, cleanup

BASE = datetime(2012, 7, 2, tzinfo=timezone.utc)


def test_hybrid_threshold_routing():
    hybrid = HybridStrategy(1000)
    assert hybrid.should_fanout(999)
    assert not hybrid.should_fanout(1000)
    assert not hybrid.should_fanout(1001)
    assert PushStrategy().should_fanout(10**9)
    assert not PullStrategy().should_fanout(0)


def test_hybrid_does_not_fanout_celebrities(conn, rdb):
    reader, normal, celeb = HIGH + 11, HIGH + 12, HIGH + 13
    try:
        conn.execute(
            "INSERT INTO users (id, follower_count) VALUES (%s, 0), (%s, 10), (%s, 5000)",
            (reader, normal, celeb),
        )
        conn.execute(
            "INSERT INTO follows (follower, followee) VALUES (%s, %s), (%s, %s)",
            (reader, normal, reader, celeb),
        )
        early = insert_post(conn, normal, "normal", BASE + timedelta(seconds=1))[0]
        late = insert_post(conn, celeb, "celeb", BASE + timedelta(seconds=2))[0]
        strategy = HybridStrategy(1000)
        assert strategy.post(conn, rdb, early, 10) == 1
        assert strategy.post(conn, rdb, late, 5000) == 0
        members = set(rdb.zrange(f"feed:{reader}", 0, -1))
        assert str(early.id) in members
        assert str(late.id) not in members
        assert [post.id for post in strategy.read(conn, rdb, reader, 10, None)] == [late.id, early.id]
    finally:
        cleanup(conn, rdb, [reader, normal, celeb])


def test_lateral_matches_join(conn, rdb):
    reader, author = HIGH + 21, HIGH + 22
    try:
        conn.execute(
            "INSERT INTO users (id, follower_count) VALUES (%s, 0), (%s, 1)",
            (reader, author),
        )
        conn.execute(
            "INSERT INTO follows (follower, followee) VALUES (%s, %s)",
            (reader, author),
        )
        created = BASE
        for offset in range(5):
            insert_post(conn, author, f"t{offset}", created)
        same = BASE + timedelta(seconds=10)
        insert_post(conn, author, "tie-a", same)
        insert_post(conn, author, "tie-b", same)
        assert [post.id for post in pull_read(conn, reader, 4, None)] == [
            post.id for post in join_read(conn, reader, 4, None)
        ]
        page = pull_read(conn, reader, 2, None)
        bound = (page[-1].created_at, page[-1].id)
        assert [post.id for post in pull_read(conn, reader, 10, bound)] == [
            post.id for post in join_read(conn, reader, 10, bound)
        ]
    finally:
        cleanup(conn, rdb, [reader, author])
