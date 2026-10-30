from datetime import datetime, timedelta, timezone

from backend.store import PullStrategy, PushStrategy, follow_user, insert_post, unfollow_user
from tests.conftest import HIGH, cleanup


def _case(conn, rdb, strategy, follower: int, followee: int):
    created = datetime(2012, 7, 5, tzinfo=timezone.utc) + timedelta(seconds=follower % 1000)
    conn.execute(
        "INSERT INTO users (id, follower_count) VALUES (%s, 0), (%s, 3) ON CONFLICT (id) DO NOTHING",
        (follower, followee),
    )
    post, _count = insert_post(conn, followee, "hello", created)
    assert strategy.read(conn, rdb, follower, 10, None) == []
    followed = follow_user(conn, rdb, strategy, follower, followee)
    assert followed["created"] is True
    assert [item.id for item in strategy.read(conn, rdb, follower, 10, None)] == [post.id]
    unfollow_user(conn, rdb, follower, followee)
    assert strategy.read(conn, rdb, follower, 10, None) == []


def test_follow_unfollow_updates_pull_read(conn, rdb):
    follower, followee = HIGH + 31, HIGH + 32
    try:
        _case(conn, rdb, PullStrategy(), follower, followee)
    finally:
        cleanup(conn, rdb, [follower, followee])


def test_follow_unfollow_updates_push_read(conn, rdb):
    follower, followee = HIGH + 33, HIGH + 34
    try:
        _case(conn, rdb, PushStrategy(), follower, followee)
    finally:
        cleanup(conn, rdb, [follower, followee])
