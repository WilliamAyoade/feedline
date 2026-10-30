from datetime import datetime, timedelta, timezone

from backend.store import KEEP, Post, fanout_feeds


def test_feed_trim_keeps_newest_800(rdb):
    user_id = 42
    base = datetime(2012, 7, 3, tzinfo=timezone.utc)
    for post_id in range(1, KEEP + 51):
        post = Post(post_id, 7, base + timedelta(seconds=post_id), "x")
        fanout_feeds(rdb, [user_id], post)
    members = {int(item) for item in rdb.zrange(f"feed:{user_id}", 0, -1)}
    assert len(members) == KEEP
    assert KEEP + 50 in members
    assert 51 in members
    assert 50 not in members
    assert 1 not in members
