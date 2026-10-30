from datetime import datetime, timedelta, timezone

from backend.store import Post, epoch_us, kway_merge


def post(post_id: int, author: int, seconds: int) -> Post:
    created = datetime(2012, 7, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    return Post(post_id, author, created, f"p{post_id}")


def test_epoch_us_matches_unix_seconds():
    created = datetime.fromtimestamp(1341100972, tz=timezone.utc)
    assert epoch_us(created) == 1341100972 * 1_000_000


def test_kway_merge_order():
    left = [post(3, 1, 30), post(1, 1, 10)]
    right = [post(4, 2, 40), post(2, 2, 20)]
    assert [item.id for item in kway_merge([left, right], 3)] == [4, 3, 2]
    assert [item.id for item in kway_merge([left, right], 10)] == [4, 3, 2, 1]


def test_kway_merge_breaks_timestamp_ties_by_id():
    created = datetime(2012, 7, 4, 8, 0, tzinfo=timezone.utc)
    first = Post(2, 1, created, "a")
    second = Post(9, 2, created, "b")
    third = Post(4, 2, created, "c")
    merged = kway_merge([[first], [second, third]], 3)
    assert [item.id for item in merged] == [9, 4, 2]
