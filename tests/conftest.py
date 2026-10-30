import pytest
import redis
import psycopg

from backend.config import DATABASE_URL
from backend.store import ensure_schema

HIGH = 8_000_000_000


@pytest.fixture
def conn():
    connection = psycopg.connect(DATABASE_URL, autocommit=True)
    ensure_schema(connection)
    yield connection
    connection.close()


@pytest.fixture
def rdb():
    client = redis.Redis.from_url("redis://127.0.0.1:6379/1", decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


def cleanup(conn, rdb, ids: list[int]) -> None:
    conn.execute("DELETE FROM posts WHERE author = ANY(%s)", (ids,))
    conn.execute(
        "DELETE FROM follows WHERE follower = ANY(%s) OR followee = ANY(%s)",
        (ids, ids),
    )
    conn.execute("DELETE FROM users WHERE id = ANY(%s)", (ids,))
    if ids:
        rdb.delete(*[f"feed:{user_id}" for user_id in ids])
