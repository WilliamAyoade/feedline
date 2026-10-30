import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://feedline:feedline@127.0.0.1:5433/feedline",
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
API_BASE = os.environ.get("API_BASE", "http://127.0.0.1:8000")

STREAM = "fanout"
GROUP = "workers"
KEEP = 800
BATCH = 1000
WARMUP_S = 60


def strategy_name() -> str:
    return os.environ.get("FEED_STRATEGY", "push").strip().lower()


def hybrid_threshold() -> int:
    return int(os.environ.get("HYBRID_THRESHOLD", "1000"))


def _retry_kwargs(retry_cls) -> dict:
    from redis.backoff import ExponentialBackoff
    from redis.exceptions import ConnectionError, TimeoutError

    # Ride out a dropped port forward (about a minute of retries) instead of crashing.
    return {
        "retry": retry_cls(ExponentialBackoff(cap=5.0, base=0.2), 20),
        "retry_on_error": [ConnectionError, TimeoutError],
        "health_check_interval": 15,
    }


def redis_client(**kwargs):
    import redis
    from redis.retry import Retry

    return redis.Redis.from_url(REDIS_URL, decode_responses=True, **_retry_kwargs(Retry), **kwargs)


def async_redis_client(**kwargs):
    import redis.asyncio as aioredis
    from redis.asyncio.retry import Retry

    return aioredis.from_url(REDIS_URL, decode_responses=True, **_retry_kwargs(Retry), **kwargs)
