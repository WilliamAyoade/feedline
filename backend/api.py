"""HTTP API: posts, feeds, follow, and a per-user SSE stream."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from redis import Redis

from backend.config import DATABASE_URL, async_redis_client, redis_client
from backend.store import (
    active_strategy,
    enqueue_post,
    featured_users,
    follow_user,
    insert_post,
    iso,
    lookup_following,
    post_dict,
    read_config,
    unfollow_user,
    user_profile,
    ensure_schema,
)


class PostIn(BaseModel):
    author: int
    text: str = Field(min_length=1, max_length=500)
    created_at: float | None = None


class FollowIn(BaseModel):
    follower: int
    followee: int


class LookupIn(BaseModel):
    follower: int
    followees: list[int] = Field(default_factory=list, max_length=200)


def format_sse(event: dict) -> str:
    body = json.dumps(event, separators=(",", ":"))
    return f"id: {event['id']}\ndata: {body}\n\n"


def catchup_posts(pool: ConnectionPool, client: Redis, user: int, last_id: int) -> list[dict]:
    strategy = active_strategy(client)
    with pool.connection() as conn:
        posts = strategy.read(conn, client, user, 200, None)
    now = iso(datetime.now(timezone.utc))
    events = []
    for post in posts:
        if post.id <= last_id:
            continue
        events.append(
            {
                "id": post.id,
                "author": post.author,
                "text": post.text,
                "created_at": iso(post.created_at),
                "accepted_at": iso(post.created_at),
                "delivered_at": now,
                "replayed": True,
            }
        )
    events.sort(key=lambda event: event["id"])
    return events


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=8,
        kwargs={"autocommit": True},
        open=True,
    )
    with pool.connection() as conn:
        ensure_schema(conn)
    app.state.pool = pool
    app.state.redis = redis_client(max_connections=50)
    app.state.aredis = async_redis_client(max_connections=250)
    yield
    await app.state.aredis.aclose()
    app.state.redis.close()
    pool.close()


app = FastAPI(title="feedline", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health(request: Request):
    client: Redis = request.app.state.redis
    pool: ConnectionPool = request.app.state.pool

    def probe():
        name, threshold = read_config(client)
        with pool.connection() as conn:
            conn.execute("SELECT 1")
        client.ping()
        return name, threshold

    try:
        name, threshold = await asyncio.to_thread(probe)
    except Exception as exc:
        raise HTTPException(503, f"not ready: {exc}") from exc
    return {"ok": True, "strategy": name, "threshold": threshold}


@app.get("/metrics")
async def metrics(request: Request):
    client: Redis = request.app.state.redis

    def read():
        keys = [
            "stats:feed_writes",
            "stats:posts",
            "stats:worst_post",
            "stats:backlog",
            "stats:backlog_peak",
            "stats:drain_s",
            "replay:state",
            "replay:posts",
        ]
        out = {key: client.get(key) for key in keys}
        out["used_memory"] = client.info("memory").get("used_memory")
        return out

    return await asyncio.to_thread(read)


@app.post("/posts")
async def create_post(body: PostIn, request: Request):
    created = (
        datetime.now(timezone.utc)
        if body.created_at is None
        else datetime.fromtimestamp(body.created_at, tz=timezone.utc)
    )
    accepted = datetime.now(timezone.utc)
    pool: ConnectionPool = request.app.state.pool
    client: Redis = request.app.state.redis

    def work():
        name, threshold = read_config(client)
        with pool.connection() as conn:
            post, count = insert_post(conn, body.author, body.text, created)
        enqueue_post(client, post, count, accepted, name, threshold)
        return post, count

    post, count = await asyncio.to_thread(work)
    payload = post_dict(post)
    payload["accepted_at"] = iso(accepted)
    payload["follower_count"] = count
    return payload


@app.get("/feed/{user}")
async def feed(
    user: int,
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    before: str | None = None,
):
    pool: ConnectionPool = request.app.state.pool
    client: Redis = request.app.state.redis

    def work():
        strategy = active_strategy(client)
        with pool.connection() as conn:
            return strategy.read(conn, client, user, limit, before)

    try:
        posts = await asyncio.to_thread(work)
    except LookupError as exc:
        raise HTTPException(400, "unknown before cursor") from exc
    return {
        "posts": [post_dict(post) for post in posts],
        "next_before": posts[-1].id if len(posts) == limit else None,
    }


@app.get("/stream/{user}")
async def stream(user: int, request: Request):
    aredis = request.app.state.aredis
    client: Redis = request.app.state.redis
    pool: ConnectionPool = request.app.state.pool
    last_header = request.headers.get("last-event-id")
    await aredis.sadd("listeners", str(user))

    async def events():
        pubsub = aredis.pubsub()
        await pubsub.subscribe(f"live:{user}")
        sent: set[int] = set()
        try:
            if last_header:
                pending = await aredis.lrange(f"pending:{user}", 0, 199)
                recovered = []
                for raw in reversed(pending):
                    event = json.loads(raw)
                    if int(event["id"]) <= int(last_header):
                        continue
                    recovered.append(event)
                extra = await asyncio.to_thread(
                    catchup_posts, pool, client, user, int(last_header)
                )
                have = {int(event["id"]) for event in recovered}
                for event in extra:
                    if int(event["id"]) not in have:
                        recovered.append(event)
                recovered.sort(key=lambda event: int(event["id"]))
                for event in recovered:
                    sent.add(int(event["id"]))
                    yield format_sse(event)
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15.0)
                if message is None:
                    yield ": keepalive\n\n"
                    continue
                if message.get("type") != "message":
                    continue
                event = json.loads(message["data"])
                post_id = int(event["id"])
                if post_id in sent:
                    continue
                sent.add(post_id)
                yield format_sse(event)
        finally:
            await pubsub.unsubscribe(f"live:{user}")
            await pubsub.aclose()
            await aredis.srem("listeners", str(user))

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/follow")
async def follow(body: FollowIn, request: Request):
    return await _mutate_follow(request, body, follow=True)


@app.post("/unfollow")
async def unfollow(body: FollowIn, request: Request):
    return await _mutate_follow(request, body, follow=False)


async def _mutate_follow(request: Request, body: FollowIn, follow: bool):
    pool: ConnectionPool = request.app.state.pool
    client: Redis = request.app.state.redis

    def work():
        with pool.connection() as conn:
            if follow:
                strategy = active_strategy(client)
                return follow_user(conn, client, strategy, body.follower, body.followee)
            return unfollow_user(conn, client, body.follower, body.followee)

    try:
        return await asyncio.to_thread(work)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/follows/lookup")
async def follows_lookup(body: LookupIn, request: Request):
    pool: ConnectionPool = request.app.state.pool

    def work():
        with pool.connection() as conn:
            return lookup_following(conn, body.follower, body.followees)

    following = await asyncio.to_thread(work)
    return {"following": following}


@app.get("/users/featured")
async def users_featured(request: Request):
    pool: ConnectionPool = request.app.state.pool

    def work():
        with pool.connection() as conn:
            return featured_users(conn)

    return await asyncio.to_thread(work)


@app.get("/users/{user_id}")
async def users_one(user_id: int, request: Request, viewer: int | None = None):
    pool: ConnectionPool = request.app.state.pool

    def work():
        with pool.connection() as conn:
            return user_profile(conn, user_id, viewer)

    profile = await asyncio.to_thread(work)
    if profile is None:
        raise HTTPException(404, "unknown user")
    return profile
