CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    follower_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS follows (
    follower BIGINT NOT NULL,
    followee BIGINT NOT NULL,
    PRIMARY KEY (follower, followee)
);

-- Leading column is followee, so fan-out (WHERE followee = ?) is an index-only scan.
CREATE INDEX IF NOT EXISTS follows_followee_idx ON follows (followee, follower);

CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    author BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS posts_author_created_idx ON posts (author, created_at DESC, id DESC);
