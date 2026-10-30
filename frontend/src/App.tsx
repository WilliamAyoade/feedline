import { useEffect, useReducer, useRef, useState } from "react";
import {
  API,
  createPost,
  follow,
  getFeatured,
  getFeed,
  getHealth,
  lookupFollowing,
  unfollow,
  type Featured,
  type Health,
  type Persona,
} from "./api";
import { emptyFeed, reduceFeed, type Post } from "./feedReducer";

const GRAPH = {
  users: 456_626,
  follows: 14_855_819,
  posts: 563_069,
  celebrityFollowers: 51_386,
};

function formatWhen(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return (
    new Intl.DateTimeFormat("en-GB", {
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "UTC",
      hourCycle: "h23",
    }).format(date) + " UTC"
  );
}

function initials(label: string, id: number): string {
  const parts = label.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  if (parts[0]) return parts[0].slice(0, 2).toUpperCase();
  return String(id).slice(0, 2);
}

function strategyCopy(health: Health | null): { name: string; line: string } {
  if (health == null) return { name: "…", line: "Connecting to the server." };
  if (health.strategy === "pull") {
    return {
      name: "Look up on read",
      line: "We do not copy a post to followers when it is written. When you open this page, we look up the people you follow and collect their newest posts.",
    };
  }
  if (health.strategy === "hybrid") {
    return {
      name: `Skip celebrities (${health.threshold.toLocaleString()}+ fans)`,
      line: `Ordinary accounts are copied into followers’ inboxes. Anyone with ${health.threshold.toLocaleString()} or more followers is looked up only when you read, so one celebrity post does not copy tens of thousands of times.`,
    };
  }
  return {
    name: "Copy on write",
    line: "When someone posts, we copy that post into every follower’s inbox immediately. Opening the feed is then just reading that inbox.",
  };
}

function personaHelp(persona: Persona, celebrityId: number | null): string {
  if (persona.label === "Most followed") {
    return "The celebrity. Post as them to send a message to tens of thousands of followers.";
  }
  if (persona.label === "Typical reader") {
    return celebrityId
      ? `A normal person who follows the celebrity (account ${celebrityId}). Keep this tab open to watch.`
      : "A normal person. Keep this tab open to watch.";
  }
  return "A typical account on this network. Most people have only a handful of followers.";
}

function viewingCopy(
  persona: Persona | undefined,
  me: number | null,
): { subtitle: string; lede: string } {
  if (me == null) {
    return {
      subtitle: "Pick a card below. That person’s home timeline is what you see.",
      lede: "You log in as one person at a time. Their timeline shows posts from people they follow, newest first.",
    };
  }
  if (persona?.label === "Typical reader") {
    return {
      subtitle: `You are a normal person (account ${me}) who follows the celebrity. This is their home timeline.`,
      lede: "Keep this tab here. In another window, switch to Most followed, write a post, then come back — a red “new posts” button appears without refresh.",
    };
  }
  if (persona?.label === "Most followed") {
    return {
      subtitle: `You are the celebrity (account ${me}). Write below to reach ${persona.follower_count.toLocaleString()} followers.`,
      lede: "The list below is who this celebrity follows — not their own posts. After you click Post, switch to Typical reader to see it land on a follower’s timeline.",
    };
  }
  if (persona?.label === "Median account") {
    return {
      subtitle: `You are a typical small account (account ${me}). Most people on this network look like this.`,
      lede: "This is still a home timeline: posts from people they follow. Switch to Typical reader to try the live demo.",
    };
  }
  return {
    subtitle: `You are account ${me}. The list below is their home timeline.`,
    lede: "Home timeline means posts from people this account follows, newest first. The red badge means new posts arrive without refresh.",
  };
}

function Avatar({ label, id, tone }: { label: string; id: number; tone?: "ink" | "red" | "paper" }) {
  return (
    <span className={`avatar tone-${tone ?? "ink"}`} aria-hidden="true">
      {initials(label, id)}
    </span>
  );
}

export function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [featured, setFeatured] = useState<Featured | null>(null);
  const [me, setMe] = useState<number | null>(null);
  const [feed, dispatch] = useReducer(reduceFeed, emptyFeed);
  const [draft, setDraft] = useState("");
  const [live, setLive] = useState(false);
  const [error, setError] = useState("");
  const [following, setFollowing] = useState<Set<number>>(new Set());
  const [nextBefore, setNextBefore] = useState<number | null>(null);
  const [freshIds, setFreshIds] = useState<Set<number>>(new Set());
  const [loadingMore, setLoadingMore] = useState(false);
  const sentinel = useRef<HTMLDivElement | null>(null);
  const meRef = useRef<number | null>(null);
  meRef.current = me;

  useEffect(() => {
    getHealth().then(setHealth).catch((err: Error) => setError(err.message));
    getFeatured()
      .then((data) => {
        setFeatured(data);
        setMe((current) => current ?? data.reader?.id ?? data.author?.id ?? null);
      })
      .catch((err: Error) => setError(err.message));
  }, []);

  useEffect(() => {
    if (me == null) return;
    let cancel = false;
    dispatch({ type: "reset" });
    setNextBefore(null);
    setFreshIds(new Set());
    getFeed(me)
      .then((page) => {
        if (cancel || meRef.current !== me) return;
        dispatch({ type: "refetch", posts: page.posts });
        setNextBefore(page.next_before);
      })
      .catch((err: Error) => setError(err.message));
    return () => {
      cancel = true;
    };
  }, [me]);

  useEffect(() => {
    if (me == null) return;
    const source = new EventSource(`${API}/stream/${me}`);
    source.onopen = () => setLive(true);
    source.onerror = () => setLive(false);
    source.onmessage = (event) => {
      const post = JSON.parse(event.data) as Post;
      dispatch({ type: "stream", post });
    };
    return () => {
      source.close();
      setLive(false);
    };
  }, [me]);

  useEffect(() => {
    if (me == null) return;
    const authors = [...new Set([...feed.posts, ...feed.pending].map((post) => post.author))];
    if (authors.length === 0) return;
    let cancel = false;
    lookupFollowing(me, authors)
      .then((result) => {
        if (!cancel) setFollowing(new Set(result.following));
      })
      .catch(() => undefined);
    return () => {
      cancel = true;
    };
  }, [me, feed.posts, feed.pending]);

  useEffect(() => {
    const node = sentinel.current;
    if (!node || me == null || nextBefore == null) return;
    const observer = new IntersectionObserver((entries) => {
      if (!entries[0]?.isIntersecting || loadingMore) return;
      setLoadingMore(true);
      getFeed(me, nextBefore)
        .then((page) => {
          dispatch({ type: "page", posts: page.posts });
          setNextBefore(page.next_before);
        })
        .catch((err: Error) => setError(err.message))
        .finally(() => setLoadingMore(false));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [me, nextBefore, loadingMore]);

  const personas: Persona[] = [featured?.author, featured?.reader, featured?.average].filter(
    (item): item is Persona => item != null,
  );
  const maxFollowers = featured?.author?.follower_count ?? GRAPH.celebrityFollowers;
  const activePersona = personas.find((persona) => persona.id === me);
  const viewing = viewingCopy(activePersona, me);
  const strategy = strategyCopy(health);

  async function submitPost() {
    if (me == null || !draft.trim()) return;
    setError("");
    try {
      await createPost(me, draft.trim());
      setDraft("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "post failed");
    }
  }

  async function toggleFollow(author: number) {
    if (me == null || author === me) return;
    const on = following.has(author);
    if (on) await unfollow(me, author);
    else await follow(me, author);
    setFollowing((current) => {
      const next = new Set(current);
      if (on) next.delete(author);
      else next.add(author);
      return next;
    });
    const page = await getFeed(me);
    dispatch({ type: "refetch", posts: page.posts });
    setNextBefore(page.next_before);
  }

  function showPending() {
    setFreshIds(new Set(feed.pending.map((post) => post.id)));
    dispatch({ type: "show-pending" });
  }

  return (
    <div className="stage">
      <aside className="rail">
        <p className="eyebrow">What this is</p>
        <h1>feedline</h1>
        <p className="thesis">
          This is a follow feed, like the home timeline on Twitter or Instagram: you see posts from
          people you follow, newest first. The hard part is a celebrity post — here, one account has{" "}
          {maxFollowers.toLocaleString()} followers, and each of them should see the post without
          waiting.
        </p>
        <dl className="stats">
          <div>
            <dt>People</dt>
            <dd>{GRAPH.users.toLocaleString()}</dd>
          </div>
          <div>
            <dt>Follows</dt>
            <dd>{GRAPH.follows.toLocaleString()}</dd>
          </div>
          <div>
            <dt>2012 posts</dt>
            <dd>{GRAPH.posts.toLocaleString()}</dd>
          </div>
          <div>
            <dt>Top fans</dt>
            <dd>{maxFollowers.toLocaleString()}</dd>
          </div>
        </dl>
        <div className="strategy-card">
          <span className="kicker">How posts reach followers right now</span>
          <strong>{strategy.name}</strong>
          <p>{strategy.line}</p>
        </div>
        <ol className="guide">
          <li>Stay on Typical reader in this tab (you follow the celebrity).</li>
          <li>Open a second window, switch to Most followed, write a post, click Post.</li>
          <li>Come back here. A red “new posts” button appears — you did not refresh.</li>
        </ol>
      </aside>

      <main className="app">
        <header className="mast">
          <div>
            <p className="dateline">You are logged in as one person at a time</p>
            <h2>{activePersona ? activePersona.label : "Choose who you are"}</h2>
            <p className="as-user">{viewing.subtitle}</p>
          </div>
          <div className={live ? "live on" : "live"} aria-live="polite">
            <span className={live ? "dot on" : "dot"} />
            {live ? "updates live" : "reconnecting"}
          </div>
        </header>

        <p className="lede">{viewing.lede}</p>

        <section className="who" aria-label="Viewing as">
          {personas.map((persona, index) => (
            <button
              key={persona.id}
              type="button"
              aria-pressed={me === persona.id}
              onClick={() => setMe(persona.id)}
            >
              <Avatar label={persona.label} id={persona.id} tone={index === 0 ? "red" : "ink"} />
              <span className="who-copy">
                <strong>{persona.label}</strong>
                <em>
                  account {persona.id} · {persona.follower_count.toLocaleString()} followers
                </em>
                <small className="who-blurb">{personaHelp(persona, featured?.author?.id ?? null)}</small>
                <span
                  className="bar"
                  style={{ width: `${Math.max(6, (persona.follower_count / maxFollowers) * 100)}%` }}
                />
              </span>
            </button>
          ))}
          <form
            onSubmit={(event) => {
              event.preventDefault();
              const data = new FormData(event.currentTarget);
              const value = Number(data.get("user"));
              if (Number.isInteger(value) && value > 0) setMe(value);
            }}
          >
            <label>
              Jump to any account
              <input name="user" inputMode="numeric" placeholder="account number" aria-label="View as user id" />
            </label>
            <button type="submit">View their feed</button>
          </form>
        </section>

        <form
          className="composer"
          onSubmit={(event) => {
            event.preventDefault();
            void submitPost();
          }}
        >
          <Avatar label={activePersona?.label ?? "You"} id={me ?? 0} tone="paper" />
          <textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            placeholder={
              me == null
                ? "Pick who you are, then write a post"
                : `Write as ${activePersona?.label ?? me}. People who follow this account will see it.`
            }
            maxLength={500}
            aria-label="Post text"
          />
          <button type="submit">Post</button>
        </form>
        {error ? <p className="error">{error}</p> : null}

        {feed.pending.length > 0 ? (
          <div className="pill">
            <button type="button" onClick={showPending}>
              {`${feed.pending.length} new post${feed.pending.length === 1 ? "" : "s"} — click to show`}
            </button>
          </div>
        ) : null}

        {feed.posts.length === 0 ? (
          <div className="empty">
            <p>This person has nothing in their feed yet.</p>
            <small>
              Follow someone, or switch to Most followed, post, then come back. New posts show up
              here without refreshing.
            </small>
          </div>
        ) : (
          <>
          <p className="feed-note">
            Newest first. The top item may be one you just posted. Older lines like “RT @123” are
            real messages from a public July 2012 Twitter dataset — not broken text.
          </p>
          <ol className="feed">
            {feed.posts.map((post) => (
              <li key={post.id} className={freshIds.has(post.id) ? "post is-new" : "post"}>
                <Avatar label={`user ${post.author}`} id={post.author} />
                <div className="post-body">
                  <div className="kicker">
                    Posted by account {post.author}
                    {featured?.author?.id === post.author ? <span>the celebrity</span> : null}
                  </div>
                  <p>{post.text}</p>
                  <div className="meta">
                    <span>Original time {formatWhen(post.created_at)}</span>
                    {me != null && post.author !== me ? (
                      <button
                        type="button"
                        className={following.has(post.author) ? "quiet" : undefined}
                        onClick={() => void toggleFollow(post.author)}
                      >
                        {following.has(post.author) ? "Unfollow" : "Follow"}
                      </button>
                    ) : null}
                  </div>
                </div>
              </li>
            ))}
          </ol>
          </>
        )}
        <div ref={sentinel} className="sentinel" />
        {loadingMore ? <p className="more">Loading earlier posts…</p> : null}
      </main>
    </div>
  );
}
