export type Post = {
  id: number;
  author: number;
  text: string;
  created_at: string;
  accepted_at?: string;
  delivered_at?: string;
  replayed?: boolean;
};

export type FeedState = {
  posts: Post[];
  pending: Post[];
};

export const emptyFeed: FeedState = { posts: [], pending: [] };

function byNewest(a: Post, b: Post): number {
  if (a.created_at === b.created_at) return b.id - a.id;
  return a.created_at < b.created_at ? 1 : -1;
}

export function dedupePosts(posts: Post[]): Post[] {
  const seen = new Set<number>();
  const out: Post[] = [];
  for (const post of posts) {
    if (seen.has(post.id)) continue;
    seen.add(post.id);
    out.push(post);
  }
  return out;
}

export type FeedAction =
  | { type: "reset" }
  | { type: "refetch"; posts: Post[] }
  | { type: "page"; posts: Post[] }
  | { type: "stream"; post: Post }
  | { type: "show-pending" };

export function reduceFeed(state: FeedState, action: FeedAction): FeedState {
  switch (action.type) {
    case "reset":
      return emptyFeed;
    case "refetch": {
      const posts = dedupePosts(action.posts).sort(byNewest);
      const ids = new Set(posts.map((post) => post.id));
      return { posts, pending: state.pending.filter((post) => !ids.has(post.id)) };
    }
    case "page": {
      const posts = dedupePosts([...state.posts, ...action.posts]).sort(byNewest);
      const ids = new Set(posts.map((post) => post.id));
      return { posts, pending: state.pending.filter((post) => !ids.has(post.id)) };
    }
    case "stream": {
      const post = action.post;
      if (state.posts.some((item) => item.id === post.id) || state.pending.some((item) => item.id === post.id)) {
        return state;
      }
      return { ...state, pending: [post, ...state.pending] };
    }
    case "show-pending":
      return {
        posts: dedupePosts([...state.pending, ...state.posts]).sort(byNewest),
        pending: [],
      };
  }
}
