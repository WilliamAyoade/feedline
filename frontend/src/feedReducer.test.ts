import { describe, expect, it } from "vitest";
import { emptyFeed, reduceFeed, type Post } from "./feedReducer";

const post: Post = {
  id: 7,
  author: 88,
  text: "RT @1503",
  created_at: "2012-07-04T05:51:05+00:00",
};

describe("reduceFeed", () => {
  it("de-duplicates a post that arrives both by stream and by refetch", () => {
    const streamed = reduceFeed(emptyFeed, { type: "stream", post });
    const refetched = reduceFeed(streamed, { type: "refetch", posts: [post] });
    const ids = [...refetched.posts, ...refetched.pending].map((item) => item.id);
    expect(ids.filter((id) => id === post.id)).toHaveLength(1);
    expect(refetched.posts.map((item) => item.id)).toEqual([post.id]);
    expect(refetched.pending).toEqual([]);
  });

  it("ignores a stream event for a post already on the feed", () => {
    const loaded = reduceFeed(emptyFeed, { type: "refetch", posts: [post] });
    const again = reduceFeed(loaded, { type: "stream", post });
    expect(again).toBe(loaded);
  });
});
