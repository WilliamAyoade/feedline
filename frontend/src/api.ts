import type { Post } from "./feedReducer";

export const API = import.meta.env.VITE_API_URL ?? "http://127.0.0.1:8000";

export type Persona = {
  id: number;
  follower_count: number;
  label: string;
};

export type Featured = {
  author: Persona | null;
  reader: Persona | null;
  average: Persona | null;
  median_followers: number;
};

export type Health = {
  ok: boolean;
  strategy: string;
  threshold: number;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json() as Promise<T>;
}

export function getHealth(): Promise<Health> {
  return request("/health");
}

export function getFeatured(): Promise<Featured> {
  return request("/users/featured");
}

export function getFeed(user: number, before?: number): Promise<{ posts: Post[]; next_before: number | null }> {
  const query = new URLSearchParams({ limit: "50" });
  if (before != null) query.set("before", String(before));
  return request(`/feed/${user}?${query}`);
}

export function createPost(author: number, text: string): Promise<Post> {
  return request("/posts", { method: "POST", body: JSON.stringify({ author, text }) });
}

export function follow(follower: number, followee: number): Promise<{ following: boolean }> {
  return request("/follow", { method: "POST", body: JSON.stringify({ follower, followee }) });
}

export function unfollow(follower: number, followee: number): Promise<{ following: boolean }> {
  return request("/unfollow", { method: "POST", body: JSON.stringify({ follower, followee }) });
}

export function lookupFollowing(follower: number, followees: number[]): Promise<{ following: number[] }> {
  return request("/follows/lookup", {
    method: "POST",
    body: JSON.stringify({ follower, followees }),
  });
}
