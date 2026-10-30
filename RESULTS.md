# feedline results

Python 3.11.15 ; Postgres 16.15 (Debian 16.15-1.pgdg13+2, aarch64) ; Redis 7.4.12 ; machine Mac15,6, macOS 26.5, 18 GB RAM, 11 CPUs, Colima 4 CPU / 5 GB / 40 GB disk ; Redis container 2.8 GB (maxmemory 2.5 GB, noeviction) ; Postgres container 1.4 GB ; dataset SNAP higgs-twitter ; date 2026-10-28 to 2026-10-30

## M1 Data

users 456,626 ; follows 14,855,819 ; direction: column 1 follows column 2 ; max followers 51,386 (user 1503) ; p99 418

posts 563,069 ; busiest 10 min: 8,977 posts starting 2012-07-04 05:51:05 UTC (unix 1341381065–1341381665)

Also dropped: 23 self-follows, 0 duplicate edges. Official SNAP edge count 14,855,842 = 14,855,819 kept + 23 self-follows. Top five accounts: 1503 (51,386), 206 (48,414), 88 (45,221), 138 (44,188), 1062 (40,120). The 4 July 2012 08:00–10:00 UTC announcement window contains 44,926 posts. Graph load took 60.3 s. Source: `results/m1.json`.

A graph scan estimated 37,759,074 push feed writes and a worst post of 45,221 writes (author 88). The timed push and hybrid-100K rows matched that write count exactly.

## Per strategy (replay speed x60)

All five rows used speed 60. Freshness is `delivered_at − accepted_at` on the API clock. Historical 2012 `created_at` is for ordering and the busy-window filter only. Catch-up (`replayed`) events are excluded. Pull’s backlog column is `n/a`.

| strategy | feed writes | write amp | Redis MB | worst post | backlog peak / drain | read p50/p95/p99 ms | celeb-reader p95 ms | req/s | fresh p50/p99 ms | busiest-10min p99 ms | match |
|---|---|---|---|---|---|---|---|---|---|---|---|
| push | 37,759,074 | 67.06 | 580.2 | 45,221 | 266 / 0.43 s | 6.65/22.45/96.10 | 22.55 | 99.29 | 16.99/606.24 | 275.98 | 1000/1000 |
| pull | 0 | 0 | 5.5 | 0 | n/a | 4.73/29.68/98.25 | 29.75 | 99.41 | 3.43/17.30 | 5.22 | 1000/1000 |
| hybrid 1K | 20,251,252 | 35.97 | 333.0 | 997 | 32 / 0.06 s | 7.10/100.07/1360.70 | 102.37 | 97.07 | 5.06/33.15 | 11.36 | 1000/1000 |
| hybrid 10K | 30,548,796 | 54.25 | 475.4 | 9,891 | 351 / 0.77 s | 9.00/32.81/337.31 | 33.49 | 98.21 | 11.33/151.62 | 51.95 | 1000/1000 |
| hybrid 100K | 37,759,074 | 67.06 | 579.6 | 45,221 | 230 / 1.84 s | 7.54/24.56/112.21 | 25.25 | 99.09 | 23.76/658.07 | 312.88 | 1000/1000 |

Pull accepted 563,067 of 563,069 posts (2 `ReadError`s from idle keep-alive; later replays closed idle connections after 3 s and had 0 errors). Every replay fell behind during the busiest hours because API intake is capped at about 230 posts/s through Colima’s SSH port forwarding, then caught up. Schedule lag at the end was 0.0–0.1 s. Freshness is timed from acceptance, so that intake cap does not inflate it.

Pull and hybrid 1,000 were measured on an 8 GB Colima VM. After host swap filled, Colima was cut to 5 GB with container caps (Redis 2.8 GB / 2.5 GB maxmemory, Postgres 1.4 GB). Hybrid 10,000, hybrid 100,000, and push ran under those caps. Push Redis was 580 MB, well under 2.5 GB.

Hybrid 1,000 cut writes 1.86× versus push (20.3M vs 37.8M) and kept busy-window freshness at 11 ms p99, but feed p95 was 100 ms versus push 22 ms (78 ms slower). Hybrid 10,000 cut writes 1.24× with p95 33 ms. Hybrid 100,000 matches push on writes because max followers is 51,386, so T=100000 creates zero celebrities.

## Tests

pytest 11 passing ; vitest 2 passing

## Notes

`EXPLAIN ANALYZE` of the pull feed for the heaviest follower (user 13115, 1,259 followees) is in `results/explain.txt`. The LATERAL plan uses `posts_author_created_idx` (`Bitmap Index Scan` per followee) and finished in 59 ms. A join over `posts` seq-scanned the table with two parallel workers and finished in 57 ms on this same user because it reads the whole post table. Typical pull p50 is 4.7 ms; those readers follow far fewer accounts than 13115.
