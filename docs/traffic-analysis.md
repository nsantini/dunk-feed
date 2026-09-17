# Bluesky traffic analysis for Dunk Feed

2026-09-18. Measured from this machine. Every number below is tagged.

- **[Certain]** measured directly, or read from a primary source.
- **[Likely]** derived from a measurement with one stated assumption.
- **[Guessing]** an estimate with no measurement behind it.

## 1. Method

Three probes ran on 2026-09-18 between 08:05 and 08:17 NZST (2026-09-17 20:05 to 20:17 UTC).

1. **Jetstream sample.** One websocket to `jetstream2.us-east.bsky.network`, four collections (`post`, `like`, `repost`, `postgate`), no compression, 418 seconds of stream time, 163,772 events. Script: `sample_jetstream.py` (not kept in the repo).
2. **Hot feed probe.** `app.bsky.feed.getFeed` on Bluesky's `hot-classic` and `whats-hot` (Discover) feeds through `public.api.bsky.app`, unauthenticated, 200 to 300 posts each.
3. **Dunk probe.** For quote posts found in `hot-classic`, fetched the original with `getPosts` and computed the PRD score with the default constants.

The sample is one Thursday evening in the US. It is not a peak. Jaz's 2024 data shows surges at two to three times baseline. Size for three times these numbers.

## 2. Network volume [Certain for the sample, Likely for the daily extrapolation]

| Stream | Events in 418 s | Per second | Per day | Avg bytes per event |
|---|---|---|---|---|
| All four collections | 163,772 | 392 | 33.8M | 564 |
| `post` create | 19,867 | 47.5 | 4.1M | 819 |
| `like` create | 117,469 | 281 | 24.3M | 525 |
| `repost` create | 22,540 | 54 | 4.7M | 532 |
| `postgate` create or update | 534 | 1.3 | 110k | 524 |
| `like` delete | 1,326 | 3.2 | 274k | |
| `repost` delete | 670 | 1.6 | 138k | |
| `post` delete | 1,110 | 2.7 | 229k | |

Ratios that matter for sizing:

- Likes to posts: **5.9 to 1**. Reposts to posts: **1.1 to 1**.
- Deletes: 1.1% of likes, 3.0% of reposts, 5.6% of posts are deleted. The PRD is right that deletes must decrement.
- 48.9% of post creates are replies. Replies are the third counter, and they arrive as posts, not as a separate collection.

Bandwidth: 92.3 MB in 418 s is **221 KB/s, 19 GB/day uncompressed** [Certain for the sample]. With Jetstream's dict-zstd at the 2.3x ratio Jaz reported, that is **about 8 GB/day, 250 GB/month, compressed** [Likely]. Check the VM provider's ingress cap. Most providers do not bill ingress.

The daily post figure (4.1M) matches the secondary figure of 1.41 billion posts in 2025 (3.9M/day) [Likely]. That cross-check gives some confidence the sample window was ordinary.

## 3. Quote posts [Certain for the sample]

| Metric | Count | Share |
|---|---|---|
| Quote posts whose target is a post | 1,551 of 19,867 posts | **7.8% of posts**, 3.7/s, **320k/day** |
| Self quotes (`quote_did == original_did`) | 214 of 1,551 | 13.8% of quotes |
| Quotes that are also replies | 67 of 1,551 | 4.3% |
| Embed targets that are not posts | 5 of 1,556 | **0.3%** (4 starter packs, 1 feed generator) |

Embed type distribution across all post creates: none 62.9%, external 13.9%, images 13.5%, record 6.8%, video 1.7%, recordWithMedia 1.1%, gallery 0.2%.

Two corrections to the PRD:

1. The PRD says the non-post embed check "discards a meaningful slice of the embed traffic for free". It discards 0.3%. Keep the check for correctness, not for volume.
2. `app.bsky.embed.gallery` exists now. It is not a quote. The embed parser must treat unknown embed types as "not a quote", not as an error.

**Net new pairs per day: about 276k** (320k minus self quotes) [Likely]. Over the 48-hour candidate window that is **about 550k live pairs**.

## 4. Postgates [Certain for the sample]

534 postgate records in 418 s. 533 carried `disableRule` and only **5 detached URIs in total**. Postgates are almost entirely authors disabling quotes on their own posts up front, not authors detaching a specific quote after the fact. Detaches are rare, so handling them costs nothing. Keep the subscription. It is 1.3 events per second.

A `disableRule` on `O` means new quotes of `O` cannot be created through the app. Existing pairs stay valid. The feed should not drop a pair on `disableRule` alone.

## 5. What "popular" means on Bluesky today [Certain for the snapshot]

| Feed | Posts read | Min likes | p10 | p50 | p90 | Max | Share with any quote | Median age |
|---|---|---|---|---|---|---|---|---|
| `hot-classic` | 199 | **12** | 13 | 20 | 67 | 266 | 18% | 12 min |
| `whats-hot` (Discover) | 200 | 22 | 62 | 274 | 1,864 | 13,108 | 74% | 5.4 h |

Observations:

- `hot-classic` admits posts at **12 likes**. The GitHub issue that says 15 is out of date, or the threshold moved. The feed is a rolling 15-minute window of fresh posts.
- Discover **contains no quote posts and no replies** at all (0 of 300). Its embed types were external, images, video, or none. This matters for phase 0: you cannot seed dunks from Discover.
- `hot-classic` is **19% quote posts** (57 of 298). Quote posts are over-represented there versus the firehose (7.8%). Quoting is a popular-post behaviour.
- The PRD's `P = 50` sits at about p80 of `hot-classic` and p8 of Discover. A post with 50 likes is "would appear near the bottom of Discover". That is a defensible floor. `P = 20` would be "median hot-classic post".

## 6. Dunk probe [Certain for the snapshot, small sample]

Seeded from the 57 quote posts in `hot-classic`. 47 pairs after removing self quotes. Default constants (`Wr = 2`, `Wc = 0.5`, `k = 5`, `P = 50`, `M = 1.25`).

| Filter | Pairs |
|---|---|
| All pairs | 47 |
| `D >= 1.25` | 10 |
| `D >= 2` | 7 |
| `D >= 5` | 0 |
| `D >= 1.25` and `max(E(O), E(Q)) >= 50` | **4** |
| `D >= 1.25` and `E(O) >= 50` | 0 |

Top pair: `E(Q) = 222` against `E(O) = 47`, ratio 4.3.

Findings:

1. **About one in twelve popular quote posts is a dunk** under the PRD rules. The rule finds things. Whether they are funny is still phase 0's question.
2. **Seed phase 0 from popular quote posts, not popular originals.** A second probe seeded from the 40 most-quoted Discover posts and their 1,378 quotes found **zero** pairs with `D >= 0.5`. When `O` is popular enough to be in Discover, no quote beats it. Dunks are found from the `Q` side. The PRD's phase 0 recipe ("seed 50 heavily-quoted posts, pull their quotes") will return nothing. Replace it with "seed popular posts that are quotes, fetch what they quote".
3. **The popularity gate binds on `Q`, not `O`.** In every qualifying pair, `Q` cleared 50 and `O` did not. Expect the feed to be "a post that took off by quoting something small", not "a big post beaten by a bigger one". If that is not the product, raise `k` or require `E(O) >= P_O` separately.
4. **`k = 5` is doing work.** Three of the ten pairs with `D >= 1.25` had `E(O) <= 11`. Without `k` they would have ratios above 10 and would top the feed.
5. `getPosts` returns `followersCount` on neither side. The follower floor guard needs `app.bsky.actor.getProfiles` (25 DIDs per call).

## 7. Jetstream protocol, verified live [Certain]

The PRD describes Jetstream v1. The current service is **v2**, and the docs recommend it for new projects.

| Item | v2 value, observed 2026-09-18 |
|---|---|
| Hosts | `wss://jetstream.us-east.bsky.network`, `wss://jetstream.us-west.bsky.network` |
| Path | `/xrpc/network.bsky.jetstream.subscribeEvents` |
| Params | `collections` (repeat, `<prefix>.*` allowed), `dids`, `kinds`, `cursor` (a `seq` or unix microseconds), `zstdDictionary=<id>`, `maxMessageSizeBytes` |
| Envelope | `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did","seq","time","operation","collection","rkey","rev","cid","record"}}` |
| Cursor | `seq`, monotonic integer. Resume is inclusive. A microsecond cursor below the retention floor is clamped, and an `#info OutdatedCursor` frame says so |
| Lookback | 36 hours by default on public instances |
| Compression | `GET /xrpc/network.bsky.jetstream.getZstdDictionary` returns a 65,536-byte dictionary, header `x-zstd-dictionary-id: 20260811`, `cache-control: immutable`. Connect with `?zstdDictionary=20260811`. Each binary frame is one zstd frame whose plain bytes are the JSON text frame |
| Limits | 100 collections, 10,000 DIDs per subscription |

The v1 path (`/subscribe`, `time_us`, `kind`/`commit` shape) still works on `jetstream1`/`jetstream2` hosts, which is what the sampler used. Do not build on it.

## 8. Public App View [Certain]

- `public.api.bsky.app` answers `getPosts`, `getQuotes`, `getFeed`, and `getProfile` **unauthenticated**. Confirmed with live calls.
- No rate-limit headers are returned. Bluesky publishes no numeric limit for this host. Keep the verifier under one request per second and back off on 429.

## 9. Sizing for a small VM [Likely]

Assumes the 48-hour candidate window and the 30-day feed window agreed on 2026-09-18.

| Working set | Estimate | Basis |
|---|---|---|
| Live pairs | 550k rows, about 170 MB on disk | 276k/day x 2 days x ~300 B |
| Hot set (post URIs on either side of a live pair) | 1.1M entries. **13 to 20 MB** as a `HashSet<u64>` of URI hashes. 90 MB or more as `HashSet<String>` | 2 URIs per pair |
| Counter rows | at most 1.1M, under 150 MB on disk | one per hot-set URI that received an event |
| Feed rows (30 days) | under 100k, a few MB | promoted pairs only |
| Counter write rate | 70 to 150 upserts/s typical, 360/s if every like hit the hot set | share of likes that target a hot-set post is unmeasured; the hot set holds ~13% of posts created in 48 h but quoted posts skew popular |
| App View calls | under 1,000/day for verification, under 500/h re-verifying promoted pairs | 25 URIs per call |
| Ingress | 8 GB/day compressed, 3x at surge | section 2 |
| Steady-state RSS | 150 to 250 MB | hot set + SQLite page cache + tokio |
| CPU | under 10% of one core at 400 events/s; JSON parse and zstd dominate | 220 KB/s |

A 2 vCPU, 2 GB VM with 10 GB of disk is enough with headroom for a 3x surge. 1 GB would work if the SQLite cache is capped at 64 MB.

## 10. What is still unmeasured

- The share of likes whose subject is in the hot set. This decides the real counter write rate. Story 04 logs it.
- Promoted pairs per day. This decides the feed's depth and the guard costs. Story 06 logs it.
- Weekend and surge behaviour. Run the sampler for 24 hours before tuning `P` and `M`.
- Whether the hot-classic follower profile of dunked-on authors makes the follower floor bite. Story 10 logs follower counts before it drops anything.

## Sources

- Live measurements described in section 1.
- [Jetstream docs](https://bsky.network/docs/jetstream/) and [bluesky-social/jetstream `docs/README.md`](https://github.com/bluesky-social/jetstream/blob/main/docs/README.md), read 2026-09-18.
- [Jaz, "Shrinking the AT Proto Firehose by >99%"](https://jazco.dev/2024/09/24/jetstream/), for the 2.3x compression ratio.
- [`app.bsky.embed.record` lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/embed/record.json): `view.record` is one of `viewRecord`, `viewNotFound`, `viewBlocked`, `viewDetached`, or a non-post view.
- [`app.bsky.feed.postgate` lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/postgate.json): `detachedEmbeddingUris` max 50, `embeddingRules` holds `disableRule`.
