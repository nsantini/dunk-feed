# 08 — Degree 2 with a shared follows cache

- **Follows**: 06
- **PRD story**: Discover people one step out; First open builds my circle
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.2 step 3, §6.4, §8, §9.2

## Outcome

After this ships, the first build runs a third step. It fetches the recent
follows of each account in the viewer's `d2_sample` and keeps them in a
shared cache. A pair enters the feed when one of its authors is followed
by an account the viewer follows. Two viewers who follow the same account
share one fetch in each `UPSTAGE_D2_REFRESH_AGE_H`. Degree-2 pairs keep
the global rank. Followers of the viewer's followers never count.

## Non-goals

- Does not refill stale cache entries on a schedule. Story 09 adds
  `Refill` jobs and the cache clean-up.
- Does not add other degree-2 paths or degree 3.
- Does not give degree-2 pairs a bonus or a penalty.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11.

## Approach

`graph/cache.rs` holds `FollowsCache`: a map from account DID to
`(fetched_at, Arc<Vec<u64>>)`, sorted. It loads an entry from SQLite on a
miss and keeps it in memory while an active circle names it.
`store/follows_cache.rs` stores each list as a BLOB of little-endian
`u64`. The worker runs `build::step_degree2` (story 03) as the last step. It
skips accounts with a fresh entry. `http/viewer.rs` builds the degree-2
set as a temporary `HashSet<u64>` from the cache entries of `d2_sample`,
passes it to `connected_indices`, and drops it after the list is built.

## Files in scope

| Path | Change |
|---|---|
| `src/graph/cache.rs` (new) | `FollowsCache`: `get`, `put`, `is_fresh`, `degree2_set(d2_sample)` |
| `src/store/follows_cache.rs` (new) | `follows_get`, `follows_put`, BLOB encode and decode |
| `src/store/mod.rs` | `Store` methods for the above |
| `src/graph/queue.rs` | `FirstBuild` runs step 3 as its last step |
| `src/graph/circle.rs` | `CircleState::BuildingD2` |
| `src/graph/mod.rs` | `GraphHandle` owns the `FollowsCache` |
| `src/http/viewer.rs` | Builds the temporary degree-2 set for each list build |
| `src/config.rs` | `UPSTAGE_D2_REFRESH_AGE_H` (default 24) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | step 3 | account with no entry, or an entry older than `UPSTAGE_D2_REFRESH_AGE_H` | One `getFollows` call, `sort=latest`, `limit=UPSTAGE_D2_FOLLOWS_DEPTH`. Result saved sorted |
| BC2 | step 3 | account with a fresh entry | No call |
| BC3 | step 3 | two viewers name one account within the age | One call in total |
| BC4 | step 3 | one account fails | Other accounts continue. The failed account has no entry. State `ready` at the end |
| BC5 | step 3 | completes | State `ready`. Cached lists dropped |
| BC6 | BLOB | round trip | `decode(encode(v)) == v` for sorted `u64` lists, including empty |
| BC7 | viewer list | author in the follows list of an account in `d2_sample` | Kept, at every depth |
| BC8 | viewer list | author only follows an account that follows the viewer | Not kept |
| BC9 | viewer list | degree-2 item | Same position as in the ranked list. No rank change |
| BC10 | viewer list | after step 3 | Every item kept before step 3 is still kept |
| BC11 | viewer list | temporary set | Not stored in the circle or the list cache |
| BC12 | cache | restart | Entries load from SQLite on the first use |

## Acceptance criteria

- [ ] AC1 — Step 3 fetches only missing or stale accounts. Checked by: `cargo test graph::queue::tests::step3_fetches_missing_only`
- [ ] AC2 — Two viewers who share an account cause one fetch. Checked by: `cargo test graph::cache::tests::shared_fetch_once`
- [ ] AC3 — BLOB encode and decode round-trip. Checked by: `cargo test store::follows_cache::tests`
- [ ] AC4 — Degree-2 pairs are kept at every depth, with no rank change. Checked by: `cargo test http::viewer::tests::degree2_kept_same_rank`
- [ ] AC5 — Followers of followers are not kept. Checked by: `cargo test http::viewer::tests::no_followers_of_followers`
- [ ] AC6 — Items from earlier steps stay after step 3. Checked by: `cargo test http::viewer::tests::step3_keeps_earlier_items`
- [ ] AC7 — All four gates pass.

## Defaults taken

- A failure for one sampled account does not fail the job. That account
  counts as "no degree-2 data" until a later build or refill. The PRD
  allows missing degree-2 pairs, not wrong ones.
- No `last_used_at` column (§8 lists none). Story 09 cleans up by
  `fetched_at`, because an entry that a circle still names is refilled
  and stays fresh.

## Suggested slices

- 1.0 `store/follows_cache.rs` and `graph/cache.rs`. Done when AC2 and AC3
  pass.
- 2.0 Step 3 in the worker. Done when AC1 passes.
- 3.0 Degree-2 set in the viewer list. Done when AC4 to AC6 pass and all
  four gates pass.
