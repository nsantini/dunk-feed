# 01 — Snapshot carries authors, caps become reusable

- **Follows**: none
- **PRD story**: none (prefactor)
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §9.1
- **Flag**: none, because this prefactor changes internal data only. The served feed bytes do not change

## Release

This story ships in the next normal deploy. It has no flag, because
viewers see no change. The response bytes stay the same for the same rows.

Rollback is a revert of the pull request and a new deploy. The story adds
no table and no stored data. A revert is safe at any time.

## Outcome

After this ships, the scorer builds one ranked list with no caps and one
`global` index list with the `01` caps. Each `FeedItem` carries the author
hashes and times that a viewer filter needs. The two caps are one function,
`caps::apply(items, indices)`, that a later story calls for each viewer.
`getFeedSkeleton` serves `global`. The response bytes do not change for the
same rows.

## Non-goals

- Does not read the JWT or filter by viewer. Story 05 and story 06 do this.
- Does not change the cursor format, the three-path resume or
  `Cache-Control`.
- Does not add `UPSTAGE_PERSONALISE`. Story 05 adds it with default
  `false`.
- Does not change the order of the global feed or the cap rules.

## Approach

`snapshot::build` sorts the rows as today and keeps every row as a
`FeedItem`. It then calls `caps::apply` on all indices `0..len` to get
`global`. `caps::apply` runs cap 1 and then cap 2 on `u32` indices, with
the same deferral logic as today. Cap 1 reads `original_did` and
`quoted_at`. Cap 2 reads `quote_did`. Both compare `u64` hashes, not
strings. `Snapshot` holds `items` (uncapped) and `global`. The skeleton
handler reads items through `global`, so its cursor `index` is a position
in `global`. The old `build` stays as a test-only oracle for the
regression test.

## Files in scope

| Path | Change |
|---|---|
| `src/scorer/snapshot.rs` | `FeedItem` gets `quote_did: u64`, `original_did: u64`, `quoted_at: i64`, `promoted_at: i64`. `build` returns items and `global`. `Snapshot` and `SnapshotHandle::swap` carry both |
| `src/scorer/snapshot/caps.rs` (new) | `pub fn apply(items: &[FeedItem], indices: &[u32]) -> Vec<u32>`, cap 1 then cap 2 |
| `src/scorer/mod.rs` | Snapshot step passes the new build result to `swap` |
| `src/http/skeleton.rs` | Pages over `global`. Three-path resume reads positions in `global` |
| `src/http/health.rs` | `snapshot_len` reads `global.len()` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `FeedItem` | built from a `FeedRow` | `quote_did` and `original_did` are `xxh3_64` of the DID bytes. `quoted_at` and `promoted_at` copy the row |
| BC2 | `snapshot::build` | any rows | `items` holds every row, sorted by `cmp_rank_then_cid`. No row is dropped |
| BC3 | `snapshot::build` | any rows | `global == caps::apply(&items, &(0..len))` |
| BC4 | `caps::apply` | two indices, same `original_did`, same UTC day of `quoted_at` | Only the first index stays |
| BC5 | `caps::apply` | same `quote_did` inside 50 output positions | The later index is deferred, then released when legal, as in `01` BC29 to BC32 |
| BC6 | `caps::apply` | a subset of indices | Caps count only the given indices. An index outside the subset never takes a slot |
| BC7 | `caps::apply` | output | Each output value is an input value. The output has no duplicates |
| BC8 | `getFeedSkeleton` | same rows as before this story | Response JSON, cursor strings and headers are byte-for-byte equal to the `01` output |
| BC9 | `/healthz` | `snapshot_len` | Equals the number of served items (`global.len()`) |

## Acceptance criteria

- [ ] AC1 — `global` equals the old capped list, item for item, on a fixture with cap-1 drops, cap-2 deferrals and ties. Checked by: `cargo test scorer::snapshot::tests::global_matches_v1`
- [ ] AC2 — `caps::apply` on a subset ignores items outside the subset. Checked by: `cargo test scorer::snapshot::caps::tests::subset_frees_slots`
- [ ] AC3 — The existing cap tests pass against `caps::apply`. Checked by: `cargo test scorer::snapshot::caps`
- [ ] AC4 — A full page and a cursor page are byte-for-byte equal to the `01` output for the same rows. Checked by: `cargo test http::skeleton::tests::global_output_unchanged`
- [ ] AC5 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- Hash function: `xxh3_64`, because `xxhash-rust` has only the `xxh3`
  feature today. The design says "xxhash64". The choice has no effect on
  behaviour.
- `promoted_at` is on `FeedItem` now, because the probe (story 03) and
  the metrics (story 10) count circle pairs from the last 24 hours.
- `caps.rs` is a child module of `snapshot`, so the path is
  `snapshot::caps::apply`, as in design §9.1.

## Suggested slices

- 1.0 `FeedItem` fields and `caps::apply` with the moved cap tests. Done
  when `cargo test scorer::snapshot::caps` passes.
- 2.0 `build` returns items and `global`. The old `build` stays as a test
  oracle. Done when `global_matches_v1` passes.
- 3.0 Skeleton and health read `global`. Done when
  `global_output_unchanged` passes and all four gates pass.

## Testing steps

1. Prepare the shell. Copy `.env.example` to `.env` and fill in the
   required values. Put a copy of a database with feed rows at
   `./upstage.db`, for example a production backup. Then run:

   ```
   export $(grep -v '^#' .env | xargs)
   export UPSTAGE_DB_PATH=./upstage.db
   FEED="at://$UPSTAGE_PUBLISHER_DID/app.bsky.feed.generator/$UPSTAGE_FEED_RKEY"
   SKEL="http://localhost:3000/xrpc/app.bsky.feed.getFeedSkeleton?feed=$FEED"
   ```

   Expected: The commands exit 0. `echo $SKEL` prints the feed URL.

2. Start the service. Wait for one scorer pass (60 s by default).

   ```
   cargo run --release -- run 2>&1 | tee run.log
   ```

   Expected: The service starts and stays up.

3. Read the served list length.

   ```
   curl -s localhost:3000/healthz | jq .snapshot_len
   ```

   Expected: A number above 0. Write it down as N.

4. Read the first page.

   ```
   curl -si "$SKEL&limit=30"
   ```

   Expected: Status 200. The header `Cache-Control: public, max-age=30` is
   present. The `feed` array has 30 items. The body has a `cursor` when N
   is more than 30.

5. Page through the whole feed. Do this inside one scorer interval. If a
   scorer pass starts during the loop, do the step again.

   ```
   c=""; : > posts.txt
   while :; do
     r=$(curl -s "$SKEL&limit=100${c:+&cursor=$c}")
     echo "$r" | jq -r '.feed[].post' >> posts.txt
     c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
   done
   wc -l < posts.txt; sort posts.txt | uniq -d | wc -l
   ```

   Expected: The first number is N. The second number is 0, so no post
   repeats.
