# 09 — Circles stay current and bounded

- **Follows**: 07, 08
- **PRD story**: My circle stays current; Limit the number of stored circles; First open builds my circle
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.3, §6.4, §7, §8, §10
- **Flag**: `UPSTAGE_PERSONALISE` (default `false`)

## Release

This story merges and deploys with `UPSTAGE_PERSONALISE=false`, the
default. The served feed does not change, and the scheduler does not
start. The flag hides the refresh, the refill, the cache clean-up and the
eviction.

To turn the feature on in one environment, set `UPSTAGE_PERSONALISE=true`
in the `.env` of that environment. Then restart with `docker compose -f
<file> up -d`. The flag applies to the whole process. There is no flag for
one user.

Rollback is `UPSTAGE_PERSONALISE=false` and a restart. A revert of the
pull request is also possible, because the story adds no migration.

## Outcome

After this ships, circles follow the viewer's network. A viewer who keeps
using the feed gets a degree-1 refresh every `UPSTAGE_GRAPH_REFRESH_AGE_H`.
Stale shared follows lists are fetched again when an active circle names
them. A new viewer's build goes before all refresh work. The service keeps
at most `UPSTAGE_MAX_VIEWERS` circles and removes idle ones after
`UPSTAGE_GRAPH_IDLE_EVICT_D`. A failed refresh keeps the old circle.

## Non-goals

- Does not add the hourly health line. Story 10 adds it.
- Does not refresh a viewer who sent no request since the last refresh.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11. With `false`, the scheduler does not start.

## Approach

`graph/queue.rs` gets three FIFO lanes: first build (0), refresh (1),
refill (2). The worker takes the next job from the highest priority lane
that is not empty. `graph/schedule.rs` runs each minute. It enqueues
`Refresh` and `Refill` jobs, removes idle circles, cleans the follows
cache and flushes `last_request_at`. A `Refresh` job builds a new `Circle`
off to the side with steps 1 and 2 and an empty `checked`. On success it
saves the circle in one transaction and swaps the `Arc`. On failure it
drops the new circle. LRU eviction runs before a first build creates a
circle.

## Files in scope

| Path | Change |
|---|---|
| `src/graph/queue.rs` | Three lanes, `Refresh(viewer)`, `Refill(account)`, priority order |
| `src/graph/schedule.rs` (new) | One-minute pass: refresh, refill, idle eviction, cache clean-up, `last_request_at` flush |
| `src/graph/build.rs` | `refresh`: builds a new circle, returns it or an error |
| `src/graph/mod.rs` | `evict(viewer, reason)`, LRU check in `enqueue_first_build` |
| `src/store/viewers.rs` | `viewer_replace_circle` (one transaction), `viewers_idle_since` |
| `src/store/follows_cache.rs` | `follows_delete_older_than` |
| `src/http/viewer.rs` | `drop_viewer` on eviction |
| `src/ingest/mod.rs` | `run` starts the scheduler when the switch is `true` |
| `src/config.rs` | `UPSTAGE_GRAPH_REFRESH_AGE_H` (6), `UPSTAGE_GRAPH_IDLE_EVICT_D` (7) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | worker | jobs in several lanes | Takes priority 0 first, then 1, then 2 |
| BC2 | scheduler | `d1_refreshed_at` older than the refresh age and a request after it | One `Refresh` job at priority 1 |
| BC3 | scheduler | no request since the last refresh | No job |
| BC4 | `Refresh` | runs | Steps 1 and 2 again, with `checked` empty at the start |
| BC5 | `Refresh` | succeeds | New circle saved in one transaction, then swapped in memory. `d1_refreshed_at` set. Cached lists dropped |
| BC6 | `Refresh` | fails at any point | Old circle stays in memory and SQLite, unchanged. Job goes back in the queue |
| BC7 | scheduler | cache entry older than `UPSTAGE_D2_REFRESH_AGE_H`, named by an active circle | One `Refill` job at priority 2 |
| BC8 | scheduler | cache entry not named by any circle, `fetched_at` older than 2 × `UPSTAGE_D2_REFRESH_AGE_H` | Removed from memory and SQLite |
| BC9 | scheduler | circle with no request for `UPSTAGE_GRAPH_IDLE_EVICT_D` | Removed from memory and SQLite. One `graph.evicted` line with `reason: "idle"` |
| BC10 | first build | count would pass `UPSTAGE_MAX_VIEWERS` | The circle with the oldest `last_request_at` is removed first. One `graph.evicted` line with `reason: "lru"` |
| BC11 | `graph.evicted` | content | `reason` only. No DID, no handle, no hash |
| BC12 | evicted viewer | next request | First-open behaviour: empty page and a new first build |
| BC13 | unfollow | after a refresh | Pairs connected only through the removed follow leave the list |
| BC14 | restart | circles older than the idle limit | Removed at the first scheduler pass |
| BC15 | scheduler | each pass | Writes pending `last_request_at` values to SQLite |

## Acceptance criteria

- [ ] AC1 — The worker takes jobs in priority order. Checked by: `cargo test graph::queue::tests::priority_order`
- [ ] AC2 — A first build queued after refresh jobs runs first. Checked by: `cargo test graph::queue::tests::first_build_jumps_refresh`
- [ ] AC3 — Refresh runs only for active viewers past the age, and resets `checked`. Checked by: `cargo test graph::schedule::tests::refresh_rules`
- [ ] AC4 — A failed refresh keeps the old circle in memory and SQLite. Checked by: `cargo test graph::build::tests::failed_refresh_keeps_old`
- [ ] AC5 — A follow or an unfollow shows in the list after the refresh. Checked by: `cargo test graph::tests::follow_changes_after_refresh`
- [ ] AC6 — Refill runs only for named stale entries. Unnamed old entries are removed. Checked by: `cargo test graph::schedule::tests::refill_and_cleanup`
- [ ] AC7 — Idle and LRU eviction remove the circle and write one line with the reason and no DID. Checked by: `cargo test graph::tests::eviction`
- [ ] AC8 — An evicted viewer gets an empty page and a new first build. Checked by: `cargo test http::skeleton::tests::evicted_viewer_first_open`
- [ ] AC9 — New config variables load with their defaults. Checked by: `cargo test config::tests::graph_refresh_defaults`
- [ ] AC10 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- `graph.evicted` is an `info` line with fields `event` and `reason`.
- The LRU check counts circles in memory, in any state.
- A `Refresh` or `Refill` that is already queued is not queued again.
- A failed job waits 30 s before it goes back in its lane, as in story 06.

## Suggested slices

- 1.0 Three lanes and priority order. Done when AC1 and AC2 pass.
- 2.0 `Refresh` with atomic replace, scheduler refresh rule. Done when AC3
  to AC5 pass.
- 3.0 Refill and cache clean-up. Done when AC6 passes.
- 4.0 Idle and LRU eviction. Done when AC7 to AC9 pass and all four gates
  pass.

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

2. Log in as a test viewer. Use an account on `bsky.social` that follows
   some authors in the feed.

   ```
   VIEWER_HANDLE=<test viewer handle>
   VIEWER_DID=$(curl -s "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=$VIEWER_HANDLE" | jq -r .did)
   ACCESS=$(curl -s -X POST https://bsky.social/xrpc/com.atproto.server.createSession \
     -H 'Content-Type: application/json' \
     -d "{\"identifier\":\"$VIEWER_HANDLE\",\"password\":\"<viewer app password>\"}" | jq -r .accessJwt)
   token() { curl -s -H "Authorization: Bearer $ACCESS" \
     "https://bsky.social/xrpc/com.atproto.server.getServiceAuth?aud=${1:-did:web:$UPSTAGE_HOSTNAME}&lxm=app.bsky.feed.getFeedSkeleton&exp=$(( $(date +%s) + ${2:-1800} ))" | jq -r .token; }
   TOKEN=$(token)
   ```

   Expected: `echo $VIEWER_DID` prints a DID. `echo $TOKEN` prints three
   parts with a dot between each part.

3. Start the service with the flag on and short refresh ages. `.env` must
   set `BSKY_HANDLE` and `BSKY_APP_PASSWORD`.

   ```
   UPSTAGE_PERSONALISE=true UPSTAGE_GRAPH_REFRESH_AGE_H=1 UPSTAGE_D2_REFRESH_AGE_H=1 \
     cargo run --release -- run 2>&1 | tee run.log
   ```

   Expected: The service starts.

4. Build the viewer's circle. Record the refresh time and the cache.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 30; curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT state, d1_refreshed_at FROM viewers WHERE viewer_did='$VIEWER_DID'"
   sqlite3 -separator ' ' "$UPSTAGE_DB_PATH" \
     "SELECT account_did, fetched_at FROM follows_cache ORDER BY account_did" > cache-1.txt
   ```

   Expected: `ready` and a refresh time. Write the time down.

5. Open the production feed in the Bluesky app. Pick a quote author X that
   the viewer does not follow. Follow X with the viewer account. Keep the
   DID of X in `X_DID`.

   ```
   X_DID=$(curl -s "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=<x handle>" | jq -r .did)
   ```

   Expected: `echo $X_DID` prints a DID.

6. Wait more than one hour. Send one request. Wait two scheduler passes.
   Read the refresh time.

   ```
   sleep 3660; TOKEN=$(token); curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30" > /dev/null; sleep 120
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT d1_refreshed_at FROM viewers WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: A time later than the time from step 4.

7. Page through the viewer's list. Search it for posts by X.

   ```
   c=""; : > posts.txt
   while :; do
     r=$(curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=100${c:+&cursor=$c}")
     echo "$r" | jq -r '.feed[].post' >> posts.txt
     c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
   done
   wc -l < posts.txt; sort posts.txt | uniq -d | wc -l
   grep -c "at://$X_DID/" posts.txt
   ```

   Expected: A number above 0, when X has a pair in the ranked list.

8. Compare the shared follows cache with step 4.

   ```
   sqlite3 -separator ' ' "$UPSTAGE_DB_PATH" \
     "SELECT account_did, fetched_at FROM follows_cache ORDER BY account_did" > cache-2.txt
   join cache-1.txt cache-2.txt | awk '$2 != $3' | wc -l
   ```

   Expected: A number above 0. The entries that the circle names were
   fetched again.

9. Unfollow X with the viewer account. Wait more than one hour. Send one
   request, wait two scheduler passes, and page through the list.

   ```
   sleep 3660; TOKEN=$(token); curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30" > /dev/null; sleep 120
   c=""; : > posts.txt
   while :; do
     r=$(curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=100${c:+&cursor=$c}")
     echo "$r" | jq -r '.feed[].post' >> posts.txt
     c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
   done
   wc -l < posts.txt; sort posts.txt | uniq -d | wc -l
   grep -c "at://$X_DID/" posts.txt
   ```

   Expected: 0, unless X is still connected through "follows me" or degree
   2.

10. Send no request for more than one hour. Read the refresh time before
    and after.

    ```
    sqlite3 "$UPSTAGE_DB_PATH" "SELECT d1_refreshed_at FROM viewers WHERE viewer_did='$VIEWER_DID'"; sleep 3720; sqlite3 "$UPSTAGE_DB_PATH" "SELECT d1_refreshed_at FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: The same time twice. A viewer with no request gets no
    refresh.

11. Restart the service with room for one circle. Log in as a second test
    viewer B, as in step 2. Send B's first request.

    ```
    UPSTAGE_PERSONALISE=true UPSTAGE_MAX_VIEWERS=1 cargo run --release -- run 2>&1 | tee run-lru.log
    # in a second shell:
    curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 5
    curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=30"; sleep 5
    grep '"graph.evicted"' run-lru.log
    sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: One `graph.evicted` line with `"reason":"lru"` and no DID.
    Then `0`: viewer A's circle is gone.

12. Send a request as viewer A.

    ```
    TOKEN=$(token); curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 2; sqlite3 "$UPSTAGE_DB_PATH" "SELECT state FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: The body is `{"feed":[]}`. A new first build starts for A. A
    second `graph.evicted` line with `"reason":"lru"` removes B.

13. Optional, because it takes one day. Restart with
    `UPSTAGE_GRAPH_IDLE_EVICT_D=1`. Send no request for 24 hours.

    ```
    UPSTAGE_PERSONALISE=true UPSTAGE_GRAPH_IDLE_EVICT_D=1 cargo run --release -- run 2>&1 | tee run-idle.log
    # 24 hours later:
    grep '"graph.evicted"' run-idle.log
    ```

    Expected: One `graph.evicted` line with `"reason":"idle"` and no DID.

14. Search all logs for the viewer DIDs.

    ```
    grep -c "$VIEWER_DID" run*.log
    ```

    Expected: 0 for each file. Do the same for viewer B.
