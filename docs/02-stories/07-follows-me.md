# 07 — "Follows me" connections

- **Follows**: 06
- **PRD story**: See only pairs from my circle; First open builds my circle
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.1, §6.2 step 2, §8, §9.2
- **Flag**: `UPSTAGE_PERSONALISE` (default `false`)

## Release

This story merges and deploys with `UPSTAGE_PERSONALISE=false`, the
default. The served feed does not change, and the graph worker does not
start. The flag hides the "follows me" step and the pairs it adds.

To turn the feature on in one environment, set `UPSTAGE_PERSONALISE=true`
in the `.env` of that environment. Then restart with `docker compose -f
<file> up -d`. The flag applies to the whole process. There is no flag for
one user.

Rollback is `UPSTAGE_PERSONALISE=false` and a restart. A revert of the
pull request is also possible, because the story adds no migration. The
`viewer_checks` rows stay in SQLite.

## Outcome

After this ships, the first build runs a second step. It asks
`getRelationships` which authors of the top ranked pairs follow the
viewer. A pair whose only link is "this author follows me" enters the
viewer's feed when it is in the top `UPSTAGE_FOLLOWS_ME_DEPTH` items. The
pairs from step 1 stay in the feed.

## Non-goals

- Does not check "follows me" below `UPSTAGE_FOLLOWS_ME_DEPTH`.
- Does not reset `checked` on refresh. Story 09 does this with the
  degree-1 refresh.
- Does not add degree 2. Story 08 adds it.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11.

## Approach

The worker runs `build::step_follows_me` (story 03) after step 1, in the
same `FirstBuild` job. The state moves `building_d1` → `building_fm` →
`ready`. The candidates come from the current snapshot's ranked items, not
from `global`. After the step, the worker saves `viewer_checks` and swaps
the circle. The filter already supports `follows_me` with a depth (story
03). The viewer list passes `UPSTAGE_FOLLOWS_ME_DEPTH` to it.

## Files in scope

| Path | Change |
|---|---|
| `src/graph/queue.rs` | `FirstBuild` runs step 2 after step 1 |
| `src/graph/circle.rs` | `CircleState::BuildingFm` |
| `src/store/viewers.rs` | Save and load `viewer_checks` (`author_hash`, `follows_me`, `checked_at`) |
| `src/http/viewer.rs` | Passes `follows_me` and the depth to `connected_indices` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | step 2 | candidates | Quoter and original DIDs of the first `UPSTAGE_FOLLOWS_ME_DEPTH` ranked items, minus `follows` and `checked` |
| BC2 | step 2 | calls | 30 DIDs for each `getRelationships` call |
| BC3 | step 2 | completes | `checked` and `follows_me` saved in one transaction. State `ready`. Cached lists dropped |
| BC4 | step 2 | fails part way | The circle keeps its step 1 data and state `ready`. The DIDs checked before the failure are kept. The job goes back in the queue |
| BC5 | viewer list | author only in `follows_me`, item index less than the depth | Kept |
| BC6 | viewer list | author only in `follows_me`, item index equal to or more than the depth | Not kept |
| BC7 | viewer list | after step 2 | Every item kept after step 1 is still kept |
| BC8 | handler | state `building_fm` | Serves the list from step 1 data |
| BC9 | restart | saved `viewer_checks` | `follows_me` and `checked` load with the circle |

## Acceptance criteria

- [ ] AC1 — Step 2 sends only unchecked candidates, 30 per call. Checked by: `cargo test graph::queue::tests::step2_candidates`
- [ ] AC2 — "Follows me" pairs are kept only within the depth. Checked by: `cargo test http::viewer::tests::follows_me_depth`
- [ ] AC3 — Items from step 1 stay after step 2. Checked by: `cargo test http::viewer::tests::step2_keeps_step1_items`
- [ ] AC4 — A failure in step 2 keeps step 1 data. Checked by: `cargo test graph::queue::tests::step2_failure_keeps_step1`
- [ ] AC5 — `viewer_checks` round-trips. Checked by: `cargo test store::viewers::tests::checks_round_trip`
- [ ] AC6 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- "Top `UPSTAGE_FOLLOWS_ME_DEPTH` items" means positions in the uncapped
  ranked list of the snapshot, as in design §6.2 and §9.2.
- After a failed step 2, the state is `ready` with partial `checked`. The
  retry sends only the DIDs not yet checked.

## Suggested slices

- 1.0 Step 2 in the worker, `viewer_checks` persistence. Done when AC1,
  AC4 and AC5 pass.
- 2.0 Viewer list uses `follows_me` with the depth. Done when AC2 and AC3
  pass and all four gates pass.

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

3. Start the service with the flag on. `.env` must set `BSKY_HANDLE` and
   `BSKY_APP_PASSWORD`. Wait for one scorer pass.

   ```
   UPSTAGE_PERSONALISE=true cargo run --release -- run 2>&1 | tee run.log
   ```

   Expected: The service starts. `curl -s localhost:3000/healthz | jq
   .snapshot_len` prints a number above 0.

4. Make sure that the viewer has no circle yet.

   ```
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: 0. If not, use another test viewer.

5. Send the first request. Then read the circle state each second.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   for i in $(seq 30); do sqlite3 "$UPSTAGE_DB_PATH" "SELECT state FROM viewers WHERE viewer_did='$VIEWER_DID'"; sleep 1; done
   ```

   Expected: `building_d1`, then `building_fm`, then `ready`.

6. Read the saved checks.

   ```
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*), sum(follows_me) FROM viewer_checks
     WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: The count is above 0. The sum is the number of checked
   authors that follow the viewer.

7. Page through the viewer's list.

   ```
   c=""; : > with-fm.txt
   while :; do
     r=$(curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=100${c:+&cursor=$c}")
     echo "$r" | jq -r '.feed[].post' >> with-fm.txt
     c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
   done
   wc -l < with-fm.txt; sort with-fm.txt | uniq -d | wc -l
   ```

   Expected: The second number is 0, so no post repeats.

8. Restart the service with a "follows me" depth of 0. Page through the
   list again. Do steps 7 and 8 inside one scorer interval.

   ```
   UPSTAGE_FOLLOWS_ME_DEPTH=0 UPSTAGE_PERSONALISE=true \
     cargo run --release -- run 2>&1 | tee run-2.log
   # in a second shell, after the key is in the cache:
   c=""; : > step1-only.txt
   while :; do
     r=$(curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=100${c:+&cursor=$c}")
     echo "$r" | jq -r '.feed[].post' >> step1-only.txt
     c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
   done
   wc -l < step1-only.txt; sort step1-only.txt | uniq -d | wc -l
   comm -23 <(sort step1-only.txt) <(sort with-fm.txt) | wc -l
   ```

   Expected: The last number is 0. Every item from step 1 is also in the
   list with "follows me" pairs.

9. Restart the service with the default depth. Read the saved checks
   again.

   ```
   UPSTAGE_PERSONALISE=true cargo run --release -- run 2>&1 | tee run-3.log
   # in a second shell:
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewer_checks WHERE viewer_did='$VIEWER_DID'"
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT state FROM viewers WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: The same count as in step 6. The state is still `ready`.
