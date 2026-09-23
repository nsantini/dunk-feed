# 06 — "I follow" circle end to end

- **Follows**: 01, 04, 05
- **PRD story**: See only pairs from my circle; First open builds my circle; Scroll without repeats
- **Size**: large
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.1, §6.2 step 1, §8, §9.2, §9.3, §9.4, §10
- **Flag**: `UPSTAGE_PERSONALISE` (default `false`)

## Release

This story merges and deploys with `UPSTAGE_PERSONALISE=false`, the
default. The served feed does not change, and the graph worker does not
start. The flag hides the circle filter, the first build, the worker and
the circle load at startup.

The migration to schema version 2 runs when the store opens, with either
flag value. It only adds tables.

To turn the feature on in one environment, set `UPSTAGE_PERSONALISE=true`
in the `.env` of that environment. Then restart with `docker compose -f
<file> up -d`. The flag applies to the whole process. There is no flag for
one user.

Rollback is `UPSTAGE_PERSONALISE=false` and a restart. The graph tables
stay in SQLite. Do not revert the pull request as a rollback. A binary
with schema version 1 refuses to open a version 2 database.

## Outcome

After this ships, with `UPSTAGE_PERSONALISE=true`, a verified viewer gets
only the pairs where an author is someone the viewer follows. The first
request returns an empty page at once and queues a build. The pairs appear
within 15 seconds. Circles are saved in SQLite, so a restart does not make
every viewer a first open. The diversity caps apply to each viewer's list
after the filter. A cursor resumes in the requester's own list.

## Non-goals

- Does not add "follows me" or degree 2. Story 07 and story 08 add them.
- Does not refresh or evict circles, and has one queue priority only.
  Story 09 adds refresh, priorities and eviction.
- Does not add metrics. Story 10 adds them.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11. With `false`, the served feed is unchanged and the
  worker does not start.

## Approach

Schema version 2 adds the four tables of design §8 in one transaction.
All four are created now, so later stories add no migration.
`store/viewers.rs` reads and writes `viewers`, `viewer_follows` and
`viewer_checks`. `graph/mod.rs` has `GraphHandle`: a
`RwLock<HashMap<ViewerDid, Arc<Circle>>>` that the handler reads. The
worker in `graph/queue.rs` takes `FirstBuild(viewer)` jobs from a
de-duplicated FIFO and runs `build::step_follows` (story 03) through
`PdsClient`. When the step completes, the worker saves the circle, swaps
the `Arc`, and increments `circle_version`. `http/viewer.rs` builds a
viewer list with `graph::filter::connected_indices` and
`snapshot::caps::apply`, and caches it per (viewer, generation,
circle_version), keeping the current and previous entry. The personalised
cursor carries the circle version (design §9.3), so a page resumes at the
exact index of the list that served the previous page.

## Files in scope

| Path | Change |
|---|---|
| `src/store/schema.rs` | `CURRENT_VERSION = 2`. Version 2 statements: `viewers`, `viewer_follows`, `viewer_checks`, `follows_cache` |
| `src/store/viewers.rs` (new) | `viewer_load_all`, `viewer_save_circle` (one transaction), `viewer_touch`, `viewer_delete` |
| `src/store/mod.rs` | `Store` methods for the above |
| `src/graph/mod.rs` | `GraphHandle`, `ViewerDid`, `CircleState`, `enqueue_first_build` |
| `src/graph/circle.rs` | Adds `state`, `last_request_at`, `d1_refreshed_at` |
| `src/graph/queue.rs` (new) | FIFO with de-duplication, worker task over `PdsClient` |
| `src/auth/did.rs` | After a queued token verifies, calls `enqueue_first_build` |
| `src/http/viewer.rs` (new) | `ViewerLists` cache, `list_for(viewer, snapshot)`, `drop_viewer` |
| `src/http/skeleton.rs` | Personalised branch serves the viewer list. Empty page when no circle or `building_d1` |
| `src/http/mod.rs` | `AppState` gets `GraphHandle` and `ViewerLists` |
| `src/ingest/mod.rs` | `run` loads circles and starts the worker when the switch is `true` |
| `src/config.rs` | `UPSTAGE_MAX_VIEWERS` and `UPSTAGE_GRAPH_RPS` defaults set to the story 04 measured values |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | migration | version 1 database | Adds the four tables in one transaction. `meta.schema_version` is 2. Version 1 tables unchanged |
| BC2 | migration | fresh database | Creates version 1 and version 2 tables. Version is 2 |
| BC3 | migration | failure part way | Nothing lands. Version stays 1 |
| BC4 | handler | verified viewer, no circle | Empty page in under 300 ms. `FirstBuild` enqueued. Circle created in `building_d1` |
| BC5 | handler | circle in `building_d1` | Empty page. No second job |
| BC6 | queue | same viewer enqueued twice | One job |
| BC7 | worker | step 1 completes | `follows` and `d2_sample` saved in one transaction. State `ready`. `d1_refreshed_at` set. Cached lists dropped |
| BC8 | worker | step 1 fails | Circle stays in `building_d1`. Job goes back in the queue after 30 s |
| BC9 | viewer list | ready circle | Item kept when `quote_did` or `original_did` is in `follows`. Then `caps::apply` on the kept indices |
| BC10 | viewer list | caps | A kept pair is never dropped because a pair outside the circle used its cap slot |
| BC11 | viewer list | fewer kept items than `limit` | Serves only those items. No `cursor`. No global items |
| BC12 | viewer list | empty circle or no matches | Empty page, 200 |
| BC13 | cache | key | (viewer, generation). Current and previous generation kept. Dropped when the circle changes |
| BC14 | cursor | resume, list for `(generation, circle_version)` still held | Page starts at `index + 1` when `items[index].cid` matches. Format `generation:circle_version:index:rank_bits:cid` |
| BC15 | cursor | from another viewer | Resumes in the requester's own list. No item outside the requester's list is served |
| BC16 | cursor | circle changes between pages, same generation | No item repeats. The feed does not end before the requester's last item |
| BC17 | cursor | list for `(generation, circle_version)` not held | `01` resume paths on the requester's current list (design D1) |
| BC18 | `run` | switch `true`, credentials missing | Worker not started. One error log line. Verified viewers get empty pages |
| BC19 | `run` | switch `true`, circles in SQLite | Loaded at startup. A circle in `building_d1` is enqueued again |
| BC20 | `run` | switch `false` | No worker, no load. Feed as in story 01 |
| BC21 | privacy | any log line | Contains no viewer DID |

## Acceptance criteria

- [ ] AC1 — Migration from version 1 and from empty works and is atomic. Checked by: `cargo test store::schema::tests::v2`
- [ ] AC2 — Circles save and load round-trip. Checked by: `cargo test store::viewers::tests`
- [ ] AC3 — The first request returns an empty page in under 300 ms and queues one job. Checked by: `cargo test http::skeleton::tests::first_open_empty_fast`
- [ ] AC4 — With a fake source, a viewer who follows 1,000 accounts sees "I follow" pairs within 15 s of the first request (paused clock). Checked by: `cargo test graph::queue::tests::i_follow_within_15s`
- [ ] AC5 — Caps apply after the filter. Checked by: `cargo test http::viewer::tests::caps_after_filter`
- [ ] AC6 — A short list is served with no fallback items. Checked by: `cargo test http::viewer::tests::short_list_no_fallback`
- [ ] AC7 — Another viewer's cursor shows nothing outside the requester's list. Checked by: `cargo test http::skeleton::tests::foreign_cursor`
- [ ] AC8 — A circle change during a scroll causes no repeat and no early end. Checked by: `cargo test http::skeleton::tests::circle_change_mid_scroll`
- [ ] AC9 — Circles survive a restart. Checked by: `cargo test graph::tests::restart_loads_circles`
- [ ] AC10 — With the switch `false`, output equals story 01 output. Checked by: `cargo test http::skeleton::tests::switch_off_unchanged`
- [ ] AC11 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- `UPSTAGE_PERSONALISE` stays `false` by default.
- After step 1 the state is `ready`. Story 07 and story 08 add
  `building_fm` and `building_d2` between them.
- The handler, not only the resolver, enqueues a first build for a
  verified viewer with no circle. This covers a cached key after a restart
  and, later, an evicted viewer.
- The cursor pins the circle version, not only the generation. A pure
  `(rank, cid)` search is not safe, because cap 2 defers items and a
  capped list is not in strict rank order.
- `last_request_at` is kept in memory on each request and written to
  SQLite at most once each 60 s for each viewer.
- The graph worker opens its own `Store` connection. SQLite runs in WAL
  mode.
- Missing credentials with the switch `true` log an error and do not stop
  `run`. Story 11 makes them required.

## Suggested slices

- 1.0 Schema version 2 and `store/viewers.rs`. Done when AC1 and AC2 pass.
- 2.0 `GraphHandle`, queue, worker, step 1, resolver and handler enqueue,
  restart load. Done when AC4 and AC9 pass.
- 3.0 `http/viewer.rs` list and cache. Done when AC5 and AC6 pass.
- 4.0 Skeleton personalised branch and cursor. Done when AC3, AC7, AC8
  and AC10 pass and all four gates pass.

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

4. Check the schema.

   ```
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT value FROM meta WHERE key='schema_version';
     SELECT name FROM sqlite_master WHERE type='table'
     AND name IN ('viewers','viewer_follows','viewer_checks','follows_cache')"
   ```

   Expected: `2`, then the four table names.

5. Make sure that the viewer has no circle yet.

   ```
   sqlite3 "$UPSTAGE_DB_PATH" "SELECT count(*) FROM viewers WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: 0. If not, use another test viewer.

6. Send the first request with the token.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: Status 200. The body is `{"feed":[]}` with no `cursor`. The
   header `Cache-Control: private, no-store` is present.

7. Read the circle state within 15 seconds of the first request.

   ```
   sleep 15; sqlite3 "$UPSTAGE_DB_PATH" "SELECT state FROM viewers WHERE viewer_did='$VIEWER_DID'"
   ```

   Expected: `ready`.

8. Read the feed again.

   ```
   curl -s -D - -o page.json -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   jq '.feed | length' page.json
   ```

   Expected: Status 200. The `feed` array holds items. The header
   `Cache-Control: private, no-store` is present.

9. Open five posts from `page.json` in the Bluesky app.

   ```
   jq -r '.feed[0:5][].post' page.json
   ```

   Expected: For each quote post, the viewer follows the quoter or the
   author of the quoted post.

10. Page through the viewer's whole list.

    ```
    c=""; : > posts.txt
    while :; do
      r=$(curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=100${c:+&cursor=$c}")
      echo "$r" | jq -r '.feed[].post' >> posts.txt
      c=$(echo "$r" | jq -r '.cursor // empty'); [ -z "$c" ] && break
    done
    wc -l < posts.txt; sort posts.txt | uniq -d | wc -l
    ```

    Expected: The second number is 0, so no post repeats. The last page
    has no `cursor`.

11. Log in as a second test viewer B, as in step 2. Keep the token in
    `TOKEN_B`. Send B's first page cursor with viewer A's token. Do this
    after B's circle is `ready`.

    ```
    CUR_B=$(curl -s -H "Authorization: Bearer $TOKEN_B" "$SKEL&limit=10" | jq -r .cursor)
    curl -s -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30&cursor=$CUR_B" \
      | jq -r '.feed[].post' | sort > foreign.txt
    comm -23 foreign.txt <(sort posts.txt) | wc -l
    ```

    Expected: 0. Viewer A sees nothing outside A's own list.

12. Restart the service. Send two requests with the token, 5 seconds
    apart.

    ```
    UPSTAGE_PERSONALISE=true cargo run --release -- run 2>&1 | tee run-2.log
    # in a second shell:
    curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 5; curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
    sqlite3 "$UPSTAGE_DB_PATH" "SELECT state FROM viewers WHERE viewer_did='$VIEWER_DID'"
    ```

    Expected: The second response holds items at once. The state is still
    `ready`. The viewer does not wait for a new build.

13. Restart the service with the flag on and no credentials.

    ```
    env -u BSKY_HANDLE -u BSKY_APP_PASSWORD UPSTAGE_PERSONALISE=true \
      cargo run --release -- run 2>&1 | tee run-nocreds.log
    ```

    Expected: One error line in the log. The service keeps running. The
    viewer gets empty pages.

14. Restart the service with the flag off.

    ```
    UPSTAGE_PERSONALISE=false cargo run --release -- run 2>&1 | tee run-off.log
    # in a second shell:
    curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
    ```

    Expected: Status 200 with the global items. The header `Cache-Control:
    public, max-age=30` is present.

15. Search all logs for the viewer DID.

    ```
    grep -c "$VIEWER_DID" run*.log
    ```

    Expected: 0 for each file.
