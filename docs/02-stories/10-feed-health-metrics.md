# 10 — Feed health metrics

- **Follows**: 09
- **PRD story**: Read feed health
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §11
- **Flag**: `UPSTAGE_PERSONALISE` (default `false`)

## Release

This story merges and deploys with `UPSTAGE_PERSONALISE=false`, the
default. The served feed does not change, and the metrics task does not
start. The flag hides the `graph.health` line.

To turn the feature on in one environment, set `UPSTAGE_PERSONALISE=true`
in the `.env` of that environment. Then restart with `docker compose -f
<file> up -d`. The flag applies to the whole process. There is no flag for
one user.

Rollback is `UPSTAGE_PERSONALISE=false` and a restart. A revert of the
pull request is also possible, because the story adds no migration.

## Outcome

After this ships, the service writes one `graph.health` JSON log line each
hour. The operator can read how full the viewers' feeds are, how much of
each feed comes from degree 2, how many circles were removed, and how busy
the graph client and queue are. A test proves that no log line at any
level names a viewer.

## Non-goals

- Does not add a metrics endpoint or an exporter. The log line is the only
  output.
- Does not set targets for the values. The Brief sets them after one week
  of data.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11. With `false`, the metrics task does not start.

## Approach

`graph/metrics.rs` holds counters and the hourly task. `PdsClient` counts
calls by method. `graph::evict` counts removals by reason. The queue
reports its depth for each lane. Each hour, the task builds each active
viewer's current list through `http/viewer.rs`. It counts the items with
`promoted_at` in the last 24 hours and the items that pass only through
degree 2. It computes the medians and the zero share, writes one line, and
resets the hourly counters. A pure function computes the fields from the
inputs, so tests do not need a clock or a log.

## Files in scope

| Path | Change |
|---|---|
| `src/graph/metrics.rs` (new) | `HealthInputs`, `health_line(inputs) -> serde_json::Value`, hourly task, counters |
| `src/graph/filter.rs` | Reports, for each kept index, if it passed only through degree 2 |
| `src/appview/pds.rs` | Call counter by method |
| `src/graph/mod.rs` | Eviction counter by reason |
| `src/graph/queue.rs` | `depth()` for each lane |
| `src/ingest/mod.rs` | `run` starts the metrics task when the switch is `true` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `graph.health` | each hour | One `info` JSON line with `event: "graph.health"` and the fields below |
| BC2 | `active_viewers` | value | Circles with a request in the last `UPSTAGE_GRAPH_IDLE_EVICT_D` |
| BC3 | `median_new_pairs_24h` | value | Median over active viewers of list items with `promoted_at` in the last 24 hours |
| BC4 | `zero_share` | value | Share of active viewers with 0 such items. `0.0` when there are no active viewers |
| BC5 | `median_discovery_share` | value | Median over active viewers with 1 or more circle items of (degree-2-only items ÷ all items) |
| BC6 | `evicted_1h` | value | Object `{ "idle": n, "lru": n }` for the last hour |
| BC7 | `graph_calls_1h` | value | Object of PDS calls by method for the last hour |
| BC8 | `queue_depth` | value | Object `{ "first_build": n, "refresh": n, "refill": n }` |
| BC9 | counters | after each line | Hourly counters go back to 0 |
| BC10 | median | even count | Mean of the two middle values |
| BC11 | privacy | every log line, every level | No viewer DID. No handle or hash of a viewer |

## Acceptance criteria

- [ ] AC1 — `health_line` gives the right fields for fixed inputs, including no viewers and an even count. Checked by: `cargo test graph::metrics::tests::health_line`
- [ ] AC2 — The degree-2-only flag is right for items with mixed connections. Checked by: `cargo test graph::filter::tests::degree2_only_flag`
- [ ] AC3 — Counters reset after each line. Checked by: `cargo test graph::metrics::tests::counters_reset`
- [ ] AC4 — A run of first build, refresh, eviction and one health line at `trace` level writes no viewer DID in any line. Checked by: `cargo test graph::metrics::tests::no_viewer_did_in_logs`
- [ ] AC5 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- AC4 captures `tracing` output with a test subscriber at `trace` level
  and searches it for each test viewer DID.
- A viewer with 0 circle items is left out of the discovery median. The
  share has no value for that viewer.
- The first line is written one hour after startup, not at startup.

## Suggested slices

- 1.0 Counters and the degree-2-only flag. Done when AC2 passes.
- 2.0 `health_line`, the hourly task and the reset. Done when AC1 and AC3
  pass.
- 3.0 Log privacy test. Done when AC4 passes and all four gates pass.

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

3. Start the service with the flag on and `trace` logs. `.env` must set
   `BSKY_HANDLE` and `BSKY_APP_PASSWORD`.

   ```
   UPSTAGE_PERSONALISE=true UPSTAGE_LOG=trace cargo run --release -- run 2>&1 | tee run.log
   ```

   Expected: The service starts.

4. Build the viewer's circle.

   ```
   curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"; sleep 30; curl -si -H "Authorization: Bearer $TOKEN" "$SKEL&limit=30"
   ```

   Expected: The second response holds items.

5. Wait one hour after startup. Read the health line.

   ```
   sleep 3600; grep '"graph.health"' run.log | jq .
   ```

   Expected: One line. It has `active_viewers`, `median_new_pairs_24h`,
   `zero_share`, `median_discovery_share`, `evicted_1h`, `graph_calls_1h`
   and `queue_depth`.

6. Check the values of the health line.

   ```
   grep '"graph.health"' run.log | head -1 | jq .
   ```

   Expected: `active_viewers` is 1. `zero_share` is from 0 to 1.
   `evicted_1h` has the keys `idle` and `lru`. `queue_depth` has the keys
   `first_build`, `refresh` and `refill`. `graph_calls_1h` counts calls by
   method.

7. Send no request. Wait one more hour. Read the second line.

   ```
   sleep 3600; grep '"graph.health"' run.log | tail -1 | jq .
   ```

   Expected: The counts cover only the second hour. They do not add the
   counts of the first hour.

8. Search the log for the viewer DID and handle.

   ```
   grep -c "$VIEWER_DID" run.log; grep -c "$VIEWER_HANDLE" run.log
   ```

   Expected: 0 and 0.

9. Restart the service with the flag off. Wait one hour.

   ```
   UPSTAGE_PERSONALISE=false cargo run --release -- run 2>&1 | tee run-off.log
   # one hour later:
   grep -c '"graph.health"' run-off.log
   ```

   Expected: 0. The metrics task does not start.
