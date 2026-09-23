# 10 — Feed health metrics

- **Follows**: 09
- **PRD story**: Read feed health
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §11

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
- [ ] AC5 — All four gates pass.

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
