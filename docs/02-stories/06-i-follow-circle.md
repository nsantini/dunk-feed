# 06 — "I follow" circle end to end

- **Follows**: 01, 04, 05
- **PRD story**: See only pairs from my circle; First open builds my circle; Scroll without repeats
- **Size**: large
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.1, §6.2 step 1, §8, §9.2, §9.3, §9.4, §10

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
- [ ] AC11 — All four gates pass.

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
