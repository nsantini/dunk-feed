# 03 — Graph probe

- **Follows**: 02
- **PRD story**: Limit the number of stored circles (measures the inputs for its defaults)
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §12, §6.1, §6.2, §9.2
- **Flag**: none, because this is an operator CLI. It writes nothing and does not change the served feed

## Release

This story ships a new CLI subcommand in the next deploy. It has no flag,
because viewers see no change. No process starts the probe. An operator
runs it by hand.

Rollback is a revert of the pull request and a new deploy. The probe
writes nothing, so there is no data to clean up.

## Outcome

After this ships, an operator runs a graph probe command for one or more
handles. The command logs in with `BSKY_HANDLE`, reads the current feed
rows from SQLite, and runs a first build for each handle in memory. It
writes nothing. It prints the cost, size, overlap and speed numbers that
story 04 needs to accept or reject the design.

The command is `upstage graph-probe --handle <h> [--handle <h> ...]`.

## Non-goals

- Does not start from `upstage run`. It is a CLI command, like `validate`
  and `dump`.
- Does not write to SQLite. The graph tables do not exist yet. Story 06
  adds them.
- Does not add a queue, a scheduler or persistence. Story 06 and story 09
  do this.
- Does not change the served feed. `UPSTAGE_PERSONALISE` does not exist
  yet.

## Approach

The build steps are pure async functions in `graph/build.rs`. They take a
`GraphSource` trait, so the probe and the later worker (story 06) share one
implementation. `PdsClient` implements `GraphSource`. A test fake counts
calls and pages. `Circle` in `graph/circle.rs` holds the sets of design
§6.1, without `state` and timestamps. The connection filter is one pure
function in `graph/filter.rs`, so the probe can time it on 100,000 items
and story 06 can call it. The probe collects a `ProbeReport` struct for
each handle and prints it. Printing is separate from collecting, so tests
check the numbers, not the text.

## Files in scope

| Path | Change |
|---|---|
| `AGENTS.md` | The `appview/` rule adds `graph/` and `graph_probe` as callers |
| `src/graph/mod.rs` (new) | Module root, `DidHash = u64`, `hash_did` |
| `src/graph/circle.rs` (new) | `Circle { follows, follows_me, checked, d2_sample }`, `heap_bytes()` |
| `src/graph/build.rs` (new) | `GraphSource` trait, `step_follows`, `step_follows_me`, `step_degree2`, each returns counts of calls, pages and time |
| `src/graph/filter.rs` (new) | `connected_indices(items, circle, d2_set, follows_me_depth) -> Vec<u32>` |
| `src/graph_probe.rs` (new) | `run`, `ProbeReport`, overlap, discovery share, order check, printing |
| `src/appview/pds.rs` | `impl GraphSource for PdsClient`, `resolve_handle`, `list_follow_records` |
| `src/cli.rs` | `GraphProbe { handle: Vec<String> }` subcommand |
| `src/config.rs` | `UPSTAGE_FOLLOWS_ME_DEPTH` (1000), `UPSTAGE_D2_FOLLOWS_SAMPLE` (100), `UPSTAGE_D2_FOLLOWS_DEPTH` (100) |
| `src/main.rs` | `mod graph; mod graph_probe;` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `step_follows` | viewer with N follows | Pages `getFollows` with `sort=latest` and `limit=100` to the end. `follows` has N hashes. `d2_sample` is the first `UPSTAGE_D2_FOLLOWS_SAMPLE` DIDs in response order |
| BC2 | `step_follows_me` | ranked items | Candidates are the quoter and original DIDs of the first `UPSTAGE_FOLLOWS_ME_DEPTH` ranked items, minus `follows` and `checked`, with no duplicates |
| BC3 | `step_follows_me` | candidates | Sent 30 per `getRelationships` call. Each sent DID goes into `checked`. Each `followedBy` DID goes into `follows_me` |
| BC4 | `step_degree2` | each account in `d2_sample` | One `getFollows` page with `limit=UPSTAGE_D2_FOLLOWS_DEPTH` when the depth is 100 or less. Result is a sorted `Vec<u64>` in a local map. An account already in the map costs no call |
| BC5 | `connected_indices` | item author in `follows` or the degree-2 set | Kept at any depth |
| BC6 | `connected_indices` | item author only in `follows_me` | Kept only when the item index is below `follows_me_depth` |
| BC7 | `connected_indices` | output | Indices in ranked order, no duplicates |
| BC8 | probe | `BSKY_HANDLE` or `BSKY_APP_PASSWORD` missing | Exits 1 before any network call |
| BC9 | probe | no `--handle` | Exits 1 with a usage message |
| BC10 | probe | each handle | Prints calls, pages and time for each step. Prints sizes of `follows`, `follows_me` and the degree-2 set. Prints circle bytes and shared-entry bytes |
| BC11 | probe | all handles | Prints the share of degree-2 accounts that two or more handles share, and the bytes saved |
| BC12 | probe | filter timing | Builds 100,000 synthetic items from the real rows (repeated) and prints the time of `connected_indices` plus `caps::apply` |
| BC13 | probe | circle pairs | Prints the count of kept items with `promoted_at` in the last 24 hours, and the share that pass only through degree 2 |
| BC14 | probe | order check | Compares the first `getFollows` page with the newest `app.bsky.graph.follow` records from `com.atproto.repo.listRecords`. Prints `match` or the first difference |
| BC15 | probe | SQLite | Reads feed rows only. No insert, update or delete |
| BC16 | probe output | any line | Handles are printed. Viewer DIDs are not printed |

## Acceptance criteria

- [ ] AC1 — The three steps make the expected calls against a fake source. Checked by: `cargo test graph::build::tests`
- [ ] AC2 — The filter keeps `follows_me` authors only within the depth, and keeps degree-2 authors at every depth. Checked by: `cargo test graph::filter::tests`
- [ ] AC3 — Overlap and discovery share are correct on fixed inputs. Checked by: `cargo test graph_probe::tests::report_numbers`
- [ ] AC4 — Missing credentials or no handle exit 1 before any network call. Checked by: `cargo test graph_probe::tests::preflight`
- [ ] AC5 — The new config variables load with their defaults. Checked by: `cargo test config::tests::graph_depth_defaults`
- [ ] AC6 — A real run against two handles prints every field. Checked by: run by hand: `cargo test -- --ignored graph_probe_live`
- [ ] AC7 — `AGENTS.md` names `graph/` and `graph_probe` as `appview/` callers. Checked by: manual review.
- [ ] AC8 — All four gates pass. Checked by: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-features` and `cargo build --release`.

## Defaults taken

- `graph/filter.rs` is a new file. Design §3 puts the viewer list in
  `http/viewer.rs`. The pure filter moves to `graph/` so the probe does
  not depend on `http/`. `http/viewer.rs` (story 06) owns the cache.
- The order check calls `listRecords` at `UPSTAGE_PDS_URL`. If that PDS
  does not host the repo, the probe prints `order check: skipped` with the
  status, and continues.
- Memory bytes are an estimate: entries × 8 bytes, plus the `HashSet`
  capacity overhead. It is not an allocator measurement.
- `graph/` and `graph_probe` call `appview/`, as design §15 allows. This
  story writes that rule into `AGENTS.md`, so reviewers do not flag it.

## Suggested slices

- 1.0 `graph/` types, `GraphSource`, the three steps, fake source tests.
  Done when `cargo test graph::build` passes.
- 2.0 `connected_indices` with tests. Done when `cargo test graph::filter`
  passes.
- 3.0 `graph_probe.rs`, CLI wiring, config, `PdsClient` impl. Done when
  `cargo test graph_probe` passes and all four gates pass.

## Testing steps

1. Prepare the shell. Copy `.env.example` to `.env`. Fill in the required
   values, `BSKY_HANDLE` and `BSKY_APP_PASSWORD`. Put a copy of a database
   with feed rows at `./upstage.db`. Then run:

   ```
   export $(grep -v '^#' .env | xargs)
   export UPSTAGE_DB_PATH=./upstage.db
   ```

   Expected: The commands exit 0.

2. Run the probe without credentials.

   ```
   env -u BSKY_HANDLE cargo run --release -- graph-probe --handle bsky.app
   ```

   Expected: The command exits 1 before any network call.

3. Run the probe with no handle.

   ```
   cargo run --release -- graph-probe
   ```

   Expected: The command exits 1 with a usage message.

4. Record a checksum of the database. Do not run `upstage run` during this
   step or the next one.

   ```
   shasum -a 256 "$UPSTAGE_DB_PATH"
   ```

   Expected: A checksum. Write it down.

5. Run the probe on two handles. Use one handle with fewer than 200
   follows and one with more than 1,000.

   ```
   cargo run --release -- graph-probe --handle <h1> --handle <h2> | tee probe.txt
   ```

   Expected: For each handle: calls, pages and time for each step. Sizes
   of `follows`, `follows_me` and the degree-2 set. Circle bytes and
   shared-entry bytes. For the run: the shared degree-2 share and the
   bytes saved. The time of `connected_indices` plus `caps::apply` on
   100,000 items. The circle pairs in 24 hours and the degree-2-only
   share. One order check line: `match`, the first difference, or `order
   check: skipped` with the status.

6. Check the database again.

   ```
   shasum -a 256 "$UPSTAGE_DB_PATH"
   ```

   Expected: The same checksum as in step 4. The probe wrote nothing.

7. Search the output for the viewer DID of each handle.

   ```
   grep -c "$(curl -s "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=<h1>" | jq -r .did)" probe.txt
   ```

   Expected: 0. Do the same for `<h2>`. The handles appear in the output.

8. Run the live probe test.

   ```
   cargo test -- --ignored graph_probe_live
   ```

   Expected: The test passes.
