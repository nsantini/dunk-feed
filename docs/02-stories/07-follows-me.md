# 07 — "Follows me" connections

- **Follows**: 06
- **PRD story**: See only pairs from my circle; First open builds my circle
- **Size**: small
- **Design**: docs/02-TECH-DESIGN-network-feed.md §6.1, §6.2 step 2, §8, §9.2

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
- [ ] AC6 — All four gates pass.

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
