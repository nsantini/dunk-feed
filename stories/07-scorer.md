# 07 — Scorer task: select, verify, promote, expire, snapshot, caps

- **Follows**: 02, 05, 06
- **PRD phase**: 1
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §7.1, §7.2, §7.3, §8, §12 (D9)

## Outcome

After this ships, the `scorer` task runs every `UPSTAGE_SCORER_INTERVAL_S`,
selects dirty candidate pairs, verifies them against the App View,
promotes or drops them, re-verifies young promoted pairs, expires old
rows, and swaps a fresh ranked snapshot into `Arc<RwLock<Arc<Vec<FeedItem>>>>`.
Once story 08 serves it, the feed reflects only App View-verified counts,
ordered by the caps in §7.3, and every pass logs its own counters.

## Non-goals

- Does not serve HTTP; story 08 reads the snapshot.
- Does not implement the follower-floor, author-state, or label guards;
  `guards.rs` here is a stub, replaced by story 10.
- Does not implement `upstage dump`; story 12.
- Does not tune `P`, `M`, or the weights; only reads config.

## Approach

Select-then-verify-then-promote runs as one pass per tick, not a
continuous stream, because §7.2 batches App View calls at 25 URIs and a
fixed cadence bounds call volume and pass duration. The snapshot is an
`Arc<RwLock<Arc<Vec<FeedItem>>>>`, swapped whole, so HTTP readers (story 08)
clone one `Arc` under a brief lock and never block on scoring. Promoted
pairs under 48 hours re-verify on a shorter interval; older ones freeze
their counts and only `rank` keeps decaying, per D9, so the feed neither
fills with stale items nor ages 30-day items out overnight.

## Files in scope

| Path | Change |
|---|---|
| `src/scorer/mod.rs` | The task: select, verify, guard (stub call), promote, expire, snapshot |
| `src/scorer/verify.rs` | Turns App View views into a `VerifiedPair` or a drop reason |
| `src/scorer/guards.rs` | Pass-through stub; real guards ship in story 10 |
| `src/scorer/snapshot.rs` | Ranked `Vec<FeedItem>`, page-cap ordering |
| `tests/fixtures/getposts_view{detached,blocked,notfound}.json`, `getposts_normal_quote.json` | Recorded `getPosts` bodies for the §8.2 embed-view table. `getposts_view_*` already exist from story 02. Record `getposts_normal_quote.json` live from the reference pair in TECH-DESIGN §1 before slice 1.0 |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | select | `candidate` pair, `first_seen_at` within 48h, either side dirty | Included; local `E` computed both sides; `dirty` cleared on read |
| BC2 | select, prefilter | `max(E_local) < P*fraction` or `D_local < M*fraction` | Excluded from this pass's App View calls |
| BC3 | verify batch | `Q` and `O` URIs | Batched 25 per `getPosts` call, capped at `UPSTAGE_APPVIEW_RPS` |
| BC4 | `Q.embed` view | `app.bsky.embed.record#view`, `record.$type == #viewRecord` | Normal quote, continue |
| BC5 | `Q.embed` view | `recordWithMedia#view`, nested `record.record.$type == #viewRecord` | Normal quote with media, continue |
| BC6 | `Q.embed` view | `record.$type == #viewDetached` | Drop, reason `detached` |
| BC7 | `Q.embed` view | `#viewBlocked` | Drop, reason `blocked` |
| BC8 | `Q.embed` view | `#viewNotFound` | Drop, reason `original_gone` |
| BC9 | `Q.embed` view | anything else | Drop, reason `not_a_post` |
| BC10 | `getPosts` response | a requested URI missing | Drop, reason `quote_gone` (`Q` missing) or `original_gone` (`O` missing) |
| BC11 | promote | qualifies on verified counts, passes the guard stub | Upsert `feed`, `pairs.state = 'promoted'` |
| BC12 | drop | fails a hard check | `pairs.state = 'dropped'` with the reason; `feed` row deleted if present |
| BC13 | re-verify | every `UPSTAGE_REVERIFY_INTERVAL_S`, promoted pairs within 48h | Re-runs verify, guard, promote; a pair no longer qualifying is demoted to `candidate`, its `feed` row deleted |
| BC14 | re-verify, boundary | promoted pair older than 48h | Never re-verified again; counts freeze; rank keeps recomputing (D9) |
| BC15 | expire | `candidate`/`dropped` pairs older than `UPSTAGE_CANDIDATE_TTL_H` | Deleted with their `counts` rows; URIs removed from the hot set |
| BC16 | expire | `feed` rows older than `UPSTAGE_FEED_TTL_D` | Deleted with their `pairs` rows |
| BC17 | ordering, base | sort | `feed` sorted by `rank DESC, quote_cid ASC` |
| BC18 | cap 1, one per original author per day | grouping | Group by `(original_did, day of quoted_at)`; keep only the top-rank item per group |
| BC19 | cap 2, one per quoting DID per 50 items | boundary | If the quoter appeared in the last 49 kept items, defer to the first spot where it does not; items deferred past the end are dropped |
| BC20 | snapshot swap | every pass | Recompute `rank` with current age, build the ordered `Vec<FeedItem{quote_uri, quote_cid, rank}>`, swap the `Arc`, write `last_scorer_pass` |
| BC21 | `ScorerError` (new error type) | batch fails after 3 retries | Logged; pass continues next tick, no promotion on a partial result |

## Acceptance criteria

- [ ] AC1 — Select applies the prefilter fraction. Checked by: `cargo test scorer::tests::prefilter_excludes_below_fraction`
- [ ] AC2 — Each embed-view row (BC4–BC10) maps to its continue or drop reason. Checked by: `cargo test scorer::verify::tests`
- [ ] AC3 — Re-verify demotes a pair that no longer qualifies. Checked by: `cargo test scorer::tests::reverify_demotes`
- [ ] AC4 — Promoted pairs over 48h are never re-verified. Checked by: `cargo test scorer::tests::old_promoted_pairs_skip_reverify`
- [ ] AC5 — Expiry removes stale rows, counts, and hot-set entries. Checked by: `cargo test scorer::tests::expiry_removes_stale_rows`
- [ ] AC6 — Cap 1 keeps only the top-rank item per original author per day. Checked by: `cargo test scorer::snapshot::tests::cap_one_per_author_per_day`
- [ ] AC7 — Cap 2 defers and drops at the 50-item boundary. Checked by: `cargo test scorer::snapshot::tests::cap_one_per_quoter_per_50`
- [ ] AC8 — The snapshot swap is atomic. Checked by: `cargo test scorer::tests::snapshot_swap_is_atomic`
- [ ] AC9 — All four gates pass.
- [ ] AC10 — A live pass promotes at least one known pair. Checked by: run by hand: `cargo test -- --ignored scorer_live_pass`, seeded with the reference pair in TECH-DESIGN §1 (`Q` = `at://did:plc:o7xt7svg2xtjbb4e2xqahqqc/app.bsky.feed.post/3mvxhe7uuck2n`, `O` = `at://did:plc:ofzkhjyyh4kl4a35wxgmobmm/app.bsky.feed.post/3mvxb5n76u22b`)

## Defaults taken

- `guards.rs` here is a pass-through stub
  (`fn check(_: &VerifiedPair) -> GuardResult::Pass`), so the pipeline
  compiles end to end; story 10 replaces it.
- Snapshot type: `std::sync::RwLock<Arc<Vec<FeedItem>>>`; readers clone the
  inner `Arc` under a lock held only for the clone.
- A batch failing after 3 retries is logged and skipped; the rest of the
  pass continues (§8.1).
- Cap 2 runs as one forward pass with a sliding window of the last 49
  quoter DIDs.
- Re-verify and select share `verify.rs`; only the input set differs, dirty
  candidates versus young promoted pairs.

## Suggested slices

- 1.0 `verify.rs`, the embed-view table, drop reasons, fixtures. Done when
  `cargo test scorer::verify` passes.
- 2.0 `mod.rs` select, verify, promote, re-verify, expire pipeline with the
  guard stub. Done when `cargo test scorer::tests` passes.
- 3.0 `snapshot.rs`, caps, atomic swap. Done when `cargo test
  scorer::snapshot` passes and all gates pass.
