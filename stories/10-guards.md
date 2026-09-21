# 10 — Guards: follower floor, author state, labels

- **Follows**: 07
- **PRD phase**: 3
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §9

## Outcome

After this ships, the scorer's `guards.rs` enforces the follower floor on
`O`'s author, drops pairs whose author is deactivated, deleted, or taken
down, and drops pairs carrying a label in `DUNK_DROP_LABELS`. Before the
follower floor drops anything, the service logs one day of `O` authors'
follower-count distribution, so the default of 2,000 is set on this
deployment's real data, not a guess.

## Non-goals

- Does not subscribe to a third-party labeler; only labels already on the
  fetched views are read, per D6.
- Does not implement per-author or per-quoter caps; those already ship in
  story 07's `snapshot.rs`.
- Does not change the block or detach handling already in
  `scorer/verify.rs`.
- Does not retune `DUNK_FOLLOWER_FLOOR` itself; this story only measures
  and logs.

## Approach

Guards read `getProfiles` results cached in the `authors` table for 24
hours, rather than a fresh call per pair, because §9 counts this as free
once the cache is in place. The one-day logging step runs the real guard
logic but only logs its would-be drops for the first `DUNK_GUARD_LOG_ONLY_H`
hours (default 24) after this ships, rather than a separate script, so the
measurement uses the exact code path that later drops pairs.

## Files in scope

| Path | Change |
|---|---|
| `src/scorer/guards.rs` | Follower floor, author state, labels; replaces story 07's stub |
| `src/store/authors.rs` | 24h cache read and write for follower count and labels |
| `src/config.rs` | Adds `DUNK_GUARD_LOG_ONLY_H` (new, see Defaults taken). `DUNK_DROP_LABELS` exists from story 01 |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | follower floor | `O`'s author `followers < DUNK_FOLLOWER_FLOOR` | Drop, reason `follower_floor` |
| BC2 | follower floor | `DUNK_FOLLOWER_FLOOR = 0` | Guard disabled; no pair drops on follower count |
| BC3 | follower floor | `authors` row for `O`'s author cached within 24h | Guard reads the cache; no new `getProfiles` call |
| BC4 | follower floor | `authors` row missing or older than 24h | One `getProfiles` call per 25 new original authors; result cached |
| BC5 | author state | author missing from the `getProfiles` response | Drop, reason `author_inactive` |
| BC6 | author state | author carries a `!takedown` label | Drop, reason `author_inactive` |
| BC7 | author state | `getPosts` omits the post entirely | Drop, reason `author_inactive`, consistent with story 07's `quote_gone`/`original_gone` |
| BC8 | labels | any label on `Q`, `O`, or either author's profile matches `DUNK_DROP_LABELS` | Drop, reason `labelled` |
| BC9 | labels | label present but not in `DUNK_DROP_LABELS` | Pair unaffected |
| BC10 | postgate `disableRule` on `O` alone | edge case, traffic-analysis §4 | Does not drop existing pairs; it only blocks new quotes at the app level |
| BC11 | log-only window | within `DUNK_GUARD_LOG_ONLY_H` hours of the first scorer pass | Guard decisions are logged with the pair's follower count and the reason it would have used; no pair is dropped by the follower floor |
| BC12 | log-only window | elapsed | The follower floor becomes live; logging of would-be drops stops |

## Acceptance criteria

- [ ] AC1 — The follower floor drops pairs below the threshold, and 0 disables it. Checked by: `cargo test scorer::guards::tests::follower_floor`
- [ ] AC2 — Author-state checks (missing, takedown, quote_gone) all map to `author_inactive`. Checked by: `cargo test scorer::guards::tests::author_state`
- [ ] AC3 — Labels in `DUNK_DROP_LABELS` drop pairs; others pass. Checked by: `cargo test scorer::guards::tests::labels`
- [ ] AC4 — The 24h `authors` cache avoids a repeat `getProfiles` call. Checked by: `cargo test store::authors::tests::cache_hit_within_24h`
- [ ] AC5 — The log-only window logs without dropping, then drops after it elapses. Checked by: `cargo test scorer::guards::tests::log_only_window`
- [ ] AC6 — All four gates pass.
- [ ] AC7 — A live pass logs a real follower distribution for known accounts. Checked by: run by hand: `cargo test -- --ignored guards_live_follower_distribution`

## Defaults taken

- `DUNK_DROP_LABELS` comes from story 01's config parser. This story only reads it.
- `DUNK_GUARD_LOG_ONLY_H` is likewise new, not in §4. Default 24, per this
  story's own requirement to log for one day before dropping.
- The one-day clock starts from this story's first successful scorer pass,
  recorded as a new `meta` key `guard_log_only_since`, so a restart during
  the window does not reset it.
- Follower-distribution log line: one `tracing::info!` per pass during the
  window, carrying the full histogram of `O` authors' follower counts seen
  that pass.

## Superseded on 2026-09-21 (review)

BC11 and BC12 changed: the follower floor drops from the first pass; the
window (`DUNK_GUARD_HISTOGRAM_H`, was `DUNK_GUARD_LOG_ONLY_H`) only controls
histogram logging and reopens when the floor changes. Guards run after the
score check. TECH-DESIGN §9 has the reason.

## Suggested slices

- 1.0 `store/authors.rs` 24h cache. Done when `cargo test store::authors`
  passes.
- 2.0 `scorer/guards.rs`: follower floor, author state, labels, log-only
  window. Done when `cargo test scorer::guards` passes and all four gates
  pass.
