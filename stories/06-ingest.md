# 06 — Ingest task: embed, hot set, ops, stats

- **Follows**: 02, 04, 05
- **PRD phase**: 1
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §5.2, §5.3, §5.5, §12 (D3)

## Outcome

After this ships, the `ingest` task consumes events from
`jetstream::Client`, turns each into zero or one `Op`, and sends batches to
the store writer, without ever calling the network itself or querying
SQLite. The hot set of URI hashes tracks which posts matter, rebuilt from
`pairs` on start. Every 60 seconds the task logs events per second by
collection, hot set size, ops per second by type, gate hit rate, channel
depth, and lag — the number traffic-analysis §10 could not measure.

## Non-goals

- Does not call the App View. Verification is story 07.
- Does not decide promotion or ranking.
- Does not decrement counters on like or repost deletes; D3 explains why
  that data does not exist in the event.
- Does not select dirty rows for scoring; only sets `counts.dirty`.

## Approach

Quote detection (`embed.rs`) and the hot set (`hotset.rs`) are pure, so they
are unit-tested without a running store or network, matching §14's testing
strategy. The hot set stores `u64` hashes of URIs, not strings, using
`xxhash-rust`'s `xxh3_64`, because §3 allows `ahash` or `xxhash-rust` and a
direct byte-to-`u64` function needs no extra hasher plumbing. The rejected
alternative is decrementing on like and repost deletes, as the PRD asks:
§12's D3 shows that needs a table of every like that ever hit the hot set,
about 14M rows over 48 hours, for 1.1% of likes. The verifier corrects the
drift before any promotion, so it is not implemented.

## Files in scope

| Path | Change |
|---|---|
| `src/ingest/mod.rs` | Extends story 02's file: the task, event to `Op`, batching, stats |
| `src/ingest/embed.rs` | Exists from story 02. Only touched if a fixture exposes a gap in `detect` |
| `src/ingest/hotset.rs` | `HashSet<u64>` of URI hashes, pure |
| `Cargo.toml` | Adds `xxhash-rust` (feature `xxh3`) |
| `tests/fixtures/commit_post_quote.json`, `commit_post_reply.json`, `commit_post_other_embed.json`, `commit_like.json`, `commit_repost.json`, `commit_postgate.json`, `commit_post_delete.json`, `commit_like_delete.json` | Recorded commit payloads, one per row below |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `post` create, embed is a post quote, `did != original_did` | event to op | `InsertPair`; both URIs added to the hot set |
| BC2 | `post` create, has `reply.parent.uri`, parent in hot set | event to op | `Incr{replies}` |
| BC3 | `post` create, neither a quote nor a tracked reply | event to op | Dropped, no `Op` |
| BC4 | `post` delete, URI in hot set | event to op | `DeletePost` (removes the pair on either side, evicts feed rows) |
| BC5 | `post` delete, URI not in hot set | event to op | Dropped, no `Op` |
| BC6 | `like` create, `record.subject.uri` in hot set | event to op | `Incr{likes}` |
| BC7 | `like` create, subject not in hot set | event to op | Dropped, no `Op`; never creates a `counts` row |
| BC8 | `repost` create, subject in hot set | event to op | `Incr{reposts}` |
| BC9 | `like` or `repost` delete | event to op, D3 | **Always dropped.** The event carries only an rkey, not the subject URI, so it cannot be matched to a `counts` row. Local counters keep the phantom count; the verifier reads true counts before any promotion |
| BC10 | `postgate` create or update | event to op | One `Detach{quote_uri}` per `detachedEmbeddingUris` entry that is in the hot set |
| BC11 | `post` update | event to op | Treated as a create for the embed check (rare, 11 in 20k per traffic-analysis) |
| BC12 | embed detection, `embed.$type == "app.bsky.embed.record"` | quote rule | Quote at `embed.record.uri` |
| BC13 | embed detection, `embed.$type == "app.bsky.embed.recordWithMedia"` | quote rule | Quote at `embed.record.record.uri` |
| BC14 | embed detection, any other type, including `gallery`, `images`, `video`, `external`, unknown, or missing | quote rule | `NotAQuote`, not an error |
| BC15 | embed detection, URI does not parse as `at://<did>/app.bsky.feed.post/<rkey>` | quote rule | `NotAQuote` |
| BC16 | hot set | invariant | Holds `u64` hashes; a URI enters on `InsertPair`, leaves on expiry or `DeletePost`; rebuilt from `pairs` in one scan on start |
| BC17 | ingest stats | computed, every 60s | Logs events/s by collection, hot set size, ops/s by type, gate hit rate (share of like/repost events whose subject was in the hot set), channel depth, and lag in seconds between `payload.time` and now |
| BC18 | Jetstream `#info OutdatedCursor` | edge case | Logged at `warn`; `counts.dirty = 1` set for every row via a store op; ingest continues from the head, never replays event by event |

## Acceptance criteria

- [ ] AC1 — Every row of the event-to-op table (BC1–BC11) holds. Checked by: `cargo test ingest::tests`
- [ ] AC2 — Like and repost deletes never produce an `Op`. Checked by: `cargo test ingest::tests::like_repost_deletes_are_dropped`
- [ ] AC3 — Quote detection matches §5.3 for record, recordWithMedia, unknown embeds. Checked by: `cargo test ingest::embed::tests`
- [ ] AC4 — The hot set rebuilds from `pairs` on start. Checked by: `cargo test ingest::hotset::tests::rebuilds_from_pairs`
- [ ] AC5 — `OutdatedCursor` marks every counts row dirty and does not replay. Checked by: `cargo test ingest::tests::outdated_cursor_marks_dirty`
- [ ] AC6 — The stats line logs all six figures every 60s. Checked by: `cargo test ingest::tests::stats_line_contains_all_fields`
- [ ] AC7 — All four gates pass.
- [ ] AC8 — A live run survives 60s against real Jetstream without panicking. Checked by: run by hand: `cargo test -- --ignored ingest_live_smoke`

## Defaults taken

- Hash function: `xxhash-rust`, feature `xxh3`, `xxh3_64(uri.as_bytes())`.
  No seed rotation; determinism across restarts is not required because the
  set rebuilds from `pairs` each start.
- Batch cadence to the writer stays at 500 ms or 1,000 ops, owned by
  `store/writer.rs` (story 05); ingest only sends.
- Stats line: one `tracing::info!` event with named fields, JSON to stdout,
  emitted on a `tokio::time::interval(60s)` tick inside the ingest task.
- `Detach` for a URI not in the hot set is dropped silently.
- The `post` update embed re-check runs the same `detect()` as create; a
  URI already tracked as a quote is not re-inserted.

## Suggested slices

- 1.0 `embed.rs` quote detection and tests. Done when `cargo test
  ingest::embed` passes.
- 2.0 `hotset.rs`, `HashSet<u64>`, and rebuild-from-pairs. Done when `cargo
  test ingest::hotset` passes.
- 3.0 `mod.rs` event-to-op mapping, stats, `OutdatedCursor` handling. Done
  when `cargo test ingest` passes and all four gates pass.
