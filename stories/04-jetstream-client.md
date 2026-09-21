# 04 — Jetstream v2 client

- **Follows**: 01
- **PRD phase**: 1
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §5.1, §12 (D4)

## Outcome

After this ships, `jetstream::Client` connects to Jetstream v2, fetches and
caches the zstd dictionary, decodes frames, and exposes `async fn
next(&mut self) -> Result<Event>` that hides reconnects and backoff. The
ingest task (story 06) calls `next()` in a loop and receives typed commit
events for `post`, `like`, `repost`, and `postgate`, resuming from the last
committed `seq` after any disconnect.

## Non-goals

- Does not decide what to do with an event; that is `ingest/mod.rs`, story
  06.
- Does not write to SQLite or a hot set.
- Does not filter by collection or DID beyond the four collections §5.1
  names.
- Does not implement Jetstream v1 (`/subscribe`, `time_us`); D4 rejects it.

## Approach

The client is hand-written against v2's `subscribeEvents` endpoint, about
200 lines, because TECH-DESIGN §1 found the Rust Jetstream crates too thin
or stale to trust. It uses `tokio-tungstenite` with rustls, already in §3's
dependency list, not a bespoke TCP and TLS stack. Reconnection backs off
1s, 2s, 4s up to a 60s cap and always resumes from the last committed `seq`,
per §5.1 step 5, rather than reconnecting from the head, because a gap-free
resume is the whole point of checkpointing.

## Files in scope

| Path | Change |
|---|---|
| `src/jetstream/mod.rs` | Client struct, `next()`, reconnect and backoff |
| `src/jetstream/client.rs` | Dictionary fetch and cache, connect, decode, reconnect, resume |
| `src/jetstream/event.rs` | serde types for the v2 envelope and payloads |
| `Cargo.toml` | Adds `tokio-tungstenite` (rustls), `zstd` |
| `tests/fixtures/zstd_dictionary.bin` | Recorded dictionary bytes |
| `tests/fixtures/commit_post.zst`, `commit_like.zst`, `commit_postgate.zst`, `info_outdated_cursor.json` | Recorded frames, one per case below |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | envelope `$type` | not `"message"` | Ignored, not an error (forward-compat) |
| BC2 | `payload.$type` | `#commit` | Decoded into `Event::Commit{..}` with the fields §5.1.4 lists |
| BC3 | `payload.$type` | `#info`, `OutdatedCursor` | Returned to the caller as `Event::OutdatedCursor`; the client itself continues from the head per §5.4 |
| BC4 | `payload.$type` | `#identity`, `#account`, `#sync` | Consumed internally; the client calls `next()` again rather than returning these to the caller |
| BC5 | zstd frame | malformed or corrupt | Logged at `warn`, frame skipped, connection stays open; never panics |
| BC6 | WebSocket | closed or errored | Backs off `1s, 2s, 4s, ..., 60s` (capped), reconnects with `cursor=<last committed seq>` |
| BC7 | zstd dictionary fetch | fails on cold start, no cached copy | `JetstreamError::Dictionary`; caller (`main.rs`) surfaces it and the process exits |
| BC8 | zstd dictionary | already cached on disk from a previous run | Client reuses the cached bytes and id, skips the HTTP fetch |
| BC9 | resume cursor | after committing `seq = N` | Reconnects with `cursor = N + 1`, never `N` (§5.4's inclusive-resume rule) |
| BC10 | `JetstreamError` (new error type) | raised | Caught in `src/jetstream/client.rs`; caller sees one log line, and a cold-start `Dictionary` failure exits the process non-zero |

## Acceptance criteria

- [ ] AC1 — Commit events for post, like, repost, and postgate decode into the right `Event` variant. Checked by: `cargo test jetstream::event::tests`
- [ ] AC2 — `#identity`/`#account`/`#sync` frames are swallowed internally. Checked by: `cargo test jetstream::client::tests::ignores_non_commit_kinds`
- [ ] AC3 — The backoff sequence caps at 60s. Checked by: `cargo test jetstream::client::tests::backoff_caps_at_60s`
- [ ] AC4 — The resume cursor is `seq + 1`. Checked by: `cargo test jetstream::client::tests::resume_uses_seq_plus_one`
- [ ] AC5 — A cached dictionary is reused without a network call. Checked by: `cargo test jetstream::client::tests::reuses_cached_dictionary`
- [ ] AC6 — All four gates pass.
- [ ] AC7 — A live connection decodes 10 real frames. Checked by: run by hand: `cargo test -- --ignored jetstream_live_connect`

## Defaults taken

- Dictionary cache: two plain files next to `UPSTAGE_DB_PATH`, e.g.
  `<db_path>.zstd-dict` and `<db_path>.zstd-dict-id`, independent of SQLite.
  The store's `meta.zstd_dict_id` (story 05) is a separate concern; this
  story does not write it.
- Backoff schedule: `min(2^attempt, 60)` seconds.
- `next()` signature: `async fn next(&mut self) -> Result<Event,
  JetstreamError>`, where `Event` is `Commit(CommitEvent) |
  OutdatedCursor`. Identity, account, and sync frames never reach the
  caller.
- Subscribed collections are fixed at `post`, `like`, `repost`, `postgate`,
  matching §5.1 step 2 exactly; not configurable.
- Fixtures recorded by hand with the `#[ignore]` live test, committed under
  `tests/fixtures/`.

## Suggested slices

- 1.0 `jetstream/event.rs` types and tests. Done when `cargo test
  jetstream::event` passes.
- 2.0 `jetstream/client.rs` dictionary fetch, cache, connect, decode. Done
  when `cargo test jetstream::client` passes for non-network cases.
- 3.0 Reconnect, backoff, and resume logic. Done when all four gates pass
  and the backoff tests pass.
