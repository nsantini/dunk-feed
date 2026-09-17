# 02 — Score module and App View client

- **Follows**: 01
- **PRD phase**: 0
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §5.3, §1, §7.1, §8.1

## Outcome

After this ships, `score.rs` exposes `engagement`, `ratio`, `qualifies`, and
`rank` as pure functions with no I/O, each backed by a passing unit test for
its boundary case from TECH-DESIGN §7.1. `appview/` exposes a typed client
over `getPosts`, `getProfiles`, `getQuotes`, and `getFeed` against
`public.api.bsky.app`, rate-limited and retried per §8.1. `validate` (story
03) and the scorer (story 07) can both call the exact functions and the
exact client the running service uses.

## Non-goals

- Does not call `getPosts` inside a scoring pipeline; that is stories 03 and
  07.
- Does not implement guards (follower floor, labels, blocks); story 10.
- Does not implement Jetstream ingestion.
- Does not implement authenticated calls (`createSession`, `uploadBlob`,
  `putRecord`); story 09.

## Approach

`score.rs` is pure so it can be unit-tested without a fixture server, and
shared verbatim by `validate` and the scorer rather than duplicated. The App
View client uses `reqwest` with rustls and typed `serde` structs for only the
fields §8.2 reads, not `atrium-api`, because TECH-DESIGN §1 rejects `atrium`
for its size on a small VM. The rate limiter is a token bucket built from
`tokio::time`, not an external rate-limiting crate, since none is listed in
§3.

## Files in scope

| Path | Change |
|---|---|
| `src/score.rs` | `E`, `D`, `rank`, `qualifies`. Pure. New file |
| `src/appview/mod.rs` | Client: `getPosts`, `getProfiles`, `getQuotes`, `getFeed`; rate limiter, retry |
| `src/appview/types.rs` | serde types, only the fields §8.2 reads |
| `src/ingest/mod.rs` | New file. Declares `pub mod embed;` only. Story 06 adds the task |
| `src/ingest/embed.rs` | Quote detection, pure (§5.3). `detect(record: &Value) -> Embed`. Shared by stories 03 and 06 |
| `Cargo.toml` | Adds `serde`, `serde_json`, `reqwest` (rustls, json) |
| `tests/fixtures/getposts_ok.json` | Recorded `getPosts` response, for tests |
| `tests/fixtures/getprofiles_ok.json` | Recorded `getProfiles` response, for tests |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `ratio` | `E(O) = 0` | Finite, via `k` smoothing; never `NaN` or `Inf` |
| BC2 | `rank` | age increases at fixed `D`, `E(Q)` | Strictly decreasing |
| BC3 | `rank` | `E(Q)` increases at fixed `D` | Strictly increasing |
| BC4 | `qualifies` | both `E(O)` and `E(Q)` below `P` | `false` |
| BC5 | `qualifies` | `E(Q) == P` exactly, `D == M` exactly | `true` (boundary is `>=`, not `>`) |
| BC6 | `engagement` | weights supplied via `Weights` struct, not read from config in this module | Output uses the supplied weight, never a literal `2.0` or `0.5` |
| BC7 | `appview::get_posts` | URI list longer than 25 | Client batches automatically into groups of 25 |
| BC8 | `appview::get_posts` | empty URI list | Returns an empty result, makes no HTTP call |
| BC9 | retry decision (pure function) | HTTP 429 or 5xx, attempt < 3 | Retries with backoff `1s, 2s, 4s` |
| BC10 | retry decision (pure function) | HTTP 429 or 5xx, attempt == 3 (4th failure) | Returns `AppViewError::Failed`; caller never sees a partial result treated as success |
| BC11 | retry decision (pure function) | HTTP 4xx other than 429 | Fails immediately, no retry |
| BC12 | `getPosts` response | a requested URI missing from the result | Absent from the returned map; the client raises no error for this case |
| BC13 | `AppViewError` (new error type) | raised | Caught in `src/appview/mod.rs`; caller sees one log line, never a panic |

## Acceptance criteria

- [ ] AC1 — `score.rs` invariant tests pass. Checked by: `cargo test score::tests`
- [ ] AC2 — `ratio` is finite when `E(O) = 0`. Checked by: `cargo test score::tests::ratio_finite_when_original_dead`
- [ ] AC3 — `rank` strictly decreases with age. Checked by: `cargo test score::tests::rank_decays_with_age`
- [ ] AC4 — `getPosts` batches correctly over 25 URIs using the fixture. Checked by: `cargo test appview::tests::batches_over_25`
- [ ] AC5 — The retry-decision function matches BC9–BC11. Checked by: `cargo test appview::tests::retry_decision`
- [ ] AC6 — All four gates pass.
- [ ] AC7 — A live `getPosts` round trip works. Checked by: run by hand: `cargo test -- --ignored appview_getposts_live`

## Defaults taken

- Retry and backoff logic is a pure function (`status`, `attempt` in,
  decision out), unit-tested directly. No mock-HTTP crate, since none is
  listed in §3; the full network path is covered only by the `#[ignore]`
  live test.
- Fixtures recorded by hand with an `#[ignore]` live test against
  `public.api.bsky.app`, committed under `tests/fixtures/`.
- serde structs use `#[serde(rename_all = "camelCase")]` and ignore unknown
  fields (no `deny_unknown_fields`), since AT Proto lexicons add fields over
  time.
- Retry policy: 3 attempts, backoff `1s, 2s, 4s`, on 429 and 5xx only,
  matching ingest's own backoff shape in §5.1 for consistency.
- Token-bucket rate limiter built with `tokio::time::interval`, capped by
  `DUNK_APPVIEW_RPS` (already parsed in story 01).
- Batch size: 25 for `getPosts` and `getProfiles`, 100 for `getQuotes` and
  `getFeed`, per §8.1.

## Suggested slices

- 1.0 `score.rs` and its tests (shared types first). Done when `cargo test
  score::tests` passes.
- 2.0 `appview/types.rs`, `appview/mod.rs` client, retry logic, fixtures.
  Done when `cargo test appview::tests` passes and all four gates pass.
