# 08 — HTTP serving: did, describe, skeleton, interactions, health

- **Follows**: 07
- **PRD phase**: 2
- **Size**: standard
- **Design**: docs/TECH-DESIGN.md §11, §11.1

## Outcome

After this ships, `axum` serves five routes on `DUNK_HTTP_ADDR`: the DID
document, `describeFeedGenerator`, `getFeedSkeleton`, `sendInteractions`,
and `/healthz`. A Bluesky client can page through the feed with `limit` and
`cursor`, and an operator can poll `/healthz` to see the service is
keeping up with Jetstream and the scorer. The skeleton route reads only the
in-memory snapshot from story 07, never SQLite, so a request stays well
under the 5 ms budget.

## Non-goals

- Does not implement `dunk publish`; story 09 writes the generator record
  this service describes.
- Does not implement guards; they are already reflected in the snapshot by
  stories 07 and 10.
- Does not personalise the feed or validate the service JWT; §11.1 says
  auth is ignored.
- Does not add a companion web page for showing the multiplier; the PRD's
  own known limitation, out of scope.

## Approach

Routes live under `src/http/`, one file per route plus a shared router in
`mod.rs`, mirroring §3's layout. `tower-http`'s trace and timeout layers
wrap the router instead of hand-rolled middleware, since both are already
in §3's dependency list. The cursor is `base64url(rank_bits_hex ":"
quote_cid)`, encoded and decoded in one pure `cursor.rs` module, not
embedded in `skeleton.rs`, so it can be unit-tested without a running
server.

## Files in scope

| Path | Change |
|---|---|
| `src/http/mod.rs` | axum router, shared `AppState` (snapshot handle, config) |
| `src/http/did.rs` | `GET /.well-known/did.json` |
| `src/http/describe.rs` | `GET /xrpc/app.bsky.feed.describeFeedGenerator` |
| `src/http/skeleton.rs` | `GET /xrpc/app.bsky.feed.getFeedSkeleton` |
| `src/http/cursor.rs` | Cursor encode and decode, pure |
| `src/http/interactions.rs` | `POST /xrpc/app.bsky.feed.sendInteractions`, writes to the store |
| `src/http/health.rs` | `GET /healthz` |
| `Cargo.toml` | Adds `axum`, `tower-http` (trace, timeout) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `GET /.well-known/did.json` | request | `{"@context":[...],"id":"did:web:<host>","service":[{"id":"#bsky_fg","type":"BskyFeedGenerator","serviceEndpoint":"https://<host>"}]}`, host from `DUNK_HOSTNAME` |
| BC2 | `GET /xrpc/app.bsky.feed.describeFeedGenerator` | request | `{"did":"did:web:<host>","feeds":[{"uri":"at://<publisher_did>/app.bsky.feed.generator/<rkey>"}]}` |
| BC3 | `getFeedSkeleton` | `feed` param missing or not the configured URI | 400 `{"error":"UnknownFeed"}` |
| BC4 | `getFeedSkeleton` | `limit` param non-numeric | 400 `{"error":"InvalidRequest"}` |
| BC5 | `getFeedSkeleton` | `limit` param out of range (0, 101, negative, or omitted) | Clamped to `1..=100`; default 50 when omitted |
| BC6 | `getFeedSkeleton` | `cursor` param fails to decode | 400 `{"error":"InvalidRequest"}` |
| BC7 | `getFeedSkeleton` | `cursor` no longer matches an item still in the snapshot | Page still starts at the first item with `(rank, cid)` strictly after the decoded value |
| BC8 | `getFeedSkeleton` | happy path | Reads the snapshot `Arc` once, scans or binary-searches to the cursor, takes `limit` items, returns `{"feed":[{"post":uri}...],"cursor":next}`; `cursor` omitted on the last page |
| BC9 | `getFeedSkeleton` | response, `feedContext` | Set to `"r=<ratio, 1 decimal>"`, under 2,000 chars, on every item |
| BC10 | `getFeedSkeleton` | auth | No auth required; a present service JWT is accepted and never validated |
| BC11 | `getFeedSkeleton` | response headers | `Cache-Control: public, max-age=30` on every response |
| BC12 | `sendInteractions` | request | `{}` response; each event row appended to `interactions` through the store writer |
| BC13 | `sendInteractions` | malformed body, not valid JSON or missing a required field | 400, no row written |
| BC14 | `/healthz` | `jetstream_lag_s <= 300` and `last_pass_age_s <= 300` | 200 with `{jetstream_lag_s, last_pass_age_s, snapshot_len}` |
| BC15 | `/healthz` | `jetstream_lag_s > 300` or `last_pass_age_s > 300` | 503, same body |
| BC16 | cursor encode and decode | round trip | `decode(encode(rank, cid)) == (rank, cid)`, including `rank = 0.0` and a CID with base64-special characters |
| BC17 | `SkeletonError` (new error type) | `UnknownFeed` or `InvalidRequest` raised in `src/http/skeleton.rs` | Mapped to the 400 bodies in BC3/BC4/BC6 by an axum error handler in `src/http/mod.rs` |

## Acceptance criteria

- [ ] AC1 — All five routes return the shapes in BC1–BC15. Checked by: `cargo test http::tests`
- [ ] AC2 — `limit` clamps to `1..=100` and defaults to 50. Checked by: `cargo test http::skeleton::tests::limit_clamps`
- [ ] AC3 — The cursor round-trips, and a stale cursor still resolves to the right position. Checked by: `cargo test http::cursor::tests`
- [ ] AC4 — `/healthz` flips to 503 past the 300s thresholds. Checked by: `cargo test http::health::tests::flips_at_300s`
- [ ] AC5 — A request against a 100k-item fake snapshot completes with one scan. Checked by: `cargo test http::skeleton::tests::budget_one_scan`
- [ ] AC6 — `sendInteractions` writes one row per event and never blocks on a full write channel. Checked by: `cargo test http::interactions::tests`
- [ ] AC7 — All four gates pass.

## Defaults taken

- Router state: `Arc<AppState>` holding the snapshot
  `Arc<RwLock<Arc<Vec<FeedItem>>>>`, the store writer handle, and config;
  injected with axum's `State` extractor.
- Cursor lookup uses `Vec::binary_search_by` against `(rank, cid)`, since
  the snapshot is already sorted that way.
- Error body shape follows the AT Proto convention
  `{"error": "<Name>", "message": "<optional>"}`; only `error` is
  populated here.
- `tower-http`'s timeout layer default: 5s per request, a safety net far
  above the 5 ms target, not the target itself.
- `/healthz` computes `jetstream_lag_s` from the writer's record of
  `meta.jetstream_seq`'s wall-clock time, and `last_pass_age_s` from
  `meta.last_scorer_pass`.

## Suggested slices

- 1.0 `cursor.rs` encode and decode, with tests. Done when `cargo test
  http::cursor` passes.
- 2.0 `did.rs`, `describe.rs`, `health.rs`. Done when `cargo test
  http::tests` for these three passes.
- 3.0 `skeleton.rs`, `interactions.rs`, `mod.rs` router wiring. Done when
  `cargo test http` passes and all four gates pass.
