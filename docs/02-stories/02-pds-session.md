# 02 — Shared PDS session with refresh

- **Follows**: none
- **PRD story**: none (prefactor)
- **Size**: standard
- **Design**: docs/02-TECH-DESIGN-network-feed.md §7, §3

## Outcome

After this ships, `appview::pds::PdsClient` owns the PDS login. It logs in
once, refreshes the session before it expires, and logs in again one time
if the refresh fails. It sends App View methods through the PDS with the
`atproto-proxy` header, behind its own limiter. It has two graph methods,
`get_follows` and `get_relationships`. `upstage publish` uses this client
and behaves as before.

## Non-goals

- Does not call the graph methods from `upstage run`. Story 03 calls them
  from the probe. Story 06 calls them from the graph worker.
- Does not change the scorer's public `AppViewClient` or
  `UPSTAGE_APPVIEW_RPS`.
- Does not make `BSKY_HANDLE` or `BSKY_APP_PASSWORD` required for
  `upstage run`. Story 11 does this.
- Does not add a priority queue. The limiter lets one call through at a
  time. Story 06 and story 09 add the queue.

## Approach

The session code moves from `publish.rs` into `appview/pds.rs`. The
existing `publish::PdsClient` trait becomes the transport seam
`PdsTransport`, with one `reqwest` implementation. `PdsClient` is the new
struct. It holds the transport, the credentials, the session in a
`tokio::sync::Mutex`, and a `time::Interval` limiter, as `AppViewClient`
does. Before each call it reads the access token `exp`. If less than 5
minutes remain, it refreshes first. An `ExpiredToken` response also
triggers a refresh and one retry. Retries on 429, 5xx and transport errors
use `appview::retry_decision`. Tests use a fake `PdsTransport` that
records each call.

## Files in scope

| Path | Change |
|---|---|
| `src/appview/pds.rs` (new) | `PdsTransport` trait, `HttpPdsTransport`, `PdsClient`, `PdsError`, `get_follows`, `get_relationships`, session refresh |
| `src/appview/mod.rs` | `pub mod pds;` |
| `src/publish.rs` | Uses `PdsClient` for `createSession`, `uploadBlob`, `putRecord`. Removes its own session code |
| `src/config.rs` | `UPSTAGE_PDS_URL` (default `https://bsky.social`), `UPSTAGE_GRAPH_RPS` (default 8, positive) |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | `PdsClient::login` | valid credentials | One `createSession` call. `accessJwt` and `refreshJwt` stay in memory only |
| BC2 | any call | access token `exp` less than 5 minutes away | `refreshSession` runs first, then the call |
| BC3 | any call | response error `ExpiredToken` | `refreshSession`, then the same call one more time |
| BC4 | `refreshSession` | fails | One `createSession`. If that fails too, the call returns `PdsError::Session` |
| BC5 | App View method (`app.bsky.*`) | request | Sent to `UPSTAGE_PDS_URL/xrpc/<nsid>` with the bearer token and `atproto-proxy: did:web:api.bsky.app#bsky_appview` |
| BC6 | repo method (`com.atproto.*`) | request | Sent with the bearer token and no `atproto-proxy` header |
| BC7 | limiter | many calls | At most `UPSTAGE_GRAPH_RPS` calls each second, one at a time |
| BC8 | 429, 5xx, transport error | retry | Same waits as `01` §8: 1 s, 2 s, 4 s, then `PdsError::Http` |
| BC9 | `get_follows(actor, limit, cursor)` | request | `app.bsky.graph.getFollows?actor=..&limit=..&cursor=..&sort=latest`. Returns subject DIDs in response order and the next cursor |
| BC10 | `get_relationships(actor, others)` | more than 30 DIDs | `PdsError::TooMany` before any network call |
| BC11 | `get_relationships` | response | Returns the DIDs whose relationship has `followedBy` set |
| BC12 | `PdsError` | any variant | Holds no token and no password. `Debug` shows `[redacted]` for secrets |
| BC13 | `upstage publish` | same inputs as today | Same calls, same record, same printed at-URI |
| BC14 | `UPSTAGE_GRAPH_RPS` | 0, negative or not a number | `ConfigError` at startup |

## Acceptance criteria

- [ ] AC1 — Refresh runs 5 minutes before `exp` and on `ExpiredToken`. Checked by: `cargo test appview::pds::tests::refresh_triggers`
- [ ] AC2 — A failed refresh logs in again one time, then fails. Checked by: `cargo test appview::pds::tests::relogin_once_then_fail`
- [ ] AC3 — App View methods carry the proxy header. Repo methods do not. Checked by: `cargo test appview::pds::tests::proxy_header`
- [ ] AC4 — `get_follows` pages to the end and keeps response order. Checked by: `cargo test appview::pds::tests::get_follows_pages`
- [ ] AC5 — `get_relationships` refuses more than 30 DIDs and returns only `followedBy` DIDs. Checked by: `cargo test appview::pds::tests::get_relationships`
- [ ] AC6 — The existing publish tests pass unchanged. Checked by: `cargo test publish`
- [ ] AC7 — The new config variables load with their defaults. Checked by: `cargo test config::tests::pds_defaults`
- [ ] AC8 — A live refresh against the real PDS works. Checked by: run by hand: `cargo test -- --ignored pds_refresh_live`
- [ ] AC9 — All four gates pass.

## Defaults taken

- The trait keeps its seam role under the new name `PdsTransport`, so
  `PdsClient` can be the struct that design §7 names.
- `exp` is read from the access token payload with base64url and
  `serde_json`. The token signature is not checked. The PDS issued it.
- `publish` keeps its own fixed `BSKY_PDS_URL` value only as the default
  of `UPSTAGE_PDS_URL`.

## Suggested slices

- 1.0 `PdsTransport`, `PdsClient` login, refresh and limiter, with the
  fake transport. Done when `cargo test appview::pds` passes for AC1 to
  AC3.
- 2.0 `get_follows` and `get_relationships`. Done when AC4 and AC5 pass.
- 3.0 `publish.rs` uses `PdsClient`. Config variables. Done when
  `cargo test publish` passes and all four gates pass.
