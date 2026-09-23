# 05 — Viewer identity behind the switch

- **Follows**: none
- **PRD story**: Viewers without a valid login
- **Size**: large
- **Design**: docs/02-TECH-DESIGN-network-feed.md §5, §9.4, §10, §4

## Outcome

After this ships, `getFeedSkeleton` can prove who the viewer is. With
`UPSTAGE_PERSONALISE=true`, a request with no token or a token that is not
valid gets an empty 200 page. A request with a valid token gets the global
list with `Cache-Control: private, no-store`. With
`UPSTAGE_PERSONALISE=false`, the default, the handler behaves exactly as
today. The request path never waits for the network.

## Non-goals

- Does not filter the feed. A verified viewer still gets `global`. Story
  06 adds the circle filter.
- Does not add a graph queue. The resolver has no graph job to enqueue
  yet. Story 06 adds the hook.
- Does not track `jti`. A token used again only gets the same feed.
- Does not change the default of `UPSTAGE_PERSONALISE`. It stays `false`
  until story 11, so this story merges with the served feed unchanged.

## Approach

`auth/jwt.rs` splits the token by hand, decodes base64url, and parses the
header and claims with `serde`. `auth/keys.rs` decodes the multibase key
(`bs58`, `z` prefix, multicodec `0xE7 0x01` or `0x80 0x24`) and verifies
with `k256` or `p256`. It rejects a high-S signature before it verifies.
`auth/did.rs` holds the key cache and the resolver task. The handler calls
`auth::verify`, which reads the cache only. A cache miss sends the DID to
the resolver over a bounded `mpsc` channel and returns
`AuthError::KeyUnknown`. The resolver fetches the DID document and fills
the cache. On a signature failure with a cached key, the handler sends a
refetch request, one time for each DID and hour. Tests sign tokens with
keys generated in the test and use DID document fixtures.

## Files in scope

| Path | Change |
|---|---|
| `AGENTS.md` | Adds a rule: `auth/` is the only module that calls the DID resolvers |
| `src/auth/mod.rs` (new) | `verify(token, now, cache, cfg) -> Result<ViewerDid, AuthError>`, `AuthError` |
| `src/auth/jwt.rs` (new) | Split, base64url, header and claims, checks 1 to 5 of §5 |
| `src/auth/keys.rs` (new) | Multibase decode, `k256` and `p256` verify, low-S check |
| `src/auth/did.rs` (new) | `KeyCache` (stale 1 h, removed 24 h, cap `2 × UPSTAGE_MAX_VIEWERS`), resolver task for `did:plc` and `did:web` |
| `src/http/skeleton.rs` | Reads `Authorization`. Branches on the switch. Empty page on `AuthError`. Sets the `Cache-Control` value for each mode |
| `src/http/mod.rs` | `AppState` gets `KeyCache` handle, resolver sender, `personalise` flag |
| `src/ingest/mod.rs` | `run` starts the resolver task only when the switch is `true` |
| `src/config.rs` | `UPSTAGE_PERSONALISE` (default `false`), `UPSTAGE_SERVICE_DID` (default `did:web:<UPSTAGE_HOSTNAME>`), `UPSTAGE_PLC_URL`, `UPSTAGE_MAX_VIEWERS` (1000) |
| `src/main.rs` | `mod auth;` |
| `Cargo.toml` | `k256` and `p256` (features `ecdsa`, `sha256`), `bs58` |

## Behaviour contracts

| Id | Subject | Case | Behaviour |
|---|---|---|---|
| BC1 | switch `false` | any request | JWT not read. Serves `global`. `Cache-Control: public, max-age=30`. Resolver not started |
| BC2 | switch `true` | no `Authorization` header, or not `Bearer` | 200, `{"feed":[]}`, no `cursor` |
| BC3 | `jwt` | not three base64url parts, or bad JSON | `AuthError::Malformed` |
| BC4 | `jwt` | `alg` not `ES256K` or `ES256`, or `typ` present and not `JWT` | `AuthError::Alg` |
| BC5 | `jwt` | `exp` more than 30 s in the past | `AuthError::Expired` |
| BC6 | `jwt` | `aud` without its `#` fragment is not `UPSTAGE_SERVICE_DID` without its fragment | `AuthError::Audience` |
| BC7 | `jwt` | `lxm` present and not `app.bsky.feed.getFeedSkeleton` | `AuthError::Method` |
| BC8 | `jwt` | `iss` not `did:plc:` or `did:web:` | `AuthError::Issuer`. With a fragment, the viewer DID is the part before `#` |
| BC9 | `verify` | key not in cache | `AuthError::KeyUnknown`. DID sent to the resolver. Handler returns an empty page at once |
| BC10 | `keys` | signature S above half the curve order | `AuthError::Signature`, no verify call |
| BC11 | `verify` | signature fails with a cached key | `AuthError::Signature`. One refetch is enqueued for that DID |
| BC12 | `verify` | all checks pass | `Ok(ViewerDid)`. Handler serves `global` with `Cache-Control: private, no-store` |
| BC13 | switch `true` | every empty page | 200, `{"feed":[]}`, no `cursor`, `Cache-Control: private, no-store` |
| BC14 | `KeyCache` | entry older than 1 h | Used, and a background refresh is enqueued |
| BC15 | `KeyCache` | entry older than 24 h | Treated as missing |
| BC16 | `KeyCache` | full | The oldest entry is removed |
| BC17 | resolver | `did:plc` | GET `UPSTAGE_PLC_URL/<did>` |
| BC18 | resolver | `did:web:<host>` | GET `https://<host>/.well-known/did.json` |
| BC19 | resolver | DID document | Key is `publicKeyMultibase` of the method with id ending `#atproto`. A document with no such method caches nothing |
| BC20 | resolver | fetch fails | Logs one warning with the error kind and no DID. Nothing cached |
| BC21 | any log line | auth or resolver | Contains no DID and no token |
| BC22 | `UPSTAGE_PERSONALISE` | not `true` or `false` | `ConfigError` at startup |

## Acceptance criteria

- [ ] AC1 — Valid ES256K and ES256 tokens verify. Checked by: `cargo test auth::tests::valid_tokens`
- [ ] AC2 — Expired, wrong `aud`, wrong `lxm`, bad `iss`, unknown `alg`, high-S and tampered tokens fail with the right error. Checked by: `cargo test auth::tests::rejects`
- [ ] AC3 — Fragments on `aud` and `iss` are removed before the check. Checked by: `cargo test auth::jwt::tests::fragments`
- [ ] AC4 — Multibase keys decode from `did:plc` and `did:web` fixtures. Checked by: `cargo test auth::did::tests::fixtures`
- [ ] AC5 — A rotated key verifies after one refetch. Checked by: `cargo test auth::did::tests::rotation_refetch`
- [ ] AC6 — Cache staleness, removal and cap work. Checked by: `cargo test auth::did::tests::cache_ages`
- [ ] AC7 — An unknown key returns an empty page without a network call on the request path. Checked by: `cargo test http::skeleton::tests::unknown_key_empty_page`
- [ ] AC8 — With the switch `true`, every empty page and every success sends `private, no-store`. Checked by: `cargo test http::skeleton::tests::personalised_headers`
- [ ] AC9 — With the switch `false`, output and headers equal story 01 output. Checked by: `cargo test http::skeleton::tests::switch_off_unchanged`
- [ ] AC10 — `AGENTS.md` names `auth/` as the only DID resolver caller. Checked by: manual review.
- [ ] AC11 — All four gates pass.

## Defaults taken

- `UPSTAGE_PERSONALISE` defaults to `false` in this story (design §4 says
  `true`). Story 11 flips it.
- `UPSTAGE_MAX_VIEWERS` is added here, at 1000, because the key cache cap
  reads it. Story 06 sets the measured default from story 04.
- The resolver channel holds 1024 entries. When it is full, the DID is
  dropped. The next request enqueues it again.
- One refetch for each DID in each hour, so a bad token cannot make the
  resolver call the PLC directory on each request.
- `did:web` hosts with a port or a path are rejected (`AuthError::Issuer`).

## Suggested slices

- 1.0 `auth/jwt.rs` and `auth/keys.rs` with signed-token tests. Done when
  AC1 to AC3 pass.
- 2.0 `auth/did.rs` cache and resolver with fixtures. Done when AC4 to
  AC6 pass.
- 3.0 Config, handler branch, headers, resolver start in `run`. Done when
  AC7 to AC9 pass and all four gates pass.
